"""
player_events.py — Stream A: telemetria de player (CMCD-like).

Modelo conceitual:
- Cada sessão = um espectador assistindo a "live-brasileirao-final".
- Sessão tem device, geo, CDN, user_id, app_version, clock_skew.
- Dentro da sessão, sequência realista de eventos:
    video_start → [heartbeat | bitrate_switch | buffer_start/end | seek
                   | ad_start/end | pause/play | error]* → video_end
- Heartbeats a cada ~10s (sinal de "ainda estou assistindo").
- Eventos discretos com probabilidades calibradas por device/CDN.

Injeções deliberadas (NÃO remova — são o ponto pedagógico do desafio):
  (i)   out-of-order em até `out_of_order_max_seconds`
  (ii)  duplicatas (~`duplicate_rate`) — simula at-least-once do broker
  (iii) ~`schema_v2_rate` de mensagens com campo extra `network_type`
  (iv)  burst de erros em `cdn-b` durante `cdn_b_degradation_window_min`
  (v)   clock skew de ±`device_clock_skew_seconds` (CONSISTENTE por device)

Saída: JSON Lines em `<output_dir>/player_events.jsonl` (1 evento por linha).
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterator

from .common import (
    APP_VERSIONS,
    ArrivalCurve,
    BITRATE_LADDER_KBPS,
    CDN_POPULATION,
    DEVICE_POPULATION,
    ERROR_CODES,
    GEO_POPULATION,
    ISP_POPULATION,
    make_rng,
    parse_iso,
    stable_user_hash,
    to_iso_ms,
    uuid_from_rng,
    weighted_choice,
    weighted_choice_tuple,
)

# Constantes do conteúdo. Apenas 1 conteúdo no take-home — o candidato pode
# extender. content_id casa com content_metadata.py e scte35_markers.py.
CONTENT_ID = "live-brasileirao-final-2026"
CHANNEL = "live-brasileirao-final-2026"

# Intervalo médio de heartbeat (segundos). Em produção 5-30s; usamos 10s.
HEARTBEAT_INTERVAL_S = 10
HEARTBEAT_JITTER_S = 2


def _pick_session_profile(rng, cfg, session_idx: int) -> dict[str, Any]:
    """Sorteia as propriedades estáticas de uma sessão (device, geo, CDN, etc)."""
    dev = weighted_choice_tuple(rng, DEVICE_POPULATION)  # (peso, type, model, os)
    geo = weighted_choice_tuple(rng, GEO_POPULATION)     # (peso, region, state, [cities])
    cdn = weighted_choice_tuple(rng, CDN_POPULATION)     # (peso, cdn_name)
    isp = weighted_choice_tuple(rng, ISP_POPULATION)     # (peso, isp_name)

    # Clock skew CONSISTENTE por device — mesmo device sempre erra na mesma direção.
    skew_s = rng.uniform(-cfg["device_clock_skew_seconds"], cfg["device_clock_skew_seconds"])

    # Duração-alvo da sessão. Lognormal: maioria fica 30-90 min, alguns saem cedo.
    duration_s = int(rng.lognormvariate(mu=7.8, sigma=0.7))
    duration_s = max(60, min(duration_s, cfg["event_duration_minutes"] * 60))

    return {
        "session_id": uuid_from_rng(rng),
        "user_id": stable_user_hash(cfg["seed"], session_idx),
        "device": {
            "type": dev[1],
            "model": dev[2],
            "os": dev[3],
            "app_version": rng.choice(APP_VERSIONS),
        },
        "geo": {
            "region": geo[1],
            "state": geo[2],
            "city": rng.choice(geo[3]),
            "isp": isp[1],
        },
        "cdn": cdn[1],
        "clock_skew_s": skew_s,
        "duration_s": duration_s,
        "starting_bitrate_kbps": weighted_choice(rng, BITRATE_LADDER_KBPS),
    }


def _in_degradation_window(elapsed_s: int, cfg) -> bool:
    """True se `elapsed_s` (segundos desde início do evento) está na janela de
    degradação proposital da cdn-b."""
    start_min, end_min = cfg["cdn_b_degradation_window_min"]
    return start_min * 60 <= elapsed_s < end_min * 60


def _emit_event(
    rng,
    session: dict[str, Any],
    event_type: str,
    real_wallclock: datetime,
    cfg: dict,
    playhead_s: int,
    bitrate_kbps: int,
    buffer_length_ms: int,
    error_code: str | None = None,
) -> dict[str, Any]:
    """Monta o dict do evento, aplicando schema v1 (default) ou v2 (~0.5%)."""
    # Aplica clock skew do device — timestamp REPORTADO pelo cliente.
    reported_ts = real_wallclock + timedelta(seconds=session["clock_skew_s"])

    evt: dict[str, Any] = {
        "event_id": uuid_from_rng(rng),
        "session_id": session["session_id"],
        "user_id": session["user_id"],
        "timestamp": to_iso_ms(reported_ts),
        "event_type": event_type,
        "content_id": CONTENT_ID,
        "is_live": True,
        "device": session["device"],
        "geo": session["geo"],
        "cdn": session["cdn"],
        "bitrate_kbps": bitrate_kbps,
        "buffer_length_ms": buffer_length_ms,
        "playhead_position_s": playhead_s,
        "error_code": error_code,
    }

    # Schema v2: adiciona network_type. Só ~0.5% das mensagens —
    # o candidato precisa lidar com schema evolution BACKWARD-compatible.
    if rng.random() < cfg["schema_v2_rate"]:
        evt["schema_version"] = "v2"
        evt["network_type"] = rng.choices(
            ["wifi", "cellular", "ethernet", "unknown"],
            weights=[0.55, 0.30, 0.10, 0.05],
            k=1,
        )[0]
    return evt


def _generate_session_events(
    rng, session: dict[str, Any], session_start: datetime, cfg: dict
) -> Iterator[dict[str, Any]]:
    """Gera a sequência completa de eventos de UMA sessão.

    A lógica é uma máquina de estados simplificada:
    - bitrate sobe/desce conforme ABR adaptativo
    - buffers acontecem com probabilidade que escala com a janela de degradação
    - ad_start/end são emitidos quando o playhead "toca" um marker SCTE-35
      (aqui não temos os markers carregados — emitimos heuristicamente
      em torno de minuto 30 (intervalo do jogo) e dentro das janelas de break)
    """
    duration_s = session["duration_s"]
    event_start = parse_iso(cfg["event_start_utc"])
    elapsed_at_start = int((session_start - event_start).total_seconds())

    current_bitrate = session["starting_bitrate_kbps"]
    current_buffer_ms = rng.randint(8000, 15000)
    playhead = 0  # segundos desde o início da SESSÃO (não do evento)

    # video_start
    yield _emit_event(
        rng, session, "video_start", session_start, cfg,
        playhead, current_bitrate, current_buffer_ms,
    )

    # Loop principal: avança em passos de ~HEARTBEAT_INTERVAL_S.
    t = 0
    in_ad_break = False
    while t < duration_s:
        step = HEARTBEAT_INTERVAL_S + rng.randint(-HEARTBEAT_JITTER_S, HEARTBEAT_JITTER_S)
        t += step
        playhead += step
        wallclock = session_start + timedelta(seconds=t)
        elapsed_global = elapsed_at_start + t  # segundos desde início do EVENTO

        # ---- Probabilidade de buffer (escala se for cdn-b na janela ruim) ----
        buffer_prob = 0.015  # 1.5% por heartbeat em condições normais
        if session["cdn"] == "cdn-b" and _in_degradation_window(elapsed_global, cfg):
            buffer_prob = 0.18  # ~12x pior na janela ruim
        if rng.random() < buffer_prob:
            current_buffer_ms = max(500, current_buffer_ms - rng.randint(3000, 8000))
            yield _emit_event(
                rng, session, "buffer_start", wallclock, cfg,
                playhead, current_bitrate, current_buffer_ms,
            )
            # rebuffering dura entre 0.5s e 8s (mais alto na janela ruim)
            max_rebuf_ms = 8000 if buffer_prob > 0.05 else 3000
            rebuf_ms = rng.randint(500, max_rebuf_ms)
            wallclock_end = wallclock + timedelta(milliseconds=rebuf_ms)
            current_buffer_ms = rng.randint(10000, 18000)
            yield _emit_event(
                rng, session, "buffer_end", wallclock_end, cfg,
                playhead, current_bitrate, current_buffer_ms,
            )

        # ---- Probabilidade de erro fatal de player (mata a sessão) ----
        error_prob = 0.0005
        if session["cdn"] == "cdn-b" and _in_degradation_window(elapsed_global, cfg):
            error_prob = 0.015  # ~30x pior
        if rng.random() < error_prob:
            err_code = weighted_choice(rng, ERROR_CODES)
            yield _emit_event(
                rng, session, "error", wallclock, cfg,
                playhead, current_bitrate, current_buffer_ms, error_code=err_code,
            )
            return  # sessão encerra abruptamente — sem video_end

        # ---- Bitrate switch (ABR) ----
        if rng.random() < 0.05:
            new_bitrate = weighted_choice(rng, BITRATE_LADDER_KBPS)
            if new_bitrate != current_bitrate:
                current_bitrate = new_bitrate
                yield _emit_event(
                    rng, session, "bitrate_switch", wallclock, cfg,
                    playhead, current_bitrate, current_buffer_ms,
                )

        # ---- Ad break (intervalo). Modelado em torno do min 47-50 do evento ----
        # Sem ler SCTE-35 markers diretamente; o candidato cruzará os dois
        # streams na junção temporal. Aqui só geramos os eventos de ad para
        # tornar o dataset rico.
        if not in_ad_break and 47 * 60 <= elapsed_global < 50 * 60 and rng.random() < 0.4:
            in_ad_break = True
            yield _emit_event(
                rng, session, "ad_start", wallclock, cfg,
                playhead, current_bitrate, current_buffer_ms,
            )
        elif in_ad_break and elapsed_global >= 50 * 60:
            in_ad_break = False
            yield _emit_event(
                rng, session, "ad_end", wallclock, cfg,
                playhead, current_bitrate, current_buffer_ms,
            )

        # ---- Heartbeat (sempre, exceto durante ads) ----
        if not in_ad_break:
            yield _emit_event(
                rng, session, "heartbeat", wallclock, cfg,
                playhead, current_bitrate, current_buffer_ms,
            )

    # video_end (se chegou até aqui sem erro fatal)
    end_wallclock = session_start + timedelta(seconds=duration_s)
    yield _emit_event(
        rng, session, "video_end", end_wallclock, cfg,
        playhead, current_bitrate, current_buffer_ms,
    )


def _apply_out_of_order_and_dupes(
    events: list[dict[str, Any]], rng, cfg
) -> list[dict[str, Any]]:
    """Embaralha localmente para simular out-of-order (até `max_seconds` de janela)
    e injeta duplicatas (mesmo `event_id` aparece duas vezes — simulando
    at-least-once do broker).

    Estratégia de OOO: dividir em "frames" de N segundos e embaralhar dentro
    do frame. Isso é mais realista que shuffle aleatório global.
    """
    if not events:
        return events

    # Duplicatas primeiro — adicionamos antes do shuffle para que a cópia
    # também participe do reordenamento.
    duplicated: list[dict[str, Any]] = []
    for evt in events:
        duplicated.append(evt)
        if rng.random() < cfg["duplicate_rate"]:
            duplicated.append(dict(evt))  # cópia rasa preserva mesmo event_id

    # Ordena por timestamp para garantir base ordenada antes do shuffle local.
    duplicated.sort(key=lambda e: e["timestamp"])

    # Out-of-order: para cada evento, com prob ~ janela/total, troca com vizinho.
    max_ooo_s = cfg["out_of_order_max_seconds"]
    # Tamanho médio de "frame" — 2x a janela para garantir cobertura
    frame_size = max_ooo_s * 2

    n = len(duplicated)
    i = 0
    while i < n:
        j = min(i + frame_size, n)
        sub = duplicated[i:j]
        rng.shuffle(sub)
        duplicated[i:j] = sub
        i = j
    return duplicated


def generate(cfg: dict[str, Any]) -> Path:
    """Gera o arquivo JSONL completo. Retorna o path de saída."""
    rng = make_rng(cfg["seed"], salt="player_events")
    event_start = parse_iso(cfg["event_start_utc"])
    curve = ArrivalCurve(
        duration_min=cfg["event_duration_minutes"],
        peak_min=60,
        halftime_min=50,
        rng=make_rng(cfg["seed"], salt="arrival_curve"),
    )

    output_dir = Path(cfg["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "player_events.jsonl"

    all_events: list[dict[str, Any]] = []
    n_sessions = cfg["n_sessions"]

    print(f"[player_events] gerando {n_sessions} sessões...")
    for idx in range(n_sessions):
        session = _pick_session_profile(rng, cfg, idx)
        offset_s = curve.arrival_time_offset_sec()
        session_start = event_start + timedelta(seconds=offset_s)
        all_events.extend(_generate_session_events(rng, session, session_start, cfg))

        if (idx + 1) % 500 == 0:
            print(f"  ...{idx + 1}/{n_sessions} sessões processadas")

    print(f"[player_events] {len(all_events)} eventos brutos; aplicando OOO + dupes...")
    all_events = _apply_out_of_order_and_dupes(all_events, rng, cfg)

    print(f"[player_events] escrevendo {len(all_events)} linhas em {output_path}")
    with output_path.open("w", encoding="utf-8") as f:
        for evt in all_events:
            f.write(json.dumps(evt, ensure_ascii=False, separators=(",", ":")))
            f.write("\n")

    return output_path
