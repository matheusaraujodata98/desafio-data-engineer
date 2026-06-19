"""
scte35_markers.py — Stream B: marcadores SCTE-35 emitidos pelo encoder/playout.

SCTE-35 é o padrão (ANSI/SCTE 35) que injeta cue tones binários no stream MPEG-TS
para sinalizar fronteiras de programa, quebras comerciais (commercial), blackouts
regionais e program_boundary. Em produção, esses cues são extraídos pelo packager
(ex.: AWS MediaTailor, Bitmovin, ou packager interno) e disponibilizados como
eventos JSON para os consumidores downstream (SSAI, analytics, ad ops).

Quantidade típica para um jogo de futebol de 90 min:
  - ~2-3 quebras comerciais no 1º tempo (cada 90s)
  - 1 program_boundary no apito final do 1º tempo
  - 1 commercial de 15 min (intervalo)
  - 1 program_boundary no início do 2º tempo
  - ~2-3 quebras comerciais no 2º tempo
  - ocasionalmente: 1 blackout (rara — direitos regionais)

A geração aqui é DETERMINÍSTICA em relação à seed, mas a estrutura cobre os
casos canônicos. Marcadores são pouquíssimos (~10-15 no total), então o
gerador é simples: hardcoded relativo ao kickoff + jitter.

Saída: JSON Lines em `<output_dir>/scte35_markers.jsonl`.
"""
from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path
from typing import Any

from .common import make_rng, parse_iso, to_iso_ms, uuid_from_rng

CHANNEL = "live-brasileirao-final-2026"

# Schedule canônico (minutos relativos ao início do EVENTO, não do jogo).
# event_start = kickoff - 15 min (pre-show). Logo, kickoff = minuto 15.
# 1º tempo: min 15-60. Intervalo: min 60-75. 2º tempo: min 75-120.
# Mas nosso evento dura só 105 min, então o 2º tempo termina mais cedo.
#
# Lista de tuplas: (minuto_relativo, break_type, duration_s, splice_command)
SCTE35_SCHEDULE: list[tuple[float, str, int, str]] = [
    # Pre-show — abertura comercial
    (5.0,  "commercial",       60,  "splice_insert"),
    (12.0, "program_boundary", 0,   "time_signal"),   # kickoff
    # 1º tempo — VAR/lesão dão margem a 2 quebras curtas
    (30.0, "commercial",       45,  "splice_insert"),
    (45.0, "commercial",       60,  "splice_insert"),
    # Fim do 1º tempo + intervalo
    (60.0, "program_boundary", 0,   "time_signal"),   # apito final do 1T
    (60.5, "commercial",       900, "splice_insert"), # intervalo de 15min
    (75.0, "program_boundary", 0,   "time_signal"),   # início do 2T
    # 2º tempo — quebras mais espaçadas
    (88.0, "commercial",       45,  "splice_insert"),
    # Blackout regional simulado (raro — direitos de transmissão regional)
    (92.5, "blackout",         180, "splice_insert"),
    (100.0, "commercial",      60,  "splice_insert"),
]


def generate(cfg: dict[str, Any]) -> Path:
    """Gera os marcadores SCTE-35. Retorna o path de saída."""
    rng = make_rng(cfg["seed"], salt="scte35")
    event_start = parse_iso(cfg["event_start_utc"])
    duration_s_total = cfg["event_duration_minutes"] * 60

    output_dir = Path(cfg["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "scte35_markers.jsonl"

    markers = []
    event_id_counter = 1000  # event_id_scte cresce monotonicamente por canal
    # PTS (Presentation Time Stamp) — 90kHz clock do MPEG-TS.
    # Começa em um valor aleatório (em produção, vem do encoder).
    pts_base = rng.randint(100_000_000, 900_000_000)

    for rel_min, break_type, duration_s, splice_cmd in SCTE35_SCHEDULE:
        # Jitter de até ±2s no momento do disparo (o encoder não dispara exato)
        jitter_s = rng.uniform(-2.0, 2.0)
        offset_s = rel_min * 60 + jitter_s
        if offset_s < 0 or offset_s > duration_s_total:
            continue  # fora da janela do evento

        wallclock = event_start + timedelta(seconds=offset_s)
        # PTS em ticks de 90kHz desde event_start
        pts_time = pts_base + int(offset_s * 90_000)

        # out_of_network=True para quebras comerciais e blackouts;
        # False para program_boundary (sinaliza "voltamos ao programa").
        out_of_network = break_type in ("commercial", "blackout")

        markers.append({
            "marker_id": uuid_from_rng(rng),
            "channel": CHANNEL,
            "splice_command": splice_cmd,
            "event_id_scte": event_id_counter,
            "out_of_network": out_of_network,
            "pts_time": pts_time,
            "wallclock": to_iso_ms(wallclock),
            "duration_s": duration_s,
            "break_type": break_type,
        })
        event_id_counter += 1

    print(f"[scte35] gerados {len(markers)} marcadores; escrevendo em {output_path}")
    with output_path.open("w", encoding="utf-8") as f:
        for m in markers:
            f.write(json.dumps(m, ensure_ascii=False, separators=(",", ":")))
            f.write("\n")

    return output_path
