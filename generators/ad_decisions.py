"""
ad_decisions.py — Dataset D (bônus): respostas do ad server por impressão.

Em SSAI (Server-Side Ad Insertion), o ad server (ex.: Google Ad Manager,
FreeWheel, Magnite, Yospace) recebe uma chamada por slot SCTE-35 e decide
qual creative servir para cada impressão. Este dataset emula a resposta:
uma linha por (user, marker, slot) descrevendo a decisão tomada.

Volume: para 2000 sessões, esperamos que ~80% peguem pelo menos 1 ad pod
durante o jogo (3-5 markers comerciais), com 2-4 anúncios por pod (slots).
Isso dá algo entre 30k–80k decisões — adequado para o take-home.

Caso de uso pedagógico: na camada Gold, o candidato deve fazer um JOIN
3-way entre player_events (eventos ad_start/ad_end), scte35_markers
(marker_id, duração) e ad_decisions (creative, advertiser, CPM) para
calcular impressões reais e revenue por anunciante. É um padrão clássico
de ad tech analytics.

Saída: JSONL em `<output_dir>/ad_decisions.jsonl`.
"""
from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path
from typing import Any

from .common import make_rng, parse_iso, to_iso_ms, uuid_from_rng

# Pool de anunciantes e criativos. Calibrado para uma final de Brasileirão:
# grandes patrocínios + automotivo + bebida + telco + bancos.
ADVERTISERS = [
    ("adv-001", "Cervejaria Brahma"),
    ("adv-002", "Banco Itaú"),
    ("adv-003", "Vivo Telecom"),
    ("adv-004", "Magalu"),
    ("adv-005", "Volkswagen"),
    ("adv-006", "Coca-Cola"),
    ("adv-007", "Nubank"),
    ("adv-008", "Mercado Livre"),
    ("adv-009", "iFood"),
    ("adv-010", "Casas Bahia"),
    ("adv-011", "Bradesco"),
    ("adv-012", "Stellantis (Fiat)"),
    ("adv-013", "Amstel"),
    ("adv-014", "TIM Brasil"),
    ("adv-015", "Heineken"),
]

# Cada anunciante tem 1-3 creatives ativos (versão 15s, 30s, 60s)
CREATIVE_DURATIONS = [15, 30, 60]


def _build_creative_pool(rng) -> list[dict[str, Any]]:
    """Constrói pool de criativos: ~30-50 criativos no total."""
    pool = []
    for adv_id, adv_name in ADVERTISERS:
        n_creatives = rng.randint(1, 3)
        for _ in range(n_creatives):
            dur = rng.choice(CREATIVE_DURATIONS)
            # CPM base por anunciante (R$) — varia bastante por categoria
            base_cpm = rng.uniform(35.0, 120.0)
            pool.append({
                "creative_id": uuid_from_rng(rng),
                "advertiser_id": adv_id,
                "advertiser_name": adv_name,
                "duration_s": dur,
                "base_cpm_brl": round(base_cpm, 2),
            })
    return pool


def _generate_decisions_for_marker(
    rng, marker: dict[str, Any], creative_pool: list[dict[str, Any]],
    n_sessions: int, cfg: dict
) -> list[dict[str, Any]]:
    """Para um marker (commercial), gera as decisões de ad servidas a cada usuário.

    Aproximamos: ~60% das sessões ativas no momento do marker pegam o ad pod.
    Cada pod tem 2-4 slots (creatives diferentes).
    """
    if marker["break_type"] != "commercial":
        return []  # blackouts e program_boundary não geram impressões

    # Quantas sessões estarão "ativas" perto desse marker (heurística simples)
    active_ratio = 0.65 if 60 <= marker["event_id_scte"] - 1000 < 90 else 0.45
    n_active = int(n_sessions * active_ratio)
    n_impressioned = int(n_active * rng.uniform(0.55, 0.75))

    # Quantos slots no pod (3-6 dependendo da duração do break)
    duration = marker["duration_s"]
    if duration >= 600:
        n_slots = rng.randint(10, 18)  # intervalo longo
    elif duration >= 60:
        n_slots = rng.randint(2, 4)
    else:
        n_slots = rng.randint(1, 2)

    marker_start = marker["wallclock"]
    # Para cada sessão impressionada, escolhemos creatives diferentes (frequency cap)
    decisions = []
    for _ in range(n_impressioned):
        served_at = marker_start  # simplificação — slot 0 do pod
        # Escolhe N creatives sem repetição (ou com, se pool pequeno)
        picked = rng.sample(creative_pool, k=min(n_slots, len(creative_pool)))
        for slot_idx, creative in enumerate(picked):
            # CPM final = base ± jitter (real-time bidding)
            final_cpm = creative["base_cpm_brl"] * rng.uniform(0.85, 1.20)
            decisions.append({
                "decision_id": uuid_from_rng(rng),
                "marker_id": marker["marker_id"],
                "event_id_scte": marker["event_id_scte"],
                "channel": marker["channel"],
                "user_id_anon": "u-" + uuid_from_rng(rng)[:12],
                "slot_index": slot_idx,
                "creative_id": creative["creative_id"],
                "creative_duration_s": creative["duration_s"],
                "advertiser_id": creative["advertiser_id"],
                "advertiser_name": creative["advertiser_name"],
                "price_cpm_brl": round(final_cpm, 2),
                "served_ts": served_at,
                "ssai_provider": "adserver-internal",
            })
    return decisions


def generate(cfg: dict[str, Any]) -> Path:
    """Gera ad_decisions cruzando com scte35_markers já produzidos.

    Importante: este gerador LÊ scte35_markers.jsonl da saída — por isso
    deve rodar DEPOIS dele. O orquestrador (generate_all.py) garante a ordem.
    """
    rng = make_rng(cfg["seed"], salt="ad_decisions")
    output_dir = Path(cfg["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)

    markers_path = output_dir / "scte35_markers.jsonl"
    if not markers_path.exists():
        raise FileNotFoundError(
            f"{markers_path} não encontrado. Rode o gerador de SCTE-35 primeiro."
        )

    markers = []
    with markers_path.open("r", encoding="utf-8") as f:
        for line in f:
            markers.append(json.loads(line))

    creative_pool = _build_creative_pool(rng)
    print(f"[ad_decisions] pool de {len(creative_pool)} criativos; "
          f"processando {len(markers)} markers")

    all_decisions = []
    for marker in markers:
        decisions = _generate_decisions_for_marker(
            rng, marker, creative_pool, cfg["n_sessions"], cfg
        )
        all_decisions.extend(decisions)

    output_path = output_dir / "ad_decisions.jsonl"
    print(f"[ad_decisions] gerados {len(all_decisions)} decisões; "
          f"escrevendo em {output_path}")
    with output_path.open("w", encoding="utf-8") as f:
        for d in all_decisions:
            f.write(json.dumps(d, ensure_ascii=False, separators=(",", ":")))
            f.write("\n")

    return output_path
