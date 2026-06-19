"""
content_metadata.py — Dataset C: catálogo de conteúdos (slowly-changing dim).

Em uma plataforma de mídia real, esta dimensão vem de um Content Management
System (CMS) — Globoplay/Disney/Netflix têm seus próprios. Aqui geramos:

  - O conteúdo principal do evento (`live-brasileirao-final-2026`)
  - ~50 conteúdos auxiliares (highlights, pré-jogo, pós-jogo, novelas
    do mesmo dia, programas concorrentes, VODs)

Cada conteúdo tem direitos por janela (rights_window), classificação,
identificadores externos (EIDR-like e Gracenote-like — formatos abstraídos),
e política de ad pod.

Saída padrão: JSONL. O candidato é livre para converter para Parquet/Iceberg
no pipeline. Se `pyarrow` estiver instalado, geramos um .parquet adicional.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .common import make_rng, parse_iso, to_iso_ms, uuid_from_rng

# Pools de gêneros e políticas de ad — calibrados para uma grade de TV BR
GENRES = [
    "sports", "telenovela", "news", "reality", "drama", "comedy",
    "documentary", "kids", "movie", "variety",
]

AD_POD_POLICIES = ["preroll_only", "midroll_dynamic_ssai", "midroll_static",
                   "no_ads", "preroll_and_midroll"]

CLASSIFICATIONS = ["L", "10", "12", "14", "16", "18"]


def _make_external_ids(rng) -> dict[str, str]:
    """Gera identificadores externos abstratos (EIDR-like / Gracenote-like).

    EIDR real tem formato '10.5240/XXXX-XXXX-XXXX-XXXX-XXXX-C'. Aqui só
    geramos algo que parece com isso. Gracenote TMS IDs têm 12 chars.
    """
    eidr_parts = [f"{rng.randint(0, 0xFFFF):04X}" for _ in range(5)]
    eidr = "10.5240/" + "-".join(eidr_parts) + "-C"
    tms_id = "".join(rng.choices("0123456789", k=12))
    return {"eidr_id": eidr, "gracenote_id": tms_id}


def _make_main_content(cfg, rng) -> dict[str, Any]:
    """O conteúdo principal — a final do Brasileirão. Casa com player_events
    e scte35_markers."""
    event_start = parse_iso(cfg["event_start_utc"])
    duration_min = cfg["event_duration_minutes"]
    rights_start = event_start - timedelta(hours=2)
    rights_end = event_start + timedelta(hours=4)

    return {
        "content_id": "live-brasileirao-final-2026",
        "title": "Final do Brasileirão 2026 — AO VIVO",
        "genre": "sports",
        "is_live": True,
        "is_premium": True,
        "scheduled_start_utc": to_iso_ms(event_start),
        "scheduled_end_utc": to_iso_ms(event_start + timedelta(minutes=duration_min)),
        "rights_window_start": to_iso_ms(rights_start),
        "rights_window_end": to_iso_ms(rights_end),
        "rights_territories": ["BR"],  # restrição geográfica
        "classification": "L",
        "ad_pod_policy": "midroll_dynamic_ssai",
        "language": "pt-BR",
        "duration_min": duration_min,
        **_make_external_ids(rng),
        "created_at": to_iso_ms(event_start - timedelta(days=30)),
        "updated_at": to_iso_ms(event_start - timedelta(days=1)),
    }


def _make_aux_content(rng, idx: int, base_dt: datetime) -> dict[str, Any]:
    """Gera 1 conteúdo auxiliar (novela, news, highlight, etc.)."""
    genre = rng.choice(GENRES)
    is_live = genre in ("news", "sports", "variety") and rng.random() < 0.3
    duration_min = rng.choice([22, 30, 45, 60, 90, 120])

    # Janela de direitos: VOD tem janela longa (meses), live é curta (horas)
    if is_live:
        window_hours = rng.choice([4, 8, 12])
        scheduled = base_dt + timedelta(hours=rng.randint(-12, 24))
    else:
        window_hours = rng.choice([24 * 30, 24 * 90, 24 * 180, 24 * 365])
        scheduled = base_dt + timedelta(days=rng.randint(-365, 0))

    title_prefix = {
        "sports": "Esporte Espetacular",
        "telenovela": "Novela das Nove",
        "news": "Jornal Nacional",
        "reality": "BBB",
        "drama": "Série Original",
        "comedy": "Tá no Ar",
        "documentary": "Doc",
        "kids": "TV Globinho",
        "movie": "Sessão da Tarde",
        "variety": "Domingão",
    }[genre]
    title = f"{title_prefix} — Ep. {rng.randint(1, 200)}"

    return {
        "content_id": f"content-{idx:05d}",
        "title": title,
        "genre": genre,
        "is_live": is_live,
        "is_premium": rng.random() < 0.3,
        "scheduled_start_utc": to_iso_ms(scheduled),
        "scheduled_end_utc": to_iso_ms(scheduled + timedelta(minutes=duration_min)),
        "rights_window_start": to_iso_ms(scheduled - timedelta(hours=1)),
        "rights_window_end": to_iso_ms(scheduled + timedelta(hours=window_hours)),
        "rights_territories": ["BR"] if rng.random() < 0.7 else ["BR", "PT", "AR", "UY"],
        "classification": rng.choices(
            CLASSIFICATIONS, weights=[0.40, 0.20, 0.15, 0.10, 0.10, 0.05]
        )[0],
        "ad_pod_policy": rng.choice(AD_POD_POLICIES),
        "language": "pt-BR",
        "duration_min": duration_min,
        **_make_external_ids(rng),
        "created_at": to_iso_ms(scheduled - timedelta(days=rng.randint(30, 365))),
        "updated_at": to_iso_ms(scheduled - timedelta(days=rng.randint(0, 30))),
    }


def generate(cfg: dict[str, Any], n_aux: int = 50) -> Path:
    """Gera o catálogo. Retorna o path do JSONL (e cria .parquet se possível)."""
    rng = make_rng(cfg["seed"], salt="content_metadata")
    event_start = parse_iso(cfg["event_start_utc"])

    output_dir = Path(cfg["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "content_metadata.jsonl"

    records = [_make_main_content(cfg, rng)]
    for i in range(n_aux):
        records.append(_make_aux_content(rng, i + 1, event_start))

    print(f"[content_metadata] gerados {len(records)} conteúdos; escrevendo em {output_path}")
    with output_path.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False, separators=(",", ":")))
            f.write("\n")

    # Bonus: se pyarrow estiver disponível, escreve também como Parquet.
    try:
        import pyarrow as pa  # type: ignore
        import pyarrow.parquet as pq  # type: ignore

        parquet_path = output_dir / "content_metadata.parquet"
        # Normaliza listas para arrays do pyarrow
        table = pa.Table.from_pylist(records)
        pq.write_table(table, parquet_path, compression="snappy")
        print(f"[content_metadata] também escreveu {parquet_path}")
    except ImportError:
        pass  # silencioso — Parquet é opcional

    return output_path
