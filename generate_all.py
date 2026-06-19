"""
generate_all.py — orquestrador dos geradores sintéticos.

Uso típico:
    python generate_all.py                       # defaults (2k sessões)
    python generate_all.py --config config/config.yaml  # com overrides
    python generate_all.py --n-sessions 500      # rápido, para iterar
    python generate_all.py --n-sessions 50000    # stress-test

Ordem de execução:
    1) content_metadata (sem dependências)
    2) scte35_markers   (sem dependências)
    3) player_events    (sem dependências; mais pesado)
    4) ad_decisions     (depende de scte35_markers)

Tudo determinístico via `seed`. Mesmo comando 2x => arquivos byte-idênticos.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from generators import (
    ad_decisions,
    content_metadata,
    player_events,
    scte35_markers,
)
from generators.common import load_config


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Gera datasets sintéticos para o desafio Operação Maracanã.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--config", type=str, default="config/config.yaml",
                   help="Path para config/config.yaml (opcional)")
    p.add_argument("--n-sessions", type=int, default=None,
                   help="Override do número de sessões de player")
    p.add_argument("--seed", type=int, default=None,
                   help="Override da seed (determinismo)")
    p.add_argument("--output-dir", type=str, default=None,
                   help="Override do diretório de saída")
    p.add_argument("--skip", nargs="*",
                   choices=["content", "scte35", "player", "ads"], default=[],
                   help="Pula geradores específicos")
    p.add_argument("--only", nargs="*",
                   choices=["content", "scte35", "player", "ads"], default=None,
                   help="Roda APENAS estes geradores (ignora --skip)")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    cfg = load_config(args.config)

    # CLI overrides têm precedência sobre o arquivo de config
    if args.n_sessions is not None:
        cfg["n_sessions"] = args.n_sessions
    if args.seed is not None:
        cfg["seed"] = args.seed
    if args.output_dir is not None:
        cfg["output_dir"] = args.output_dir

    print("=" * 60)
    print("Operação Maracanã — geração de datasets sintéticos")
    print("=" * 60)
    print(f"config efetiva:\n{json.dumps(cfg, indent=2, default=str)}")
    print("-" * 60)

    Path(cfg["output_dir"]).mkdir(parents=True, exist_ok=True)

    selected = set(args.only) if args.only else (
        {"content", "scte35", "player", "ads"} - set(args.skip)
    )

    paths = {}
    t0 = time.time()

    # Ordem fixa: content e scte35 primeiro (independentes/leves),
    # depois player (pesado), depois ads (depende de scte35).
    if "content" in selected:
        t = time.time()
        paths["content"] = content_metadata.generate(cfg)
        print(f"[content_metadata] OK em {time.time() - t:.1f}s")

    if "scte35" in selected:
        t = time.time()
        paths["scte35"] = scte35_markers.generate(cfg)
        print(f"[scte35_markers] OK em {time.time() - t:.1f}s")

    if "player" in selected:
        t = time.time()
        paths["player"] = player_events.generate(cfg)
        print(f"[player_events] OK em {time.time() - t:.1f}s")

    if "ads" in selected:
        if not (Path(cfg["output_dir"]) / "scte35_markers.jsonl").exists():
            print("[ads] SKIPPED — scte35_markers.jsonl não existe. "
                  "Rode primeiro com 'scte35' incluído.", file=sys.stderr)
        else:
            t = time.time()
            paths["ads"] = ad_decisions.generate(cfg)
            print(f"[ad_decisions] OK em {time.time() - t:.1f}s")

    elapsed = time.time() - t0
    print("-" * 60)
    print(f"Concluído em {elapsed:.1f}s. Artefatos:")
    for name, path in paths.items():
        size_mb = path.stat().st_size / 1024 / 1024
        # Conta linhas (rápido o suficiente para o porte do dataset)
        with path.open("r", encoding="utf-8") as f:
            n_lines = sum(1 for _ in f)
        print(f"  {name:20s} {str(path):50s} {size_mb:>7.1f} MB  {n_lines:>10,} linhas")

    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
