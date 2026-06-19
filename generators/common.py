"""
common.py — utilidades compartilhadas entre os geradores.

Centraliza:
- População de devices, geos, CDNs, ISPs (calibrados para o mercado BR)
- Bitrate ladder típica de OTT
- Helpers para determinismo (seeds), timestamps e IDs
- Carregamento de config/config.yaml (com fallback para defaults)

Sem dependências externas além de stdlib + (opcional) pyyaml.
"""
from __future__ import annotations

import hashlib
import json
import os
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Config & seeds
# ---------------------------------------------------------------------------

DEFAULT_CONFIG: dict[str, Any] = {
    # Janela temporal da transmissão (UTC). 105 min = 15 min pre + 90 min jogo.
    "event_start_utc": "2026-05-20T21:45:00Z",
    "event_duration_minutes": 105,
    # Volumetria base do take-home. Ajustar para cima cuidadosamente.
    "n_sessions": 2000,
    # Pico de CCV (concorrentes) — usado para modelar a curva de chegada.
    "peak_concurrency_ratio": 0.65,
    # Determinismo. Mesma seed => mesmos arquivos.
    "seed": 42,
    # Janela de degradação proposital na CDN-b (minutos relativos ao início).
    "cdn_b_degradation_window_min": [60, 75],
    # Probabilidade default de injeções deliberadas.
    "out_of_order_max_seconds": 15,
    "duplicate_rate": 0.01,
    "schema_v2_rate": 0.005,
    "device_clock_skew_seconds": 5,
    # Diretório de saída.
    "output_dir": "data/raw",
}


def load_config(path: str | os.PathLike | None = None) -> dict[str, Any]:
    """Carrega config/config.yaml (se existir e pyyaml estiver instalado); senão, defaults.

    Não obriga pyyaml: o desafio inteiro roda com `python generate_all.py` sem deps.
    """
    cfg = DEFAULT_CONFIG.copy()
    if path is None:
        return cfg
    p = Path(path)
    if not p.exists():
        return cfg
    try:
        import yaml  # type: ignore
    except ImportError:
        # Sem pyyaml: aceita JSON também, ou simplesmente devolve defaults.
        if p.suffix.lower() == ".json":
            cfg.update(json.loads(p.read_text(encoding="utf-8")))
        return cfg
    cfg.update(yaml.safe_load(p.read_text(encoding="utf-8")) or {})
    return cfg


def make_rng(seed: int, salt: str = "") -> random.Random:
    """Cria um RNG isolado e determinístico para cada gerador.

    Por que isolar? Para que rodar só o gerador de SCTE-35 não consuma entropia
    do gerador de player_events — assim a Stream A continua bit-a-bit idêntica.
    """
    h = hashlib.sha256(f"{seed}:{salt}".encode()).digest()
    return random.Random(int.from_bytes(h[:8], "big"))


# ---------------------------------------------------------------------------
# Populações (calibradas, não realistas-realistas — boas o suficiente)
# ---------------------------------------------------------------------------

# (peso, tipo, modelo, OS) — pesos relativos somam ~1.0
DEVICE_POPULATION: list[tuple[float, str, str, str]] = [
    (0.18, "smart_tv", "LG-OLED-2024", "webOS"),
    (0.12, "smart_tv", "Samsung-QLED-2023", "Tizen"),
    (0.10, "smart_tv", "TCL-Roku-2024", "RokuOS"),
    (0.20, "mobile", "iPhone-15", "iOS"),
    (0.15, "mobile", "Samsung-Galaxy-S24", "Android"),
    (0.05, "mobile", "Motorola-G84", "Android"),
    (0.08, "web", "Chrome-Desktop", "Windows"),
    (0.04, "web", "Safari-Desktop", "macOS"),
    (0.03, "tablet", "iPad-Air", "iPadOS"),
    (0.03, "ctv", "Chromecast-4K", "AndroidTV"),
    (0.02, "ctv", "AppleTV-4K", "tvOS"),
]

APP_VERSIONS = ["1.40.2", "1.41.0", "1.41.3", "1.42.0", "1.42.1"]

# (peso, região, estado, principais cidades) — bias para SE como no Brasil real
GEO_POPULATION: list[tuple[float, str, str, list[str]]] = [
    (0.22, "SE", "SP", ["São Paulo", "Campinas", "Santos", "Sorocaba"]),
    (0.15, "SE", "RJ", ["Rio de Janeiro", "Niterói", "Petrópolis"]),
    (0.06, "SE", "MG", ["Belo Horizonte", "Uberlândia"]),
    (0.03, "SE", "ES", ["Vitória", "Vila Velha"]),
    (0.05, "S", "RS", ["Porto Alegre", "Caxias do Sul"]),
    (0.04, "S", "PR", ["Curitiba", "Londrina"]),
    (0.03, "S", "SC", ["Florianópolis", "Joinville"]),
    (0.06, "NE", "BA", ["Salvador", "Feira de Santana"]),
    (0.04, "NE", "PE", ["Recife", "Olinda"]),
    (0.04, "NE", "CE", ["Fortaleza"]),
    (0.04, "NE", "MA", ["São Luís"]),
    (0.03, "NE", "PB", ["João Pessoa"]),
    (0.03, "NE", "RN", ["Natal"]),
    (0.04, "CO", "DF", ["Brasília"]),
    (0.03, "CO", "GO", ["Goiânia"]),
    (0.03, "CO", "MT", ["Cuiabá"]),
    (0.03, "N", "PA", ["Belém"]),
    (0.02, "N", "AM", ["Manaus"]),
    (0.02, "N", "RO", ["Porto Velho"]),
    (0.01, "N", "TO", ["Palmas"]),
]

# CDN mix. cdn-b será degradado deliberadamente na janela configurada.
CDN_POPULATION: list[tuple[float, str]] = [
    (0.50, "cdn-a"),
    (0.30, "cdn-b"),
    (0.20, "cdn-c"),
]

# Provedores de internet — abstratos, com slot maior para "outros".
ISP_POPULATION: list[tuple[float, str]] = [
    (0.22, "isp-vivo"),
    (0.18, "isp-claro"),
    (0.15, "isp-tim"),
    (0.10, "isp-oi"),
    (0.08, "isp-algar"),
    (0.07, "isp-sumicity"),
    (0.20, "isp-outros"),
]

# Bitrate ladder típico de OTT em kbps. Distribuição enviesada para qualidades
# medianas-altas (smart TVs e mobile-WiFi dominam).
BITRATE_LADDER_KBPS: list[tuple[float, int]] = [
    (0.05, 400),
    (0.08, 1200),
    (0.20, 2500),
    (0.30, 4500),
    (0.27, 5800),
    (0.10, 8000),
]

# Códigos de erro de player com pesos. Os códigos seguem convenção solta;
# em produção viriam do SDK do player (Bitmovin/Theo/Shaka/JWPlayer).
ERROR_CODES: list[tuple[float, str]] = [
    (0.35, "NETWORK_TIMEOUT"),
    (0.20, "MANIFEST_DOWNLOAD_FAIL"),
    (0.15, "SEGMENT_DOWNLOAD_FAIL"),
    (0.10, "DRM_LICENSE_FAIL"),
    (0.10, "DECODER_INIT_FAIL"),
    (0.05, "ADS_FAIL"),
    (0.05, "UNKNOWN"),
]


# ---------------------------------------------------------------------------
# Helpers de amostragem ponderada
# ---------------------------------------------------------------------------

def weighted_choice(rng: random.Random, population: list[tuple[float, Any]]) -> Any:
    """Amostragem ponderada simples. `population` é lista de (peso, item)."""
    weights = [w for w, _ in population]
    items = [it for _, it in population]
    return rng.choices(items, weights=weights, k=1)[0]


def weighted_choice_tuple(rng: random.Random, population: list[tuple]) -> tuple:
    """Como `weighted_choice`, mas para tuplas onde a 1ª posição é o peso."""
    weights = [t[0] for t in population]
    return rng.choices(population, weights=weights, k=1)[0]


# ---------------------------------------------------------------------------
# Timestamps & IDs
# ---------------------------------------------------------------------------

def parse_iso(s: str) -> datetime:
    """Parse de ISO-8601 aceitando 'Z' (UTC). Datetime tz-aware."""
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)


def to_iso_ms(dt: datetime) -> str:
    """Serializa em ISO-8601 com milissegundos e 'Z' final."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    s = dt.astimezone(timezone.utc).isoformat(timespec="milliseconds")
    return s.replace("+00:00", "Z")


def stable_user_hash(seed: int, session_idx: int) -> str:
    """Pseudonimização determinística — emula `user_id` já anonimizado pela app.

    Em produção, a chave de mapeamento viveria em um KMS/Vault. Aqui é só
    consistência: mesmo seed => mesmos hashes.
    """
    h = hashlib.sha256(f"user:{seed}:{session_idx // 3}".encode()).hexdigest()
    return h[:16]  # 64 bits são suficientes para o desafio


def uuid_from_rng(rng: random.Random) -> str:
    """UUID v4 determinístico (a partir do RNG). Não use em produção."""
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


# ---------------------------------------------------------------------------
# Curva de chegada (audiência ao vivo)
# ---------------------------------------------------------------------------

@dataclass
class ArrivalCurve:
    """Modela a curva de chegada de espectadores em um evento ao vivo.

    A intuição: público sobe rápido nos primeiros minutos (pre-show), satura
    no início do 2º tempo, dá um leve dip no intervalo, e cai exponencialmente
    após o fim. Implementado como soma de gaussianas + decay.
    """
    duration_min: int
    peak_min: int = 60  # minuto onde a curva atinge o pico
    halftime_min: int = 50  # leve dip no intervalo
    rng: random.Random = field(default_factory=lambda: random.Random())

    def arrival_time_offset_sec(self) -> int:
        """Sorteia um offset em segundos (desde o início) para uma nova sessão."""
        # Mistura: 70% picam no 2º tempo, 20% no 1º tempo, 10% pre-show
        roll = self.rng.random()
        if roll < 0.10:
            mu, sigma = 5, 4  # pre-show
        elif roll < 0.30:
            mu, sigma = 25, 8  # 1º tempo
        elif roll < 0.45:
            # leve onda no intervalo (gente que liga TV no intervalo do jogo)
            mu, sigma = self.halftime_min, 3
        else:
            mu, sigma = self.peak_min, 12  # 2º tempo / climax
        t = self.rng.gauss(mu, sigma)
        t = max(0.0, min(float(self.duration_min - 1), t))
        return int(t * 60)
