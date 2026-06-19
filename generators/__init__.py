"""Geradores sintéticos para o desafio Operação Maracanã.

Módulos:
- common: utils compartilhadas (config, seeds, populações)
- player_events: Stream A (telemetria de player)
- scte35_markers: Stream B (cue tones SCTE-35)
- content_metadata: Dataset C (catálogo SCD)
- ad_decisions: Dataset D (impressões de ad — bônus)
"""

from . import (
    ad_decisions,
    common,
    content_metadata,
    player_events,
    scte35_markers,
)

__all__ = [
    "common",
    "player_events",
    "scte35_markers",
    "content_metadata",
    "ad_decisions",
]
