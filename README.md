# 🏟️ Operação Maracanã - Solução de Engenharia de Dados
**Candidato:** Matheus Araujo
**Design Doc:** Para detalhes profundos de arquitetura, trade-offs, resolução do cruzamento financeiro e anomalias de QoE, acesse o nosso [DESIGN.md](./docs/DESIGN.md).

## 📊 Dashboard em Produção (Cloud)

**🔗 Acesse ao vivo:** **[desafio-data-engineer-globo.streamlit.app](https://desafio-data-engineer-globo.streamlit.app)**

O dashboard de negócios, que inicialmente rodava apenas localmente (conectado a um PostgreSQL em `localhost`), agora está publicado na nuvem via **Streamlit Community Cloud**. O app está conectado a uma instância PostgreSQL gerenciada, lendo diretamente a camada **Gold** do pipeline (reconciliação financeira + métricas de QoE), sem necessidade de subir nada localmente para visualizar os resultados.

> ℹ️ Para detalhes de como esse deploy foi feito (variáveis de ambiente, secrets, arquitetura de conexão), veja a seção [Deploy no Streamlit Cloud](#deploy-no-streamlit-cloud) mais abaixo.

## 🚀 Como Executar o Projeto (How to Run)

Este pipeline foi construído para rodar localmente utilizando Docker para infraestrutura e PySpark para o processamento de dados. 

**Pré-requisitos:** Python 3.12+, Docker e Docker Compose.

**1. Subir a Infraestrutura Base (Kafka/PostgreSQL)**
```bash
docker-compose up -d
```

**2. Gerar os Dados Sintéticos**
```bash
python generate_all.py --n-sessions 2000
```

**3. Executar o Pipeline PySpark (Medallion Architecture)**
```bash
# Ingestão e Validação de Contrato
python jobs/bronze/producer.py

# Processamento Streaming e QoE (Deixe rodando em um terminal)
python jobs/silver/streaming.py

# Reconciliação Financeira Batch
python jobs/gold/batch_gold.py
```

**4. Visualizar o Dashboard de Negócios (Streamlit) — versão local**
```bash
streamlit run app/app.py
```
> 💡 Não quer rodar nada localmente? O mesmo dashboard já está publicado na nuvem: **[desafio-data-engineer-globo.streamlit.app](https://desafio-data-engineer-globo.streamlit.app)** (veja detalhes do deploy [aqui](#deploy-no-streamlit-cloud)).

## Deploy no Streamlit Cloud

O dashboard está publicado via GitHub + Streamlit Cloud, evoluindo de uma execução 100% local (item 4 do "Como Executar") para uma versão acessível publicamente:

**🔗 App em produção:** https://desafio-data-engineer-globo.streamlit.app

### Como o deploy foi feito

1. Publicação deste repositório no GitHub.
2. No [Streamlit Cloud](https://streamlit.io/cloud), o app foi apontado para `app/app.py` como entrypoint.
3. As variáveis de conexão com o PostgreSQL foram configuradas via **Secrets** do próprio painel do Streamlit Cloud (não no repositório):
   - `POSTGRES_HOST`
   - `POSTGRES_PORT`
   - `POSTGRES_DB`
   - `POSTGRES_USER`
   - `POSTGRES_PASSWORD`

### Local vs. Nuvem

| | Execução Local | Execução em Nuvem (atual) |
|---|---|---|
| **Onde roda** | `streamlit run app/app.py` na máquina do usuário | Streamlit Community Cloud |
| **Banco de dados** | PostgreSQL local via Docker Compose (`localhost`) | PostgreSQL gerenciado, acessado remotamente |
| **Configuração de credenciais** | Arquivo `.env` / `secrets.toml` local (nunca versionado) | Secrets configurados diretamente no painel do Streamlit Cloud |
| **Acesso** | Apenas na própria máquina | URL pública, qualquer avaliador pode acessar |

⚠️ **Sem as variáveis de ambiente configuradas**, o dashboard tenta conectar em `localhost`, o que funciona apenas na execução local — esse é o fallback proposital para manter o app utilizável em ambos os cenários sem alterar código.

🔒 **Sobre segurança:** nenhuma credencial de banco está commitada neste repositório. O `.gitignore` foi reforçado para bloquear `.env`, `secrets.toml`, `.streamlit/`, chaves (`*.pem`, `*.key`), e arquivos de credenciais de cloud (`credentials.json`, `.aws/`, `.gcp/`) — veja a seção correspondente do arquivo.

## 🤖 Declaração de Transparência (Uso de IA)
Conforme solicitado na Fase 5 do desafio, declaro o uso de assistentes de Inteligência Artificial (Copilot / Gemini) durante o desenvolvimento deste projeto. 
* **Onde a IA foi utilizada:** Geração de *boilerplate* de código, refatoração e organização da árvore de diretórios, e revisão de sintaxe de documentação.
* **Onde a IA NÃO tomou decisões:** A modelagem arquitetural (Medallion), a escolha de algoritmos de otimização (HyperLogLog), as estratégias de retenção de estado em *streaming* (*Watermarks*), e a lógica matemática para resolução de anomalias financeiras (*Fan-Out* em CTEs) foram decisões exclusivas de engenharia humana.

---

# Geradores sintéticos — "Operação Maracanã"

Geradores para os quatro datasets do desafio. Saída em JSON Lines, determinística por seed.

## TL;DR

```bash
# 1) Clonar o repo, entrar na pasta deste README.
# 2) (Opcional) instalar extras: pip install -r requirements.txt
# 3) Rodar:
python generate_all.py                       # default: 2000 sessões, ~30s
python generate_all.py --n-sessions 500      # rápido, ~5s
python generate_all.py --only scte35 content # só metadados, segundos
```

Saída em `data/raw/`:

| Arquivo | Conteúdo | Volume típico (2k sessões) |
|---|---|---|
| `player_events.jsonl`     | Stream A — telemetria de player    | ~525k linhas / ~260 MB |
| `scte35_markers.jsonl`    | Stream B — cue tones SCTE-35       | ~10 linhas / <1 KB |
| `content_metadata.jsonl`  | Dataset C — catálogo (SCD)          | ~51 linhas / ~20 KB |
| `content_metadata.parquet` | (se pyarrow instalado)              | ~51 linhas / ~10 KB |
| `ad_decisions.jsonl`      | Dataset D — impressões de ad (bônus) | ~30-80k linhas / ~10-25 MB |

## Estrutura do projeto

```
generators/
├── __init__.py
├── common.py            # config, RNG determinístico, populações, ArrivalCurve
├── player_events.py     # Stream A
├── scte35_markers.py    # Stream B
├── content_metadata.py  # Dataset C (JSONL + Parquet opcional)
└── ad_decisions.py      # Dataset D
config/config.yaml       # parâmetros (override por CLI também)
docs/DESIGN.md           # design doc principal
docs/evidencias/         # evidências e resultados CSV
sql/                     # queries de negócio e reconciliação
app/                     # futuro dashboard Streamlit
tests/                   # testes futuros
generate_all.py          # orquestrador
requirements.txt         # deps OPCIONAIS (PyYAML, pyarrow)
```

## Schemas (exemplo por arquivo)

### `player_events.jsonl` — uma linha por evento

```json
{
  "event_id": "8f3c1b2e-...",
  "session_id": "a1b2c3d4-...",
  "user_id": "9f8e7d6c5b4a3210",
  "timestamp": "2026-05-20T22:14:33.812Z",
  "event_type": "heartbeat",
  "content_id": "live-brasileirao-final-2026",
  "is_live": true,
  "device": {
    "type": "smart_tv", "model": "LG-OLED-2024",
    "os": "webOS", "app_version": "1.42.0"
  },
  "geo": {"region": "SE", "state": "RJ", "city": "Rio de Janeiro", "isp": "isp-vivo"},
  "cdn": "cdn-a",
  "bitrate_kbps": 5800,
  "buffer_length_ms": 12000,
  "playhead_position_s": 4823,
  "error_code": null
}
```

**Event types emitidos:** `video_start`, `video_end`, `heartbeat`, `buffer_start`, `buffer_end`, `bitrate_switch`, `ad_start`, `ad_end`, `error`. `seek` está reservado mas não é gerado nesta versão — pode ser estendido pelo candidato.

**Schema v2** (~0.5% das mensagens): adiciona `schema_version: "v2"` e `network_type: "wifi" | "cellular" | "ethernet" | "unknown"`. Use isto para demonstrar schema evolution com compat BACKWARD.

### `scte35_markers.jsonl` — uma linha por marcador

```json
{
  "marker_id": "e1d2c3b4-...",
  "channel": "live-brasileirao-final-2026",
  "splice_command": "splice_insert",
  "event_id_scte": 1003,
  "out_of_network": true,
  "pts_time": 459000000,
  "wallclock": "2026-05-20T22:14:30.000Z",
  "duration_s": 60,
  "break_type": "commercial"
}
```

`break_type` ∈ {`commercial`, `program_boundary`, `blackout`}. `splice_command` ∈ {`splice_insert`, `time_signal`}.

### `content_metadata.jsonl` — uma linha por conteúdo

```json
{
  "content_id": "live-brasileirao-final-2026",
  "title": "Final do Brasileirão 2026 — AO VIVO",
  "genre": "sports",
  "is_live": true,
  "is_premium": true,
  "scheduled_start_utc": "2026-05-20T21:45:00.000Z",
  "scheduled_end_utc":   "2026-05-20T23:30:00.000Z",
  "rights_window_start": "2026-05-20T19:45:00.000Z",
  "rights_window_end":   "2026-05-21T01:45:00.000Z",
  "rights_territories": ["BR"],
  "classification": "L",
  "ad_pod_policy": "midroll_dynamic_ssai",
  "language": "pt-BR",
  "duration_min": 105,
  "eidr_id": "10.5240/XXXX-XXXX-XXXX-XXXX-XXXX-C",
  "gracenote_id": "123456789012",
  "created_at": "2026-04-20T21:45:00.000Z",
  "updated_at": "2026-05-19T21:45:00.000Z"
}
```

### `ad_decisions.jsonl` (bônus) — uma linha por (impressão × slot)

```json
{
  "decision_id": "...",
  "marker_id": "e1d2c3b4-...",
  "event_id_scte": 1003,
  "channel": "live-brasileirao-final-2026",
  "user_id_anon": "u-9f8e7d6c5b4a",
  "slot_index": 0,
  "creative_id": "...",
  "creative_duration_s": 30,
  "advertiser_id": "adv-001",
  "advertiser_name": "Cervejaria Brahma",
  "price_cpm_brl": 78.42,
  "served_ts": "2026-05-20T22:14:30.000Z",
  "ssai_provider": "adserver-internal"
}
```

> `user_id_anon` em `ad_decisions` NÃO casa diretamente com `user_id` em `player_events` — é o comportamento típico de ad servers (que não veem o user_id da plataforma). Para reconciliar, o candidato precisa juntar via `marker_id` + janela temporal de sessões ativas. É proposital — testa raciocínio de join multi-source.

## Injeções deliberadas (NÃO remova!)

Estas anomalias são o ponto pedagógico do desafio. O candidato precisa lidar com elas explicitamente — são as razões pelas quais SLOs de freshness/exactly-once/schema evolution existem.

| # | Injeção | Default | O que testar |
|---|---|---|---|
| (i)   | **Out-of-order** em até 15 s | 100% — sempre embaralhado em frames | watermarks, allowed lateness, side outputs |
| (ii)  | **Duplicatas** (mesmo `event_id`) | ~1% | dedupe idempotente, exactly-once |
| (iii) | **Schema v2** com `network_type` | ~0.5% | compat BACKWARD, default values, registry |
| (iv)  | **Burst em cdn-b** [min 60-75] | error rate ~50x, buffer rate ~9x | detecção de anomalia, alerta multi-burn-rate |
| (v)   | **Clock skew** por device | ±5 s consistente por device | event-time vs ingestion-time, watermarks |

Validação rápida das injeções:

```bash
python <<'EOF'
import json
from collections import Counter
events = [json.loads(l) for l in open("data/raw/player_events.jsonl")]
ids = Counter(e["event_id"] for e in events)
print(f"events: {len(events):,}")
print(f"dupes:  {sum(1 for c in ids.values() if c > 1):,}")
print(f"v2:     {sum(1 for e in events if 'network_type' in e):,}")
EOF
```

## Parametrização

Edite `config/config.yaml` ou use flags de CLI (CLI tem precedência):

```bash
python generate_all.py --n-sessions 10000 --seed 7 --output-dir /tmp/maracana
python generate_all.py --only player --n-sessions 500
python generate_all.py --skip ads        # pula o dataset bônus
python generate_all.py --config config/alt.yaml # config alternativo
```

Parâmetros principais (todos em `config/config.yaml`):

| Campo | Default | Efeito |
|---|---|---|
| `n_sessions`                   | 2000  | Volume principal. Cada sessão gera ~700 eventos médios. |
| `event_duration_minutes`       | 105   | Janela total (15min pre + 90min jogo). |
| `seed`                         | 42    | Determinismo total — mesma seed → arquivos byte-idênticos. |
| `cdn_b_degradation_window_min` | [60, 75] | Janela do burst de erros em cdn-b. |
| `duplicate_rate`               | 0.01  | % de duplicatas injetadas. |
| `schema_v2_rate`               | 0.005 | % de mensagens em schema v2. |
| `device_clock_skew_seconds`    | 5     | Skew máximo ±, por device. |

## Volumetria & performance

Medições em laptop padrão (single-process Python 3.12). Cada sessão emite ~260 eventos em média (heartbeats a cada 10s ao longo de ~45min de duração média, mais eventos discretos).

| `n_sessions` | Eventos | Tempo | Tamanho JSONL |
|---:|---:|---:|---:|
|     200 |   ~55 k  | ~2 s   | ~27 MB |
|   2 000 |  ~525 k  | ~16 s  | ~260 MB |
|  10 000 |  ~2.6 M  | ~80 s  | ~1.3 GB |
|  50 000 | ~13 M    | ~7 min | ~6.5 GB |

Para o take-home, **2000 é suficiente**. Volumes maiores são para o candidato que quiser estressar o pipeline batch ou medir throughput de streaming.

## Troubleshooting

**`ModuleNotFoundError: yaml`** — instale PyYAML (`pip install pyyaml`) ou rode sem config (defaults via CLI).

**Parquet não gerado** — instale pyarrow (`pip install pyarrow`). JSONL é o caminho principal; Parquet é só conveniência.

**Quero mais conteúdo no catálogo** — edite `content_metadata.generate(cfg, n_aux=200)` ou exponha como flag.

**Quero múltiplos eventos (não só 1 jogo)** — rode múltiplas vezes com seeds e `output_dir` diferentes, depois concatene. Boa extensão para o candidato Staff demonstrar multi-tenancy.

## Próximos passos (para o candidato)

1. Defina contratos para os 3 streams (Avro/Proto/JSON Schema) com compatibilidade reversa.
2. Suba um broker local (Kafka/Redpanda via Docker, ou simulação em memória).
3. Crie um producer que transforma os arquivos JSONL em tópicos.
4. Implemente uma camada Bronze imutável, Silver streaming, Gold batch.
5. Adicione observabilidade de dados (Soda/GE/dbt tests) cobrindo as 4 dimensões clássicas.
6. Documente decisões em um arquivo `docs/DESIGN.md` (trade-offs, ADRs).