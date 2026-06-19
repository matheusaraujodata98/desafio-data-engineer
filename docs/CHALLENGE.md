# Desafio Técnico — Data Engineer
## Operação Maracanã — Telemetria de Audiência em Tempo Real

Bem-vindo, e obrigado por dedicar tempo a este processo. Este documento descreve o desafio que pedimos para você resolver antes do nosso próximo passo, que será uma conversa técnica com parte do time. Leia com calma — vale mais entender o espírito da coisa do que sair codando.

---

## TL;DR (1 minuto de leitura)

- **O que é:** um slice vertical de uma plataforma híbrida (batch + streaming) para telemetria de audiência de uma transmissão ao vivo, com elementos de plataforma de dados (contratos, qualidade, observabilidade).
- **Quanto tempo gastar:** entre **6 e 10 horas de trabalho efetivo**. Se passar muito disso, pare e mande o que tem — é melhor entrega menor e bem feita do que grande e atropelada.
- **Stack:** você escolhe. O que importa são as decisões, não a marca.
- **Como te avaliamos:** código, arquitetura, visão de plataforma, observabilidade, comunicação no `DESIGN.md` e maturidade no domínio de mídia.
- **Depois:** você terá uma sessão de live coding de ~90 min com 2 pessoas do time. Será conversa em cima do que você já entregou + uma pequena extensão em par.
- **Prazo:** **7 dias corridos** a partir do recebimento. Se precisar de mais tempo, fale com a gente.

---

## O cenário

Você acaba de entrar no **Centro de Excelência em Observabilidade & Plataforma de Dados** de uma grande media company brasileira. O CoE atende ~20 squads internas: jornalismo, esportes, novelas, BBB, ad tech, billing, infraestrutura de playout.

A próxima grande transmissão é a **final do Brasileirão 2026**, um *tent-pole event* com pico estimado de **8 milhões de espectadores simultâneos** entre OTT, CTV, mobile e web. Durante o segundo tempo está marcada uma janela publicitária crítica — três quebras com inserção dinâmica de anúncios via SSAI, sinalizadas por marcações SCTE-35.

Três áreas dependem dos seus dados:

- **A diretoria comercial** precisa de números **near real-time** (latência alvo p95 < 30 s) para vender pacotes de patrocínio cross-channel durante o próprio jogo.
- **A diretoria de produto** precisa de QoE por device, CDN e região para decidir, *durante o jogo*, se aciona um plano de mitigação (failover de CDN, redução de bitrate ladder).
- **O financeiro** precisa, no dia seguinte, de uma reconciliação **batch** auditável — idempotente, com lineage e versionamento — que servirá de input para o faturamento de patrocínio. É a "verdade contábil" e cada centavo conta.

**Sua missão:** projetar e implementar um slice vertical dessa plataforma que atenda os três casos.

---

## Sua missão, em detalhe

Você vai construir um pipeline end-to-end que cobre seis camadas. Todas são obrigatórias, mas o nível de profundidade que você dá a cada uma é escolha sua — comunique a priorização em um arquivo chamado `DESIGN.md`.

**F1. Ingestão e contratos (Bronze).**
Defina o contrato de dados dos três streams principais (Avro, Protobuf ou JSON Schema — você escolhe) com regras de compatibilidade explícitas. Implemente um producer que lê o gerador e publica num broker (Kafka, Redpanda, ou simulação em memória se for inviável). Persista uma camada Bronze imutável em Parquet, Iceberg ou Delta local.

**F2. Streaming (Silver, near real-time).**
Desenvolva uma pipeline que sessioniza `player_events` por `session_id`, calcula métricas de QoE (rebuffering ratio, video startup time, exit-before-video-start, average bitrate, error rate) e métricas de audiência (CCV por região/device/CDN) em janelas de 1 min. Faça a junção temporal com os `scte35_markers` para enriquecer cada sessão com o ad break corrente. Garanta idempotência e documente a estratégia de deduplicação.

**F3. Batch (Gold, fonte da verdade).**
Recompute as mesmas métricas a partir da Bronze com cobertura completa do dia, aplicando regras mais rigorosas (sessões fechadas, dedupe global, junção com `content_metadata` e `ad_decisions`). Produza tabelas Gold particionadas. Discuta no `DESIGN.md` por que existem as duas camadas, quando elas vão divergir, e como reconciliar.

**F4. Servimento e dashboard mínimo.**
Um endpoint (FastAPI, Streamlit, Grafana sobre ClickHouse, ou equivalente) mostrando: CCV por região, rebuffering ratio rolling de 5 min por CDN, um alerta visual quando a `cdn-b` degradar, e uma tabela de impacto comercial por marker SCTE-35.

**F5. Observabilidade de dados.**
Pelo menos **6 data quality checks** cobrindo as quatro dimensões clássicas — *freshness*, *volume*, *schema*, *distribution*. Ao menos um deve ser runtime no streaming, não só pós-batch. Documente SLOs de dados em YAML e ao menos um runbook de incidente.

**F6. Schema evolution end-to-end.**
Demonstre como um campo novo (sugestão: `network_type`) evolui do contrato → streaming → Gold → dashboard, sem quebrar consumidores antigos. Documente a estratégia (registry, compat modes, default values).

---

## Requisitos não-funcionais

Estes não são opcionais, mas o nível de elaboração é seu:

- **Reprodutível.** Um `docker compose up` ou `make demo` deve subir tudo sem muitas configurações extras. Considere uso de um arquivo `env.example` para configurações de variáveis.
- **Idempotente.** Reprocessar a Bronze deve produzir Gold bit-a-bit idêntica (a menos de timestamps de execução).
- **Garantias explícitas.** Discuta no `DESIGN.md` onde você aplica exactly-once e onde at-least-once é suficiente — e por quê.
- **Late-arriving data.** Estratégia documentada para watermarks, allowed lateness e re-emissão.
- **Custo.** Estimativa qualitativa do gargalo dominante se o pipeline rodasse 24/7.
- **LGPD.** O `user_id` chega anonimizado, mas mostre onde fica o ponto de pseudonimização e onde a chave viveria em produção.
- **Recuperação.** Como o pipeline se recupera de uma queda do broker por 30 min.

---

## Datasets fornecidos

Você receberá um gerador sintético em Python (`generators/`) que produz quatro datasets em JSON Lines. Ele é determinístico (mesma seed → mesmos arquivos).

```bash
python generate_all.py                       # default: 2000 sessões, ~16s
python generate_all.py --n-sessions 500      # rápido, para iterar
python generate_all.py --only scte35 content # só metadados
```

| Arquivo | O que é | Volume (default) |
|---|---|---|
| `player_events.jsonl`    | Stream A — telemetria de player (CMCD-like) | ~525k eventos |
| `scte35_markers.jsonl`   | Stream B — cue tones de quebra comercial | ~10 marcadores |
| `content_metadata.jsonl` | Dataset C — catálogo SCD | ~51 conteúdos |
| `ad_decisions.jsonl`     | Dataset D — impressões de ad (uso opcional) | ~16k decisões |

Os schemas completos estão no `README.md` do gerador. Leia.

**Importante — anomalias propositais.** O gerador injeta cinco condições adversas que você precisa lidar explicitamente:

1. **Eventos fora de ordem** em até 15 s
2. **Duplicatas** (~1% das mensagens — emula at-least-once do broker)
3. **Schema v2** em ~0.5% dos eventos (campo extra `network_type`)
4. **Burst de degradação na cdn-b** entre os minutos 60 e 75 (errors ~50x, buffers ~9x)
5. **Clock skew de ±5 s** consistente por device

Não tente "limpar" essas anomalias do gerador — elas são o ponto pedagógico. Sua pipeline precisa conviver com elas e o `DESIGN.md` deve explicar como.

---

## O que entregar

Um repositório Git (link público ou compartilhado conosco) contendo:

1. **`README.md`** — quickstart, decisões de stack, como executar, limitações conhecidas.
2. **`DESIGN.md`** — design doc de 4 a 8 páginas equivalentes (markdown serve), com:
   - Um diagrama de arquitetura (Excalidraw, Mermaid, draw.io — o que você preferir).
   - Seção de **trade-offs** ("considerei X, escolhi Y porque…").
   - Uma seção **"se eu tivesse mais 2 semanas"** com o que você priorizaria.
3. **Código** organizado, com testes (mínimo: testes de transformação na Silver, testes de contrato, e ao menos 1 teste end-to-end "smoke").
4. **`CONTRACTS/`** — schemas versionados (`.avsc`, `.proto` ou `.json`) com compat policy documentada.
5. **`OBSERVABILITY/`** — definições dos data quality checks (Soda, Great Expectations, dbt tests ou código próprio), SLOs em YAML, screenshot do dashboard se faltar tempo.
6. **`RUNBOOK.md`** — ao menos 2 incidentes hipotéticos com runbook ("broker down 30 min", "schema drift detectado em produção").

---

## Como avaliamos

A avaliação é em seis dimensões. Não esperamos "excelente" em todas — esperamos consciência sobre onde você priorizou e por quê.

- **Código e engenharia.** Legibilidade, testes, idempotência, error handling, estrutura modular, logs estruturados.
- **Arquitetura e trade-offs.** Diagrama claro, escolhas justificadas, consciência de batch vs streaming, exactly-once, late data, watermarks.
- **Visão de plataforma.** Contratos versionados, schema evolution, lineage (mesmo manual), self-service mínimo, governança. Pensamos em como esse pipeline vira um template reutilizável para outras squads.
- **Observabilidade de dados.** Cobertura das 4 dimensões (freshness, volume, schema, distribution), SLOs documentados, runbook para pelo menos 1 incidente.
- **Comunicação.** Clareza do `README` e do `DESIGN.md`, qualidade dos diagramas, capacidade de explicar o porquê das escolhas.
- **Sabor de domínio.** Entendimento de QoE, ad insertion, SCTE-35, live vs VOD. Você não precisa ter trabalhado em mídia antes — precisa ter curiosidade suficiente para ler sobre.

IMPORTANTE!!!! Prestamos atenção extra a sinais de: escopo proposto explicitamente, o que você decidiu *não* fazer, articulação de custo organizacional, ADRs no design doc, e propostas que vão além de "este pipeline funcional" rumo a "este pipeline como produto de plataforma".

---

## Stack e ferramentas

**Stack-agnóstico.** Aceitamos qualquer combinação razoável: Kafka/Redpanda/RabbitMQ para broker; Spark/Flink/Bytewax/Faust/RisingWave para streaming; Iceberg/Delta/Parquet puro no lake; Postgres/ClickHouse/Druid/DuckDB para serving. Forçar uma stack específica enviesaria o filtro para experiência prévia em vez de raciocínio. O **porquê** importa mais que o **o quê**.

**Uso de IA assistente.** Assumimos que você vai usar Copilot, Cursor, Claude, ChatGPT, o que for. Não tem problema, e não tira pontos. **Mencione no `README` onde e como você usou** — isso só ajuda a calibrar a conversa no live coding. O que avaliamos é a sua capacidade de julgar, decidir e defender o que ficou no repositório, não a velocidade de digitação.

---

## A sessão de live coding (~90 min)

Para tirar a ansiedade: a sessão **não é "completar o que faltou"** nem reescrever do zero. É uma conversa estruturada em cima do que você já entregou, com uma extensão pequena em par. Formato aproximado:

- **10 min** — você apresenta a arquitetura em 5 min, fazemos perguntas.
- **20 min** — deep-dive num trade-off que você fez. Queremos entender seu raciocínio sob perguntas.
- **30 min** — extensão ao vivo em par. Será uma de quatro coisas, escolhida com base no seu take-home: adicionar um check de qualidade em runtime; implementar reprocessamento de uma janela sem duplicar Gold; propagar um campo novo do contrato até o dashboard; ou diagnosticar um incidente sintético.
- **20 min** — whiteboard / system design. Como você escalaria isso de 8 M CCV para 80 M, ou de 1 transmissão por semana para 50 simultâneas.
- **10 min** — suas perguntas para nós.

Não é teste de memória nem de algoritmo de LeetCode. É exatamente o que a vaga vai exigir no dia a dia.

---

## Bonus (opcionais — só se sobrar tempo e energia)

Estes não são esperados. Não fazer não tira pontos. Fazer bem pode enriquecer a conversa do live:

- Lineage automatizado via OpenLineage/Marquez.
- Backfill seguro de Gold com isolamento de compute.
- Streaming SQL declarativo (Flink SQL, RisingWave, ksqlDB, Materialize) para uma das transformações.
- Catálogo mínimo (DataHub, OpenMetadata, Backstage) com 5 assets registrados.
- CI completo com checks de schema compat bloqueando PRs incompatíveis.
- Detecção de anomalia ML-driven (z-score, Prophet) em uma das métricas.
- Estimativa de custo detalhada para um cenário 24/7 com 50k eventos/s.
- Demonstração de exactly-once end-to-end com Kafka transactions + sink transacional.

---

## Perguntas frequentes

**"Posso usar uma linguagem que não seja Python?"** Sim. A stack principal do time é Python e é necessário que você saiba fazer em python. Mas vai de Go, Rust, o que você dominar além de python e explique o motivo da portabilidade. Só lembre que o gerador é Python e é preciso usá-lo.

**"Tenho que rodar isso em cloud?"** Não. Tudo deve rodar localmente. Se você quiser demonstrar escolhas de cloud no `DESIGN.md`, ótimo — mas a entrega tem que subir em um linux/macos comum.

**"Não consegui terminar tudo no prazo, o que faço?"** Mande o que tem. Sério. O `DESIGN.md` com a seção "se eu tivesse mais 2 semanas" é onde você conta o resto. Atropelar a entrega é pior que entregar parcial e bem comunicado.

**"Posso fazer perguntas durante o desafio?"** Pode. Mande para o canal que combinamos no início do processo. Em geral respondemos em até 1 dia útil. Se a dúvida for sobre escopo, prefira tomar uma decisão e justificar no `DESIGN.md` em vez de esperar nossa resposta.

**"Vocês vão olhar meu commit history?"** Provavelmente sim, mas sem julgamento sobre "como" você trabalha — só para entender o fluxo do seu pensamento. Não force commits artificiais, mas os faça.

**"E se eu já trabalhei com esse tipo de pipeline antes?"** Melhor. Use o `DESIGN.md` para contar onde você está reaproveitando ideias e por que elas se aplicam (ou não) ao nosso cenário.

---

## Como entregar

Quando estiver pronto, mande para o ponto de contato do processo:

- O link do repositório (público ou compartilhado com o usuário que combinarmos).
- Uma estimativa de quantas horas você gastou — é só para nos ajudar a calibrar o desafio para os próximos candidatos. Não influencia avaliação.

A partir do recebimento, agendamos a sessão de live coding em até 5 dias úteis.

---

Boa sorte. Estamos torcendo por você.
