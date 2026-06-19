# 🚨 Operação Maracanã - RUNBOOK de Incidentes

Este documento descreve os procedimentos de resposta a incidentes para a infraestrutura de dados da transmissão, focando em manter a disponibilidade e integridade dos dados durante situações críticas.

---

## Incidente 1: Queda do Broker de Mensageria (Redpanda/Kafka)

**Descrição:** O broker de mensageria sofre indisponibilidade, parando a ingestão de telemetria da Camada Bronze.

**Sintomas:**
* Jobs PySpark de Streaming (Silver) com `0.0 records/sec`.
* Aumento exponencial no *Consumer Lag* dos grupos de consumo.

**Ações de Mitigação:**
1. **Verificar Infraestrutura:** Validar o status dos containers (`docker ps`). Se o broker estiver em *CrashLoopBackOff*, verificar logs de OOM (Out of Memory).
2. **Reinício de Nó:** Executar `docker restart <nome_container_broker>`.
3. **Verificação de Checkpoint:** O job de streaming (`jobs/silver/streaming.py`) foi desenhado com **Checkpointing**. Ao reiniciar o broker, o Spark retomará a leitura exatamente do *offset* onde parou, garantindo a continuidade sem perda de dados.

---

## Incidente 2: Degradação Massiva na Qualidade de Vídeo (CDN-B)

**Descrição:** O dashboard de Observabilidade detecta que a `error_rate` da CDN-B ultrapassou o SLO crítico de 5%.

> ℹ️ O dashboard roda tanto localmente (`streamlit run app/app.py`) quanto em produção, publicado em [desafio-data-engineer-globo.streamlit.app](https://desafio-data-engineer-globo.streamlit.app) (Streamlit Community Cloud, lendo a camada Gold via PostgreSQL gerenciado). A detecção do incidente pode partir de qualquer um dos dois ambientes; as ações de mitigação abaixo são as mesmas.

**Sintomas:**
* Banner vermelho de erro no Dashboard (Abas de Observabilidade), seja na versão local ou na versão em nuvem.
* Picos de latência e taxa de erro no gráfico de linhas do `app/app.py`.

**Ações de Mitigação:**
1. **Acionamento:** Notificar via Slack/PagerDuty a equipe de Engenharia de Vídeo/CDN.
2. **Traffic Shaping:** Operação manual ou automatizada para remover a `cdn-b` da rotação do manifesto HLS/DASH, redirecionando o tráfego para `cdn-a` e `cdn-c`.
3. **Análise de Dados:** O pipeline de dados deve ser mantido rodando para que os analistas possam auditar o faturamento afetado durante o período da degradação.

**Recuperação e Garantia de Dados:**
* A reconciliação financeira (Camada Gold) está protegida pela lógica de *Fan-Out* e deduplicação, assegurando que, mesmo com a degradação na rede, o faturamento dos anúncios seja calculado com base nas visualizações efetivas bem-sucedidas.

---

## Incidente 3: Indisponibilidade do Dashboard em Nuvem (Streamlit Cloud)

**Descrição:** O dashboard publicado em [desafio-data-engineer-globo.streamlit.app](https://desafio-data-engineer-globo.streamlit.app) fica inacessível ou apresenta erro de conexão com o banco, enquanto a versão local segue funcional.

**Sintomas:**
* Erro de conexão exibido na página do app (ex.: falha ao conectar em `POSTGRES_HOST`).
* App em estado "sleeping" ou não carregando no painel do Streamlit Cloud.

**Ações de Mitigação:**
1. **Verificar Secrets:** Confirmar no painel do Streamlit Cloud se `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER` e `POSTGRES_PASSWORD` ainda estão configurados e válidos — credenciais rotacionadas no banco gerenciado precisam ser atualizadas manualmente nos Secrets do app.
2. **Verificar o Banco Gerenciado:** Confirmar se a instância PostgreSQL está ativa e aceitando conexões externas (whitelisting de IP, se aplicável).
3. **Reiniciar o App:** No painel do Streamlit Cloud, usar a opção "Reboot app" para forçar uma nova inicialização.
4. **Fallback:** Enquanto o ambiente em nuvem está sendo restaurado, a versão local (`streamlit run app/app.py`, item 4 do "Como Executar") continua disponível como contingência, desde que apontada para um Postgres acessível.

---

## 🛠️ Manutenção Pós-Incidente
Após a resolução de qualquer incidente:
1. **Post-Mortem:** Registrar a causa raiz e o tempo de indisponibilidade (MTTR).
2. **Backfill:** Se necessário, executar reprocessamento (backfill) da camada Bronze para a Silver para preencher lacunas nos dados, utilizando o histórico de arquivos Parquet preservados.