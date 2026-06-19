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

**Sintomas:**
* Banner vermelho de erro no Dashboard (Abas de Observabilidade).
* Picos de latência e taxa de erro no gráfico de linhas do `app/app.py`.

**Ações de Mitigação:**
1. **Acionamento:** Notificar via Slack/PagerDuty a equipe de Engenharia de Vídeo/CDN.
2. **Traffic Shaping:** Operação manual ou automatizada para remover a `cdn-b` da rotação do manifesto HLS/DASH, redirecionando o tráfego para `cdn-a` e `cdn-c`.
3. **Análise de Dados:** O pipeline de dados deve ser mantido rodando para que os analistas possam auditar o faturamento afetado durante o período da degradação.

**Recuperação e Garantia de Dados:**
* A reconciliação financeira (Camada Gold) está protegida pela lógica de *Fan-Out* e deduplicação, assegurando que, mesmo com a degradação na rede, o faturamento dos anúncios seja calculado com base nas visualizações efetivas bem-sucedidas.

---

## 🛠️ Manutenção Pós-Incidente
Após a resolução de qualquer incidente:
1. **Post-Mortem:** Registrar a causa raiz e o tempo de indisponibilidade (MTTR).
2. **Backfill:** Se necessário, executar reprocessamento (backfill) da camada Bronze para a Silver para preencher lacunas nos dados, utilizando o histórico de arquivos Parquet preservados.