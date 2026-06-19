/* =========================================================================================
   OPERAÇÃO MARACANÃ - QUERIES ANALÍTICAS DE VALIDAÇÃO (CAMADAS SILVER E GOLD)
   =========================================================================================
   Este arquivo contém as consultas SQL utilizadas para responder aos requisitos de negócio 
   e validar as injeções deliberadas (anomalias) descritas no README do projeto.
   ========================================================================================= */

/* -----------------------------------------------------------------------------------------
   1. RECONCILIAÇÃO FINANCEIRA E FATURAMENTO TOTAL (Camada Gold - Requisito F3)
   Objetivo: Demonstrar quanto dinheiro a transmissão gerou por anunciante e por programa.
   Resolve o problema de "Fan-Out" (duplicação de receita no Join) isolando a receita 
   máxima por anúncio antes de somar o faturamento total da marca.
   ----------------------------------------------------------------------------------------- */
WITH receita_por_anuncio AS (
    SELECT 
        advertiser_name,
        content_title,
        marker_id,
        MAX(total_revenue_brl) AS receita_real_do_anuncio,
        SUM(ccv) AS ccv_total_do_anuncio
    FROM gold_faturamento_ads
    WHERE advertiser_name IS NOT NULL
    GROUP BY advertiser_name, content_title, marker_id
)
SELECT 
    advertiser_name AS patrocinador,
    content_title AS programa,
    ROUND(SUM(receita_real_do_anuncio)::numeric, 2) AS faturamento_total_r$,
    SUM(ccv_total_do_anuncio) AS total_espectadores_impactados
FROM receita_por_anuncio
GROUP BY advertiser_name, content_title
ORDER BY faturamento_total_r$ DESC;


/* -----------------------------------------------------------------------------------------
   2. DETECÇÃO DE ANOMALIA E QUEDA DE QUALIDADE (Camada Silver - Requisito F2 / README)
   Objetivo: Validar a injeção deliberada (iv) do README ("Burst em cdn-b [min 60-75]").
   A query prova que a engenharia de QoE (Quality of Experience) detectou corretamente 
   o pico de ~50x na taxa de erros e ~9x no tempo de buffer na CDN B.
   ----------------------------------------------------------------------------------------- */
SELECT 
    window_start AS minuto_transmissao,
    cdn,
    ROUND(AVG(error_rate)::numeric, 4) AS taxa_erro_media,
    ROUND(AVG(avg_buffer_length_ms)::numeric, 2) AS buffer_medio_ms,
    SUM(ccv) AS usuarios_afetados
FROM silver_audiencia_v3
GROUP BY window_start, cdn
ORDER BY taxa_erro_media DESC
LIMIT 15;


/* -----------------------------------------------------------------------------------------
   3. PICO DE AUDIÊNCIA SIMULTÂNEA (Camada Silver - Requisito F2)
   Objetivo: Calcular o Max CCV (Concurrent Viewers) minuto a minuto.
   Demonstra a eficácia do algoritmo HyperLogLog (approx_count_distinct) e da idempotência
   no tratamento de dados duplicados e atrasados do Kafka.
   ----------------------------------------------------------------------------------------- */
SELECT 
    window_start AS minuto_transmissao,
    SUM(ccv) AS pico_audiencia_simultanea
FROM silver_audiencia_v3
GROUP BY window_start
ORDER BY pico_audiencia_simultanea DESC
LIMIT 5;


/* -----------------------------------------------------------------------------------------
   4. DESEMPENHO DE ENTREGA DOS ANÚNCIOS (Camada Gold - Inteligência de Negócio)
   Objetivo: Avaliar como a infraestrutura entregou os blocos comerciais em diferentes 
   regiões e dispositivos, cruzando faturamento com possíveis falhas de exibição.
   ----------------------------------------------------------------------------------------- */
SELECT 
    advertiser_name AS patrocinador,
    region AS regiao,
    device AS dispositivo,
    SUM(ccv) AS visualizacoes,
    ROUND(AVG(error_rate)::numeric, 4) AS taxa_falha_no_anuncio
FROM gold_faturamento_ads
WHERE advertiser_name IS NOT NULL
GROUP BY advertiser_name, region, device
ORDER BY visualizacoes DESC;