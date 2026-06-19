from __future__ import annotations

import os
from collections import defaultdict
from decimal import Decimal
from typing import Any, Iterable

from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
import streamlit as st


APP_TITLE = "Operação Maracanã - Observabilidade"

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "user": os.getenv("POSTGRES_USER", "maracana_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "maracana_pass"),
    "dbname": os.getenv("POSTGRES_DB", "maracana_gold"),
    "port": int(os.getenv("POSTGRES_PORT", "15432")),
}

MAX_CCV_QUERY = """
SELECT
    window_start,
    SUM(ccv) AS max_ccv
FROM silver_audiencia_v3
GROUP BY window_start
ORDER BY max_ccv DESC
LIMIT 1
"""

RECONCILIATION_QUERY = """
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
    ROUND(SUM(receita_real_do_anuncio)::numeric, 2) AS faturamento_total_brl,
    SUM(ccv_total_do_anuncio) AS total_espectadores_impactados
FROM receita_por_anuncio
GROUP BY advertiser_name, content_title
ORDER BY faturamento_total_brl DESC
"""

CDN_QUERY = """
SELECT
    window_start,
    cdn,
    ROUND(AVG(error_rate)::numeric, 4) AS error_rate
FROM silver_audiencia_v3
GROUP BY window_start, cdn
ORDER BY window_start, cdn
"""


st.set_page_config(page_title=APP_TITLE, layout="wide")
@st.cache_resource(show_spinner=False)
def get_connection_pool() -> SimpleConnectionPool:
    """Create and cache the PostgreSQL connection pool."""

    return SimpleConnectionPool(minconn=1, maxconn=4, **DB_CONFIG)


@st.cache_data(show_spinner=False)
def fetch_rows(query: str) -> list[dict[str, Any]]:
    """Execute a SQL query and return rows as dictionaries.

    Args:
        query: SQL text to execute.

    Returns:
        A list of rows represented as dictionaries.

    Raises:
        RuntimeError: If the query cannot be executed.
    """

    pool = get_connection_pool()
    connection = None

    try:
        connection = pool.getconn()
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            return [dict(row) for row in cursor.fetchall()] if cursor.description else []
    except Exception as exc:
        if connection is not None and not connection.closed:
            connection.rollback()
        raise RuntimeError(f"Falha ao consultar o PostgreSQL local: {exc}") from exc
    finally:
        if connection is not None:
            pool.putconn(connection)


def format_currency_brl(value: Any) -> str:
    """Format a numeric value as Brazilian currency."""

    amount = Decimal(str(value or 0))
    formatted = f"{amount:,.2f}"
    return f"R$ {formatted.replace(',', 'X').replace('.', ',').replace('X', '.')}"


def format_integer(value: Any) -> str:
    """Format a numeric value with thousands separators."""

    amount = int(Decimal(str(value or 0)))
    return f"{amount:,}".replace(",", ".")


def to_decimal(value: Any) -> Decimal:
    """Convert any SQL value to Decimal safely."""

    return Decimal(str(value or 0))


def render_styles() -> None:
    """Inject custom styling for the executive dashboard."""

    st.markdown(
        """
        <style>
            .hero-shell {
                padding: 1.25rem 1.35rem;
                border-radius: 24px;
                background: linear-gradient(135deg, #08111f 0%, #0f1d33 50%, #0a2744 100%);
                border: 1px solid rgba(255, 255, 255, 0.08);
                box-shadow: 0 18px 45px rgba(0, 0, 0, 0.22);
                color: #f5f7fb;
                margin-bottom: 1rem;
            }

            .hero-shell h1 {
                margin: 0;
                font-size: 2.15rem;
                line-height: 1.05;
                letter-spacing: -0.03em;
            }

            .hero-shell p {
                margin: 0.6rem 0 0;
                color: rgba(245, 247, 251, 0.82);
                font-size: 0.98rem;
            }

            .kpi-card {
                background: linear-gradient(180deg, #101a2d 0%, #0b1321 100%);
                border: 1px solid rgba(103, 169, 255, 0.16);
                border-radius: 22px;
                padding: 1.2rem 1.25rem;
                box-shadow: 0 16px 40px rgba(4, 10, 18, 0.22);
                color: #f8fbff;
            }

            .kpi-label {
                font-size: 0.82rem;
                text-transform: uppercase;
                letter-spacing: 0.14em;
                color: #8fb8ff;
                margin-bottom: 0.45rem;
            }

            .kpi-value {
                font-size: 2.15rem;
                font-weight: 800;
                line-height: 1;
                color: #ffffff;
                margin-bottom: 0.45rem;
            }

            .kpi-subtitle {
                font-size: 0.92rem;
                color: rgba(248, 251, 255, 0.72);
            }

            .card-icon {
                font-size: 1.2rem;
                margin-right: 0.4rem;
            }

            .section-pill {
                display: inline-block;
                padding: 0.3rem 0.7rem;
                border-radius: 999px;
                background: rgba(103, 169, 255, 0.12);
                color: #b9d4ff;
                font-size: 0.76rem;
                letter-spacing: 0.12em;
                text-transform: uppercase;
                margin-bottom: 0.6rem;
            }

            .stTabs [data-baseweb="tab-list"] {
                gap: 0.35rem;
            }

            .stTabs [data-baseweb="tab"] {
                background: rgba(10, 18, 31, 0.72);
                border: 1px solid rgba(255, 255, 255, 0.08);
                border-radius: 14px 14px 0 0;
                padding: 0.7rem 1rem;
            }

            .stTabs [aria-selected="true"] {
                background: #102033;
                color: #ffffff;
            }

            .metric-grid {
                display: grid;
                grid-template-columns: repeat(2, minmax(0, 1fr));
                gap: 1rem;
                margin-top: 0.65rem;
                margin-bottom: 1.25rem;
            }

            @media (max-width: 860px) {
                .metric-grid {
                    grid-template-columns: 1fr;
                }

                .hero-shell h1 {
                    font-size: 1.75rem;
                }
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_sidebar() -> None:
    """Render the project sidebar with context and refresh action."""

    st.sidebar.markdown("## 🏟️ Operação Maracanã")
    st.sidebar.markdown(
        """
        **Dashboard de Camada Gold**

        Painel executivo da fase de observabilidade, lendo a base analítica local em PostgreSQL para acompanhar faturamento e QoE.
        """
    )
    st.sidebar.markdown(
        """
        **Arquitetura**

        - Camada Gold: reconciliação financeira e indicadores executivos.
        - Camada Silver: telemetria agregada de audiência e qualidade.
        - PostgreSQL local: fonte única do painel.
        """
    )

    if st.sidebar.button("Atualizar Dados", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.sidebar.caption(f"Fonte: {DB_CONFIG['host']}:{DB_CONFIG['port']} / {DB_CONFIG['dbname']}")
    st.sidebar.caption("Visual otimizado para leitura executiva e inspeção operacional.")


def aggregate_revenue_by_sponsor(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Aggregate reconciliation rows by sponsor for the executive bar chart."""

    aggregated: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    programs_by_sponsor: dict[str, set[str]] = defaultdict(set)

    for row in rows:
        sponsor = str(row.get("patrocinador") or "Sem patrocinador")
        aggregated[sponsor] += to_decimal(row.get("faturamento_total_brl"))
        program = str(row.get("programa") or "Sem programa")
        programs_by_sponsor[sponsor].add(program)

    chart_rows = []
    for sponsor, revenue in sorted(aggregated.items(), key=lambda item: item[1], reverse=True):
        chart_rows.append(
            {
                "patrocinador": sponsor,
                "faturamento_total_brl": float(revenue),
                "programas": len(programs_by_sponsor[sponsor]),
            }
        )
    return chart_rows


def build_revenue_figure(rows: list[dict[str, Any]]):
    """Create the polished executive revenue chart."""

    import plotly.graph_objects as go

    chart_rows = aggregate_revenue_by_sponsor(rows)
    sponsors = [row["patrocinador"] for row in chart_rows]
    revenues = [row["faturamento_total_brl"] for row in chart_rows]
    programs = [row["programas"] for row in chart_rows]

    figure = go.Figure(
        data=[
            go.Bar(
                x=sponsors,
                y=revenues,
                marker=dict(
                    color=revenues,
                    colorscale=[[0, "#0A2540"], [0.55, "#0F4C81"], [1, "#2DE2E6"]],
                    line=dict(color="#DCEBFF", width=0.6),
                ),
                hovertemplate=(
                    "<b>%{x}</b><br>"
                    "Faturamento: R$ %{y:,.2f}<br>"
                    "Programas: %{customdata}<extra></extra>"
                ),
                customdata=programs,
                width=0.55,
            )
        ]
    )

    figure.update_layout(
        template="plotly_white",
        height=470,
        margin=dict(l=10, r=10, t=15, b=15),
        xaxis=dict(title="Patrocinador", showgrid=False, tickfont=dict(color="#0d1b2a")),
        yaxis=dict(title="Faturamento Total (R$)", showgrid=False, zeroline=False),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        showlegend=False,
    )
    return figure


def build_cdn_figure(rows: list[dict[str, Any]]):
    """Create the QoE line chart with emphasis on the CDN-B anomaly."""

    import plotly.graph_objects as go

    series: dict[str, list[tuple[Any, float]]] = defaultdict(list)
    for row in rows:
        series[str(row.get("cdn") or "cdn-unknown")].append(
            (row.get("window_start"), float(to_decimal(row.get("error_rate"))))
        )

    figure = go.Figure()
    for cdn_name, points in sorted(series.items()):
        points = sorted(points, key=lambda item: item[0])
        x_values = [item[0] for item in points]
        y_values = [item[1] for item in points]
        is_cdn_b = cdn_name.lower() == "cdn-b"
        figure.add_trace(
            go.Scatter(
                x=x_values,
                y=y_values,
                mode="lines",
                name=cdn_name,
                line=dict(color="#FF4B4B" if is_cdn_b else "#D1D5DB", width=3 if is_cdn_b else 2),
                hovertemplate="<b>%{fullData.name}</b><br>%{x}<br>Error rate: %{y:.2%}<extra></extra>",
            )
        )

    figure.add_hline(
        y=0.05,
        line_dash="dash",
        line_color="#FF8A8A",
        annotation_text="Limite crítico de 5%",
        annotation_position="top left",
    )
    figure.update_layout(
        template="plotly_white",
        height=470,
        margin=dict(l=10, r=10, t=15, b=15),
        xaxis=dict(title="Janela de tempo", showgrid=False),
        yaxis=dict(title="Error rate", showgrid=False, tickformat=".0%", zeroline=False),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return figure


def render_kpi_card(icon: str, label: str, value: str, subtitle: str) -> None:
    """Render a custom executive KPI card."""

    st.markdown(
        f"""
        <div class="kpi-card">
            <div class="kpi-label"><span class="card-icon">{icon}</span>{label}</div>
            <div class="kpi-value">{value}</div>
            <div class="kpi-subtitle">{subtitle}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_executive_tab() -> None:
    """Render the financial executive tab."""

    st.markdown('<div class="section-pill">Visão executiva</div>', unsafe_allow_html=True)
    st.markdown(
        """
        <div class="hero-shell">
            <h1>Operação Maracanã</h1>
            <p>Monitoração executiva da camada Gold com foco em faturamento, reconciliação e eficiência comercial.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    max_ccv_rows = fetch_rows(MAX_CCV_QUERY)
    reconciliation_rows = fetch_rows(RECONCILIATION_QUERY)
    total_revenue = sum(to_decimal(row.get("faturamento_total_brl")) for row in reconciliation_rows)
    max_ccv = max_ccv_rows[0].get("max_ccv") if max_ccv_rows else 0

    st.markdown(
        "<div class='metric-grid'>"
        "<div id='kpi-1'></div><div id='kpi-2'></div>"
        "</div>",
        unsafe_allow_html=True,
    )

    col1, col2 = st.columns(2)
    with col1:
        render_kpi_card(
            "👥",
            "Pico de Audiência",
            format_integer(max_ccv),
            "Métrica máxima agregada por janela de transmissão",
        )
    with col2:
        render_kpi_card(
            "💰",
            "Faturamento Total",
            format_currency_brl(total_revenue),
            "Receita consolidada da reconciliação sem fan-out",
        )

    st.markdown("### Reconciliação Financeira")
    st.caption("Faturamento consolidado por patrocinador com visual executivo limpo.")
    st.plotly_chart(build_revenue_figure(reconciliation_rows), width="stretch")


def render_observability_tab() -> None:
    """Render the observability and QoE tab."""

    st.markdown('<div class="section-pill">Observabilidade & QoE</div>', unsafe_allow_html=True)

    cdn_rows = fetch_rows(CDN_QUERY)
    cdn_b_peak = max(
        (to_decimal(row.get("error_rate")) for row in cdn_rows if str(row.get("cdn", "")).lower() == "cdn-b"),
        default=Decimal("0"),
    )

    if cdn_b_peak > Decimal("0.05"):
        st.error(
            f"Incidente de degradação detectado: a CDN-B atingiu pico de {cdn_b_peak:.2%}, acima do limite crítico de 5%."
        )
    else:
        st.warning(
            f"CDN-B sob controle neste recorte. Pico observado: {cdn_b_peak:.2%} com limite operacional de 5%."
        )

    st.markdown("### Taxa de Erro por CDN")
    st.caption("Série temporal agregada por janela para visualizar o comportamento da infraestrutura de entrega.")
    st.plotly_chart(build_cdn_figure(cdn_rows), width="stretch")


def main() -> None:
    """Application entry point."""

    render_styles()
    render_sidebar()

    st.title(APP_TITLE)
    st.caption("Dashboard interativo de produção para leitura executiva e observabilidade operacional.")

    try:
        get_connection_pool()
    except Exception as exc:
        st.error(f"Não foi possível criar a conexão com o PostgreSQL local: {exc}")
        st.stop()

    tab_executive, tab_observability = st.tabs(["📊 Visão Executiva & Faturamento", "🛠️ Observabilidade & QoE"])

    with tab_executive:
        render_executive_tab()

    with tab_observability:
        render_observability_tab()


if __name__ == "__main__":
    main()