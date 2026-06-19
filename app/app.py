from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
from typing import Any, Iterable
import time

from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
import streamlit as st


APP_TITLE = "Operação Maracanã - Observabilidade"

# --- CONFIGURAÇÃO NUVEM (Lendo dos Segredos do Streamlit Cloud) ---
DB_CONFIG = {
    "host": st.secrets["DB_HOST"],
    "port": 5432,
    "dbname": st.secrets["DB_NAME"],
    "user": st.secrets["DB_USER"],
    "password": st.secrets["DB_PASSWORD"]
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


st.set_page_config(page_title="Maracanã Executivo", page_icon="⚽", layout="wide", initial_sidebar_state="expanded")

@st.cache_resource(show_spinner=False)
def get_connection_pool() -> SimpleConnectionPool:
    """Create and cache the PostgreSQL connection pool."""
    return SimpleConnectionPool(minconn=1, maxconn=4, **DB_CONFIG)


@st.cache_data(show_spinner=False)
def fetch_rows(query: str) -> list[dict[str, Any]]:
    """Execute a SQL query and return rows as dictionaries."""
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
        raise RuntimeError(f"Falha ao consultar o banco de dados na Nuvem: {exc}") from exc
    finally:
        if connection is not None:
            pool.putconn(connection)


def format_currency_brl(value: Any) -> str:
    amount = Decimal(str(value or 0))
    formatted = f"{amount:,.2f}"
    return f"R$ {formatted.replace(',', 'X').replace('.', ',').replace('X', '.')}"


def format_integer(value: Any) -> str:
    amount = int(Decimal(str(value or 0)))
    return f"{amount:,}".replace(",", ".")


def to_decimal(value: Any) -> Decimal:
    return Decimal(str(value or 0))


def render_styles() -> None:
    """Inject custom styling for the executive dashboard, hiding default Streamlit UI."""
    st.markdown(
        """
        <style>
            /* Esconde as marcas do Streamlit para um visual "White-Label" */
            #MainMenu {visibility: hidden;}
            header {visibility: hidden;}
            footer {visibility: hidden;}
            
            /* Ajusta o padding superior que o header oculto deixa vazio */
            .block-container {
                padding-top: 2rem;
                padding-bottom: 0rem;
            }

            .hero-shell {
                padding: 1.5rem 2rem;
                border-radius: 16px;
                background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%);
                border: 1px solid rgba(255, 255, 255, 0.05);
                box-shadow: 0 10px 30px rgba(0, 0, 0, 0.3);
                color: #f8fafc;
                margin-bottom: 1.5rem;
            }

            .hero-shell h1 {
                margin: 0;
                font-size: 2.25rem;
                font-weight: 700;
                letter-spacing: -0.02em;
                background: -webkit-linear-gradient(#fff, #cbd5e1);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
            }

            .hero-shell p {
                margin: 0.5rem 0 0;
                color: #94a3b8;
                font-size: 1rem;
            }

            .kpi-card {
                background: #1e293b;
                border: 1px solid rgba(255, 255, 255, 0.05);
                border-radius: 12px;
                padding: 1.5rem;
                box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
                transition: transform 0.2s ease, box-shadow 0.2s ease;
            }
            
            .kpi-card:hover {
                transform: translateY(-2px);
                box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.2);
                border: 1px solid rgba(56, 189, 248, 0.3);
            }

            .kpi-label {
                font-size: 0.875rem;
                font-weight: 600;
                text-transform: uppercase;
                letter-spacing: 0.05em;
                color: #94a3b8;
                margin-bottom: 0.5rem;
                display: flex;
                align-items: center;
            }

            .kpi-value {
                font-size: 2.5rem;
                font-weight: 700;
                color: #f8fafc;
                line-height: 1.2;
                margin-bottom: 0.25rem;
            }

            .kpi-trend {
                font-size: 0.875rem;
                font-weight: 500;
                color: #34d399; /* Verde sucesso */
                background: rgba(52, 211, 153, 0.1);
                padding: 2px 8px;
                border-radius: 12px;
                display: inline-block;
                margin-bottom: 0.5rem;
            }

            .kpi-subtitle {
                font-size: 0.875rem;
                color: #64748b;
                margin-top: 0.5rem;
                border-top: 1px solid rgba(255,255,255,0.05);
                padding-top: 0.5rem;
            }

            .card-icon {
                font-size: 1.25rem;
                margin-right: 0.5rem;
            }

            .stTabs [data-baseweb="tab-list"] {
                gap: 1rem;
                border-bottom: 1px solid #334155;
            }

            .stTabs [data-baseweb="tab"] {
                padding: 1rem 1.5rem;
                background: transparent;
                border: none;
                color: #94a3b8;
            }

            .stTabs [aria-selected="true"] {
                color: #38bdf8;
                border-bottom: 2px solid #38bdf8;
                background: rgba(56, 189, 248, 0.05);
            }

            /* Estiliza o botão da sidebar */
            .stButton>button {
                border-radius: 8px;
                font-weight: 600;
                border: 1px solid #38bdf8;
                color: #38bdf8;
                transition: all 0.3s;
            }
            .stButton>button:hover {
                background: #38bdf8;
                color: #0f172a;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_sidebar() -> None:
    st.sidebar.markdown("## 🏟️ Centro de Controle")
    st.sidebar.markdown(
        """
        <div style="color: #94a3b8; font-size: 0.9rem; margin-bottom: 2rem;">
        Monitoramento executivo em tempo real. Camadas Silver e Gold materializadas no Neon.
        </div>
        """, unsafe_allow_html=True
    )

    if st.sidebar.button("🔄 Sincronizar Dados", use_container_width=True):
        st.cache_data.clear()
        st.toast('Conectando ao banco de dados...', icon='⏳')
        time.sleep(0.5) # Pequeno delay para efeito visual de carregamento
        st.toast('Painel atualizado com sucesso!', icon='✅')

    st.sidebar.markdown("---")
    st.sidebar.caption("🟢 **Status:** Operacional")
    st.sidebar.caption(f"🔌 **Host:** `{DB_CONFIG['host'].split('.')[0]}`")
    st.sidebar.caption("⏱️ **Latência tolerada:** 15s")


def aggregate_revenue_by_sponsor(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
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
                    colorscale=[[0, "#1e293b"], [0.5, "#3b82f6"], [1, "#38bdf8"]],
                    line=dict(color="#0f172a", width=1),
                ),
                hovertemplate=(
                    "<b style='font-size: 14px;'>%{x}</b><br><br>"
                    "Faturamento: <b>R$ %{y:,.2f}</b><br>"
                    "Ações no Conteúdo: %{customdata}<extra></extra>"
                ),
                customdata=programs,
                width=0.4,
            )
        ]
    )

    figure.update_layout(
        template="plotly_dark",
        height=400,
        margin=dict(l=0, r=0, t=30, b=0),
        xaxis=dict(title="", showgrid=False, tickfont=dict(size=12, color="#cbd5e1")),
        yaxis=dict(title="", showgrid=True, gridcolor="rgba(255,255,255,0.05)", zeroline=False, tickprefix="R$ "),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        showlegend=False,
        hoverlabel=dict(bgcolor="#0f172a", font_size=13, font_family="sans-serif")
    )
    return figure


def build_cdn_figure(rows: list[dict[str, Any]]):
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
                mode="lines+markers" if is_cdn_b else "lines",
                name=cdn_name.upper(),
                line=dict(color="#ef4444" if is_cdn_b else "#475569", width=3 if is_cdn_b else 1.5),
                marker=dict(size=6, color="#ef4444") if is_cdn_b else None,
                hovertemplate="<b>%{fullData.name}</b><br>Tempo: %{x}<br>Taxa de Erro: <b>%{y:.2%}</b><extra></extra>",
            )
        )

    figure.add_hline(
        y=0.05,
        line_dash="dot",
        line_color="#f87171",
        annotation_text="Limite Crítico de SLO (5%)",
        annotation_position="top left",
        annotation_font_color="#f87171"
    )
    
    figure.update_layout(
        template="plotly_dark",
        height=400,
        margin=dict(l=0, r=0, t=10, b=0),
        xaxis=dict(title="", showgrid=False, tickfont=dict(color="#94a3b8")),
        yaxis=dict(title="", showgrid=True, gridcolor="rgba(255,255,255,0.05)", tickformat=".1%", zeroline=False),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="left", x=0, font=dict(color="#cbd5e1")),
        hoverlabel=dict(bgcolor="#0f172a", font_size=13)
    )
    return figure


def render_kpi_card(icon: str, label: str, value: str, subtitle: str, trend: str = "") -> None:
    trend_html = f'<div class="kpi-trend">↑ {trend}</div>' if trend else ""
    st.markdown(
        f"""
        <div class="kpi-card">
            <div class="kpi-label"><span class="card-icon">{icon}</span>{label}</div>
            <div class="kpi-value">{value}</div>
            {trend_html}
            <div class="kpi-subtitle">{subtitle}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_executive_tab() -> None:
    st.markdown(
        """
        <div class="hero-shell">
            <h1>Operação Maracanã</h1>
            <p>Consolidação financeira Gold e auditoria de engajamento do evento ao vivo.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    max_ccv_rows = fetch_rows(MAX_CCV_QUERY)
    reconciliation_rows = fetch_rows(RECONCILIATION_QUERY)
    total_revenue = sum(to_decimal(row.get("faturamento_total_brl")) for row in reconciliation_rows)
    max_ccv = max_ccv_rows[0].get("max_ccv") if max_ccv_rows else 0

    col1, col2 = st.columns(2)
    with col1:
        render_kpi_card(
            "👥",
            "Pico de Audiência Simultânea",
            format_integer(max_ccv),
            "Espectadores consolidados na Camada Silver sem duplicatas.",
            trend="Estável"
        )
    with col2:
        render_kpi_card(
            "💰",
            "Faturamento Reconciliado",
            format_currency_brl(total_revenue),
            "Receita limpa auditada na Camada Gold (livre de fan-out).",
            trend="Rendimento Nominal"
        )

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("### 📊 Faturamento por Patrocinador")
    st.markdown("<p style='color: #94a3b8; font-size: 0.9rem;'>Distribuição do impacto financeiro das cotas de publicidade.</p>", unsafe_allow_html=True)
    
    # Ocultar o ModeBar (menu flutuante do plotly)
    st.plotly_chart(build_revenue_figure(reconciliation_rows), use_container_width=True, config={'displayModeBar': False})


def render_observability_tab() -> None:
    st.markdown(
        """
        <div class="hero-shell" style="background: linear-gradient(135deg, #1e1b4b 0%, #312e81 100%);">
            <h1>Observabilidade e SLOs</h1>
            <p>Monitoria em tempo real da qualidade de entrega (QoE) por provedor de borda.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    cdn_rows = fetch_rows(CDN_QUERY)
    cdn_b_peak = max(
        (to_decimal(row.get("error_rate")) for row in cdn_rows if str(row.get("cdn", "")).lower() == "cdn-b"),
        default=Decimal("0"),
    )

    if cdn_b_peak > Decimal("0.05"):
        st.error(
            f"⚠️ **Incidente Crítico:** A CDN-B violou o limite de SLO com taxa de falha atingindo **{cdn_b_peak:.2%}**."
        )
    else:
        st.success(
            f"✅ **Tráfego Estável:** Operação de todas as CDNs dentro do limite tolerável de 5%."
        )

    st.markdown("### 📈 Taxa de Erro por CDN (Degradação)")
    st.markdown("<p style='color: #94a3b8; font-size: 0.9rem;'>Série temporal da qualidade de streaming agregada em janelas de 15 segundos.</p>", unsafe_allow_html=True)
    
    st.plotly_chart(build_cdn_figure(cdn_rows), use_container_width=True, config={'displayModeBar': False})


def main() -> None:
    render_styles()
    render_sidebar()

    try:
        get_connection_pool()
    except Exception as exc:
        st.error(f"Não foi possível criar a conexão com o banco de dados corporativo: {exc}")
        st.stop()

    tab_executive, tab_observability = st.tabs(["💎 Visão de Negócios (Gold)", "📡 Telemetria e Infra (Silver)"])

    with tab_executive:
        render_executive_tab()

    with tab_observability:
        render_observability_tab()


if __name__ == "__main__":
    main()