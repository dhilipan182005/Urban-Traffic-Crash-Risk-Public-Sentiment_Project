"""
Urban Traffic Crash Risk & Public Sentiment — Pipeline Dashboard
"""

import os, glob, re
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

#Page config
st.set_page_config(
    page_title="Urban Traffic Dashboard",
    page_icon="🚦",
    layout="wide",
    initial_sidebar_state="collapsed",
)

#Paths
BASE         = os.path.dirname(os.path.abspath(__file__))
GOLD_CHICAGO = os.path.join(BASE, "data", "gold", "chicago")
GOLD_YOUTUBE = os.path.join(BASE, "data", "gold", "youtube")

LAYERS = {
    "Bronze Chicago": os.path.join(BASE, "data", "bronze", "chicago"),
    "Bronze YouTube": os.path.join(BASE, "data", "bronze", "youtube"),
    "Silver Chicago": os.path.join(BASE, "data", "silver", "chicago"),
    "Silver YouTube": os.path.join(BASE, "data", "silver", "youtube"),
    "Gold Chicago":   GOLD_CHICAGO,
    "Gold YouTube":   GOLD_YOUTUBE,
}

# Design tokens
BG       = "#0d1117"
CARD     = "#161b22"
BORDER   = "#21262d"
TEXT     = "#e6edf3"
MUTED    = "#8b949e"
BLUE     = "#58a6ff"
GREEN    = "#3fb950"
ORANGE   = "#f0883e"
PURPLE   = "#bc8cff"
RED      = "#f85149"
YELLOW   = "#d29922"

CHART_COLORS = [BLUE, GREEN, ORANGE, PURPLE, RED, YELLOW, "#79c0ff", "#56d364"]

CHART_BASE = dict(
    plot_bgcolor =CARD,
    paper_bgcolor=CARD,
    font         =dict(family="Inter, sans-serif", color=MUTED, size=12),
    title_font   =dict(family="Inter, sans-serif", color=TEXT,  size=15, weight=600),
    xaxis        =dict(gridcolor=BORDER, linecolor=BORDER, tickfont=dict(color=MUTED, size=11)),
    yaxis        =dict(gridcolor=BORDER, linecolor=BORDER, tickfont=dict(color=MUTED, size=11)),
    legend       =dict(bgcolor="rgba(0,0,0,0)", font=dict(color=MUTED, size=11)),
    margin       =dict(t=48, b=12, l=12, r=12),
    hoverlabel   =dict(bgcolor="#1c2128", font_color=TEXT, bordercolor=BORDER),
)

#Helpers
def _flatten(df):
    for col in df.columns:
        if df[col].dtype == object:
            s = df[col].dropna().iloc[:1]
            if len(s) and isinstance(s.iloc[0], (dict, list)):
                df[col] = df[col].astype(str)
    return df

def _parse_coords(val):
    m = re.search(r'array\(\[\s*([\-0-9.]+)\s*,\s*([\-0-9.]+)\s*\]', str(val))
    if m:
        lon, lat = float(m.group(1)), float(m.group(2))
        return f"{lat:.4f}, {lon:.4f}"
    return str(val)[:55]

@st.cache_data(show_spinner=False)
def load(folder, sample_file=False, nrows=None):
    import pyarrow.parquet as pq
    files = sorted(glob.glob(os.path.join(folder, "part-*.parquet")))
    if not files:
        return None
    src = files[0] if (sample_file or nrows) else folder
    try:
        if nrows:
            rows = next(pq.ParquetFile(src).iter_batches(batch_size=nrows))
            df   = rows.to_pandas()
        else:
            df = pd.read_parquet(src)
    except Exception:
        df = pd.read_parquet(files[0])
    return _flatten(df)

def layer_ok(path):
    return os.path.isdir(path) and bool(os.listdir(path))

#Global CSS
st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

/* ── Base ── */
html, body, [class*="css"] {{
    font-family: 'Inter', sans-serif;
    background-color: {BG};
    color: {TEXT};
}}
.stApp {{ background-color: {BG}; }}
.block-container {{ padding: 1.6rem 2.2rem 3rem; max-width: 1400px; }}

/* ── Header ── */
.dash-header {{
    display: flex;
    align-items: center;
    gap: 1rem;
    padding: 1.4rem 2rem;
    background: {CARD};
    border: 1px solid {BORDER};
    border-radius: 10px;
    margin-bottom: 1.6rem;
}}
.dash-header-icon {{
    width: 42px; height: 42px;
    background: linear-gradient(135deg, {BLUE}, {PURPLE});
    border-radius: 10px;
    display: flex; align-items: center; justify-content: center;
    font-size: 1.2rem; flex-shrink: 0;
}}
.dash-header h1 {{
    margin: 0; font-size: 1.45rem; font-weight: 700;
    color: {TEXT}; letter-spacing: -0.02em;
}}
.dash-header p {{
    margin: 0.2rem 0 0; font-size: 0.83rem; color: {MUTED};
}}

/* ── Section heading ── */
.sec-head {{
    font-size: 0.72rem; font-weight: 600; text-transform: uppercase;
    letter-spacing: 0.12em; color: {MUTED};
    margin: 1.8rem 0 0.8rem; display: flex; align-items: center; gap: 0.5rem;
}}
.sec-head::after {{
    content: ''; flex: 1; height: 1px; background: {BORDER};
}}

/* ── KPI cards ── */
.kpi-card {{
    background: {CARD};
    border: 1px solid {BORDER};
    border-radius: 10px;
    padding: 1.1rem 1.4rem;
}}
.kpi-label {{
    font-size: 0.75rem; font-weight: 500; color: {MUTED};
    text-transform: uppercase; letter-spacing: 0.06em; margin-bottom: 0.4rem;
}}
.kpi-value {{
    font-size: 1.9rem; font-weight: 700; color: {TEXT};
    letter-spacing: -0.03em; line-height: 1;
}}
.kpi-sub {{
    font-size: 0.75rem; color: {MUTED}; margin-top: 0.3rem;
}}
.kpi-accent-blue   {{ border-top: 3px solid {BLUE};   }}
.kpi-accent-green  {{ border-top: 3px solid {GREEN};  }}
.kpi-accent-orange {{ border-top: 3px solid {ORANGE}; }}

/* ── Chart wrapper ── */
.chart-card {{
    background: {CARD};
    border: 1px solid {BORDER};
    border-radius: 10px;
    padding: 1rem;
    margin-bottom: 1rem;
}}

/* ── Pipeline status ── */
.pipeline-grid {{
    display: grid;
    grid-template-columns: repeat(6, 1fr);
    gap: 0.6rem;
    margin-bottom: 1.2rem;
}}
.pip-cell {{
    background: {CARD};
    border: 1px solid {BORDER};
    border-radius: 8px;
    padding: 0.65rem 0.9rem;
}}
.pip-name {{
    font-size: 0.72rem; font-weight: 600;
    color: {MUTED}; margin-bottom: 0.3rem;
}}
.pip-ok  {{
    font-size: 0.72rem; font-weight: 700;
    color: {GREEN}; background: rgba(63,185,80,.12);
    border-radius: 4px; padding: 1px 7px; display: inline-block;
}}
.pip-err {{
    font-size: 0.72rem; font-weight: 700;
    color: {RED}; background: rgba(248,81,73,.12);
    border-radius: 4px; padding: 1px 7px; display: inline-block;
}}

/* ── Dataframe override ── */
[data-testid="stDataFrame"] {{ border-radius: 8px; overflow: hidden; }}
[data-testid="stDataFrame"] th {{
    background: #1c2128 !important; color: {MUTED} !important;
    font-size: 0.75rem !important;
}}
[data-testid="stDataFrame"] td {{
    background: {CARD} !important; color: {TEXT} !important;
    font-size: 0.82rem !important;
}}

/* ── Divider ── */
hr {{ border-color: {BORDER} !important; margin: 1.2rem 0 !important; }}

/* ── Streamlit default overrides ── */
[data-testid="stMetricValue"] {{ color: {TEXT} !important; }}
.stSpinner > div {{ color: {BLUE} !important; }}
</style>
""", unsafe_allow_html=True)

#Header
st.markdown(f"""
<div class="dash-header">
  <div class="dash-header-icon">🚦</div>
  <div>
    <h1>Urban Traffic Crash Risk &amp; Public Sentiment</h1>
    <p>Chicago crash data &nbsp;·&nbsp; YouTube sentiment &nbsp;·&nbsp; Gold-layer pipeline analytics</p>
  </div>
</div>
""", unsafe_allow_html=True)

#Pipeline status
cells = ""
for name, path in LAYERS.items():
    ok    = layer_ok(path)
    badge = f'<span class="pip-ok">● Active</span>' if ok else f'<span class="pip-err">● Missing</span>'
    cells += f'<div class="pip-cell"><div class="pip-name">{name}</div>{badge}</div>'

st.markdown(f'<div class="pipeline-grid">{cells}</div>', unsafe_allow_html=True)

#Load data
with st.spinner("Loading pipeline data…"):
    trends_df  = load(os.path.join(GOLD_CHICAGO, "monthly_trends"))
    risk_df    = load(os.path.join(GOLD_CHICAGO, "location_risk"), sample_file=True, nrows=50_000)
    youtube_df = load(os.path.join(GOLD_YOUTUBE, "channel_stats"))
    top_vids   = load(os.path.join(GOLD_YOUTUBE, "top_videos"))

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — CHICAGO CRASHES
# ══════════════════════════════════════════════════════════════════════════════
st.markdown('<div class="sec-head">Chicago Crash Analytics</div>', unsafe_allow_html=True)

if trends_df is None and risk_df is None:
    st.info("No Chicago gold data found — run the pipeline first.")
else:
    # KPI cards
    if trends_df is not None:
        total_crashes  = int(trends_df["total_crashes"].sum())
        total_injuries = int(trends_df["total_injuries"].sum())
        months         = len(trends_df)
        avg_per_month  = total_crashes // months if months else 0

        k1, k2, k3, k4 = st.columns(4)
        for col, label, value, sub, accent in [
            (k1, "Total Crashes",    f"{total_crashes:,}",  "All recorded incidents",      "blue"),
            (k2, "Total Injuries",   f"{total_injuries:,}", "Persons injured",             "orange"),
            (k3, "Months Tracked",   str(months),            "Data coverage period",        "green"),
            (k4, "Avg / Month",      f"{avg_per_month:,}",  "Mean crashes per month",      "orange"),
        ]:
            col.markdown(f"""
            <div class="kpi-card kpi-accent-{accent}">
              <div class="kpi-label">{label}</div>
              <div class="kpi-value">{value}</div>
              <div class="kpi-sub">{sub}</div>
            </div>""", unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

    cl, cr = st.columns([3, 2])

    # Monthly bar chart
    with cl:
        if trends_df is not None:
            df = trends_df.copy()
            df["period"] = df["year"].astype(str) + "-" + df["month"].astype(str).str.zfill(2)
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=df["period"], y=df["total_crashes"],
                marker_color=BLUE,
                marker_line_width=0,
                hovertemplate="<b>%{x}</b><br>Crashes: %{y:,}<extra></extra>",
            ))
            fig.update_layout(
                **CHART_BASE,
                title="Monthly Crash Volume",
                xaxis_tickangle=-45,
                showlegend=False,
                bargap=0.25,
            )
            st.markdown('<div class="chart-card">', unsafe_allow_html=True)
            st.plotly_chart(fig, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)

    # Severity donut
    with cr:
        if risk_df is not None and "severity" in risk_df.columns:
            sev = (
                risk_df.groupby("severity")["total_crashes"]
                .sum().reset_index()
                .sort_values("total_crashes", ascending=False)
                .head(6)
            )
            fig2 = go.Figure(go.Pie(
                labels=sev["severity"],
                values=sev["total_crashes"],
                hole=0.55,
                marker=dict(colors=CHART_COLORS, line=dict(color=CARD, width=2)),
                textfont=dict(color=TEXT, size=12),
                hovertemplate="<b>%{label}</b><br>%{value:,} crashes<br>%{percent}<extra></extra>",
            ))
            fig2.update_layout(
                **CHART_BASE,
                title="Crashes by Severity",
                annotations=[dict(
                    text="Severity", x=0.5, y=0.5,
                    font=dict(size=12, color=MUTED, family="Inter"),
                    showarrow=False,
                )],
            )
            fig2.update_layout(
                legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color=MUTED, size=11),
                            orientation="v", x=1.02, y=0.5)
            )
            st.markdown('<div class="chart-card">', unsafe_allow_html=True)
            st.plotly_chart(fig2, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)

    # Top locations table
    if risk_df is not None and "location" in risk_df.columns:
        st.markdown('<div class="sec-head" style="margin-top:0.6rem">Top 10 High-Risk Locations</div>',
                    unsafe_allow_html=True)
        top_loc = (
            risk_df.groupby("location")[["total_crashes", "total_injuries"]]
            .sum().reset_index()
            .sort_values("total_crashes", ascending=False)
            .head(10)
        )
        top_loc["location"] = top_loc["location"].apply(_parse_coords)
        top_loc = top_loc.rename(columns={
            "location": "📍 Location (lat, lon)",
            "total_crashes": "💥 Crashes",
            "total_injuries": "🩺 Injuries",
        }).reset_index(drop=True)
        st.dataframe(top_loc, use_container_width=True, hide_index=True)

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — YOUTUBE SENTIMENT
# ══════════════════════════════════════════════════════════════════════════════
st.markdown('<div class="sec-head">YouTube Public Sentiment</div>', unsafe_allow_html=True)

if youtube_df is None and top_vids is None:
    st.info("No YouTube gold data found — run the pipeline first.")
else:
    yl, yr = st.columns(2)

    with yl:
        if youtube_df is not None and "video_count" in youtube_df.columns:
            df_yt = youtube_df.sort_values("video_count", ascending=False).head(10)
            fig3 = go.Figure(go.Bar(
                x=df_yt["channel"],
                y=df_yt["video_count"],
                marker=dict(
                    color=df_yt["video_count"],
                    colorscale=[[0, "#1f3a5f"], [0.5, BLUE], [1, "#a8d4ff"]],
                    line_width=0,
                ),
                hovertemplate="<b>%{x}</b><br>Videos: %{y}<extra></extra>",
            ))
            fig3.update_layout(
                **CHART_BASE,
                title="Videos per Channel",
                showlegend=False,
                xaxis_tickangle=-30,
                bargap=0.3,
            )
            st.markdown('<div class="chart-card">', unsafe_allow_html=True)
            st.plotly_chart(fig3, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)
        else:
            st.info("Channel stats not available.")

    with yr:
        if youtube_df is not None and "viral_video_count" in youtube_df.columns:
            df_v = youtube_df.copy()
            df_v["non_viral"] = df_v["video_count"] - df_v["viral_video_count"]
            df_v = df_v.sort_values("viral_video_count", ascending=False).head(10)

            fig4 = go.Figure()
            fig4.add_trace(go.Bar(
                name="Viral", x=df_v["channel"], y=df_v["viral_video_count"],
                marker_color=ORANGE, marker_line_width=0,
                hovertemplate="<b>%{x}</b><br>Viral: %{y}<extra></extra>",
            ))
            fig4.add_trace(go.Bar(
                name="Non-Viral", x=df_v["channel"], y=df_v["non_viral"],
                marker_color="#2d3748", marker_line_width=0,
                hovertemplate="<b>%{x}</b><br>Non-viral: %{y}<extra></extra>",
            ))
            fig4.update_layout(
                **CHART_BASE,
                title="Viral vs Non-Viral Videos",
                barmode="stack",
                xaxis_tickangle=-30,
                bargap=0.3,
            )
            fig4.update_layout(
                legend=dict(
                    bgcolor="rgba(0,0,0,0)", font=dict(color=MUTED, size=11),
                    orientation="h", x=0, y=1.08,
                )
            )
            st.markdown('<div class="chart-card">', unsafe_allow_html=True)
            st.plotly_chart(fig4, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)
        else:
            st.info("Viral breakdown not available.")

    if top_vids is not None:
        st.markdown('<div class="sec-head" style="margin-top:0.6rem">Top Performing Videos</div>',
                    unsafe_allow_html=True)
        wanted    = ["title", "channel", "engagement_score", "is_viral"]
        show_cols = [c for c in wanted if c in top_vids.columns]
        st.dataframe(top_vids[show_cols].head(10).reset_index(drop=True),
                     use_container_width=True, hide_index=True)

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown(f"""
<hr>
<p style="text-align:center; color:{MUTED}; font-size:0.78rem; margin:0.5rem 0 0;">
  Urban Traffic Crash Risk &amp; Public Sentiment Pipeline &nbsp;·&nbsp; Gold Layer &nbsp;·&nbsp; Data auto-refreshes on pipeline run
</p>
""", unsafe_allow_html=True)
