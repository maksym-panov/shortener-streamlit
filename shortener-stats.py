import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import pycountry
from datetime import datetime

st.set_page_config(page_title="Shortener Microservice Analytics", layout="wide", initial_sidebar_state="expanded")

st.title("Shortener Microservice Analytics")
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        border-radius: 10px;
        padding: 15px;
        text-align: center;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_data
def load_data():
    try:
        df_clicks = pd.read_csv('clicks_stream.csv')
        df_clicks['timestamp'] = pd.to_datetime(df_clicks['timestamp'])

        df_meta = pd.read_csv('urls_metadata.csv')
        df_meta['created_at'] = pd.to_datetime(df_meta['created_at'])

        df_merged = df_clicks.merge(
            df_meta[['short_code', 'user_id', 'is_active', 'created_at']],
            on='short_code',
            how='left',
            suffixes=('', '_meta')
        )
        return df_merged
    except FileNotFoundError:
        st.error("Error: CSV files not found.")
        return pd.DataFrame()


def get_iso3_country_code(alpha2_code):
    try:
        return pycountry.countries.get(alpha_2=alpha2_code).alpha_3
    except:
        return None


df = load_data()

if df.empty:
    st.stop()

st.sidebar.header("Filtering params")

platforms = df['platform'].unique()
selected_platforms = st.sidebar.multiselect("Platforms", platforms, default=platforms)

statuses = df['status_code'].unique()
selected_statuses = st.sidebar.multiselect("HTTP Status", statuses, default=statuses)

mask = (
        (df['platform'].isin(selected_platforms)) &
        (df['status_code'].isin(selected_statuses))
)
df_filtered = df.loc[mask]

col1, col2, col3, col4 = st.columns(4)

total_reqs = len(df_filtered)
avg_latency = df_filtered['latency_ms'].mean()
error_rate = (len(df_filtered[df_filtered['status_code'] >= 400]) / total_reqs * 100) if total_reqs > 0 else 0
unique_users = df_filtered['ip_address'].nunique()

col1.metric("Total Requests", f"{total_reqs:,}")
col2.metric("Mean Latency", f"{avg_latency:.1f} ms", delta_color="inverse")
col3.metric("Error Rate", f"{error_rate:.2f}%", delta_color="inverse")
col4.metric("Unique IPs", f"{unique_users:,}")

st.markdown("---")

tab_geo, tab_perf, tab_feature, tab_biz = st.tabs([
    "Geography and Devices",
    "Performance & Reliability",
    "Feature Engineering",
    "Business Metrics"
])

with tab_geo:
    st.subheader("Geospatial distribution of traffic")

    row1_col1, row1_col2 = st.columns([2, 1])

    with row1_col1:
        country_counts = df_filtered.groupby('country_code').size().reset_index(name='count')
        country_counts['iso_alpha_3'] = country_counts['country_code'].apply(get_iso3_country_code)

        fig_map = px.choropleth(
            country_counts,
            locations="iso_alpha_3",
            color="count",
            hover_name="country_code",
            color_continuous_scale=px.colors.sequential.Plasma,
            title="Global Traffic Map",
            projection="natural earth"
        )
        fig_map.update_layout(margin={"r": 0, "t": 40, "l": 0, "b": 0})
        st.plotly_chart(fig_map, use_container_width=True)

    with row1_col2:
        top_ua = df_filtered.groupby('user_agent').size().nlargest(10).index
        df_sunburst = df_filtered[df_filtered['user_agent'].isin(top_ua)]
        df_sunburst['request_count'] = 1

        fig_sun = px.sunburst(
            df_sunburst,
            path=['platform', 'user_agent'],
            values='request_count',
            title="Platform -> User Agent",
            color='platform'
        )
        st.plotly_chart(fig_sun, use_container_width=True)

with tab_perf:
    st.subheader("System performance analysis")

    col_p1, col_p2 = st.columns([2, 1])

    with col_p1:
        df_resampled = df_filtered.set_index('timestamp').resample('H').agg({
            'latency_ms': 'mean',
            'event_id': 'count'
        }).rename(columns={'event_id': 'requests'})

        fig_line = go.Figure()
        fig_line.add_trace(go.Scatter(x=df_resampled.index, y=df_resampled['latency_ms'],
                                      mode='lines', name='Avg Latency (ms)', line=dict(color='orange')))
        fig_line.add_trace(go.Scatter(x=df_resampled.index, y=df_resampled['requests'],
                                      mode='lines', name='RPH', yaxis='y2',
                                      line=dict(color='cyan', width=1, dash='dot')))

        fig_line.update_layout(
            title="Load vs Latency",
            yaxis=dict(title="Latency (ms)"),
            yaxis2=dict(title="Requests per Hour", overlaying='y', side='right'),
            hovermode="x unified"
        )
        st.plotly_chart(fig_line, use_container_width=True)

    with col_p2:
        T = 500
        satisfied = len(df_filtered[df_filtered['latency_ms'] < T])
        tolerating = len(df_filtered[(df_filtered['latency_ms'] >= T) & (df_filtered['latency_ms'] < 4 * T)])
        total = len(df_filtered)

        apdex_score = (satisfied + (tolerating / 2)) / total if total > 0 else 0

        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number+delta",
            value=apdex_score,
            title={'text': "Apdex Score"},
            delta={'reference': 0.85},
            gauge={
                'axis': {'range': [0, 1]},
                'bar': {'color': "darkblue"},
                'steps': [
                    {'range': [0, 0.5], 'color': "red"},
                    {'range': [0.5, 0.85], 'color': "yellow"},
                    {'range': [0.85, 1], 'color': "green"}],
                'threshold': {
                    'line': {'color': "white", 'width': 4},
                    'thickness': 0.75,
                    'value': 0.94}}))
        st.plotly_chart(fig_gauge, use_container_width=True)

    df_filtered['hour'] = df_filtered['timestamp'].dt.hour
    df_filtered['day_name'] = df_filtered['timestamp'].dt.day_name()

    days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

    heatmap_data = df_filtered.pivot_table(index='day_name', columns='hour', values='event_id',
                                           aggfunc='count').reindex(days_order)

    fig_heat = px.imshow(
        heatmap_data,
        labels=dict(x="Hour", y="Day", color="Requests"),
        x=heatmap_data.columns,
        y=heatmap_data.index,
        color_continuous_scale='Viridis',
        aspect="auto"
    )
    st.plotly_chart(fig_heat, use_container_width=True)

with tab_feature:
    st.header("Feature engineering for ML")

    df_features = df_filtered.copy()

    df_features['is_weekend'] = df_features['timestamp'].dt.dayofweek >= 5
    df_features['part_of_day'] = pd.cut(df_features['timestamp'].dt.hour,
                                        bins=[0, 6, 12, 18, 24],
                                        labels=['Night', 'Morning', 'Afternoon', 'Evening'],
                                        include_lowest=True)

    df_features['log_latency'] = np.log1p(df_features['latency_ms'])

    st.dataframe(
        df_features[['timestamp', 'latency_ms', 'log_latency', 'is_weekend', 'part_of_day', 'country_code']].head(10))

    col_f1, col_f2 = st.columns(2)

    with col_f1:
        fig_box = px.box(df_features, x="part_of_day", y="latency_ms", color="is_weekend",
                         title="Latency Distribution")
        st.plotly_chart(fig_box, use_container_width=True)

    with col_f2:
        df_encoded = df_features.copy()
        df_encoded['part_of_day_code'] = df_encoded['part_of_day'].cat.codes
        df_encoded['platform_code'] = df_encoded['platform'].astype('category').cat.codes

        corr_cols = ['latency_ms', 'status_code', 'is_weekend', 'part_of_day_code', 'platform_code']
        corr_matrix = df_encoded[corr_cols].corr()

        fig_corr = px.imshow(corr_matrix, text_auto=True, color_continuous_scale='RdBu_r', zmin=-1, zmax=1)
        st.plotly_chart(fig_corr, use_container_width=True)

with tab_biz:
    st.header("Business Analysis")

    link_counts = df_filtered['short_code'].value_counts().reset_index()
    link_counts.columns = ['short_code', 'clicks']
    link_counts['cumulative_clicks'] = link_counts['clicks'].cumsum()
    link_counts['cumulative_perc'] = link_counts['cumulative_clicks'] / link_counts['clicks'].sum()
    link_counts['rank'] = range(1, len(link_counts) + 1)

    fig_pareto = go.Figure()
    fig_pareto.add_trace(go.Bar(x=link_counts['rank'], y=link_counts['clicks'], name='Clicks'))
    fig_pareto.add_trace(go.Scatter(x=link_counts['rank'], y=link_counts['cumulative_perc'],
                                    name='Cumulative %', yaxis='y2', line=dict(color='red')))

    fig_pareto.update_layout(
        title="Traffic Distribution (Pareto)",
        xaxis=dict(title="Link Rank"),
        yaxis=dict(title="Clicks"),
        yaxis2=dict(title="Cumulative %", overlaying='y', side='right', tickformat=".0%"),
        showlegend=True
    )
    st.plotly_chart(fig_pareto, use_container_width=True)

st.markdown("---")
st.caption(f"© {datetime.now().year} Maksym Panov Bachelor Thesis' Analytics Module")