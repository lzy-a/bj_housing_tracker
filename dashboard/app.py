"""
北京楼市看板 - Streamlit应用
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import sys
from pathlib import Path

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from etl.db_manager import DatabaseManager
from config.settings import DASHBOARD_CONFIG

# 页面配置
st.set_page_config(
    page_title="北京楼市看板",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 初始化数据库
db = DatabaseManager()

# 侧边栏
st.sidebar.title("🏙️ 北京楼市看板")

page = st.sidebar.radio(
    "选择页面",
    ["📊 概览", "📈 价格趋势", "🗺️ 区域分析", "📰 政策新闻", "ℹ️ 关于"]
)

# 主页面
if page == "📊 概览":
    st.title("北京楼市概览")
    
    # 获取最新数据
    stats = db.get_district_stats()
    
    if not stats.empty:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            avg_price = stats['avg_price'].mean()
            st.metric("平均房价（万元）", f"{avg_price/10000:.1f}")
        
        with col2:
            avg_unit_price = stats['avg_unit_price'].mean()
            st.metric("平均单价（元/㎡）", f"{avg_unit_price:,.0f}")
        
        with col3:
            total_listings = stats['listing_count'].sum()
            st.metric("房源总数", f"{total_listings:,}")
        
        with col4:
            latest_update = stats['timestamp'].max()
            st.metric("最后更新", latest_update.strftime("%m-%d %H:%M"))
        
        st.divider()
        
        # 区域房价对比
        st.subheader("各区房价对比")
        fig = px.bar(
            stats.sort_values('avg_price', ascending=False),
            x='district',
            y='avg_price',
            title="各区平均房价",
            labels={'district': '区', 'avg_price': '平均房价（元）'}
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # 数据表格
        st.subheader("区级统计数据")
        st.dataframe(
            stats[[
                'district', 'avg_price', 'median_price',
                'avg_unit_price', 'median_unit_price', 'listing_count'
            ]].sort_values('avg_price', ascending=False),
            use_container_width=True
        )
    else:
        st.info("📊 暂无数据，请先运行爬虫收集数据")

elif page == "📈 价格趋势":
    st.title("价格趋势分析")
    
    # 选择区域
    trend_data = db.get_price_trend()
    
    if not trend_data.empty:
        districts = trend_data['district'].unique()
        selected_district = st.selectbox(
            "选择区域",
            districts,
            key="district_select"
        )
        
        # 过滤数据
        district_trend = trend_data[trend_data['district'] == selected_district]
        
        # 价格趋势图
        fig = px.line(
            district_trend,
            x='date',
            y='avg_unit_price',
            title=f"{selected_district}区 单价趋势",
            labels={'date': '日期', 'avg_unit_price': '平均单价（元/㎡）'}
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # 房源数量趋势
        fig2 = px.bar(
            district_trend,
            x='date',
            y='listing_count',
            title=f"{selected_district}区 房源数量趋势",
            labels={'date': '日期', 'listing_count': '房源数量'}
        )
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("📈 暂无趋势数据，请先运行爬虫收集数据")

elif page == "🗺️ 区域分析":
    st.title("区域深度分析")
    st.info("该功能开发中...")

elif page == "📰 政策新闻":
    st.title("房产政策新闻")
    st.info("该功能开发中...")

elif page == "ℹ️ 关于":
    st.title("关于本项目")
    st.markdown("""
    ### 北京楼市看板
    
    本看板汇总北京房产市场数据，包括：
    - 📊 实时房价数据（链家、安居客等）
    - 📈 价格趋势分析
    - 🗺️ 区域对比分析
    - 📰 房产政策新闻
    
    **数据来源：**
    - 链家网
    - 北京住建委
    - 安居客
    
    **更新频率：** 每6小时自动更新
    """)