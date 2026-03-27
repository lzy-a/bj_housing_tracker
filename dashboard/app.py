import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
import sys
from pathlib import Path

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from etl.db_manager import DatabaseManager

# 页面配置
st.set_page_config(
    page_title="北京楼市看板",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 初始化数据库
db = DatabaseManager()

# 标题
st.title("🏠 北京楼市看板")

# 侧边栏
st.sidebar.title("导航")
page = st.sidebar.radio(
    "选择页面",
    ["📊 概览", "📈 价格趋势", "📰 关于"]
)

if page == "📊 概览":
    st.header("北京楼市概览")
    
    # 获取最新数据
    stats = db.get_district_stats()
    listings = db.get_latest_listings(20)
    
    if not stats.empty:
        # 关键指标
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
            st.metric("最后更新", datetime.now().strftime("%m-%d %H:%M"))
        
        st.divider()
        
        # 区域房价对比
        st.subheader("各区房价对比")
        fig = px.bar(
            stats.sort_values('avg_price', ascending=False),
            x='district',
            y='avg_price',
            title="各区平均房价（万元）",
            labels={'district': '区', 'avg_price': '平均房价（万元）'}
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # 数据表格
        st.subheader("区级统计数据")
        display_stats = stats[['district', 'avg_price', 'median_price', 'avg_unit_price', 'listing_count']].copy()
        display_stats['avg_price'] = display_stats['avg_price'] / 10000
        display_stats['median_price'] = display_stats['median_price'] / 10000
        st.dataframe(display_stats, use_container_width=True)
        
        st.divider()
        
        # 最新房源
        st.subheader("最新房源")
        if not listings.empty:
            st.dataframe(listings[['title', 'price', 'area', 'unit_price', 'district']], use_container_width=True)
        else:
            st.info("暂无房源数据")
    else:
        st.info("📊 暂无数据，请先运行爬虫收集数据")

elif page == "📈 价格趋势":
    st.header("价格趋势分析")
    st.info("该功能开发中...")

elif page == "📰 关于":
    st.header("关于本项目")
    st.markdown("""
    ### 北京楼市看板
    
    本看板汇总北京房产市场数据。
    
    **功能：**
    - 📊 实时房价数据统计
    - 📈 价格趋势分析
    - 🗺️ 区域对比分析
    
    **数据来源：**
    - 链家网
    
    **更新频率：** 手动运行爬虫更新
    """)