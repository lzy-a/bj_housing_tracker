import pandas as pd
import logging
from typing import Dict, List


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataCleaner:
    """数据清洗类"""
    
    @staticmethod
    def clean_listings(listings: List[Dict]) -> pd.DataFrame:
        """清洗房源数据"""
        if not listings:
            return pd.DataFrame()
        
        df = pd.DataFrame(listings)
        logger.info(f"原始数据数量: {len(df)}")
        
        # 处理缺失值
        df = df.dropna(subset=['price', 'area'])
        
        # 数据类型转换
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        df['area'] = pd.to_numeric(df['area'], errors='coerce')
        df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
        
        # 移除异常值
        df = df[df['price'] > 0]
        df = df[df['area'] > 0]
        df = df[df['unit_price'] > 0]
        
        logger.info(f"清洗后数据数量: {len(df)}")
        return df
    
    @staticmethod
    def aggregate_by_district(df: pd.DataFrame) -> pd.DataFrame:
        """按区聚合数据"""
        if df.empty:
            return pd.DataFrame()
        
        district_stats = df.groupby('district').agg({
            'price': ['mean', 'median', 'count'],
            'unit_price': ['mean', 'median'],
            'area': ['mean']
        }).reset_index()
        
        district_stats.columns = ['district', 'avg_price', 'median_price', 
                                   'count', 'avg_unit_price', 'median_unit_price', 'avg_area']
        
        return district_stats