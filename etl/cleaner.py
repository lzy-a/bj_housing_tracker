"""
数据清洗和转换模块
"""
import pandas as pd
import numpy as np
from typing import Dict, List
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataCleaner:
    """数据清洗类"""
    
    @staticmethod
    def clean_listings(listings: List[Dict]) -> pd.DataFrame:
        """清洗房源数据"""
        df = pd.DataFrame(listings)
        
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
        
        # 移除极端值（3倍标准差）
        for col in ['price', 'unit_price']:
            mean = df[col].mean()
            std = df[col].std()
            df = df[(df[col] >= mean - 3*std) & (df[col] <= mean + 3*std)]
        
        return df
    
    @staticmethod
    def aggregate_by_district(df: pd.DataFrame) -> pd.DataFrame:
        """按区聚合数据"""
        agg_dict = {
            'price': ['mean', 'median', 'min', 'max', 'std'],
            'unit_price': ['mean', 'median'],
            'area': ['mean', 'median']
        }
        
        district_stats = df.groupby('district').agg(agg_dict)
        district_stats.columns = ['_'.join(col).strip() for col in district_stats.columns.values]
        
        return district_stats.reset_index()
    
    @staticmethod
    def calculate_price_trends(df: pd.DataFrame, window: int = 7) -> pd.DataFrame:
        """计算价格趋势"""
        df = df.sort_values('timestamp')
        
        # 按区计算移动平均
        df['price_ma'] = df.groupby('district')['unit_price'].transform(
            lambda x: x.rolling(window=window, min_periods=1).mean()
        )
        
        # 计算环比变化
        df['price_change_pct'] = df.groupby('district')['unit_price'].pct_change() * 100
        
        return df