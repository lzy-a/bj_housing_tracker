import psycopg2
import pandas as pd
from pathlib import Path
import logging
from datetime import datetime
import time
from psycopg2 import extras
from psycopg2 import pool
import sys

# 确保项目根目录在 path 中
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import DB_CONFIG as DEFAULT_DB_CONFIG

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    """数据库管理类"""

    def __init__(self, db_config=None):
        """初始化数据库连接池

        Args:
            db_config: PostgreSQL 连接配置，如果为 None，则使用 settings.py 中的 DB_CONFIG
        """
        if db_config is None:
            self.db_config = DEFAULT_DB_CONFIG.copy()
        elif isinstance(db_config, str):
            # 兼容旧调用（传入 SQLite 路径），提示迁移到 PostgreSQL
            raise ValueError(
                f"已不支持 SQLite。请使用 PostgreSQL 连接。"
                f"确保 Docker 已启动: docker-compose up -d"
            )
        else:
            self.db_config = db_config
        
        # 创建连接池
        self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=10,
            **self.db_config
        )
        self._init_db()
    
    def _get_connection(self):
        """从连接池获取连接"""
        return self.connection_pool.getconn()
    
    def _return_connection(self, conn):
        """将连接返回连接池"""
        if conn:
            self.connection_pool.putconn(conn)
    
    def _init_db(self):
        """初始化数据库表"""
        # 从连接池获取连接
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # 1. 每日区域大盘表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS district_snapshots (
                id SERIAL PRIMARY KEY,
                record_date DATE NOT NULL,               -- 爬取日期
                region VARCHAR(50) NOT NULL,             -- 区域 (如: '西城区', '朝阳区', '海淀区')
                total_listings INTEGER,                  -- 当日该区在售房源总数
                avg_unit_price FLOAT,                   -- 1. 简单均价 (算术平均)：反映市场整体挂牌水平
                median_unit_price FLOAT,                -- 2. 中位数单价：最贴近"体感"的真实房价，过滤极端值
                weighted_avg_price FLOAT,               -- 3. 资产平米价 (总价除以总面积)：反映区域资产价值
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(record_date, region)
            )
        ''')
        
        # 2. 房源详情主表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS property_details (
                id SERIAL PRIMARY KEY,
                house_id VARCHAR(20) UNIQUE NOT NULL,   -- 房源唯一ID
                title TEXT NOT NULL,                    -- 房源标题
                region VARCHAR(50) NOT NULL,            -- 行政区
                biz_circle VARCHAR(50),                 -- 商圈
                community VARCHAR(100),                 -- 小区名
                community_id VARCHAR(20),               -- 5i5j 小区物理ID（租售关联桥梁）
                layout VARCHAR(20),                     -- 户型 (2室1厅)
                area FLOAT,                             -- 面积
                price FLOAT,                            -- 总价 (万)
                unit_price FLOAT,                       -- 单价
                orientation VARCHAR(20),                -- 朝向
                decoration VARCHAR(20),                 -- 装修程度
                floor_info VARCHAR(50),                 -- 楼层信息
                building_type VARCHAR(50),              -- 建筑类型 (板楼/塔楼)
                build_year INTEGER,                     -- 建筑年代
                address_raw TEXT,                       -- 原始字符串留底
                
                -- 【关键时间维度】
                first_seen_date DATE,                   -- 首次入库日期 (用于计算"新上房源")
                last_seen_date DATE,                    -- 最后一次被爬虫抓到的日期 (用于判定"下架")
                last_update_date DATE,                  -- 【业务时间】网页显示的最后更新日期 (用于判定"僵尸房源")
                
                status INTEGER DEFAULT 1,               -- 状态 (1:在售, 0:下架/消失)
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 3. 价格历史轨迹表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS price_history (
                id SERIAL PRIMARY KEY,
                house_id VARCHAR(20) NOT NULL,          -- 房源唯一ID
                price FLOAT NOT NULL,                   -- 变动后的总价 (万)
                unit_price FLOAT NOT NULL,              -- 变动后的单价
                record_date DATE NOT NULL,              -- 抓取到变动的日期 (爬取时间)
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (house_id) REFERENCES property_details(house_id)
            )
        ''')
        
        # 4. 社区信息表（包含经纬度和乡镇）
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS community_info (
                id SERIAL PRIMARY KEY,
                community VARCHAR(100) UNIQUE NOT NULL, -- 小区名
                region VARCHAR(50) NOT NULL,            -- 区域
                town_id VARCHAR(20),                    -- 乡镇ID
                town_name VARCHAR(50),                  -- 乡镇名称
                longitude FLOAT,                        -- 经度
                latitude FLOAT                         -- 纬度
            )
        ''')
        
        # 创建索引
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_property_details_house_id ON property_details(house_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_property_details_region ON property_details(region)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_price_history_house_id ON price_history(house_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_price_history_record_date ON price_history(record_date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_district_snapshots_record_date ON district_snapshots(record_date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_district_snapshots_region ON district_snapshots(region)')

        # 为社区信息表创建索引
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_community_info_community ON community_info(community)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_community_info_region ON community_info(region)')

        # 5. 租房房源主表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS rental_details (
                id SERIAL PRIMARY KEY,
                house_id VARCHAR(20) UNIQUE NOT NULL,
                community_id VARCHAR(20),
                title TEXT NOT NULL,
                region VARCHAR(50) NOT NULL,
                biz_circle VARCHAR(50),
                community VARCHAR(100),
                layout VARCHAR(20),
                area FLOAT,
                rent_price FLOAT,
                rent_type VARCHAR(10),
                orientation VARCHAR(20),
                decoration VARCHAR(20),
                floor_info VARCHAR(50),
                first_seen_date DATE,
                last_seen_date DATE,
                status INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # 6. 租金变动记录
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS rent_history (
                id SERIAL PRIMARY KEY,
                house_id VARCHAR(20) NOT NULL,
                rent_price FLOAT NOT NULL,
                record_date DATE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (house_id) REFERENCES rental_details(house_id)
            )
        ''')

        # 7. 每日区域租赁大盘
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS district_rent_snapshots (
                id SERIAL PRIMARY KEY,
                record_date DATE NOT NULL,
                region VARCHAR(50) NOT NULL,
                total_rentals INTEGER,
                avg_rent_price FLOAT,
                median_rent_price FLOAT,
                avg_unit_rent FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(record_date, region)
            )
        ''')

        # 8. 小区级租售联动表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS community_metrics (
                id SERIAL PRIMARY KEY,
                record_date DATE NOT NULL,
                community_id VARCHAR(20),
                community VARCHAR(100) NOT NULL,
                region VARCHAR(50) NOT NULL,
                biz_circle VARCHAR(50),
                sale_count INTEGER,
                avg_sale_price FLOAT,
                avg_sale_unit_price FLOAT,
                rental_count INTEGER,
                avg_rent_price FLOAT,
                avg_rent_unit_price FLOAT,
                price_rent_ratio FLOAT,
                rental_yield FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(community_id)
            )
        ''')

        # 租赁相关索引
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_rental_details_house_id ON rental_details(house_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_rental_details_region ON rental_details(region)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_rental_details_community_id ON rental_details(community_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_rent_history_house_id ON rent_history(house_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_rent_history_record_date ON rent_history(record_date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_district_rent_snapshots_date ON district_rent_snapshots(record_date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_district_rent_snapshots_region ON district_rent_snapshots(region)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_community_metrics_date ON community_metrics(record_date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_community_metrics_community_id ON community_metrics(community_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_property_details_community_id ON property_details(community_id)')

        # 加速看板查询的复合索引
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_ph_house_date ON price_history (house_id, record_date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_pd_comm_region_date ON property_details (community, region, first_seen_date, last_seen_date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_ph_date ON price_history (record_date)')
        


        conn.commit()
        cursor.close()
        self._return_connection(conn)
        logger.info("数据库初始化完成: PostgreSQL")
    


    def insert_property_details(self, house_id: str, title: str, region: str, biz_circle: str, community: str, 
                            layout: str, area: float, price: float, unit_price: float, 
                            orientation: str, decoration: str, floor_info: str, 
                            building_type: str, build_year: int, address_raw: str, last_update_date: str):
        """插入或更新房源详情，带重试机制"""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                conn = self._get_connection()
                cursor = conn.cursor()
                today = datetime.now().date()
                action = None
                
                # 检查房源是否已存在
                cursor.execute('SELECT first_seen_date, price, unit_price FROM property_details WHERE house_id = %s', (house_id,))
                result = cursor.fetchone()
                
                if result:
                    # 已存在，更新信息
                    first_seen_date = result[0]
                    old_price = result[1]
                    old_unit_price = result[2]
                    
                    cursor.execute('''
                        UPDATE property_details 
                        SET title = %s, region = %s, biz_circle = %s, community = %s, layout = %s, area = %s, 
                            price = %s, unit_price = %s, orientation = %s, decoration = %s, floor_info = %s, 
                            building_type = %s, build_year = %s, address_raw = %s, last_seen_date = %s, 
                            last_update_date = %s, status = 1, updated_at = CURRENT_TIMESTAMP
                        WHERE house_id = %s
                    ''', (title, region, biz_circle, community, layout, area, price, unit_price, 
                        orientation, decoration, floor_info, building_type, build_year, address_raw, 
                        today, last_update_date, house_id))
                    action = "更新"
                    logger.debug(f"更新房源信息: {house_id}")
                else:
                    # 不存在，插入新记录
                    first_seen_date = today
                    old_price = None
                    old_unit_price = None
                    
                    cursor.execute('''
                        INSERT INTO property_details 
                        (house_id, title, region, biz_circle, community, layout, area, price, unit_price, 
                        orientation, decoration, floor_info, building_type, build_year, address_raw, 
                        first_seen_date, last_seen_date, last_update_date, status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
                    ''', (house_id, title, region, biz_circle, community, layout, area, price, unit_price, 
                        orientation, decoration, floor_info, building_type, build_year, address_raw, 
                        today, today, last_update_date))
                    action = "插入"
                    logger.debug(f"插入新房源: {house_id}")
                
                conn.commit()
                cursor.close()
                self._return_connection(conn)
                return action
            except psycopg2.OperationalError as e:
                if "connection is closed" in str(e) or "could not connect" in str(e) and retry_count < max_retries - 1:
                    logger.warning(f"数据库连接失败，第 {retry_count + 1} 次重试...")
                    retry_count += 1
                    time.sleep(1)
                    continue
                else:
                    logger.error(f"插入/更新房源详情失败: {e}")
                    break
            except Exception as e:
                logger.error(f"插入/更新房源详情失败: {e}")
                break
            finally:
                if 'cursor' in locals():
                    try:
                        cursor.close()
                    except:
                        pass
                if 'conn' in locals():
                    try:
                        self._return_connection(conn)
                    except:
                        pass
        return None
    
    def insert_price_history(self, house_id: str, price: float, unit_price: float, record_date: str):
        """插入价格历史记录，带重试机制"""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                conn = self._get_connection()
                cursor = conn.cursor()
                
                cursor.execute('''
                    INSERT INTO price_history (house_id, price, unit_price, record_date)
                    VALUES (%s, %s, %s, %s)
                ''', (house_id, price, unit_price, record_date))
                conn.commit()
                cursor.close()
                self._return_connection(conn)
                logger.debug(f"成功插入价格历史记录: {house_id} - {record_date}")
                return True
            except psycopg2.OperationalError as e:
                if "connection is closed" in str(e) or "could not connect" in str(e) and retry_count < max_retries - 1:
                    logger.warning(f"数据库连接失败，第 {retry_count + 1} 次重试...")
                    retry_count += 1
                    time.sleep(1)
                    continue
                else:
                    logger.error(f"插入价格历史记录失败: {e}")
                    break
            except Exception as e:
                logger.error(f"插入价格历史记录失败: {e}")
                break
            finally:
                if 'conn' in locals():
                    try:
                        conn.close()
                    except:
                        pass
        return False
    
    def insert_district_snapshot(self, record_date: str, region: str, total_listings: int, avg_unit_price: float, median_unit_price: float, weighted_avg_price: float):
        """插入区域快照数据"""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO district_snapshots (record_date, region, total_listings, avg_unit_price, median_unit_price, weighted_avg_price)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (record_date, region) DO UPDATE
                SET total_listings = EXCLUDED.total_listings,
                    avg_unit_price = EXCLUDED.avg_unit_price,
                    median_unit_price = EXCLUDED.median_unit_price,
                    weighted_avg_price = EXCLUDED.weighted_avg_price
            ''', (record_date, region, total_listings, avg_unit_price, median_unit_price, weighted_avg_price))
            conn.commit()
            logger.info(f"成功插入区域快照数据: {record_date} - {region}")
        except Exception as e:
            logger.error(f"插入区域快照数据失败: {e}")
        finally:
            cursor.close()
            self._return_connection(conn)
    
    def mark_disappeared_properties(self, region: str):
        """标记消失的房源为下架状态"""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            # 将状态为2（待确认）的房源标记为0（下架）
            cursor.execute('''
                UPDATE property_details 
                SET status = 0 
                WHERE region = %s AND status = 2
            ''', (region,))
            affected = cursor.rowcount
            conn.commit()
            logger.info(f"成功标记 {affected} 个消失的房源为下架状态")
        except Exception as e:
            logger.error(f"标记消失房源失败: {e}")
        finally:
            cursor.close()
            self._return_connection(conn)
    
    def get_district_snapshots(self, region: str = None) -> pd.DataFrame:
        """获取区域快照数据"""
        conn = self._get_connection()
        try:
            query = 'SELECT * FROM district_snapshots'
            params = []
            if region:
                query += ' WHERE region = %s'
                params.append(region)
            query += ' ORDER BY record_date DESC'
            return pd.read_sql_query(query, conn, params=params)
        except Exception as e:
            logger.error(f"查询区域快照数据失败: {e}")
            return pd.DataFrame()
        finally:
            self._return_connection(conn)
    
    def get_property_details(self, region: str = None, status: int = None) -> pd.DataFrame:
        """获取房源详情数据"""
        conn = self._get_connection()
        try:
            query = 'SELECT * FROM property_details WHERE 1=1'
            params = []
            if region:
                query += ' AND region = %s'
                params.append(region)
            if status is not None:
                query += ' AND status = %s'
                params.append(status)
            return pd.read_sql_query(query, conn, params=params)
        except Exception as e:
            logger.error(f"查询房源详情数据失败: {e}")
            return pd.DataFrame()
        finally:
            self._return_connection(conn)
    
    def get_price_history(self, house_id: str = None) -> pd.DataFrame:
        """获取价格历史数据"""
        conn = self._get_connection()
        try:
            if house_id:
                query = 'SELECT * FROM price_history WHERE house_id = %s ORDER BY record_date DESC'
                return pd.read_sql_query(query, conn, params=(house_id,))
            else:
                return pd.read_sql_query('SELECT * FROM price_history ORDER BY record_date DESC', conn)
        except Exception as e:
            logger.error(f"查询价格历史数据失败: {e}")
            return pd.DataFrame()
        finally:
            self._return_connection(conn)
    
    def get_property(self, house_id: str) -> dict:
        """获取房源详情"""
        conn = self._get_connection()
        cursor = conn.cursor(cursor_factory=extras.DictCursor)
        try:
            cursor.execute('SELECT * FROM property_details WHERE house_id = %s', (house_id,))
            result = cursor.fetchone()
            if result:
                return dict(result)
            return None
        except Exception as e:
            logger.error(f"获取房源详情失败: {e}")
            return None
        finally:
            cursor.close()
            self._return_connection(conn)
    
    def get_latest_price(self, house_id: str) -> float:
        """获取房源最新价格"""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            # 先从price_history表获取最新价格
            cursor.execute('''
                SELECT price FROM price_history 
                WHERE house_id = %s 
                ORDER BY record_date DESC 
                LIMIT 1
            ''', (house_id,))
            result = cursor.fetchone()
            if result:
                return result[0]
            
            # 如果price_history表中没有记录，从property_details表获取
            cursor.execute('SELECT price FROM property_details WHERE house_id = %s', (house_id,))
            result = cursor.fetchone()
            if result:
                return result[0]
            
            return 0
        except Exception as e:
            logger.error(f"获取最新价格失败: {e}")
            return 0
        finally:
            cursor.close()
            self._return_connection(conn)
    
    def update_property_status(self, house_id: str = None, region: str = None, status: int = None, last_seen_date: str = None, last_update_date: str = None):
        """更新房源状态"""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            if house_id:
                # 更新单个房源
                update_fields = []
                params = []
                
                if status is not None:
                    update_fields.append('status = %s')
                    params.append(status)
                if last_seen_date:
                    update_fields.append('last_seen_date = %s')
                    params.append(last_seen_date)
                if last_update_date:
                    update_fields.append('last_update_date = %s')
                    params.append(last_update_date)
                
                if update_fields:
                    update_fields.append('updated_at = CURRENT_TIMESTAMP')
                    query = f"UPDATE property_details SET {', '.join(update_fields)} WHERE house_id = %s"
                    params.append(house_id)
                    cursor.execute(query, params)
                    conn.commit()
                    logger.debug(f"成功更新房源状态: {house_id}")
            elif region:
                # 更新整个区域的房源状态
                if status is not None:
                    cursor.execute('UPDATE property_details SET status = %s, updated_at = CURRENT_TIMESTAMP WHERE region = %s', (status, region))
                    conn.commit()
                    logger.info(f"成功更新 {region} 区域的房源状态为 {status}")
        except Exception as e:
            logger.error(f"更新房源状态失败: {e}")
        finally:
            cursor.close()
            self._return_connection(conn)
    
    def batch_insert_property_details(self, properties):
        """批量插入房源详情
        
        Args:
            properties: 房源列表，每个元素是包含房源信息的字典
        """
        if not properties:
            return
        
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            # 使用 execute_values 进行批量插入
            from psycopg2.extras import execute_values
            
            today = datetime.now().date()
            values = []
            
            for prop in properties:
                # 检查房源是否已存在
                cursor.execute('SELECT first_seen_date FROM property_details WHERE house_id = %s', (prop['house_id'],))
                result = cursor.fetchone()
                
                if result:
                    # 已存在，更新
                    cursor.execute('''
                        UPDATE property_details
                        SET title = %s, region = %s, biz_circle = %s, community = %s, community_id = %s,
                            layout = %s, area = %s, price = %s, unit_price = %s, orientation = %s,
                            decoration = %s, floor_info = %s, building_type = %s, build_year = %s,
                            address_raw = %s, last_seen_date = %s, last_update_date = %s,
                            status = 1, updated_at = CURRENT_TIMESTAMP
                        WHERE house_id = %s
                    ''', (prop['title'], prop['region'], prop['biz_circle'], prop['community'],
                          prop.get('community_id'), prop['layout'],
                          prop['area'], prop['price'], prop['unit_price'], prop['orientation'], prop['decoration'],
                          prop['floor_info'], prop['building_type'], prop['build_year'], prop['address_raw'],
                          today, prop['last_update_date'], prop['house_id']))
                else:
                    # 不存在，插入
                    values.append((
                        prop['house_id'], prop['title'], prop['region'], prop['biz_circle'], prop['community'],
                        prop.get('community_id'),
                        prop['layout'], prop['area'], prop['price'], prop['unit_price'], prop['orientation'],
                        prop['decoration'], prop['floor_info'], prop['building_type'], prop['build_year'],
                        prop['address_raw'], today, today, prop['last_update_date'], 1
                    ))
            
            # 批量插入新记录
            if values:
                execute_values(
                    cursor,
                    '''
                    INSERT INTO property_details
                    (house_id, title, region, biz_circle, community, community_id,
                    layout, area, price, unit_price, orientation, decoration, floor_info,
                    building_type, build_year, address_raw,
                    first_seen_date, last_seen_date, last_update_date, status)
                    VALUES %s
                    ON CONFLICT (house_id) DO NOTHING
                    ''',
                    values
                )
            
            conn.commit()
            logger.info(f"批量插入/更新 {len(properties)} 条房源详情")
        except Exception as e:
            logger.error(f"批量插入房源详情失败: {e}")
            conn.rollback()
        finally:
            cursor.close()
            self._return_connection(conn)
    
    def batch_insert_price_history(self, prices):
        """批量插入价格历史
        
        Args:
            prices: 价格历史列表，每个元素是包含价格信息的字典
        """
        if not prices:
            return
        
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            # 使用 execute_values 进行批量插入
            from psycopg2.extras import execute_values
            
            values = [(p['house_id'], p['price'], p['unit_price'], p['record_date']) for p in prices]
            
            execute_values(
                cursor,
                '''
                INSERT INTO price_history (house_id, price, unit_price, record_date)
                VALUES %s
                ''',
                values
            )
            
            conn.commit()
            logger.info(f"批量插入 {len(prices)} 条价格历史记录")
        except Exception as e:
            logger.error(f"批量插入价格历史失败: {e}")
            conn.rollback()
        finally:
            cursor.close()
            self._return_connection(conn)
    
    def batch_insert_community_info(self, communities):
        """批量插入社区信息
        
        Args:
            communities: 社区信息列表，每个元素是包含社区信息的字典
        """
        if not communities:
            return
        
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            # 使用 execute_values 进行批量插入
            from psycopg2.extras import execute_values
            
            values = [(c['community'], c['region'], c.get('town_id'), c.get('town_name'), c.get('经度'), c.get('纬度')) for c in communities]
            
            execute_values(
                cursor,
                '''
                INSERT INTO community_info (community, region, town_id, town_name, longitude, latitude)
                VALUES %s
                ON CONFLICT (community) DO UPDATE
                SET region = EXCLUDED.region,
                    town_id = EXCLUDED.town_id,
                    town_name = EXCLUDED.town_name,
                    longitude = EXCLUDED.longitude,
                    latitude = EXCLUDED.latitude
                ''',
                values
            )
            
            conn.commit()
            logger.info(f"批量插入/更新 {len(communities)} 条社区信息")
        except Exception as e:
            logger.error(f"批量插入社区信息失败: {e}")
            conn.rollback()
        finally:
            cursor.close()
            self._return_connection(conn)
    
    def execute_query(self, query, params=None):
        """执行自定义SQL查询"""
        conn = self._get_connection()
        try:
            return pd.read_sql_query(query, conn, params=params)
        except Exception as e:
            logger.error(f"执行SQL查询失败: {e}")
            return pd.DataFrame()
        finally:
            self._return_connection(conn)

    # ================================================================
    # 租房相关
    # ================================================================

    def batch_insert_rental_details(self, rentals):
        """批量插入租房详情"""
        if not rentals:
            return

        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            from psycopg2.extras import execute_values

            today = datetime.now().date()
            values = []

            for r in rentals:
                cursor.execute('SELECT first_seen_date FROM rental_details WHERE house_id = %s', (r['house_id'],))
                result = cursor.fetchone()

                if result:
                    cursor.execute('''
                        UPDATE rental_details
                        SET title = %s, region = %s, biz_circle = %s, community = %s, community_id = %s,
                            layout = %s, area = %s, rent_price = %s, rent_type = %s,
                            orientation = %s, decoration = %s, floor_info = %s,
                            last_seen_date = %s, status = 1, updated_at = CURRENT_TIMESTAMP
                        WHERE house_id = %s
                    ''', (r['title'], r['region'], r['biz_circle'], r['community'],
                          r.get('community_id'), r['layout'], r['area'], r['rent_price'],
                          r.get('rent_type', '整租'), r['orientation'], r['decoration'],
                          r['floor_info'], today, r['house_id']))
                else:
                    values.append((
                        r['house_id'], r.get('community_id'), r['title'], r['region'],
                        r['biz_circle'], r['community'], r['layout'], r['area'],
                        r['rent_price'], r.get('rent_type', '整租'), r['orientation'],
                        r['decoration'], r['floor_info'], today, today, 1
                    ))

            if values:
                execute_values(cursor, '''
                    INSERT INTO rental_details
                    (house_id, community_id, title, region, biz_circle, community,
                     layout, area, rent_price, rent_type, orientation, decoration,
                     floor_info, first_seen_date, last_seen_date, status)
                    VALUES %s
                    ON CONFLICT (house_id) DO NOTHING
                ''', values)

            conn.commit()
            logger.info(f"批量插入/更新 {len(rentals)} 条租房房源")
        except Exception as e:
            logger.error(f"批量插入租房详情失败: {e}")
            conn.rollback()
        finally:
            cursor.close()
            self._return_connection(conn)

    def batch_insert_rent_history(self, prices):
        """批量插入租金变动"""
        if not prices:
            return

        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            from psycopg2.extras import execute_values
            values = [(p['house_id'], p['rent_price'], p['record_date']) for p in prices]
            execute_values(cursor, '''
                INSERT INTO rent_history (house_id, rent_price, record_date)
                VALUES %s
            ''', values)
            conn.commit()
            logger.info(f"批量插入 {len(prices)} 条租金变动记录")
        except Exception as e:
            logger.error(f"批量插入租金变动失败: {e}")
            conn.rollback()
        finally:
            cursor.close()
            self._return_connection(conn)

    def insert_district_rent_snapshot(self, record_date, region, total_rentals,
                                       avg_rent_price, median_rent_price, avg_unit_rent):
        """插入区域租赁快照"""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO district_rent_snapshots
                (record_date, region, total_rentals, avg_rent_price, median_rent_price, avg_unit_rent)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (record_date, region) DO UPDATE
                SET total_rentals = EXCLUDED.total_rentals,
                    avg_rent_price = EXCLUDED.avg_rent_price,
                    median_rent_price = EXCLUDED.median_rent_price,
                    avg_unit_rent = EXCLUDED.avg_unit_rent
            ''', (record_date, region, total_rentals, avg_rent_price, median_rent_price, avg_unit_rent))
            conn.commit()
            logger.info(f"成功插入区域租赁快照: {record_date} - {region}")
        except Exception as e:
            logger.error(f"插入区域租赁快照失败: {e}")
        finally:
            cursor.close()
            self._return_connection(conn)

    def compute_community_metrics(self, record_date):
        """计算指定日期的小区级租售联动指标"""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO community_metrics
                (record_date, community_id, community, region, biz_circle,
                 sale_count, avg_sale_price, avg_sale_unit_price,
                 rental_count, avg_rent_price, avg_rent_unit_price,
                 price_rent_ratio, rental_yield)
                SELECT
                    %s AS record_date,
                    COALESCE(s.community_id, r.community_id) AS community_id,
                    COALESCE(s.community, r.community) AS community,
                    COALESCE(s.region, r.region) AS region,
                    COALESCE(s.biz_circle, r.biz_circle) AS biz_circle,
                    s.sale_count,
                    s.avg_sale_price,
                    s.avg_sale_unit_price,
                    r.rental_count,
                    r.avg_rent_price,
                    r.avg_rent_unit_price,
                    CASE WHEN r.avg_rent_price > 0
                         THEN ROUND((s.avg_sale_price * 10000) / (r.avg_rent_price * 12))
                    END AS price_rent_ratio,
                    CASE WHEN s.avg_sale_price > 0
                         THEN ROUND((r.avg_rent_price * 12) / (s.avg_sale_price * 10000) * 100)
                    END AS rental_yield
                FROM (
                    SELECT community_id, community, region, biz_circle,
                           COUNT(*) AS sale_count,
                           ROUND(AVG(price)) AS avg_sale_price,
                           ROUND(AVG(unit_price)) AS avg_sale_unit_price
                    FROM property_details
                    WHERE status = 1 AND community_id IS NOT NULL
                    GROUP BY community_id, community, region, biz_circle
                ) s
                FULL OUTER JOIN (
                    SELECT community_id, community, region, biz_circle,
                           COUNT(*) AS rental_count,
                           ROUND(AVG(rent_price)) AS avg_rent_price,
                           ROUND(AVG(rent_price / NULLIF(area, 0))) AS avg_rent_unit_price
                    FROM rental_details
                    WHERE status = 1 AND community_id IS NOT NULL
                    GROUP BY community_id, community, region, biz_circle
                ) r ON s.community_id = r.community_id
                ON CONFLICT (community_id) DO UPDATE
                SET community = EXCLUDED.community,
                    region = EXCLUDED.region,
                    biz_circle = EXCLUDED.biz_circle,
                    sale_count = EXCLUDED.sale_count,
                    avg_sale_price = EXCLUDED.avg_sale_price,
                    avg_sale_unit_price = EXCLUDED.avg_sale_unit_price,
                    rental_count = EXCLUDED.rental_count,
                    avg_rent_price = EXCLUDED.avg_rent_price,
                    avg_rent_unit_price = EXCLUDED.avg_rent_unit_price,
                    price_rent_ratio = EXCLUDED.price_rent_ratio,
                    rental_yield = EXCLUDED.rental_yield
            ''', (record_date,))
            conn.commit()
            logger.info(f"社区租售联动指标计算完成: {record_date}")
        except Exception as e:
            logger.error(f"计算社区租售联动指标失败: {e}")
            conn.rollback()
        finally:
            cursor.close()
            self._return_connection(conn)
