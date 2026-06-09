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

        # 租赁相关索引
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_rental_details_house_id ON rental_details(house_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_rental_details_region ON rental_details(region)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_rental_details_community_id ON rental_details(community_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_rent_history_house_id ON rent_history(house_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_rent_history_record_date ON rent_history(record_date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_district_rent_snapshots_date ON district_rent_snapshots(record_date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_district_rent_snapshots_region ON district_rent_snapshots(region)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_property_details_community_id ON property_details(community_id)')

        # 加速看板查询的复合索引
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_ph_house_date ON price_history (house_id, record_date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_pd_comm_region_date ON property_details (community, region, first_seen_date, last_seen_date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_ph_date ON price_history (record_date)')

        # ========== Finder 模块表 ==========

        # 8. 收藏小区
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS watchlist_communities (
                id SERIAL PRIMARY KEY,
                community_id VARCHAR(20),
                community VARCHAR(100) NOT NULL,
                region VARCHAR(50),
                biz_circle VARCHAR(50),
                is_active BOOLEAN DEFAULT TRUE,
                filter_criteria JSONB DEFAULT '{}',
                added_date DATE DEFAULT CURRENT_DATE,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(community)
            )
        ''')

        # 9. 房源照片
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS rental_photos (
                id SERIAL PRIMARY KEY,
                house_id VARCHAR(20) NOT NULL,
                photo_url TEXT NOT NULL,
                local_path TEXT,
                room_type VARCHAR(30),
                downloaded BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (house_id) REFERENCES rental_details(house_id)
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_rental_photos_house_id ON rental_photos(house_id)')

        # 10. 多维度评分
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS rental_scores (
                id SERIAL PRIMARY KEY,
                house_id VARCHAR(20) NOT NULL,
                score_date DATE NOT NULL,
                scores JSONB,
                llm_summary TEXT,
                scoring_model VARCHAR(50),
                raw_input TEXT,
                raw_output TEXT,
                notified BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(house_id, score_date),
                FOREIGN KEY (house_id) REFERENCES rental_details(house_id)
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_rental_scores_house_id ON rental_scores(house_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_rental_scores_date ON rental_scores(score_date)')

        # rental_details 新增列（详情页爬取标记）
        cursor.execute('''
            ALTER TABLE rental_details
            ADD COLUMN IF NOT EXISTS detail_scraped BOOLEAN DEFAULT FALSE
        ''')
        # watchlist_communities 新增列（筛选条件）
        cursor.execute('''
            ALTER TABLE watchlist_communities
            ADD COLUMN IF NOT EXISTS filter_criteria JSONB DEFAULT '{}'
        ''')
        cursor.execute('''
            ALTER TABLE rental_details
            ADD COLUMN IF NOT EXISTS detail_scraped_at TIMESTAMP
        ''')
        cursor.execute('''
            ALTER TABLE rental_scores
            ADD COLUMN IF NOT EXISTS raw_input TEXT
        ''')
        cursor.execute('''
            ALTER TABLE rental_scores
            ADD COLUMN IF NOT EXISTS raw_output TEXT
        ''')

        # 11. 小区通勤缓存
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS community_commute (
                id SERIAL PRIMARY KEY,
                community VARCHAR(100) NOT NULL,
                community_id VARCHAR(20),
                dest_name VARCHAR(100),
                transit_minutes INTEGER,
                transit_distance INTEGER,
                walking_distance INTEGER,
                lng FLOAT,
                lat FLOAT,
                calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(community, dest_name)
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_community_commute_name ON community_commute(community)')

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

    def load_property_prices(self) -> dict:
        """启动时一次加载全量二手房 id → 最新价格，之后全部内存比对"""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                WITH latest_ph AS (
                    SELECT DISTINCT ON (house_id) house_id, price
                    FROM price_history
                    ORDER BY house_id, record_date DESC
                )
                SELECT pd.house_id, COALESCE(lp.price, pd.price) AS last_price
                FROM property_details pd
                LEFT JOIN latest_ph lp ON pd.house_id = lp.house_id
                WHERE pd.status = 1
            ''')
            return {row[0]: row[1] for row in cursor.fetchall()}
        except Exception as e:
            logger.error(f"加载二手房价格失败: {e}")
            return {}
        finally:
            cursor.close()
            self._return_connection(conn)

    def load_rent_prices(self) -> dict:
        """启动时一次加载全量租房 id → 最新租金，之后全部内存比对"""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                WITH latest_rh AS (
                    SELECT DISTINCT ON (house_id) house_id, rent_price
                    FROM rent_history
                    ORDER BY house_id, record_date DESC
                )
                SELECT rd.house_id, COALESCE(lr.rent_price, rd.rent_price) AS last_rent
                FROM rental_details rd
                LEFT JOIN latest_rh lr ON rd.house_id = lr.house_id
                WHERE rd.status = 1
            ''')
            return {row[0]: row[1] for row in cursor.fetchall()}
        except Exception as e:
            logger.error(f"加载租房价格失败: {e}")
            return {}
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

            # 批次内去重：同 house_id 保留最后一条
            properties = list({p['house_id']: p for p in properties}.values())

            today = datetime.now().date()
            values = [(prop['house_id'], prop['title'], prop['region'], prop['biz_circle'],
                       prop['community'], prop.get('community_id'), prop['layout'], prop['area'],
                       prop['price'], prop['unit_price'], prop['orientation'], prop['decoration'],
                       prop['floor_info'], prop['building_type'], prop['build_year'],
                       prop['address_raw'], today, today, prop['last_update_date'], 1)
                      for prop in properties]

            execute_values(cursor, '''
                INSERT INTO property_details
                (house_id, title, region, biz_circle, community, community_id,
                layout, area, price, unit_price, orientation, decoration, floor_info,
                building_type, build_year, address_raw,
                first_seen_date, last_seen_date, last_update_date, status)
                VALUES %s
                ON CONFLICT (house_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    region = EXCLUDED.region,
                    biz_circle = EXCLUDED.biz_circle,
                    community = EXCLUDED.community,
                    community_id = EXCLUDED.community_id,
                    layout = EXCLUDED.layout,
                    area = EXCLUDED.area,
                    price = EXCLUDED.price,
                    unit_price = EXCLUDED.unit_price,
                    orientation = EXCLUDED.orientation,
                    decoration = EXCLUDED.decoration,
                    floor_info = EXCLUDED.floor_info,
                    building_type = EXCLUDED.building_type,
                    build_year = EXCLUDED.build_year,
                    address_raw = EXCLUDED.address_raw,
                    last_seen_date = EXCLUDED.last_seen_date,
                    last_update_date = EXCLUDED.last_update_date,
                    status = 1,
                    updated_at = CURRENT_TIMESTAMP
            ''', values)
            
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

            # 批次内去重：同 house_id 保留最后一条
            rentals = list({r['house_id']: r for r in rentals}.values())

            today = datetime.now().date()
            values = [(r['house_id'], r.get('community_id'), r['title'], r['region'],
                       r['biz_circle'], r['community'], r['layout'], r['area'],
                       r['rent_price'], r.get('rent_type', '整租'), r['orientation'],
                       r['decoration'], r['floor_info'], today, today, 1)
                      for r in rentals]

            execute_values(cursor, '''
                INSERT INTO rental_details
                (house_id, community_id, title, region, biz_circle, community,
                 layout, area, rent_price, rent_type, orientation, decoration,
                 floor_info, first_seen_date, last_seen_date, status)
                VALUES %s
                ON CONFLICT (house_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    region = EXCLUDED.region,
                    biz_circle = EXCLUDED.biz_circle,
                    community = EXCLUDED.community,
                    community_id = EXCLUDED.community_id,
                    layout = EXCLUDED.layout,
                    area = EXCLUDED.area,
                    rent_price = EXCLUDED.rent_price,
                    rent_type = EXCLUDED.rent_type,
                    orientation = EXCLUDED.orientation,
                    decoration = EXCLUDED.decoration,
                    floor_info = EXCLUDED.floor_info,
                    last_seen_date = EXCLUDED.last_seen_date,
                    status = 1,
                    updated_at = CURRENT_TIMESTAMP
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

    # ========== Finder 模块方法 ==========

    # --- Watchlist ---

    def get_watchlist(self, active_only=False):
        """获取收藏小区列表"""
        conn = self._get_connection()
        cursor = conn.cursor(cursor_factory=extras.RealDictCursor)
        try:
            sql = "SELECT * FROM watchlist_communities"
            if active_only:
                sql += " WHERE is_active = TRUE"
            sql += " ORDER BY added_date DESC"
            cursor.execute(sql)
            return cursor.fetchall()
        finally:
            cursor.close()
            self._return_connection(conn)

    def add_to_watchlist(self, community, region=None, biz_circle=None, community_id=None,
                         notes=None, filter_criteria=None):
        """添加收藏小区"""
        import json
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            fc = json.dumps(filter_criteria or {}, ensure_ascii=False)
            cursor.execute('''
                INSERT INTO watchlist_communities (community, region, biz_circle, community_id, notes, filter_criteria)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (community) DO UPDATE
                SET region = COALESCE(EXCLUDED.region, watchlist_communities.region),
                    biz_circle = COALESCE(EXCLUDED.biz_circle, watchlist_communities.biz_circle),
                    community_id = COALESCE(EXCLUDED.community_id, watchlist_communities.community_id),
                    notes = COALESCE(EXCLUDED.notes, watchlist_communities.notes),
                    filter_criteria = EXCLUDED.filter_criteria
            ''', (community, region, biz_circle, community_id, notes, fc))
            conn.commit()
            logger.info(f"收藏小区: {community}")
        except Exception as e:
            logger.error(f"添加收藏失败: {e}")
            conn.rollback()
        finally:
            cursor.close()
            self._return_connection(conn)

    def update_watchlist(self, watchlist_id, **kwargs):
        """更新收藏小区（is_active, notes 等）"""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            sets = []
            vals = []
            for k, v in kwargs.items():
                if k in ('is_active', 'notes', 'community', 'region', 'biz_circle'):
                    sets.append(f"{k} = %s")
                    vals.append(v)
            if not sets:
                return
            vals.append(watchlist_id)
            cursor.execute(
                f"UPDATE watchlist_communities SET {', '.join(sets)} WHERE id = %s",
                vals
            )
            conn.commit()
        except Exception as e:
            logger.error(f"更新收藏失败: {e}")
            conn.rollback()
        finally:
            cursor.close()
            self._return_connection(conn)

    def remove_from_watchlist(self, watchlist_id):
        """删除收藏小区"""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("DELETE FROM watchlist_communities WHERE id = %s", (watchlist_id,))
            conn.commit()
        except Exception as e:
            logger.error(f"删除收藏失败: {e}")
            conn.rollback()
        finally:
            cursor.close()
            self._return_connection(conn)

    # --- Photos ---

    def insert_rental_photos(self, house_id, photos):
        """批量插入房源照片
        photos: list of dict with keys: photo_url, room_type
        """
        if not photos:
            return
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("DELETE FROM rental_photos WHERE house_id = %s", (house_id,))
            for p in photos:
                cursor.execute('''
                    INSERT INTO rental_photos (house_id, photo_url, room_type)
                    VALUES (%s, %s, %s)
                ''', (house_id, p['photo_url'], p.get('room_type', 'other')))
            conn.commit()
            logger.info(f"插入 {len(photos)} 张照片: house_id={house_id}")
        except Exception as e:
            logger.error(f"插入照片失败: {e}")
            conn.rollback()
        finally:
            cursor.close()
            self._return_connection(conn)

    def update_photo_local_path(self, photo_id, local_path):
        """更新照片的本地路径"""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "UPDATE rental_photos SET local_path = %s, downloaded = TRUE WHERE id = %s",
                (local_path, photo_id)
            )
            conn.commit()
        except Exception as e:
            logger.error(f"更新照片路径失败: {e}")
            conn.rollback()
        finally:
            cursor.close()
            self._return_connection(conn)

    def get_photos_for_house(self, house_id):
        """获取房源的所有照片"""
        conn = self._get_connection()
        cursor = conn.cursor(cursor_factory=extras.RealDictCursor)
        try:
            cursor.execute(
                "SELECT * FROM rental_photos WHERE house_id = %s ORDER BY id",
                (house_id,)
            )
            return cursor.fetchall()
        finally:
            cursor.close()
            self._return_connection(conn)

    # --- Scores ---

    def insert_rental_score(self, house_id, scores, llm_summary='', scoring_model='',
                            raw_input='', raw_output=''):
        """插入评分结果"""
        import json
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            from datetime import date
            cursor.execute('''
                INSERT INTO rental_scores (house_id, score_date, scores, llm_summary, scoring_model, raw_input, raw_output)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (house_id, score_date) DO UPDATE
                SET scores = EXCLUDED.scores,
                    llm_summary = EXCLUDED.llm_summary,
                    scoring_model = EXCLUDED.scoring_model,
                    raw_input = EXCLUDED.raw_input,
                    raw_output = EXCLUDED.raw_output
            ''', (house_id, date.today(), json.dumps(scores, ensure_ascii=False),
                  llm_summary, scoring_model, raw_input, raw_output))
            conn.commit()
            logger.info(f"插入评分: house_id={house_id}")
        except Exception as e:
            logger.error(f"插入评分失败: {e}")
            conn.rollback()
        finally:
            cursor.close()
            self._return_connection(conn)

    def get_listings_with_scores(self, min_score=None, max_price=None, min_area=None,
                                  max_area=None, region=None, community=None,
                                  rent_type=None, layout=None,
                                  sort_by='community_count', sort_dir='DESC',
                                  page=1, page_size=20):
        """带筛选/排序/分页的房源+评分查询（按社区分页，不断裂）"""
        conn = self._get_connection()
        cursor = conn.cursor(cursor_factory=extras.RealDictCursor)
        try:
            # 基础查询（不含 LIMIT/OFFSET）
            base_sql = """
                WITH latest_scores AS (
                    SELECT DISTINCT ON (house_id)
                        house_id, score_date, scores, llm_summary, notified
                    FROM rental_scores
                    ORDER BY house_id, score_date DESC
                )
                SELECT
                    r.house_id, r.title, r.region, r.biz_circle, r.community,
                    r.layout, r.area, r.rent_price, r.rent_type, r.orientation,
                    r.decoration, r.floor_info, r.community_id, r.first_seen_date,
                    s.score_date, s.scores, s.llm_summary, s.notified,
                    cm.transit_minutes, w.id as watchlist_id,
                    COUNT(*) OVER (PARTITION BY r.community) as community_count
                FROM rental_details r
                JOIN latest_scores s ON r.house_id = s.house_id
                LEFT JOIN community_commute cm ON cm.community = r.community AND cm.dest_name = '太平桥站'
                JOIN watchlist_communities w ON w.community = r.community AND w.is_active = TRUE
                WHERE r.status = 1
            """
            params = []

            if min_score is not None:
                base_sql += " AND (s.scores->>'推荐指数')::int >= %s"
                params.append(min_score)
            if max_price is not None:
                base_sql += " AND r.rent_price <= %s"
                params.append(max_price)
            if min_area is not None:
                base_sql += " AND r.area >= %s"
                params.append(min_area)
            if max_area is not None:
                base_sql += " AND r.area <= %s"
                params.append(max_area)
            if region:
                base_sql += " AND r.region = %s"
                params.append(region)
            if community:
                base_sql += " AND r.community = %s"
                params.append(community)
            if rent_type:
                base_sql += " AND r.rent_type = %s"
                params.append(rent_type)
            if layout:
                base_sql += " AND r.layout LIKE %s"
                params.append(f'{layout}%')

            # 获取匹配的社区列表（按房源数排序）
            comm_sql = f"SELECT community, COUNT(*) as cnt FROM ({base_sql}) sub GROUP BY community"
            allowed_sorts = {
                'score_date': 'MAX(sub.score_date)',
                'rent_price': 'MIN(sub.rent_price)',
                'area': 'MAX(sub.area)',
                'community_count': 'COUNT(*)',
            }
            sort_col = allowed_sorts.get(sort_by, 'COUNT(*)')
            sort_dir_sql = 'ASC' if sort_dir == 'ASC' else 'DESC'
            comm_sql += f" ORDER BY {sort_col} {sort_dir_sql}"

            cursor.execute(comm_sql, params)
            all_communities = cursor.fetchall()
            total_communities = len(all_communities)

            # 按社区分页（每页 5 个社区）
            communities_per_page = 5
            start = (page - 1) * communities_per_page
            end = start + communities_per_page
            page_communities = [c['community'] for c in all_communities[start:end]]

            if not page_communities:
                return {'listings': [], 'total': 0, 'page': page, 'page_size': 0,
                        'total_communities': 0, 'communities_per_page': communities_per_page}

            # 获取这些社区的所有房源（保持社区排序顺序）
            placeholders = ','.join(['%s'] * len(page_communities))
            order_cases = ' '.join(f"WHEN r.community = %s THEN {i}" for i in range(len(page_communities)))
            listings_sql = f"{base_sql} AND r.community IN ({placeholders}) ORDER BY CASE {order_cases} END, r.rent_price"
            params_with_order = params + page_communities + page_communities  # 一份用于 IN，一份用于 CASE
            cursor.execute(listings_sql, params_with_order)
            rows = cursor.fetchall()

            return {'listings': rows, 'total': len(rows), 'page': page,
                    'page_size': len(rows), 'total_communities': total_communities,
                    'communities_per_page': communities_per_page,
                    'page_communities': page_communities}
        finally:
            cursor.close()
            self._return_connection(conn)

    def get_listing_detail(self, house_id):
        """获取单个房源的完整信息（含照片和评分）"""
        conn = self._get_connection()
        cursor = conn.cursor(cursor_factory=extras.RealDictCursor)
        try:
            cursor.execute("SELECT * FROM rental_details WHERE house_id = %s", (house_id,))
            listing = cursor.fetchone()
            if not listing:
                return None

            cursor.execute(
                "SELECT * FROM rental_photos WHERE house_id = %s ORDER BY id",
                (house_id,)
            )
            photos = cursor.fetchall()

            cursor.execute('''
                SELECT * FROM rental_scores WHERE house_id = %s
                ORDER BY score_date DESC LIMIT 1
            ''', (house_id,))
            score = cursor.fetchone()

            result = dict(listing)
            result['photos'] = photos
            result['score'] = score
            return result
        finally:
            cursor.close()
            self._return_connection(conn)

    def get_unscored_in_watchlist(self):
        """获取收藏小区中未评分的活跃房源（应用筛选条件）"""
        conn = self._get_connection()
        cursor = conn.cursor(cursor_factory=extras.RealDictCursor)
        try:
            cursor.execute('''
                SELECT r.house_id, r.title, r.region, r.biz_circle, r.community,
                       r.layout, r.area, r.rent_price, r.rent_type, r.orientation,
                       r.decoration, r.floor_info, r.community_id
                FROM rental_details r
                JOIN watchlist_communities w ON r.community = w.community
                WHERE w.is_active = TRUE
                  AND r.status = 1
                  AND NOT EXISTS (
                      SELECT 1 FROM rental_scores s
                      WHERE s.house_id = r.house_id AND s.score_date = CURRENT_DATE
                  )
                  AND (w.filter_criteria->>'rent_type' IS NULL
                       OR w.filter_criteria->>'rent_type' = ''
                       OR r.rent_type = w.filter_criteria->>'rent_type')
                  AND (w.filter_criteria->>'layout' IS NULL
                       OR w.filter_criteria->>'layout' = ''
                       OR r.layout LIKE w.filter_criteria->>'layout' || '%')
                  AND (w.filter_criteria->>'max_price' IS NULL
                       OR w.filter_criteria->>'max_price' = ''
                       OR r.rent_price <= (w.filter_criteria->>'max_price')::float)
                ORDER BY r.first_seen_date DESC
            ''')
            return cursor.fetchall()
        finally:
            cursor.close()
            self._return_connection(conn)

    def mark_detail_scraped(self, house_id):
        """标记房源详情页已爬取"""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                UPDATE rental_details
                SET detail_scraped = TRUE, detail_scraped_at = CURRENT_TIMESTAMP
                WHERE house_id = %s
            ''', (house_id,))
            conn.commit()
        except Exception as e:
            logger.error(f"标记详情页已爬取失败: {e}")
            conn.rollback()
        finally:
            cursor.close()
            self._return_connection(conn)

    def get_unnotified_high_scores(self):
        """获取未通知的高分房源"""
        conn = self._get_connection()
        cursor = conn.cursor(cursor_factory=extras.RealDictCursor)
        try:
            cursor.execute('''
                SELECT s.house_id, s.score_date, s.scores, s.llm_summary,
                       r.title, r.region, r.biz_circle, r.community,
                       r.layout, r.area, r.rent_price, r.rent_type
                FROM rental_scores s
                JOIN rental_details r ON s.house_id = r.house_id
                WHERE s.notified = FALSE
                  AND s.score_date = CURRENT_DATE
                ORDER BY s.score_date DESC
            ''')
            return cursor.fetchall()
        finally:
            cursor.close()
            self._return_connection(conn)

    def mark_notified(self, house_ids):
        """标记房源已通知"""
        if not house_ids:
            return
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                UPDATE rental_scores SET notified = TRUE
                WHERE house_id = ANY(%s) AND score_date = CURRENT_DATE
            ''', (house_ids,))
            conn.commit()
        except Exception as e:
            logger.error(f"标记已通知失败: {e}")
            conn.rollback()
        finally:
            cursor.close()
            self._return_connection(conn)

    def search_communities(self, keyword, limit=20):
        """搜索小区名（自动补全用）"""
        conn = self._get_connection()
        cursor = conn.cursor(cursor_factory=extras.RealDictCursor)
        try:
            cursor.execute('''
                SELECT DISTINCT community, region, biz_circle, community_id
                FROM rental_details
                WHERE community ILIKE %s AND status = 1
                ORDER BY community
                LIMIT %s
            ''', (f'%{keyword}%', limit))
            return cursor.fetchall()
        finally:
            cursor.close()
            self._return_connection(conn)

    def get_market_data(self, community: str, layout_prefix: str = None) -> dict:
        """获取小区同户型的单位面积租金均价，供评分参考。"""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            sql = """
                SELECT AVG(rent_price / area), AVG(rent_price), AVG(area), COUNT(*)
                FROM rental_details
                WHERE community = %s AND status = 1
                  AND rent_price > 0 AND area > 0
            """
            params = [community]
            if layout_prefix:
                sql += " AND layout LIKE %s"
                params.append(f'{layout_prefix}%')
            cursor.execute(sql, params)
            row = cursor.fetchone()
            if row and row[0]:
                return {
                    'avg_unit_price': round(float(row[0]), 1),  # 元/㎡
                    'avg_price': round(float(row[1]), 0),
                    'avg_area': round(float(row[2]), 1),
                    'count': row[3],
                }
            return {}
        finally:
            cursor.close()
            self._return_connection(conn)

    # --- Commute ---

    def get_commute(self, community: str, dest_name: str = '太平桥站') -> dict:
        """获取小区通勤数据（缓存）"""
        conn = self._get_connection()
        cursor = conn.cursor(cursor_factory=extras.RealDictCursor)
        try:
            cursor.execute('''
                SELECT * FROM community_commute
                WHERE community = %s AND dest_name = %s
            ''', (community, dest_name))
            row = cursor.fetchone()
            return dict(row) if row else None
        finally:
            cursor.close()
            self._return_connection(conn)

    def save_commute(self, community: str, community_id: str, dest_name: str,
                     transit_minutes: int, transit_distance: int, walking_distance: int,
                     lng: float, lat: float):
        """保存通勤数据"""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO community_commute
                (community, community_id, dest_name, transit_minutes, transit_distance,
                 walking_distance, lng, lat)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (community, dest_name) DO UPDATE
                SET transit_minutes = EXCLUDED.transit_minutes,
                    transit_distance = EXCLUDED.transit_distance,
                    walking_distance = EXCLUDED.walking_distance,
                    lng = EXCLUDED.lng, lat = EXCLUDED.lat,
                    calculated_at = CURRENT_TIMESTAMP
            ''', (community, community_id, dest_name, transit_minutes,
                  transit_distance, walking_distance, lng, lat))
            conn.commit()
        except Exception as e:
            logger.error(f"保存通勤数据失败: {e}")
            conn.rollback()
        finally:
            cursor.close()
            self._return_connection(conn)

    def get_uncached_communities(self, dest_name: str = '太平桥站',
                                 max_price: float = None, layout_prefix: str = None) -> list:
        """获取没有通勤缓存的小区列表（只算有符合条件房源的小区）"""
        conn = self._get_connection()
        cursor = conn.cursor(cursor_factory=extras.RealDictCursor)
        try:
            sql = '''
                SELECT DISTINCT r.community, r.community_id, r.region, r.biz_circle
                FROM rental_details r
                WHERE r.status = 1 AND r.community IS NOT NULL AND r.community != ''
                AND NOT EXISTS (
                    SELECT 1 FROM community_commute c
                    WHERE c.community = r.community AND c.dest_name = %s
                )
            '''
            params = [dest_name]
            if max_price:
                sql += ' AND r.rent_price <= %s'
                params.append(max_price)
            if layout_prefix:
                sql += ' AND r.layout LIKE %s'
                params.append(f'{layout_prefix}%')
            cursor.execute(sql, params)
            return cursor.fetchall()
        finally:
            cursor.close()
            self._return_connection(conn)

    def get_all_commutes(self, dest_name: str = '太平桥站', max_minutes: int = 60,
                         min_price: float = None, max_price: float = None,
                         layout_prefix: str = None, min_count: int = None) -> list:
        """获取所有通勤数据（用于发现功能）"""
        conn = self._get_connection()
        cursor = conn.cursor(cursor_factory=extras.RealDictCursor)
        try:
            sql = '''
                SELECT c.*, COUNT(DISTINCT r.house_id) as listing_count,
                       MIN(r.rent_price) as min_rent, AVG(r.rent_price) as avg_rent
                FROM community_commute c
                JOIN rental_details r ON r.community = c.community AND r.status = 1
                WHERE c.dest_name = %s AND c.transit_minutes <= %s
            '''
            params = [dest_name, max_minutes]
            if min_price:
                sql += ' AND r.rent_price >= %s'
                params.append(min_price)
            if max_price:
                sql += ' AND r.rent_price <= %s'
                params.append(max_price)
            if layout_prefix:
                sql += ' AND r.layout LIKE %s'
                params.append(f'{layout_prefix}%')
            sql += ' GROUP BY c.id'
            if min_count:
                sql += ' HAVING COUNT(DISTINCT r.house_id) >= %s'
                params.append(min_count)
            sql += ' ORDER BY c.transit_minutes'
            cursor.execute(sql, params)
            return cursor.fetchall()
        finally:
            cursor.close()
            self._return_connection(conn)
