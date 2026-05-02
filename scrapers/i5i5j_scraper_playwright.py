#!/usr/bin/env python3
"""
使用 Playwright 的 5i5j 爬虫
"""
import asyncio
import logging
import re
import sys
from pathlib import Path
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from datetime import datetime
import requests

sys.path.insert(0, str(Path(__file__).parent.parent))
from config.settings import I5I5J_PHONE, I5I5J_PASSWORD

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class I5I5JScraperPlaywright:
    """使用 Playwright 的 5i5j 爬虫"""
    
    def __init__(self, debug_port=9223):
        """初始化爬虫"""
        self.debug_port = debug_port
        self.base_url = "https://bj.5i5j.com"
        self.browser = None
        self.context = None
        self.pages = []
    
    async def connect(self):
        """连接到现有 Chrome 实例"""
        # 启动 playwright 驱动
        self.playwright = await async_playwright().start()
        http_url = f"http://localhost:{self.debug_port}"
        logger.info(f"🔗 尝试连接到 Chrome 实例: {http_url}")
        
        try:
            # 先通过 HTTP URL 获取 WebSocket URL
            response = requests.get(f"{http_url}/json/version")
            response.raise_for_status()
            ws_url = response.json()['webSocketDebuggerUrl']
            logger.info(f"✅ 成功获取 WebSocket URL: {ws_url}")
            
            # 🔗 使用 WebSocket URL 连接已有的 Chrome
            self.browser = await self.playwright.chromium.connect_over_cdp(ws_url)
            logger.info(f"✅ 成功连接到 Chrome 实例")
            
            # 🚨 关键修改：不要 new_context，直接拿第一个已经存在的 context
            # 这样才能确保你使用的是那个'已经登录'的环境
            if len(self.browser.contexts) > 0:
                self.context = self.browser.contexts[0]
                logger.info(f"✅ 成功获取现有浏览器上下文")
            else:
                self.context = await self.browser.new_context()
                logger.info("✅ 成功创建浏览器上下文")
                
            logger.info(f"✅ 成功接管现有 Chrome 环境")
        except Exception as e:
            logger.error(f"❌ 连接失败: {e}")
            logger.error("请确保 Chrome 已启动并开启了远程调试端口: /Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome --remote-debugging-port=9223")
            raise
        return self
    
    async def create_pages(self, count):
        """创建多个标签页"""
        for i in range(count):
            page = await self.context.new_page()
            self.pages.append(page)
        logger.info(f"✅ 已创建 {len(self.pages)} 个标签页")
    
    async def close(self):
        """关闭浏览器"""
        # 只关闭页面，不关闭浏览器
        # 因为浏览器是手动开启的，关闭它会影响用户使用
        if self.pages:
            for page in self.pages:
                try:
                    await page.close()
                except Exception as e:
                    logger.error(f"❌ 关闭页面失败: {e}")
        # 停止 playwright 驱动
        if hasattr(self, 'playwright') and self.playwright:
            await self.playwright.stop()
        logger.info("✅ 已关闭所有标签页，释放资源")
    
    async def check_and_login(self):
        """检查是否需要登录并自动登录"""
        logger.info("🔐 开始检查登录状态")
        
        # 创建一个临时页面来检测登录状态
        login_check_page = await self.context.new_page()
        
        try:
            # 导航到特定页面来检测登录状态
            login_check_url = "https://bj.5i5j.com/ershoufang/xichengqu/n1/"
            logger.info(f"🚀 导航到登录检测页面: {login_check_url}")
            
            # 导航到页面，等待网络空闲
            await login_check_page.goto(login_check_url, wait_until="networkidle", timeout=30000)
            
            # 检查是否是登录页面
            login_tab = await login_check_page.query_selector('.login-tab[data-type="password-login"]')
            if login_tab:
                logger.info("🔐 检测到登录页面，开始自动登录")
                
                # 点击密码登录选项
                await login_tab.click()
                logger.info("✅ 点击了密码登录选项")
                
                # 等待输入框出现
                await login_check_page.wait_for_selector('#phone1', timeout=5000)
                await login_check_page.wait_for_selector('.password', timeout=5000)
                
                # 输入账号密码
                phone = I5I5J_PHONE
                password = I5I5J_PASSWORD
                
                await login_check_page.fill('#phone1', phone)
                await login_check_page.fill('.password', password)
                logger.info("✅ 输入了账号密码")
                
                # 点击登录按钮
                login_button = await login_check_page.query_selector('#login-submit')
                if login_button:
                    await login_button.click()
                    logger.info("✅ 点击了登录按钮")
                    
                    # 等待登录完成
                    await login_check_page.wait_for_load_state('networkidle', timeout=15000)
                    logger.info("✅ 登录完成")
                    
                    # 等待3秒，确保登录状态完全保存
                    logger.info("⏳ 等待3秒，确保登录状态完全保存")
                    await asyncio.sleep(3)
                else:
                    logger.error("❌ 未找到登录按钮")
            else:
                logger.info("✅ 未检测到登录页面，继续操作")
        except Exception as e:
            logger.error(f"❌ 自动登录失败: {e}")
        finally:
            # 关闭登录检查页面
            await login_check_page.close()
            logger.info("✅ 登录检查页面已关闭")
    
    @staticmethod
    def _parse_title_and_id(list_con):
        """从 listCon div 提取标题和房源ID，返回 (title, house_code) 或 (None, None)"""
        title_h3 = list_con.find('h3', {'class': 'listTit'})
        if not title_h3:
            return None, None
        title_link = title_h3.find('a')
        if not title_link:
            return None, None
        title = title_link.get_text(strip=True)
        href = title_link.get('href', '')
        m = re.search(r'/ershoufang/(\d+).html', href)
        if not m:
            return None, None
        return title, m.group(1)

    @staticmethod
    def _parse_info_fields(first_p):
        """从基本信息p标签提取户型/面积/朝向/楼层/装修/建筑年代，返回 dict"""
        info_text = first_p.get_text(strip=True)
        build_year = None
        mac_title = first_p.find('span', {'class': 'mac_title'})
        if mac_title:
            m = re.search(r'约(\d{4})年建成', mac_title.get_text(strip=True))
            if m:
                try:
                    build_year = int(m.group(1))
                except (ValueError, TypeError):
                    pass

        info_parts = [p.strip() for p in info_text.split('·')]
        if any('车位' in p for p in info_parts):
            return None  # 跳过车位

        layout, area, orientation, floor, decoration = '未知', None, '未知', '未知', '未知'
        for part in info_parts:
            if '室' in part and '地下' not in part:
                layout = part
            elif '房间' in part:
                layout = part
            elif '平米' in part:
                m = re.search(r'(\d+\.?\d*)', part)
                if m:
                    try:
                        area = float(m.group(1))
                    except (ValueError, TypeError):
                        pass
            elif any(d in part for d in ['东', '南', '西', '北']):
                orientation = part
            elif '层' in part:
                floor = part
            elif any(d in part for d in ['精装', '简装', '毛坯', '豪装', '中装']):
                decoration = part

        return {
            'layout': layout, 'area': area, 'orientation': orientation,
            'floor_info': floor, 'decoration': decoration,
            'build_year': build_year, 'address_raw': info_text
        }

    @staticmethod
    def _parse_position(list_x):
        """从 listX div 提取商圈和小区，返回 (biz_circle, community, community_id)"""
        biz_circle, community, community_id = '未知', '未知', None
        i02 = list_x.find('i', {'class': 'i_02'})
        if not i02:
            return biz_circle, community, community_id
        second_p = i02.find_parent('p')
        if not second_p:
            return biz_circle, community, community_id

        community_link = second_p.find('a')
        if community_link:
            community = community_link.get_text(strip=True)
            m_cid = re.search(r'/xiaoqu/(\d+)\.html', community_link.get('href', ''))
            if m_cid:
                community_id = m_cid.group(1)

        i02_next = i02.next_sibling
        if i02_next:
            biz_circle_text = ''
            current = i02_next
            while current and current.name != 'a':
                if isinstance(current, str):
                    biz_circle_text += current
                current = current.next_sibling
            biz_circle = biz_circle_text.strip()

        if not biz_circle:
            position_text = second_p.get_text(strip=True)
            if '·' in position_text:
                parts = position_text.split('·')
                if len(parts) > 1:
                    biz_circle = parts[0].strip()
        return biz_circle, community, community_id

    @staticmethod
    def _parse_update_time(list_x):
        """提取更新时间，返回日期字符串"""
        i03 = list_x.find('i', {'class': 'i_03'})
        if i03:
            third_p = i03.find_parent('p')
            if third_p:
                m = re.search(r'(\d{4}-\d{2}-\d{2})', third_p.get_text())
                if m:
                    return m.group(1)
        return datetime.now().strftime('%Y-%m-%d')

    @staticmethod
    def _parse_price(jia_div):
        """从 jia div 提取总价和单价，返回 (price, unit_price) 或 (None, None)"""
        price_p = jia_div.find('p', {'class': 'redC'})
        unit_price_ps = jia_div.find_all('p')
        if not price_p or len(unit_price_ps) < 2:
            return None, None

        try:
            price = float(price_p.get_text(strip=True).replace('万', '').strip())
        except (ValueError, TypeError):
            price = 0

        try:
            unit_price = float(unit_price_ps[1].get_text(strip=True).replace('元/m²', '').replace(',', '').strip())
        except (ValueError, TypeError):
            unit_price = 0

        return price, unit_price

    @classmethod
    def extract_information(cls, soup):
        """提取房源信息"""
        data = []
        try:
            house_list = soup.find('ul', {'class': 'pList'})
            if not house_list:
                logger.warning("⚠️ 未找到房源列表")
                return data

            items = house_list.find_all('li', recursive=False)
            logger.info(f"📦 找到 {len(items)} 个房源")

            for house_item in items:
                try:
                    if house_item.get('style') == 'display:none;':
                        continue
                    if house_item.find('div', {'class': 'tag-now'}):
                        continue

                    list_con = house_item.find('div', {'class': 'listCon'})
                    if not list_con:
                        continue

                    title, house_code = cls._parse_title_and_id(list_con)
                    if not house_code:
                        continue

                    list_x = list_con.find('div', {'class': 'listX'})
                    if not list_x:
                        continue

                    first_p = list_x.find('p', text=lambda t: t and '室' in t)
                    if not first_p:
                        i01 = list_x.find('i', {'class': 'i_01'})
                        if i01:
                            first_p = i01.find_parent('p')
                        else:
                            continue

                    info = cls._parse_info_fields(first_p)
                    if info is None:
                        continue  # 车位

                    biz_circle, community, community_id = cls._parse_position(list_x)
                    update_time = cls._parse_update_time(list_x)

                    jia_div = list_x.find('div', {'class': 'jia'})
                    if not jia_div:
                        continue
                    price, unit_price = cls._parse_price(jia_div)
                    if price is None:
                        continue

                    if info['area'] is not None and info['area'] > 0 and price > 0 and unit_price > 0:
                        data.append({
                            'house_id': house_code,
                            'title': title,
                            'region': '',
                            'biz_circle': biz_circle,
                            'community': community,
                            'community_id': community_id,
                            'layout': info['layout'],
                            'area': info['area'],
                            'price': price,
                            'unit_price': unit_price,
                            'orientation': info['orientation'],
                            'decoration': info['decoration'],
                            'floor_info': info['floor_info'],
                            'building_type': '未知',
                            'build_year': info['build_year'],
                            'address_raw': info['address_raw'],
                            'update_time': update_time
                        })
                except Exception as e:
                    logger.debug(f"解析房源失败: {e}")
                    continue

            logger.info(f"✅ 提取 {len(data)} 条有效房源")
            return data
        except Exception as e:
            logger.error(f"提取失败: {e}")
            return data