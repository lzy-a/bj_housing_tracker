#!/usr/bin/env python3
"""
5i5j 租房爬虫 — 复用 CDP 连接和页面管理，独立解析逻辑
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


class I5I5JRentScraperPlaywright:
    """5i5j 租房爬虫"""

    def __init__(self, debug_port=9223):
        self.debug_port = debug_port
        self.base_url = "https://bj.5i5j.com"
        self.browser = None
        self.context = None
        self.pages = []

    async def connect(self):
        """连接到现有 Chrome 实例"""
        self.playwright = await async_playwright().start()
        http_url = f"http://localhost:{self.debug_port}"
        logger.info(f"连接 Chrome: {http_url}")

        response = requests.get(f"{http_url}/json/version")
        response.raise_for_status()
        ws_url = response.json()['webSocketDebuggerUrl']

        self.browser = await self.playwright.chromium.connect_over_cdp(ws_url)
        if len(self.browser.contexts) > 0:
            self.context = self.browser.contexts[0]
        else:
            self.context = await self.browser.new_context()
        logger.info("已连接 Chrome")
        return self

    async def create_pages(self, count):
        for i in range(count):
            page = await self.context.new_page()
            self.pages.append(page)
        logger.info(f"已创建 {len(self.pages)} 个标签页")

    async def close(self):
        if self.pages:
            for page in self.pages:
                try:
                    await page.close()
                except Exception:
                    pass
        if hasattr(self, 'playwright') and self.playwright:
            await self.playwright.stop()
        logger.info("已关闭所有标签页")

    async def check_and_login(self):
        """检查登录状态并自动登录"""
        logger.info("检查登录状态")
        login_check_page = await self.context.new_page()
        try:
            login_check_url = "https://bj.5i5j.com/zufang/xichengqu/n1/"
            await login_check_page.goto(login_check_url, wait_until="networkidle", timeout=30000)

            login_tab = await login_check_page.query_selector('.login-tab[data-type="password-login"]')
            if login_tab:
                logger.info("检测到登录页面，自动登录")
                await login_tab.click()
                await login_check_page.wait_for_selector('#phone1', timeout=5000)
                await login_check_page.wait_for_selector('.password', timeout=5000)
                await login_check_page.fill('#phone1', I5I5J_PHONE)
                await login_check_page.fill('.password', I5I5J_PASSWORD)
                login_button = await login_check_page.query_selector('#login-submit')
                if login_button:
                    await login_button.click()
                    await login_check_page.wait_for_load_state('networkidle', timeout=15000)
                    await asyncio.sleep(3)
                    logger.info("登录完成")
            else:
                logger.info("已登录")
        except Exception as e:
            logger.error(f"登录检查失败: {e}")
        finally:
            await login_check_page.close()

    # ================================================================
    # 解析方法 — 镜像出售爬虫，适配租房字段
    # ================================================================

    @staticmethod
    def _parse_title_and_id(list_con):
        """提取标题和租房房源ID，返回 (title, house_code)"""
        title_h3 = list_con.find('h3', {'class': 'listTit'})
        if not title_h3:
            return None, None
        title_link = title_h3.find('a')
        if not title_link:
            return None, None
        title = title_link.get_text(strip=True)
        href = title_link.get('href', '')
        m = re.search(r'/zufang/(\d+)\.html', href)
        if not m:
            return None, None
        return title, m.group(1)

    @staticmethod
    def _parse_info_fields(first_p):
        """提取户型/面积/朝向/楼层/装修，返回 dict"""
        info_text = first_p.get_text(strip=True)
        info_parts = [p.strip() for p in info_text.split('·')]
        if any('车位' in p for p in info_parts):
            return None

        layout, area, orientation, floor, decoration = '未知', None, '未知', '未知', '未知'
        for part in info_parts:
            if ('室' in part or '房间' in part) and '地下' not in part:
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
            'address_raw': info_text
        }

    @staticmethod
    def _parse_position(list_x):
        """提取商圈和小区，返回 (biz_circle, community, community_id)"""
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
        """提取更新时间"""
        i03 = list_x.find('i', {'class': 'i_03'})
        if i03:
            third_p = i03.find_parent('p')
            if third_p:
                m = re.search(r'(\d{4}-\d{2}-\d{2})', third_p.get_text())
                if m:
                    return m.group(1)
        return datetime.now().strftime('%Y-%m-%d')

    @staticmethod
    def _parse_rent_price(jia_div):
        """提取月租金，返回 rent_price 或 None"""
        price_p = jia_div.find('p', {'class': 'redC'})
        if not price_p:
            return None
        price_text = price_p.get_text(strip=True).replace('元/月', '').replace(',', '').strip()
        try:
            return float(price_text)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _parse_rent_type(list_con):
        """提取整租/合租类型"""
        # 标题中通常包含整租/合租
        title_h3 = list_con.find('h3', {'class': 'listTit'})
        if title_h3:
            title_text = title_h3.get_text(strip=True)
            if '合租' in title_text or '合住' in title_text:
                return '合租'
        return '整租'

    @classmethod
    def extract_information(cls, soup):
        """提取租房信息"""
        data = []
        try:
            house_list = soup.find('ul', {'class': 'pList'})
            if not house_list:
                logger.warning("未找到房源列表")
                return data

            items = house_list.find_all('li', recursive=False)
            logger.info(f"找到 {len(items)} 个租房房源")

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
                        continue

                    biz_circle, community, community_id = cls._parse_position(list_x)
                    update_time = cls._parse_update_time(list_x)

                    jia_div = list_x.find('div', {'class': 'jia'})
                    if not jia_div:
                        continue
                    rent_price = cls._parse_rent_price(jia_div)
                    if rent_price is None:
                        continue

                    rent_type = cls._parse_rent_type(list_con)

                    if info['area'] is not None and info['area'] > 0 and rent_price > 0:
                        data.append({
                            'house_id': house_code,
                            'title': title,
                            'region': '',
                            'biz_circle': biz_circle,
                            'community': community,
                            'community_id': community_id,
                            'layout': info['layout'],
                            'area': info['area'],
                            'rent_price': rent_price,
                            'rent_type': rent_type,
                            'orientation': info['orientation'],
                            'decoration': info['decoration'],
                            'floor_info': info['floor_info'],
                            'update_time': update_time
                        })
                except Exception as e:
                    logger.debug(f"解析租房房源失败: {e}")
                    continue

            logger.info(f"提取 {len(data)} 条有效租房房源")
            return data
        except Exception as e:
            logger.error(f"提取失败: {e}")
            return data
