"""
详情页爬虫 — 访问 5i5j 租房详情页/小区列表页，提取照片和房源信息。
独立于批量爬虫，需要启用图片的 Chrome 实例。
"""
import asyncio
import logging
import random
import re
import sys
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

sys.path.insert(0, str(Path(__file__).parent.parent))
from config.settings import (
    CHROME_DEBUG_PORT, I5I5J_PHONE, I5I5J_PHONE_2, I5I5J_PASSWORD,
)

logger = logging.getLogger(__name__)


class RentalDetailScraper:
    """5i5j 租房详情页 + 小区列表页爬虫"""

    def __init__(self, debug_port=None):
        self.debug_port = debug_port or CHROME_DEBUG_PORT
        self.playwright = None
        self.browser = None
        self.context = None

    async def connect(self):
        """连接到已有 Chrome 实例"""
        self.playwright = await async_playwright().start()
        r = requests.get(f"http://localhost:{self.debug_port}/json/version", timeout=5)
        r.raise_for_status()
        ws_url = r.json()['webSocketDebuggerUrl']
        self.browser = await self.playwright.chromium.connect_over_cdp(ws_url)
        self.context = self.browser.contexts[0] if self.browser.contexts else await self.browser.new_context()
        logger.info("详情页爬虫已连接 Chrome")
        return self

    async def close(self):
        if self.playwright:
            await self.playwright.stop()

    async def check_and_login(self):
        """检查登录状态，需要则自动登录"""
        page = await self.context.new_page()
        try:
            await page.goto("https://bj.5i5j.com/zufang/xichengqu/n1/",
                            wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(2)

            try:
                body_text = await page.inner_text('body')
                if '点击页面或移动鼠标' in body_text:
                    await page.mouse.move(random.randint(100, 500), random.randint(100, 400))
                    await asyncio.sleep(0.3)
                    await page.mouse.click(random.randint(100, 500), random.randint(100, 400))
                    await page.wait_for_load_state('networkidle', timeout=15000)
                    await asyncio.sleep(2)
            except Exception:
                pass

            try:
                login_tab = await page.wait_for_selector(
                    '.login-tab[data-type="password-login"]', timeout=3000)
            except Exception:
                login_tab = None

            if login_tab:
                logger.info("需要登录")
                await login_tab.click()
                await page.wait_for_selector('#phone1', timeout=5000)
                await page.fill('#phone1', random.choice([I5I5J_PHONE, I5I5J_PHONE_2]))
                await page.fill('.password', I5I5J_PASSWORD)
                btn = await page.wait_for_selector('#login-submit', timeout=3000)
                await btn.click()
                try:
                    await page.wait_for_selector('ul.pList', timeout=15000)
                except Exception:
                    pass
                logger.info("登录完成")
            else:
                logger.info("已登录")
        finally:
            await page.close()

    # ── 小区列表页 ──

    @staticmethod
    def build_community_url(community_id: str, filters: dict = None) -> str:
        """拼接带筛选参数的小区列表页 URL。
        filters: {'max_price': '5000', 'layout': '1室', 'min_price': '0'}
        URL 格式: /zufang/{cid}/b{min}e{max}r{room}/
        """
        base = f"https://bj.5i5j.com/zufang/{community_id}/"
        if not filters:
            return base

        parts = []
        min_p = filters.get('min_price', '0')
        max_p = filters.get('max_price', '')
        layout = filters.get('layout', '')

        # 价格: b{min}e{max}
        if max_p:
            parts.append(f"b{min_p}e{int(float(max_p))}")

        # 户型: r{N}（从 "1室" 提取数字）
        if layout:
            import re
            m = re.search(r'(\d+)', layout)
            if m:
                parts.append(f"r{m.group(1)}")

        if parts:
            return base + ''.join(parts) + '/'
        return base

    async def scrape_community(self, community_id: str, filters: dict = None, max_pages: int = 5) -> list:
        """爬取小区租房列表页，支持翻页和 URL 参数筛选。"""
        base_url = self.build_community_url(community_id, filters)
        all_listings = []
        seen_ids = set()

        page = await self.context.new_page()
        try:
            for page_num in range(1, max_pages + 1):
                if page_num == 1:
                    url = base_url
                else:
                    # 翻页: /zufang/90760/b0e5000r1/n2/
                    url = base_url.rstrip('/') + f'n{page_num}/'

                logger.info(f"访问: {url}")
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                await asyncio.sleep(random.uniform(1, 2))

                # 模拟鼠标（预防反爬）
                await self._human_like_mouse(page)

                # 反爬验证
                for _ in range(3):
                    try:
                        body_text = await page.inner_text('body')
                        if '点击页面或移动鼠标' not in body_text:
                            break
                        logger.info(f"  反爬验证，点击通过...")
                        x, y = random.randint(200, 600), random.randint(200, 400)
                        await page.mouse.move(x, y, steps=10)
                        await asyncio.sleep(random.uniform(0.5, 1))
                        await page.mouse.click(x, y)
                        await asyncio.sleep(random.uniform(1, 2))
                    except Exception:
                        break

                if 'login' in page.url:
                    await self.check_and_login()
                    await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    await asyncio.sleep(2)

                html = await page.content()
                page_listings = self._parse_community_listings(html)

                if not page_listings:
                    break

                # 去重
                new_count = 0
                for l in page_listings:
                    if l['house_id'] not in seen_ids:
                        seen_ids.add(l['house_id'])
                        all_listings.append(l)
                        new_count += 1

                logger.info(f"  第{page_num}页: {len(page_listings)} 条, 新增 {new_count} 条")

                if new_count == 0:
                    break

                await asyncio.sleep(random.uniform(0.5, 1))

            return all_listings
        except Exception as e:
            logger.error(f"爬取小区列表失败 {community_id}: {e}")
            return all_listings
        finally:
            await page.close()

    @staticmethod
    def _parse_community_listings(html: str) -> list:
        """解析小区列表页 HTML，提取房源基本信息。复用现有列表页解析逻辑。"""
        soup = BeautifulSoup(html, 'lxml')
        data = []

        house_list = soup.find('ul', {'class': 'pList'})
        if not house_list:
            return data

        for item in house_list.find_all('li', recursive=False):
            try:
                if item.get('style') == 'display:none;':
                    continue
                if item.find('div', {'class': 'tag-now'}):
                    continue

                list_con = item.find('div', {'class': 'listCon'})
                if not list_con:
                    continue

                # house_id
                title_tag = list_con.find(['h2', 'h3'], class_='listTit')
                if not title_tag:
                    continue
                title_link = title_tag.find('a')
                if not title_link:
                    continue
                href = title_link.get('href', '')
                m = re.search(r'/zufang/(\d+)\.html', href)
                if not m:
                    continue
                house_id = m.group(1)
                title = title_link.get_text(strip=True)

                list_x = list_con.find('div', {'class': 'listX'})
                if not list_x:
                    continue

                # 户型/面积/朝向/楼层/装修
                first_p = list_x.find('p', text=lambda t: t and '室' in t)
                if not first_p:
                    i01 = list_x.find('i', {'class': 'i_01'})
                    if i01:
                        first_p = i01.find_parent('p')
                    else:
                        continue

                info_text = first_p.get_text(strip=True)
                info_parts = [p.strip() for p in info_text.split('·')]
                if any('车位' in p for p in info_parts):
                    continue

                layout, area, orientation, floor, decoration = '未知', None, '未知', '未知', '未知'
                for part in info_parts:
                    if ('室' in part or '房间' in part) and '地下' not in part:
                        layout = part.replace(' ', '')
                    elif '平米' in part or '㎡' in part:
                        am = re.search(r'(\d+\.?\d*)', part)
                        if am:
                            try:
                                area = float(am.group(1))
                            except (ValueError, TypeError):
                                pass
                    elif any(d in part for d in ['东', '南', '西', '北']):
                        orientation = part
                    elif '层' in part:
                        floor = part
                    elif any(d in part for d in ['精装', '简装', '毛坯', '豪装', '中装']):
                        decoration = part

                # 商圈/小区
                biz_circle, community, community_id_val = '未知', '未知', None
                i02 = list_x.find('i', {'class': 'i_02'})
                if i02:
                    second_p = i02.find_parent('p')
                    if second_p:
                        cl = second_p.find('a')
                        if cl:
                            community = cl.get_text(strip=True)
                            mc = re.search(r'/xiaoqu/(\d+)\.html', cl.get('href', ''))
                            if mc:
                                community_id_val = mc.group(1)

                # 价格
                jia_div = list_x.find('div', {'class': 'jia'})
                if not jia_div:
                    continue
                rent_price = None
                rent_type = '整租'
                jia_text = jia_div.get_text(strip=True)
                type_m = re.search(r'出租方式[：:]\s*(.+)', jia_text)
                if type_m and '合' in type_m.group(1):
                    rent_type = '合租'
                strong = jia_div.find('strong')
                if strong:
                    try:
                        rent_price = float(strong.get_text(strip=True).replace(',', ''))
                    except (ValueError, TypeError):
                        pass
                if rent_price is None:
                    pm = re.search(r'(\d+)\s*元/月', jia_text)
                    if pm:
                        rent_price = float(pm.group(1))

                if rent_price is None or area is None or area <= 0:
                    continue

                data.append({
                    'house_id': house_id,
                    'title': title,
                    'region': '',
                    'biz_circle': biz_circle,
                    'community': community,
                    'community_id': community_id_val,
                    'layout': layout,
                    'area': area,
                    'rent_price': rent_price,
                    'rent_type': rent_type,
                    'orientation': orientation,
                    'decoration': decoration,
                    'floor_info': floor,
                })
            except Exception as e:
                logger.debug(f"解析房源失败: {e}")
                continue

        return data

    # ── 详情页 ──

    async def _human_like_mouse(self, page):
        """模拟人类鼠标行为，预防反爬。"""
        try:
            # 随机移动几次
            for _ in range(random.randint(2, 4)):
                x, y = random.randint(100, 800), random.randint(100, 600)
                await page.mouse.move(x, y, steps=random.randint(5, 15))
                await asyncio.sleep(random.uniform(0.1, 0.3))
            # 随机滚动
            await page.mouse.wheel(0, random.randint(-100, 100))
            await asyncio.sleep(random.uniform(0.2, 0.5))
        except Exception:
            pass

    async def scrape_detail(self, house_id: str, retries: int = 2) -> dict:
        """爬取单个房源详情页，返回照片列表和附加信息。"""
        url = f"https://bj.5i5j.com/zufang/{house_id}.html"

        for attempt in range(retries):
            page = await self.context.new_page()
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                await asyncio.sleep(random.uniform(2, 3))

                # about:blank 检测
                if page.url == 'about:blank' or not page.url.startswith('http'):
                    logger.info(f"  {house_id} 页面未加载，重试")
                    await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    await asyncio.sleep(random.uniform(2, 3))
                    if not page.url.startswith('http'):
                        return {'house_id': house_id, 'photos': [], 'facilities': [], 'description': ''}

                # 模拟鼠标（不管有没有反爬都做）
                await self._human_like_mouse(page)

                # 反爬验证 — 检测到了就多点几次
                for _ in range(3):
                    try:
                        body_text = await page.inner_text('body')
                        if '点击页面或移动鼠标' not in body_text:
                            break
                        logger.info(f"  {house_id} 反爬验证，点击通过...")
                        x, y = random.randint(200, 600), random.randint(200, 400)
                        await page.mouse.move(x, y, steps=10)
                        await asyncio.sleep(random.uniform(0.5, 1))
                        await page.mouse.click(x, y)
                        await asyncio.sleep(random.uniform(1, 2))
                        try:
                            await page.wait_for_load_state('networkidle', timeout=10000)
                        except Exception:
                            pass
                    except Exception:
                        break

                # 登录页
                if 'login' in page.url:
                    await self.check_and_login()
                    await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    await asyncio.sleep(2)

                # 空白页检测 — 刷新无用，直接跳过
                body_text = await page.inner_text('body')
                if len(body_text.strip()) < 100:
                    logger.warning(f"  {house_id} 空白页，跳过")
                    return {'house_id': house_id, 'photos': [], 'facilities': [], 'description': ''}

                html = await page.content()
                result = self._parse_detail(house_id, html)

                if result.get('photos'):
                    return result

                # 没照片，重新加载页面（不用 reload，用 goto）
                if attempt < retries - 1:
                    await asyncio.sleep(random.uniform(3, 4))
                    await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    await asyncio.sleep(random.uniform(3, 4))
                    await self._human_like_mouse(page)
                    html = await page.content()
                    result = self._parse_detail(house_id, html)
                    if result.get('photos'):
                        return result

            except Exception as e:
                logger.error(f"详情页失败 {house_id} ({attempt+1}/{retries}): {e}")
            finally:
                await page.close()

        return {'house_id': house_id, 'photos': [], 'facilities': [], 'description': ''}

        return {'house_id': house_id, 'photos': [], 'facilities': [], 'description': ''}

    async def scrape_batch(self, house_ids: list, concurrency: int = 3) -> list:
        """批量爬取详情页"""
        results = []
        sem = asyncio.Semaphore(concurrency)

        async def _one(hid):
            async with sem:
                result = await self.scrape_detail(hid)
                results.append(result)
                await asyncio.sleep(random.uniform(0.5, 1.5))

        await asyncio.gather(*[_one(hid) for hid in house_ids])
        return results

    @staticmethod
    def _parse_detail(house_id: str, html: str) -> dict:
        """解析详情页 HTML，提取照片和附加信息。"""
        soup = BeautifulSoup(html, 'lxml')
        photos = []
        facilities = []
        description = ''

        seen_urls = set()
        big_slide = soup.find('div', class_='big-slide')
        if big_slide:
            for img in big_slide.find_all('img'):
                src = img.get('src', '').strip()
                if not src or src in seen_urls:
                    continue
                seen_urls.add(src)
                alt = img.get('alt', '')
                room_type = _classify_room(alt, src)
                photos.append({'photo_url': src, 'room_type': room_type})

        if not photos:
            small_con = soup.find('div', class_='small-con')
            if small_con:
                for img in small_con.find_all('img'):
                    src = img.get('src', '').strip()
                    if not src or src in seen_urls:
                        continue
                    seen_urls.add(src)
                    alt = img.get('alt', '')
                    room_type = _classify_room(alt, src)
                    photos.append({'photo_url': src, 'room_type': room_type})

        detail_main = soup.find('div', class_='detail-main')
        if detail_main:
            for li in detail_main.find_all('li'):
                text = li.get_text(strip=True)
                if text and len(text) < 20 and not any(c in text for c in '：:'):
                    facilities.append(text)

        info = soup.find('div', class_='infocontent')
        if info:
            description = info.get_text(strip=True)[:1000]

        return {
            'house_id': house_id,
            'photos': photos,
            'facilities': facilities,
            'description': description,
        }


def _classify_room(alt: str, url: str) -> str:
    alt_lower = alt.lower()
    url_lower = url.lower()
    if 'floorplan' in url_lower or '户型' in alt_lower:
        return 'floor_plan'
    for keyword, room_type in [
        ('卧室', 'bedroom'), ('床', 'bedroom'), ('主卧', 'bedroom'), ('次卧', 'bedroom'),
        ('客厅', 'living_room'), ('厅', 'living_room'),
        ('厨房', 'kitchen'), ('厨', 'kitchen'),
        ('卫生间', 'bathroom'), ('洗手间', 'bathroom'), ('浴室', 'bathroom'),
        ('阳台', 'balcony'), ('外景', 'exterior'), ('小区', 'exterior'),
    ]:
        if keyword in alt:
            return room_type
    return 'other'
