# 爬虫页面标准流程

每次写新爬虫，访问页面时必须按顺序检查以下内容：

## 必检项（按顺序）

```python
async def visit_page(page, url):
    # 1. 访问页面
    await page.goto(url, wait_until="domcontentloaded", timeout=30000)
    await asyncio.sleep(random.uniform(1, 2))

    # 2. 反爬验证 — "点击页面或移动鼠标"
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

    # 3. 登录重定向 — URL 包含 'login'
    if 'login' in page.url:
        await self.check_and_login()
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(2)

    # 4. 空白页检测 — body 内容太少
    body_text = await page.inner_text('body')
    if len(body_text.strip()) < 100:
        # 空白页，刷新一次
        await page.reload(wait_until="domcontentloaded", timeout=20000)
        await asyncio.sleep(random.uniform(2, 3))
        body_text = await page.inner_text('body')
        if len(body_text.strip()) < 100:
            return None  # 仍然空白，跳过

    # 5. 图片加载等待（详情页需要）
    await asyncio.sleep(random.uniform(2, 3))

    return await page.content()
```

## 各场景适用项

| 场景 | 反爬 | 登录 | 空白页 | 图片等待 | 重试 |
|------|------|------|--------|----------|------|
| 小区列表页 | ✓ | ✓ | ✓ | - | 翻页时每页都检查 |
| 详情页 | ✓ | ✓ | ✓ | ✓ | 无照片时重试1次 |
| 搜索结果页 | ✓ | ✓ | ✓ | - | - |

## 注意事项

- 反爬验证放在登录检测之前（有些页面反爬会重定向到验证页）
- 空白页刷新没用，直接跳过
- 详情页图片加载需要等 2-3 秒
- 无照片时重试一次（刷新 + 等 3-4 秒），仍没有就放弃
- 每个新页面都要用这个流程，不要跳过任何步骤
