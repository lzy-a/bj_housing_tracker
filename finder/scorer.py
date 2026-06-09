"""
多模态评分器 — 把房源文字信息 + 照片发给 Claude Vision，返回多维度评分。
"""
import base64
import json
import logging
import sys
from pathlib import Path

from anthropic import Anthropic

sys.path.insert(0, str(Path(__file__).parent.parent))
from config.settings import (
    FINDER_API_KEY, FINDER_BASE_URL, FINDER_MODEL,
    FINDER_VISION_API_KEY, FINDER_VISION_BASE_URL, FINDER_VISION_MODEL,
)

logger = logging.getLogger(__name__)

SCORING_DIMENSIONS = ['装修硬件', '居住体验', '性价比', '推荐指数', '通勤']

SYSTEM_PROMPT = """你是一个专业的租房顾问。用户会给你一套待租房源的文字描述、房间照片，以及该区域的市场参考数据（含通勤时间）。

## 第一步：描述你看到的
先看所有照片，逐张描述每个房间的情况：
- 这是什么房间
- 装修实际水平（不是文字标注的，是你亲眼看到的）
- 家电家具是否齐全
- 有没有明显问题（老旧、破损、脏乱、管道外露等）

**重要：区分真实照片和户型图**
- 户型图（floor plan）只展示房间布局，不反映实际装修状况
- 户型图上的家具示意图是画的，不是真实存在的
- 如果只有户型图没有实拍照片，装修硬件和居住体验无法判断，最高不超过4分

## 第二步：按以下标准打分（1-10 整数）

### 1. 装修硬件（基于照片，忽略文字标注）
- 9-10: 全新装修，品牌家电，现代设计
- 7-8:  干净整洁，装修较新，家电齐全
- 5-6:  普通装修，有使用痕迹，基本家电
- 3-4:  明显老旧，厨卫有污渍/管道外露
- 1-2:  严重老化/破损，不能直接入住
- 厨卫最能反映真实水平。只有户型图时最高4分。

### 2. 居住体验（采光+空间+楼层+朝向）
- 9-10: 采光极好，空间宽敞，中间楼层，南向
- 7-8:  采光良好，空间够用，楼层/朝向较好
- 5-6:  采光一般，空间紧凑，楼层/朝向中等
- 3-4:  采光差/空间局促/低楼层/朝北
- 1-2:  无采光/顶层无电梯/严重噪音
- 朝向: 南 > 东南 > 西南 > 东 > 西 > 北
- 楼层: 中间层(总楼层×0.3~0.7) > 低层 > 顶层

### 3. 性价比（价格+通勤 综合判断）
性价比不只是"便宜"，而是"这个价格拿到这些条件值不值"。
- 参考数据告诉你同小区同户型的单价（元/㎡/月）和通勤时间
- 同样价格，通勤25min的比50min的性价比高
- 同样通勤，价格低的性价比高
- 装修好的可以接受稍贵的价格
- 综合考虑价格、通勤、装修来判断

### 4. 通勤（基于参考数据中的通勤时间）
- 9-10: ≤15分钟
- 7-8:  15-25分钟
- 5-6:  25-35分钟
- 3-4:  35-50分钟
- 1-2:  50分钟以上

### 5. 推荐指数（最终判断）
综合装修、居住体验、性价比、通勤，判断值不值得实地看房。

## 输出格式
先输出照片描述（2-3句话），再输出评分 JSON：
```json
{"scores": {"装修硬件": 7, "居住体验": 6, "性价比": 8, "通勤": 7, "推荐指数": 7}, "summary": "总结"}
```"""


class RentalScorer:
    def __init__(self, api_key=None, model=None, base_url=None):
        # 默认客户端（纯文字也能用）
        self.client = Anthropic(
            api_key=api_key or FINDER_API_KEY,
            base_url=base_url or FINDER_BASE_URL,
        )
        self.model = model or FINDER_MODEL

        # 视觉客户端（单独配置，支持多模态）
        vision_key = FINDER_VISION_API_KEY or api_key or FINDER_API_KEY
        vision_url = FINDER_VISION_BASE_URL or base_url or FINDER_BASE_URL
        vision_model = FINDER_VISION_MODEL or model or FINDER_MODEL

        if FINDER_VISION_API_KEY or FINDER_VISION_BASE_URL or FINDER_VISION_MODEL:
            self.vision_client = Anthropic(api_key=vision_key, base_url=vision_url)
            self.vision_model = vision_model
        else:
            self.vision_client = self.client
            self.vision_model = self.model

    def score_listing(self, listing: dict, image_paths: list,
                      market_data: dict = None, commute_data: dict = None) -> dict:
        """对一个房源评分。返回 scores, summary, raw_input, raw_output。"""
        text = self._build_text(listing, market_data, commute_data)
        n_photos = len(image_paths) if image_paths else 0
        raw_input = f"[文字信息]\n{text}\n\n[照片] {n_photos} 张"

        if image_paths:
            image_blocks = self._build_image_blocks(image_paths)
            if image_blocks:
                try:
                    result = self._call_api(text, image_blocks, listing.get('house_id'))
                    result['raw_input'] = raw_input
                    return result
                except Exception as e:
                    logger.warning(f"图片评分失败，降级纯文字: {e}")

        result = self._call_api(text, [], listing.get('house_id'))
        result['raw_input'] = raw_input
        return result

    def _call_api(self, text: str, image_blocks: list, house_id: str) -> dict:
        content = [{"type": "text", "text": text}] + image_blocks
        client = self.vision_client if image_blocks else self.client
        model = self.vision_model if image_blocks else self.model
        response = client.messages.create(
            model=model,
            max_tokens=8192,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": content}],
        )
        # 取 TextBlock，跳过 ThinkingBlock
        raw = ''
        for block in response.content:
            if hasattr(block, 'text'):
                raw = block.text
                break
        logger.info(f"评分完成: house_id={house_id}, blocks={len(response.content)}, "
                     f"raw_len={len(raw)}, "
                     f"input={response.usage.input_tokens}, output={response.usage.output_tokens}")
        if not raw:
            logger.warning(f"house_id={house_id}: 模型返回空内容, block types={[type(b).__name__ for b in response.content]}")
        result = self._parse_response(raw)
        result['raw_output'] = raw
        if not any(v > 0 for v in result['scores'].values()):
            logger.warning(f"house_id={house_id}: 解析后全0, raw={raw[:200]}")
        return result

    @staticmethod
    def _build_text(listing: dict, market_data: dict = None, commute_data: dict = None) -> str:
        parts = [
            f"房源信息：",
            f"- 小区: {listing.get('community', '')}",
            f"- 区域: {listing.get('region', '')} · {listing.get('biz_circle', '')}",
            f"- 户型: {listing.get('layout', '')}",
            f"- 面积: {listing.get('area', '')}㎡",
            f"- 租金: {listing.get('rent_price', '')}元/月",
            f"- 租型: {listing.get('rent_type', '')}",
            f"- 朝向: {listing.get('orientation', '')}",
            f"- 装修: {listing.get('decoration', '')}",
            f"- 楼层: {listing.get('floor_info', '')}",
        ]
        if market_data:
            avg_unit = market_data.get('avg_unit_price', 0)
            price = listing.get('rent_price', 0)
            area = listing.get('area', 0)
            if avg_unit > 0 and price > 0 and area > 0:
                my_unit = price / area
                diff_pct = (my_unit - avg_unit) / avg_unit * 100
                diff_str = f"低于均价{abs(diff_pct):.0f}%" if diff_pct < 0 else f"高于均价{diff_pct:.0f}%"
                parts.append(f"\n市场参考数据（同小区同户型）：")
                parts.append(f"- 平均单价: {avg_unit:.1f}元/㎡/月")
                parts.append(f"- 平均总价: {market_data.get('avg_price', 0):.0f}元/月")
                parts.append(f"- 平均面积: {market_data.get('avg_area', 0):.1f}㎡")
                parts.append(f"- 当前房源单价: {my_unit:.1f}元/㎡/月")
                parts.append(f"- 对比: {diff_str}")
                parts.append(f"- 参考样本: {market_data.get('count', '?')}套")

        if commute_data:
            parts.append(f"\n通勤信息（到19号线太平桥站）：")
            parts.append(f"- 公交+步行: {commute_data['transit_minutes']}分钟")
        return '\n'.join(parts)

    @staticmethod
    def _build_image_blocks(image_paths: list) -> list:
        blocks = []
        for path in image_paths[:10]:  # 最多 10 张
            p = Path(path)
            if not p.exists():
                continue
            data = p.read_bytes()
            # 跳过过大的图片
            if len(data) > 5 * 1024 * 1024:
                continue
            b64 = base64.standard_b64encode(data).decode()
            media_type = 'image/jpeg'
            if p.suffix.lower() == '.png':
                media_type = 'image/png'
            elif p.suffix.lower() == '.webp':
                media_type = 'image/webp'
            blocks.append({
                "type": "image",
                "source": {"type": "base64", "media_type": media_type, "data": b64},
            })
        return blocks

    @staticmethod
    def _parse_response(raw: str) -> dict:
        """从模型回复中解析 JSON 评分。"""
        import re

        # 尝试提取含 "scores" 的 JSON 块
        # 策略：找所有 {...} 块，选含 "scores" 的那个
        candidates = []
        for m in re.finditer(r'\{[^{}]*"scores"[^{}]*\{[^}]*\}[^}]*\}', raw, re.DOTALL):
            candidates.append(m.group())
        for m in re.finditer(r'\{[^{}]*\}', raw):
            if '"scores"' in m.group():
                candidates.append(m.group())

        raw_json = ''
        if candidates:
            raw_json = candidates[0]
        else:
            # fallback: 找最后一个完整的 JSON 对象
            depth = 0
            start = -1
            for i, ch in enumerate(raw):
                if ch == '{':
                    if depth == 0:
                        start = i
                    depth += 1
                elif ch == '}':
                    depth -= 1
                    if depth == 0 and start != -1:
                        raw_json = raw[start:i+1]

        try:
            data = json.loads(raw_json)
            scores = data.get('scores', {})
            valid_scores = {}
            for dim in SCORING_DIMENSIONS:
                val = scores.get(dim, 0)
                if isinstance(val, (int, float)) and 0 <= val <= 10:
                    valid_scores[dim] = int(val)
                else:
                    valid_scores[dim] = 0
            return {
                'scores': valid_scores,
                'summary': data.get('summary', ''),
            }
        except (json.JSONDecodeError, ValueError) as e:
            logger.warning(f"JSON 解析失败: {e}\n原始回复: {raw[:300]}")
            return {'scores': {d: 0 for d in SCORING_DIMENSIONS}, 'summary': raw[:200]}
