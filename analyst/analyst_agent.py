"""
Claude API 集成：调用 Anthropic SDK 进行分析，并解析回复中的 kb-update JSON。
"""

import json
import re
import logging
from anthropic import Anthropic

logger = logging.getLogger(__name__)


class AnalystAgent:
    """封装 Claude API 调用和分析响应解析。"""

    def __init__(self, api_key: str, model: str = "claude-sonnet-4-20250514",
                 base_url: str = None):
        if not api_key:
            raise ValueError("MIMO_KEY 未设置，请在 .env 中配置")
        kwargs = {"api_key": api_key}
        if base_url:
            kwargs["base_url"] = base_url
        self.client = Anthropic(**kwargs)
        self.model = model

    def analyze(self, system_prompt: str, user_prompt: str, max_tokens: int = 4096,
                thinking: bool = True, temperature: float = 1.0) -> str:
        """调用 Claude API 进行分析，返回 Markdown 文本。"""
        logger.info(f"调用 Claude API: model={self.model}, max_tokens={max_tokens}, "
                     f"thinking={thinking}, temperature={temperature}")

        kwargs = {
            "model": self.model,
            "max_tokens": max_tokens,
            "system": system_prompt,
            "messages": [{"role": "user", "content": user_prompt}],
        }
        if thinking:
            kwargs["thinking"] = {"type": "enabled"}
        if temperature is not None:
            kwargs["temperature"] = temperature

        response = self.client.messages.create(**kwargs)

        text = response.content[0].text
        usage = response.usage
        logger.info(
            f"API 调用完成: input={usage.input_tokens}, output={usage.output_tokens}, "
            f"cache_read={getattr(usage, 'cache_read_input_tokens', 0)}"
        )
        return text

    def extract_kb_update(self, response_text: str) -> dict:
        """从 Claude 回复中提取 kb-update JSON 代码块。"""
        match = re.search(r'```kb-update\s*\n(.*?)\n```', response_text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(1))
            except json.JSONDecodeError as e:
                logger.warning(f"kb-update JSON 解析失败: {e}")
        else:
            logger.info("回复中未找到 kb-update 块")
        return {}
