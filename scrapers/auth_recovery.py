#!/usr/bin/env python3
"""多标签爬虫的共享登录恢复协调器。"""
import asyncio


def is_login_url(url):
    """判断页面是否被重定向到登录页。"""
    return "/user/login" in (url or "")


class AuthRecoveryCoordinator:
    """确保多个标签在会话失效时只执行一次重新登录。"""

    def __init__(self, login_callback, logger, label=""):
        self._login_callback = login_callback
        self._logger = logger
        self._label = label
        self._lock = asyncio.Lock()
        self._ready = asyncio.Event()
        self._ready.set()
        self._generation = 0
        self.reauth_count = 0

    async def navigate(self, page, url, page_label, max_attempts=4):
        """导航到目标页；遇到登录重定向时协调一次重登并重试原页。"""
        for attempt in range(max_attempts):
            await self._ready.wait()
            observed_generation = self._generation
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)

            if not is_login_url(page.url):
                return True

            self._logger.warning(
                f"{self._label}检测到登录重定向 {page_label}，暂停并发任务并恢复登录"
            )
            await self._recover(observed_generation)

            if attempt + 1 < max_attempts:
                self._logger.info(f"{self._label}登录恢复完成，重试 {page_label}")

        self._logger.error(f"{self._label}登录恢复后仍被重定向 {page_label}")
        return False

    async def recover_after_redirect(self, page, url, page_label):
        """处理导航后续阶段才发生的登录重定向。"""
        if not is_login_url(page.url):
            return True
        return await self.navigate(page, url, page_label)

    async def _recover(self, observed_generation):
        # 先阻止尚未开始下一次导航的标签继续请求。
        self._ready.clear()
        try:
            async with self._lock:
                if self._generation != observed_generation:
                    return

                await self._login_callback()
                self._generation += 1
                self.reauth_count += 1
                self._logger.info(
                    f"{self._label}共享登录已刷新（第 {self.reauth_count} 次）"
                )
        finally:
            self._ready.set()
