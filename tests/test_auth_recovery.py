import asyncio
import logging
import unittest

from scrapers.auth_recovery import AuthRecoveryCoordinator


class FakePage:
    def __init__(self, auth_state):
        self.auth_state = auth_state
        self.url = ""
        self.goto_count = 0

    async def goto(self, url, **kwargs):
        self.goto_count += 1
        if self.auth_state["logged_in"]:
            self.url = url
        else:
            self.url = f"https://bj.5i5j.com/user/login/?preUrl={url}"


class AuthRecoveryCoordinatorTest(unittest.IsolatedAsyncioTestCase):
    async def test_concurrent_redirects_trigger_one_login_and_retry_all_pages(self):
        auth_state = {"logged_in": False, "login_count": 0}

        async def login():
            await asyncio.sleep(0.01)
            auth_state["login_count"] += 1
            auth_state["logged_in"] = True

        coordinator = AuthRecoveryCoordinator(
            login, logging.getLogger(__name__), label="[测试] "
        )
        pages = [FakePage(auth_state) for _ in range(5)]
        urls = [
            f"https://bj.5i5j.com/ershoufang/shijingshanqu/n{i}/"
            for i in range(1, 6)
        ]

        results = await asyncio.gather(
            *(
                coordinator.navigate(page, url, f"P{i}")
                for i, (page, url) in enumerate(zip(pages, urls), start=1)
            )
        )

        self.assertEqual(results, [True] * 5)
        self.assertEqual(auth_state["login_count"], 1)
        self.assertEqual(coordinator.reauth_count, 1)
        self.assertTrue(all(page.url == url for page, url in zip(pages, urls)))
        self.assertEqual(sum(page.goto_count for page in pages), 6)
        self.assertEqual(sorted(page.goto_count for page in pages), [1, 1, 1, 1, 2])


if __name__ == "__main__":
    unittest.main()
