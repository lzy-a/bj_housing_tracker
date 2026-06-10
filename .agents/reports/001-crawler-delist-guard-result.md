# Task 001 Result: Crawler Delist Guard

## Status: SUBMITTED_FOR_REVIEW

## Changed Files

1. **`run_crawler_playwright.py`**
2. **`run_crawler_rent.py`**

## Behavior Summary

### Page Outcome Contract

| Outcome | Meaning | Effect on region |
|---------|---------|-----------------|
| `success` | Listings extracted normally (including recovered click challenge) | Reset no-data counter |
| `no_data` | Known empty page (`.n_no_data`, `.no-result`, `.empty-tip`) | Increment no-data counter |
| `suspicious` | Hard failure (see below) | Set `region_suspicious = True` |

### Click Challenge Handling

Click verification (`点击页面或移动鼠标`) is normal site behavior. Two paths:

1. **Recovered**: Click → `ul.pList` appears → parse listings → `success`. Does NOT block delisting.
2. **Unrecovered**: Click → `ul.pList` does NOT appear → `suspicious`. Blocks delisting for the region.

### Hard Suspicious Reasons (block delisting)

- Unrecovered click challenge
- Login prompt
- Unknown empty page (no `pList` and no known no-data selector)
- Page navigation exception
- Parser/worker exception

### Region Outcome Logic

```
region_ok = is_region_finished AND NOT region_suspicious
```

- `is_region_finished`: 3 consecutive no-data pages (normal pagination end)
- `region_suspicious`: any hard failure seen during crawl

### Consumer Behavior

- Tracks `region_outcomes` dict from control messages
- Only marks disappeared listings for regions with `ok=True`
- Skipped regions logged: `⏭️ {region}: 跳过下架标记 (reason=suspicious_pages)`

## Commands Run

```bash
python -m py_compile run_crawler_playwright.py  # PASS
python -m py_compile run_crawler_rent.py        # PASS
```

## Questions for Codex

None.
