"""
项目分析师 SQL 查询库。
所有查询参考 Metabase 看板已验证的 SQL 模式，适配 Python 直连执行。
"""

# ================================================================
# A. 区域大盘
# ================================================================

# A1: 最新一日各区快照（参考 Card 43）
DISTRICT_LATEST_SNAPSHOT = """
SELECT region, total_listings,
       ROUND(avg_unit_price::numeric, 0) AS avg_unit_price,
       ROUND(median_unit_price::numeric, 0) AS median_unit_price,
       ROUND(weighted_avg_price::numeric, 0) AS weighted_avg_price
FROM district_snapshots
WHERE record_date = (SELECT MAX(record_date) FROM district_snapshots)
ORDER BY region
"""

# A2: 各区环比变化（近3日均值 vs 7-9天前均值的 MA3 对比）
DISTRICT_WOW_CHANGE = """
WITH all_dates AS (
    SELECT DISTINCT record_date
    FROM district_snapshots
    ORDER BY record_date DESC
    LIMIT 12
),
ranked AS (
    SELECT ds.record_date, ds.region, ds.total_listings, ds.median_unit_price,
           ROW_NUMBER() OVER (ORDER BY ds.record_date DESC) AS rn
    FROM district_snapshots ds
    JOIN all_dates ad ON ds.record_date = ad.record_date
),
current_window AS (
    SELECT region,
           ROUND(AVG(median_unit_price)::numeric, 0) AS current_price_ma3,
           ROUND(AVG(total_listings)::numeric, 0) AS current_listings_ma3
    FROM ranked WHERE rn BETWEEN 1 AND 3
    GROUP BY region
),
week_ago_window AS (
    SELECT region,
           ROUND(AVG(median_unit_price)::numeric, 0) AS week_ago_price_ma3,
           ROUND(AVG(total_listings)::numeric, 0) AS week_ago_listings_ma3
    FROM ranked WHERE rn BETWEEN 8 AND 10
    GROUP BY region
)
SELECT c.region, c.current_price_ma3, c.current_listings_ma3,
       w.week_ago_price_ma3, w.week_ago_listings_ma3,
       ROUND(((c.current_price_ma3 - w.week_ago_price_ma3) / NULLIF(w.week_ago_price_ma3, 0) * 100)::numeric, 2) AS price_wow_pct,
       ROUND(((c.current_listings_ma3 - w.week_ago_listings_ma3) / NULLIF(w.week_ago_listings_ma3, 0) * 100)::numeric, 2) AS listings_wow_pct
FROM current_window c
JOIN week_ago_window w ON c.region = w.region
ORDER BY c.region
"""

# ================================================================
# B. 调价分析
# ================================================================

# B1: 今日调价统计（参考 Card 42，补 diff<>0 过滤）
PRICE_ADJUSTMENTS_TODAY = """
SELECT v.region,
       COUNT(*) FILTER (WHERE v.diff > 0) AS price_increases,
       COUNT(*) FILTER (WHERE v.diff < 0) AS price_decreases,
       COUNT(*) AS total_adjustments
FROM v_house_price_changes v
WHERE v.record_date = (SELECT MAX(record_date) FROM v_house_price_changes)
  AND v.diff IS NOT NULL AND v.diff <> 0
GROUP BY v.region
ORDER BY v.region
"""

# B2: 近7天调价趋势（参考 Card 42，补 diff<>0 过滤）
PRICE_ADJUSTMENTS_7DAY = """
SELECT v.record_date,
       COUNT(*) FILTER (WHERE v.diff > 0) AS price_increases,
       COUNT(*) FILTER (WHERE v.diff < 0) AS price_decreases,
       COUNT(*) AS total_adjustments
FROM v_house_price_changes v
WHERE v.record_date >= CURRENT_DATE - INTERVAL '7 days'
  AND v.diff IS NOT NULL AND v.diff <> 0
GROUP BY v.record_date
ORDER BY v.record_date
"""

# ================================================================
# C. 供需变化（参考 Card 69 逻辑重写）
# ================================================================

# C1: 近30天供需变化 — 新增/下架房源
# 用 district_snapshots 日期做驱动表，按 region 关联 property_details
SUPPLY_DEMAND = """
WITH all_dates AS (
    SELECT DISTINCT record_date, region
    FROM district_snapshots
),
daily AS (
    SELECT
        ad.record_date,
        ad.region,
        COUNT(CASE WHEN pd.first_seen_date = ad.record_date THEN 1 END) AS new_cnt,
        COUNT(CASE WHEN pd.first_seen_date <= ad.record_date
                    AND (pd.status = 1 OR pd.last_seen_date >= ad.record_date)
              THEN 1 END) AS active_cnt
    FROM all_dates ad
    JOIN property_details pd ON pd.region = ad.region
    GROUP BY ad.record_date, ad.region
),
with_delisted AS (
    SELECT
        record_date, region, new_cnt,
        LAG(active_cnt) OVER (PARTITION BY region ORDER BY record_date) - active_cnt + new_cnt AS delisted_cnt
    FROM daily
)
SELECT record_date, region, new_cnt,
       COALESCE(GREATEST(delisted_cnt, 0), 0) AS delisted_cnt
FROM with_delisted
WHERE record_date > (SELECT MIN(record_date) FROM all_dates)
ORDER BY record_date DESC, region
LIMIT 30
"""

# ================================================================
# D. 租售比
# ================================================================

# D1: 各区租售比（基于当前活跃房源）
RENT_YIELD_BY_DISTRICT = """
SELECT s.region,
       ROUND(s.avg_unit_price::numeric, 0) AS avg_sale_price_psm,
       ROUND(r.avg_rent_psm::numeric, 2) AS avg_rent_psm,
       ROUND(((r.avg_rent_psm * 12) / NULLIF(s.avg_unit_price, 0) * 100)::numeric, 2) AS rent_yield_pct,
       ROUND((s.avg_unit_price / NULLIF(r.avg_rent_psm, 0))::numeric, 0) AS price_to_rent_ratio
FROM (
    SELECT region, AVG(unit_price) AS avg_unit_price
    FROM property_details WHERE status = 1 AND unit_price > 0
    GROUP BY region
) s
JOIN (
    SELECT region,
           AVG(rent_price / NULLIF(area, 0)) AS avg_rent_psm
    FROM rental_details
    WHERE status = 1 AND rent_price >= 100 AND rent_type = '整租' AND area > 0
    GROUP BY region
) r ON s.region = r.region
ORDER BY s.region
"""

# ================================================================
# E. 梯队指数（参考 Metabase Cards 60-63 重写）
# ================================================================

# E1: 四梯队周度价格指数
# Tier 定义对齐 Metabase：region + 价格阈值 + 小区/商圈白名单
TIERED_INDEX = """
WITH tier1 AS (
    SELECT DISTINCT house_id FROM property_details pd
    WHERE pd.status = 1
      AND layout NOT LIKE '%%车位%%' AND layout NOT LIKE '%%车库%%' AND layout NOT LIKE '%%地下%%'
      AND (
        (pd.region = '西城区' AND (pd.community IN ('中海凯旋', '西城晶华', '阳光丽景', '官苑八号', '丰侨公寓', '丰汇园小区')
            OR (pd.biz_circle IN ('金融街', '德胜', '月坛') AND pd.price >= 1800 AND pd.unit_price > 130000)))
        OR (pd.region = '海淀区' AND (pd.community IN ('万柳华府', '万城华府', '西山壹号院', '龙湖唐宁ONE', '葛洲坝紫郡兰园', '如园')
            OR (pd.biz_circle = '万柳' AND pd.price >= 1500)
            OR (pd.biz_circle = '四季青' AND pd.price >= 2500)))
        OR (pd.region = '朝阳区' AND (pd.community IN ('霄云路8号', '红玺台', '泛海国际', '棕榈泉国际公寓', '星河湾', '广渠金茂府')
            OR (pd.biz_circle IN ('朝阳公园', '太阳宫', '农展馆') AND pd.price >= 2500)
            OR (pd.biz_circle = '望京' AND pd.price >= 2000 AND pd.build_year >= 2010)))
        OR (pd.region = '东城区' AND (pd.community IN ('中海紫御公馆', '国瑞城西区', '霞公府', '缘溪堂')
            OR (pd.price >= 2000 AND pd.unit_price > 120000)))
      )
),
tier2 AS (
    SELECT DISTINCT house_id FROM property_details pd
    WHERE pd.status = 1 AND house_id NOT IN (SELECT house_id FROM tier1)
      AND layout NOT LIKE '%%车位%%' AND layout NOT LIKE '%%车库%%' AND layout NOT LIKE '%%地下%%'
      AND (
        (pd.region = '朝阳区' AND pd.biz_circle IN ('望京', '双井', '朝青', '太阳宫', '三元桥', '酒仙桥', '亚运村', '奥森'))
        OR (pd.region = '海淀区' AND pd.biz_circle IN ('上地', '知春路', '万寿路', '西北旺', '清河', '四季青', '五道口', '中关村'))
        OR (pd.region = '西城区' AND pd.biz_circle IN ('德胜', '月坛', '广安门', '马连道'))
        OR (pd.region = '东城区' AND pd.biz_circle IN ('崇文门', '东直门', '安定门'))
      )
),
tier3 AS (
    SELECT DISTINCT house_id FROM property_details pd
    WHERE pd.status = 1 AND house_id NOT IN (SELECT house_id FROM tier1) AND house_id NOT IN (SELECT house_id FROM tier2)
      AND layout NOT LIKE '%%车位%%' AND layout NOT LIKE '%%车库%%' AND layout NOT LIKE '%%地下%%'
      AND (
        (pd.region = '朝阳区' AND pd.biz_circle IN ('常营', '管庄', '垡头', '王四营', '十八里店', '四惠', '芍药居', '百子湾'))
        OR (pd.region = '丰台区' AND pd.biz_circle IN ('玉泉营', '角门', '马家堡', '丰台科技园', '看丹桥', '青塔'))
        OR (pd.region = '石景山区')
        OR (pd.region = '海淀区' AND pd.biz_circle IN ('西二旗', '西三旗', '回龙观'))
      )
),
tier4 AS (
    SELECT DISTINCT house_id FROM property_details pd
    WHERE pd.status = 1
      AND house_id NOT IN (SELECT house_id FROM tier1)
      AND house_id NOT IN (SELECT house_id FROM tier2)
      AND house_id NOT IN (SELECT house_id FROM tier3)
      AND layout NOT LIKE '%%车位%%' AND layout NOT LIKE '%%车库%%' AND layout NOT LIKE '%%地下%%'
),
all_tiers AS (
    SELECT house_id, '第一梯队' AS tier_name FROM tier1
    UNION ALL SELECT house_id, '第二梯队' FROM tier2
    UNION ALL SELECT house_id, '第三梯队' FROM tier3
    UNION ALL SELECT house_id, '第四梯队' FROM tier4
),
tier_daily AS (
    SELECT DATE(ph.record_date) AS record_date, t.tier_name,
           ROUND(AVG(ph.unit_price)) AS avg_unit_price,
           COUNT(DISTINCT ph.house_id) AS listing_count
    FROM price_history ph
    JOIN all_tiers t ON t.house_id = ph.house_id
    WHERE DATE(ph.record_date) >= CURRENT_DATE - INTERVAL '60 days'
    GROUP BY DATE(ph.record_date), t.tier_name
)
SELECT tier_name, record_date, avg_unit_price, listing_count,
       ROUND((avg_unit_price * 1.0 / FIRST_VALUE(avg_unit_price) OVER (PARTITION BY tier_name ORDER BY record_date) * 100)::numeric, 1) AS price_index
FROM tier_daily
ORDER BY tier_name, record_date
"""

# ================================================================
# F. 板块分析
# ================================================================

# F1: 板块抗跌排名（参考 Card 75：最近9日期 MA3 对比 + 租金回报率）
BIZ_CIRCLE_RESILIENCE = """
WITH all_dates AS (
    SELECT DISTINCT DATE(record_date) AS record_date FROM price_history
    ORDER BY record_date DESC LIMIT 9
),
biz_daily AS (
    SELECT d.record_date, pd.region, pd.biz_circle,
        ROUND(AVG(ph.unit_price)) AS avg_price,
        COUNT(DISTINCT pd.house_id) AS listing_count
    FROM all_dates d
    JOIN property_details pd ON 1=1
    LEFT JOIN price_history ph ON ph.house_id = pd.house_id AND DATE(ph.record_date) <= d.record_date
    WHERE pd.status = 1
      AND d.record_date BETWEEN DATE(pd.first_seen_date) AND DATE(pd.last_seen_date)
      AND ph.id = (
          SELECT id FROM price_history
          WHERE house_id = pd.house_id AND DATE(record_date) <= d.record_date
          ORDER BY record_date DESC LIMIT 1
      )
    GROUP BY d.record_date, pd.region, pd.biz_circle
),
with_rn AS (
    SELECT *, ROW_NUMBER() OVER (ORDER BY record_date DESC) AS date_rn FROM all_dates
),
ma3 AS (
    SELECT bd.region, bd.biz_circle,
        ROUND(AVG(CASE WHEN w.date_rn BETWEEN 1 AND 3 THEN bd.avg_price END)) AS current_ma3,
        ROUND(AVG(CASE WHEN w.date_rn BETWEEN 7 AND 9 THEN bd.avg_price END)) AS week_ago_ma3,
        MAX(bd.listing_count) FILTER (WHERE w.date_rn = 1) AS listing_count
    FROM biz_daily bd JOIN with_rn w ON bd.record_date = w.record_date
    GROUP BY bd.region, bd.biz_circle
),
rent_yield AS (
    SELECT biz_circle,
        ROUND(AVG(rent_price / NULLIF(area, 0))::numeric, 2) AS avg_rent_psm
    FROM rental_details
    WHERE status = 1 AND rent_price >= 100 AND rent_type = '整租'
    GROUP BY biz_circle
)
SELECT
    m.region, m.biz_circle,
    m.current_ma3, m.week_ago_ma3,
    ROUND(((m.current_ma3 - m.week_ago_ma3) / NULLIF(m.week_ago_ma3, 0) * 100)::numeric, 2) AS price_change_pct,
    ROUND((ry.avg_rent_psm * 12 / NULLIF(m.current_ma3, 0) * 100)::numeric, 2) AS rent_yield_pct,
    m.listing_count
FROM ma3 m
LEFT JOIN rent_yield ry ON m.biz_circle = ry.biz_circle
WHERE m.week_ago_ma3 IS NOT NULL AND m.week_ago_ma3 > 0
ORDER BY price_change_pct DESC
LIMIT 30
"""

# ================================================================
# H. 日报专项 —— 个体粒度数据
# ================================================================

# H1: 今日降价 TOP15（具体房源，非汇总）
TODAY_PRICE_DROPS = """
WITH latest_date AS (
    SELECT MAX(record_date) AS dt FROM price_history
),
today_price AS (
    SELECT ph.house_id, ph.price, ph.unit_price
    FROM price_history ph, latest_date ld
    WHERE ph.record_date = ld.dt
),
prev_price AS (
    SELECT DISTINCT ON (ph.house_id)
        ph.house_id, ph.price, ph.unit_price
    FROM price_history ph
    WHERE ph.record_date < (SELECT dt FROM latest_date)
    ORDER BY ph.house_id, ph.record_date DESC
)
SELECT pd.house_id, pd.title, pd.region, pd.biz_circle, pd.community,
       pd.layout, ROUND(pd.area::numeric, 1) AS area,
       ROUND(pp.price::numeric, 0) AS prev_price,
       ROUND(tp.price::numeric, 0) AS new_price,
       ROUND((tp.price - pp.price)::numeric, 0) AS change_amount,
       ROUND(((tp.price - pp.price) / NULLIF(pp.price, 0) * 100)::numeric, 1) AS change_pct,
       ROUND(tp.unit_price::numeric, 0) AS unit_price,
       pd.build_year,
       (CURRENT_DATE - pd.first_seen_date) AS days_on_market
FROM today_price tp
JOIN property_details pd ON pd.house_id = tp.house_id AND pd.status = 1
JOIN prev_price pp ON pp.house_id = tp.house_id
WHERE tp.price < pp.price
ORDER BY change_amount ASC
LIMIT 15
"""

# H2: 今日新上房源（按单价排序，挑高/中/低各几套有代表性的）
TODAY_NEW_LISTINGS = """
SELECT pd.house_id, pd.title, pd.region, pd.biz_circle, pd.community,
       pd.layout, ROUND(pd.area::numeric, 1) AS area,
       ROUND(pd.price::numeric, 0) AS price,
       ROUND(pd.unit_price::numeric, 0) AS unit_price,
       pd.build_year, pd.orientation, pd.decoration,
       pd.floor_info
FROM property_details pd
WHERE pd.first_seen_date = (SELECT MAX(first_seen_date) FROM property_details WHERE status = 1)
  AND pd.status = 1
  AND pd.unit_price > 0
ORDER BY pd.unit_price DESC
LIMIT 20
"""

# H3: 近14天调价脉搏（各区每天涨/跌占比 + 累计天数）
DAILY_PULSE = """
WITH daily AS (
    SELECT record_date, region,
           COUNT(*) FILTER (WHERE diff < 0) AS decreases,
           COUNT(*) FILTER (WHERE diff > 0) AS increases,
           COUNT(*) AS total
    FROM v_house_price_changes
    WHERE diff IS NOT NULL AND diff <> 0
      AND record_date >= CURRENT_DATE - INTERVAL '14 days'
    GROUP BY record_date, region
)
SELECT region,
       COUNT(*) AS active_days,
       COUNT(*) FILTER (WHERE decreases > increases) AS net_decrease_days,
       ROUND(AVG(decreases)::numeric, 0) AS avg_decreases,
       ROUND(AVG(increases)::numeric, 0) AS avg_increases,
       ROUND((AVG(decreases) / NULLIF(AVG(decreases) + AVG(increases), 0) * 100)::numeric, 1) AS decrease_ratio_pct
FROM daily
GROUP BY region
ORDER BY region
"""

# ================================================================
# G. 租赁市场
# ================================================================

# G1: 各区租赁最新快照
RENTAL_LATEST_SNAPSHOT = """
SELECT region, total_rentals,
       ROUND(avg_rent_price::numeric, 0) AS avg_rent_price,
       ROUND(median_rent_price::numeric, 0) AS median_rent_price,
       ROUND(avg_unit_rent::numeric, 2) AS avg_unit_rent
FROM district_rent_snapshots
WHERE record_date = (SELECT MAX(record_date) FROM district_rent_snapshots)
ORDER BY region
"""

# G2: 租赁市场周环比
RENTAL_WOW_CHANGE = """
WITH all_dates AS (
    SELECT DISTINCT record_date
    FROM district_rent_snapshots
    ORDER BY record_date DESC
    LIMIT 12
),
ranked AS (
    SELECT drs.record_date, drs.region, drs.total_rentals, drs.median_rent_price,
           ROW_NUMBER() OVER (ORDER BY drs.record_date DESC) AS rn
    FROM district_rent_snapshots drs
    JOIN all_dates ad ON drs.record_date = ad.record_date
),
current_window AS (
    SELECT region, ROUND(AVG(median_rent_price)::numeric, 0) AS current_rent_ma3,
           ROUND(AVG(total_rentals)::numeric, 0) AS current_rentals_ma3
    FROM ranked WHERE rn BETWEEN 1 AND 3 GROUP BY region
),
week_ago_window AS (
    SELECT region, ROUND(AVG(median_rent_price)::numeric, 0) AS week_ago_rent_ma3,
           ROUND(AVG(total_rentals)::numeric, 0) AS week_ago_rentals_ma3
    FROM ranked WHERE rn BETWEEN 8 AND 10 GROUP BY region
)
SELECT c.region, c.current_rent_ma3, c.current_rentals_ma3,
       w.week_ago_rent_ma3, w.week_ago_rentals_ma3,
       ROUND(((c.current_rent_ma3 - w.week_ago_rent_ma3) / NULLIF(w.week_ago_rent_ma3, 0) * 100)::numeric, 2) AS rent_wow_pct,
       ROUND(((c.current_rentals_ma3 - w.week_ago_rentals_ma3) / NULLIF(w.week_ago_rentals_ma3, 0) * 100)::numeric, 2) AS rental_listings_wow_pct
FROM current_window c
JOIN week_ago_window w ON c.region = w.region
ORDER BY c.region
"""
