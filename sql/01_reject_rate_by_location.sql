-- =============================================================================
-- 01_reject_rate_by_location.sql — KPI 1, WHERE scrap happens: Plant → Line → Station → Part.
--
-- Business question: which plant, line and station reject the most parts, and on which part?
-- Owner: Quality engineering.
--
-- How it works: gold_reject_rate holds additive counts at the lowest grain. GROUP BY ROLLUP adds a
-- subtotal row for every level of the hierarchy in ONE pass (ADR-007), and the rate is computed
-- AFTER summing, because a plant's rate is not the average of its stations' rates.
-- Portable SQL: runs on Athena (Trino) and on Spark SQL (the local tests run it).
-- =============================================================================

SELECT
    -- Which level of the hierarchy this row is (a rolled-up column comes back as NULL)
    CASE
        WHEN plant IS NULL THEN '0 All plants'
        WHEN line IS NULL THEN '1 Plant'
        WHEN station IS NULL THEN '2 Line'
        WHEN part IS NULL THEN '3 Station'
        ELSE '4 Part'
    END AS level,
    plant,
    line,
    station,
    part,

    -- Additive counts first: they always add up from one level to the next
    SUM(consumed_units) AS consumed_units,
    SUM(rejected_units) AS rejected_units,

    -- The KPI, from the summed counts (NULLIF avoids dividing by zero on an idle station)
    ROUND(100.0 * SUM(rejected_units) / NULLIF(SUM(consumed_units) + SUM(rejected_units), 0), 2) AS reject_rate_pct,

    -- Who caused it: share of the rejects that came from the supplier (vs. our own process)
    ROUND(100.0 * SUM(supplier_defect_units) / NULLIF(SUM(rejected_units), 0), 1) AS supplier_share_of_rejects_pct
FROM gold_reject_rate
-- In production, add a date filter (WHERE dt >= ...) so Athena reads only the days it needs
GROUP BY ROLLUP (plant, line, station, part)
ORDER BY plant NULLS FIRST, line NULLS FIRST, station NULLS FIRST, part NULLS FIRST
