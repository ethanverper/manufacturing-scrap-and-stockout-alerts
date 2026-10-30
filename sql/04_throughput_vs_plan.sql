-- =============================================================================
-- 04_throughput_vs_plan.sql — KPI 3: finished units vs plan, Plant → Line → Shift.
--
-- Business question: which line and shift are behind the production plan?
-- Owner: Plant manager.
-- =============================================================================

SELECT
    -- Which level of the hierarchy this row is
    CASE
        WHEN plant IS NULL THEN '0 All plants'
        WHEN line IS NULL THEN '1 Plant'
        WHEN shift IS NULL THEN '2 Line'
        ELSE '3 Shift'
    END AS level,
    plant,
    line,
    shift,

    -- Additive counts
    SUM(planned_units) AS planned_units,
    SUM(produced_units) AS produced_units,

    -- The KPI, and the gap in units (what the plant manager has to recover)
    ROUND(100.0 * SUM(produced_units) / NULLIF(SUM(planned_units), 0), 1) AS throughput_pct,
    SUM(planned_units) - SUM(produced_units) AS units_behind_plan
FROM gold_throughput
GROUP BY ROLLUP (plant, line, shift)
ORDER BY plant NULLS FIRST, line NULLS FIRST, shift NULLS FIRST
