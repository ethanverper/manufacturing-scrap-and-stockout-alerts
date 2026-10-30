-- =============================================================================
-- 03_stock_coverage.sql — KPI 2: days of stock left per plant and part, day by day.
--
-- Business question: which parts are running out, and which supplier must we call?
-- Owner: Purchasing / material planning.
--
-- No ROLLUP here on purpose: coverage in days is not additive (2 days of batteries + 2 days of
-- connectors is not "4 days" of anything). The hierarchy Plant → Part → Supplier is read row by row.
-- =============================================================================

SELECT
    dt,
    plant,
    part,
    supplier,

    -- What moved during the day, and where the shelf ended
    received_units,
    consumed_units + rejected_units AS used_units,
    closing_units,

    -- The KPI and the same threshold flag the real-time alert uses
    coverage_days,
    is_below_threshold
FROM gold_stock_coverage
ORDER BY plant, part, dt
