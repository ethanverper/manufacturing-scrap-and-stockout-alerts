-- =============================================================================
-- 06_data_quality.sql — did every raw record end up somewhere? One row per day.
--
-- Business question: can we trust today's KPIs?
-- Owner: Data engineering.
--
-- raw = curated + duplicates + quarantined, or the Glue job would have failed before writing.
-- =============================================================================

SELECT
    dt,
    raw_records,
    curated_records,
    duplicate_records,
    quarantined_records,

    -- Share of raw records that were bad or repeated: a rising number means a scanner problem
    ROUND(100.0 * CAST(duplicate_records AS DOUBLE) / NULLIF(raw_records, 0), 2) AS duplicate_pct,
    ROUND(100.0 * CAST(quarantined_records AS DOUBLE) / NULLIF(raw_records, 0), 3) AS quarantined_pct,
    is_balanced
FROM gold_data_quality
ORDER BY dt
