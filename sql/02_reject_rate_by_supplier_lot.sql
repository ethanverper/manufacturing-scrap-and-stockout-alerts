-- =============================================================================
-- 02_reject_rate_by_supplier_lot.sql — KPI 1, WHO causes scrap: Supplier → Part → Lot.
--
-- Business question: is a supplier, or one specific lot, sending defective parts?
-- Owner: Quality (quarantine the lot) and Purchasing (claim it from the supplier).
--
-- Only SUPPLIER_DEFECT rejects count here: process defects are our stations' fault, not the lot's.
-- =============================================================================

SELECT
    -- Which level of the hierarchy this row is
    CASE
        WHEN supplier IS NULL THEN '0 All suppliers'
        WHEN part IS NULL THEN '1 Supplier'
        WHEN lot IS NULL THEN '2 Part'
        ELSE '3 Lot'
    END AS level,
    supplier,
    part,
    lot,

    -- Additive counts
    SUM(consumed_units) AS consumed_units,
    SUM(supplier_defect_units) AS supplier_defect_units,

    -- Supplier defect rate: defective parts the supplier sent, over all parts taken from that lot
    ROUND(
        100.0 * SUM(supplier_defect_units) / NULLIF(SUM(consumed_units) + SUM(supplier_defect_units), 0), 2
    ) AS supplier_defect_rate_pct
FROM gold_reject_rate
GROUP BY ROLLUP (supplier, part, lot)
ORDER BY supplier NULLS FIRST, part NULLS FIRST, supplier_defect_rate_pct DESC
