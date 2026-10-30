-- =============================================================================
-- 05_scrap_cost_by_supplier.sql — KPI 4: money lost to scrap, Plant → Supplier → Part.
--
-- Business question: how much did scrap cost, and how much of it can we claim from suppliers?
-- Owner: Plant controller (finance).
--
-- Recoverable = SUPPLIER_DEFECT (a chargeback candidate). Internal loss = PROCESS_DEFECT.
-- =============================================================================

SELECT
    -- Which level of the hierarchy this row is
    CASE
        WHEN plant IS NULL THEN '0 All plants'
        WHEN supplier IS NULL THEN '1 Plant'
        WHEN part IS NULL THEN '2 Supplier'
        ELSE '3 Part'
    END AS level,
    plant,
    supplier,
    part,

    -- Units and dollars, split by who pays
    SUM(rejected_units) AS rejected_units,
    ROUND(SUM(scrap_cost_usd), 2) AS scrap_cost_usd,
    ROUND(SUM(CASE WHEN is_recoverable THEN scrap_cost_usd ELSE 0 END), 2) AS recoverable_usd,
    ROUND(SUM(CASE WHEN is_recoverable THEN 0 ELSE scrap_cost_usd END), 2) AS internal_loss_usd
FROM gold_scrap_cost
GROUP BY ROLLUP (plant, supplier, part)
ORDER BY plant NULLS FIRST, supplier NULLS FIRST, scrap_cost_usd DESC
