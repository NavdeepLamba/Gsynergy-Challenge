CREATE OR REPLACE TABLE `linear-sight-452704-a1.anjan_data_eng_learning.mview_weekly_sales` AS 
SELECT 
    t.pos_site_id,
    t.sku_id,
    t.fsclwk_id,
    t.price_substate_id,
    t.type,
    SUM(CAST(t.sales_units AS FLOAT64)) AS total_sales_units,
    SUM(CAST(t.sales_dollars AS FLOAT64)) AS total_sales_dollars,
    SUM(CAST(t.discount_dollars AS FLOAT64)) AS total_discount_dollars
FROM `linear-sight-452704-a1.anjan_data_eng_learning.staged_facts` as t
GROUP BY t.pos_site_id, t.sku_id, t.fsclwk_id, t.price_substate_id, t.type;
