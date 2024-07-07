CREATE OR REPLACE FUNCTION fnc_cross_selling(num_groups BIGINT, max_churn BIGINT,
                                             max_stab NUMERIC, max_sku NUMERIC, marg_share NUMERIC)
    RETURNS TABLE
            (
                Customer_ID          BIGINT,
                SKU_Name             VARCHAR,
                Offer_Discount_Depth NUMERIC
            )
AS
$$
BEGIN
    RETURN query
        WITH group_select AS (
            SELECT gr.customer_id,
                   gr.group_id
            FROM (
                     SELECT *,
                            row_number()
                            over (PARTITION BY groups_.customer_id, group_id ORDER BY group_affinity_index DESC) AS COUNT
                     FROM groups_
                 ) AS gr
            WHERE COUNT <= num_groups
              AND group_churn_rate <= max_churn
              AND group_stability_index < max_stab
        )
------------   2   --------------------
           , sku_peer AS (SELECT c.customer_id,
                                 sku_id,
                                 customer_primary_store,
                                 sku_retail_price - sku_purchase_price AS difference,
                                 group_id
                          FROM customers c
                                   JOIN stores s ON c.customer_primary_store = s.transaction_store_id
                                   JOIN group_select gs ON c.customer_id = gs.customer_id
                          ORDER BY customer_id, difference DESC
        )
           , sku_max AS (SELECT sp.customer_id,
                                sp.group_id,
                                sp.customer_primary_store,
                                MAX(sku_retail_price - sku_purchase_price) AS max_marg
                         FROM sku_peer sp
                                  JOIN stores ON sp.sku_id = stores.sku_id
                                  JOIN product_grid pg ON stores.sku_id = pg.sku_id
                         GROUP BY sp.customer_id, sp.group_id, sp.customer_primary_store
        )
------------   3   --------------------
           , count_trans_sku AS (
            SELECT DISTINCT sm.customer_id, COUNT(c.transaction_id) AS count_sku
            FROM sku_max sm
                     JOIN purchase_history ph ON sm.customer_id = ph.customer_id AND sm.group_id = ph.group_id
                     JOIN checks c ON ph.transaction_id = c.transaction_id
            GROUP BY sm.customer_id, c.sku_id
        )
           , count_trans_group AS (SELECT sm.customer_id,
                                          COUNT(transaction_id) AS count_group
                                   FROM sku_max sm
                                            JOIN purchase_history ph
                                                 ON sm.customer_id = ph.customer_id AND sm.group_id = ph.group_id
                                   GROUP BY sm.customer_id, ph.group_id
        )
           , margin_share AS (SELECT cs.customer_id,
                                     count_sku / count_group::NUMERIC AS sku_share
                              FROM count_trans_sku cs
                                       JOIN count_trans_group cg ON cs.customer_id = cg.customer_id
                              WHERE count_sku / count_group::NUMERIC <= max_sku / 100
        )
        SELECT sm.customer_id,
               pg.sku_name,
               (CASE
                    WHEN ((marg_share / 100.0) * max_marg) / sku_retail_price >=
                         ceil(group_minimum_discount * 100 / 5) * 5 / 100
                        THEN ceil(group_minimum_discount * 100 / 5::NUMERIC) * 5 END) AS Offer_Discount_Depth
        FROM sku_max sm
                 JOIN margin_share ms ON sm.customer_id = ms.customer_id
                 JOIN product_grid pg ON sm.group_id = pg.group_id
                 JOIN groups_ g ON sm.group_id = g.group_id AND sm.customer_id = g.customer_id
                 JOIN stores s ON pg.sku_id = s.sku_id AND sm.customer_primary_store = s.transaction_store_id;
END
$$ LANGUAGE plpgsql;

SELECT * FROM fnc_cross_selling(5, 3, 0.5, 100, 30);
