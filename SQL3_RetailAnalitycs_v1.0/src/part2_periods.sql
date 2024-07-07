CREATE OR REPLACE VIEW periods
            (
             customer_id,
             group_id,
             first_group_purchase_date,
             last_group_purchase_date,
             group_purchase,
             group_frequency,
             group_min_discount
                )
AS
(
WITH periods_ AS (
    SELECT customer_id,
           group_id,
           MIN(transaction_datetime) AS first_group_purchase_date,
           MAX(transaction_datetime) AS last_group_purchase_date,
           COUNT(transaction_id)     AS group_purchase,
           ((EXTRACT(EPOCH FROM(MAX(transaction_datetime)::TIMESTAMP - MIN(transaction_datetime)::TIMESTAMP))/(60*60*24) + 1)::DOUBLE PRECISION / COUNT(transaction_id))::NUMERIC
                                     AS group_frequency
    FROM purchase_history
    GROUP BY 1, 2
    ORDER BY 1, 2
),
     min_disc AS (
         SELECT customer_id,
                group_id,
                min(sku_discount / checks.sku_summ) AS group_min_discount
         FROM purchase_history ph
                  JOIN checks ON checks.transaction_id = ph.transaction_id
         WHERE sku_discount <> 0
         GROUP BY 1, 2
     )
SELECT p.customer_id,
       p.group_id,
       first_group_purchase_date,
       last_group_purchase_date,
       group_purchase,
       group_frequency,
       coalesce(group_min_discount, 0)
FROM periods_ p
         FULL JOIN min_disc m
                   ON p.group_id = m.group_id
                       AND p.customer_id = m.customer_id
    );

SELECT * FROM periods;