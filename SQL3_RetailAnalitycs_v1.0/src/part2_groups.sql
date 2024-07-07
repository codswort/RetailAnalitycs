CREATE OR REPLACE FUNCTION fnc_create_groups(type_margin_calc INTEGER DEFAULT 0, number INTEGER DEFAULT 1)
    RETURNS TABLE
            (
                customer_id            BIGINT,
                group_id               BIGINT,
                group_affinity_index   NUMERIC,
                group_churn_rate       NUMERIC,
                group_stability_index  NUMERIC,
                group_margin           NUMERIC,
                group_discount_share   NUMERIC,
                group_minimum_discount NUMERIC,
                group_average_discount NUMERIC
            )
AS
$$
BEGIN
    RETURN QUERY (
        WITH sku_list AS (
            SELECT DISTINCT c.customer_id,
                            pg.group_id
            FROM checks
                     JOIN transactions t ON checks.transaction_id = t.transaction_id
                     JOIN cards c ON c.customer_card_id = t.customer_card_id
                     JOIN product_grid pg ON checks.sku_id = pg.sku_id
            ORDER BY c.customer_id, pg.group_id
        )
           , affinity AS (
            SELECT s.customer_id,
                   s.group_id,
                   (p.group_purchase::DOUBLE PRECISION / COUNT(ph.transaction_id))::NUMERIC AS group_affinity_index
            FROM sku_list s
                     JOIN purchase_history ph ON s.customer_id = ph.customer_id
                     JOIN periods p ON s.customer_id = p.customer_id AND s.group_id = p.group_id
            WHERE transaction_datetime >= first_group_purchase_date
              AND transaction_datetime <= last_group_purchase_date
            GROUP BY s.customer_id, s.group_id, p.group_purchase
        )
           , churn_index AS (
            SELECT ph.customer_id,
                   ph.group_id,
                   affinity.group_affinity_index,
                   (DATE_PART('day', (SELECT * FROM date_of_analysis_formation) - MAX(transaction_datetime))) AS date
            FROM purchase_history ph
                     JOIN affinity ON ph.customer_id = affinity.customer_id AND ph.group_id = affinity.group_id
            GROUP BY ph.customer_id, ph.group_id, affinity.group_affinity_index
        )
           , churn_rate AS (
            SELECT ci.customer_id,
                   ci.group_id,
                   ci.group_affinity_index,
                   (date::DOUBLE PRECISION / p.group_frequency)::NUMERIC AS group_churn_rate
            FROM churn_index ci
                     JOIN periods p ON ci.customer_id = p.customer_id AND ci.group_id = p.group_id
        )
           , stabil_group1 AS (
            SELECT ph.customer_id,
                   ph.group_id,
                   DATE_PART('day',
                             (transaction_datetime -
                              LAG(transaction_datetime) OVER (PARTITION BY ph.customer_id, ph.group_id
                                  ORDER BY transaction_datetime))) AS intervals
            FROM purchase_history ph
        )
           , stabil_group2 AS (
            SELECT s.customer_id,
                   s.group_id,
                   CASE
                       WHEN (intervals - group_frequency) < 0 THEN (-(intervals - group_frequency) / group_frequency)::NUMERIC
                       ELSE ((intervals - group_frequency) / group_frequency)::NUMERIC END AS absolute_deviation
            FROM stabil_group1 s
                     JOIN periods p ON s.group_id = p.group_id AND s.customer_id = p.customer_id
            WHERE intervals IS NOT NULL
        )
           , stabil_group AS (
            SELECT s.customer_id,
                   s.group_id,
                   cr.group_affinity_index,
                   cr.group_churn_rate,
                   AVG(absolute_deviation) AS group_stability_index
            FROM stabil_group2 s
                     JOIN churn_rate cr ON s.customer_id = cr.customer_id AND s.group_id = cr.group_id
            GROUP BY s.customer_id, s.group_id, cr.group_affinity_index, cr.group_churn_rate
        )
           , margin_period AS (
            SELECT s.customer_id,
                   s.group_id,
                   SUM(group_summ_paid - group_cost) AS group_margin
            FROM sku_list s
                     JOIN purchase_history ph ON s.group_id = ph.group_id AND s.customer_id = ph.customer_id
                     JOIN periods p ON s.customer_id = p.customer_id AND s.group_id = p.group_id
            WHERE transaction_datetime >=
                  (SELECT *
                   FROM date_of_analysis_formation) - CONCAT($2, ' days')::INTERVAL
              AND transaction_datetime <= (SELECT *
                                           FROM date_of_analysis_formation)
            GROUP BY s.customer_id, s.group_id
        )
           , margin_num_trans AS (
            SELECT s.customer_id,
                   s.group_id,
                   SUM(group_summ_paid - group_cost) AS group_margin
            FROM sku_list s
                     JOIN purchase_history ph ON s.group_id = ph.group_id AND s.customer_id = ph.customer_id
                     JOIN periods p ON s.customer_id = p.customer_id AND s.group_id = p.group_id
            GROUP BY s.customer_id, s.group_id
            ORDER BY s.customer_id, s.group_id
            LIMIT $2
        )
           , margin AS (
            SELECT s.customer_id,
                   s.group_id,
                   SUM(group_summ_paid - group_cost) AS group_margin
            FROM sku_list s
                     JOIN purchase_history ph ON s.group_id = ph.group_id AND s.customer_id = ph.customer_id
                     JOIN periods p ON s.customer_id = p.customer_id AND s.group_id = p.group_id
            GROUP BY s.customer_id, s.group_id
        )
           , discount AS (
            SELECT s.customer_id,
                   s.group_id,
                   (count(ph.transaction_id)::DOUBLE PRECISION / group_purchase)::NUMERIC AS group_discount_share
            FROM sku_list s
                     JOIN purchase_history ph ON s.group_id = ph.group_id AND s.customer_id = ph.customer_id
                     JOIN checks c ON ph.transaction_id = c.transaction_id
                     JOIN periods p ON s.customer_id = p.customer_id AND s.group_id = p.group_id
            WHERE sku_discount > 0
            GROUP BY s.customer_id, s.group_id, group_purchase
        )
           , min_discount AS (
            SELECT s.customer_id,
                   s.group_id,
                   MIN(group_min_discount) AS Group_Minimum_Discount
            FROM sku_list s
                     JOIN periods p ON s.customer_id = p.customer_id AND s.group_id = p.group_id
            WHERE group_min_discount <> 0
            GROUP BY s.customer_id, s.group_id
        )
           , avr_discount AS (
            SELECT s.customer_id,
                   s.group_id,
                   (SUM(group_summ_paid) / SUM(group_summ))::NUMERIC AS group_average_discount
            FROM sku_list s
                     JOIN purchase_history ph ON s.group_id = ph.group_id AND s.customer_id = ph.customer_id
            GROUP BY s.customer_id, s.group_id
        )
        SELECT sk.customer_id,
               sk.group_id,
               sg.group_affinity_index,
               sg.group_churn_rate,
               sg.group_stability_index,
           CASE type_margin_calc
--                case 0
                   WHEN 0 THEN m.group_margin
                   WHEN 1 THEN mp.group_margin
                   ELSE mt.group_margin
                   END AS group_margin,
               d.group_discount_share,
               md.group_minimum_discount,
               ad.group_average_discount
        FROM sku_list sk
                 JOIN stabil_group sg ON sk.customer_id = sg.customer_id AND sk.group_id = sg.group_id
                 FULL JOIN discount d ON sk.customer_id = d.customer_id AND sk.group_id = d.group_id
                 FULL JOIN min_discount md ON sk.customer_id = md.customer_id AND sk.group_id = md.group_id
                 JOIN avr_discount ad ON sk.customer_id = ad.customer_id AND sk.group_id = ad.group_id
                 FULL JOIN margin m ON sk.customer_id = m.customer_id AND sk.group_id = m.group_id
                 FULL JOIN margin_period mp ON sk.customer_id = mp.customer_id AND sk.group_id = mp.group_id
                 FULL JOIN margin_num_trans mt ON sk.customer_id = mt.customer_id AND sk.group_id = mt.group_id
        WHERE sk.customer_id IS NOT NULL
    );
END
$$ LANGUAGE plpgsql;

-- для выбора метода расчета маржи по периоду, нужно передать первым параметром 1, а вторым количество дней
-- для выбора метода расчета маржи по количеству транзакций, нужно передать первым параметром 2, а вторым количество транзакций
-- для расчета маржи по всем транзакциям параметры не указываются

CREATE OR REPLACE VIEW groups_
AS
SELECT *
FROM fnc_create_groups();

SELECT * FROM groups_;






