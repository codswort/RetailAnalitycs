CREATE OR REPLACE VIEW customers
            (
             Customer_ID,
             Customer_Average_Check,
             Customer_Average_Check_Segment,
             Customer_Frequency,
             Customer_Frequency_Segment,
             Customer_Inactive_Period,
             Customer_Churn_Rate,
             Customer_Churn_Segment,
             Customer_Segment,
             Customer_Primary_Store
                )
AS
(
WITH customer_av_ch AS (
    SELECT cards.customer_id,
           SUM(transaction_summ)::NUMERIC / COUNT(transaction_id)::NUMERIC AS aveg
    FROM cards
             JOIN transactions ON transactions.customer_card_id = cards.customer_card_id
    GROUP BY cards.customer_id
)
   , customer_av_ch_with_rank AS (
    SELECT customer_id,
           aveg,
           CASE
               WHEN ROW_NUMBER() OVER (ORDER BY aveg DESC) <=
                    ceil(((SELECT COUNT(*) FROM customer_av_ch) * 10) / 100) THEN 'High'
               WHEN ROW_NUMBER() OVER (ORDER BY aveg DESC) > (((SELECT COUNT(*) FROM customer_av_ch) * 35) / 100)
                   THEN 'Low'
               ELSE 'Medium'
               END AS customer_average_check_segment
    FROM customer_av_ch
)
   , trans_and_customer_id AS (
    SELECT transactions.transaction_id, cards.customer_id
    FROM transactions
             JOIN cards
                  ON cards.customer_card_id = transactions.customer_card_id)
   , cust_fr AS (
    SELECT ch.customer_id,
           (extract(EPOCH FROM (max(t.transaction_datetime) - min(t.transaction_datetime))) /
            (60 * 60 * 24 * count(t.transaction_id)))::NUMERIC AS customer_frequency
    FROM customer_av_ch_with_rank ch
             JOIN trans_and_customer_id ON ch.customer_id = trans_and_customer_id.customer_id
             JOIN transactions t ON trans_and_customer_id.transaction_id = t.transaction_id
    GROUP BY 1
    ORDER BY 1 ASC)
   , cust_fr_with_rank AS (
    SELECT customer_id,
           customer_frequency,
           CASE
               WHEN ROW_NUMBER() OVER (ORDER BY customer_frequency ASC) <=
                    ceil(((SELECT COUNT(*) FROM cust_fr) * 10) / 100) THEN 'Often'
               WHEN ROW_NUMBER() OVER (ORDER BY customer_frequency ASC) >
                    ceil(((SELECT COUNT(*) FROM cust_fr) * 35) / 100) THEN 'Rarely'
               ELSE 'Occasionally'
               END AS Customer_Frequency_Segment
    FROM cust_fr
)
   , customer_inactive_per AS (
    SELECT cust_fr.customer_id,
           (extract(EPOCH FROM( MAX(date_of_analysis_formation.analysis_formation)
               - MAX(t.transaction_datetime)))/(60 * 60 * 24 )::NUMERIC) AS Customer_Inactive_Period
    FROM cust_fr
             CROSS JOIN date_of_analysis_formation
             JOIN trans_and_customer_id ON cust_fr.customer_id = trans_and_customer_id.customer_id
             JOIN transactions t ON trans_and_customer_id.transaction_id = t.transaction_id
    GROUP BY 1)
   , customer_ch_rate AS (
    SELECT customer_inactive_per.customer_id,
           (customer_inactive_per.Customer_Inactive_Period /
            cust_fr.customer_frequency)::NUMERIC AS Customer_Churn_Rate
    FROM customer_inactive_per
             JOIN cust_fr ON customer_inactive_per.customer_id = cust_fr.customer_id
)
   , customer_churn_rate_with_rank AS (
    SELECT customer_id,
           Customer_Churn_Rate,
           CASE
               WHEN Customer_Churn_Rate BETWEEN 0 AND 2 THEN 'Low'
               WHEN Customer_Churn_Rate > 2 AND Customer_Churn_Rate <= 5 THEN 'Medium'
               ELSE 'High'
               END AS Customer_Churn_Segment
    FROM customer_ch_rate
)
   , low_high_med AS (
    SELECT customer_av_ch_with_rank.Customer_Average_Check_Segment,
           CASE
               WHEN customer_av_ch_with_rank.Customer_Average_Check_Segment = 'Low' THEN 1
               WHEN customer_av_ch_with_rank.Customer_Average_Check_Segment = 'High' THEN 3
               WHEN customer_av_ch_with_rank.Customer_Average_Check_Segment = 'Medium' THEN 2
               END AS SortOrder_lmh
    FROM customer_av_ch_with_rank
    GROUP BY Customer_Average_Check_Segment
)
   , rar_occ_oft AS (
    SELECT cust_fr_with_rank.Customer_Frequency_Segment,
           CASE
               WHEN cust_fr_with_rank.Customer_Frequency_Segment = 'Rarely' THEN 1
               WHEN cust_fr_with_rank.Customer_Frequency_Segment = 'Often' THEN 3
               WHEN cust_fr_with_rank.Customer_Frequency_Segment = 'Occasionally' THEN 2
               END AS SortOrder_roo
    FROM cust_fr_with_rank
    GROUP BY Customer_Frequency_Segment
)
   , table_for_cust_segment AS (SELECT ROW_NUMBER()
                                       OVER (ORDER BY low_high_med.SortOrder_lmh, rar_occ_oft.SortOrder_roo,lhm.SortOrder_lmh) AS Segment,
                                       low_high_med.Customer_Average_Check_Segment,
                                       rar_occ_oft.Customer_Frequency_Segment,
                                       lhm.Customer_Average_Check_Segment                                                      AS Customer_Churn_Segment
                                FROM low_high_med
                                         CROSS JOIN rar_occ_oft
                                         CROSS JOIN low_high_med lhm
)
   , cust_segment AS (
    SELECT customer_av_ch_with_rank.customer_id, table_for_cust_segment.Segment
    FROM customer_av_ch_with_rank
             JOIN cust_fr_with_rank ON cust_fr_with_rank.customer_id = customer_av_ch_with_rank.customer_id
             JOIN customer_churn_rate_with_rank
                  ON cust_fr_with_rank.customer_id = customer_churn_rate_with_rank.customer_id
             JOIN table_for_cust_segment
                  ON customer_av_ch_with_rank.Customer_Average_Check_Segment =
                     table_for_cust_segment.customer_average_check_segment
                      AND cust_fr_with_rank.Customer_Frequency_Segment =
                          table_for_cust_segment.Customer_Frequency_Segment
                      AND customer_churn_rate_with_rank.Customer_Churn_Segment =
                          table_for_cust_segment.Customer_Churn_Segment
)
   , store AS (
    SELECT DISTINCT trans_and_customer_id.customer_id,
                    transactions.transaction_store_id,
                    transactions.transaction_id
    FROM trans_and_customer_id
             JOIN transactions ON trans_and_customer_id.transaction_id = transactions.transaction_id
)
   , person_share AS (
    SELECT trans_and_customer_id.customer_id, count(transactions.transaction_id) AS cust_count_
    FROM transactions
             JOIN trans_and_customer_id ON transactions.transaction_id = trans_and_customer_id.transaction_id
    GROUP BY trans_and_customer_id.customer_id
    ORDER BY 1
)
   , count_store AS (
    SELECT customer_id,
           transaction_store_id,
           count(transaction_id) AS count_
    FROM store
    GROUP BY customer_id, transaction_store_id
    ORDER BY 1
)
   , final_share AS (
    SELECT c.customer_id,
           c.transaction_store_id,
           (c.count_::DOUBLE PRECISION / p.cust_count_)::NUMERIC AS count_
    FROM count_store c
             JOIN person_share p ON c.customer_id = p.customer_id
    ORDER BY 1
)
   , last AS (
    SELECT DISTINCT store.customer_id,
                    store.transaction_store_id,
                    transactions.transaction_datetime
    FROM store
             JOIN transactions ON transactions.transaction_store_id = store.transaction_store_id
    ORDER BY transactions.transaction_datetime DESC
)
   , three_last AS (
    SELECT customer_id,
           transaction_store_id,
           transaction_datetime
    FROM (
             SELECT *, row_number() OVER (PARTITION BY customer_id ORDER BY transaction_datetime DESC) AS count
             FROM last
         ) AS gr
    WHERE count <= 3
)
   , Customer_Primary_St AS
    (SELECT three_last.customer_id,
            CASE
                WHEN
                    (MAX(three_last.transaction_store_id) =
                     MIN(three_last.transaction_store_id))
                    THEN max(three_last.transaction_store_id)
                WHEN
                        (SELECT count(*)
                         FROM (SELECT DISTINCT final_share.transaction_store_id
                               FROM final_share
                               WHERE final_share.count_ = (SELECT max(f.count_)
                                                           FROM final_share f
                                                           WHERE f.customer_id = final_share.customer_id
                                                           GROUP BY f.customer_id)
                                 AND final_share.customer_id = three_last.customer_id) AS coun) = 1
                    THEN (SELECT DISTINCT final_share.transaction_store_id
                          FROM final_share
                          WHERE final_share.count_ = (SELECT max(f.count_)
                                                      FROM final_share f
                                                      WHERE f.customer_id = final_share.customer_id
                                                      GROUP by f.customer_id)
                            AND final_share.customer_id = three_last.customer_id)
                ELSE
                    (
                        SELECT dop.transaction_store_id
                        FROM (SELECT DISTINCT final_share.transaction_store_id, last.transaction_datetime
                              frOm final_share
                                       JOIN last ON last.customer_id = final_share.customer_id
                                  AND last.transaction_store_id = final_share.transaction_store_id
                              WHERE final_share.count_ = (SELECT max(f.count_)
                                                          FROM final_share f
                                                          WHERE f.customer_id = final_share.customer_id
                                                          GROUP by f.customer_id)
                                AND final_share.customer_id = three_last.customer_id
                              ORDER BY transaction_datetime DESC
                              LIMIT 1) AS dop
                    )
                END AS Customer_Primary_Store
     FROM three_last
     GROUP BY customer_id
    )

SELECT customer_av_ch_with_rank.customer_id,
       customer_av_ch_with_rank.aveg AS Customer_Average_Check,
       customer_av_ch_with_rank.customer_average_check_segment,
       cust_fr_with_rank.customer_frequency,
       cust_fr_with_rank.Customer_Frequency_Segment,
       customer_inactive_per.Customer_Inactive_Period,
       customer_churn_rate_with_rank.Customer_Churn_Rate,
       customer_churn_rate_with_rank.Customer_Churn_Segment,
       cust_segment.segment,
       Customer_Primary_St.Customer_Primary_Store
FROM customer_av_ch_with_rank
         JOIN cust_fr_with_rank ON cust_fr_with_rank.customer_id = customer_av_ch_with_rank.customer_id
         JOIN customer_inactive_per ON customer_inactive_per.customer_id = cust_fr_with_rank.customer_id
         JOIN customer_churn_rate_with_rank
              ON customer_churn_rate_with_rank.customer_id = customer_inactive_per.customer_id
         JOIN cust_segment ON cust_segment.customer_id = customer_churn_rate_with_rank.customer_id
         JOIN Customer_Primary_St ON Customer_Primary_St.customer_id = cust_segment.customer_id
    );

SELECT * FROM customers;