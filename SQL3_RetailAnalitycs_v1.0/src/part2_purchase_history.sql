CREATE OR REPLACE VIEW purchase_history
            (
             customer_id,
             transaction_id,
             transaction_datetime,
             group_id,
             group_cost,
             group_summ,
             group_summ_paid
                )
AS
(
SELECT DISTINCT pd.customer_id,
                t.transaction_id,
                t.transaction_datetime,
                group_id,
                SUM(sku_purchase_price * ch.sku_amount),
                SUM(ch.sku_summ),
                SUM(ch.sku_summ_paid)
FROM personal_data pd
         JOIN cards c ON pd.customer_id = c.customer_id
         JOIN transactions t ON c.customer_card_id = t.customer_card_id
         JOIN checks ch ON t.transaction_id = ch.transaction_id
         JOIN product_grid pg ON pg.sku_id = ch.sku_id
         JOIN stores s ON pg.sku_id = s.sku_id AND t.transaction_store_id = s.transaction_store_id
GROUP BY 1, 2, 3, 4
    );

SELECT * FROM purchase_history;