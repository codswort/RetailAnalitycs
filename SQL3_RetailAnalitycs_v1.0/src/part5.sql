CREATE OR REPLACE FUNCTION fnc_offers_frequency_of_visits(
    first_date_of_the_period DATE,
    last_date_of_the_period DATE,
    added_number_of_transactions BIGINT,
    maximum_index_of_churn NUMERIC,
    maximum_share_of_transactions_with_a_discount NUMERIC,
    allowable_share_of_margin NUMERIC)
RETURNS TABLE (
    Customer_ID BIGINT,
    Start_Date TIMESTAMP,
    End_Date TIMESTAMP,
    Required_Transactions_Count BIGINT,
    Group_Name VARCHAR,
    Offer_Discount_Depth NUMERIC
)
AS $$
BEGIN
    RETURN QUERY
        WITH pre AS (
                SELECT groups_.customer_id,
                       first_date_of_the_period::TIMESTAMP,
                       last_date_of_the_period::TIMESTAMP,
                       round((SELECT extract(EPOCH FROM (SELECT first_date_of_the_period::TIMESTAMP -
                                                                last_date_of_the_period::TIMESTAMP) / 86400)::NUMERIC /
                                     (SELECT DISTINCT customer_frequency
                                      FROM customers
                                      WHERE customers.customer_id = groups_.customer_id)))::BIGINT
                           + added_number_of_transactions AS required_transactions_count, --добавляемое число транзакций
                       groups_sku.group_name,
                       (CEIL(groups_.Group_Minimum_Discount * 20) * 5) AS offer_discount_depth,
                       groups_.group_affinity_index,
                       ROW_NUMBER() OVER (PARTITION BY groups_.customer_id ORDER BY groups_.group_affinity_index DESC) AS count
                FROM groups_
                         JOIN groups_sku ON groups_sku.group_id = groups_.group_id
                WHERE ((WITH gr AS (
                    SELECT product_grid.group_id,
                           sum(stores.sku_retail_price - stores.sku_purchase_price) /
                           sum(stores.sku_retail_price) AS average_margin
                    FROM stores
                             JOIN product_grid
                                  ON product_grid.sku_id = stores.sku_id AND product_grid.group_id = groups_.group_id
                    GROUP BY 1
                )
                        SELECT (average_margin * allowable_share_of_margin)
                        FROM gr
                       ) > (ceil(groups_.Group_Minimum_Discount * 20) * 5))
                  AND groups_.group_churn_rate <= maximum_index_of_churn
                  AND groups_.group_discount_share <= maximum_share_of_transactions_with_a_discount / 100
)
    SELECT pre.customer_id,
           pre.first_date_of_the_period,
           pre.last_date_of_the_period,
           pre.required_transactions_count,
           pre.group_name,
           pre.offer_discount_depth
           FROM pre WHERE pre.count = 1;
END
$$ LANGUAGE plpgsql;

SELECT * FROM fnc_offers_frequency_of_visits('2022-08-18 00:00:00', '2022-08-18 00:00:00', 1, 3, 70, 30);

