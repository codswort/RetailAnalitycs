
CREATE OR REPLACE FUNCTION fnc_aveg(meth INT, first_date DATE, second_date DATE,
    coeff_increase NUMERIC DEFAULT 0,
    max_ind_outflow NUMERIC DEFAULT 0,
    max_share INT DEFAULT 0,
    allowe_marge NUMERIC DEFAULT 0)
RETURNS TABLE (
    cust_id BIGINT,
    av NUMERIC,
    Group_Name VARCHAR,
    Offer_Discount_Depth DECIMAL
)
AS
$$
    BEGIN
        IF first_date < (SELECT MIN(transactions.transaction_datetime) FROM transactions) THEN
            first_date = (SELECT MIN(transactions.transaction_datetime) FROM transactions);
        END IF;
        IF second_date <= first_date OR (second_date > (SELECT analysis_formation FROM date_of_analysis_formation)) THEN
            second_date = (SELECT analysis_formation FROM date_of_analysis_formation);
        END IF;
        RETURN QUERY
        WITH av_ch AS (
                    SELECT cards.customer_id, (SUM(checks.sku_summ)/COUNT(transactions.transaction_id)::NUMERIC) AS aveg
                    FROM cards
                    JOIN transactions ON cards.customer_card_id = transactions.customer_card_id
                    JOIN checks ON transactions.transaction_id = checks.transaction_id
                    WHERE transaction_datetime BETWEEN first_date AND second_date
                    GROUP BY cards.customer_id
                    ),
        select_group AS (
        SELECT gr.customer_id, gr.group_id, gr.group_affinity_index,
       gr.group_churn_rate, gr.Group_Discount_Share, (FLOOR( gr.Group_Minimum_Discount * 20) * 5) AS offer_discount_depth
       FROM (

        SELECT *, ROW_NUMBER() OVER (PARTITION BY grou.group_id ORDER BY grou.group_affinity_index DESC) AS ord
        FROM (SELECT * FROM groups_
                WHERE groups_.group_churn_rate<= (max_ind_outflow::DECIMAL) AND groups_.Group_Discount_Share<=(max_share::DECIMAL/100)
                    AND
                    (floor(groups_.Group_Minimum_Discount * 20) * 5)<((allowe_marge::DECIMAL/100)*
                                                                (SELECT Group_Margin FROM groups_ gs WHERE gs.group_id = groups_.group_id
                                                                    AND gs.customer_id = groups_.customer_id))
                                                                          ) AS grou
           ) AS gr
        WHERE ord = 1
       )
    SELECT av_ch.customer_id, av_ch.aveg * coeff_increase,
            groups_sku.group_name, select_group.offer_discount_depth
    FROM av_ch
            JOIN select_group ON av_ch.customer_id=select_group.customer_id
            JOIN groups_sku ON groups_sku.group_id = select_group.group_id
    ;
    END
$$ LANGUAGE plpgsql;

SELECT * FROM fnc_aveg(1,'2022-01-02','2022-01-02',1.15,3,70,30);


CREATE OR REPLACE FUNCTION fnc_aveg(meth INT, count_of_trans INT DEFAULT 0,
    coeff_increase NUMERIC DEFAULT 0,
    max_ind_outflow NUMERIC DEFAULT 0,
    max_share INT DEFAULT 0,
    allowe_marge NUMERIC DEFAULT 0)
RETURNS TABLE (
    cust_id BIGINT,
    av NUMERIC,
    Group_Name varchar,
    Offer_Discount_Depth DECIMAL
)
AS
$$
    WITH select_trans_id AS (
        SELECT gh.customer_id, transaction_id, transaction_datetime
        FROM (
             select cards.customer_id, transaction_id, transaction_datetime
                 , row_number() OVER (PARTITION BY cards.customer_id ORDER BY transaction_datetime DESC) AS ord
                    FROM transactions
                    JOIN cards ON cards.customer_card_id = transactions.customer_card_id
                 ) AS gh
        WHERE
                    ord<= count_of_trans
    ), av_ch AS (
        SELECT cards.customer_id,
               (SUM(checks.sku_summ) / ( COUNT(select_trans_id)):: NUMERIC) AS av_c
        FROM checks
        JOIN select_trans_id ON checks.transaction_id = select_trans_id.transaction_id
        JOIN cards ON cards.customer_id=select_trans_id.customer_id
        GROUP BY cards.customer_id
    ),
    select_group AS (
        SELECT gr.customer_id, gr.group_id, gr.group_affinity_index,
       gr.group_churn_rate, gr.Group_Discount_Share, (FLOOR( gr.Group_Minimum_Discount * 20) * 5) AS offer_discount_depth
       FROM (

        SELECT *, ROW_NUMBER() OVER (PARTITION BY grou.group_id ORDER BY grou.group_affinity_index DESC) AS ord
        FROM (SELECT * FROM groups_
                WHERE groups_.group_churn_rate<= (max_ind_outflow::DECIMAL) AND groups_.Group_Discount_Share<=(max_share::DECIMAL/100)
                    AND
                    (FLOOR(groups_.Group_Minimum_Discount * 20) * 5)<((allowe_marge::DECIMAL/100)*
                                                                (SELECT Group_Margin FROM groups_ gs WHERE gs.group_id = groups_.group_id
                                                                    AND gs.customer_id = groups_.customer_id))
                                                                          ) AS grou
           ) AS gr
        WHERE ord = 1
       )
    SELECT av_ch.customer_id, av_ch.av_c * coeff_increase,
            groups_sku.group_name, select_group.offer_discount_depth
    FROM av_ch
            JOIN select_group ON av_ch.customer_id=select_group.customer_id
            JOIN groups_sku ON groups_sku.group_id = select_group.group_id
    ;
$$ LANGUAGE sql;

SELECT * FROM fnc_aveg(2,100,1.15,3,70,30);
