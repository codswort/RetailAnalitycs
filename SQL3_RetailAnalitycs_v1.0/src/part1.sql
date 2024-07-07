-- Создание базы данных "Retail Analytics"
DROP DATABASE IF EXISTS retail_analytics;
CREATE DATABASE retail_analytics;

-- Создание таблиц
CREATE TABLE IF NOT EXISTS personal_data
(
    customer_id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    customer_name VARCHAR NOT NULL CHECK (customer_name ~* '^[A-ZА-Я][-a-zа-я]*([- ][A-ZА-Я][-a-zа-я]*)*$'),
    customer_surname VARCHAR NOT NULL CHECK (customer_surname ~* '^[A-ZА-Я][-a-zа-я]*([- ][A-ZА-Я][-a-zа-я]*)*$'),
    customer_primary_email VARCHAR NOT NULL CHECK (customer_primary_email ~* '^([a-z0-9_\.-]+)@([a-z0-9_\.-]+)\.([a-z\.]{2,6})$'),
    customer_primary_phone VARCHAR NOT NULL CHECK (customer_primary_phone ~* '^(\+7[0-9]{10})')
);

CREATE TABLE IF NOT EXISTS cards
(
    customer_card_id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    customer_id BIGINT REFERENCES personal_data(customer_id)
);

CREATE TABLE IF NOT EXISTS groups_sku
(
    group_id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    group_name VARCHAR
);

CREATE TABLE IF NOT EXISTS product_grid
(
    sku_id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    sku_name VARCHAR NOT NULL,
    group_id BIGINT REFERENCES groups_sku(group_id)
);

CREATE TABLE IF NOT EXISTS stores
(
    transaction_store_id BIGINT GENERATED ALWAYS AS IDENTITY,
    sku_id BIGINT REFERENCES product_grid(sku_id),
    sku_purchase_price NUMERIC,
    sku_retail_price NUMERIC
);

CREATE TABLE IF NOT EXISTS transactions
(
    transaction_id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    customer_card_id BIGINT REFERENCES cards(customer_card_id),
    transaction_summ NUMERIC,
    transaction_datetime TIMESTAMP NOT NULL  CHECK (to_char(transaction_datetime, 'DD.MM.YYYY HH24:MI:SS'::TEXT) SIMILAR TO '[0-9]{2}\.[0-9]{2}\.[0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2}'),
    transaction_store_id BIGINT
);

CREATE TABLE IF NOT EXISTS checks
(
    transaction_id BIGINT REFERENCES transactions(transaction_id),
    sku_id BIGINT REFERENCES product_grid(sku_id),
    sku_amount NUMERIC,
    sku_summ NUMERIC,
    sku_summ_paid NUMERIC,
    sku_discount NUMERIC
);

CREATE TABLE IF NOT EXISTS date_of_analysis_formation
(
    analysis_formation TIMESTAMP NOT NULL  CHECK (to_char(analysis_formation, 'DD.MM.YYYY HH24:MI:SS'::TEXT) SIMILAR TO '[0-9]{2}\.[0-9]{2}\.[0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2}')
);

--импорт данных из tsv
SET datestyle TO dmy;
CREATE OR REPLACE PROCEDURE fnc_import(path_to_file TEXT,end_file_name TEXT, CHAR DEFAULT 'E''\t''')
AS
$$
BEGIN
    EXECUTE CONCAT('COPY personal_data FROM ', '''', $1, 'Personal_Data', $2,'.tsv'' DELIMITER ', $3,' CSV');
    EXECUTE CONCAT('COPY cards FROM ', '''', $1, 'Cards', $2,'.tsv'' DELIMITER ', $3, ' CSV');
    EXECUTE CONCAT('COPY groups_sku FROM ', '''', $1, 'Groups_Sku', $2,'.tsv'' DELIMITER ', $3, ' CSV');
    EXECUTE CONCAT('COPY product_grid FROM ', '''', $1, 'SKU', $2,'.tsv'' DELIMITER ', $3, ' CSV');
    EXECUTE CONCAT('COPY stores FROM ', '''', $1, 'Stores', $2,'.tsv'' DELIMITER ', $3, ' CSV');
    EXECUTE CONCAT('COPY transactions FROM ', '''', $1, 'Transactions', $2,'.tsv'' DELIMITER ', $3,' CSV');
    EXECUTE CONCAT('COPY checks FROM ', '''', $1, 'Checks', $2,'.tsv'' DELIMITER ', $3, ' CSV');
    EXECUTE CONCAT('COPY date_of_analysis_formation FROM ', '''', $1, 'Date_Of_Analysis_Formation.tsv'' DELIMITER ', $3, ' CSV');
END
$$ LANGUAGE PLPGSQL;

-- вызов процедуры импорта
CALL fnc_import('/Users/quayleco/projects/sql/SQL3_RetailAnalitycs_v1.0-1/datasets/','_Mini');



--  экспорт данных в csv

CREATE OR REPLACE PROCEDURE fnc_export(path_to_file TEXT,end_file_name TEXT)
AS
$$
BEGIN
    EXECUTE CONCAT('COPY personal_data TO ', '''', $1, 'Personal_Data', $2,'.tsv'' CSV');
    EXECUTE CONCAT('COPY cards TO ', '''', $1, 'Cards', $2,'.tsv'' CSV');
    EXECUTE CONCAT('COPY groups_sku TO ', '''', $1, 'Groups_Sku', $2,'.tsv'' CSV');
    EXECUTE CONCAT('COPY product_grid TO ', '''', $1, 'SKU', $2,'.tsv'' CSV');
    EXECUTE CONCAT('COPY stores TO ', '''', $1, 'Stores', $2,'.tsv'' CSV');
    EXECUTE CONCAT('COPY transactions TO ', '''', $1, 'Transactions', $2,'.tsv'' CSV');
    EXECUTE CONCAT('COPY checks TO ', '''', $1, 'Checks', $2,'.tsv'' CSV');
    EXECUTE CONCAT('COPY date_of_analysis_formation TO ', '''', $1, 'Date_Of_Analysis_Formation.tsv'' CSV');
END
$$ LANGUAGE PLPGSQL;

-- вызов процедуры экспорта
-- CALL fnc_export('/Users/quayleco/projects/sql/SQL3_RetailAnalitycs_v1.0-1/datasets/', '_Mini');
