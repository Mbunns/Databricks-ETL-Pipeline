CREATE STREAMING TABLE `myproject`.`bronze`.`bronze_s3_transactions` (
    transaction_id STRING COLLATE UTF8_BINARY,
    transaction_date TIMESTAMP,
    customer_id STRING COLLATE UTF8_BINARY,
    product_id STRING COLLATE UTF8_BINARY,
    product_name STRING COLLATE UTF8_BINARY,
    category STRING COLLATE UTF8_BINARY,
    quantity BIGINT,
    unit_price DOUBLE,
    total_amount DOUBLE,
    store_location STRING COLLATE UTF8_BINARY,
    payment_method STRING COLLATE UTF8_BINARY,
    _rescued_data STRING COLLATE UTF8_BINARY
  )
  COMMENT 'The table contains records of transactions processed via a file upload interface. It includes details such as transaction IDs, dates, customer information, product details, and payment methods. Use cases might include analyzing sales trends, customer purchasing behavior, and inventory management.' AS
select
  `transaction_id` as `transaction_id`,
  `transaction_date` as `transaction_date`,
  `customer_id` as `customer_id`,
  `product_id` as `product_id`,
  `product_name` as `product_name`,
  `category` as `category`,
  `quantity` as `quantity`,
  `unit_price` as `unit_price`,
  `total_amount` as `total_amount`,
  `store_location` as `store_location`,
  `payment_method` as `payment_method`,
  `_rescued_data` as `_rescued_data`
from
  stream read_files(
    's3://for-my-project-2026/',
    schemaHints =>
      '`transaction_id` string, `transaction_date` timestamp, `customer_id` string, `product_id` string, `product_name` string, `category` string, `quantity` bigint, `unit_price` double, `total_amount` double, `store_location` string, `payment_method` string, `_rescued_data` string'
  )