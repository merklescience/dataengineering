create_temp_ch_tables_default = """
    DROP TABLE IF EXISTS {{chain}}.daily_etl_{{ds_nodash}};
    CREATE TABLE {{chain}}.daily_etl_{{ds_nodash}}
    (
        `block` UInt64,
        `block_timestamp` DateTime,
        `transaction_hash` String,
        `sender_address` String,
        `receiver_address` String,
        `type` UInt8,
        `token_address` String,
        `coin_value` String,
        `fee` Float64,
        `log_index` UInt16
    )
    ENGINE = Log()
"""
truncate_streaming_tables_default = (
    "ALTER TABLE {{chain}}.{{table}} DELETE WHERE toYYYYMMDD(block_date_time) "
    "<= {{ds_nodash}}"
)
insert_ch_main_table_default = """
INSERT INTO {{chain}}.{{table}}
SELECT toUnixTimestamp(now()) AS insert_time,
       block,
       transaction_hash AS transaction_id,
       sender_address,
       receiver_address,
       type,
       token_address,
       coin_value,
       fee,
       block_timestamp AS block_date_time,
       log_index
FROM {{chain}}.daily_etl_{{ds_nodash}}
"""
bq_address_details_default = """
DELETE FROM `intelligence-team.{{chain}}_flat.address_details_daily` WHERE date(block_date)='{{export_date}}';
INSERT INTO `intelligence-team.{{chain}}_flat.address_details_daily`
SELECT
  address,
  block_date,
  SUM(outgoing_value) AS outgoing_value,
  SUM(incoming_value) AS incoming_value,
  SUM(outgoing_value_usd) AS outgoing_value_usd,
  SUM(incoming_value_usd) AS incoming_value_usd,
  MIN(outgoing_min_txn_date) AS outgoing_min_txn_date,
  MIN(incoming_min_txn_date) AS incoming_min_txn_date,
  MAX(outgoing_max_txn_date) AS outgoing_max_txn_date,
  MAX(incoming_max_txn_date) AS incoming_max_txn_date,
  SUM(outgoing_txn_count) AS outgoing_txn_count,
  SUM(incoming_txn_count) AS incoming_txn_count
FROM (
  SELECT
    sender_address AS address,
    DATE(block_timestamp) AS block_date,
    SUM(CASE
        WHEN type IN (0, 1) THEN (fee + coin_value)
    END
      ) AS outgoing_value,
    0 AS incoming_value,
    SUM((coin_value + fee) * coin_price_usd) AS outgoing_value_usd,
    0 AS incoming_value_usd,
    MIN(block_timestamp) AS outgoing_min_txn_date,
    '2200-01-01' AS incoming_min_txn_date,
    MAX(block_timestamp) AS outgoing_max_txn_date,
    '1970-01-01' AS incoming_max_txn_date,
    COUNT(DISTINCT transaction_hash) AS outgoing_txn_count,
    0 AS incoming_txn_count
  FROM
    `intelligence-team.{{chain}}_flat.tld`
   WHERE date(block_timestamp)='{{export_date}}'
  GROUP BY
    address,
    block_date
  UNION ALL
  SELECT
    receiver_address AS address,
    DATE(block_timestamp) AS block_date,
    0 AS outgoing_value,
    SUM(CASE
        WHEN type IN (0, 1) THEN coin_value
    END
      ) AS incoming_value,
    0 AS outgoing_value_usd,
    SUM(coin_value * coin_price_usd) AS incoming_value_usd,
    '2200-01-01' AS outgoing_min_txn_date,
    MIN(block_timestamp) AS incoming_min_txn_date,
    '1970-01-01' AS outgoing_max_txn_date,
    MAX(block_timestamp) AS incoming_max_txn_date,
    0 AS outgoing_txn_count,
    COUNT(DISTINCT transaction_hash) AS incoming_txn_count
  FROM
     `intelligence-team.{{chain}}_flat.tld`
  WHERE date(block_timestamp)='{{export_date}}'
  GROUP BY
    address,
    block_date )
GROUP BY
  address,
  block_date
"""
bq_address_links_default = """
DELETE FROM `intelligence-team.{{chain}}_flat.address_links_daily` WHERE date(block_date)='{{export_date}}';
INSERT INTO `intelligence-team.{{chain}}_flat.address_links_daily`
SELECT
  sender_address,
  receiver_address,
  DATE(block_timestamp) AS block_date,
  SUM(CASE
      WHEN type IN (0, 1) THEN coin_value
  END
    ) AS outgoing_value,
  SUM(CASE
      WHEN type IN (0, 1) THEN coin_value
  END
    ) AS incoming_value,
  SUM(coin_value * coin_price_usd) AS outgoing_value_usd,
  SUM(coin_value * coin_price_usd) AS incoming_value_usd,
  COUNT(DISTINCT transaction_hash) AS txn_count,
  MIN(block_timestamp) AS min_txn_date,
  MAX(block_timestamp) AS max_txn_date
FROM
  `intelligence-team.{{chain}}_flat.tld`
WHERE date(block_timestamp)='{{export_date}}'
GROUP BY
  sender_address,
  receiver_address,
  block_date
"""
# If log index does not exist in dump table this query needs to be replaced
bq_to_ch_csv_dump_default = """
EXPORT DATA
  OPTIONS( uri='gs://{{bq_dump_bucket}}/{{bq_dump_path}}*.csv',
    format='CSV',
    overwrite=TRUE,
    header=TRUE) AS
SELECT
  block,
  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', block_timestamp) AS block_timestamp,
  transaction_hash,
  sender_address,
  receiver_address,
  type,
  token_address,
  coin_value,
  fee,
  log_index
FROM
  `intelligence-team.{{chain}}_flat.tld`
WHERE
  date(block_timestamp) = '{{export_date}}'
"""
