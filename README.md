# Snowflake output plugin for Embulk

Snowflake output plugin for Embulk loads records to Snowflake.

## Overview

* **Plugin type**: output
* **Load all or nothing**: depends on the mode. see below.
* **Resume supported**: depends on the mode. see below.

## Configuration

- **host**: database host name (string, required)
- **user**: database login user name (string, required)
- **password**: database login password (string, default: "")
- **privateKey**: database login using key-pair authentication(string, default: ""). This authentication method requires a 2048-bit (minimum) RSA key pair.
- **private_key_passphrase**: passphrase for private_key (string, default: "")
- **warehouse**: destination warehouse name (string, required)
- **database**: destination database name (string, required)
- **schema**: destination schema name (string, default: "public")
- **table**: destination table name (string, required)
- **role**: role to execute queries (string, default: "")
- **retry_limit**: max retry count for database operations (integer, default: 12). When intermediate table to create already created by another process, this plugin will retry with another table name to avoid collision.
- **retry_wait**: initial retry wait time in milliseconds (integer, default: 1000 (1 second))
- **max_retry_wait**: upper limit of retry wait, which will be doubled at every retry (integer, default: 1800000 (30 minutes))
- **max_upload_retries**: maximum number of retries for file upload to Snowflake (integer, default: 3).
- **max_copy_retries**: maximum number of retries for COPY operations in Snowflake (integer, default: 3). Retries occur when transient errors such as communication failures happen during the COPY process.
- **mode**: "insert", "insert_direct", "truncate_insert", "replace" or "merge". See below. (string, required)
- **merge_keys**: key column names for merging records in merge mode (string array, required in merge mode if table doesn't have primary key)
- **merge_rule**: list of column assignments for updating existing records used in merge mode, for example `"foo" = T."foo" + S."foo"` (`T` means target table and `S` means source table). (string array, default: always overwrites with new values)
- **batch_size**: size of a single batch insert (integer, default: 16777216)
- **match_by_column_name**: specify whether to load semi-structured data into columns in the target table that match corresponding columns represented in the data. ("case_sensitive", "case_insensitive", "none", default: "none")
- **upload_jdbc_log_to_s3**: enable automatic upload of JDBC driver logs to S3 when communication errors occur (boolean, default: false)
- **s3_bucket**: S3 bucket name for JDBC log upload (string, required when upload_jdbc_log_to_s3 is true)
- **s3_prefix**: S3 key prefix for JDBC log upload (string, required when upload_jdbc_log_to_s3 is true)
- **s3_region**: AWS region for S3 bucket (string, required when upload_jdbc_log_to_s3 is true)
- **s3_access_key_id**: AWS access key ID for S3 access (string, optional - uses IAM role if not specified)
- **s3_secret_access_key**: AWS secret access key for S3 access (string, optional - uses IAM role if not specified)
- **default_timezone**: If input column type (embulk type) is timestamp, this plugin needs to format the timestamp into a SQL string. This default_timezone option is used to control the timezone. You can overwrite timezone for each columns using column_options option. (string, default: `UTC`)
- **column_options**: advanced: a key-value pairs where key is a column name and value is options for the column.
  - **type**: type of a column when this plugin creates new tables (e.g. `VARCHAR(255)`, `INTEGER NOT NULL UNIQUE`). This used when this plugin creates intermediate tables (insert, truncate_insert and merge modes), when it creates the target table (insert_direct and replace modes), and when it creates nonexistent target table automatically. (string, default: depends on input column type. `BIGINT` if input column type is long, `BOOLEAN` if boolean, `DOUBLE PRECISION` if double, `CLOB` if string, `TIMESTAMP` if timestamp)
  - **value_type**: This plugin converts input column type (embulk type) into a database type to build a TSV to put TSV to internal storage. This value_type option controls the type of the value in a TSV. (string, default: depends on the sql type of the column. Available values options are: `byte`, `short`, `int`, `long`, `double`, `float`, `boolean`, `string`, `nstring`, `date`, `time`, `timestamp`, `decimal`, `json`, `null`, `pass`)
  - **timestamp_format**: If input column type (embulk type) is timestamp and value_type is `string` or `nstring`, this plugin needs to format the timestamp value into a string. This timestamp_format option is used to control the format of the timestamp. (string, default: `%Y-%m-%d %H:%M:%S.%6N`)
  - **timezone**: If input column type (embulk type) is timestamp, this plugin needs to format the timestamp value into a SQL string. In this cases, this timezone option is used to control the timezone. (string, value of default_timezone option is used by default)
- **before_load**: if set, this SQL will be executed before loading all records. In truncate_insert mode, the SQL will be executed after truncating. replace mode doesn't support this option.
- **after_load**: if set, this SQL will be executed after loading all records.

### Modes

* **insert**:
  * Behavior: This mode writes rows to some intermediate tables first. If all those tasks run correctly, runs `INSERT INTO <target_table> SELECT * FROM <intermediate_table_1> UNION ALL SELECT * FROM <intermediate_table_2> UNION ALL ...` query. If the target table doesn't exist, it is created automatically.
  * Transactional: Yes. This mode successfully writes all rows, or fails with writing zero rows.
  * Resumable: No.
* **insert_direct**:
  * Behavior: This mode inserts rows to the target table directly. If the target table doesn't exist, it is created automatically.
  * Transactional: No. If fails, the target table could have some rows inserted.
  * Resumable: No.
* **truncate_insert**:
  * Behavior: Same with `insert` mode excepting that it truncates the target table right before the last `INSERT ...` query.
  * Transactional: Yes.
  * Resumable: No.
* **replace**:
  * Behavior: This mode writes rows to an intermediate table first. If all those tasks run correctly, drops the target table and alters the name of the intermediate table into the target table name.
  * Transactional: Yes.
  * Resumable: No.
* **merge**:
  * Behavior: This mode writes rows to some intermediate tables first. If all those tasks run correctly, runs MERGE INTO ... WHEN MATCHED THEN UPDATE ...  WHEN NOT MATCHED THEN INSERT ... query. Namely, if merge keys of a record in the intermediate tables already exist in the target table, the target record is updated by the intermediate record, otherwise the intermediate record is inserted. If the target table doesn't exist, it is created automatically.
  * Transactional: Yes.
  * Resumable: No.

## JDBC Log Upload to S3

This plugin supports automatic upload of JDBC driver logs to S3 when communication errors occur. This feature is useful for debugging connection issues with Snowflake.

### Configuration Example

```yaml
out:
  type: snowflake
  host: your-account.snowflakecomputing.com
  user: your_user
  password: your_password
  warehouse: your_warehouse
  database: your_database
  schema: your_schema
  table: your_table
  mode: insert
  
  # JDBC log upload configuration
  upload_jdbc_log_to_s3: true
  s3_bucket: your-log-bucket
  s3_prefix: snowflake-jdbc-logs
  s3_region: us-east-1
  
  # Optional: Explicit AWS credentials (uses IAM role if not specified)
  s3_access_key_id: YOUR_ACCESS_KEY_ID
  s3_secret_access_key: YOUR_SECRET_ACCESS_KEY
```

### Authentication Methods

1. **IAM Role (Recommended)**: Leave `s3_access_key_id` and `s3_secret_access_key` unspecified. The plugin will use the default AWS credentials provider chain (IAM role, environment variables, etc.).

2. **Explicit Credentials**: Specify both `s3_access_key_id` and `s3_secret_access_key` for explicit authentication.

### Behavior

- JDBC logs are only uploaded when communication errors occur during Snowflake operations
- The plugin automatically finds the latest `snowflake_jdbc*.log` file in the system temp directory
- Logs are uploaded to `s3://{bucket}/{prefix}/{filename}`
- If S3 upload fails, a warning is logged but the original error is still thrown
- If required S3 configuration is missing, a warning is logged and log upload is skipped

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
