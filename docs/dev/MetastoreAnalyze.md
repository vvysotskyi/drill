# Metastore ANALYZE commands

Drill provides functionality to collect, use and store into Drill Metastore table metadata.

To enable metastore usage, `metastore.enabled` option should be set to true.

To collect table metadata, the following command should be used:

`ANALYZE TABLE [table_name] [COLUMNS (col1, col2, ...)] REFRESH METADATA [partition LEVEL] {COMPUTE | ESTIMATE} | STATISTICS [(column1, column2, ...)] [ SAMPLE numeric PERCENT ]`

For the case when this command is executed at the first time, whole table metadata will be collected and stored into
 metastore.
If analyze was already executed for table, and table data wasn't changed, all further analyze commands wouldn't
 trigger table analyzing and message that table metadata is up to date will be returned.

# Incremental analyze

For the case when some table data was updated, Drill will try to produce incremental analyze - calculate metadata only
 for updated data and reuse required metadata from the metastore.

Incremental analyze wouldn't be produced for the following cases:
 - List of interesting columns specified in analyze is not a subset of interesting columns from the previous analyze;
 - Specified metadata level differs from the metadata level in previous analyze

# Metadata usage

Drill provides ability to use metadata obtained from the metastore at the planning stage to prune segments, files and
 row groups.

Tables metadata from the metastore is exposed into `INFORMATION_SCHEMA` tables (if metastore usage is enabled).

The following tables are populated with table metadata from the metastore:

`TABLES` table has the following additional columns populated from the metastore:
 - `TABLE_SOURCE` - table data type: `PARQUET`, `CSV`, `JSON`
 - `LOCATION` - table location: `/tmp/nation`
 - `NUM_ROWS` - number of rows in a table if know, `null` if not known
 - `LAST_MODIFIED_TIME` - table's last modification time

`COLUMNS` table has the following additional columns populated from the metastore:
 - `COLUMN_SIZE` (already existed but was not included, applicable for all sources) - estimated column size, for example for boolean `1`, for integer `11` (sign + 10 digits), etc.
 - `COLUMN_DEFAULT` (already existed but never was filled in) - column default value
 - `COLUMN_FORMAT` - usually applicable for date time columns: `yyyy-MM-dd`
 - `NUM_NULLS` - number of nulls in column values
 - `MIN_VAL` - column min value in String representation: `aaa`
 - `MAX_VAL` - column max value in String representation: `zzz`
 - `NDV` - number of distinct values in column, expressed in Double
 - `EST_NUM_NON_NULLS` - estimated number of non null values, expressed in Double
 - `IS_NESTED` - if column is nested. Nested columns are extracted from columns with struct type.

`PARTITIONS`  table has the following additional columns populated from the metastore:
 - `TABLE_CATALOG` - table catalog (currently we have only one catalog): `DRILL`
 - `TABLE_SCHEMA` - table schema: `dfs.tmp`
 - `TABLE_NAME` - table name: `nation`
 - `METADATA_KEY` - top level segment key, the same for all nested segments and partitions: `part_int=3`
 - `METADATA_TYPE` - `SEGMENT` or `PARTITION`
 - `METADATA_IDENTIFIER` - current metadata identifier: `part_int=3/part_varchar=g`
 - `PARTITION_COLUMN` - partition column name: `part_varchar`
 - `PARTITION_VALUE` - partition column value: `g`
 - `LOCATION` - segment location, `null` for partitions: `/tmp/nation/part_int=3`
 - `LAST_MODIFIED_TIME` - last modification time

# Metastore-related options

 - `metastore.enabled` Enables Drill Metastore usage to be able to store table metadata during `ANALYZE TABLE` commands 
execution and to be able to read table metadata during regular queries execution or when querying some `INFORMATION_SCHEMA` tables.
 - `metastore.metadata.store.depth_level` Specifies maximum level depth for collecting metadata.
 - `metastore.metadata.use_schema` Enables schema usage, stored to the Metastore.
 - `metastore.metadata.use_statistics` Enables statistics usage, stored in the Metastore, at the planning stage.
 - `metastore.metadata.fallback_to_file_metadata` Allows using file metadata cache for the case when required metadata is absent in the Metastore.
 - `metastore.retrieval.retry_attempts` Specifies the number of attempts for retrying query planning after detecting that query metadata is changed. 
 If the number of retries was exceeded, query will be planned without metadata information from the Metastore.