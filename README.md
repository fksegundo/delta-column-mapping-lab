# Delta Column Mapping Lab

Standalone Rust smoke-test project for delta-rs column mapping write support.

This lab validates behavior from delta-rs PR
[`delta-io/delta-rs#4411`](https://github.com/delta-io/delta-rs/pull/4411).
It depends on the matching fork branch:

```toml
deltalake = { git = "https://github.com/fksegundo/delta-rs", branch = "fk/column-mapping-name-write" }
```

## What It Creates

`cargo run` creates three Delta tables under `tables/`:

- `regular_events`: normal Delta table with Hive-style partition directories.
- `column_mapping_events`: `delta.columnMapping.mode = name` table created with physical Parquet names from the first commit.
- `evolved_column_mapping_events`: starts as a normal Delta table, enables column mapping, then appends new columns with `SchemaMode::Merge` and opts into Hive-style partition path preservation.

All tables are partitioned by:

```text
partition_year/partition_month/partition_day
```

Those columns are derived from `partition_date`.

The generator writes 1,000 records for each partition date:

```text
2026-04-27
2026-04-28
2026-05-01
```

Generated CSV inputs are written to `data/` and generated Delta tables are
written to `tables/`. Both directories are ignored by git.

## Commands

Generate CSV files only:

```bash
cargo run -- generate
```

Create or recreate all tables:

```bash
cargo run -- setup
```

Read and validate existing tables:

```bash
cargo run -- read
```

Run the complete flow:

```bash
cargo run
```

## Expected Behavior

- `regular_events` creates paths like `partition_year=2026/partition_month=04/partition_day=27`.
- `column_mapping_events` writes randomized data prefixes and stores partition values with physical column names in the Delta log.
- `evolved_column_mapping_events` keeps writing under Hive-style partition directories by explicitly using `with_preserve_column_mapping_hive_style_partitions(true)`.
- Existing columns in `evolved_column_mapping_events`, including partition columns, keep physical names equal to logical names.
- Evolved data columns receive distinct `col-*` physical names.
- All tables read back through delta-rs with logical column names.
- DataFusion SQL filters use logical column names for both partition columns and mapped data columns.
- Report-style DataFusion SQL queries combine partition filters with logical data-column filters such as `customer_name` and `amount`.

The successful run ends with:

```text
regular: rows=3000
column_mapping: rows=3000
evolved_column_mapping: rows=6000
validation passed
```

## Why This Exists

The target real-world scenario is a Delta table that was created before column
mapping, then later enabled `delta.columnMapping.mode = name` and evolved its
schema. In that shape, existing partition columns may still have identical
logical and physical names, while new data columns use generated `col-*`
physical names.

The lab verifies that delta-rs can write and query that mixed layout without
writing logical names into Parquet for newly evolved columns.
