use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use deltalake::arrow::array::{Float64Array, Int64Array, StringArray, StringViewArray};
use deltalake::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::datafusion::prelude::SessionContext;
use deltalake::kernel::{
    Action, ColumnMetadataKey, DataType, MetadataValue, PrimitiveType, Protocol, StructField,
    StructType,
};
use deltalake::operations::collect_sendable_stream;
use deltalake::operations::write::SchemaMode;
use deltalake::{DeltaTable, TableProperty, open_table};
use serde_json::{Value, json};
use url::Url;

type LabResult<T = ()> = Result<T, Box<dyn Error>>;

const REGULAR_TABLE: &str = "regular_events";
const COLUMN_MAPPING_TABLE: &str = "column_mapping_events";
const EVOLVED_COLUMN_MAPPING_TABLE: &str = "evolved_column_mapping_events";
const RECORDS_PER_PARTITION: usize = 1000;
const PARTITION_DATES: [&str; 3] = ["2026-04-27", "2026-04-28", "2026-05-01"];

#[tokio::main]
async fn main() -> LabResult {
    let command = std::env::args().nth(1).unwrap_or_else(|| "run".to_string());

    match command.as_str() {
        "generate" => generate_csv()?,
        "setup" => setup().await?,
        "read" => read_and_validate().await?,
        "run" => {
            setup().await?;
            read_and_validate().await?;
        }
        other => {
            return Err(
                format!("unknown command `{other}`; use generate, setup, read, or run").into(),
            );
        }
    }

    Ok(())
}

async fn setup() -> LabResult {
    generate_csv()?;

    let tables_root = tables_root();
    reset_dir(&tables_root)?;

    create_and_populate_regular(&tables_root.join(REGULAR_TABLE)).await?;
    create_and_populate_column_mapping(&tables_root.join(COLUMN_MAPPING_TABLE)).await?;
    create_and_populate_evolved_column_mapping(&tables_root.join(EVOLVED_COLUMN_MAPPING_TABLE))
        .await?;

    println!("created tables under {}", tables_root.display());
    Ok(())
}

fn generate_csv() -> LabResult {
    let status = Command::new("python3")
        .arg("scripts/generate_events.py")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .status()?;
    if !status.success() {
        return Err(format!("CSV generator failed with status {status}").into());
    }
    Ok(())
}

async fn create_and_populate_regular(table_path: &Path) -> LabResult {
    fs::create_dir_all(table_path)?;
    let table = DeltaTable::try_from_url(table_url(table_path)?).await?;
    let table = table
        .create()
        .with_columns(table_fields(false, true, false))
        .with_partition_columns(partition_columns())
        .await?;

    table
        .write(vec![events_batch(&evolved_csv(), true)?])
        .await?;
    Ok(())
}

async fn create_and_populate_column_mapping(table_path: &Path) -> LabResult {
    fs::create_dir_all(table_path)?;
    let table = DeltaTable::try_from_url(table_url(table_path)?).await?;
    let table = table
        .create()
        .with_columns(table_fields(true, true, false))
        .with_partition_columns(partition_columns())
        .with_actions([Action::Protocol(column_mapping_protocol()?)])
        .with_raise_if_key_not_exists(false)
        .with_configuration(column_mapping_configuration(9))
        .await?;

    table
        .write(vec![events_batch(&evolved_csv(), true)?])
        .await?;
    Ok(())
}

async fn create_and_populate_evolved_column_mapping(table_path: &Path) -> LabResult {
    fs::create_dir_all(table_path)?;
    let table = DeltaTable::try_from_url(table_url(table_path)?).await?;
    let table = table
        .create()
        .with_columns(table_fields(false, false, false))
        .with_partition_columns(partition_columns())
        .await?;

    table.write(vec![events_batch(&base_csv(), false)?]).await?;
    activate_column_mapping_for_existing_table(table_path)?;

    let table = open_table(table_url(table_path)?).await?;
    table
        .write(vec![events_batch(&evolved_csv(), true)?])
        .with_schema_mode(SchemaMode::Merge)
        .await?;

    Ok(())
}

async fn read_and_validate() -> LabResult {
    let tables_root = tables_root();
    let regular_path = tables_root.join(REGULAR_TABLE);
    let column_mapping_path = tables_root.join(COLUMN_MAPPING_TABLE);
    let evolved_path = tables_root.join(EVOLVED_COLUMN_MAPPING_TABLE);

    let regular = read_summary(&regular_path).await?;
    let column_mapping = read_summary(&column_mapping_path).await?;
    let evolved = read_summary(&evolved_path).await?;

    assert_summary(&regular, RECORDS_PER_PARTITION);
    assert_summary(&column_mapping, RECORDS_PER_PARTITION);
    assert_summary(&evolved, RECORDS_PER_PARTITION * 2);

    assert_hive_partition_exists(&regular_path);
    assert!(
        !column_mapping_path.join("partition_year=2026").exists(),
        "fresh column-mapped table should not use logical Hive partition directories"
    );
    assert_hive_partition_exists(&evolved_path);

    let fresh_add_summary = latest_add_summary(&column_mapping_path)?;
    assert_physical_partition_keys(&fresh_add_summary);

    let evolved_add_summary = latest_add_summary(&evolved_path)?;
    assert_logical_partition_keys(&evolved_add_summary);
    assert!(
        !has_random_prefix_dirs(&evolved_path),
        "expected evolved table to keep Hive-style partition directories when partition physical names match logical names"
    );
    assert_fresh_column_mapping_schema(&column_mapping_path)?;
    assert_evolved_column_mapping_schema(&evolved_path)?;
    validate_logical_filters(&regular_path, &column_mapping_path, &evolved_path).await?;

    println!(
        "regular: rows={}, counts={:?}",
        regular.row_count, regular.partition_counts
    );
    println!(
        "column_mapping: rows={}, counts={:?}, latest_add_partition_keys={:?}",
        column_mapping.row_count, column_mapping.partition_counts, fresh_add_summary.partition_keys
    );
    println!(
        "evolved_column_mapping: rows={}, counts={:?}, latest_add_partition_keys={:?}",
        evolved.row_count, evolved.partition_counts, evolved_add_summary.partition_keys
    );
    println!("validation passed");

    Ok(())
}

async fn validate_logical_filters(
    regular_path: &Path,
    column_mapping_path: &Path,
    evolved_path: &Path,
) -> LabResult {
    assert_query_count(
        regular_path,
        "SELECT COUNT(*) FROM events WHERE partition_year = '2026' AND partition_month = '04' AND partition_day = '27'",
        RECORDS_PER_PARTITION as i64,
    )
    .await?;
    assert_query_count(
        column_mapping_path,
        "SELECT COUNT(*) FROM events WHERE partition_year = '2026' AND partition_month = '04' AND partition_day = '27'",
        RECORDS_PER_PARTITION as i64,
    )
    .await?;
    assert_query_count(
        column_mapping_path,
        "SELECT COUNT(*) FROM events WHERE event_id = 100001 AND source_system = 'batch'",
        1,
    )
    .await?;
    assert_query_count(
        evolved_path,
        "SELECT COUNT(*) FROM events WHERE partition_year = '2026' AND partition_month = '04' AND partition_day = '27'",
        (RECORDS_PER_PARTITION * 2) as i64,
    )
    .await?;
    assert_query_count(
        evolved_path,
        "SELECT COUNT(*) FROM events WHERE event_id = 100001 AND source_system = 'batch'",
        1,
    )
    .await?;
    validate_report_filters(regular_path, column_mapping_path, evolved_path).await?;

    Ok(())
}

async fn validate_report_filters(
    regular_path: &Path,
    column_mapping_path: &Path,
    evolved_path: &Path,
) -> LabResult {
    let report_query = "SELECT COUNT(*) FROM events \
        WHERE partition_year = '2026' \
        AND partition_month = '04' \
        AND partition_day = '27' \
        AND customer_name = 'customer-100123' \
        AND amount BETWEEN 494.90 AND 494.92";

    assert_query_count(regular_path, report_query, 1).await?;
    assert_query_count(column_mapping_path, report_query, 1).await?;
    assert_query_count(evolved_path, report_query, 1).await?;

    Ok(())
}

async fn assert_query_count(table_path: &Path, sql: &str, expected: i64) -> LabResult {
    let actual = query_count(table_path, sql).await?;
    assert_eq!(actual, expected, "unexpected row count for query: {sql}");
    Ok(())
}

async fn query_count(table_path: &Path, sql: &str) -> LabResult<i64> {
    let table = open_table(table_url(table_path)?).await?;
    let ctx = SessionContext::new();
    table.update_datafusion_session(&ctx.state())?;
    ctx.register_table("events", table.table_provider().await?)?;
    let batches = ctx.sql(sql).await?.collect().await?;
    let batch = batches
        .first()
        .ok_or_else(|| format!("query returned no batches: {sql}"))?;
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| format!("query count should be Int64: {sql}"))?;
    Ok(values.value(0))
}

async fn read_summary(table_path: &Path) -> LabResult<TableSummary> {
    let table = open_table(table_url(table_path)?).await?;
    let (_table, stream) = table.scan_table().await?;
    let batches = collect_sendable_stream(stream).await?;

    let mut partition_counts = BTreeMap::new();
    let mut row_count = 0;
    for batch in batches {
        row_count += batch.num_rows();
        for value in string_values(&batch, "partition_date")? {
            *partition_counts.entry(value).or_insert(0) += 1;
        }
    }

    Ok(TableSummary {
        row_count,
        partition_counts,
    })
}

fn string_values(batch: &RecordBatch, column: &str) -> LabResult<Vec<String>> {
    let array = batch
        .column_by_name(column)
        .ok_or_else(|| format!("missing {column} column"))?;

    match array.data_type() {
        ArrowDataType::Utf8 => {
            let values = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| format!("{column} should be Utf8"))?;
            Ok(values
                .iter()
                .map(|value| value.unwrap_or_default().to_string())
                .collect())
        }
        ArrowDataType::Utf8View => {
            let values = array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| format!("{column} should be Utf8View"))?;
            Ok(values
                .iter()
                .map(|value| value.unwrap_or_default().to_string())
                .collect())
        }
        other => Err(format!("{column} should be Utf8, got {other:?}").into()),
    }
}

fn latest_add_summary(table_path: &Path) -> LabResult<AddSummary> {
    let latest_json = latest_log_json(table_path)?;
    let mut partition_keys = BTreeSet::new();
    for line in fs::read_to_string(latest_json)?.lines() {
        let value: Value = serde_json::from_str(line)?;
        let Some(add) = value.get("add") else {
            continue;
        };
        let Some(partition_values) = add.get("partitionValues").and_then(Value::as_object) else {
            continue;
        };
        partition_keys.extend(partition_values.keys().cloned());
    }

    Ok(AddSummary { partition_keys })
}

fn latest_schema_mapping(table_path: &Path) -> LabResult<BTreeMap<String, FieldMapping>> {
    let metadata = latest_metadata_action(table_path)?;
    let schema_string = metadata
        .get("schemaString")
        .and_then(Value::as_str)
        .ok_or("missing metadata schemaString")?;
    let schema: Value = serde_json::from_str(schema_string)?;
    let fields = schema
        .get("fields")
        .and_then(Value::as_array)
        .ok_or("missing schema fields")?;

    let mut mappings = BTreeMap::new();
    for field in fields {
        let logical_name = field
            .get("name")
            .and_then(Value::as_str)
            .ok_or("missing field name")?
            .to_string();
        let metadata = field
            .get("metadata")
            .and_then(Value::as_object)
            .ok_or_else(|| format!("missing metadata for field {logical_name}"))?;
        let physical_name = metadata
            .get(ColumnMetadataKey::ColumnMappingPhysicalName.as_ref())
            .and_then(Value::as_str)
            .ok_or_else(|| format!("missing physical name for field {logical_name}"))?
            .to_string();
        mappings.insert(
            logical_name.clone(),
            FieldMapping {
                logical_name,
                physical_name,
            },
        );
    }

    Ok(mappings)
}

fn activate_column_mapping_for_existing_table(table_path: &Path) -> LabResult {
    let metadata_path = table_path.join("_delta_log/00000000000000000000.json");
    let mut metadata = read_metadata_action(&metadata_path)?;
    metadata["schemaString"] = Value::String(serde_json::to_string(&StructType::try_new(
        table_fields(true, false, true),
    )?)?);
    metadata["configuration"] = json!({
        "delta.columnMapping.mode": "name",
        "delta.columnMapping.maxColumnId": "7"
    });

    let next_version = next_log_version(table_path)?;
    let activation_path = table_path
        .join("_delta_log")
        .join(format!("{next_version:020}.json"));
    let contents = [
        json!({
            "commitInfo": {
                "operation": "SET TBLPROPERTIES",
                "operationParameters": {
                    "properties": "{\"delta.columnMapping.mode\":\"name\"}"
                },
                "isBlindAppend": true,
                "engineInfo": "delta-column-mapping-lab"
            }
        })
        .to_string(),
        json!({"protocol": {"minReaderVersion": 2, "minWriterVersion": 5}}).to_string(),
        json!({"metaData": metadata}).to_string(),
    ]
    .join("\n");
    fs::write(activation_path, format!("{contents}\n"))?;
    Ok(())
}

fn read_metadata_action(path: &Path) -> LabResult<Value> {
    for line in fs::read_to_string(path)?.lines() {
        let value: Value = serde_json::from_str(line)?;
        if let Some(metadata) = value.get("metaData") {
            return Ok(metadata.clone());
        }
    }
    Err(format!("missing metadata action in {}", path.display()).into())
}

fn latest_metadata_action(table_path: &Path) -> LabResult<Value> {
    let mut logs = fs::read_dir(table_path.join("_delta_log"))?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
        .collect::<Vec<_>>();
    logs.sort();

    for path in logs.into_iter().rev() {
        if let Ok(metadata) = read_metadata_action(&path) {
            return Ok(metadata);
        }
    }

    Err(format!("missing metadata action in {}", table_path.display()).into())
}

fn latest_log_json(table_path: &Path) -> LabResult<PathBuf> {
    fs::read_dir(table_path.join("_delta_log"))?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
        .max()
        .ok_or_else(|| "missing Delta log JSON file".into())
}

fn next_log_version(table_path: &Path) -> LabResult<u64> {
    let latest = latest_log_json(table_path)?;
    let stem = latest
        .file_stem()
        .and_then(|value| value.to_str())
        .ok_or("invalid Delta log filename")?;
    Ok(stem.parse::<u64>()? + 1)
}

fn events_batch(path: &Path, include_evolved_columns: bool) -> LabResult<RecordBatch> {
    let mut reader = csv::Reader::from_path(path)?;
    let mut event_ids = Vec::new();
    let mut customer_names = Vec::new();
    let mut amounts = Vec::new();
    let mut partition_dates = Vec::new();
    let mut partition_years = Vec::new();
    let mut partition_months = Vec::new();
    let mut partition_days = Vec::new();
    let mut event_categories = Vec::new();
    let mut source_systems = Vec::new();

    for record in reader.deserialize::<BTreeMap<String, String>>() {
        let record = record?;
        event_ids.push(required(&record, "event_id")?.parse::<i64>()?);
        customer_names.push(required(&record, "customer_name")?);
        amounts.push(required(&record, "amount")?.parse::<f64>()?);
        partition_dates.push(required(&record, "partition_date")?);
        partition_years.push(required(&record, "partition_year")?);
        partition_months.push(required(&record, "partition_month")?);
        partition_days.push(required(&record, "partition_day")?);
        if include_evolved_columns {
            event_categories.push(required(&record, "event_category")?);
            source_systems.push(required(&record, "source_system")?);
        }
    }

    let mut fields = vec![
        Field::new("event_id", ArrowDataType::Int64, false),
        Field::new("customer_name", ArrowDataType::Utf8, true),
        Field::new("amount", ArrowDataType::Float64, true),
        Field::new("partition_date", ArrowDataType::Utf8, false),
        Field::new("partition_year", ArrowDataType::Utf8, false),
        Field::new("partition_month", ArrowDataType::Utf8, false),
        Field::new("partition_day", ArrowDataType::Utf8, false),
    ];
    let mut columns: Vec<Arc<dyn deltalake::arrow::array::Array>> = vec![
        Arc::new(Int64Array::from(event_ids)),
        Arc::new(StringArray::from(customer_names)),
        Arc::new(Float64Array::from(amounts)),
        Arc::new(StringArray::from(partition_dates)),
        Arc::new(StringArray::from(partition_years)),
        Arc::new(StringArray::from(partition_months)),
        Arc::new(StringArray::from(partition_days)),
    ];

    if include_evolved_columns {
        fields.push(Field::new("event_category", ArrowDataType::Utf8, true));
        fields.push(Field::new("source_system", ArrowDataType::Utf8, true));
        columns.push(Arc::new(StringArray::from(event_categories)));
        columns.push(Arc::new(StringArray::from(source_systems)));
    }

    Ok(RecordBatch::try_new(
        Arc::new(ArrowSchema::new(fields)),
        columns,
    )?)
}

fn required(record: &BTreeMap<String, String>, column: &str) -> LabResult<String> {
    record
        .get(column)
        .cloned()
        .ok_or_else(|| format!("missing CSV column {column}").into())
}

fn table_fields(
    column_mapping: bool,
    include_evolved_columns: bool,
    keep_existing_physical_names: bool,
) -> Vec<StructField> {
    let mut definitions = vec![
        ("event_id", PrimitiveType::Long),
        ("customer_name", PrimitiveType::String),
        ("amount", PrimitiveType::Double),
        ("partition_date", PrimitiveType::String),
        ("partition_year", PrimitiveType::String),
        ("partition_month", PrimitiveType::String),
        ("partition_day", PrimitiveType::String),
    ];
    if include_evolved_columns {
        definitions.push(("event_category", PrimitiveType::String));
        definitions.push(("source_system", PrimitiveType::String));
    }

    definitions
        .into_iter()
        .enumerate()
        .map(|(idx, (name, primitive))| {
            let mut field = StructField::new(name, DataType::Primitive(primitive), true);
            if name == "event_id" || name.starts_with("partition_") {
                field.nullable = false;
            }
            if column_mapping {
                let physical_name = if keep_existing_physical_names {
                    name.to_string()
                } else {
                    format!("col-{name}")
                };
                field.metadata.insert(
                    ColumnMetadataKey::ColumnMappingId.as_ref().to_string(),
                    MetadataValue::Number((idx + 1) as i64),
                );
                field.metadata.insert(
                    ColumnMetadataKey::ColumnMappingPhysicalName
                        .as_ref()
                        .to_string(),
                    MetadataValue::String(physical_name),
                );
            }
            field
        })
        .collect()
}

fn column_mapping_configuration(max_column_id: i64) -> [(String, Option<String>); 2] {
    [
        (
            TableProperty::ColumnMappingMode.as_ref().to_string(),
            Some("name".to_string()),
        ),
        (
            "delta.columnMapping.maxColumnId".to_string(),
            Some(max_column_id.to_string()),
        ),
    ]
}

fn partition_columns() -> [&'static str; 3] {
    ["partition_year", "partition_month", "partition_day"]
}

fn column_mapping_protocol() -> LabResult<Protocol> {
    Ok(serde_json::from_value(json!({
        "minReaderVersion": 2,
        "minWriterVersion": 5
    }))?)
}

fn assert_summary(summary: &TableSummary, expected_per_partition: usize) {
    assert_eq!(
        summary.row_count,
        expected_per_partition * PARTITION_DATES.len()
    );
    for partition_date in PARTITION_DATES {
        assert_eq!(
            summary.partition_counts.get(partition_date),
            Some(&expected_per_partition),
            "unexpected row count for {partition_date}"
        );
    }
}

fn assert_hive_partition_exists(table_path: &Path) {
    let hive_partition = table_path.join("partition_year=2026/partition_month=04/partition_day=27");
    assert!(
        hive_partition.exists(),
        "expected Hive partition path {}",
        hive_partition.display()
    );
}

fn assert_physical_partition_keys(summary: &AddSummary) {
    assert!(
        summary
            .partition_keys
            .iter()
            .all(|key| key.starts_with("col-")),
        "expected physical partition keys, got {:?}",
        summary.partition_keys
    );
}

fn assert_logical_partition_keys(summary: &AddSummary) {
    let expected = partition_columns()
        .into_iter()
        .map(str::to_string)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        summary.partition_keys, expected,
        "expected existing partition columns to keep logical physical names"
    );
}

fn assert_fresh_column_mapping_schema(table_path: &Path) -> LabResult {
    let mappings = latest_schema_mapping(table_path)?;
    for logical_name in [
        "event_id",
        "customer_name",
        "amount",
        "partition_date",
        "partition_year",
        "partition_month",
        "partition_day",
        "event_category",
        "source_system",
    ] {
        let mapping = mappings
            .get(logical_name)
            .ok_or_else(|| format!("missing schema field {logical_name}"))?;
        assert_ne!(
            mapping.physical_name, mapping.logical_name,
            "fresh column-mapped field {logical_name} should have a distinct physical name"
        );
    }
    Ok(())
}

fn assert_evolved_column_mapping_schema(table_path: &Path) -> LabResult {
    let mappings = latest_schema_mapping(table_path)?;
    for logical_name in [
        "event_id",
        "customer_name",
        "amount",
        "partition_date",
        "partition_year",
        "partition_month",
        "partition_day",
    ] {
        let mapping = mappings
            .get(logical_name)
            .ok_or_else(|| format!("missing schema field {logical_name}"))?;
        assert_eq!(
            mapping.physical_name, mapping.logical_name,
            "pre-existing field {logical_name} should keep physical name equal to logical name"
        );
    }
    for logical_name in ["event_category", "source_system"] {
        let mapping = mappings
            .get(logical_name)
            .ok_or_else(|| format!("missing evolved schema field {logical_name}"))?;
        assert_ne!(
            mapping.physical_name, mapping.logical_name,
            "evolved field {logical_name} should get a distinct physical name"
        );
        assert!(
            mapping.physical_name.starts_with("col-"),
            "evolved field {logical_name} should get a Delta physical name, got {}",
            mapping.physical_name
        );
    }
    Ok(())
}

fn has_random_prefix_dirs(table_path: &Path) -> bool {
    fs::read_dir(table_path)
        .into_iter()
        .flatten()
        .filter_map(Result::ok)
        .filter_map(|entry| entry.file_name().into_string().ok())
        .any(|name| name.len() == 2 && name.chars().all(|c| c.is_ascii_hexdigit()))
}

fn base_csv() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data/events_base.csv")
}

fn evolved_csv() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data/events_evolved.csv")
}

fn tables_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tables")
}

fn table_url(path: &Path) -> LabResult<Url> {
    Url::from_directory_path(path)
        .map_err(|_| format!("failed to convert path to file URL: {}", path.display()).into())
}

fn reset_dir(path: &Path) -> LabResult {
    if path.exists() {
        fs::remove_dir_all(path)?;
    }
    fs::create_dir_all(path)?;
    Ok(())
}

#[derive(Debug)]
struct TableSummary {
    row_count: usize,
    partition_counts: BTreeMap<String, usize>,
}

#[derive(Debug)]
struct AddSummary {
    partition_keys: BTreeSet<String>,
}

#[derive(Debug)]
struct FieldMapping {
    logical_name: String,
    physical_name: String,
}
