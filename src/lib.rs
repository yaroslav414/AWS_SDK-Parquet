use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use aws_config::{BehaviorVersion, Region, retry::RetryConfig};
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use awscreds::Credentials;
use aws_sdk_s3::Client;
use aws_smithy_types::timeout::TimeoutConfig;
use aws_sdk_s3::config::Builder;
use datafusion::arrow::pyarrow::PyArrowType;
use datafusion::arrow::array::RecordBatch;
use datafusion::prelude::*;
use futures_util::TryStreamExt;
use object_store::aws::AmazonS3Builder;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use pyo3::prelude::*;
use url::Url;

const AWS_MAX_RETRIES: u32 = 10;

#[pyfunction]
fn read_data(py: Python<'_>, region: String, bucket: String, key: String, partition: Option<String>, sql: Option<String>) -> PyResult<PyArrowType<Vec<RecordBatch>>> {
    let res = pyo3_asyncio::tokio::run(py, async move {
        let ctx = SessionContext::new();
        let df = run_data_read(ctx, &region, &bucket, &key, partition.as_deref(), sql.as_deref()).await?;
        let batches = df.collect().await?;

        Ok(batches)
    }); 

    Ok(res?.into())
}

#[pyfunction]
fn scan_data(py: Python<'_>, region: String, bucket: String, key: String, sql: Option<String>) -> PyResult<PyArrowType<Vec<RecordBatch>>> {
    let res = pyo3_asyncio::tokio::run(py, async move {
        let ctx = SessionContext::new();
        let df = run_data_scan(ctx, &region, &bucket, &key, sql.as_deref()).await?;
        let batches = df.collect().await?;

        Ok(batches)
    }); 

    Ok(res?.into())
}

#[pymodule]
fn datalake_sdk(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(scan_data, m)?)?;
    m.add_function(wrap_pyfunction!(read_data, m)?)?;

    Ok(())
}

pub async fn run_data_scan(ctx: SessionContext, region: &str, bucket: &str, key: &str, sql: Option<&str>) -> Result<DataFrame> {
    println!("start scaning data lake: {} sql: {:?}", key, sql);
    let res = scan_data_from_s3(ctx, region, bucket, key, sql).await?;

    Ok(res)
}

pub async fn run_data_read(ctx: SessionContext, region: &str, bucket: &str, key: &str, partition: Option<&str>, sql: Option<&str>) -> Result<DataFrame> {
    println!("start reading data lake: {} sql: {:?}", key, sql);

    let client = get_aws_client(region).await;

    if let Some(partition) = partition {
        println!("parsing sql query");
        let partition = partition.split_whitespace().collect::<Vec<_>>();
        let partition = partition.join("").replace("'", "");
        let prefix = format!("{key}{partition}");

        let res = read_data_from_s3(client, ctx, bucket, &prefix, sql).await?;

        Ok(res)
    } else {
        let res = ctx.read_empty()?;

        Ok(res)
    }
}


pub async fn get_aws_client(region: &str) -> Client {
    let region = Region::new(region.to_string());

    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .region(region)
        .load()
        .await;

    let timeout= TimeoutConfig::builder()
        .operation_timeout(Duration::from_secs(60 * 5))
        .operation_attempt_timeout(Duration::from_secs(60 * 5))
        .connect_timeout(Duration::from_secs(60 * 5))
        .build();

    let config_builder = Builder::from(&sdk_config)
        .timeout_config(timeout)
        .retry_config(RetryConfig::standard().with_max_attempts(AWS_MAX_RETRIES));

    let config = config_builder.build();
   
    Client::from_conf(config)
}

pub async fn get_aws_object(client: Client, bucket: &str, key: &str) -> Result<GetObjectOutput> {
    let req = client
        .get_object()
        .bucket(bucket)
        .key(key);

    let res = req.send().await?;

    Ok(res)
}

pub async fn read_file(client: Client, bucket: &str, key: &str) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    let mut object = get_aws_object(client, bucket, key).await?;
    while let Some(bytes) = object.body.try_next().await? {
        buf.extend(bytes.to_vec());
    }

    Ok(buf)
}

pub fn select_all_exclude(df: DataFrame, to_exclude: &[&str]) -> Result<DataFrame> {
    let columns = df
        .schema()
        .fields()
        .iter()
        .map(|x| x.name().as_str())
        .filter(|x| !to_exclude.iter().any(|col| col.eq(x)))
        .collect::<Vec<_>>();
    
    let res = df.clone().select_columns(&columns)?;

    Ok(res)
}

pub async fn list_keys(client: Client, bucket: &str, prefix: &str) -> Result<Vec<String>> {
	let mut stream = client
        .list_objects_v2()
        .prefix(prefix)
        .bucket(bucket)
        .into_paginator()
        .send();
    
	let mut files = Vec::new();
    while let Some(objects) = stream.next().await.transpose()? {        
        for obj in objects.contents() {
            if !obj.key().unwrap_or("no_file_name").ends_with('/') {
                let file_name = obj.key().unwrap_or("no_file_name").to_string();
                files.push(file_name);
            }
        }
    }

	Ok(files)
}

async fn file_to_df(client: Client, ctx: SessionContext, bucket: String, key: String) -> Result<DataFrame> {
    let data = read_file(client, &bucket, &key).await?;
    let stream = ParquetRecordBatchStreamBuilder::new(Cursor::new(data)).await?.build()?;
    let batches = stream.try_collect::<Vec<_>>().await?;
    let df = ctx.read_batches(batches)?;

    Ok(df)
}

async fn concat_dfs(ctx: SessionContext, dfs: Vec<DataFrame>) -> Result<DataFrame> {
    let mut batches = vec![];
    for df in dfs {
        let batch = df.collect().await?;
        batches.extend(batch);
    }
    let res = ctx.read_batches(batches)?;

    Ok(res)
}

async fn df_sql(df: DataFrame, sql: &str) -> Result<DataFrame> {
    // let filter = df.parse_sql_expr(sql)?;

    // let res = df.filter(filter)?;

    // Ok(res)

    todo!()
}

async fn read_data_from_s3(client: Client, ctx: SessionContext, bucket: &str, prefix: &str, sql: Option<&str>) -> Result<DataFrame> {
    // println!("parsing sql query");
    // let partition = partition.split_whitespace().collect::<Vec<_>>();
    // let partition = partition.join("").replace("'", "");

    println!("collecting files for prefix: {}", prefix);
    // let prefix = format!("{key}/{partition}");
    let files = list_keys(client.clone(), bucket, &prefix).await?;
    println!("found files: {:?}", files);
    let mut tasks = vec![];
    let mut dfs = vec![];
    println!("reading files for partition: {}", prefix);
    for file in files {
        let task = tokio::spawn(file_to_df(client.clone(), ctx.clone(), bucket.to_string(), file));
        tasks.push(task);
    }
    for res in tasks {
        let df = res.await??;
        dfs.push(df);
    }

    println!("concating dfs");
    let res = concat_dfs(ctx, dfs).await?;
    // let res = df_sql(res, sql).await?;
    
    Ok(res)
}

async fn scan_data_from_s3(ctx: SessionContext, region: &str, bucket: &str, key: &str, sql: Option<&str>) -> Result<DataFrame> {
    let creds = Credentials::default()?;
    let aws_access_key_id = creds.access_key.unwrap_or_default();
    let aws_secret_access_key = creds.secret_key.unwrap_or_default();
    let aws_session_token = creds.session_token.unwrap_or_default();

    let s3 = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region(region)
        .with_access_key_id(aws_access_key_id)
        .with_secret_access_key(aws_secret_access_key)
        .with_token(aws_session_token)
        .build()?;

    let path = format!("s3://{bucket}");
    let s3_url = Url::parse(&path)?;
    ctx.runtime_env().register_object_store(&s3_url, Arc::new(s3));

    let path = format!("s3://{bucket}/{key}");
    ctx.register_parquet("t", &path, ParquetReadOptions::default()).await?;
    
    let sql = match sql {
        Some(v) => format!("select * from t where {v}"),
        None => "select * from t".to_string(),
    };

    let res = ctx.sql(&sql).await?;

    Ok(res)
}