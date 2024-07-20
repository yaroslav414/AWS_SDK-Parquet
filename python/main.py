import datalake_sdk
import datafusion 
import time


def scan_data_example():
    region = "region"
    bucket = "bucket"
    key = "path/to/data/" # key or folder
    sql = "foo = 'bar'" # where clause comes here
    batches = datalake_sdk.scan_data(region, bucket, key, sql)
    ctx = datafusion.SessionContext()
    df = ctx.create_dataframe([batches])
    df = df.to_polars()
    print(len(df))
    print(df.head(10))


def read_data_example():
    region = "region"
    bucket = "bucket"
    key = "path/to/data/" # key or folder
    partition = "'foo'" # partion (folder) that contain parquet files
    batches = datalake_sdk.read_data(region, bucket, key, partition, None)
    ctx = datafusion.SessionContext()
    df = ctx.create_dataframe([batches])
    df = df.to_polars()
    print("df len: ", len(df))
    print(df.head(10))


if __name__  == "__main__":
    start_time = time.time()

    # scan_data_example()
    # read_data_example()

    print("exec time: {:.2f} sec".format(time.time() - start_time))
