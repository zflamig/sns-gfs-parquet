import boto3
from botocore import UNSIGNED
from botocore.config import Config
from pyarrow import float32, float64, int32, schema, Table
from pyarrow import parquet as pq
import xarray as xr
import numpy as np
import os
import sys
import re
import json
import datetime

TARGET_BUCKET = os.environ.get("TARGET_BUCKET", None)


def response():
    response = {
        "statusCode": 200,
        "headers": {
            # 'Content-Length': len(encoded),
        },
        "isBase64Encoded": True,
        # "body": encoded
    }
    return response


def download_variable(
    key, bucket, variable=":TMP:2 m above ground:", file="/tmp/gfs.grib"
):
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    resp = s3.get_object(Bucket=bucket, Key=key + ".idx")
    index_data = resp["Body"].read().decode()
    byte_start = 0
    byte_end = 0
    index_lines = index_data.split("\n")
    var_indices = [i for i, s in enumerate(index_lines) if variable in s]
    if len(var_indices) == 0:
        print("Your variable {} was not found in the index file!".format(variable))
    if len(var_indices) > 1:
        print(
            "Your variable {} was found multiple times in the index file. Try narrowing down your search!".format(
                variable
            )
        )
    index_start = var_indices[0]
    index_end = index_start + 1
    byte_start = index_lines[index_start].split(":")[1]
    byte_end = index_lines[index_end].split(":")[1]
    resp = s3.get_object(
        Bucket=bucket, Key=key, Range="bytes={}-{}".format(byte_start, byte_end)
    )
    with open(file, "wb") as f:
        f.write(resp["Body"].read())


def convert_to_parquet(key, bucket):
    fields = [
        ("i", int32()),
        ("j", int32()),
        ("longitude", float32()),
        ("latitude", float32()),
        ("t2m", float32()),
    ]
    schema_parquet = schema(fields)

    file = "/tmp/gfs.grib"
    download_variable(key, bucket, file=file)
    ds = xr.open_dataset(file, engine="cfgrib")
    date64 = ds["time"].values
    date = (date64 - np.datetime64("1970-01-01T00:00:00Z")) / np.timedelta64(1, "s")
    date = datetime.datetime.utcfromtimestamp(date)
    run_date = f"{date:%Y-%m-%d-%H}"
    forecast_hour = key[-3:]
    date_valid = date + datetime.timedelta(hours=float(forecast_hour))
    valid_date = f"{date_valid:%Y-%m-%d-%H}"
    x_orig = ds.variables["longitude"].values[:]
    y_orig = ds.variables["latitude"].values[:]
    x = ds.variables["longitude"].values[:]
    y = ds.variables["latitude"].values[:]
    yT = y[np.newaxis].T
    y = np.hstack([yT for num in range(x_orig.size)])
    x = np.vstack([x for num in range(y_orig.size)])
    i = np.arange(x_orig.size)
    j = np.arange(y_orig.size)
    jT = j[np.newaxis].T
    j = np.hstack([jT for num in range(x_orig.size)])
    i = np.vstack([i for num in range(y_orig.size)])

    output = f"/tmp/data.pq"
    outputKey = f"run={run_date}/f={forecast_hour}/data.pq"

    with pq.ParquetWriter(output, schema_parquet, compression="GZIP") as pw:
        data_dict = {
            "i": np.copy(i.flatten()),
            "j": np.copy(j.flatten()),
            "longitude": np.copy(x.flatten()),
            "latitude": np.copy(y.flatten()),
            "t2m": np.copy(ds.variables["t2m"].values[:].flatten()),
        }
        table = Table.from_pydict(data_dict, schema_parquet)
        pw.write_table(table)

    if TARGET_BUCKET is None:
        print("No target bucket configured, not uploading to S3")
    else:
        boto3.resource("s3").Bucket(TARGET_BUCKET).upload_file(output, outputKey)

    os.unlink(file)
    os.unlink(output)


def lambda_handler(event, context):

    print(event["Records"][0]["Sns"]["Message"])

    sns_message = json.loads(event["Records"][0]["Sns"]["Message"])
    s3_event = sns_message["Records"][0]["s3"]

    print("s3_event is", s3_event)

    bucket = s3_event["bucket"]["name"]
    key = s3_event["object"]["key"]
    print("Handling", event, key)

    # key name looks like
    # gfs.20210607/12/atmos/gfs.t12z.pgrb2.0p25.f001

    format_string = "gfs.(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})/(?P<hour1>\d{2})/atmos/gfs.t(?P<hour2>\d{2})z.pgrb2.0p25.f(?P<forecast_hour>\d+)"

    r = re.match(format_string, key)
    if not r:
        print("Not a matching GFS key")
        return response()  # Key doesn't match, probably not GFS data we want

    key_dict = r.groupdict()

    date = datetime.datetime(
        int(key_dict["year"]),
        int(key_dict["month"]),
        int(key_dict["day"]),
        int(key_dict["hour1"]),
    ) + datetime.timedelta(hours=int(key_dict["forecast_hour"]))

    realdate = date.strftime("%Y-%m-%d-%H-%M")

    print("Processing GFS forecast file valid for", realdate)

    convert_to_parquet(key, bucket)

    return response()
