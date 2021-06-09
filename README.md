# Converting GFS Data to Parquet When It Arrives

This example creates an AWS Lambda for extracting the 2-meter temperature field from GFS and converting it into the columnar parquet format. The AWS Lambda container image is built with AWS CodePipeline storing the image in Amazon ECR and doing continuous deployment of the AWS Lambda function.

The function subscribes to the public Amazon SNS feeds available for the [NOAA GFS data from the Registry of Open Data on AWS](https://registry.opendata.aws/noaa-gfs-bdp-pds/).

## Docker usage

```
docker build -t gfs .
```

Run the container

```
docker run --rm -p 9000:8080 gfs
```

Test it out

```
curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" \
  -d @- <<EOF
        {"Records": [{"Sns": {"Message": "{\"Records\":[{\"s3\":{\"bucket\":{\"name\":\"noaa-gfs-bdp-pds\"},\"object\":{\"key\":\"gfs.20210607/12/atmos/gfs.t12z.pgrb2.0p25.f001\"}}}]}"}}]}
EOF
```

