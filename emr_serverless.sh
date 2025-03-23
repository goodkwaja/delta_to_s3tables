#!/bin/bash

# 파라미터 체크
if [ $# -lt 5 ]; then
    echo "Usage: $0 <application_id> <source_db> <target_db> <output_path> <target_format> [namespace]"
    echo "Example:"
    echo "  Delta: $0 00fr3bu10tegoq2p tpcds_1tb tpcds_1tb_delta s3://output-path delta"
    echo "  S3Tables: $0 00fr3bu10tegoq2p tpcds_1tb s3tablesbucket arn:aws:s3tables:region:account:bucket/path s3tables tpcds"
    exit 1
fi

# 파라미터 설정
APPLICATION_ID=$1
SOURCE_DB=$2
TARGET_DB=$3
OUTPUT_PATH=$4
TARGET_FORMAT=$5
NAMESPACE=${6:-""}

# Spark 기본 속성 설정
PROPERTIES='{
    "spark.jars": "s3://2025-jobkorea-data/jars/*",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.hadoop.hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.catalog.'${TARGET_DB}'": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.'${TARGET_DB}'.catalog-impl": "software.amazon.s3tables.iceberg.S3TablesCatalog",
    "spark.executor.cores": "16",
    "spark.executor.memory": "64G",
    "spark.emr-serverless.executor.disk": "1000g",
    "spark.emr-serverless.executor.disk.type": "shuffle_optimized"'

# S3Tables 형식일 경우 warehouse 설정 추가
[ "${TARGET_FORMAT,,}" = "s3tables" ] && \
    PROPERTIES="$PROPERTIES, \"spark.sql.catalog.${TARGET_DB}.warehouse\": \"$OUTPUT_PATH\""

PROPERTIES="$PROPERTIES}"

# 실행 정보 출력
echo "Starting EMR Serverless job with parameters:"
echo "Application ID: $APPLICATION_ID"
echo "Source Database: $SOURCE_DB"
echo "Target Database: $TARGET_DB"
echo "Output Path/Warehouse: $OUTPUT_PATH"
echo "Target Format: $TARGET_FORMAT"
echo "Namespace: $NAMESPACE"
echo "----------------------------------------"

# EMR Serverless 작업 실행
aws emr-serverless start-job-run \
    --application-id "$APPLICATION_ID" \
    --execution-role-arn arn:aws:iam::202786921482:role/emr_serverless \
    --job-driver '{
        "sparkSubmit": {
            "entryPoint": "s3://2025-jobkorea-data/code/to_migration.py",
            "entryPointArguments": [
                "'"$SOURCE_DB"'",
                "'"$TARGET_DB"'",
                "'"$OUTPUT_PATH"'",
                "'"$TARGET_FORMAT"'",
                "'"$NAMESPACE"'"
            ]
        }
    }' \
    --configuration-overrides '{
        "applicationConfiguration": [{
            "classification": "spark-defaults",
            "properties": '"$PROPERTIES"'
        }]
    }' \
    --name "table migration"
