#!/bin/bash

# 파라미터 체크
if [ $# -lt 6 ]; then
    echo "Usage: $0 <cluster_id> <source_db> <target_db> <output_path> <target_format> [namespace]"
    echo "Example:"
    echo "  Delta: $0 j-XXXXX tpcds_1tb tpcds_1tb_delta s3://output-path delta"
    echo "  S3Tables: $0 j-XXXXX tpcds_1tb s3tablesbucket arn:aws:s3tables:region:account:bucket/path s3tables tpcds"
    exit 1
fi

# 파라미터 설정
CLUSTER_ID=$1
SOURCE_DB=$2
TARGET_DB=$3
OUTPUT_PATH=$4
TARGET_FORMAT=$5
NAMESPACE=${6:-""}

# EMR Step 실행
aws emr add-steps \
    --cluster-id "$CLUSTER_ID" \
    --steps '[{
        "Type": "CUSTOM_JAR",
        "Name": "Table Migration",
        "ActionOnFailure": "CONTINUE",
        "Jar": "command-runner.jar",
        "Args": [
            "spark-submit",
            "--deploy-mode", "cluster",
            "--packages", "software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.3",
            "--conf", "spark.jars=/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar,s3://2025-jobkorea-data/jars/*",
            "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "--conf", "spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
            "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "--conf", "spark.sql.catalog.'${TARGET_DB}'=org.apache.iceberg.spark.SparkCatalog",
            "--conf", "spark.sql.catalog.'${TARGET_DB}'.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog",
            "--conf", "spark.sql.defaultCatalog='${TARGET_DB}'",
            "--conf", "spark.sql.catalog.'${TARGET_DB}'.warehouse='${OUTPUT_PATH}'",
            "s3://2025-jobkorea-data/code/to_migration.py",
            "'${SOURCE_DB}'",
            "'${TARGET_DB}'",
            "'${OUTPUT_PATH}'",
            "'${TARGET_FORMAT}'",
            "'${NAMESPACE}'"
        ]
    }]'
