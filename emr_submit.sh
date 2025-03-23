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

# Spark 기본 설정
SPARK_CONFIGS=(
    "--deploy-mode" "client"
    "--packages" "software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.3"
    "--conf" "spark.jars=/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar,s3://2025-jobkorea-data/jars/*"
    "--conf" "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    "--conf" "spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
    "--conf" "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
    "--conf" "spark.sql.catalog.${TARGET_DB}=org.apache.iceberg.spark.SparkCatalog"
    "--conf" "spark.sql.catalog.${TARGET_DB}.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog"
    "--conf" "spark.sql.defaultCatalog=${TARGET_DB}"
)

# S3Tables 형식일 경우 warehouse 설정 추가
[ "${TARGET_FORMAT,,}" = "s3tables" ] && \
    SPARK_CONFIGS+=("--conf" "spark.sql.catalog.${TARGET_DB}.warehouse=$OUTPUT_PATH")

# 실행 정보 출력
echo "Starting table migration with parameters:"
echo "Cluster ID: $CLUSTER_ID"
echo "Source Database: $SOURCE_DB"
echo "Target Database: $TARGET_DB"
echo "Output Path/Warehouse: $OUTPUT_PATH"
echo "Target Format: $TARGET_FORMAT"
echo "Namespace: $NAMESPACE"
echo "----------------------------------------"

# Spark Submit 실행
spark-submit "${SPARK_CONFIGS[@]}" \
    ./to_migration.py \
    "$SOURCE_DB" \
    "$TARGET_DB" \
    "$OUTPUT_PATH" \
    "$TARGET_FORMAT" \
    "$NAMESPACE"

# 종료 상태 확인
[ $? -eq 0 ] && echo "Migration completed successfully" || { echo "Migration failed"; exit 1; }
