import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# 원본 데이터베이스 이름 설정
RAW_DATABASE = sys.argv[1]
# DELTA 테이블 저장 경로
DELTA_OUTPUT_PATH = sys.argv[2]
# Delta 테이블이 저장될 데이터베이스
DELTA_DATABASE = sys.argv[3]

# Spark 세션 생성
spark = SparkSession.builder.enableHiveSupport().getOrCreate()

def get_partition_columns(database, table_name):
    """파티션 컬럼 이름만 가져오기"""
    return spark.sql(f"DESCRIBE DETAIL {database}.{table_name}") \
        .select("partitionColumns").collect()[0][0]

def glue_register(source_df, target_table, partition_columns, target_path, format = "DELTA"):
    """Delta 테이블을 Glue 카탈로그에 등록"""
    schema_fields, partition_fields = [], []
    for field in source_df.schema.fields:
        field_def = f"`{field.name}` {field.dataType.simpleString()}"
        (partition_fields if field.name in partition_columns else schema_fields).append(field_def)
        
    create_sql = (f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            {', '.join(schema_fields)}
        )
        USING {format}
        {f"PARTITIONED BY ({', '.join(partition_fields)})" if partition_fields else ""}
        LOCATION '{target_path}'
    """)
    spark.sql(create_sql)
    return create_sql

def migrate_to_delta(table_name):
    """Glue 카탈로그 테이블을 Delta Lake로 마이그레이션"""
    # 소스 테이블의 파티션 컬럼 조회
    partition_columns = get_partition_columns(RAW_DATABASE, table_name)
    print(f"Detected partition columns: {partition_columns}")
    
    # 소스 테이블 데이터 읽기
    source_df = spark.table(f"{RAW_DATABASE}.{table_name}")
    print(f"Source table schema: {source_df.printSchema()}")

    # Delta 데이터베이스 생성
    spark.sql(f"""
        CREATE DATABASE IF NOT EXISTS {DELTA_DATABASE}
        LOCATION '{DELTA_OUTPUT_PATH}'
        COMMENT 'Database for Delta Lake tables converted from Raw'
    """)

    target_path = f"{DELTA_OUTPUT_PATH}/{table_name}"
    writer = source_df.write \
        .format("delta") \
        .mode("overwrite")
    if partition_columns:
        writer = writer.partitionBy(*partition_columns)

    writer.save(target_path)

    # 카탈로그 등록
    create_table_sql = glue_register(source_df, f"{DELTA_DATABASE}.{table_name}", partition_columns, target_path)
    spark.sql(create_table_sql)
    print(f"Successfully migrated table: {table_name}")

if __name__ == "__main__":
    try:
        # 원본 데이터베이스의 테이블 목록 가져오기
        tables = spark.catalog.listTables(RAW_DATABASE)

        for table in tables:
            table_name = table.name        
            migrate_to_delta(table_name)
            
    except Exception as e:
        print(f"Migration process failed: {str(e)}")
        sys.exit(1)
