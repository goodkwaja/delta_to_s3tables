import sys
from pyspark.sql import SparkSession

# 필수 파라미터 체크
if len(sys.argv) < 5:
    print("Usage: script.py <source_db> <target_db> <output_path> <target_format> [namespace]")
    sys.exit(1)

# 설정값
SOURCE_DATABASE = sys.argv[1]      # 소스 데이터베이스
TARGET_DATABASE = sys.argv[2]      # 대상 데이터베이스
OUTPUT_PATH = sys.argv[3]          # 대상 저장 경로
TARGET_FORMAT = sys.argv[4]        # "delta" 또는 "iceberg" 또는 "s3tables"

# 저장할 S3Tables Namespace
NAMESPACE = sys.argv[5] if len(sys.argv) > 5 else "tpcds"

# Spark 세션 생성
spark = SparkSession.builder.enableHiveSupport().getOrCreate()

def get_partition_columns(database, table_name):
    """파티션 컬럼 이름만 가져오기"""
    return spark.sql(f"DESCRIBE DETAIL {database}.{table_name}") \
        .select("partitionColumns").collect()[0][0]

def glue_register(source_df, target_table, partition_columns, target_path, format="DELTA"):
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

def migrate_table(table_name):
    """테이블 변환 (Raw → Delta/Iceberg/S3Tables)"""
    print(f"\nProcessing table: {table_name}")
    
    # 소스 테이블의 파티션 컬럼 조회
    partition_columns = get_partition_columns(SOURCE_DATABASE, table_name)
    print(f"Detected partition columns: {partition_columns}")
    
    # 소스 테이블 데이터 읽기
    source_df = spark.table(f"{SOURCE_DATABASE}.{table_name}")
    print(f"Source table schema: {source_df.printSchema()}")
    
    # S3Tables 형식인 경우 Iceberg로 변환
    data_format = "iceberg" if TARGET_FORMAT.lower() == "s3tables" else TARGET_FORMAT.lower()

    if TARGET_FORMAT.lower() == "s3tables":
        # s3tables 일때 네임스페이스 생성 
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {TARGET_DATABASE}.{NAMESPACE}")
        spark.sql(f"DROP TABLE IF EXISTS {TARGET_DATABASE}.{NAMESPACE}.{table_name} PURGE")
        target_table = f"{TARGET_DATABASE}.{NAMESPACE}.{table_name}"
    else:
        # 대상 데이터베이스 생성
        spark.sql(f"""
            CREATE DATABASE IF NOT EXISTS {TARGET_DATABASE}
            LOCATION '{OUTPUT_PATH}'
            COMMENT 'Database for {data_format} tables'
        """)
        target_table = f"{TARGET_DATABASE}.{table_name}"

    print(f"\ntarget_table: {target_table}")
    # 데이터 저장
    writer = source_df.write \
        .format(data_format) \
        .mode("overwrite")
    
    if partition_columns:
        writer = writer.partitionBy(*partition_columns)
    
    if data_format == "delta":
        # Delta 일때 save + glue_register
        target_path = f"{OUTPUT_PATH}/{table_name}"
        writer.save(target_path)
        create_table_sql = glue_register(source_df, target_table, 
                                       partition_columns, target_path)
        print(f"\nRegister Glue Catalog : {create_table_sql}")
    else:
        # Iceberg/S3Tabels 일때 saveAsTable
        writer.saveAsTable(target_table)
    
    print(f"Successfully migrated table: {table_name}")

if __name__ == "__main__":
    try:
        target_format = TARGET_FORMAT.lower()
        if target_format not in ["delta", "iceberg", "s3tables"]:
            raise ValueError(f"Unsupported target format: {TARGET_FORMAT}")
            
        if target_format == "s3tables" and not NAMESPACE:
            raise ValueError("Namespace is required for S3Tables format")
        
        print(f"Starting migration from {SOURCE_DATABASE} to {TARGET_DATABASE}")
        print(f"Target format: {target_format}")
        print(f"Target path: {OUTPUT_PATH}")
        print(f"Namespace: {NAMESPACE}")
           
        # 소스 데이터베이스의 테이블 목록 가져오기
        tables = spark.catalog.listTables(SOURCE_DATABASE)
        
        for table in tables:
            migrate_table(table.name)
            
        print("\nMigration completed successfully!")
        
    except Exception as e:
        print(f"Migration process failed: {str(e)}")
        sys.exit(1)
