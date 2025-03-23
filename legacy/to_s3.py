import sys
from pyspark.sql import SparkSession


spark = SparkSession.builder.enableHiveSupport().getOrCreate()

DELTA_DATABASE = "spark_catalog.tpcds_1tb_delta"
NAMESPACE_S3TABLES = ""

def get_partition_info(database_name, table_name):
    """테이블의 파티션 정보 조회"""
    try:
        table_info = spark.sql(f"DESCRIBE {database_name}.{table_name}")
        
        partition_info = []
        is_partition_section = False
        
        for row in table_info.collect():
            if row['col_name'].startswith('# Partition'):
                is_partition_section = True
                continue
            if is_partition_section and not row['col_name'].startswith('#'):
                partition_info.append((row['col_name'], row['data_type']))
                
        return partition_info
        
    except Exception as e:
        print(f"Error getting partition info: {str(e)}")
        raise


def migrate_to_S3tables(table_name):
    """Delta Lake 테이블을 Iceberg로 변환"""
    print(f"\nProcessing table: {table_name}")
    
    # 파티션 정보 조회
    partition_info = get_partition_info(DELTA_DATABASE, table_name)
    partition_columns = [p[0] for p in partition_info]
    print(f"Detected partition columns: {partition_columns}")
    
    # 테이블 위치 가져오기
    table_location = spark.sql(f"DESCRIBE FORMATTED {DELTA_DATABASE}.{table_name}") \
        .filter("col_name = 'Location'") \
        .select("data_type") \
        .collect()[0][0].strip()
        
    print(f"Reading Delta table from location: {table_location}")
    
    # Delta Lake 테이블 읽기
    source_df = spark.read \
        .format("delta") \
        .load(table_location)
    
    print("SPARK SESSION READY")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS s3tablesbucket.tpc")
    print("NAMESPACE CREATED")
    
    # Iceberg 테이블로 저장
    writer = source_df.write \
        .format("iceberg") \
        .mode("overwrite") \
    
    if partition_columns:
        writer = writer.partitionBy(*partition_columns)
    writer.saveAsTable(f"s3tablesbucket.tpc.{table_name}")

if __name__ == "__main__":
    try:
        # Delta Lake 데이터베이스의 테이블 목록 조회
        tables = spark.catalog.listTables(DELTA_DATABASE)
        for table in tables:
            migrate_to_S3tables(table.name)
            break
        print("\nMigration completed successfully!")
        
    except Exception as e:
        print(f"Migration process failed: {str(e)}")
        sys.exit(1)
