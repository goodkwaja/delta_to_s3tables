import sys
from pyspark.sql import SparkSession

# Delta Lake 데이터베이스
DELTA_DATABASE = f"spark_catalog.{sys.argv[1]}"

# S3 Tables 저장 경로
ARN_S3TABLES_PATH = sys.argv[2]

# S3 Tables namespace
NAMESPACE_S3TABLES = sys.argv[3]

# EMR Serverless용 Spark 세션 생성
spark = SparkSession.builder \
    .config("spark.jars", "/usr/share/aws/delta/lib/delta-spark.jar,/usr/share/aws/delta/lib/delta-storage.jar,s3://2025-jobkorea-data/jars/*") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
    .config("spark.sql.catalog.s3tablesbucket", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.s3tablesbucket.catalog-impl", "software.amazon.s3tables.iceberg.S3TablesCatalog") \
    .config("spark.sql.catalog.s3tablesbucket.warehouse", ARN_S3TABLES_PATH) \
    .config("spark.sql.catalog.defaultCatalog", "s3tablesbucket") \
    .config("spark.sql.catalog.s3tablesbucket.client.region", "ap-northeast-2") \
    .enableHiveSupport() \
    .getOrCreate()

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
    try:
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


        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE_S3TABLES}")
        
        # # Iceberg 데이터베이스 생성
        # spark.sql(f"""
        #     CREATE DATABASE IF NOT EXISTS {ICEBERG_DATABASE}
        #     LOCATION '{ICEBERG_OUTPUT_PATH}'
        #     COMMENT 'Database for Iceberg tables converted from Delta Lake'
        # """)
        
        # # 테이블이 이미 존재하면 삭제
        # spark.sql(f"DROP TABLE IF EXISTS {ICEBERG_DATABASE}.{table_name}")
        
        # # Iceberg 테이블로 저장
        # writer = source_df.write \
        #     .format("iceberg") \
        #     .mode("overwrite") \

        # if partition_columns:
        #     writer = writer.partitionBy(*partition_columns)
            
        # writer.saveAsTable(f"{ICEBERG_DATABASE}.{table_name}")
        # print(f"Successfully created S3 tables: {ICEBERG_DATABASE}.{table_name}")
        
        # # 테이블 정보 확인
        # result_df = spark.table(f"{ICEBERG_DATABASE}.{table_name}")
        
        # if partition_columns:
        #     print("\nPartition information:")
        #     result_df.groupBy(*partition_columns).count().show()
            
    except Exception as e:
        print(f"Error migrating table {table_name}: {str(e)}")
        raise


if __name__ == "__main__":
    try:
        # Delta Lake 데이터베이스의 테이블 목록 조회
        tables = spark.catalog.listTables(DELTA_DATABASE)
        for table in tables:
            migrate_to_S3tables(table.name)
            
        print("\nMigration completed successfully!")
        
    except Exception as e:
        print(f"Migration process failed: {str(e)}")
        sys.exit(1)