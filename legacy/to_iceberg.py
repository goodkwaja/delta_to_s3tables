import sys
from pyspark.sql import SparkSession

# Delta Lake 데이터베이스
DELTA_DATABASE = sys.argv[1]
# Iceberg 테이블 저장 경로
ICEBERG_OUTPUT_PATH = sys.argv[2] 
# Iceberg 테이블이 저장될 데이터베이스
ICEBERG_DATABASE = f"aws_glue.{sys.argv[3]}"

# EMR Serverless용 Spark 세션 생성
spark = SparkSession.builder \
    .config("spark.jars", "/usr/share/aws/delta/lib/delta-spark.jar,/usr/share/aws/delta/lib/delta-storage.jar,/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.catalog.aws_glue", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.aws_glue.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.aws_glue.warehouse", {ICEBERG_OUTPUT_PATH}) \
    .config("spark.hadoop.hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
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

def migrate_to_iceberg(table_name):
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
            
        # Iceberg 테이블 생성 및 데이터 저장
        target_path = f"{ICEBERG_OUTPUT_PATH}/{table_name}"
        
        # Iceberg 데이터베이스 생성
        spark.sql(f"""
            CREATE DATABASE IF NOT EXISTS {ICEBERG_DATABASE}
            LOCATION '{ICEBERG_OUTPUT_PATH}'
            COMMENT 'Database for Iceberg tables converted from Delta Lake'
        """)
        
        # 테이블이 이미 존재하면 삭제
        spark.sql(f"DROP TABLE IF EXISTS {ICEBERG_DATABASE}.{table_name}")
        
        # Iceberg 테이블로 저장
        writer = source_df.write \
            .format("iceberg") \
            .mode("overwrite") \
        
        if partition_columns:
            writer = writer.partitionBy(*partition_columns)
        
        writer.saveAsTable(f"{ICEBERG_DATABASE}.{table_name}")

        print(f"Successfully created Iceberg table: {ICEBERG_DATABASE}.{table_name}")            
    except Exception as e:
        print(f"Error migrating table {table_name}: {str(e)}")
        raise
        
if __name__ == "__main__":
    try:
        # Delta Lake 데이터베이스의 테이블 목록 조회
        tables = spark.catalog.listTables(DELTA_DATABASE)
        
        print(f"Starting migration from Delta Lake to Iceberg")
        print(f"Source database: {DELTA_DATABASE}")
        print(f"Target database: {ICEBERG_DATABASE}")
        print(f"Target path: {ICEBERG_OUTPUT_PATH}")
        
        for table in tables:
            migrate_to_iceberg(table.name)
            
        print("\nMigration completed successfully!")
        
    except Exception as e:
        print(f"Migration process failed: {str(e)}")
        sys.exit(1)
