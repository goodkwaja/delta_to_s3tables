import os
import sys
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from databricks.connect import DatabricksSession
import boto3

glue_client = boto3.client('glue')
spark = DatabricksSession.builder.serverless(True).getOrCreate()

def create_database(database_name):
    try:
        glue_client.create_database(DatabaseInput={'Name': database_name})
    except glue_client.exceptions.AlreadyExistsException:
        pass

def create_table(database_name, table_name, columns, detail, partition_keys):
    try:
        glue_client.delete_table(DatabaseName=database_name, Name=table_name)
    except glue_client.exceptions.EntityNotFoundException:
        pass
        
    glue_client.create_table(
        DatabaseName=database_name,
        TableInput={
            'Name': table_name,
            'StorageDescriptor': {
                'Columns': columns,
                'Location': detail.location if detail.location else '',
                'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                'SerdeInfo': {
                    'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                    'Parameters': {'serialization.format': '1'}
                }
            },
            'PartitionKeys': partition_keys,
            'TableType': 'EXTERNAL_TABLE',
            'Parameters': {
                'classification': 'delta',
                'delta.minReaderVersion': str(detail.minReaderVersion),
                'delta.minWriterVersion': str(detail.minWriterVersion)
            }
        }
    )
    print(f"Created table: {database_name}.{table_name}")

def process_table(table):
    try:
        # 기본 정보 추출
        catalog_name = table.table_catalog
        schema_name = table.table_schema
        table_name = table.table_name
        source_path = f"{catalog_name}.{schema_name}.{table_name}"
        database_name = f"dbx_{catalog_name}_{schema_name}"
        
        # 테이블 메타데이터 수집
        detail = spark.sql(f"DESCRIBE DETAIL {source_path}").collect()[0]
        describe_result = spark.sql(f"DESCRIBE {source_path}").collect()
        
        # 컬럼 정보 처리
        column_dict = {
            row['col_name']: {'Name': row['col_name'], 'Type': row['data_type']}
            for row in describe_result
            if row['col_name'] and not row['col_name'].startswith('#')
        }
        
        # 일반 컬럼과 파티션 컬럼 분리
        columns = [
            col for name, col in column_dict.items() 
            if name not in detail.partitionColumns
        ]
        
        partition_keys = [
            column_dict[name] 
            for name in detail.partitionColumns 
            if name in column_dict
        ]
        
        # 데이터베이스 및 테이블 생성
        create_database(database_name)
        create_table(database_name, table_name, columns, detail, partition_keys)
        return True
        
    except Exception as e:
        print(f"Error processing {source_path}: {e}")
        return False

def main():
    try:
        start_time = datetime.now()
        successful = 0
        failed = 0
        
        catalogs = ["workspace", "samples"]
        tables = spark.sql(f"""
            SELECT *
            FROM system.information_schema.tables
            WHERE table_catalog IN ({', '.join(f"'{c}'" for c in catalogs)})
              AND data_source_format = 'DELTA'
        """).collect()
        
        # I/O 바운드 작업이므로 CPU 코어 수 * 4
        workers = min(len(tables), os.cpu_count() * 4)
        
        with ThreadPoolExecutor(max_workers=workers) as executor:
            results = list(executor.map(process_table, tables))
            successful = sum(1 for x in results if x)
            failed = sum(1 for x in results if not x)
                
        duration = datetime.now() - start_time
        print(f"\nMigration Summary:")
        print(f"Total Processed: {len(tables)}")
        print(f"Successful: {successful}")
        print(f"Failed: {failed}")
        print(f"Duration: {duration}")
        
    except Exception as e:
        print(f"Migration failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
