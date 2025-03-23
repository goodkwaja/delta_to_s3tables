[S3 Tables]
aws s3tables list-tables --table-bucket-arn arn:aws:s3tables:ap-northeast-2:202786921482:bucket/jobkorea-test --namespace <namespace>
aws s3tables delete-table --table-bucket-arn arn:aws:s3tables:ap-northeast-2:202786921482:bucket/jonkorea-data --namespace name --name <tablename>
aws s3tables list-namespaces --table-bucket-arn arn:aws:s3tables:ap-northeast-2:202786921482:bucket/jobkorea-test
aws s3tables delete-namespace --table-bucket-arn arn:aws:s3tables:ap-northeast-2:202786921482:bucket/jobkorea-test --namespace namespace  
aws s3tables delete-table-bucket --table-bucket-arn arn:aws:s3tables:ap-northeast-2:202786921482:bucket/jobkorea-tests

=========================================================================================================================
최종 코드 커맨드
=========================================================================================================================

====================
EMR Serverless
====================
aws emr-serverless start-job-run \
  --application-id 00fr3bu10tegoq2p \
  --execution-role-arn arn:aws:iam::202786921482:role/emr_serverless \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://2025-jobkorea-data/code/to_migration.py",
      "entryPointArguments": [
        "tpcds_1tb",
        "tpcds_1tb_delta_test",
        "s3://2025-jobkorea-data/data/1tb_delta_test",
        "delta",
        "namespace"
      ]
    }
  }' \
  --configuration-overrides '{
    "applicationConfiguration": [{
      "classification": "spark-defaults",
      "properties": {
        "spark.jars.packages": "software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.3",
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.hadoop.hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.hive.HiveCatalog",
        "spark.sql.catalog.s3tablesbucket": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.s3tablesbucket.catalog-impl": "software.amazon.s3tables.iceberg.S3TablesCatalog",
        "spark.sql.catalog.s3tablesbucket.warehouse": "arn:aws:s3tables:ap-northeast-2:202786921482:bucket/tpcds-1tb-iceberg"
      }
    }]
  }' \
  --name "table migration"

./emr_serverless.sh \
  00fr3bu10tegoq2p \
  tpcds_1tb \
  tpcds_1tb_delta \
  s3://2025-jobkorea-data/data/1tb_delta \
  delta

./emr_serverless.sh \
  00fr3bu10tegoq2p \
  tpcds_1tb_delta \
  s3tablesbucket \
  arn:aws:s3tables:ap-northeast-2:202786921482:bucket/s3tables-demo \
  s3tables \
  tpcds_1tb
  

====================
emr add-steps
====================
aws emr add-steps \
  --cluster-id j-E9QLG82SGY8I \
  --steps '[{
    "Name": "table migration",
    "ActionOnFailure": "CONTINUE",
    "HadoopJarStep": {
      "Jar": "command-runner.jar",
      "Args": [
        "spark-submit",
        "--deploy-mode", "cluster",
        "--packages", "software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.3",
        "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "--conf", "spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
        "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.hive.HiveCatalog",
        "--conf", "spark.sql.catalog.s3tablesbucket=org.apache.iceberg.spark.SparkCatalog",
        "--conf", "spark.sql.catalog.s3tablesbucket.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog",
        "--conf", "spark.sql.catalog.s3tablesbucket.warehouse=arn:aws:s3tables:ap-northeast-2:202786921482:bucket/tpcds-1tb-iceberg",
        "s3://2025-jobkorea-data/code/to_migration.py",
        "tpcds_1tb",
        "tpcds_1tb_delta_test",
        "s3://2025-jobkorea-data/data/1tb_delta_test",
        "delta",
        "namespace"
      ]
    }
  }]'


./emr_steps.sh \
  j-E9QLG82SGY8I  \
  tpcds_1tb_delta \
  s3tablesbucket \
  arn:aws:s3tables:ap-northeast-2:202786921482:bucket/s3tables-demo \
  s3tables \
  tpcds_1tb


====================
spark-submit
====================
spark-submit \
  --deploy-mode client \
  --packages "software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.3" \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
  --conf "spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.sql.catalog.s3tablesbucket=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.s3tablesbucket.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog" \
  --conf "spark.sql.catalog.s3tablesbucket.warehouse=arn:aws:s3tables:ap-northeast-2:202786921482:bucket/tpcds-1tb-iceberg" \
  s3://2025-jobkorea-data/code/to_migration.py \
  tpcds_1tb \
  tpcds_1tb_delta_test \
  s3://2025-jobkorea-data/data/1tb_delta_test \
  delta
  namespace

./emr_submit.sh \
  j-E9QLG82SGY8I  \
  tpcds_1tb_delta \
  s3tablesbucket \
  arn:aws:s3tables:ap-northeast-2:202786921482:bucket/s3tables-demo \
  s3tables \
  tpcds_1tb
