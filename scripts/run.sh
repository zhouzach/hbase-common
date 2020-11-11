
spark-submit \
--class org.rabbit.spark.SparkDataFrameConnector \
--name report_timing \
--master yarn \
--deploy-mode client \
--driver-memory 1g \
--num-executors 8 \
--executor-cores 2 \
--executor-memory 7g \
--conf spark.sql.hive.caseSensitiveInferenceMode=INFER_ONLY \
--conf spark.lineage.enabled=false \
--conf spark.sql.sources.partitionOverwriteMode=dynamic \
--conf spark.executor.memoryOverhead=2048 \
/data/warehouse/batch/hbase-common-1.0.jar