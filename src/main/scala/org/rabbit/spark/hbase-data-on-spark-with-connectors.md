
https://docs.cloudera.com/HDPDocuments/HDP2/HDP-2.6.4/bk_spark-component-guide/content/ch08s05.html

Software connectors are architectural elements in the cluster that facilitate interaction between different Hadoop components. 
For real-time and near-real-time data analytics, there are connectors that bridge the gap between the HBase key-value store and complex relational SQL queries that Spark supports. 
Developers can enrich applications and interactive tools with connectors because connectors allow operations such as complex SQL queries on top of an HBase table inside Spark and table JOINs against data frames.

[Important]	Important
The HDP bundle includes two different connectors that extract datasets out of HBase and streams them into Spark:

Hortonworks Spark-HBase Connector

RDD-Based Spark-HBase Connector: a connector from Apache HBase that uses resilient distributed datasets (RDDs)

​Selecting a Connector
The two connectors are designed to meet the needs of different workloads. In general, use the Hortonworks Spark-HBase Connector for SparkSQL, DataFrame, and other fixed schema workloads. Use the RDD-Based Spark-HBase Connector for RDDs and other flexible schema workloads.

Hortonworks Spark-HBase Connector

When using the connector developed by Hortonworks, the underlying context is data frame, with support for optimizations such as partition pruning, predicate pushdowns, and scanning. The connector is highly optimized to push down filters into the HBase level, speeding up workload. The tradeoff is limited flexibility because you must specify your schema upfront. The connector leverages the standard Spark DataSource API for query optimization.

The connector is open-sourced for the community. The Hortonworks Spark-HBase Connector library is available as a downloadable Spark package at https://github.com/hortonworks-spark/shc. The repository readme file contains information about how to use the package with Spark applications.

For more information about the connector, see A Year in Review blog.

RDD-Based Spark-HBase Connector

The RDD-based connector is developed by the Apache community. The connector is designed with full flexibility in mind: you can define schema on read and therefore it is suitable for workloads where schema is undefined at ingestion time. However, the architecture has some tradeoffs when it comes to performance.

Refer to the following table for other factors that might affect your choice of connector, source repos, and code examples.

​
Table 8.1. Comparison of the Spark-HBase Connectors

 	Hortonworks Spark-HBase Connector Connector	RDD-Based Spark-HBase Connector
Source	Hortonworks	Apache HBase community
Apache Open Source?	Yes	Yes
Requires a Schema?	Yes: Fixed schema	No: Flexible schema
Suitable Data for Connector	SparkSQL or DataFrame	RDD
Main Repo	shc git repo	Apache hbase-spark git repo
Sample Code for Java	Not available	Apache hbase.git repo
Sample Code for Scala	shc git repo	Apache hbase.git repo


​Using the Connector with Apache Phoenix
If you use a Spark-HBase connector in an environment that uses Apache Phoenix as a SQL skin, be aware that both connectors use only HBase .jar files by default. If you want to submit jobs on an HBase cluster with Phoenix enabled, you must include --jars phoenix-server.jar in your spark-submit command. For example:

./bin/spark-submit --class your.application.class \
--master yarn-client \
--num-executors 2 \
--driver-memory 512m \
--executor-memory 512m --executor-cores 1 \
--packages com.hortonworks:shc:1.0.0-1.6-s_2.10 \
--repositories http://repo.hortonworks.com/content/groups/
public/ \
--jars /usr/hdp/current/phoenix-client/phoenix-server.jar \
--files /etc/hbase/conf/hbase-site.xml /To/your/application/jar