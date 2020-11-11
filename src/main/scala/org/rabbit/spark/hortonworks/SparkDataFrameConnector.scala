package org.rabbit.spark.hortonworks

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * https://github.com/hortonworks-spark/shc
 * https://docs.azure.cn/zh-cn/hdinsight/hdinsight-using-spark-query-hbase
 */
object SparkDataFrameConnector {
  val sparkSession: SparkSession = SparkSession.builder.appName("Simple Application")
    .master("local")
    .getOrCreate()



  def main(args: Array[String]): Unit = {

//        readFromHBase().show
    write2HBase(readFromHBase())


  }

  def write2HBase(df: DataFrame) = {

    /**
     * Given a DataFrame with specified schema, above will create an HBase table with 5 regions and save the DataFrame inside.
     * Note that if HBaseTableCatalog.newTable is not specified, the table has to be pre-created.
     */
    df.toDF.write.options(
      Map(HBaseTableCatalog.tableCatalog -> sinkCatalog, HBaseTableCatalog.newTable -> "5"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }

  /**
   * todo filter 性能分析
   * https://www.iteblog.com/archives/2522.html
   */
  def readFromHBase() = {
    val df = withCatalog(sourceCatalog)

    /**
     * df.filter((($"col0" <= "row050" && $"col0" > "row040") ||
     * $"col0" === "row005" ||
     * $"col0" === "row020" ||
     * $"col0" ===  "r20" ||
     * $"col0" <= "row005") &&
     * ($"col4" === 1 ||
     * $"col4" === 42))
     *
     */
    df
//      .filter(
//        ($"col1" === 42 &&
//          $"col3" === "female")
//      )
      .select("pkName", "col1", "col2", "col3")

  }


  /**
   * rowkey 关键字不能更改
   *
   * @return
   */
  def sourceCatalog =
    s"""{
       |"table":{"namespace":"ods", "name":"user_hbase4"},
       |"rowkey":"pk",
       |"columns":{
       |"pkName":{"cf":"rowkey", "col":"pk", "type":"string"},
       |"col1":{"cf":"cf", "col":"age", "type":"int"},
       |"col2":{"cf":"cf", "col":"created_time", "type":"string"},
       |"col3":{"cf":"cf", "col":"sex", "type":"string"}
       |}
       |}""".stripMargin

  /**
   * rowkey 关键字不能更改
   *
   * @return
   */
  def sinkCatalog =
    s"""{
       |"table":{"namespace":"ods", "name":"user_hbase12"},
       |"rowkey":"pk",
       |"columns":{
       |"pkName":{"cf":"rowkey", "col":"pk", "type":"string"},
       |"col1":{"cf":"cf", "col":"age", "type":"int"},
       |"col2":{"cf":"cf", "col":"created_time", "type":"string"},
       |"col3":{"cf":"cf", "col":"sex", "type":"string"}
       |}
       |}""".stripMargin


  def withCatalog(catalog: String): DataFrame = {

    sparkSession.sqlContext
      .read
      .options(Map(HBaseTableCatalog.tableCatalog -> catalog))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
  }

}


