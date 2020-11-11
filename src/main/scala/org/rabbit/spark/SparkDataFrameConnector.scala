package org.rabbit.spark

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.spark.datasources.HBaseTableCatalog
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * https://github.com/hortonworks-spark/shc
 * https://docs.azure.cn/zh-cn/hdinsight/hdinsight-using-spark-query-hbase
 *
 */
object SparkDataFrameConnector {
  val sparkSession: SparkSession = SparkSession.builder.appName("Simple Application")
    .master("local")
    .getOrCreate()

  val conf = HBaseConfiguration.create()
  val hbaseContext = new HBaseContext(sparkSession.sparkContext, conf)

  def main(args: Array[String]): Unit = {

    import sparkSession.implicits._
    val df = Seq(
      (38, 20, "2020-11-28", "male"),
      (39, 22, "2020-11-38", "female")
    ).toDF("pkName", "col1", "col2", "col3")
    write2HBase(df)

    filterAndSelect().show
//    write2HBase(readFromHbase(sourceCatalog))

  }

  /**
   * rowkey 关键字不能更改
   *
   * @return
   */
  def sinkCatalog =
    s"""{
       |"table":{ "name":"ODS:user_hbase3"},
       |"rowkey":"pk",
       |"columns":{
       |"pkName":{"cf":"rowkey", "col":"pk", "type":"string"},
       |"col1":{"cf":"cf", "col":"age", "type":"int"},
       |"col2":{"cf":"cf", "col":"created_time", "type":"string"},
       |"col3":{"cf":"cf", "col":"sex", "type":"string"}
       |}
       |}""".stripMargin

  def write2HBase(df: DataFrame) = {

    /**
     * Given a DataFrame with specified schema, above will create an HBase table with 5 regions and save the DataFrame inside.
     * Note that if HBaseTableCatalog.newTable is not specified, the table has to be pre-created.
     */
    df.write.options(
      Map(HBaseTableCatalog.tableCatalog -> sinkCatalog, HBaseTableCatalog.newTable -> "5"))
      .format("org.apache.hadoop.hbase.spark")
      .save()
  }

  /**
   * rowkey 关键字不能更改
   *
   * pkName,col1,col2,col3 为DataFrame的列名
   * pk,age,created_time,sex为hbase中的列名
   *
   * @return
   */
  def sourceCatalog =
    s"""{
       |"table":{ "name":"ODS:user_hbase3"},
       |"rowkey":"pk",
       |"columns":{
       |"pkName":{"cf":"rowkey", "col":"pk", "type":"int"},
       |"col1":{"cf":"cf", "col":"age", "type":"int"},
       |"col2":{"cf":"cf", "col":"created_time", "type":"string"},
       |"col3":{"cf":"cf", "col":"sex", "type":"string"}
       |}
       |}""".stripMargin

  def readFromHbase(catalog: String): DataFrame = {
    sparkSession.sqlContext
      .read
      .options(Map(HBaseTableCatalog.tableCatalog -> catalog))
      .format("org.apache.hadoop.hbase.spark")
      .load()
  }

  /**
   * todo filter 性能分析
   * https://www.iteblog.com/archives/2522.html
   */
  def filterAndSelect() = {
    val df = readFromHbase(sourceCatalog)

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


}

