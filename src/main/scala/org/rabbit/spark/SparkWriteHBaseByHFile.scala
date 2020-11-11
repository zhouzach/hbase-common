package org.rabbit.spark

import java.net.URI

import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, TableOutputFormat}
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.rabbit.models.UserData

import scala.collection.mutable.ListBuffer

/**
 * https://www.cnblogs.com/swordfall/p/10517177.html
 */
object SparkWriteHBaseByHFile {

  val sparkSession = SparkSession.builder().appName("insertWithBulkLoad")
    .master("local[4]").getOrCreate()

  def main(args: Array[String]): Unit = {
    val tableName = args(0)

    val hbaseConf = HBaseConfiguration.create()
    //        hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, "cdh3")
    //        hbaseConf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
    hbaseConf.set("hbase.fs.tmp.dir", "/tmp")
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)


    val hfilePath = args(1)

    generateHFile(hbaseConf, tableName, hfilePath)
    //    bulkLoadWithExistedHFile(hfilePath, hbaseConf, tableName)
    //    insertWithBulkLoadWithMulti(hbaseConf, tableName)

  }

  /**
   * 批量插入 多列
   */
  def insertWithBulkLoadWithMulti(conf: Configuration, tableName: String): Unit = {
    val tmpPath = "hdfs://nameservice1/tmp/insert-hbase"
    generateHFile(conf, tableName, tmpPath)
    bulkLoadWithExistedHFile(tmpPath, conf, tableName)

    FileSystem.get(new URI(tmpPath), new Configuration)
      .delete(new Path(tmpPath), true)
  }

  def generateHFile(conf: Configuration, tableName: String, outputPath: String): Unit = {

    val conn = ConnectionFactory.createConnection(conf)
    val table = conn.getTable(TableName.valueOf(tableName))

    val job = Job.getInstance(conf)
    //设置job的输出格式
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    job.setOutputFormatClass(classOf[HFileOutputFormat2])
    HFileOutputFormat2.configureIncrementalLoad(job, table, conn.getRegionLocator(TableName.valueOf(tableName)))

    val rdd =
    //      sc.textFile("v2120/a.txt")
    //      .map(_.split(","))
      sparkSession.sparkContext.parallelize(Seq(
        UserData("lily", "female", 12, "2020-07-13 19:51:42.923"),
        UserData("lucy", "female", 22, "2020-08-13 19:52:42.923"),
        UserData("jack", "male", 13, "2020-07-23 19:51:42.923"),
        UserData("jack1", "male", 13, "2020-07-23 19:51:42.923"),
        UserData("jack2", "male", 13, "2020-07-23 19:51:42.923"),
        UserData("jack3", "male", 13, "2020-07-23 19:51:42.923"),
        UserData("jack4", "male", 13, "2020-07-23 19:51:42.923"),
        UserData("jack5", "male", 13, "2020-07-23 19:51:42.923"),
        UserData("jack6", "male", 13, "2020-07-23 19:51:42.923"),
        UserData("jack7", "male", 13, "2020-07-23 19:51:42.923"),
        UserData("jack8", "male", 13, "2020-07-23 19:51:42.923"),
        UserData("jack9", "male", 13, "2020-07-23 19:51:42.923"),
        UserData("jack10", "male", 13, "2020-07-23 19:51:42.923"),
        UserData("jack11", "male", 13, "2020-07-23 19:51:42.923"),
        UserData("jack12", "male", 13, "2020-07-23 19:51:42.923"),
        UserData("jack13", "male", 13, "2020-07-23 19:51:42.923"),
        UserData("jack14", "male", 13, "2020-07-23 19:51:42.923"),
        UserData("jack15", "male", 13, "2020-07-23 19:51:42.923"),
        UserData("willson", "male", 14, "2020-08-13 19:51:42.923"),
        UserData("tom", "male", 22, "2020-06-13 19:51:42.923")
      ))
        .map(x => (DigestUtils.md5Hex(x.uid).substring(0, 3) + x.uid, x.uid, x.sex, x.age, x.created_time))
        .sortBy(_._1) //rowkey 必须升序
        .flatMap(x => {
          val listBuffer = new ListBuffer[(ImmutableBytesWritable, KeyValue)]
          val kv1: KeyValue = new KeyValue(Bytes.toBytes(x._1), Bytes.toBytes("cf"), Bytes.toBytes("uid"), Bytes.toBytes(x._1 + ""))
          val kv2: KeyValue = new KeyValue(Bytes.toBytes(x._1), Bytes.toBytes("cf"), Bytes.toBytes("sex"), Bytes.toBytes(x._2 + ""))
          val kv3: KeyValue = new KeyValue(Bytes.toBytes(x._1), Bytes.toBytes("cf"), Bytes.toBytes("age"), Bytes.toBytes(x._3 + ""))
          val kv4: KeyValue = new KeyValue(Bytes.toBytes(x._1), Bytes.toBytes("cf"), Bytes.toBytes("created_time"), Bytes.toBytes(x._4 + ""))

          //qualifier 必须按照lexically顺序添加
          listBuffer.append((new ImmutableBytesWritable, kv3))
          listBuffer.append((new ImmutableBytesWritable, kv4))
          listBuffer.append((new ImmutableBytesWritable, kv2))
          listBuffer.append((new ImmutableBytesWritable, kv1))
          listBuffer
        }
        )


    isFileExist(outputPath, sparkSession.sparkContext)

    rdd.saveAsNewAPIHadoopFile(outputPath, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], job.getConfiguration)


  }

  /**
   * 判断hdfs上文件是否存在，存在则删除
   */
  def isFileExist(filePath: String, sc: SparkContext): Unit = {
    val output = new Path(filePath)
    val hdfs = FileSystem.get(new URI(filePath), new Configuration)
    if (hdfs.exists(output)) {
      hdfs.delete(output, true)
    }
  }

  def bulkLoadWithExistedHFile(hfilePath: String, conf: Configuration, tableName: String) = {
    val conn = ConnectionFactory.createConnection(conf)
    val admin = conn.getAdmin
    val table = conn.getTable(TableName.valueOf(tableName))

    val bulkLoader = new LoadIncrementalHFiles(conf)
    bulkLoader.doBulkLoad(new Path(hfilePath),
      admin,
      table,
      conn.getRegionLocator(TableName.valueOf(tableName)))
  }
}
