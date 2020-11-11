package org.rabbit.spark

import com.fasterxml.uuid.Generators
import org.apache.hadoop.hbase.CellUtil._
import org.apache.hadoop.hbase.client.{Delete, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, HBaseConfiguration}
import org.apache.hadoop.mapreduce.Job
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.rabbit.config.SparkConfig
import org.rabbit.models.UserData
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

object SparkHbaseHelper {
  val logger = LoggerFactory.getLogger(this.getClass)



  //屏蔽不必要的日志显示在终端上
//    Logger.getLogger("org.apache.spark").setLevel(Level.INFO)


  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      //    .enableHiveSupport()
      .master("local")
      .getOrCreate()



    val tableName = "ods:user_hbase1"

    //    SparkHbaseHelper.getRawRDDByFamilyColumn(
    //      SparkConfig.spark.sparkContext,
    //      "ods:user_hbase4",
    //      "cf",
    //      "age"
    //    )
    //      .collect()
    //      .foreach(u => println(u))

    val data = spark.sparkContext.parallelize(Seq(
      UserData("lily", "female", 12, "2020-07-13 19:51:42.923"),
      UserData("tom", "male", 22, "2020-06-13 19:51:42.923")
    ))

    val func = (user: UserData) =>{
      val rowKey =  Generators.timeBasedGenerator().generate().toString
      val put = new Put(Bytes.toBytes(rowKey))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("uid"), Bytes.toBytes(user.uid))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("sex"), Bytes.toBytes(user.sex))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("age"), Bytes.toBytes(user.age))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("created_time"), Bytes.toBytes(user.created_time))

      (new ImmutableBytesWritable, put)
    }

    putRDD[UserData](tableName,data,func)

    spark.stop()





  }

  def getTableName(orgName: String, tableName: String): String = {
    s"$orgName:$tableName"
  }

  def getRawRDDByFamilyColumn(
                               sparkContext: SparkContext,
                               tableName: String,
                               columnFamily: String,
                               columnQualifier: String,
                               startRow: String = "0",
                               stopRow: String = "~~~~"
                             ): RDD[Result] = {
    val hConf = HBaseConfiguration.create()
    hConf.set("hbase.mapreduce.inputtable", tableName)
    //    hConf.addResource("core-site.xml")
    //    hConf.addResource("hbase-site.xml")
    //    hConf.addResource("hdfs-site.xml")
    hConf.set("hbase.mapreduce.scan.row.start", startRow)
    hConf.set("hbase.mapreduce.scan.row.stop", stopRow)
    //    hConf.set(TableInputFormat.SCAN_COLUMN_FAMILY, columnFamily)
    hConf.set("hbase.mapreduce.scan.columns", s"$columnFamily:$columnQualifier")
    sparkContext.newAPIHadoopRDD(hConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]).map(_._2)
  }

  def getRDD[T: ClassTag](spark: SparkSession,
                           tableName: String,
                           columnFamily: String,
                           qualifier: String,
                           startRow: String,
                           stopRow: String,
                           toT: Array[Byte] => T
                         ): RDD[T] = {
    val rawRDD = getRawRDD(spark,tableName, startRow, stopRow)
    rawRDD.map(data => {
      toT(data._2.getValue(columnFamily.getBytes(), qualifier.getBytes()))
    })
  }

  // 只会扫描 'columns' 指定的那些列
  // 'columns' 都属于 'columnFamily'
  def getRDD[T: ClassTag](spark: SparkSession,tableName: String,
                          columnFamily: String,
                          columns: List[String],
                          startRow: String,
                          stopRow: String,
                          toT: Result => T): RDD[T] = {
    val hConf = HBaseConfiguration.create()
    hConf.set("hbase.mapreduce.inputtable", tableName)
    hConf.addResource("core-site.xml")
    hConf.addResource("hbase-site.xml")
    hConf.addResource("hdfs-site.xml")
    //    hConf.set("hbase.zookeeper.quorum", ConfigurationHelper.hbaseIp)
    //    hConf.set("hbase.zookeeper.property.clientPort", ConfigurationHelper.hbasePort)
    //    hConf.set("zookeeper.znode.parent", ConfigurationHelper.hbaseZnode)
    hConf.set("hbase.mapreduce.scan.row.start", startRow)
    hConf.set("hbase.mapreduce.scan.row.stop", stopRow)
    hConf.set("hbase.mapreduce.scan.columns", constructCFandQualifier(columnFamily, columns))

    val rawRDD: RDD[(ImmutableBytesWritable, Result)] = spark.sparkContext.newAPIHadoopRDD(hConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    rawRDD.map(res => toT(res._2))
  }

  def getRawRDD(spark: SparkSession,
                 tableName: String,
                 startRow: String,
                 stopRow: String
               ): RDD[(ImmutableBytesWritable, Result)] = {
    val hConf = HBaseConfiguration.create()
    hConf.set("hbase.mapreduce.inputtable", tableName)
    hConf.addResource("core-site.xml")
    hConf.addResource("hbase-site.xml")
    hConf.addResource("hdfs-site.xml")
    //    hConf.set("hbase.zookeeper.quorum", ConfigurationHelper.hbaseIp)
    //    hConf.set("hbase.zookeeper.property.clientPort", ConfigurationHelper.hbasePort)
    //    hConf.set("zookeeper.znode.parent", ConfigurationHelper.hbaseZnode)
    hConf.set("hbase.mapreduce.scan.row.start", startRow)
    hConf.set("hbase.mapreduce.scan.row.stop", stopRow)
    spark.sparkContext.newAPIHadoopRDD(hConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
  }

  def getRDD(spark: SparkSession,
              tableName: String,
              columnFamily: String,
              qualifier: String,
              startRow: String,
              stopRow: String
            ): RDD[String] = {
    getRDD[String](spark,tableName, columnFamily, qualifier, startRow, stopRow, (x: Array[Byte]) => Bytes.toString(x))
  }

  def getRDD[T: ClassTag](spark: SparkSession,
                           tableName: String,
                           startRow: String,
                           stopRow: String,
                           toT: Result => T
                         ): RDD[T] = {
    val rawRDD = getRawRDD(spark,tableName, startRow, stopRow)
    rawRDD.map(data => {
      toT(data._2)
    })
  }

  def res2Map(res: Result): Map[String, Array[Byte]] = {
    val cells = res.rawCells()
    cells.map(cell => {
      (Bytes.toString(cloneQualifier(cell)), cloneValue(cell))
    }).toMap
  }

  def res2StrMap(res: Result): Map[String, String] = {
    val cells = res.rawCells()
    cells.map(cell => {
      (Bytes.toString(cloneQualifier(cell)), Bytes.toString(cloneValue(cell)))
    }).toMap
  }

  def res2MapWithCF(res: Result): Map[String, Array[Byte]] = {
    val cells = res.rawCells()
    cells.map(cell => {
      (Bytes.toString(cloneFamily(cell)) + ":" + Bytes.toString(cloneQualifier(cell)), cloneValue(cell))
    }).toMap
  }

  def res2MapWithCFK(res: Result): (String, Map[String, Array[Byte]]) = {
    (Bytes.toString(res.getRow), res2MapWithCF(res))
  }

  def res2MapWithK(res: Result): (String, Map[String, Array[Byte]]) = {
    (Bytes.toString(res.getRow), res2Map(res))
  }

  def res2StrMapWithK(res: Result): (String, Map[String, String]) = {
    (Bytes.toString(res.getRow), res2StrMap(res))
  }

  // (唯一的column value, row-Key)
  // 给 master table 2.0 使用 -- Bin
  def singleColumnAndRK(res: Result): (String, String) = {
    val cells: Array[Cell] = res.rawCells()
    if (cells.isEmpty) {
      ("", Bytes.toString(res.getRow))
    } else {
      (Bytes.toString(cloneValue(cells(0))), Bytes.toString(res.getRow))
    }
  }

  //This is used for fast migrating.
  def res2Put(res: Result): (ImmutableBytesWritable, Put) = {
    val p = new Put(res.getRow)
    val cells = res.rawCells()
    cells.foreach(cell => {
      p.addColumn(cloneFamily(cell), cloneQualifier(cell), cloneValue(cell))
    })
    (new ImmutableBytesWritable, p)
  }

  //  def putRDD[A <: Any](
  //            tableName: String,
  //            columnFamily: String,
  //            data: RDD[(String, Map[String, A])]
  //            ): Unit = {
  //    putRDD(tableName, columnFamily, data, keyMap2Put)
  //  }

  def putRDD[A <: Any](
                        tableName: String,
                        data: RDD[(String, Map[String, A])]
                      ): Unit = {
    putRDD(tableName, data, cfkMap2Put(_: (String, Map[String, A])))
  }


  /**
   *
   * val func = (user: UserData) =>{
   * val rowKey =  Generators.timeBasedGenerator().generate().toString
   * val put = new Put(Bytes.toBytes(rowKey))
   *       put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("uid"), Bytes.toBytes(user.uid))
   *       put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("sex"), Bytes.toBytes(user.sex))
   *       put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("age"), Bytes.toBytes(user.age))
   *       put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("created_time"), Bytes.toBytes(user.created_time))
   *
   * (new ImmutableBytesWritable, put)
   * }
   *
   * @param tableName
   * @param data
   * @param t
   * @tparam T
   */
  def putRDD[T](
                 tableName: String,
                 data: RDD[T],
                 t: T => (ImmutableBytesWritable, Put)
               ): Unit = {
    val hConf = HBaseConfiguration.create()
//    hConf.addResource("core-site.xml")
//    hConf.addResource("hbase-site.xml")
//    hConf.addResource("hdfs-site.xml")
        hConf.set("hbase.zookeeper.quorum", "localhost")
        hConf.set("hbase.zookeeper.property.clientPort", "2181")
    //    hConf.set("zookeeper.znode.parent", ConfigurationHelper.hbaseZnode)
    hConf.set("hbase.mapred.outputtable", tableName)
    hConf.set("mapreduce.output.fileoutputformat.outputdir", "./tmp")
    val job = Job.getInstance(hConf)
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    data.map(t).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  def putRDD[T](
                 tableName: String,
                 columnFamily: String,
                 data: RDD[T],
                 t: (T, String) => (ImmutableBytesWritable, Put)
               ): Unit = {
    val hConf = HBaseConfiguration.create()
    hConf.addResource("core-site.xml")
    hConf.addResource("hbase-site.xml")
    hConf.addResource("hdfs-site.xml")
    //    hConf.set("hbase.zookeeper.quorum", ConfigurationHelper.hbaseIp)
    //    hConf.set("hbase.zookeeper.property.clientPort", ConfigurationHelper.hbasePort)
    //    hConf.set("zookeeper.znode.parent", ConfigurationHelper.hbaseZnode)
    hConf.set("hbase.mapred.outputtable", tableName)
    hConf.set("mapreduce.output.fileoutputformat.outputdir", "./tmp")
    val job = Job.getInstance(hConf)
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    //    logger.warn(s"ip = ${ConfigurationHelper.hbaseIp}, port = ${ConfigurationHelper.hbasePort}, znode = ${ConfigurationHelper.hbaseZnode}" +
    //      s", tableName = ${tableName}, cf = ${columnFamily}.")
    data.map(x => t(x, columnFamily)).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  def keyMap2Put[A <: Any](in: (String, Map[String, A]), columnFamily: String): (ImmutableBytesWritable, Put) = {
    val p = new Put(Bytes.toBytes(in._1))
    in._2.map(y => {
      y._2 match {
        case s: String => (Bytes.toBytes(y._1), Bytes.toBytes(s))
        case i: Int => (Bytes.toBytes(y._1), Bytes.toBytes(i))
        case d: Double => (Bytes.toBytes(y._1), Bytes.toBytes(d))
        case l: Long => (Bytes.toBytes(y._1), Bytes.toBytes(l))
        case f: Float => (Bytes.toBytes(y._1), Bytes.toBytes(f))
        case b: Boolean => (Bytes.toBytes(y._1), Bytes.toBytes(b))
        case t => (Bytes.toBytes(y._1), Bytes.toBytes(t.toString))
      }
    }).foreach(kv => {
      p.addColumn(Bytes.toBytes(columnFamily), kv._1, kv._2)
    })
    (new ImmutableBytesWritable, p)
  }

  def cfkMap2Put[A <: Any](in: (String, Map[String, A])): (ImmutableBytesWritable, Put) = {
    val p = new Put(Bytes.toBytes(in._1))
    in._2.map(y => {
      val cf = y._1.split(":")(0)
      val qualifier = y._1.split(":")(1)
      y._2 match {
        case s: String => (Bytes.toBytes(cf), Bytes.toBytes(qualifier), Bytes.toBytes(s))
        case i: Int => (Bytes.toBytes(cf), Bytes.toBytes(qualifier), Bytes.toBytes(i))
        case d: Double => (Bytes.toBytes(cf), Bytes.toBytes(qualifier), Bytes.toBytes(d))
        case l: Long => (Bytes.toBytes(cf), Bytes.toBytes(qualifier), Bytes.toBytes(l))
        case f: Float => (Bytes.toBytes(cf), Bytes.toBytes(qualifier), Bytes.toBytes(f))
        case b: Boolean => (Bytes.toBytes(cf), Bytes.toBytes(qualifier), Bytes.toBytes(b))
        case t => (Bytes.toBytes(cf), Bytes.toBytes(qualifier), Bytes.toBytes(t.toString))
      }
    }).foreach(kv => {
      p.addColumn(kv._1, kv._2, kv._3)
    })
    (new ImmutableBytesWritable, p)
  }

  def getDF[A <: Any](
                       rdd: RDD[A],
                       columnInfo: List[ColumnInfo],
                       t2Row: (A, List[ColumnInfo]) => Row,
                       schema: StructType,
                       session: Option[SparkSession]
                     ): DataFrame = {
    val ss = session.fold(SparkConfig.spark) { x => x }
    ss.createDataFrame(rdd.map(z => t2Row(z, columnInfo)), schema)
  }

  def getDF(
             rdd: RDD[Map[String, Array[Byte]]],
             columnInfo: List[ColumnInfo],
             session: Option[SparkSession]
           ): DataFrame = {
    val schema = columnInfo2Schema(columnInfo)
    getDF[Map[String, Array[Byte]]](rdd, columnInfo, rawMap2Row, schema, session)
  }

  def getDFWithK(
                  rdd: RDD[(String, Map[String, Array[Byte]])],
                  columnInfo: List[ColumnInfo],
                  session: Option[SparkSession]
                ): DataFrame = {
    val newColumnInfo = ColumnInfo("", "ROWKEY") :: columnInfo
    val schema = columnInfo2Schema(newColumnInfo)
    getDF[(String, Map[String, Array[Byte]])](rdd, newColumnInfo, rawMapWithK2Row, schema, session)
  }


  //Should you need original row key to be added to dataframe, prepend a `ROWKEY` typed columnInfo to the list.
  def columnInfo2Schema(
                         columnInfo: List[ColumnInfo]
                       ): StructType = {
    StructType(
      columnInfo.map(z => ColumnInfo(z.columnName.replaceAll("`", ""), z.columnType)).map(ci => {
        ci.columnType match {
          case "String" => StructField(ci.columnName, StringType)
          case "Int" => StructField(ci.columnName, IntegerType)
          case "Double" => StructField(ci.columnName, DoubleType)
          case "Boolean" => StructField(ci.columnName, BooleanType)
          case "Float" => StructField(ci.columnName, FloatType)
          case "Long" => StructField(ci.columnName, LongType)
          case "Short" => StructField(ci.columnName, ShortType)
          case "ROWKEY" => StructField("ROWKEY", StringType)
          case _ => StructField(ci.columnName, NullType)
        }
      })
    )
  }

  def rawMapWithK2Row(
                       data: (String, Map[String, Array[Byte]]),
                       columnInfo: List[ColumnInfo]
                     ): Row = {
    Row.fromSeq(columnInfo.map(z => ColumnInfo(z.columnName.replaceAll("`", ""), z.columnType)).map(x => {
      x.columnType match {
        case "String" => Bytes.toString(data._2.getOrElse(x.columnName, Bytes.toBytes("")))
        case "Int" => Bytes.toInt(data._2.getOrElse(x.columnName, Bytes.toBytes(0)))
        case "Double" => Bytes.toDouble(data._2.getOrElse(x.columnName, Bytes.toBytes(0.0d)))
        case "Long" => Bytes.toLong(data._2.getOrElse(x.columnName, Bytes.toBytes(0l)))
        case "Float" => Bytes.toFloat(data._2.getOrElse(x.columnName, Bytes.toBytes(0.0f)))
        case "Short" => Bytes.toShort(data._2.getOrElse(x.columnName, Bytes.toBytes(0.toShort)))
        case "Boolean" => Bytes.toBoolean(data._2.getOrElse(x.columnName, Bytes.toBytes(false)))
        case "ROWKEY" => data._1
        case z => ""
      }
    }))
  }

  def rawMap2Row(
                  data: Map[String, Array[Byte]],
                  columnInfo: List[ColumnInfo]
                ): Row = {
    Row.fromSeq(columnInfo.map(z => ColumnInfo(z.columnName.replaceAll("`", ""), z.columnType)).map(x => {
      x.columnType match {
        case "String" => Bytes.toString(data.getOrElse(x.columnName, Bytes.toBytes("")))
        case "Int" => Bytes.toInt(data.getOrElse(x.columnName, Bytes.toBytes(0)))
        case "Double" => Bytes.toDouble(data.getOrElse(x.columnName, Bytes.toBytes(0.0d)))
        case "Long" => Bytes.toLong(data.getOrElse(x.columnName, Bytes.toBytes(0l)))
        case "Float" => Bytes.toFloat(data.getOrElse(x.columnName, Bytes.toBytes(0.0f)))
        case "Short" => Bytes.toFloat(data.getOrElse(x.columnName, Bytes.toBytes(0.toShort)))
        case "Boolean" => Bytes.toBoolean(data.getOrElse(x.columnName, Bytes.toBytes(false)))
        case z => ""
      }
    }))
  }

  // 组合一个 column family 下的多个 columns
  // 使其满足 TableInputFormat 的 SCAN_COLUMNS 规则: Space delimited list of columns and column families to scan
  private def constructCFandQualifier(columnFamily: String, columns: List[String]): String = {
    columns.map(col => s"$columnFamily:$col").mkString(" ")
  }

  def deleteRDD(tableName: String,
                cf: String,
                data: RDD[(String, List[String])]): Unit = {
    deleteRDD(tableName, cf, data, rkCol2Delete)
  }

  def deleteRDD(tableName: String, rows: RDD[String]): Unit = {
    val hConf = HBaseConfiguration.create()
    hConf.addResource("core-site.xml")
    hConf.addResource("hbase-site.xml")
    hConf.addResource("hdfs-site.xml")
    //    hConf.set("hbase.zookeeper.quorum", ConfigurationHelper.hbaseIp)
    //    hConf.set("hbase.zookeeper.property.clientPort", ConfigurationHelper.hbasePort)
    //    hConf.set("zookeeper.znode.parent", ConfigurationHelper.hbaseZnode)
    hConf.set("hbase.mapred.outputtable", tableName)
    hConf.set("mapreduce.output.fileoutputformat.outputdir", "./tmp")
    val job = Job.getInstance(hConf)
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    rows.map((rk: String) => {
      val del = new Delete(Bytes.toBytes(rk))
      (new ImmutableBytesWritable, del)
    }).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  def deleteRDD(tableName: String,
                cf: String,
                data: RDD[(String, List[String])],
                t: ((String, List[String]), String) => (ImmutableBytesWritable, Delete)): Unit = {
    val hConf = HBaseConfiguration.create()
    hConf.addResource("core-site.xml")
    hConf.addResource("hbase-site.xml")
    hConf.addResource("hdfs-site.xml")
    //    hConf.set("hbase.zookeeper.quorum", ConfigurationHelper.hbaseIp)
    //    hConf.set("hbase.zookeeper.property.clientPort", ConfigurationHelper.hbasePort)
    //    hConf.set("zookeeper.znode.parent", ConfigurationHelper.hbaseZnode)
    hConf.set("hbase.mapred.outputtable", tableName)
    hConf.set("mapreduce.output.fileoutputformat.outputdir", "./tmp")
    val job = Job.getInstance(hConf)
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    data.map((d: (String, List[String])) => t(d, cf)).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  def rkCol2Delete(in: (String, List[String]), cf: String): (ImmutableBytesWritable, Delete) = {
    val del = new Delete(Bytes.toBytes(in._1))
    in._2.foreach(col => {
      // delete all versions
      del.addColumns(Bytes.toBytes(cf), Bytes.toBytes(col))
    })
    (new ImmutableBytesWritable, del)
  }

}
case class ColumnInfo(columnName: String, var columnType: String)
