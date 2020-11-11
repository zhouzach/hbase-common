package org.rabbit.spark

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.rabbit.{ResultFunctions, ScanFilter}

import scala.reflect.ClassTag

object SparkRDDConnector {

  val sparkConf = new SparkConf().setAppName("HBaseDistributedScanExample ").setMaster("local")
  val sc = new SparkContext(sparkConf)

  val conf = HBaseConfiguration.create()
  val hbaseContext = new HBaseContext(sc, conf)

  def main(args: Array[String]): Unit = {

    val tableName = "ODS:user_hbase3"
    //    val tableName = "REPORT:CONSUMPTION_BEHAVIOR"
    val columnFamily = "cf"

    val scan = new Scan()
    scan.setCaching(100)

//    bulkPut(tableName, columnFamily)
//    bulkGet(tableName)
//
//    distributedScan(tableName, ScanFilter.byRowPrefix(scan, "2"))
//    distributedScan(tableName, ScanFilter.byRowPrefix(scan, "2"), ResultFunctions.resultHandler)
//
//    distributedScan(tableName, ScanFilter.byRowRange(scan, "2", "4"))
//    distributedScan(tableName, ScanFilter.byRowRange(scan, "3", "3~"), ResultFunctions.rowKeyAndResult)
//    distributedScan(tableName, ScanFilter.byRowValue(scan, "4"), ResultFunctions.rowKeyAndResult)
//    distributedScan(tableName, ScanFilter.bySingleColumnValue(scan, "cf", "num", "3"),
//      ResultFunctions.rowKeyAndResult)

//    val rowkeys = hbaseContext.hbaseRDD(TableName.valueOf(tableName),
//      ScanFilter.byRowRange(scan, "2", "4")).map(kv => kv._1.get())
//    bulkDelete(tableName, rowkeys)

    bulkDelete(tableName)
    distributedScan(tableName, scan, ResultFunctions.rowKeyAndResult)

    sc.stop()
  }


  def bulkGet(tableName: String) = {

    //rowKey集合
    val rdd = sc.parallelize(Array(
      Bytes.toBytes("1"),
//      Bytes.toBytes("2")
      //Bytes.toBytes("3")
      Bytes.toBytes("4"),
      Bytes.toBytes("5")
    ))

    val getRdd = hbaseContext.bulkGet[Array[Byte], String](
      TableName.valueOf(tableName),
      2,
      rdd,
      record => {
        val str = Bytes.toString(record)
        println(s"making Get: $str")
        new Get(record)
      },
      (result: Result) => { //某个rowKey的result

        val it = result.listCells().iterator() //取出某个rowKey的所有qualifier
        val b = new StringBuilder

        b.append(Bytes.toString(result.getRow) + ":")

        while (it.hasNext) {
          val cell = it.next()
          val qualifier = Bytes.toString(CellUtil.cloneQualifier(cell)) //取出列名
          if (qualifier.equals("counter")) { // 根据不同的列，选择不同的反序列化方法
            b.append("(" + qualifier + "," + Bytes.toLong(CellUtil.cloneValue(cell)) + ")")
          } else {
            b.append("(" + qualifier + "," + Bytes.toString(CellUtil.cloneValue(cell)) + ")")
          }
        }
        b.toString()
      })
    getRdd.collect().foreach(v => println(v))
  }

  def distributedScan(tableName: String, scan: Scan) = {
    val getRdd = hbaseContext.hbaseRDD(TableName.valueOf(tableName), scan)
    //      getRdd.foreach(v => println(Bytes.toString(v._1.get()))) //print rowKey
    //      println("Length: " + getRdd.map(r => r._1.copyBytes()).collect().length) // collect rowKey
    //      getRdd.map(r => r._1.copyBytes()).map(v =>Bytes.toString(v)).collect().foreach(println(_))
    getRdd
      .map(r => r._1.copyBytes()).map(v => Bytes.toString(v)).collect().foreach(println(_))

  }

  def distributedScan[U: ClassTag](tableName: String, scan: Scan, convertResult: (Result) => U) = {
    val getRdd = hbaseContext.hbaseRDD(TableName.valueOf(tableName), scan)
    getRdd.foreach(v => println(Bytes.toString(v._1.get()))) //print rowKey
    println("Length: " + getRdd.map(r => r._1.copyBytes()).collect().length) // collect rowKey
    getRdd.map(r => convertResult(r._2)).collect().foreach(v => println(v))
  }

  def distributedScan(tableName: String, scan: Scan, f: ((ImmutableBytesWritable, Result)) => String) = {
    val getRdd = hbaseContext.hbaseRDD(TableName.valueOf(tableName), scan, f)
    getRdd.collect().foreach(v => println(v))
  }

  def bulkDelete(tableName: String) = {
    //[Array[Byte]]
    val rdd = sc.parallelize(Array(
      Bytes.toBytes("1"),
      Bytes.toBytes("2"),
      Bytes.toBytes("3"),
      Bytes.toBytes("4"),
      Bytes.toBytes("5")
    ))

    hbaseContext.bulkDelete[Array[Byte]](rdd,
      TableName.valueOf(tableName),
      putRecord => new Delete(putRecord),
      4)
  }

  def bulkDelete(tableName: String, rdd: RDD[Array[Byte]]) = {

    hbaseContext.bulkDelete[Array[Byte]](rdd,
      TableName.valueOf(tableName),
      putRecord => new Delete(putRecord),
      4)

  }

  def bulkPut(tableName: String, columnFamily: String) = {
    //[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])]
    val rdd = sc.parallelize(Array(
      (Bytes.toBytes("1"),
        Array((Bytes.toBytes(columnFamily), Bytes.toBytes("num"), Bytes.toBytes("1")))),
      (Bytes.toBytes("2"),
        Array((Bytes.toBytes(columnFamily), Bytes.toBytes("num"), Bytes.toBytes("2")))),
      (Bytes.toBytes("3"),
        Array((Bytes.toBytes(columnFamily), Bytes.toBytes("num"), Bytes.toBytes("3")))),
      (Bytes.toBytes("4"),
        Array((Bytes.toBytes(columnFamily), Bytes.toBytes("num"), Bytes.toBytes("4")))),
      (Bytes.toBytes("5"),
        Array((Bytes.toBytes(columnFamily), Bytes.toBytes("num"), Bytes.toBytes("5"))))
    ))

    hbaseContext.bulkPut[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])](rdd,
      TableName.valueOf(tableName),
      (putRecord) => {
        val put = new Put(putRecord._1)
        putRecord._2.foreach((putValue) =>
          put.addColumn(putValue._1, putValue._2, putValue._3))
        put
      })
  }

  def bulkPutWithTimestamp(tableName: String, columnFamily: String) = {

    val rdd = sc.parallelize(Array(
      (Bytes.toBytes("6"),
        Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("1")))),
      (Bytes.toBytes("7"),
        Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("2")))),
      (Bytes.toBytes("8"),
        Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("3")))),
      (Bytes.toBytes("9"),
        Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("4")))),
      (Bytes.toBytes("10"),
        Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("5"))))))

    val timeStamp = System.currentTimeMillis()
    hbaseContext.bulkPut[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])](rdd,
      TableName.valueOf(tableName),
      (putRecord) => {
        val put = new Put(putRecord._1)
        putRecord._2.foreach((putValue) => put.addColumn(putValue._1, putValue._2,
          timeStamp, putValue._3))
        put
      })

  }

  /**
   *
   * @param inputPath directory to the input data files, the path can be comma separated paths
   *                  * as a list of inputs
   * @param tableName
   * @param columnFamily
   */
  def bulkPutFromFile(inputPath: String, tableName: String, columnFamily: String) = {
    var rdd = sc.hadoopFile(
      inputPath,
      classOf[TextInputFormat],
      classOf[LongWritable],
      classOf[Text]).map(v => {
      System.out.println("reading-" + v._2.toString)
      v._2.toString
    })

    hbaseContext.bulkPut[String](rdd,
      TableName.valueOf(tableName),
      (putRecord) => {
        val data = putRecord.split(",")
        val put = new Put(Bytes.toBytes(data(0)))
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(data(1)),
          Bytes.toBytes(data(2)))
        put
      })
  }

}
