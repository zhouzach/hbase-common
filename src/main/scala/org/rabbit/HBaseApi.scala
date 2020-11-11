package org.rabbit

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}



object HBaseApi {
  //创建表
  def createHTable(connection: Connection,tablename: String): Unit=
  {
    //Hbase表模式管理器
    val admin = connection.getAdmin
    //本例将操作的表名
    val tableName = TableName.valueOf(tablename)
    //如果需要创建表
    if (!admin.tableExists(tableName)) {
      //创建Hbase表模式
      val tableDescriptor = new HTableDescriptor(tableName)
      //创建列簇1    artitle
      tableDescriptor.addFamily(new HColumnDescriptor("artitle".getBytes()))
      //创建列簇2    author
      tableDescriptor.addFamily(new HColumnDescriptor("author".getBytes()))
      //创建表
      admin.createTable(tableDescriptor)
      println("create done.")
    }

  }

  //删除表
  def deleteHTable(connection:Connection,tablename:String):Unit={
    //本例将操作的表名
    val tableName = TableName.valueOf(tablename)
    //Hbase表模式管理器
    val admin = connection.getAdmin
    if (admin.tableExists(tableName)){
      admin.disableTable(tableName)
      admin.deleteTable(tableName)
    }

  }
  //插入记录
  def insertHTable(connection:Connection,tablename:String,family:String,column:String,key:String,value:String):Unit={
    try{
      val userTable = TableName.valueOf(tablename)
      val table=connection.getTable(userTable)
      //准备key 的数据
      val p=new Put(key.getBytes)
      //为put操作指定 column 和 value
      p.addColumn(family.getBytes,column.getBytes,value.getBytes())
      //验证可以提交两个clomun？？？？不可以
      // p.addColumn(family.getBytes(),"china".getBytes(),"JAVA for china".getBytes())
      //提交一行
      table.put(p)
    }
  }

  //基于KEY查询某条数据
  def getAResult(connection:Connection,tablename:String,family:String,column:String,key:String):Unit={
    var table:Table=null
    try{
      val userTable = TableName.valueOf(tablename)
      table=connection.getTable(userTable)
      val g=new Get(key.getBytes())
      val result=table.get(g)
      val value=Bytes.toString(result.getValue(family.getBytes(),column.getBytes()))
      println("key:"+value)
    }finally{
      if(table!=null)table.close()

    }

  }

  //删除某条记录
  def deleteRecord(connection:Connection,tablename:String,family:String,column:String,key:String): Unit ={
    var table:Table=null
    try{
      val userTable=TableName.valueOf(tablename)
      table=connection.getTable(userTable)
      val d=new Delete(key.getBytes())
      d.addColumn(family.getBytes(),column.getBytes())
      table.delete(d)
      println("delete record done.")
    }finally{
      if(table!=null)table.close()
    }
  }

  //扫描记录
  def scanRecord(connection:Connection,tablename:String,family:String,column:String): Unit ={
    var table:Table=null
    var scanner:ResultScanner=null
    try{
      val userTable=TableName.valueOf(tablename)
      table=connection.getTable(userTable)
      val s=new Scan()
      s.addColumn(family.getBytes(),column.getBytes())
      scanner=table.getScanner(s)
      println("scan...for...")
      var result:Result=scanner.next()
      while(result!=null){
        println("Found row:" + result)
        println("Found value: "+Bytes.toString(result.getValue(family.getBytes(),column.getBytes())))
        result=scanner.next()
      }


    }finally{
      if(table!=null)
        table.close()

    }
  }

  def main(args: Array[String]): Unit = {
    // val sparkConf = new SparkConf().setAppName("HBaseTest")
    //启用spark上下文，只有这样才能驱动spark并行计算框架
    //val sc = new SparkContext(sparkConf)
    //创建一个配置，采用的是工厂方法
    val conf = HBaseConfiguration.create()
    val tablename = "ods:user_hbase4"
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("zookeeper.znode.parent", "/hbase")
    conf.set("hbase.zookeeper.quorum", "cdh1,cdh2,cdh3")
    // conf.set("hbase.zookeeper.quorum", "hadoop1.snnu.edu.cn,hadoop3.snnu.edu.cn")

    conf.set("hbase.mapreduce.inputtable", tablename)

    try{
      //Connection 的创建是个重量级的工作，线程安全，是操作hbase的入口
      val connection= ConnectionFactory.createConnection(conf)
      //创建表测试
      try {
//        createHTable(connection, "blog")
        //插入数据,重复执行为覆盖

//        insertHTable(connection,"blog","artitle","engish","002","c++ for me")
//        insertHTable(connection,"blog","artitle","engish","003","python for me")
//        insertHTable(connection,"blog","artitle","chinese","002","C++ for china")
        //删除记录
        // deleteRecord(connection,"blog","artitle","chinese","002")
        //扫描整个表
        scanRecord(connection,tablename,"cf","age")
        //删除表测试
        // deleteHTable(connection, "blog")
      }finally {
        connection.close
        //   sc.stop
      }
    }
  }
}
