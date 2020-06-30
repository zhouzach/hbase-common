package org.rabbit

import java.sql.{Connection, PreparedStatement, ResultSet, SQLException}

import com.alibaba.druid.pool.DruidDataSource
import org.slf4j.LoggerFactory

object PhoenixUtil {

  private val logger = LoggerFactory.getLogger(getClass)
  private val dataSource = new DruidDataSource()

  def initDataSource(url: String, driverClassName: String, username: String, password: String) = {

    dataSource.setUrl(url)
    dataSource.setDriverClassName(driverClassName)
    dataSource.setUsername(username)
    dataSource.setPassword(password)

    dataSource.setInitialSize(10)
    dataSource.setMinIdle(10)
    dataSource.setMaxActive(100)

    dataSource.setMaxWait(60000)
    dataSource.setTimeBetweenEvictionRunsMillis(60000)
    dataSource.setMinEvictableIdleTimeMillis(300000)

    /**
      * 让连接池知道数据库已经断开了，并且自动测试连接查询:
      * 用来检测连接是否有效的sql，要求是一个查询语句，常用select 'x'。
      * 如果validationQuery为null，testOnBorrow、testOnReturn、testWhileIdle都不会起作用
      */
//    dataSource.setValidationQuery("SELECT 'x' FROM DUAL")
//    dataSource.setTestWhileIdle(true)
    dataSource.setTestOnBorrow(true)
//    dataSource.setTestOnReturn(false)
    dataSource.setTestWhileIdle(false)


    dataSource.setPoolPreparedStatements(false)
    dataSource.setMaxPoolPreparedStatementPerConnectionSize(10)

    try {
      dataSource.setFilters("stat");
    } catch {
      case e: SQLException =>
        logger.error(e.getMessage)
    }

    dataSource
  }




  def getConnection(url: String, driverClassName: String, username: String, password: String): Option[Connection] = {
    initDataSource(url, driverClassName, username, password)

    try {
      Some(dataSource.getConnection)
    } catch {
      case sqlExp: SQLException =>
        sqlExp.printStackTrace()
        logger.info(sqlExp.toString)
        None
    }
  }

  def close(connection: Connection) = {
    if(connection != null) {
      try {
        connection.close()
      }catch {
        case sqlExp: SQLException =>
          sqlExp.printStackTrace()
          logger.info(sqlExp.toString)
        case exp: Exception =>
          exp.printStackTrace()
          logger.info(exp.toString)
      }
    }
  }

  def execQuery[T](connection: Option[Connection], query: String)(function: Option[ResultSet] => T) = {
    try {
      if(connection.nonEmpty) {
        val statement = connection.get.createStatement()
        val rs = statement.executeQuery(query)
        function(Some(rs))
      } else {
        function(None)
      }
    }catch {
      case sqlExp: SQLException =>
        sqlExp.printStackTrace()
        logger.info(sqlExp.toString)
        function(None)
      case exp: Exception =>
        exp.printStackTrace()
        logger.info(exp.toString)
        function(None)
    }
  }

  def execUpdate[T](connection: Option[Connection], updateStr: String) = {
    try {
      if(connection.nonEmpty) {
        val statement = connection.get.createStatement()
        statement.executeUpdate(updateStr)
      }
    }catch {
      case sqlExp: SQLException =>
        sqlExp.printStackTrace()
        logger.info(sqlExp.toString)
      case exp: Exception =>
        exp.printStackTrace()
        logger.info(exp.toString)
    }
  }


  def executeBatch[T](connection: Option[Connection], insertSql: String)(function: Option[PreparedStatement] => T) = {

    try {
      if(connection.nonEmpty) {
        val preparedStmt = connection.get.prepareStatement(insertSql)

        function(Some(preparedStmt))
      } else {
        function(None)
      }
    }catch {
      case sqlExp: SQLException =>
        sqlExp.printStackTrace()
        logger.info(sqlExp.toString)
        function(None)

    }

  }

  def main(args: Array[String]): Unit = {

    val driver = "org.apache.phoenix.jdbc.PhoenixDriver"
    val url = "jdbc:phoenix:cdh1,cdh2,cdh3:2181"
    val username = ""
    val password = ""

    val connection = getConnection(url, driver, username,password)
    println(connection)

//    val md = connection.get.getMetaData()
//    val rs2 = md.getTables(null, null, "%", null)
//    while (rs2.next()) {
//      println(rs2.getString(3))
//    }

    println()
    val rs3 = connection.get.createStatement().executeQuery(s"""SELECT * FROM "ods.user_hbase5" """)
    while (rs3.next()) {
      println(
        s"""
          |${rs3.getString(1)}, ${rs3.getString(2)}, ${rs3.getString(3)}, ${rs3.getString(4)}
          |
          |
          |""".stripMargin)
    }
    rs3.close()



//
//    val insertSql = "INSERT INTO APP.PARTSUPP VALUES(?, ?, ?, ?)"
//
//    val tenRecords = (preparedStmtOpt: Option[PreparedStatement]) => {
//
//      preparedStmtOpt match {
//        case Some(preparedStmt) =>
//
//          for (x <- 1 to 10) {
//            preparedStmt.setInt(1, x*100)
//            preparedStmt.setInt(2, x)
//            preparedStmt.setInt(3, x*1000)
//            preparedStmt.setBigDecimal(4, java.math.BigDecimal.valueOf(100.2))
//            preparedStmt.addBatch()
//          }
//          preparedStmt.executeBatch()
//
//          preparedStmt.close()
//
//        case None =>
//      }
//    }
//    executeBatch(Some(connection.get),insertSql)(tenRecords)
//
//
//
//    val add2Map = (rs: Option[ResultSet]) => {
//
//      val map = Map.empty[String, Any]
//      rs match {
//        case Some(result) =>
//          while (result.next()) {
//            val name = result.getString("index_database")
//            //              val age = result.getInt("TRIGGER_GROUP")
//            val age = result.getString("index_table")
//            map += (name -> age)
//          }
//          map
//        case None => map
//      }
//    }
//
//    val add2List = (rs: Option[ResultSet]) => {
//
//      var res: Seq[String] = Seq()
//      rs match {
//        case Some(result) =>
//          while (result.next()) {
//            val table = result.getString("index_table")
//            res = res :+ table
//          }
//          res
//        case None => res
//      }
//    }
//
//    val sql = ""
//
//    execQuery(Some(dataSource.getConnection()), sql)(add2Map).foreach {
//      case (k, v) =>
//        println(k)
//        println(v)
//    }
//
//    execQuery(Some(dataSource.getConnection()), sql)(add2List).foreach {
//      println(_)
//    }

    close(connection.get)

  }


}
