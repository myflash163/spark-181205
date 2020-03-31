package com.atguigu.bigdata.spark.core

import java.sql.{Connection, PreparedStatement}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}


object Spark18_Mysql {
  def main(args: Array[String]): Unit = {

    //1.初始化配置信息及SparkContext
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    //3.定义连接mysql的参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop102:3306/rdd"
    val userName = "root"
    val passWd = "812324"
    //查询数据
    val sql = "select name,age from user where id >= ? and id <=?"

    val jdbcRDD = new JdbcRDD(sc, () => {
      Class.forName(driver)
      java.sql.DriverManager.getConnection(url, userName, passWd)
    }, sql, 1, 5, 2, (rs) => {
      println(rs.getString(1) + "," + rs.getInt(2))
    })
    println(jdbcRDD.collect)
    //保存数据

    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("zhangsan", 20), ("lisi", 30), ("wangwu", 40)))
    //connection 每次都创建，效率低
    dataRDD.foreach {
      case (username: String, age: Int) => {
        val connection: Connection = java.sql.DriverManager.getConnection(url, userName, passWd)
        val sql = "insert into user(name,age) values(?,?)"
        val statement: PreparedStatement = connection.prepareStatement(sql)
        statement.setString(1, username)
        statement.setInt(2, age)
        statement.executeUpdate()
        statement.close()
        connection.close()
      }
    }

  //每个分区 创建一个connection
    dataRDD.foreachPartition(datas => {
      val connection: Connection = java.sql.DriverManager.getConnection(url, userName, passWd)
      datas.foreach {
        case (username: String, age: Int) => {
          val sql = "insert into user(name,age) values(?,?)"
          val statement: PreparedStatement = connection.prepareStatement(sql)
          statement.setString(1, username)
          statement.setInt(2, age)
          statement.executeUpdate()
          statement.close()
        }
      }
      connection.close()
    })

    sc.stop()
  }
}
