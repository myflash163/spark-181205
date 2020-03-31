package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON


object Spark17_JSON {
  def main(args: Array[String]): Unit = {

    //1.初始化配置信息及SparkContext
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val json: RDD[String] = sc.textFile("in/user.json")
    val result: RDD[Option[Any]] = json.map(JSON.parseFull)
    result.foreach(println)


    sc.stop()
  }
}
