package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_Oper4_flatMap {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)
    val listRDD: RDD[List[Int]] = sc.makeRDD(Array(List(1,2),List(3,4)))
    val flatMapRDD: RDD[Int] = listRDD.flatMap(datas=>datas)

    flatMapRDD.collect().foreach(println)


  }

}
