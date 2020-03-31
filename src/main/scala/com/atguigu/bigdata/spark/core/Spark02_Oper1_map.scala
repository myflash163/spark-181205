package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Oper1_map {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)
    //map算子
    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)
    //所有RDD算子的计算功能全部由Executor执行
    val mapRDD: RDD[Int] = listRDD.map(_ * 2)
    mapRDD.collect().foreach(println)


  }

}
