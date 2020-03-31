package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark09_Oper8_sample {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)
    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)
    val sampleRDD: RDD[Int] = listRDD.sample(false, 0.6, 1)
    sampleRDD.collect().foreach(println)


  }

}
