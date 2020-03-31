package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark16_Checkpoint {
  def main(args: Array[String]): Unit = {

    //1.初始化配置信息及SparkContext
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    sc.setCheckpointDir("cp")
    //2.创建一个RDD
    val rdd: RDD[Int] = sc.parallelize(List(1, 2, 3, 4))
    val mapRDD: RDD[(Int, Int)] = rdd.map((_, 1))
    mapRDD.checkpoint()
    val reduceRDD: RDD[(Int, Int)] = mapRDD.reduceByKey(_ + _)
    reduceRDD.checkpoint()
    reduceRDD.foreach(println)

    println(reduceRDD.toDebugString)
    sc.stop()
  }
}
