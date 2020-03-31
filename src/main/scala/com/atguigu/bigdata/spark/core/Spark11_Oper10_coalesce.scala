package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark11_Oper10_coalesce {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)
    //缩减分区，可以简单理解未合并分区
    val listRDD: RDD[Int] = sc.makeRDD(1 to 16, 4)
    println("缩减分区前=" + listRDD.partitions.size)
    val coalesceRDD: RDD[Int] = listRDD.coalesce(3)
    println("缩减分区后=" + coalesceRDD.partitions.size)
    coalesceRDD.saveAsTextFile("output")


  }

}
