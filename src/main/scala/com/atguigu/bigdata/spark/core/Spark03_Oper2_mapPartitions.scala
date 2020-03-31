package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Oper2_mapPartitions {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)
    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)
    /**
     * mapPartitions可以对一个RDD中的所有的分区进行遍历
     * mapPartitions效率优于map算子，减少了发送到执行器执行交互次数
     * mapPartitions可能会出现内存溢出
     */
    val mapPartitionsRDD: RDD[Int] = listRDD.mapPartitions(datas => {
      datas.map(_ * 2)
    })
    mapPartitionsRDD.collect().foreach(println)

  }

}
