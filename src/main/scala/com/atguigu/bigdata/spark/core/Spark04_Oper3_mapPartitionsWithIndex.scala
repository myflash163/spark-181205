package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_Oper3_mapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)
    val listRDD: RDD[Int] = sc.makeRDD(1 to 10, 2)
    val tupleRDD: RDD[(Int, String)] = listRDD.mapPartitionsWithIndex({
      case (num, datas) => {
        datas.map((_, "分区号：" + num))
      }
    })
    tupleRDD.collect().foreach(println)


  }

}
