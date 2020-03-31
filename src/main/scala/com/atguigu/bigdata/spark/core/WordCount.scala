package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WorldCount")
    val sc = new SparkContext(config)

    val lines: RDD[String] = sc.textFile("in/world.txt")
    val words: RDD[String] = lines.flatMap(_.split(" "))
    val wordToOne: RDD[(String, Int)] = words.map((_, 1))
    val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_+_)
    val result: Array[(String, Int)] = wordToSum.collect()
    result.foreach(println)
    sc.stop()

  }
}
