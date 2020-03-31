package com.atguigu.bigdata.spark.core

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

//自定义累加器
object Spark21_Accumulator {
  def main(args: Array[String]): Unit = {

    //1.初始化配置信息及SparkContext
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val dataRDD: RDD[String] = sc.makeRDD(List("hadoop","hive","hbase","scala","spark"), 2)
    /*    val i: Int = dataRDD.reduce(_ + _)
    println(i)*/
    //创建累加器对象
    val accumulator: WordAccumulator = new WordAccumulator
    sc.register(accumulator)

    dataRDD.foreach {
      case word => {
        accumulator.add(word)
      }
    }
    println("sum=" + accumulator.value)

    sc.stop()
  }
}

//声明累加器
class WordAccumulator extends AccumulatorV2[String, util.ArrayList[String]] {
  private val list = new util.ArrayList[String]()
  //是否是初始化状态
  override def isZero: Boolean = {
    list.isEmpty
  }

  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {
    new WordAccumulator
  }

  override def reset(): Unit = {
    list.clear()
  }

  override def add(v: String): Unit = {
    if (v.contains("h")) {
      list.add(v)
    }
  }

  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
    list.addAll(other.value)
  }

  override def value: util.ArrayList[String] = list
}