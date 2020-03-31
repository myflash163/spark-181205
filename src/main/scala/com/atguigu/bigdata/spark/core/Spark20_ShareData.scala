package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}


object Spark20_ShareData {
  def main(args: Array[String]): Unit = {

    //1.初始化配置信息及SparkContext
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    /*    val i: Int = dataRDD.reduce(_ + _)
    println(i)*/
    //创建累加器对象
    val accumulator: LongAccumulator = sc.longAccumulator
    dataRDD.foreach {
      case i => {
        accumulator.add(i)
      }
    }
    println("sum=" + accumulator.value)

    sc.stop()
  }
}
