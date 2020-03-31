package com.atguigu.bigdata.spark.core

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//自定义累加器
object Spark22_Broadcast {
  def main(args: Array[String]): Unit = {

    //1.初始化配置信息及SparkContext
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val rdd1 = sc.makeRDD(List((1, "a"), (2, "b"), (3, "c")))
    val list = List((1, 1), (2, 2), (3, 3))
    //可以使用广播变量减少数据的传输
    val broadcast: Broadcast[List[(Int, Int)]] = sc.broadcast(list)
    val resultRDD: RDD[(Int, (String, Any))] = rdd1.map {
      case (key, value) => {
        var v2: Any = null
        for (t <- broadcast.value) {
          if (key == t._1) {
            v2 = t._2
          }
        }
        (key, (value, v2))
      }
    }
    resultRDD.foreach(println)


    sc.stop()
  }
}

