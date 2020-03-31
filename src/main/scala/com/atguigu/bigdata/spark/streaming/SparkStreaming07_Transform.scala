package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SparkStreaming07_Transform {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming01_WorldCount")

    val streamContext = new StreamingContext(sparkConf, Seconds(5))
    val socketLineDStream: ReceiverInputDStream[String] = streamContext.socketTextStream("hadoop102", 9999)

    //转换
    //运行Driver中 (1)
    socketLineDStream.transform {
      case rdd => {
        //运行在Driver中（m=采集周期）
        rdd.map {
          //运行中Executor中（n）
          case x => {
            x
          }
        }
      }
    }

    //socketLineDStream.foreachRDD()



    //启动采集器
    streamContext.start()
    //Driver等待采集器执行
    streamContext.awaitTermination()
  }

}
