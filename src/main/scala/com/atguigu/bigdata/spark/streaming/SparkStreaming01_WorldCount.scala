package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming01_WorldCount {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming01_WorldCount")

    val streamContext = new StreamingContext(sparkConf, Seconds(3))

    //3.通过监控端口创建DStream，读进来的数据为一行行
    val socketLineDStream: ReceiverInputDStream[String] = streamContext.socketTextStream("hadoop102", 9999)
    //将采集的数据进行分解
    val workdDStream: DStream[String] = socketLineDStream.flatMap(line => line.split(" "))
    //将数据进行结构的转换方便统计分析
    val mapDStream: DStream[(String, Int)] = workdDStream.map((_, 1))

    val worldToSumDStream: DStream[(String, Int)] = mapDStream.reduceByKey(_ + _)
    worldToSumDStream.print()
    //启动采集器
    streamContext.start()
    //Driver等待采集器执行
    streamContext.awaitTermination()

    //服务器端不停止程序
    //streamContext.stop()
  }

}
