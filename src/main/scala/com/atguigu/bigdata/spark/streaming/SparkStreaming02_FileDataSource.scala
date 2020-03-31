package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming02_FileDataSource {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming01_WorldCount")

    val streamContext = new StreamingContext(sparkConf, Seconds(3))
    //从文件夹种采集数据
    val fileDStream: DStream[String] = streamContext.textFileStream("test")
    //将采集的数据进行分解
    val workdDStream: DStream[String] = fileDStream.flatMap(line => line.split(" "))
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
