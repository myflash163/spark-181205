package com.atguigu.bigdata.spark.streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SparkStreaming03_MyReceiver {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming01_WorldCount")

    val streamContext = new StreamingContext(sparkConf, Seconds(3))

    val receiverDStream: ReceiverInputDStream[String] = streamContext.receiverStream(new MyReceiver("hadoop102", 9999))
    //将采集的数据进行分解
    val workdDStream: DStream[String] = receiverDStream.flatMap(line => line.split(" "))
    //将数据进行结构的转换方便统计分析
    val mapDStream: DStream[(String, Int)] = workdDStream.map((_, 1))

    val worldToSumDStream: DStream[(String, Int)] = mapDStream.reduceByKey(_ + _)
    worldToSumDStream.print()
    //启动采集器
    streamContext.start()
    //Driver等待采集器执行
    streamContext.awaitTermination()

  }

}

//声明采集器
class MyReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
  private var socket: Socket = null

  def receive(): Unit = {
    println("host:" + host)
    socket = new Socket(host, port)
    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, "UTF-8"))
    var line: String = null
    while ((line = reader.readLine()) != null) {
      if ("END".equals(line)) {
        return
      }
      this.store(line)
    }

  }

  override def onStart(): Unit = {
    println("onStart:" + host)
    new Thread(new Runnable {
      override def run(): Unit = {
        receive()
      }
    }).start()

  }

  override def onStop(): Unit = {
    if (socket != null) {
      socket.close()
      socket = null
    }
  }
}
