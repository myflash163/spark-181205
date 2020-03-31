package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_Oper11_aggregateByKey {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)
    val listRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
    val aggRDD: RDD[(String, Int)] = listRDD.aggregateByKey(0)(math.max(_, _), _ + _)

    //foldByKey 是aggregateByKey的简化
    val foldRDD: RDD[(String, Int)] = listRDD.foldByKey(0)(_ + _)

    val combine = listRDD.combineByKey((_,1),(acc:(Int,Int),v)=>(acc._1+v,acc._2+1),(acc1:(Int,Int),acc2:(Int,Int))=>(acc1._1+acc2._1,acc1._2+acc2._2))
    aggRDD.saveAsTextFile("output")


  }

}
