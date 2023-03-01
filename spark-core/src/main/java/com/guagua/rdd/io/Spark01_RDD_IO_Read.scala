package com.guagua.rdd.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_IO_Read {

  def main(args: Array[String]): Unit = {
    // 1、建立和spark 框架的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)

    val value: RDD[String] = sc.textFile("output1")
    println(value.collect().mkString(","))

    val rdd1: RDD[(String, Int)] = sc.objectFile("output2")
    println(rdd1.collect().mkString(","))

    val value1: RDD[(String, Int)] = sc.sequenceFile("output3")
    println(value1.collect().mkString(","))


    sc.stop()
  }

}
