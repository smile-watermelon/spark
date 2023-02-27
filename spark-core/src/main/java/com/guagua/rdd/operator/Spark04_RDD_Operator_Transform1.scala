package com.guagua.rdd.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Transform1 {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.makeRDD(List("hello world", "hello spark"))

    val flatRDD: RDD[String] = rdd.flatMap(str => {
      str.split(" ")
    })

    flatRDD.collect().foreach(println)

    sc.stop()
  }

}
