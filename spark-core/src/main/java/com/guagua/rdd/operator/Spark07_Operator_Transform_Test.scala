package com.guagua.rdd.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_Operator_Transform_Test {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local[*]").setAppName("spark")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.textFile("data/apache.log")

    /**
     * 从服务器日志数据 apache.log 中获取 2015 年 5 月 17 日的请求路径
     */
    rdd.filter(line => {
      val fields: Array[String] = line.split(" ")
      fields(3).startsWith("17/05/2015")
//      line.contains("17/05/2015")
    }).collect().foreach(println)

//    filterRDD.map(line => {
//      val fields: Array[String] = line.split(" ")
//      fields(fields.length - 1)
//
//    }).collect().foreach(println)


    sc.stop()
  }

}
