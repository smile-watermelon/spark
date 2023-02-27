package com.guagua.rdd.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Operator_Transform_Test {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    /**
     * 取分区最大数，然后求和
     */
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4), 2)

    /**
     * 将同一个分区中的数据，转换为相同类型的内存数组，分区不变
     */
    val glomRDD: RDD[Array[Int]] = rdd.glom()

    val mapRDD: RDD[Int] = glomRDD.map(data => {
      data.max
    })

    println(mapRDD.collect().sum)

    sc.stop()
  }

}
