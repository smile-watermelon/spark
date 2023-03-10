package com.guagua.rdd.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_Transform_Test {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)


    /**
     * 获取指定分区的数据
     */
    val mapRDD: RDD[Int] = rdd.mapPartitionsWithIndex((index, iter) => {
      if (index == 1) {
        iter
      } else {
        Nil.iterator
      }
    })

    mapRDD.collect().foreach(println)

    sc.stop()
  }

}
