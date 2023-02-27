package com.guagua.rdd.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_Transform1 {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    /**
     * 分区和数据映射
     */
    val mapRDD: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex((index, iter) => {
      iter.map(num => {
        (index, num)
      })
    })


    mapRDD.collect().foreach(println)

    sc.stop()
  }

}
