package com.guagua.rdd.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[List[Int]] = sc.makeRDD(List(List(1, 2), List(3, 4)))

    val flatRdd: RDD[Int] = rdd.flatMap(list => {
      list
    })



    flatRdd.collect().foreach(println)

    sc.stop()
  }

}
