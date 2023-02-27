package com.guagua.rdd.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Transform2 {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(List(1,2),3,List(4,5)))

    val flatRDD: RDD[Any] = rdd.flatMap(data => {
      data match {
        case list: List[_] => list
        case dat => List(dat)
      }
    })

    flatRDD.collect().foreach(println)

    sc.stop()
  }

}
