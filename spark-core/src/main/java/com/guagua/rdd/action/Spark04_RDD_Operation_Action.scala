package com.guagua.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operation_Action {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 1, 3, 4), 2)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 1), ("a", 1)))

    /**
     * Map(4 -> 1, 2 -> 1, 1 -> 1, 3 -> 1)
     */
    //    val intToLong: collection.Map[Int, Long] = rdd.countByValue()
    //    println(intToLong)

    val stringToLong: collection.Map[String, Long] = rdd1.countByKey()
    println(stringToLong)

    sc.stop()

  }

}
