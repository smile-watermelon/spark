package com.guagua.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operation_Action {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    /**
     * aggregateByKey：初始值只做分区内计算
     * aggregate: 初始值不仅做分区内计算，还做分区间计算
     */
    //    val sum: Int = rdd.aggregate(0)(_ + _, _ + _)
    val sum: Int = rdd.fold(0)(_ + _)
    println(sum)

    sc.stop()

  }

}
