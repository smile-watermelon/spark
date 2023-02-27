package com.guagua.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory_Par1 {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    // 【*】当前本机CPU的所有核数，local 用单线程模拟单核
    sparkConf.setMaster("local[*]").setAppName("RDD")

    val sc: SparkContext = new SparkContext(sparkConf)
    // [1, 2] [3, 4]
    //    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    // [1] [2] [3,4]
//    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 3)

    // [1], [2,3] [4,5]
    /**
     * val start = ((i * length) / numSlices).toInt
     * val end = (((i + 1) * length) / numSlices).toInt
     */
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 3)

    rdd.saveAsTextFile("output")


    sc.stop()
  }
}
