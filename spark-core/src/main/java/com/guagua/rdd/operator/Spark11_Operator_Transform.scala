package com.guagua.rdd.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark11_Operator_Transform {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local[*]").setAppName("spark")
    val sc: SparkContext = new SparkContext(sparkConf)

    /**
     * coalesce 扩大分区
     *
     * 如果不进行shuffle 操作没有意义
     * 缩减分区使用coalesce
     * 扩大分区使用 repartition 底层调用就是coalesce，肯定采用shuffle
     * coalesce(numPartitions, shuffle = true)
     */
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6), 2)

    val newRDD: RDD[Int] = rdd.repartition(3)

    newRDD.saveAsTextFile("output")




    sc.stop()
  }

}
