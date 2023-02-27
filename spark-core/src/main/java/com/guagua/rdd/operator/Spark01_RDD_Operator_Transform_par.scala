package com.guagua.rdd.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform_par {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)


    /**
     * 1、rdd 的计算一个分区内的数据是一个一个执行的逻辑
     * 只有前面一个数据的逻辑全部执行完成后，才会执行下一个数据
     * 分区内数据的执行是有序的
     *  2、不同分区的计算是无序的
     */
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)

    val mapRDD: RDD[Int] = rdd.map(num => {
      println(">>>>>>>" + num)
      num
    })

    val mapRDD1: RDD[Int] = mapRDD.map(num => {
      println("#############" +  num)
      num
    })

    mapRDD1.collect()


    sc.stop()
  }

}
