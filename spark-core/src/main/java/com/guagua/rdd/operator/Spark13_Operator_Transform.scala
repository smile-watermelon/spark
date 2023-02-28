package com.guagua.rdd.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark13_Operator_Transform {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local[*]").setAppName("spark")
    val sc: SparkContext = new SparkContext(sparkConf)

    // todo 双value 操作
    val rdd1 = sc.makeRDD(List(1, 2, 3, 4))
    val rdd2 = sc.makeRDD(List(3, 4, 5, 6))

    // 交集
    val value: RDD[Int] = rdd1.intersection(rdd2)
    println(value.collect().mkString(","))
    // 并集
    val value1: RDD[Int] = rdd1.union(rdd2)
    println(value1.collect().mkString(","))
    // 差集
    val value2: RDD[Int] = rdd1.subtract(rdd2)
    println(value2.collect().mkString(","))

    /**
     * 拉链
     * 1、分区数量要保持一致
     * 2、分区内的元素数量要保持一致
     */
    val value3: RDD[(Int, Int)] = rdd1.zip(rdd2)
    println(value3.collect().mkString(","))

    sc.stop()
  }

}
