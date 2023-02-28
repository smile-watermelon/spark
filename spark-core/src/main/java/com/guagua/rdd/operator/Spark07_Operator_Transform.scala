package com.guagua.rdd.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_Operator_Transform {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local[*]").setAppName("spark")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)


    /**
     * filter 可能会产生数据倾斜
     */
    val filterRDD: RDD[Int] = rdd.filter(_ % 2 == 1)
    filterRDD.collect().foreach(println)


    sc.stop()
  }

}
