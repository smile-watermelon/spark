package com.guagua.rdd.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_Operator_Transform1 {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local[*]").setAppName("spark")
    val sc: SparkContext = new SparkContext(sparkConf)

    /**
     * 排序
     */
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("1", 1), ("11", 2), ("2", 3)), 2)

    /**
     * (1,1)
     * (11,2)
     * (2,3)
     */
    //    val newRDD: RDD[(String, Int)] = rdd.sortBy(t => t._1)
    /**
     * (1,1)
     * (2,3)
     * (11,2)
     */
    val newRDD: RDD[(String, Int)] = rdd.sortBy(t => t._1.toInt, false)

    newRDD.collect().foreach(println)

    sc.stop()
  }

}
