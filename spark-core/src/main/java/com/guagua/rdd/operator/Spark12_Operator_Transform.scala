package com.guagua.rdd.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_Operator_Transform {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local[*]").setAppName("spark")
    val sc: SparkContext = new SparkContext(sparkConf)

    /**
     * 排序
     * 中间存在shuffle 操作
     */
    val rdd: RDD[Int] = sc.makeRDD(List(2, 3, 4, 1,6,5), 2)

    rdd.sortBy(num => num).collect().foreach(println)




    sc.stop()
  }

}
