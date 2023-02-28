package com.guagua.rdd.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark10_Operator_Transform {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local[*]").setAppName("spark")
    val sc: SparkContext = new SparkContext(sparkConf)

    /**
     * 缩小分区
     * coalesce 默认情况下不会将分区的数据打乱重新组合
     * 1，2 在同一个分区
     * 3，4在同一个分区
     * 5，6在同一个分区
     * part0 => 1,2
     * part1 => 3,4,5,6
     * 3，4的数据并没有被打乱，这样可能出现数据倾斜，
     *
     * 默认不会进行shuffle ，传入第二个参数true，进行shuffle
     */
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6), 3)

    val newRDD: RDD[Int] = rdd.coalesce(2, true)

    newRDD.saveAsTextFile("output")




    sc.stop()
  }

}
