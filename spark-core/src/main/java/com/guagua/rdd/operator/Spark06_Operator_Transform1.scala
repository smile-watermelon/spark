package com.guagua.rdd.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_Operator_Transform1 {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local[*]").setAppName("spark")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.makeRDD(List("hello", "Spark", "Scala", "Hadoop"), 2)


    /**
     * 分组和分区没有必然的关系
     *  groupBy 会将数据打乱，重新组合，这个操作称之为shuffle
     */
    val groupRDD: RDD[(Char, Iterable[String])] = rdd.groupBy(_.charAt(0))

    groupRDD.collect().foreach(println)


    sc.stop()
  }

}
