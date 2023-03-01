package com.guagua.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Persist {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)

    val list: List[String] = List("hello spark", "hello scala")

    val lines: RDD[String] = sc.makeRDD(list)

    val flatRDD: RDD[String] = lines.flatMap(_.split(" "))

    val mapRDD: RDD[(String, Int)] = flatRDD.map((_, 1))

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    reduceRDD.collect().foreach(println)

    println("--------------")


    val lines1: RDD[String] = sc.makeRDD(list)

    val flatRDD1: RDD[String] = lines1.flatMap(_.split(" "))

    val mapRDD1: RDD[(String, Int)] = flatRDD1.map((_, 1))

    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD1.groupByKey()

    groupRDD.collect().foreach(println)


    sc.stop()
  }

}
