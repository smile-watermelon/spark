package com.guagua.rdd.dep

import org.apache.spark.{SparkConf, SparkContext, rdd}
import org.apache.spark.rdd.RDD

object Spark01_RDD_Dep {

  def main(args: Array[String]): Unit = {
    // 1、建立和spark 框架的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)

    val lines: RDD[String] = sc.textFile("data/words.txt")
    println(lines.toDebugString)
    println("---------------")

    val words: rdd.RDD[String] = lines.flatMap(_.split(" "))
    println(words.toDebugString)
    println("---------------")

    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)
    println(wordGroup.toDebugString)
    println("---------------")


    val wordCount: RDD[(String, Int)] = wordGroup.map {
      case (word, list) => (word, list.size)
    }
    println(wordCount.toDebugString)
    println("---------------")

    val array: Array[(String, Int)] = wordCount.collect()

    array.foreach(println)

    sc.stop()
  }

}
