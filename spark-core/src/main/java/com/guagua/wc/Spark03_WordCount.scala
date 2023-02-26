package com.guagua.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, rdd}

object Spark03_WordCount {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)

    val lines: RDD[String] = sc.textFile("data")

    val words: rdd.RDD[String] = lines.flatMap(_.split(" "))

    val wordToOne: RDD[(String, Int)] = words.map(word => (word, 1))

    // spark 提供了更多的功能，可以将分组聚合使用一个方法实现

    // wordToOne.reduceByKey((x, y) => (x + y))
    // 变量只使用一次，可以省略变量
    // wordToOne.reduceByKey((x, y) => x + y)

    // 相同的key 可以对value进行聚合
    val wordCount: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)

    val array: Array[(String, Int)] = wordCount.collect()

    array.foreach(println)


    // 3、关闭连接
    sc.stop();
  }

}
