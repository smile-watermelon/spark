package com.guagua.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, rdd}

object Spark02_WordCount {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)

    val lines: RDD[String] = sc.textFile("data")

    val words: rdd.RDD[String] = lines.flatMap(_.split(" "))

    val wordToOne: RDD[(String, Int)] = words.map(word => (word, 1))

    val wordGroup: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(t => t._1)


    val wordCount: RDD[(String, Int)] = wordGroup.map {
      case (word, list) => {

        list.reduce((t1, t2) => {
          (t1._1, t1._2 + t2._2)
        })
      }
    }

    val array: Array[(String, Int)] = wordCount.collect()

    array.foreach(println)


    // 3、关闭连接
    sc.stop();
  }

}
