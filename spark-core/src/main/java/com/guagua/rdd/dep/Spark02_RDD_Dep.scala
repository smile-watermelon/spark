package com.guagua.rdd.dep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, rdd}

object Spark02_RDD_Dep {

  /**
   * ToDo
   * OneToOne (Narrow 窄)依赖，上游RDD的数据，只被下游的一个RDD分区使用
   *    org.apache.spark.OneToOneDependency
   * Shuflle (宽) 依赖，上游RDD的数据，被下游的多个RDD分区使用
   *    org.apache.spark.ShuffleDependency
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)

    val lines: RDD[String] = sc.textFile("data/words.txt")
    println(lines.dependencies)
    println("---------------")

    val words: rdd.RDD[String] = lines.flatMap(_.split(" "))
    println(words.dependencies)
    println("---------------")

    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)
    println(wordGroup.dependencies)
    println("---------------")


    val wordCount: RDD[(String, Int)] = wordGroup.map {
      case (word, list) => (word, list.size)
    }
    println(wordCount.dependencies)
    println("---------------")

    val array: Array[(String, Int)] = wordCount.collect()

    array.foreach(println)

    sc.stop()
  }

}
