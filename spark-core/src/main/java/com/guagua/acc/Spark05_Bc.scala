package com.guagua.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark05_Bc {

  /**
   * 闭包数据都是以 task 发送的，每个任务重都包含闭包数据
   * 这样会导致一个executor 包含大量重复的数据
   * 一个executor 是一个JVM
   * 闭包数据可以保存在executor的内存中，达到共享的目的
   * ToDo 通过广播变量实现，只读
   */
  def main(args: Array[String]): Unit = {
    // 1、建立和spark 框架的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("b", 2), ("c", 3)
    ))

    //    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(
    //      ("a", 2), ("b", 3), ("c", 4)
    //    ))
    // join 会导致数据量成几何增长，有并且会影响shuffle的性能，不推荐使用
    //    val joinRDD: RDD[(String, (Int, Int))] = rdd.join(rdd1)
    //    joinRDD.collect().foreach(println)

    val map: mutable.HashMap[String, Int] = mutable.HashMap[String, Int](
      ("a", 2), ("b", 3), ("c", 4)
    )
    rdd.map {
      case (word, num) => {
        val n: Int = map.getOrElse(word, 0)
        (word, (num, n))
      }
    }.collect().foreach(println)

    sc.stop()
  }


}