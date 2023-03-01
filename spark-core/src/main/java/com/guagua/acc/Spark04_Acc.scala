package com.guagua.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark04_Acc {

  def main(args: Array[String]): Unit = {
    // 1、建立和spark 框架的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)

    val acc = new MyAccumulator()

    sc.register(acc, "wordCountAcc")
    // 设置累加器
    val rdd: RDD[String] = sc.makeRDD(List("hello world", "hello spark"))

    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))


    flatRDD.foreach(word => {
      acc.add(word)
    })

    println(acc.value)


    sc.stop()
  }

  class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {


    private var wcMap = mutable.HashMap[String, Long]()

    override def isZero: Boolean = {
      wcMap.isEmpty
    }

    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      new MyAccumulator()
    }

    override def reset(): Unit = {
      wcMap.clear()
    }

    override def add(word: String): Unit = {
      val c: Long = wcMap.getOrElse(word, 0L) + 1
      wcMap.update(word, c)
    }


    /**
     * driver 合并多个累加器
     *
     * @param other
     */
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      val map1 = this.wcMap
      val map2 = other.value

      map2.foreach{
        case (word, count) => {
          val newCount = map1.getOrElse(word, 0L) + count
          map1.update(word, newCount)
        }
      }
    }

    override def value: mutable.Map[String, Long] = {
      wcMap
    }
  }


}