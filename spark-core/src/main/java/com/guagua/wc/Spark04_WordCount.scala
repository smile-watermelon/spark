package com.guagua.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, rdd}

import scala.collection.mutable


/**
 * ./bin/spark-submit --class org.apache.spark.examples.SparkPi --master spark://hadoop-01:7077 ./examples/jars/spark-examples_2.12-3.2.3.jar 10
 */
object Spark04_WordCount {

  def wordCount1(sc: SparkContext) = {
    val lines: RDD[String] = sc.makeRDD(List("hello world", "hello spark"))

    val words: rdd.RDD[String] = lines.flatMap(_.split(" "))
    val group: RDD[(String, Iterable[String])] = words.groupBy(v => v)

    val res: RDD[(String, Int)] = group.mapValues(i => i.size)
    res.collect().foreach(println)
  }

  def wordCount2(sc: SparkContext) = {
    val lines: RDD[String] = sc.makeRDD(List("hello world", "hello spark"))

    val words: rdd.RDD[String] = lines.flatMap(_.split(" "))
    val value: RDD[(String, Int)] = words.map((_, 1))
    val group: RDD[(String, Iterable[Int])] = value.groupByKey()

    val res: RDD[(String, Int)] = group.mapValues(i => i.size)
    res.collect().foreach(println)
  }

  def wordCount3(sc: SparkContext) = {
    val lines: RDD[String] = sc.makeRDD(List("hello world", "hello spark"))

    val words: rdd.RDD[String] = lines.flatMap(_.split(" "))
    val value: RDD[(String, Int)] = words.map((_, 1))
    val group: RDD[(String, Int)] = value.reduceByKey(_ + _)

    group.collect().foreach(println)
  }

  def wordCount4(sc: SparkContext) = {
    val lines: RDD[String] = sc.makeRDD(List("hello world", "hello spark"))

    val words: rdd.RDD[String] = lines.flatMap(_.split(" "))
    val value: RDD[(String, Int)] = words.map((_, 1))
    val group: RDD[(String, Int)] = value.aggregateByKey(0)(_ + _, _ + _)

    group.collect().foreach(println)
  }

  def wordCount5(sc: SparkContext) = {
    val lines: RDD[String] = sc.makeRDD(List("hello world", "hello spark"))

    val words: rdd.RDD[String] = lines.flatMap(_.split(" "))
    val value: RDD[(String, Int)] = words.map((_, 1))
    val group: RDD[(String, Int)] = value.foldByKey(0)(_ + _)

    group.collect().foreach(println)
  }

  def wordCount6(sc: SparkContext) = {
    val lines: RDD[String] = sc.makeRDD(List("hello world", "hello spark"))

    val words: rdd.RDD[String] = lines.flatMap(_.split(" "))
    val value: RDD[(String, Int)] = words.map((_, 1))
    val group: RDD[(String, Int)] = value.combineByKey(v => v, _ + _, _ + _)

    group.collect().foreach(println)
  }

  def wordCount7(sc: SparkContext) = {
    val lines: RDD[String] = sc.makeRDD(List("hello world", "hello spark"))

    val words: rdd.RDD[String] = lines.flatMap(_.split(" "))
    val value: RDD[(String, Int)] = words.map((_, 1))
    val group: collection.Map[String, Long] = value.countByKey()

    group.foreach(println)
  }

  def wordCount8(sc: SparkContext) = {
    val lines: RDD[String] = sc.makeRDD(List("hello world", "hello spark"))

    val words: rdd.RDD[String] = lines.flatMap(_.split(" "))
    val group: collection.Map[String, Long] = words.countByValue()

    group.foreach(println)
  }

  def wordCount9(sc: SparkContext) = {
    val lines: RDD[String] = sc.makeRDD(List("hello world", "hello spark"))

    val words: rdd.RDD[String] = lines.flatMap(_.split(" "))
    //    val group: collection.Map[String, Long] = words.countByValue()
    val mapWord: RDD[mutable.Map[String, Long]] = words.map(
      word => {
        mutable.Map[String, Long]((word, 1))
      }
    )
    val group: mutable.Map[String, Long] = mapWord.reduce(
      (map1, map2) => {
        map2.foreach {
          case (word, count) => {
            val newValue: Long = map1.getOrElse(word, 0L) + count
            map1.update(word, newValue)
          }
        }
        map1
      }
    )

    group.foreach(println)
  }


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)

    //    wordCount1(sc)
    //    println("--------")
    //
    //    wordCount2(sc)
    //    println("--------")
    //
    //    wordCount3(sc)
    //    println("--------")
    //
    //    wordCount4(sc)
    //    println("--------")
    //
    //    wordCount5(sc)
    //    println("--------")
    //
    //    wordCount6(sc)
    //    println("--------")
    //
    //    wordCount7(sc)
    //    println("--------")
    //
    //    wordCount8(sc)
    //    println("--------")

    wordCount9(sc)
    println("--------")





    // 3、关闭连接
    sc.stop();
  }

}
