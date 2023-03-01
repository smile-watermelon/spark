package com.guagua.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Persist {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)

    val list: List[String] = List("hello spark", "hello scala")

    val lines: RDD[String] = sc.makeRDD(list)

    val flatRDD: RDD[String] = lines.flatMap(_.split(" "))

    val mapRDD: RDD[(String, Int)] = flatRDD.map(t => {
      println("@@@@@@@")
      (t, 1)
    })

    /**
     * 默认持久化的操作，是将数据保存在内存中，
     * 如果想将数据保存在文件中，在persist（）传入保存级别
     * 对一些运行时间比较长，重要的数据可以持久化数据，让计算速度更快
     */
//    mapRDD.cache()
    mapRDD.persist(StorageLevel.DISK_ONLY)

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    reduceRDD.collect().foreach(println)

    println("--------------")

    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()

    groupRDD.collect().foreach(println)


    sc.stop()
  }

}
