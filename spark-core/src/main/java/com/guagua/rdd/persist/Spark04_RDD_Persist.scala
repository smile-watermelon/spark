package com.guagua.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Persist {

  /**
   * ToDo cache, persist，checkpoint的区别
   *
   * cache: 数据保存在内存中，速度快，但不安全
   * persist: 数据保存在磁盘文件中，更安全，但job 执行完成后会删除保存的临时文件
   * checkpoint：数据保存在指定的文件路径中，job执行完成后，不会删除文件
   *             单独使用checkpoint 作业会计算两次，运行速度慢，一般会和 cache 联合使用
   *
   */
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)

    // ToDo 设置检查点路径
    sc.setCheckpointDir("checkpoint")

    val list: List[String] = List("hello spark", "hello scala")

    val lines: RDD[String] = sc.makeRDD(list)

    val flatRDD: RDD[String] = lines.flatMap(_.split(" "))

    val mapRDD: RDD[(String, Int)] = flatRDD.map(t => {
      println("@@@@@@@")
      (t, 1)
    })

    // ToDo 需要落盘，需要指定检查点的保存路径，检查点路径中保存的文件，当作业执行完毕后不会被删除
    mapRDD.cache()
    mapRDD.checkpoint()

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    reduceRDD.collect().foreach(println)


    println("--------------")

    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()

    groupRDD.collect().foreach(println)


    sc.stop()
  }

}
