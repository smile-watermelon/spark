package com.guagua.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    // 【*】当前本机CPU的所有核数，local 用单线程模拟单核
    sparkConf.setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val seq: Seq[Int] = Seq[Int](1, 2, 3, 4)

    //    val value: RDD[Int] = sc.parallelize(seq)
    // 使用内存数据生成rdd
    val value: RDD[Int] = sc.makeRDD(seq)

    value.collect().foreach(println)


    sc.stop()
  }
}
