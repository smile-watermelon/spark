package com.guagua.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operation_Action {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // 触发计算任务

    /**
     * collect 将不同分区的数据，按照分区顺序采集到driver 内存中
     */
    //    val array: Array[Int] = rdd.collect()
    //    println(array.mkString(","))

    val sum: Int = rdd.reduce(_ + _)
    println(sum)

    // 数据源中元素的个数
    val count: Long = rdd.count()

    // 获取数据源中的第一个
    val first: Int = rdd.first()
    println(first)

    // 取前两个数据
    val ints: Array[Int] = rdd.take(2)
    println(ints.mkString(","))

    // 排序取n个数据
    val ints1: Array[Int] = rdd.takeOrdered(2)(Ordering.Int.reverse)
    println(ints1.mkString(","))

    sc.stop()

  }

}
