package com.guagua.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File_Par1 {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    // 【*】当前本机CPU的所有核数，local 用单线程模拟单核
    sparkConf.setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    /**
     * part-00000 1，2
     * part-00001 3
     * part-00002
     *
     * totalSize = 7
     * goalSize = 7 / 2 = 3 byte
     * 剩余1个字节，1 / 3 = 33.3% 大于 hadoop 的1.1 也就是 10%，所以会产生三个分区
     * 7 / 3 = 2 ... 1 (1.1) + 1  = 3
     *
     *  数据分区是如何分配的
     *  1、hadoop 是按行读取数据的
     *  2、数据读取时按照偏移量计算，偏移量不会被重复读取
     *
     *  0 =》1@@ =》012 （字节）
     *  1 =》2@@ =》345
     *  2 =》3   =》6
     *
     * 3、数据分区的偏移量的计算范围
     * 0 =》[0, 3]（字节范围） = [1,2]
     * 1 =》[3, 6]           = [3]
     * 2 =》[6, 7]           = []
     *
     */
    val rdd: RDD[String] = sc.textFile("data/1.txt", 2)

    rdd.saveAsTextFile("output")

    sc.stop()
  }
}
