package com.guagua.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File_Par {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    // 【*】当前本机CPU的所有核数，local 用单线程模拟单核
    sparkConf.setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)


    /**
     * defaultParallelism 默认并行度的值, 没有设置这个值 sparkConf.set("spark.default.parallelism", "5")
     * math.min(defaultParallelism, 2)
     * spark 读取文件底层使用的是Hadoop的读取方式
     * FileInputFormat getSplits() totalSize
     * long goalSize = totalSize / (long)(numSplits == 0 ? 1 : numSplits);
     * totalSize = 7
     * goalSize = 7 / 2 = 3 byte
     *
     * 剩余1个字节，1 / 3 = 33.3% 大于 hadoop 的1.1 也就是 10%，所以会产生三个分区
     * 7 / 3 = 2 ... 1 (1.1) + 1  = 3
     */
    val rdd: RDD[String] = sc.textFile("data/1.txt", 2)

    rdd.saveAsTextFile("output")

    sc.stop()
  }
}
