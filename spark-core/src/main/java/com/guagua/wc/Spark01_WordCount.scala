package com.guagua.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, rdd}

object Spark01_WordCount {

  def main(args: Array[String]): Unit = {

    // 1、建立和spark 框架的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)

    // 2、执行业务操作
    // 2.1、读取文件,获取一行一行数据
    val lines: RDD[String] = sc.textFile("data")

    // 2.2、将一行数据，拆分成一个一个的单词
    val words: rdd.RDD[String] = lines.flatMap(_.split(" "))

    // 2.3、对单词进行分组
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)

    // 2.4、对分组后的数据进行转换
    val wordCount: RDD[(String, Int)] = wordGroup.map {
      case (word, list) => (word, list.size)
    }

    // 2.5 收集数据
    val array: Array[(String, Int)] = wordCount.collect()

    // 打印数据
    array.foreach(println)


    // 3、关闭连接
    sc.stop();
  }

}
