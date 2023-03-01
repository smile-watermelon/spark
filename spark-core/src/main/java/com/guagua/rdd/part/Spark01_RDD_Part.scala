package com.guagua.rdd.part

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Spark01_RDD_Part {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(String, String)] = sc.makeRDD(List(
      ("nba", "xxxxx"),
      ("wba", "xxxxx"),
      ("cba", "xxxxx"),
      ("nba", "xxxxx"),
      ("wba", "xxxxx")
    )).partitionBy(new MyPartitioner)

    rdd.saveAsTextFile("output")

    sc.stop()
  }

  class MyPartitioner extends Partitioner {
    override def numPartitions: Int = 3

    /**
     * 返回数据分区的索引，从0开始
     *
     * @param key
     * @return
     */
    override def getPartition(key: Any): Int = {
      // 模式匹配
      key match {
        case "nba" => 0
        case "cba" => 1
        case _ => 2
      }
    }
  }
}
