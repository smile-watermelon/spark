package com.guagua.rdd.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark24_Operator_Transform {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local[*]").setAppName("spark")
    val sc: SparkContext = new SparkContext(sparkConf)

    /**
     * todo agent.log：时间戳，省份，城市，用户，广告，中间字段使用空格分隔。
     * 统计出 每一个省份 每个广告 被点击数量排行的 Top3
     */
    val rdd: RDD[String] = sc.textFile("data/agent.log")

    val reduceRDD: RDD[((String, String), Int)] = rdd.map(line => {
      val fields: Array[String] = line.split(" ")
      ((fields(1), fields(fields.length - 1)), 1)

    }).reduceByKey(_ + _)

    val groupRdd: RDD[(String, Iterable[(String, Int)])] = reduceRDD.map {
      case (tuple, sum) => (tuple._1, (tuple._2, sum))
    }.groupByKey()

    groupRdd.mapValues(iter => {
      iter.toList.sortBy(v => v._2)(Ordering.Int.reverse).take(3)
    }).collect().foreach(println)


    sc.stop()
  }

}
