package com.guagua.rdd.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark15_Operator_Transform {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local[*]").setAppName("spark")
    val sc: SparkContext = new SparkContext(sparkConf)

    // todo reduceByKey
    /**
     * 相等的key 进行value的聚合操作
     * scala 了的聚合是量量操作，spark也一样
     */
    val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("b", 4)))
    val value: RDD[(String, Int)] = rdd.reduceByKey((x: Int, y: Int) => {
      println(s"x=${x}, y=${y}")
      x + y
    })

    value.collect().foreach(println)

    sc.stop()
  }

}
