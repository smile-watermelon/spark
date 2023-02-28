package com.guagua.rdd.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark17_Operator_Transform1 {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local[*]").setAppName("spark")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)), 2)

//    val value: RDD[(String, Int)] = rdd.aggregateByKey(0)(
//      (x, y) => math.max(x, y),
//      (x, y) => x + y
//    )

    // 上面步骤的简化
    val value: RDD[(String, Int)] = rdd.foldByKey(0)(_ +_)

    value.collect().foreach(println)

    sc.stop()
  }

}
