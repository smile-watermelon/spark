package com.guagua.rdd.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark17_Operator_Transform {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local[*]").setAppName("spark")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)), 2)

    // aggregateByKey存在函数柯里化有两个参数列表
    // 第一个参数列表
    //  初始值 主要用于当碰见第一个key的时候，进行分区计算
    // 第二个参数列表
          // 第一个参数表示分区内计算规则
          // 第二个参数表示分区间计算规则
    val value: RDD[(String, Int)] = rdd.aggregateByKey(0)(
      (x, y) => math.max(x, y),
      (x, y) => x + y
    )

    value.collect().foreach(println)

    sc.stop()
  }

}
