package com.guagua.rdd.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark18_Operator_Transform {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local[*]").setAppName("spark")
    val sc: SparkContext = new SparkContext(sparkConf)

    /**
     * 求平均值
     */
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3),
      ("b", 3), ("b", 2), ("a", 6)
    ), 2)

    /**
     * combineByKey方法参数
     * 第一个参数：将相同key 的第一个数据进行结构转换
     * 第二个参数：分区内的计算方式
     * 第三个参数：分区间的计算方式
     */
    val value: RDD[(String, (Double, Int))] = rdd.combineByKey(
      v => (v, 1),
      (t: (Double, Int), v) => {
        (t._1 + v, t._2 + 1)
      },
      (t1: (Double, Int), t2: (Double, Int)) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )

    value.mapValues{
      case (num, count) => {
        num / count
      }
    }.collect().foreach(println)

    sc.stop()
  }

}
