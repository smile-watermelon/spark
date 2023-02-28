package com.guagua.rdd.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark17_Operator_Transform2 {

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

    val newRDD: RDD[(String, (Double, Int))] = rdd.aggregateByKey((0.0, 0))(
      (t, v) => {
        (t._1 + v, t._2 + 1)
      },
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )
    newRDD.mapValues {
      case (num, count) => {
        num / count
      }
    }.collect().foreach(println)

//    newRDD.map(t => {
//      (t._1, t._2._1 / t._2._2)
//    }).collect().foreach(println)


    sc.stop()
  }

}
