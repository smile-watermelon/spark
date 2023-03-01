package com.guagua.req

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Top10Analysis {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)

    /**
     * [0 2019-07-17]
     * [1 39]
     * [2 e17469bf-0aa1-4658-9f76-309859dcd641]
     * [3 19]
     * [4 2019-07-17 00:02:38]
     * [5 null]
     * [6 -1]       非点击事件
     * [7 -1]
     * [8 1,19,17,3,14] 订单中所有品类的集合
     * [9 99,46]        订单中所有商品的 ID 集合
     * [10 null]    支付中所有品类的 ID 集合
     * [11 null]    支付中所有商品的 ID 集合
     * [12 20]      城市 id
     */
    /**
     * Q todo rdd 重复使用
     * q cogroup 可能存在shuffle 性能较低
     */
    val rdd: RDD[String] = sc.textFile("data/user_visit_action.txt")

    val datas: RDD[(String, (Int, Int, Int))] = rdd.flatMap {
      line => {
        val fields: Array[String] = line.split("_")

        if (fields(6) != "-1") {
          List((fields(6), (1, 0, 0)))
        } else if (fields(8) != "null") {
          val ids: Array[String] = fields(8).split(",")
          ids.map(id => (id, (0, 1, 0)))
        } else if (fields(10) != "null") {
          val ids: Array[String] = fields(10).split(",")
          ids.map(id => (id, (0, 0, 1)))
        } else {
          Nil
        }
      }
    }

    val result: RDD[(String, (Int, Int, Int))] = datas.reduceByKey {
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    }

    val resultSorted: Array[(String, (Int, Int, Int))] = result.sortBy(_._2, false).take(10)
    resultSorted.foreach(println)

    sc.stop()
  }

}
