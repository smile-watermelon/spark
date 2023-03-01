package com.guagua.req

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Top10Analysis {

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

    rdd.cache()

    // 统计点击数量
    val clickAction: RDD[String] = rdd.filter(line => {
      val fields: Array[String] = line.split("_")
      fields(6) != "-1"
    })

    val clickCount: RDD[(String, Int)] = clickAction.map(line => {
      val fields: Array[String] = line.split("_")
      (fields(6), 1)
    }).reduceByKey(_ + _)

    val rdd1 = clickCount.map {
      case (pid, count) => {
        (pid, (count, 0, 0))
      }
    }

    // 统计下单数量
    val orderAction: RDD[String] = rdd.filter(line => {
      val fields: Array[String] = line.split("_")
      fields(8) != "null"
    })

    val orderCount: RDD[(String, Int)] = orderAction.flatMap(
      line => {
        val fields: Array[String] = line.split("_")
        val cid: String = fields(8)
        val cids: Array[String] = cid.split(",")
        cids.map(id => (id, 1))
      }
    ).reduceByKey(_ + _)

    val rdd2 = orderCount.map {
      case (pid, count) => {
        (pid, (0, count, 0))
      }
    }

    // 统计支付数量
    val payAction: RDD[String] = rdd.filter(line => {
      val fields: Array[String] = line.split("_")
      fields(10) != "null"
    })

    val payCount: RDD[(String, Int)] = payAction.flatMap(line => {
      val fields: Array[String] = line.split("_")
      val payIds: Array[String] = fields(10).split(",")
      payIds.map(id => (id, 1))
    }).reduceByKey(_ + _)

    val rdd3 = payCount.map {
      case (pid, count) => {
        (pid, (0, 0, count))
      }
    }
    // 将三个数据源合并在一起，统一进行聚合计算
    val unionRDD: RDD[(String, (Int, Int, Int))] = rdd1.union(rdd2).union(rdd3)

    val result: RDD[(String, (Int, Int, Int))] = unionRDD.reduceByKey {
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t1._3)
      }
    }

    val resultSorted: Array[(String, (Int, Int, Int))] = result.sortBy(_._2, false).take(10)
    resultSorted.foreach(println)

    sc.stop()
  }

}
