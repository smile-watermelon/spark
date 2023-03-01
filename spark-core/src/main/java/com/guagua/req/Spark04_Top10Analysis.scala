package com.guagua.req

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark04_Top10Analysis {

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

    val acc = new HotCategoryAccumulator
    sc.register(acc, "HotCategoryAccumulator")

    rdd.foreach {
      line => {
        val fields: Array[String] = line.split("_")

        if (fields(6) != "-1") {
          acc.add((fields(6), "click"))
        } else if (fields(8) != "null") {
          val ids: Array[String] = fields(8).split(",")
          ids.foreach {
            id => {
              acc.add(id, "order")
            }
          }
        } else if (fields(10) != "null") {
          val ids: Array[String] = fields(10).split(",")
          ids.foreach {
            id => {
              acc.add(id, "pay")
            }
          }
        }
      }
    }

    val value: mutable.HashMap[String, HotCategory] = acc.value

    val categories: Iterable[HotCategory] = value.values
    val result: List[HotCategory] = categories.toList.sortWith {
      (left, right) => {
        if (left.clickCnt > right.clickCnt) {
          true
        } else if (left.clickCnt == right.clickCnt) {
          if (left.orderCnt > right.orderCnt) {
            true
          } else if (left.orderCnt == right.orderCnt) {
            left.payCnt > right.payCnt
          } else {
            false
          }
        } else {
          false
        }
      }
    }


    result.take(10).foreach(println)

    sc.stop()
  }

  /**
   * 商品统计封装
   *
   * @param clickCnt
   * @param orderCnt
   * @param payCnt
   */
  case class HotCategory(cid: String, var clickCnt: Int, var orderCnt: Int, var payCnt: Int)


  /**
   * 自定义累加器
   * in （商品id, 事件）
   * out (商品id, HotCategory)
   */
  class HotCategoryAccumulator extends AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] {

    private val map = mutable.HashMap[String, HotCategory]()

    override def isZero: Boolean = {
      map.isEmpty
    }

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = new HotCategoryAccumulator

    override def reset(): Unit = {
      map.clear()
    }

    override def add(v: (String, String)): Unit = {
      val cid: String = v._1
      val event: String = v._2
      val category: HotCategory = map.getOrElse(cid, new HotCategory(cid, 0, 0, 0))
      if (event == "click") {
        category.clickCnt += 1
      } else if (event == "order") {
        category.orderCnt += 1
      } else if (event == "pay") {
        category.payCnt += 1
      }
      map.update(cid, category)
    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
      val map1 = this.map
      val map2 = other.value

      map2.foreach {
        case (cid, hc) => {
          val category: HotCategory = map1.getOrElse(cid, new HotCategory(cid, 0, 0, 0))
          category.clickCnt += hc.clickCnt
          category.orderCnt += hc.orderCnt
          category.payCnt += hc.payCnt
          map1.update(cid, category)
        }
      }
    }

    override def value: mutable.HashMap[String, HotCategory] = map
  }
}
