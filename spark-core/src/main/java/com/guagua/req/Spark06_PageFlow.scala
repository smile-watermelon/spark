package com.guagua.req

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_PageFlow {

  /**
   * 页面跳转率需求
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.textFile("data/user_visit_action.txt")

    val mapRDD: RDD[UserVisitAction] = rdd.map {
      line => {
        val fields: Array[String] = line.split("_")
        UserVisitAction(
          fields(0),
          fields(1).toLong,
          fields(2),
          fields(3).toLong,
          fields(4),
          fields(5),
          fields(6).toLong,
          fields(7).toLong,
          fields(8),
          fields(9),
          fields(10),
          fields(11),
          fields(12).toLong
        )
      }
    }
    mapRDD.cache()

    val ids: List[Long] = List[Long](1, 2, 3, 4, 5, 6, 7)
    val pageIdsFlow: List[(Long, Long)] = ids.zip(ids.tail)

    // 计算分母
    val pageIdCount: Map[Long, Long] = mapRDD.filter {
      action => {
        ids.init.contains(action.page_id)
      }
    }.map(action => {
      (action.page_id, 1L)
    }).reduceByKey(_ + _).collect().toMap

    // 计算分子

    val sessionGroupRDD: RDD[(String, Iterable[UserVisitAction])] = mapRDD.groupBy(_.session_id)

    val pageFlowCount: RDD[(String, List[((Long, Long), Int)])] = sessionGroupRDD.mapValues {
      iter => {
        val sortedList: List[UserVisitAction] = iter.toList.sortBy(_.action_time)
        val pageIds: List[Long] = sortedList.map(_.page_id)
        val pageFlow: List[(Long, Long)] = pageIds.zip(pageIds.tail)
        pageFlow.filter(
          t => {
            pageIdsFlow.contains(t)
          }
        ).map(t => (t, 1))
      }
    }

    val flat: RDD[((Long, Long), Int)] = pageFlowCount.map(_._2).flatMap(list => list)

    val pageFlowSum: RDD[((Long, Long), Int)] = flat.reduceByKey(_ + _)
    pageFlowSum.foreach{
      case ((pre, next), sum) => {
        val l: Long = pageIdCount.getOrElse(pre, 0L)
        println(s"页面${pre}跳转到页面${next} 的跳转率为：" + (sum.toDouble / l))
      }
    }

    sc.stop()
  }

  case class UserVisitAction(
                              date: String, //用户点击行为的日期
                              user_id: Long, //用户的ID
                              session_id: String, //Session的ID
                              page_id: Long, //某个页面的ID
                              action_time: String, //动作的时间点
                              search_keyword: String, //用户搜索的关键词
                              click_category_id: Long, //某一个商品品类的ID
                              click_product_id: Long, //某一个商品的ID
                              order_category_ids: String, //一次订单中所有品类的ID集合
                              order_product_ids: String, //一次订单中所有商品的ID集合
                              pay_category_ids: String, //一次支付中所有品类的ID集合
                              pay_product_ids: String, //一次支付中所有商品的ID集合
                              city_id: Long
                            ) //城市 id

}
