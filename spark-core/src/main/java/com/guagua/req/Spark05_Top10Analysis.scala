package com.guagua.req

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_Top10Analysis {

  /**
   * 求商品分类top10 下 前10位的活跃用户
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.textFile("data/user_visit_action.txt")

    val limit = 10
    val topIds: Array[String] = getTop10(rdd, limit)

    val filterRDD: RDD[String] = rdd.filter {
      line => {
        val fields: Array[String] = line.split("_")
        if (fields(6) != "-1") {
          topIds.contains(fields(6))
        } else {
          false
        }
      }
    }

    val sessionCountRDD: RDD[((String, String), Int)] = filterRDD.map {
      line => {
        val fields: Array[String] = line.split("_")
        ((fields(6), fields(2)), 1)
      }
    }.reduceByKey(_ + _)

    val convertRDD: RDD[(String, (String, Int))] = sessionCountRDD.map {
      case ((cid, sessionId), count) => {
        (cid, (sessionId, count))
      }
    }
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = convertRDD.groupByKey()

    val result: RDD[(String, List[(String, Int)])] = groupRDD.mapValues {
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(5)
      }
    }
    result.collect().foreach(println)


    sc.stop()
  }

  private def getTop10(rdd: RDD[String], limit: Int) = {

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

    result.sortBy(_._2, false).take(limit).map(_._1)
  }
}
