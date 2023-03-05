package com.guagua.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession, functions}
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Spark06_Test2 {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
    val spark: SparkSession = SparkSession.builder()
      // todo 要开启hive 支持
      .enableHiveSupport()
      .config(sparkConf).getOrCreate()

    import spark.implicits._

    spark.sql("use test")
    spark.udf.register("cityRemark", functions.udaf(new CityRemarkUDAF()))

    spark.sql(
      """
        |select
        |                uva.*,
        |                ci.area,
        |                ci.city_name,
        |                pi.product_name
        |            from user_visit_action as uva
        |            join city_info as ci on uva.city_id = ci.city_id
        |            join product_info as pi on pi.product_id = uva.click_product_id
        |            where uva.click_product_id > -1
        |""".stripMargin).createOrReplaceTempView("t1")

    spark.sql(
      """
        |select
        |            area,
        |            product_name,
        |            count(*) as clickCount,
        |            cityRemark(city_name) as city_remark
        | from t1
        | group by area,product_name
        |""".stripMargin).createOrReplaceTempView("t2")

    spark.sql(
      """
        | select
        |        *,
        |        rank() over(partition by area order by clickCount desc ) as rank
        | from t2
        |""".stripMargin).createOrReplaceTempView("t3")


    val df: DataFrame = spark.sql(
      """
        |select *
        |from t3
        |where rank <=3
        |""".stripMargin)

    val res: Dataset[MyResult] = df.as[MyResult]
    res.foreach {
      row: MyResult => println(row)
    }

    //    res.foreach(println)
    //    val rdd: RDD[Row] = df.rdd
    //    rdd.foreach(println)


    spark.close()
  }

  case class MyResult(area: String, product_name: String, clickCount: Long, city_remark: String, rank: Long)

  case class Buff(var total: Long, var cityMap: mutable.Map[String, Long])

  /**
   * in:
   * buf:
   * out:
   */
  class CityRemarkUDAF extends Aggregator[String, Buff, String] {
    override def zero: Buff = {
      Buff(0L, mutable.Map[String, Long]())
    }

    override def reduce(buff: Buff, city: String): Buff = {
      buff.total += 1
      val count: Long = buff.cityMap.getOrElse(city, 0L) + 1
      buff.cityMap.update(city, count)
      buff
    }

    override def merge(b1: Buff, b2: Buff): Buff = {
      b1.total = b1.total + b2.total
      val map1 = b1.cityMap
      val map2 = b2.cityMap

      map2.foreach {
        case (city, count) => {
          val newCount: Long = map1.getOrElse(city, 0L) + count
          map1.update(city, newCount)
        }
      }
      b1.cityMap = map1
      b1
    }

    override def finish(buff: Buff): String = {
      val list: ListBuffer[String] = ListBuffer[String]()

      val totalCount: Long = buff.total
      val cityMap: mutable.Map[String, Long] = buff.cityMap

      val cityList: List[(String, Long)] = cityMap.toList.sortWith {
        case (pre, next) => {
          pre._2 > next._2
        }
      }.take(2)

      val hasMore: Boolean = cityMap.size > 2
      var sum = 0.0

      cityList.foreach {
        case (city, count) => {
          val ratio: Double = (count.doubleValue() * 100 / totalCount)
          val f: String = ratio.formatted("%.2f")
          list.append(s"${city} ${f}%")
          sum += ratio
        }
      }

      if (hasMore) {
        val el: String = (100 - sum).formatted("%.2f")
        list.append(s"其他 ${el}%")
      } else {
        list.append(s"其他 0%")
      }
      list.mkString(",")

    }

    override def bufferEncoder: Encoder[Buff] = Encoders.product

    override def outputEncoder: Encoder[String] = Encoders.STRING

  }

}
