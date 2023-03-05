package com.guagua.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Spark06_Test1{

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
    val spark: SparkSession = SparkSession.builder()
      // todo 要开启hive 支持
      .enableHiveSupport()
      .config(sparkConf).getOrCreate()

    spark.sql("use test")
    spark.sql(
      """
        |select *
        |from(
        |select
        |        *,
        |        rank() over(partition by area order by clickCount desc ) as rank
        |    from(
        |        select
        |            area,
        |            product_name,
        |            count(*) as clickCount
        |        from (
        |            select
        |                uva.*,
        |                ci.area,
        |                ci.city_name,
        |                pi.product_name
        |            from user_visit_action as uva
        |            join city_info as ci on uva.city_id = ci.city_id
        |            join product_info as pi on pi.product_id = uva.click_product_id
        |            where uva.click_product_id > -1
        |        ) as t1 group by area,product_name
        |    ) as t2
        |) where rank <=3
        |""".stripMargin).show()

//    group by ci.area,pi.product_name




    spark.close()
  }

  case class User(id:Int, username:String, age:Int)

}
