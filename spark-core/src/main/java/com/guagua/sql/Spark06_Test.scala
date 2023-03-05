package com.guagua.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Spark06_Test{

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
    val spark: SparkSession = SparkSession.builder()
      // todo 要开启hive 支持
      .enableHiveSupport()
      .config(sparkConf).getOrCreate()

    spark.sql("use test")
    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS `user_visit_action`(
        |  `date` string,
        |  `user_id` bigint,
        |  `session_id` string,
        |  `page_id` bigint,
        |  `action_time` string,
        |  `search_keyword` string,
        |  `click_category_id` bigint,
        |  `click_product_id` bigint,
        |  `order_category_ids` string,
        |  `order_product_ids` string,
        |  `pay_category_ids` string,
        |  `pay_product_ids` string,
        |  `city_id` bigint)
        |row format delimited fields terminated by '\t'
        |""".stripMargin)

    spark.sql("load data local inpath 'data/sqltest/user_visit_action.txt' into table test.user_visit_action")

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS `product_info`(
        |  `product_id` bigint,
        |  `product_name` string,
        |  `extend_info` string)
        |row format delimited fields terminated by '\t'
        |""".stripMargin)

    spark.sql("load data local inpath 'data/sqltest/product_info.txt' into table test.product_info")

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS `city_info`(
        |  `city_id` bigint,
        |  `city_name` string,
        |  `area` string)
        |row format delimited fields terminated by '\t'
        |""".stripMargin)

    spark.sql(
      """
        |load data local inpath 'data/sqltest/city_info.txt' into table test.city_info
        |""".stripMargin)

    spark.close()
  }

  case class User(id:Int, username:String, age:Int)

}
