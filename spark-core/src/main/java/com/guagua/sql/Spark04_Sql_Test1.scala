package com.guagua.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

object Spark04_Sql_Test1 {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
    val spark: SparkSession = SparkSession.builder()
      .enableHiveSupport()
      .config(sparkConf).getOrCreate()
    // 开启隐式转换功能，spark这个对象使用隐式转换
    import spark.implicits._

    spark.sql("use test")

    // 准备数据
    spark.sql(
      """
        |CREATE TABLE `user_visit_action`(
        |`date` string,
        |`user_id` bigint,
        |`session_id` string,
        |`page_id` bigint,
        |`action_time` string,
        |`search_keyword` string,
        |`click_category_id` bigint,
        |`click_product_id` bigint,
        |`order_category_ids` string,
        |`order_product_ids` string,
        |`pay_category_ids` string,
        |`pay_product_ids` string,
        |`city_id` bigint)
        |row format delimited fields terminated by '\t'
        |""".stripMargin)

    spark.sql("load data local inpath 'data/user_visit_action.txt' into table user_visit_action")


    spark.stop()
  }


}
