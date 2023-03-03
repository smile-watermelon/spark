package com.guagua.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Spark02_Sql_UDF {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    // 开启隐式转换功能，spark这个对象使用隐式转换
    import spark.implicits._


    val userFrame: DataFrame = spark.read.json("data/user.json")
    userFrame.createOrReplaceTempView("user")

    spark.udf.register("prefix", (name: String) => "name:" + name)

    spark.sql("select prefix(username) as uname,age from user").show


    spark.stop()
  }

  case class User(id: Int, name: String, age: Int)
}
