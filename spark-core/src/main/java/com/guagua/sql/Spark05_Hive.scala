package com.guagua.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Spark05_Hive{


  /**
   * Q：hive 2.3.9 在 mysql 8 初始化数据库时，hive 元数据库的创建会出现，创建索引长度 3072 的问题，
   * select @@global.sql_mode
   * 解决方法：修改mysql 的sql_mode =""
   * set global sql_mode=""
   * flush privileges
   */
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
    val spark: SparkSession = SparkSession.builder()
      // todo 要开启hive 支持
      .enableHiveSupport()
      .config(sparkConf).getOrCreate()

    spark.sql("use test")
    spark.sql("show tables").show()


    spark.close()
  }

  case class User(id:Int, username:String, age:Int)

}
