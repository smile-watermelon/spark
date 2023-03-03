package com.guagua.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Spark05_Hive{

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
    val spark: SparkSession = SparkSession.builder()
      .config(sparkConf).getOrCreate()
    import spark.implicits._

    spark.sql("show tables").show()


    spark.close()
  }

  case class User(id:Int, username:String, age:Int)

}
