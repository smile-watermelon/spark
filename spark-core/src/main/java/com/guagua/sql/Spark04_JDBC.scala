package com.guagua.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Spark04_JDBC {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
    val spark: SparkSession = SparkSession.builder()
      .config(sparkConf).getOrCreate()
    import spark.implicits._

    val df: DataFrame = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://mysql8:3306/spark_sql?useUnicode=true&characterEncoding=UTF-8")
      .option("user", "guagua")
      .option("password", "16351018")
      .option("dbtable", "user")
      .load()
//      .show()

    val users: RDD[User] = spark.sparkContext.makeRDD(List(
      User(3, "guagua", 18)
    ))
    val userDf: DataFrame = users.toDF()
    userDf.write
      .format("jdbc")
      .option("url", "jdbc:mysql://mysql8:3306/spark_sql?useUnicode=true&characterEncoding=UTF-8")
      .option("user", "guagua")
      .option("password", "16351018")
      .option("dbtable", "user")
      .mode(SaveMode.Append)
      .save()

    spark.close()
  }

  case class User(id:Int, username:String, age:Int)

}
