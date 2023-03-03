package com.guagua.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Sql_Basic {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    // 开启隐式转换功能，spark这个对象使用隐式转换
    import spark.implicits._


    // RDD
    val userFrame: DataFrame = spark.read.json("data/user.json")
    userFrame.show()
    //        spark.read.csv()
    //        spark.read.jdbc()

    // DataFrame => sql
    userFrame.createOrReplaceTempView("user")
    spark.sql("select * from user").show()
    spark.sql("select username, age from user").show()
    spark.sql("select avg(age) from user").show()

    // DataFrame => DSL
    userFrame.select("age", "username").show()
    userFrame.select($"age" + 1).show()

    // DataFrame

    // DataSet
    val seq: Seq[Int] = Seq(1, 2, 3, 4)
    val dataSet: Dataset[Int] = seq.toDS()
    dataSet.show()


    // RDD <=> DataFrame
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 20), (2, "lisi", 30), (3, "wangwu", 30)))
    val frame: DataFrame = rdd.toDF("id", "name", "age")
    //    val rdd1: RDD[Row] = frame.rdd

    // DataFrame <=> Dataset
    val ds: Dataset[User] = frame.as[User]
    val frame1: DataFrame = ds.toDF()

    // RDD <=> DataSet

    val ds1: Dataset[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }.toDS()

    val rdd1: RDD[User] = ds1.rdd


    spark.stop()
  }

  case class User(id: Int, name: String, age: Int)
}
