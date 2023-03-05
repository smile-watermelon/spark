package com.guagua.stream

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object WordCount {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("stream")
    val spark: SparkSession = SparkSession.builder()
      .enableHiveSupport()
      .config(sparkConf)
      .getOrCreate()


    
    spark.close()
  }

}
