package com.guagua.rdd.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.Date

object Spark06_Operator_Transform_Test {

  /**
   * 获取apache.log 中每个时间段的访问量
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local[*]").setAppName("spark")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.textFile("data/apache.log")


    val mapRDD: RDD[(String, Int)] = rdd.map(line => {
      val fields: Array[String] = line.split(" ")
      val dateStr: String = fields(3)
      //      val hour: String = date.split(":")(1)
      //      hour
      val dateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val d: Date = dateFormat.parse(dateStr)
      val dateFormatHour = new SimpleDateFormat("HH")
      val hour: String = dateFormatHour.format(d)
      (hour, 1)
    })

    val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupBy(_._1)

    groupRDD.map{
      case (hour, iter) => (hour, iter.size)
    }.collect().foreach(println)



    sc.stop()
  }

}
