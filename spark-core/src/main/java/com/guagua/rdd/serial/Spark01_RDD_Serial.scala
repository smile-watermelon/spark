package com.guagua.rdd.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Serial {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hive", "spark"))

    val search: Search = new Search("h")

    // scala 类的构造参数是类的属性，
    //    search.getMatch1(rdd).collect().foreach(println)

    search.getMatch2(rdd).collect().foreach(println)


    sc.stop()
  }

  /**
   * 类的构造参数其实是类的属性，构造参数要进行闭包检测，其实就等同于类进行闭包检测
   *
   * @param query
   */
  class Search(query: String) extends Serializable {
    def isMatch(s: String): Boolean = {
      s.contains(query)
    }

    // 函数序列化案例
    def getMatch1(rdd: RDD[String]): RDD[String] = {
      //      rdd.filter(this.isMatch)
      rdd.filter(isMatch)
    }

    // 属性序列化案例
    def getMatch2(rdd: RDD[String]): RDD[String] = {
      // todo 将query 的生命周期，转为方法内的局部变量
      val s = query
      rdd.filter(x => x.contains(s))
      //      rdd.filter(x => x.contains(this.query))
      //      rdd.filter(x => x.contains(query))
      //val q = query
      //rdd.filter(x => x.contains(q))
    }
  }
}
