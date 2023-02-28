package com.guagua.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operation_Action {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    /**
     * collect 按照分区顺序对数据进行收集，foreach 在是在driver端打印
     * driver 端发送完数据，后executor 进行数据计算后，将结果返回给driver，采集后的数据在driver端打印
     */
    rdd.collect().foreach(println)
    println("--------------")

    /**
     * 是在 executor 端打印
     * 为了区分scala 的方法和RDD 的方法，
     * 把RDD的方法称为算子，
     * RDD方法外的操作，例如上面的：println("--------------") 是在driver端执行，
     * foreach里的println 方法是在 exectuor 端执行
     */
    rdd.foreach(println)
    sc.stop()

  }

}
