package com.guagua.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Operation_Action {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1),("a", 2),("a", 3) ), 1)

    rdd.saveAsTextFile("output")
    rdd.saveAsObjectFile("output1")
    rdd.saveAsSequenceFile("output2")

    sc.stop()

  }

}
