package com.guagua.rdd.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark21_Operator_Transform {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local[*]").setAppName("spark")
    val sc: SparkContext = new SparkContext(sparkConf)

    /**
     * todo join
     */
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3)
    ) )

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(
      ("b", 3), ("b", 2), ("a", 6)
    ))

    /**
     * 1、相同的key 出现多次会进行多次join，可能会出现笛卡尔积
     * 2、没有相同的key 数据会过滤掉
     */
    /**
     * (a,(1,6))
     * (a,(2,6))
     * (b,(3,3))
     * (b,(3,2))
     */
    rdd.join(rdd1).collect().foreach(println)



      sc.stop()
  }

}
