package com.guagua.rdd.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark22_Operator_Transform {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local[*]").setAppName("spark")
    val sc: SparkContext = new SparkContext(sparkConf)

    /**
     * todo leftOuterJoin
     */
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("c", 2), ("b", 3)
    ) )

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(
      ("b", 3), ("b", 2), ("a", 6)
    ))


    /**
     * (a,(1,Some(6)))
     * (b,(3,Some(3)))
     * (b,(3,Some(2)))
     * (c,(2,None))
     *
     */
    rdd.leftOuterJoin(rdd1).collect().foreach(println)



      sc.stop()
  }

}
