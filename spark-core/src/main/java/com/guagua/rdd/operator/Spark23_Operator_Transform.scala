package com.guagua.rdd.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark23_Operator_Transform {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local[*]").setAppName("spark")
    val sc: SparkContext = new SparkContext(sparkConf)

    /**
     * todo cogroup 分组连接
     */
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("c", 2), ("b", 3)
    ) )

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(
      ("b", 3), ("b", 2), ("a", 6),("b", 4)
    ))


    /**
     * (a,(CompactBuffer(1),CompactBuffer(6)))
     * (b,(CompactBuffer(3),CompactBuffer(3, 2, 4)))
     * (c,(CompactBuffer(2),CompactBuffer()))
     *
     */
    rdd.cogroup(rdd1).collect().foreach(println)



      sc.stop()
  }

}
