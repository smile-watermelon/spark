package com.guagua.rdd.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark09_Operator_Transform {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local[*]").setAppName("spark")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,1,2,3))

    /**
     * 去重，
     * map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)
     * （1， null）(1, null)根据reduceByKey 相同的key 进行聚合操作，但只关心key,再经过map 取第一个数
     */
    rdd.distinct().collect().foreach(println)

    sc.stop()
  }

}
