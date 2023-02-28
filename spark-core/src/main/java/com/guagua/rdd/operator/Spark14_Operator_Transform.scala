package com.guagua.rdd.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark14_Operator_Transform {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local[*]").setAppName("spark")
    val sc: SparkContext = new SparkContext(sparkConf)

    // todo key - value
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    val tuple: RDD[(Int, Int)] = rdd.map((_, 1))
    // 根据指定的数据规则，对数据进行重新分区
    // 分区器和分区数量相等，会返回RDD 自己
    /**
     * 分区器
     * 默认是hashPartition
     * RangePartition 范围分区器， 一般在排序的时候用的比较多
     */
    val newRDD = tuple.partitionBy(new HashPartitioner(2))
    val newRDD1 = newRDD.partitionBy(new HashPartitioner(2))

      newRDD1.saveAsTextFile("output")

    sc.stop()
  }

}
