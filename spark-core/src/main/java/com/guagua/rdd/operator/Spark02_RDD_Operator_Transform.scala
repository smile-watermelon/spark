package com.guagua.rdd.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    /**
     * 一次性把一个分区的数据拿过来处理，但是会将分区的数据整个加载到内存中进行引用
     * 如果处理完的数据是不会被释放掉的，存在对象引用
     * 在内存较小，数据量较大的场合下，容易出现内存溢出
     */
    val mapRdd: RDD[Int] = rdd.mapPartitions(iter => {
      println(">>>>>>>")
      iter.map(_ * 2)
    })

    mapRdd.collect().foreach(println)

    sc.stop()
  }

}
