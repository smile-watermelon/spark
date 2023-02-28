package com.guagua.rdd.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark08_Operator_Transform {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local[*]").setAppName("spark")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6,7,8,9,10))

    /**
     * 需要传递三个参数
     * 第一个参数表示：抽取后的数据是否放回，true：放回，false 不放回
     * 第二个参数表示：
     *      如果抽取不放回，数据源中每条数据被抽取的概率, 基准值概念
     *      如果抽取放回，数据源中每条数据被抽取的可能次数
     * 第三个参数表示，抽取数据时随机算法的种子
     * 种子确定好后，每个数的概率就会被确定好了
     * 如果不传递第三个参数，使用的是当前的系统时间
     */
      //    println(rdd.sample(false, 0.5, 2)
//    println(rdd.sample(false, 0.4)
//      .collect().mkString(","))

    /**
     * 如果抽取放回
     */
    println(rdd.sample(true, 2)
      .collect().mkString(","))

    sc.stop()
  }

}
