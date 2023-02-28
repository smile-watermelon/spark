package com.guagua.rdd.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_Operator_Transform {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local[*]").setAppName("spark")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)


    /**
     * groupBy 会将数据源中的每一个数据进行分组判断，根据返回的key 进行分组
     * 相同key值的数据会放到同一组
     * @param num
     */
    def groupFunction(num : Int): Int = {
      num % 2
    }

    val groupRDD: RDD[(Int, Iterable[Int])] = rdd.groupBy(groupFunction)

    groupRDD.collect().foreach(println)



    sc.stop()
  }

}
