package com.guagua.rdd.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark16_Operator_Transform {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local[*]").setAppName("spark")
    val sc: SparkContext = new SparkContext(sparkConf)

    /**
     * todo groupByKey
     * spark中shuffle操作必须要落盘处理
     *
     * groupByKey 和reduceByKey的区别
     * groupByKey 和reduceByKey 都会对不同分区的数据，进行shuffle的操作
     * groupByKey 只是进行数据的分区，并不会减少数据
     * reduceByKey 会在shuffle 前对数据进行聚合操作，减少落盘的数据量，性能要比groupByKey高
     * 分区内，分区间
     * reduceByKey 聚合分区内和分区间聚合要求聚合key都是相同的，
     * ToDo 例如：求分区最大值，然后对分区间最大值进行求和，这种情况下reduceByKey就做不了了
     */
    /**
     * 相同的key的数据分到一个组，构成二元组
     * 二元组第一个元素是key, 第二个元素是value的集合
     */
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("b", 4)))
    val value: RDD[(String, Iterable[Int])] = rdd.groupByKey()

    rdd.reduceByKey((x:Int, y:Int) => x + y)

    /**
     * (a,CompactBuffer(1, 2))
     * (b,CompactBuffer(3, 4))
     */
    value.collect().foreach(println)

    val value1: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1)

    /**
     * (a,CompactBuffer((a,1), (a,2)))
     * (b,CompactBuffer((b,3), (b,4)))
     */
    value1.collect().foreach(println)

    sc.stop()
  }

}
