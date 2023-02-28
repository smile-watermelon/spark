package com.guagua.rdd.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark20_Operator_Transform {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local[*]").setAppName("spark")
    val sc: SparkContext = new SparkContext(sparkConf)

    /**
     * todo 区别
     */
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3),
      ("b", 3), ("b", 2), ("a", 6)
    ), 2)

    /**
     * reduceByKey:
     *
     *    combineByKeyWithClassTag[V](
     *        (v: V) => v,
     *        func,
     *        func)
     *
     * aggregateByKey:
     *
     *    combineByKeyWithClassTag[U](
     *        (v: V) => cleanedSeqOp(createZero(), v),
     *        cleanedSeqOp,
     *        combOp)
     *
     * foldByKey:
     *
     *    combineByKeyWithClassTag[V](
     *        (v: V) => cleanedFunc(createZero(), v),
     *        cleanedFunc,
     *        cleanedFunc)
     *
     *
     *  combineByKey:
     *
     *     combineByKeyWithClassTag(
     *        createCombiner,
     *        mergeValue,
     *        mergeCombiners)
     *
     */

    rdd.reduceByKey(_+_)
    rdd.aggregateByKey(0)(_+_, _+_)
    rdd.foldByKey(0)(_+_)
    rdd combineByKey(
      v => (v, 1),
      (t: (Int, Int), v) => t._1 + v,
      (t1: (Int, Int), t2: (Int, Int)) => (t1._1 + t2._1, t1._2 + t2._2)
    )

      sc.stop()
  }

}
