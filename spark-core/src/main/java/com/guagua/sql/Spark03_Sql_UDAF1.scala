package com.guagua.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row, SparkSession, functions}

object Spark03_Sql_UDAF1 {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    // 开启隐式转换功能，spark这个对象使用隐式转换


    val userFrame: DataFrame = spark.read.json("data/user.json")
    userFrame.createOrReplaceTempView("user")

    // 强类型udf
    spark.udf.register("avgAge", functions.udaf(new MyAvgUDAF()))

    spark.sql("select avgAge(age) from user").show


    spark.stop()
  }


  /**
   * 泛型
   * -IN, 输入类型
   * BUF,
   * OUT 输出
   */

  /**
   * 样例类，构造参数的属性不能更改，var 可以修改
   *
   * @param total
   * @param count
   */
  case class Buff(var total: Long, var count: Long)

  class MyAvgUDAF extends Aggregator[Long, Buff, Long] {
    //    初始值，缓冲区的初始化
    override def zero: Buff = {
      Buff(0L, 0L)
    }

    /**
     * 根据输入数据更新缓冲区数据
     *
     * @param b
     * @param a
     * @return
     */
    override def reduce(buff: Buff, in: Long): Buff = {
      buff.total = buff.total + in
      buff.count = buff.count + 1;
      buff
    }

    override def merge(b1: Buff, b2: Buff): Buff = {
      b1.total = b1.total + b2.total
      b1.count = b1.count + b2.count
      b1
    }

    override def finish(reduction: Buff): Long = {
      reduction.total / reduction.count
    }

    /**
     * 缓冲区的编码操作, 自定义的类 是 product
     * @return
     */
    override def bufferEncoder: Encoder[Buff] = Encoders.product

    /**
     * 输出的编码操作, 如果是Scala 存在的类，scalaLong
     * @return
     */
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }
}
