package com.guagua.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Spark03_Sql_UDAF {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    // 开启隐式转换功能，spark这个对象使用隐式转换


    val userFrame: DataFrame = spark.read.json("data/user.json")
    userFrame.createOrReplaceTempView("user")

    spark.udf.register("avgAge", new MyAvgUDAF())

    spark.sql("select avgAge(age) from user").show


    spark.stop()
  }


  class MyAvgUDAF extends UserDefinedAggregateFunction {
    // 输入结构，也就是输入的参数类型
    override def inputSchema: StructType = {
      StructType(
        Array(
          StructField("age", LongType)
        )
      )
    }

    // 缓冲区做计算的结构
    override def bufferSchema: StructType = {
      StructType(
        Array(
          StructField("total", LongType),
          StructField("sum", LongType)
        )
      )
    }

    // 函数计算结果的数据类型
    override def dataType: DataType = LongType

    // 函数的稳定性
    override def deterministic: Boolean = true

    // 缓冲区初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      //      方式-
      //      buffer(0) = 0L
      //      buffer(1) = 0L
      //方式二
      buffer.update(0, 0L)
      buffer.update(1, 0L)
    }

    // 根据输入的每一条数据，更新缓冲区
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer.update(0, buffer.getLong(0) + input.getLong(0))
      buffer.update(1, buffer.getLong(1) + 1)
    }

    /**
     * 缓冲区数据合并
     *
     * @param buffer1
     * @param buffer2
     */
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0))
      buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1))
    }

    /**
     * 计算平均值
     *
     * @param buffer
     * @return
     */
    override def evaluate(buffer: Row): Any = {
      buffer.getLong(0) / buffer.getLong(1)
    }
  }
}
