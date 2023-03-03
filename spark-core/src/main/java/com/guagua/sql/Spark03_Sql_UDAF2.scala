package com.guagua.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

object Spark03_Sql_UDAF2 {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    // 开启隐式转换功能，spark这个对象使用隐式转换
    import spark.implicits._


    val userFrame: DataFrame = spark.read.json("data/user.json")
    userFrame.createOrReplaceTempView("user")

    val ds: Dataset[User] = userFrame.as[User]

    /**
     * 起别名的时候有问题
     * Exception in thread "main" org.apache.spark.sql.AnalysisException: Typed column myavgudaf(knownnotnull(assertnotnull(input[0, com.guagua.sql.Spark03_Sql_UDAF2$Buff, true])).total AS `total`,
     * knownnotnull(assertnotnull(input[0, com.guagua.sql.Spark03_Sql_UDAF2$Buff, true])).count AS `count`,
     * newInstance(class com.guagua.sql.Spark03_Sql_UDAF2$Buff),
     * boundreference()) AS `avg` that needs input type and schema cannot be passed in untyped `select` API. Use the typed `Dataset.select` API instead.;

     */
    val ageAvg: TypedColumn[User, Long] = new MyAvgUDAF().toColumn
    // 将udaf 转换为列
    //    val ageAvg: Column = ageAvg

    ds.select(ageAvg).show


    spark.stop()
  }


  case class User(username: String, age: Long)

  /**
   * 样例类，构造参数的属性不能更改，var 可以修改
   *
   * @param total
   * @param count
   */
  case class Buff(var total: Long, var count: Long)

  /**
   * 泛型
   * -IN, 输入类型
   * BUF,
   * OUT 输出
   */
  class MyAvgUDAF extends Aggregator[User, Buff, Long] {
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
    override def reduce(buff: Buff, in: User): Buff = {
      buff.total = buff.total + in.age
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
     *
     * @return
     */
    override def bufferEncoder: Encoder[Buff] = Encoders.product

    /**
     * 输出的编码操作, 如果是Scala 存在的类，scalaLong
     *
     * @return
     */
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }
}
