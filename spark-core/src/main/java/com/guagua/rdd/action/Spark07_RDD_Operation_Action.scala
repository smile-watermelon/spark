package com.guagua.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Operation_Action {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // Caused by: java.io.NotSerializableException: com.guagua.rdd.action.Spark07_RDD_Operation_Action$User
    val user: User = new User()

    rdd.collect().foreach{
      num => {
        println("driver age=" + (user.age + num))
      }
    }

    /**
     * RDD算子中传递的函数中包含闭包操作，那么就会进行检测功能，叫做闭包检测功能
     * User 不进行序列化，报下面的错误，就是在闭包检测的时候抛出来的
     * ToDo at org.apache.spark.util.ClosureCleaner$.ensureSerializable(ClosureCleaner.scala:416)
     *
     * ensureSerializable()
     *    case ex: Exception => throw new SparkException("Task not serializable", ex)
     */
    rdd.foreach{
      num => {
        println("age= " + (user.age + num))
      }
    }

    sc.stop()

  }

//  class User extends Serializable {

  class User {

  /**
   * 样例类在编译时，会自动混入序列化特质（实现序列化接口）
   */
//  case class User() {
    var age: Int = 30
  }

}
