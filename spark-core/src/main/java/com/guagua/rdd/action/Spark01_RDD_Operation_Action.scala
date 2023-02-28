package com.guagua.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operation_Action {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // 触发计算任务
    /**
     * sc.runJob()
     * dagScheduler.runJob(rdd, cleanedFunc, partitions, callSite, resultHandler, localProperties.get)
     *  val waiter = submitJob(rdd, func, partitions, callSite, resultHandler, properties)
     *    dagScheduler.handleJobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties)
     *      val job = new ActiveJob(jobId, finalStage, callSite, listener, properties)
     */
    rdd.collect()




    sc.stop()

  }

}
