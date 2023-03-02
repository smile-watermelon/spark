package com.guagua.framework.service

import com.guagua.framework.common.TService
import com.guagua.framework.dao.WordCountDao
import org.apache.spark.rdd.RDD

class WordCountService extends TService {

  private val wordCountDao: WordCountDao = new WordCountDao
  override def analysis(): Any = {

    val lines: RDD[String] = wordCountDao.readFile("data/words.txt")

    val words: RDD[String] = lines.flatMap(_.split(" "))

    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)

    val wordCount: RDD[(String, Int)] = wordGroup.map {
      case (word, list) => (word, list.size)
    }

    val array: Array[(String, Int)] = wordCount.collect()

    array.foreach(println)

  }
}
