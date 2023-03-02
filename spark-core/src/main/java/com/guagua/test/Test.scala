package com.guagua.test

object Test {

  def main(args: Array[String]): Unit = {
    val ids: List[Long] = List[Long](1, 2, 3, 4, 5, 6, 7)
    val tail: List[Long] = ids.tail
//    tail.foreach(println)
    val tuples: List[(Long, Long)] = ids.zip(tail)
    val init: List[Long] = ids.init

    tuples.foreach(println)
    init.foreach(println)
  }

}
