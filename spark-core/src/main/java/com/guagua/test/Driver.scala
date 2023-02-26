package com.guagua.test

import java.io.ObjectOutputStream
import java.net.Socket

object Driver {

  def main(args: Array[String]): Unit = {
    val client = new Socket("localhost", 9999)

    val out = client.getOutputStream

    val objOut = new ObjectOutputStream(out)

    val task = new Task()
    objOut.writeObject(task)

    objOut.flush()


    objOut.close()
    client.close()

    println("发送数据完成...")
  }
}
