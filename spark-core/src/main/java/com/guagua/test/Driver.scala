package com.guagua.test

import java.io.ObjectOutputStream
import java.net.Socket

object Driver {

  def main(args: Array[String]): Unit = {
    val client1 = new Socket("localhost", 9999)
    val client2 = new Socket("localhost", 8888)

    val out1 = client1.getOutputStream

    val objOut1 = new ObjectOutputStream(out1)

    val dataStruct = new DataStruct()
    val  task = new Task()
    task.datas = dataStruct.datas.take(2)
    task.logic = dataStruct.logic

    objOut1.writeObject(task)
    objOut1.flush()

    objOut1.close()
    client1.close()
    println("发送[9999]数据完成...")

    val out2 = client2.getOutputStream

    val objOut2 = new ObjectOutputStream(out2)

    task.datas = dataStruct.datas.takeRight(2)
    task.logic = dataStruct.logic

    objOut2.writeObject(task)
    objOut2.flush()


    objOut2.close()
    client2.close()
    println("发送[8888]数据完成...")
  }
}
