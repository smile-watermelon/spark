package com.guagua.test

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}

object Executor {


  def main(args: Array[String]): Unit = {
    val ss = new ServerSocket(9999)

    println("等待客户端连接...")

    val client: Socket = ss.accept()

    val in: InputStream = client.getInputStream

    val task: Task = new ObjectInputStream(in).readObject().asInstanceOf[Task]

    val ints: List[Int] = task.compute()

    println(ints)

    in.close()
    client.close()
    ss.close()

  }
}
