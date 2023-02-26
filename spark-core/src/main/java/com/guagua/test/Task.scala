package com.guagua.test

class Task extends Serializable {

  var datas: List[Int] = _

  var logic: (Int) => Int = _

  def compute(): List[Int] = {
    datas.map(logic)
  }


}
