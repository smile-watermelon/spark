package com.guagua.framework.application

import com.guagua.framework.common.TApplication
import com.guagua.framework.controller.WordCountController

object WordCountApplication extends App with TApplication {

  start() {

    val controller = new WordCountController
    controller.scheduler()

  }


}
