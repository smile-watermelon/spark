package com.guagua.framework.controller

import com.guagua.framework.common.TController
import com.guagua.framework.service.WordCountService

class WordCountController extends TController{

  private val wordCountService: WordCountService = new WordCountService
  override def scheduler(): Any = {
      wordCountService.analysis()
  }
}
