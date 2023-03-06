package com.kakao.adrec.atom.metric
package common.utils

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class LogicalDateTime(executionDateTimeStr: String) {
  private val datetimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  private val dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val hourFormat = DateTimeFormatter.ofPattern("HH")
  private val executionLocalDateTime: LocalDateTime = LocalDateTime.parse(executionDateTimeStr, datetimeFormat)

  def getDate(): String = {
    executionLocalDateTime.format(dateFormat)
  }

  def getHour():String = {
    executionLocalDateTime.format(hourFormat)
  }

  def getMinusDayDateTime(diff: Int): String = {
    executionLocalDateTime.minusDays(diff).format(datetimeFormat)
  }

  def getMinusHourDate(diff: Int): String = {
    executionLocalDateTime.minusHours(diff).format(dateFormat)
  }

  def getMinusHour(diff: Int): String = {
    executionLocalDateTime.minusHours(diff).format(hourFormat)
  }

}
