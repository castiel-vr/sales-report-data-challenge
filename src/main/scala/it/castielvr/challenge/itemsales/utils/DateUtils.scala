package it.castielvr.challenge.itemsales.utils

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}

object DateUtils {

  lazy val yearMonthFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMM")

  def extractDateAsYMString(millis: Long): String = {
    val a = Instant.ofEpochMilli(millis).atZone(ZoneOffset.UTC).toLocalDate

    a.format(yearMonthFormatter)
  }
}
