package it.castielvr.challenge.itemsales.model

import java.util.Date
import java.util.Calendar

case class InputSales(
                      event_id: String,
                      region: String,
                      area: String,
                      market: String,
                      ipcode: String,
                      quantity: Long,
                      timestamp: Long,
                      arrivalDay: String
                     ){

  def timestampMillis: Long = timestamp*1000

  def extractPeriodKeys: Seq[(String, String)] = {

    val cal = Calendar.getInstance
    cal.setTime(new Date(timestampMillis))
    // NOTE: Month at this point is in [0,11]
    val month: Int = cal.get(Calendar.MONTH)
    val day: Int = cal.get(Calendar.DAY_OF_MONTH)
    val year: Int = cal.get(Calendar.YEAR)
    Seq(
      "Y" -> year.toString,
      "S" -> s"${year.toString}${"%02d".format((month/6) +1)}",
      "Q" -> s"${year.toString}${"%02d".format((month/3) +1)}",
      "M" -> s"${year.toString}${"%02d".format(month + 1)}",
      "D" -> s"${year.toString}${"%02d".format(month + 1)}${"%02d".format(day)}"
    )
  }
}
