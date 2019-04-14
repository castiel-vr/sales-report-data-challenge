package it.castielvr.challenge.itemsales.io.dto

import spray.json._
import DefaultJsonProtocol._
import it.castielvr.challenge.itemsales.model.KpiSales
import it.castielvr.challenge.itemsales.utils.CollectionUtils

import scala.collection.immutable

object KpiSalesDTO {
  def apply(kpiSales: KpiSales): KpiSalesDTO = {
    KpiSalesDTO(
      kpiSales.periodType,
      kpiSales.periodId,
      kpiSales.salesSum,
      fromMapToJsonArrayString(kpiSales.salesByRegion),
      fromMapToJsonArrayString(kpiSales.salesByArea),
      fromMapToJsonArrayString(kpiSales.salesByMarket)
    )
  }

  sealed case class GeoSales(geoId: String, totalSales: Long)

  def fromMapToJsonArrayString(map: Map[String, Long]): String = {
    implicit val geoSalesFormat: RootJsonFormat[GeoSales] = jsonFormat2(GeoSales)

    val tempList = map.map {
      case (key, value) =>
        GeoSales(
          key,
          value
        )
    }.toList.sortBy(_.totalSales).reverse

    tempList.toJson.toString
  }
}

case class KpiSalesDTO(
                        period_type: String,
                        period_id: String,
                        sales_sum: Long,
                        sales_by_region: String,
                        sales_by_area: String,
                        sales_by_market: String
                      )
