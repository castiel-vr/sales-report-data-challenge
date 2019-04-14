package it.castielvr.challenge.itemsales.model

import it.castielvr.challenge.itemsales.io.dto.KpiSalesDTO
import it.castielvr.challenge.itemsales.utils.CollectionUtils
import spray.json.DefaultJsonProtocol._
import spray.json._

object KpiSales {
  def apply(kpiDto: KpiSalesDTO): KpiSales = {
    KpiSales(
      kpiDto.period_type,
      kpiDto.period_id,
      kpiDto.sales_sum,
      fromJsonArrayStringToMap(kpiDto.sales_by_region),
      fromJsonArrayStringToMap(kpiDto.sales_by_area),
      fromJsonArrayStringToMap(kpiDto.sales_by_market)
    )
  }

  sealed case class GeoSales(geoId: String, totalSales: Long)

  def fromJsonArrayStringToMap(str: String): Map[String, Long] = {
    implicit val geoSalesFormat: RootJsonFormat[GeoSales] = jsonFormat2(GeoSales)

    val geoSales: Seq[GeoSales] = str.parseJson.convertTo[List[GeoSales]]

    geoSales.map(
      gs =>
        gs.geoId -> gs.totalSales
    ).toMap
  }
}

case class KpiSales(
                     periodType: String,
                     periodId: String,
                     salesSum: Long,
                     salesByRegion: Map[String, Long],
                     salesByArea: Map[String, Long],
                     salesByMarket: Map[String, Long]
                   ) {
  def +(other: KpiSales): KpiSales = {
    require(periodType == other.periodType && periodId == other.periodId)
    KpiSales(
      periodType,
      periodId,
      salesSum + other.salesSum,
      CollectionUtils.mergeMaps(salesByRegion, other.salesByRegion, (a: Long, b: Long) => a + b),
      CollectionUtils.mergeMaps(salesByArea, other.salesByArea, (a: Long, b: Long) => a + b),
      CollectionUtils.mergeMaps(salesByMarket, other.salesByMarket, (a: Long, b: Long) => a + b)
    )
  }
}
