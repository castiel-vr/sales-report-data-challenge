package it.castielvr.challenge.itemsales.io

import it.castielvr.challenge.itemsales.model.KpiSales
import org.apache.spark.sql.{Dataset, SparkSession}

trait KpiIoInterface {
  def readKpi(implicit sparkSession: SparkSession): Dataset[KpiSales]

  def writeKpi( kpis: Dataset[KpiSales])(implicit sparkSession: SparkSession): Unit
}
