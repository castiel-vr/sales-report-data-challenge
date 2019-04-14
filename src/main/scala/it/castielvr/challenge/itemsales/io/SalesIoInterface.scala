package it.castielvr.challenge.itemsales.io

import it.castielvr.challenge.itemsales.model.InputSales
import org.apache.spark.sql.{Dataset, SparkSession}

trait SalesIoInterface {
  def readInputSales(implicit sparkSession: SparkSession): Dataset[InputSales]
}
