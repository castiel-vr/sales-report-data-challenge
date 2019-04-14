package it.castielvr.challenge.itemsales.io.impl

import it.castielvr.challenge.itemsales.io.SalesIoInterface
import it.castielvr.challenge.itemsales.model.InputSales
import org.apache.spark.sql.{Dataset, SparkSession}

class SalesIoImpl(hdfsRootPath: String) extends SalesIoInterface {
  def readInputSales(implicit sparkSession: SparkSession): Dataset[InputSales] = {

    import sparkSession.implicits._

    sparkSession
      .read
      .option("multiline", "true")
      .json(hdfsRootPath).as[InputSales]
  }
}
