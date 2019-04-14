package it.castielvr.challenge.itemsales.io.impl

import it.castielvr.challenge.itemsales.config.PostgreSQLConfig
import it.castielvr.challenge.itemsales.io.KpiIoInterface
import it.castielvr.challenge.itemsales.io.dto.KpiSalesDTO
import it.castielvr.challenge.itemsales.model.KpiSales
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

class KpiIoImpl(postgresConfig: PostgreSQLConfig) extends KpiIoInterface {
  def readKpi(implicit sparkSession: SparkSession): Dataset[KpiSales] = {
    import sparkSession.implicits._

    val opts = Map(
      "url" -> postgresConfig.url,
      "dbtable" -> postgresConfig.dbtable,
      "driver" -> "org.postgresql.Driver",
      "user" -> postgresConfig.username,
      "password" -> postgresConfig.password
    )

    sparkSession.
      read.format("jdbc")
      .options(opts)
      .load
      .as[KpiSalesDTO]
      .map(k => KpiSales(k))
  }

  def writeKpi( kpis: Dataset[KpiSales])(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._

    val opts = Map(
      "url" -> postgresConfig.url,
      "dbtable" -> postgresConfig.dbtable,
      "driver" -> "org.postgresql.Driver",
      "user" -> postgresConfig.username,
      "password" -> postgresConfig.password
    )

    kpis
      .map(k => KpiSalesDTO(k))
      .write
      .mode(SaveMode.Overwrite)
      .format("jdbc")
      .options(opts)
      .save
  }
}
