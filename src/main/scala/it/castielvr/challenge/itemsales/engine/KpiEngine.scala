package it.castielvr.challenge.itemsales.engine

import it.castielvr.challenge.itemsales.io.{KpiIoInterface, SalesIoInterface}
import it.castielvr.challenge.itemsales.model.{InputSales, KpiSales}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel

object KpiEngine {

  def updateKpiWithNewSales(inputSalesHdfsRootPath: String, startPartition: String, endPartition: String)
                           (salesIo: SalesIoInterface, kpiIo: KpiIoInterface)
                           (implicit sparkSession: SparkSession): Unit = {

    /* Read input data
        - Sales data to be processed
        - Kpi to be updated
     */
    val newInputSales: Dataset[InputSales] = salesIo.readInputSales
      .filter( is => is.arrivalDay >= startPartition && is.arrivalDay <= endPartition)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val kpiStatus = kpiIo.readKpi
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

     // This count solve a bug on jdbc read that cause it to be empty without. (I suppose it's a bug, to be explored)
     kpiStatus.count
     /*
      Process new Kpi values
     */

    val updatedKpi = KpiEngine.updateKpi(kpiStatus, newInputSales)
    kpiIo.writeKpi(updatedKpi)
  }

  protected def updateKpi(currentKpi: Dataset[KpiSales], salesUpdate: Dataset[InputSales])
                       (implicit sparkSession: SparkSession): Dataset[KpiSales] = {
    import sparkSession.implicits._

    // Extract period to update KPI
    val kpiUpdate: Dataset[KpiSales] = salesUpdate.flatMap {
      salesUpd =>
        val periodKeys = salesUpd.extractPeriodKeys
        periodKeys.map {
          pk =>
            val sum = salesUpd.quantity
            KpiSales(pk._1, pk._2, sum, Map(salesUpd.region -> sum), Map(salesUpd.area -> sum), Map(salesUpd.market -> sum))
        }
    }

    currentKpi
      .union(kpiUpdate)
      .groupByKey(ck => (ck.periodType, ck.periodId))
      .reduceGroups(_ + _)
      .map(_._2)
  }
}
