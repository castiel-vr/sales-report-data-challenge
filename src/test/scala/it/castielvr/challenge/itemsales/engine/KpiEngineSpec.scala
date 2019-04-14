package it.castielvr.challenge.itemsales.engine

import it.castielvr.challenge.itemsales.io.{KpiIoInterface, SalesIoInterface}
import it.castielvr.challenge.itemsales.model.InputSales
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest._
import Matchers._
import it.castielvr.challenge.itemsales.model.KpiSales

class KpiEngineSpec extends FlatSpec {

  private val sparkConf = new SparkConf()
    .setAppName("KpiEngineSpec")

  implicit val sparkSession: SparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .master("local[2]")
    .getOrCreate()

  import sparkSession.implicits._

  "The KpiEngine" should "generate new Kpi for new sales input" in {
    val fakeSalesIo = new SalesIoInterface {
      override def readInputSales(implicit sparkSession: SparkSession): Dataset[InputSales] = {
        val inputSales = Seq(
          InputSales("4e9iAk", "EUROPE", "CENTRAL EUROPE", "SWITZERLAND", "2157100", 742, 1513191152, "2018-01-01"),
          InputSales("4e9iAl", "EUROPE", "CENTRAL EUROPE", "SWITZERLAND", "2157100", 15, 1513191156, "2018-01-01")
        )
        sparkSession.sparkContext.parallelize(inputSales).toDS()
      }
    }
    var output = sparkSession.emptyDataset[KpiSales]

    val fakeKpiIo = new KpiIoInterface {
      override def readKpi(implicit sparkSession: SparkSession): Dataset[KpiSales] = {
        sparkSession.emptyDataset[KpiSales]
      }

      override def writeKpi(kpis: Dataset[KpiSales])(implicit sparkSession: SparkSession): Unit = {
        output = kpis
      }
    }

    KpiEngine.updateKpiWithNewSales("fake", "2018-01-01","2018-01-01")(fakeSalesIo, fakeKpiIo)

    val resultCollection = output.collect()

    assert(resultCollection.length == 5)

    resultCollection should contain(KpiSales("Y", "2017", 757, Map("EUROPE" -> 757), Map("CENTRAL EUROPE" -> 757), Map("SWITZERLAND" -> 757)))
    resultCollection should contain(KpiSales("S", "201702", 757, Map("EUROPE" -> 757), Map("CENTRAL EUROPE" -> 757), Map("SWITZERLAND" -> 757)))
    resultCollection should contain(KpiSales("Q", "201704", 757, Map("EUROPE" -> 757), Map("CENTRAL EUROPE" -> 757), Map("SWITZERLAND" -> 757)))
    resultCollection should contain(KpiSales("M", "201712", 757, Map("EUROPE" -> 757), Map("CENTRAL EUROPE" -> 757), Map("SWITZERLAND" -> 757)))
    resultCollection should contain(KpiSales("D", "20171213", 757, Map("EUROPE" -> 757), Map("CENTRAL EUROPE" -> 757), Map("SWITZERLAND" -> 757)))
  }

  it should "update old Kpi for new sales input" in {
    val fakeSalesIo = new SalesIoInterface {
      override def readInputSales(implicit sparkSession: SparkSession): Dataset[InputSales] = {
        val inputSales = Seq(
          InputSales("4e9iAk", "EUROPE", "CENTRAL EUROPE", "SWITZERLAND", "2157100", 742, 1513191152, "2018-01-01"),
          InputSales("4e9iAl", "EUROPE", "CENTRAL EUROPE", "SWITZERLAND", "2157100", 15, 1513191156, "2018-01-01")
        )
        sparkSession.sparkContext.parallelize(inputSales).toDS()
      }
    }
    var output = sparkSession.emptyDataset[KpiSales]

    val fakeKpiIo = new KpiIoInterface {
      override def readKpi(implicit sparkSession: SparkSession): Dataset[KpiSales] = {
        val kpi = Seq(
          KpiSales("Y", "2017", 100, Map("EUROPE" -> 100), Map("CENTRAL EUROPE" -> 100), Map("SWITZERLAND" -> 100))
        )
        sparkSession.sparkContext.parallelize(kpi).toDS()
      }

      override def writeKpi(kpis: Dataset[KpiSales])(implicit sparkSession: SparkSession): Unit = {
        output = kpis
      }
    }

    KpiEngine.updateKpiWithNewSales("fake", "2018-01-01","2018-01-01")(fakeSalesIo, fakeKpiIo)

    val resultCollection = output.collect()

    assert(resultCollection.length == 5)

    resultCollection should contain(KpiSales("Y", "2017", 857, Map("EUROPE" -> 857), Map("CENTRAL EUROPE" -> 857), Map("SWITZERLAND" -> 857)))
    resultCollection should contain(KpiSales("S", "201702", 757, Map("EUROPE" -> 757), Map("CENTRAL EUROPE" -> 757), Map("SWITZERLAND" -> 757)))
    resultCollection should contain(KpiSales("Q", "201704", 757, Map("EUROPE" -> 757), Map("CENTRAL EUROPE" -> 757), Map("SWITZERLAND" -> 757)))
    resultCollection should contain(KpiSales("M", "201712", 757, Map("EUROPE" -> 757), Map("CENTRAL EUROPE" -> 757), Map("SWITZERLAND" -> 757)))
    resultCollection should contain(KpiSales("D", "20171213", 757, Map("EUROPE" -> 757), Map("CENTRAL EUROPE" -> 757), Map("SWITZERLAND" -> 757)))
  }

  it should "leave unaltered old Kpi if not affected by new sales input" in {
    val fakeSalesIo = new SalesIoInterface {
      override def readInputSales(implicit sparkSession: SparkSession): Dataset[InputSales] = {
        val inputSales = Seq(
          InputSales("4e9iAk", "EUROPE", "CENTRAL EUROPE", "SWITZERLAND", "2157100", 742, 1513191152, "2018-01-01"),
          InputSales("4e9iAl", "EUROPE", "CENTRAL EUROPE", "SWITZERLAND", "2157100", 15, 1513191156, "2018-01-01")
        )
        sparkSession.sparkContext.parallelize(inputSales).toDS()
      }
    }
    var output = sparkSession.emptyDataset[KpiSales]

    val fakeKpiIo = new KpiIoInterface {
      override def readKpi(implicit sparkSession: SparkSession): Dataset[KpiSales] = {
        val kpi = Seq(
          KpiSales("Y", "2018", 100, Map("EUROPE" -> 100), Map("CENTRAL EUROPE" -> 100), Map("SWITZERLAND" -> 100))
        )
        sparkSession.sparkContext.parallelize(kpi).toDS()
      }

      override def writeKpi(kpis: Dataset[KpiSales])(implicit sparkSession: SparkSession): Unit = {
        output = kpis
      }
    }

    KpiEngine.updateKpiWithNewSales("fake", "2018-01-01", "2018-01-01")(fakeSalesIo, fakeKpiIo)

    val resultCollection = output.collect()

    assert(resultCollection.length == 6)

    resultCollection should contain(KpiSales("Y", "2018", 100, Map("EUROPE" -> 100), Map("CENTRAL EUROPE" -> 100), Map("SWITZERLAND" -> 100)))
    resultCollection should contain(KpiSales("Y", "2017", 757, Map("EUROPE" -> 757), Map("CENTRAL EUROPE" -> 757), Map("SWITZERLAND" -> 757)))
    resultCollection should contain(KpiSales("S", "201702", 757, Map("EUROPE" -> 757), Map("CENTRAL EUROPE" -> 757), Map("SWITZERLAND" -> 757)))
    resultCollection should contain(KpiSales("Q", "201704", 757, Map("EUROPE" -> 757), Map("CENTRAL EUROPE" -> 757), Map("SWITZERLAND" -> 757)))
    resultCollection should contain(KpiSales("M", "201712", 757, Map("EUROPE" -> 757), Map("CENTRAL EUROPE" -> 757), Map("SWITZERLAND" -> 757)))
    resultCollection should contain(KpiSales("D", "20171213", 757, Map("EUROPE" -> 757), Map("CENTRAL EUROPE" -> 757), Map("SWITZERLAND" -> 757)))

  }
}
