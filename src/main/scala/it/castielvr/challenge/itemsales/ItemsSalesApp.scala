package it.castielvr.challenge.itemsales

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import it.castielvr.challenge.itemsales.config.CliConfig
import it.castielvr.challenge.itemsales.config.PostgreSQLConfig
import it.castielvr.challenge.itemsales.engine.KpiEngine
import it.castielvr.challenge.itemsales.io.impl.{KpiIoImpl, SalesIoImpl}
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object ItemsSalesApp {

  def main(args: Array[String]): Unit = {

    /*
      Retrieve application parameters
        - Root path for new data
        - Partition to be processed
        - Connection info to retrieve/write KPI
     */
    val cliConfig = CliConfig.parseArgs(args)

    val configFileLocation: String = cliConfig.appConfigPath
    val startPartitionToBeProcessed: String = cliConfig.startPartition
    val endPartitionToBeProcessed: String = cliConfig.endPartition

    val configFile = new File(configFileLocation)
    val conf: Config = ConfigFactory.parseFile(configFile)

    val inputSalesHdfsRootPath: String = conf.getString("hdfs.new-sales-path")
    val postgresConfig: PostgreSQLConfig = PostgreSQLConfig(conf.getConfig("postgreSQL"))

    val sparkConf = new SparkConf()
      .setAppName("ItemsSalesApp")

    if(sparkConf.getOption("master").isEmpty){
      sparkConf.setMaster("local[4]")
    }

    implicit val sparkSession: SparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    KpiEngine.updateKpiWithNewSales(inputSalesHdfsRootPath, startPartitionToBeProcessed, endPartitionToBeProcessed)(new SalesIoImpl(inputSalesHdfsRootPath), new KpiIoImpl(postgresConfig))

    sparkSession.stop()
  }
}
