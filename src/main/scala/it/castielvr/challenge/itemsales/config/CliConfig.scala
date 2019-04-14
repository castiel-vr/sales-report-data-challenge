package it.castielvr.challenge.itemsales.config

import org.apache.commons.cli.{BasicParser, Options}

object CliConfig {

  def parseArgs(args: Array[String]): CliConfig = {
    val options = new Options()
    options.addOption("c", "config-path", true, "display current time")
    options.addOption("s", "start-partition", true, "display current time")
    options.addOption("e", "end-partition", true, "display current time")
    val parser = new BasicParser
    val cmd = parser.parse(options, args)

    CliConfig(
      cmd.getOptionValue("c"),
      cmd.getOptionValue("s"),
      cmd.getOptionValue("e")
    )
  }
}

case class CliConfig(
                    appConfigPath: String,
                    startPartition: String,
                    endPartition: String
                    )
