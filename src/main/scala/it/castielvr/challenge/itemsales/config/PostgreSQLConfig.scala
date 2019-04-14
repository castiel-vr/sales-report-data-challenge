package it.castielvr.challenge.itemsales.config

import com.typesafe.config.Config

object PostgreSQLConfig{
  def apply(conf: Config): PostgreSQLConfig = {
    PostgreSQLConfig(
      conf.getString("url"),
      conf.getString("dbtable"),
      conf.getString("username"),
      conf.getString("password")
    )
  }
}

case class PostgreSQLConfig(
                         url: String,
                         dbtable: String,
                         username: String,
                         password: String
                         )
