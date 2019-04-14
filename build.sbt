name := "ItemsSales-SparkProcessing"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.0"
val scalatestVersion = "3.0.5"
val typesafeConfigVersion = "1.3.2"
val commonsCliVersion = "1.2"
val sprayJsonVersion = "1.3.5"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided" excludeAll( ExclusionRule(organization = "org.baz") )
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided" excludeAll( ExclusionRule(organization = "org.baz") )
libraryDependencies += "org.scalactic" %% "scalactic" % scalatestVersion
libraryDependencies += "org.scalatest" %% "scalatest" % scalatestVersion % "test"
libraryDependencies += "com.typesafe" % "config" % typesafeConfigVersion
libraryDependencies += "commons-cli" % "commons-cli" % commonsCliVersion
libraryDependencies += "io.spray" %%  "spray-json" % sprayJsonVersion

assemblyMergeStrategy in assembly := {
  case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
