import AssemblyKeys._
assemblySettings

jarName in assembly := "dataProcess20171124.jar"
test in assembly := {}
mainClass in assembly := Some( "cn.adatafun.dataprocess.accumulate.BannerAcc")

assemblyOption in packageDependency ~= { _.copy(appendContentHash = true) }

excludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  cp.filter { each =>
    each.data.getName == "avro-ipc-1.7.7-tests.jar" ||
    each.data.getName == "parquet-format-2.3.0-incubating.jar" ||
    each.data.getName == "aopalliance-repackaged-2.4.0-b34.jar" ||
    each.data.getName == "commons-beanutils-1.7.0.jar" ||
    each.data.getName == "commons-beanutils-core-1.8.0.jar" ||
    each.data.getName == "hadoop-yarn-api-2.2.0.jar" ||
    each.data.getName == "javax.inject-2.4.0-b34.jar"}
}

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
{
  case PathList("org", "apache", xs @ _*) => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "axiom.xml" => MergeStrategy.filterDistinctLines
  case PathList(ps @ _*) if ps.last endsWith "Log$Logger.class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "ILoggerFactory.class" => MergeStrategy.first
  case x => old(x)
}
}