package cn.adatafun.dataprocess.conf

import java.io.FileInputStream
import java.util.Properties

object PropertiesGetter {
  def loadProperties(): Properties = {
    val propertiese = new Properties()
    val path = this.getClass.getClassLoader.getResourceAsStream("resource.properties")
    propertiese.load(path)
//      Thread.currentThread().getContextClassLoader.getResource("resource.properties")
//      .getPath
    propertiese
  }
  def main(args: Array[String])={
    val prop = loadProperties()
    println(prop.getProperty("hello"))
  }
}
