package cn.adatafun.dataprocess.conf
import PropertiesGetter.loadProperties

/**
  * Created by yanggf on 2017/11/22.
  */
object TestField {

  def main(args: Array[String]): Unit ={
    val prop = loadProperties()
    val fieldName = "browseNum"
    val script = prop.getProperty("updateScript")
    prop.setProperty("fieldName", "browseNum")
    val fieldScript = script.replaceAll("fieldName",fieldName)
    println(script)
  }

}
