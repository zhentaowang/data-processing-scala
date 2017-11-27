package cn.adatafun.dataprocess.accumulate

import cn.adatafun.dataprocess.conf.ESMysqlSpark
import cn.adatafun.dataprocess.banner.Banner.getBannerId
import cn.adatafun.dataprocess.banner.Banner.Banner
import cn.adatafun.dataprocess.util.TodayGet.isYestoday
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark._

/**
  * Created by yanggf on 2017/11/21.
  */
object BannerAcc {

  def accBrowseNum(): Unit = {
    val conf = ESMysqlSpark.getSparkAccConf()
    val prop = ESMysqlSpark.getSystemProp()
    val updateScript = prop.getProperty("updateScript").replaceAll("fieldName","browseNum")
    conf.set("es.update.script", updateScript)
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val propMysql = ESMysqlSpark.getMysqlConfLog()
    val tableName = "tbd_visit_url"
    val urlDS = sparkSession.read.jdbc(propMysql.getProperty("url"),
      tableName, propMysql)
    import sparkSession.implicits._

    //browseNum process
    urlDS.select(urlDS("user_id"), urlDS("url"), urlDS("create_time"))
      .filter(row => !getBannerId(row.getString(1)).equals("") &&
      isYestoday(row.getTimestamp(2).getTime))
      .map(row => ((row.getString(0), getBannerId(row.getString(1))), 1))
      .rdd
      .map(each => Banner(each._1._1 + each._1._2,
        each._1._1, each._1._2, each._2)) //take 200 foreach println
      .saveToEs("userbanner/userbanner")
    sparkSession.stop()
  }

  def main(args: Array[String]): Unit ={
    accBrowseNum()
  }
}
