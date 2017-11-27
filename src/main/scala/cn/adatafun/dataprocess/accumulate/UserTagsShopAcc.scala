package cn.adatafun.dataprocess.accumulate

import cn.adatafun.dataprocess.conf.ESMysqlSpark
import cn.adatafun.dataprocess.usertags.UserTagsShop.{ShopBrowseNum, ShopCollectionNum, parseJson}
import org.apache.spark.sql.SparkSession
import cn.adatafun.dataprocess.util.TodayGet.isYestoday
import org.elasticsearch.spark._

/**
  * Created by yanggf on 2017/11/23.
  */
object UserTagsShopAcc {
  def accShopBrowseNum(): Unit ={
    val conf = ESMysqlSpark.getSparkAccConf()
    val prop = ESMysqlSpark.getSystemProp()
    val updateScript = prop.getProperty("updateScript").replaceAll("fieldName", "shopBrowseNum")
    conf.set("es.update.script", updateScript)
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val propMysqlLog = ESMysqlSpark.getMysqlConfLog()
    val browseDS = sparkSession.read.jdbc(propMysqlLog.getProperty("url"),
      "tbd_url_element", propMysqlLog)
    import sparkSession.implicits._
    //browseNum Process
    browseDS.filter("url = '/VirtualCard-v5/shop/detail' or " +
      "url = '/VirtualCard-v5/shop/getNearbyShare'")
      .select(browseDS("param"), browseDS("create_time"))
        .filter(row => isYestoday(row.getTimestamp(1).getTime))
      .map(row => parseJson(row.getString(0)))
      .filter(each => !each.equals("") )
      .map(x => (x, 1))
      .rdd.map(each => ShopBrowseNum(
      each._1, each._1, each._2
    ))
      .saveToEs("usertags/usertags")
  }

  def accShopCollectionNum(): Unit ={
    val conf = ESMysqlSpark.getSparkAccConf()
    val prop = ESMysqlSpark.getSystemProp()
    val updateScript = prop.getProperty("updateScript").replaceAll("fieldName", "shopCollectionNum")
    conf.set("es.update.script", updateScript)
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    import sparkSession.implicits._
    //collectionNum Process
    val propMysqlBusiness = ESMysqlSpark.getMysqlConfBusiness()
    val collectDS = sparkSession.read.jdbc(propMysqlBusiness.getProperty("url"),
      "tb_user_collect", propMysqlBusiness)
    collectDS.filter(row => isYestoday(row.getTimestamp(4).getTime))
      .map(row => ((row.getAs(1).toString, row.getString(3)), 1))
      .filter(row => row._1._2.startsWith("F"))
      .rdd.map(each => (each._1._1, 1))
      .map(each => ShopCollectionNum(each._1, each._1,
        each._2)) //take 200 foreach println
      .saveToEs("usertags/usertags")
  }

  def main(args: Array[String]): Unit ={
    accShopBrowseNum()
    accShopCollectionNum()
  }

}
