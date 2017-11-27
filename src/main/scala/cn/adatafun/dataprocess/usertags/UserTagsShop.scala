package cn.adatafun.dataprocess.usertags

import cn.adatafun.dataprocess.conf.ESMysqlSpark
import spray.json.JsonParser
import org.elasticsearch.spark._

/**
  * Created by yanggf on 2017/11/17.
  */
object UserTagsShop {
  case class ShopBrowseNum(id: String, userId: String,
                           shopBrowseNum: Int)
  case class ShopCollectionNum(id: String, userId: String,
                               shopCollectionNum: Int)
  def parseJson(jsonStr: String): String = {
    val json = JsonParser(jsonStr).asJsObject
    try{
      json.getFields("userId")(0).toString().replace("\"","")
    } catch {
      case ex: java.lang.IndexOutOfBoundsException =>{
        ""
      }
    }
  }
  def main(args: Array[String]): Unit = {
    val sparkSession = ESMysqlSpark.getSparkSession()
    val propMysqlLog = ESMysqlSpark.getMysqlConfLog()
    val browseDS = sparkSession.read.jdbc(propMysqlLog.getProperty("url"),
      "tbd_url_element", propMysqlLog)

    import sparkSession.implicits._
    //browseNum Process
    browseDS.filter("url = '/VirtualCard-v5/shop/detail' or " +
      "url = '/VirtualCard-v5/shop/getNearbyShare'")
      .select(browseDS("param"))
      .map(row => parseJson(row.getString(0)))
      .filter(each => !each.equals("") )
      .map(x => (x, 1))
      .rdd.reduceByKey((x, y) => x+y).map(each => ShopBrowseNum(
      each._1, each._1, each._2
    )).saveToEs("usertags/usertags")

    //collectionNum Process
    val propMysqlBusiness = ESMysqlSpark.getMysqlConfBusiness()
    val collectDS = sparkSession.read.jdbc(propMysqlBusiness.getProperty("url"),
      "tb_user_collect", propMysqlBusiness)
    collectDS.map(row => ((row.getAs(1).toString, row.getString(3)), 1))
      .filter(row => row._1._2.startsWith("F"))
      .rdd.map(each => (each._1._1, 1)).reduceByKey((x, y) => x + y)
      .map(each => ShopCollectionNum(each._1, each._1,
        each._2)).saveToEs("usertags/usertags")
  }
}
