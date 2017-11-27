package cn.adatafun.dataprocess.shop

import cn.adatafun.dataprocess.conf.ESMysqlSpark
import cn.adatafun.dataprocess.rest.Rest.{BrowseNum, CollectionNum}
import spray.json.JsonParser
import org.elasticsearch.spark._

object Shop {
  def parseJson(jsonStr: String): (String, String) = {
    val json = JsonParser(jsonStr).asJsObject
    try{
      (json.getFields("userId")(0).toString().replace("\"",""),
        json.getFields("code")(0).toString().replace("\"",""))
    } catch {
      case ex: java.lang.IndexOutOfBoundsException =>{
        ("","")
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
      .filter(each => !each._1.equals("") && !each._2.equals(""))
      .map(x => (x, 1))
      .rdd.reduceByKey((x, y) => x+y).map(each => BrowseNum(
      each._1._1 + each._1._2, each._1._1, each._1._2, each._2
    )).saveToEs("usershop/usershop")

    //collectionNum Process
    val propMysqlBusiness = ESMysqlSpark.getMysqlConfBusiness()
    val collectDS = sparkSession.read.jdbc(propMysqlBusiness.getProperty("url"),
    "tb_user_collect", propMysqlBusiness)
    collectDS.map(row => ((row.getAs(1).toString, row.getString(3)), 1))
      .filter(row => row._1._2.startsWith("F"))
      .rdd.reduceByKey((x, y) => x + y)
      .map(each => CollectionNum(each._1._1 + each._1._2, each._1._1,
        each._1._2, each._2)).saveToEs("usershop/usershop")
  }

}
