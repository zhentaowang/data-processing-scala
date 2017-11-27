package cn.adatafun.dataprocess.usertags

import cn.adatafun.dataprocess.banner.Banner._
import cn.adatafun.dataprocess.conf.ESMysqlSpark
import org.elasticsearch.spark._
import spray.json.JsonParser

object UserTagsOne {
  case class ArticleBrowseNum(id: String, userId: String, articleBrowseNum: Int)
  case class ArticleFavorNum(id: String, userId: String, articleFavorNum: Int)
  case class BannerBrowseNum(id: String, userId: String, bannerBrowseNum: Int)
  case class CarAverageOrderAmount(id: String, userId: String, carAverageOrderAmount: Double)
  case class CarBrowseNum(id: String, userId: String, carBrowseNum: Int)
  case class CarConsumptionNum(id: String, userId: String, carconsumptionNum: Int)

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

  def main(args: Array[String])={
    val sparkSession = ESMysqlSpark.getSparkSession()
    val propMysql = ESMysqlSpark.getMysqlConfLog()
    val tableName = "tbd_visit_url"
    val urlDS = sparkSession.read.jdbc(propMysql.getProperty("url"),
      tableName, propMysql)
    import sparkSession.implicits._
    //articleBrowseNum
    urlDS.select(urlDS("user_id"),urlDS("url"),urlDS("create_time"))
      .filter(row => row.getString(1).startsWith("https://m.dragonpass.com.cn/institute/") &&
        !row.isNullAt(0))// && row.getTimestamp(2).getTime() > yestoday)
      .map(row => (row.getString(0), 1))
      .rdd.reduceByKey((x, y) => x+y)
      .map(each => ArticleBrowseNum(each._1, each._1, each._2))
      .saveToEs("usertags/usertags")

    val propMysqlBussiness = ESMysqlSpark.getMysqlConfBusiness()
    val favorTable = "tbd_lettres_favor"
    val favorDS = sparkSession.read.jdbc(propMysqlBussiness.getProperty("url"),
      favorTable, propMysqlBussiness)
    //articleFavorNum
    favorDS.select(favorDS("user_id"), favorDS("lettres_id"), favorDS("create_time"))
      .filter(row => !row.isNullAt(0) && !row.isNullAt(1))
      .map(row => (row.getString(0), 1))
      .rdd.reduceByKey((x, y) => x+y)
      .map(each => ArticleFavorNum(each._1, each._1, each._2)).saveToEs("usertags/usertags")

    val bannerDS = sparkSession.read.jdbc(propMysql.getProperty("url"),
      "tbd_visit_url", propMysql)
    //bannerBrowseNum
    bannerDS.select(bannerDS("user_id"), bannerDS("url"))
      .filter(row => !row.isNullAt(0) && getBannerId(row.getString(1)).equals(""))
      .map(row => (row.getString(0), 1))
      .rdd.reduceByKey((x, y) => x+y)
      .map(each => BannerBrowseNum(each._1,
        each._1, each._2)).saveToEs("usertags/usertags")

    //carAverageOrderAmount
    val carOrderDS = sparkSession.read.jdbc(propMysqlBussiness.getProperty("url"),
    "tb_order_limousine", propMysqlBussiness)
    val tbOrderDS = sparkSession.read.jdbc(propMysqlBussiness.getProperty("url"),
    "tb_order", propMysqlBussiness)
    val carTbOrderDS = carOrderDS.join(tbOrderDS, carOrderDS("order_no")
      .equalTo(tbOrderDS("order_no")), "left_outer")
    carTbOrderDS.select(carTbOrderDS("user_id"), carTbOrderDS("actual_amount"))
      .na.drop().map(row => (row.getAs(0).toString, (row.getDouble(1), 1)))
      .rdd.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .filter(each => each._2._2 != 0)
      .map(each => CarAverageOrderAmount(each._1, each._1, each._2._1/each._2._2))
      .saveToEs("usertags/usertags")

    //carBroseNum
    val propMysqlLog = ESMysqlSpark.getMysqlConfLog()
    val carDS = sparkSession.read.jdbc(propMysqlLog.getProperty("url"),
    "tbd_url_element", propMysqlLog)
    carDS.filter("url = '/VirtualCard-v6/limousine/carTypeList' or " +
      "url = '/VirtualCard-v6/trafficsite/listHot/limousine' "
      + "or url = '/VirtualCard-v6/trafficsite/list/limousine'")
      .select(carDS("param"))
      .map(row => parseJson(row.getString(0)))
      .filter(each => !each.equals("")).map(x => (x, 1))
      .rdd.reduceByKey((x,y) => x+y)
      .map(each => CarBrowseNum(each._1, each._1, each._2))
      .saveToEs("usertags/usertags")

    //carConsumptionNum
    carTbOrderDS.select(carTbOrderDS("user_id"), carTbOrderDS("actual_amount"))
      .na.drop().map(row => (row.getAs(0).toString, 1))
      .rdd.reduceByKey((x, y) => x+y)
      .filter(each => each._2 != 0)
      .map(each => CarConsumptionNum(each._1, each._1, each._2))
      .saveToEs("usertags/usertags")
  }

}
