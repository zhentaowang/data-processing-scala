package cn.adatafun.dataprocess.article

import cn.adatafun.dataprocess.conf.ESMysqlSpark
import org.elasticsearch.spark._

object Article {
  case class ArticleBrowseNum(id:String, userId:String, restaurantCode:String, browseNum:Int)
  case class ArticleFavorNum(id:String, userId:String, restaurantCode:String, favorNum:Int)
  def main(args: Array[String])={
    val sparkSession = ESMysqlSpark.getSparkSession()
    val propMysql = ESMysqlSpark.getMysqlConfLog()
    val tableName = "tbd_visit_url"
    val urlDS = sparkSession.read.jdbc(propMysql.getProperty("url"),
      tableName, propMysql)
    import sparkSession.implicits._
    //browseNum
    urlDS.select(urlDS("user_id"),urlDS("url"),urlDS("create_time"))
      .filter(row => row.getString(1).startsWith("https://m.dragonpass.com.cn/institute/") &&
        !row.isNullAt(0))// && row.getTimestamp(2).getTime() > yestoday)
      .map(row => ((row.getString(0),row.getString(1).split("/")(4).split("\\?")(0)), 1))
      .rdd.reduceByKey((x, y) => x+y)
      .map(each => ArticleBrowseNum(each._1._1 + each._1._2, each._1._1,
        each._1._2, each._2)).saveToEs("userarticle/userarticle")

    //articlefavorNum
    val propMysqlBussiness = ESMysqlSpark.getMysqlConfBusiness()
    val favorTable = "tbd_lettres_favor"
    val favorDS = sparkSession.read.jdbc(propMysqlBussiness.getProperty("url"),
      favorTable, propMysqlBussiness)
    favorDS.select(favorDS("user_id"), favorDS("lettres_id"), favorDS("create_time"))
      .filter(row => !row.isNullAt(0) && !row.isNullAt(1))
      //&& row.getTimestamp(2).getTime() > yestoday)
      .map(row => ((row.getString(0), row.getString(1)), 1))
      .rdd.reduceByKey((x, y) => x+y)
      .map(each => ArticleFavorNum(each._1._1 + each._1._2,
        each._1._1, each._1._2, each._2)).saveToEs("userarticle/userarticle")
  }

}
