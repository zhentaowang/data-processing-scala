package cn.adatafun.dataprocess.accumulate

import cn.adatafun.dataprocess.article.Article.ArticleFavorNum
import cn.adatafun.dataprocess.conf.ESMysqlSpark
import org.apache.spark.{SparkContext, sql}
import cn.adatafun.dataprocess.util.TodayGet.isYestoday
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark._

/**
  * Created by yanggf on 2017/11/20.
  */
object ArticleAcc {
  case class Article(id: String, userId: String,
                     restaurantCode: String, browseNum: Int)

  def accFavorNum(): Unit = {
    //articlefavorNum accumulative
    val conf = ESMysqlSpark.getSparkAccConf()
    val propMysqlBussiness = ESMysqlSpark.getMysqlConfBusiness()
    val prop = ESMysqlSpark.getSystemProp()
    val favorTable = "tbd_lettres_favor"
    val favorNumScript = prop.getProperty("updateScript").replaceAll("fieldName", "favorNum")
    conf.set("es.update.script", favorNumScript)
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val favorDS = sparkSession.read.jdbc(propMysqlBussiness.getProperty("url"),
      favorTable, propMysqlBussiness)

    import sparkSession.implicits._

    favorDS.select(favorDS("user_id"), favorDS("lettres_id"), favorDS("create_time"))
      .filter(row => !row.isNullAt(0) && !row.isNullAt(1)
        && isYestoday(row.getTimestamp(2).getTime()))
      .map(row => ((row.getString(0), row.getString(1)), 1))
      .rdd
      .map(each => ArticleFavorNum(each._1._1 + each._1._2,
        each._1._1, each._1._2, each._2)) //take 200 foreach println
      .saveToEs("userarticle/userarticle")
    sparkSession.stop()
  }

  def accBrowseNum(): Unit = {
    val prop = ESMysqlSpark.getSystemProp()
    val browseNumScript = prop.getProperty("updateScript").replaceAll("fieldName", "browseNum")
    val conf = ESMysqlSpark.getSparkAccConf()
    conf.set("es.update.script", browseNumScript)
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    import sparkSession.implicits._

    val propMysql = ESMysqlSpark.getMysqlConfLog()
    val tableName = "tbd_visit_url"
    val urlDS = sparkSession.read.jdbc(propMysql.getProperty("url"),
      tableName, propMysql)

    //browseNum accumulative process
    urlDS.select(urlDS("user_id"),urlDS("url"),urlDS("create_time"))
      .filter(row => row.getString(1).startsWith("https://m.dragonpass.com.cn/institute/") &&
        !row.isNullAt(0) && isYestoday(row.getTimestamp(2).getTime()) )
      .map(row => ((row.getString(0),row.getString(1).split("/")(4).split("\\?")(0)), 1))
      .map(each => Article(each._1._1 + each._1._2, each._1._1,
        each._1._2, each._2))
      .rdd //take 200 foreach println
      .saveToEs("userarticle/userarticle")
    sparkSession.stop()
  }

  def main(args: Array[String]): Unit = {
    accBrowseNum()
    accFavorNum()
  }
}
