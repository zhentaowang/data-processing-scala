package cn.adatafun.dataprocess.accumulate

import cn.adatafun.dataprocess.banner.Banner._
import cn.adatafun.dataprocess.conf.ESMysqlSpark
import cn.adatafun.dataprocess.usertags.UserTagsOne._
import org.apache.spark.sql.SparkSession
import cn.adatafun.dataprocess.util.TodayGet.isYestoday
import org.elasticsearch.spark._

/**
  * Created by yanggf on 2017/11/23.
  */
object UserTagsOneAcc {

  def accArticleBrowseNum(): Unit ={
    val conf = ESMysqlSpark.getSparkAccConf()
    val prop = ESMysqlSpark.getSystemProp()
    val updateScript = prop.getProperty("updateScript").replaceAll("fieldName", "articleBrowseNum")
    conf.set("es.update.script", updateScript)
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val propMysql = ESMysqlSpark.getMysqlConfLog()
    val tableName = "tbd_visit_url"
    val urlDS = sparkSession.read.jdbc(propMysql.getProperty("url"),
      tableName, propMysql)
    import sparkSession.implicits._
    //articleBrowseNum
    urlDS.select(urlDS("user_id"),urlDS("url"),urlDS("create_time"))
      .filter(row => row.getString(1).startsWith("https://m.dragonpass.com.cn/institute/") &&
        !row.isNullAt(0) && isYestoday(row.getTimestamp(2).getTime()))
      .map(row => (row.getString(0), 1))
      .rdd
      .map(each => ArticleBrowseNum(each._1, each._1, each._2))
      .saveToEs("usertags/usertags")
    sparkSession.stop()
  }

  def accArticleFavorNum(): Unit ={
    val conf = ESMysqlSpark.getSparkAccConf()
    val prop = ESMysqlSpark.getSystemProp()
    val updateScript = prop.getProperty("updateScript").replaceAll("fieldName", "articleFavorNum")
    conf.set("es.update.script", updateScript)
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val propMysqlBussiness = ESMysqlSpark.getMysqlConfBusiness()
    val favorTable = "tbd_lettres_favor"
    val favorDS = sparkSession.read.jdbc(propMysqlBussiness.getProperty("url"),
      favorTable, propMysqlBussiness)
    import sparkSession.implicits._
    //articleFavorNum
    favorDS.select(favorDS("user_id"), favorDS("lettres_id"), favorDS("create_time"))
      .filter(row => !row.isNullAt(0) && !row.isNullAt(1)
        && isYestoday(row.getTimestamp(2).getTime()))
      .map(row => (row.getString(0), 1))
      .rdd
      .map(each => ArticleFavorNum(each._1, each._1, each._2))
      .saveToEs("usertags/usertags")
    sparkSession.stop()
  }

  def accBannerBrowseNum(): Unit ={
    val conf = ESMysqlSpark.getSparkAccConf()
    val prop = ESMysqlSpark.getSystemProp()
    val updateScript = prop.getProperty("updateScript").replaceAll("fieldName", "bannerBrowseNum")
    conf.set("es.update.script", updateScript)
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val propMysqlLog = ESMysqlSpark.getMysqlConfLog()
    val bannerDS = sparkSession.read.jdbc(propMysqlLog.getProperty("url"),
      "tbd_visit_url", propMysqlLog)
    import sparkSession.implicits._
    //bannerBrowseNum
    bannerDS.select(bannerDS("user_id"), bannerDS("url"), bannerDS("create_time"))
      .filter(row => !row.isNullAt(0) && getBannerId(row.getString(1)).equals("")
        && isYestoday(row.getTimestamp(2).getTime))
      .map(row => (row.getString(0), 1))
      .rdd
      .map(each => BannerBrowseNum(each._1,
        each._1, each._2)).saveToEs("usertags/usertags")
    sparkSession.stop()
  }

  def accCarBrowseNum(): Unit ={
    val conf = ESMysqlSpark.getSparkAccConf()
    val prop = ESMysqlSpark.getSystemProp()
    val updateScript = prop.getProperty("updateScript").replaceAll("fieldName", "carBrowseNum")
    conf.set("es.update.script", updateScript)
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    import sparkSession.implicits._
    //carBroseNum
    val propMysqlLog = ESMysqlSpark.getMysqlConfLog()
    val carDS = sparkSession.read.jdbc(propMysqlLog.getProperty("url"),
      "tbd_url_element", propMysqlLog)
    carDS.filter("url = '/VirtualCard-v6/limousine/carTypeList' or " +
      "url = '/VirtualCard-v6/trafficsite/listHot/limousine' "
      + "or url = '/VirtualCard-v6/trafficsite/list/limousine'")
      .select(carDS("param"), carDS("create_time"))
        .filter(row => isYestoday(row.getTimestamp(1).getTime))
      .map(row => parseJson(row.getString(0)))
      .filter(each => !each.equals("")).map(x => (x, 1))
      .rdd
      .map(each => CarBrowseNum(each._1, each._1, each._2))
      .saveToEs("usertags/usertags")
      sparkSession.stop()
  }

  def accCarConsumptionNum(): Unit ={
    val conf = ESMysqlSpark.getSparkAccConf()
    val prop = ESMysqlSpark.getSystemProp()
    val updateScript = prop.getProperty("updateScript").replaceAll("fieldName", "carConsumptionNum")
    conf.set("es.update.script", updateScript)
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val propMysqlBussiness = ESMysqlSpark.getMysqlConfBusiness()
    val carOrderDS = sparkSession.read.jdbc(propMysqlBussiness.getProperty("url"),
      "tb_order_limousine", propMysqlBussiness)
    val tbOrderDS = sparkSession.read.jdbc(propMysqlBussiness.getProperty("url"),
      "tb_order", propMysqlBussiness)
    val carTbOrderDS = carOrderDS.join(tbOrderDS, carOrderDS("order_no")
      .equalTo(tbOrderDS("order_no")), "left_outer")
    import sparkSession.implicits._
    //carConsumptionNum
    carTbOrderDS.select(carTbOrderDS("user_id"), carTbOrderDS("cdate"))
      .na.drop().filter(row => isYestoday(row.getTimestamp(1).getTime))
      .map(row => (row.getAs(0).toString, 1))
      .rdd
      .filter(each => each._2 != 0)
      .map(each => CarConsumptionNum(each._1, each._1, each._2))
      .saveToEs("usertags/usertags")
    sparkSession.stop()
  }

  def main(args: Array[String]): Unit ={
    accArticleBrowseNum()
    accArticleFavorNum()
    accBannerBrowseNum()
    accCarBrowseNum()
    accCarConsumptionNum()
  }
}
