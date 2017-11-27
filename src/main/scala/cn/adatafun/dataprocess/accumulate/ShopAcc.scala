package cn.adatafun.dataprocess.accumulate

import cn.adatafun.dataprocess.conf.ESMysqlSpark
import cn.adatafun.dataprocess.rest.Rest.{BrowseNum, CollectionNum}
import org.apache.spark.sql.SparkSession
import cn.adatafun.dataprocess.util.TodayGet.isYestoday
import cn.adatafun.dataprocess.shop.Shop.parseJson
import org.elasticsearch.spark._

/**
  * Created by yanggf on 2017/11/22.
  */
object ShopAcc {
  def accBrowseNum(): Unit ={
    val conf = ESMysqlSpark.getSparkAccConf()
    val prop = ESMysqlSpark.getSystemProp()
    val updateScript = prop.getProperty("updateScript").replaceAll("fieldName", "browseNum")
    conf.set("es.update.script", updateScript)
    val propMysqlLog = ESMysqlSpark.getMysqlConfLog()
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val browseDS = sparkSession.read.jdbc(propMysqlLog.getProperty("url"),
      "tbd_url_element", propMysqlLog)

    import sparkSession.implicits._
    //browseNum Process
    browseDS.coalesce(8)
      .filter("url = '/VirtualCard-v5/shop/detail' or " +
      "url = '/VirtualCard-v5/shop/getNearbyShare'")
      .select(browseDS("param"), browseDS("create_time"))
        .filter(row => isYestoday(row.getTimestamp(1).getTime))
      .map(row => parseJson(row.getString(0)))
      .filter(each => !each._1.equals("") && !each._2.equals(""))
      .map(x => (x, 1))
      .rdd.map(each => BrowseNum(
      each._1._1 + each._1._2, each._1._1, each._1._2, each._2
    )) //take 200 foreach println
      .saveToEs("usershop/usershop")
    sparkSession.stop()
  }

  def accCollectionNum(): Unit ={
    val conf = ESMysqlSpark.getSparkAccConf()
    val prop = ESMysqlSpark.getSystemProp()
    val updateScript = prop.getProperty("updateScript").replaceAll("fieldName", "collectionNum")
    conf.set("es.update.script", updateScript)
    val propMysqlBusiness = ESMysqlSpark.getMysqlConfBusiness()
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()

    import sparkSession.implicits._
    //collectionNum Process
    val collectDS = sparkSession.read.jdbc(propMysqlBusiness.getProperty("url"),
      "tb_user_collect", propMysqlBusiness)
    collectDS.filter(row => isYestoday(row.getTimestamp(4).getTime))
      .map(row => ((row.getAs(1).toString, row.getString(3)), 1))
      .filter(row => row._1._2.startsWith("F"))
      .rdd
      .map(each => CollectionNum(each._1._1 + each._1._2, each._1._1,
        each._1._2, each._2)) //take 20 foreach println
      .saveToEs("usershop/usershop")
    sparkSession.stop()
  }
def main(args: Array[String]): Unit ={
    accBrowseNum()
    accCollectionNum()
  }
}
