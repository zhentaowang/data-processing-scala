package cn.adatafun.dataprocess.accumulate

import cn.adatafun.dataprocess.conf.ESMysqlSpark
import cn.adatafun.dataprocess.rest.Rest._
import org.apache.spark.sql.SparkSession
import cn.adatafun.dataprocess.util.TodayGet.isYestoday
import org.elasticsearch.spark._

/**
  * Created by yanggf on 2017/11/22.
  */
object RestAcc {
  def accConsumptionNum(): Unit ={
    val conf = ESMysqlSpark.getSparkAccConf()
    val prop = ESMysqlSpark.getSystemProp()
    val updateScript = prop.getProperty("updateScript").replaceAll("fieldName", "consumptionNum")
    conf.set("es.update.script", updateScript)
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val propMysqlBusiness = ESMysqlSpark.getMysqlConfBusiness()
    val restOrderDS = sparkSession.read.jdbc(propMysqlBusiness.getProperty("url"),
      "restaurant_order_detail2", propMysqlBusiness)
    val tbOrderDS = sparkSession.read.jdbc(propMysqlBusiness.getProperty("url"),
      "tb_order", propMysqlBusiness)
    val restOrderJoinRestDS = restOrderDS.join(tbOrderDS, restOrderDS("fd_code").
      equalTo(tbOrderDS("order_no")), "left_outer")
    import sparkSession.implicits._
    //consumptionNum
    restOrderJoinRestDS.select(tbOrderDS("user_id"), restOrderDS("fd_restaurant_code"),
      restOrderDS("ordertime")).filter(row => !row.isNullAt(0) &&
    isYestoday(row.getTimestamp(2).getTime))
      .map(row => ((row.getLong(0).toString, row.getString(1)),1))
      .rdd.map(each =>
      ConsumptionNum(each._1._1 + each._1._2, each._1._1, each._1._2,
        each._2)).saveToEs("userrest/userrest")
    sparkSession.stop()
  }

  def accUsageCounter(): Unit ={
    val conf = ESMysqlSpark.getSparkAccConf()
    val prop = ESMysqlSpark.getSystemProp()
    val updateScript = prop.getProperty("updateScript").replaceAll("fieldName", "usageCounter")
    conf.set("es.update.script", updateScript)
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val propMysqlBusiness = ESMysqlSpark.getMysqlConfBusiness()
    val tbOrderDS = sparkSession.read.jdbc(propMysqlBusiness.getProperty("url"),
      "tb_order", propMysqlBusiness)
    import sparkSession.implicits._
    //usageCounter
    val usageDS = sparkSession.read.jdbc(propMysqlBusiness.getProperty("url"),
      "td_restaurant_order", propMysqlBusiness)
    val usageTbOrderDS = usageDS.join(tbOrderDS,
      usageDS("fd_code").equalTo(tbOrderDS("order_no")), "left_outer")
    usageTbOrderDS.select(usageTbOrderDS("user_id"),
      usageTbOrderDS("fd_restaurant_code"), usageDS("fd_date"))
      .filter(row => !row.isNullAt(0) && isYestoday(row.getTimestamp(2).getTime))
      .map(row => ((row.getLong(0).toString, row.getString(1)),1))
      .rdd
      .map(each =>
        UsageCounter(each._1._1 + each._1._2, each._1._1, each._1._2,
          each._2)).saveToEs("userrest/userrest")
    sparkSession.stop()
  }

  def accBrowseNum(): Unit ={
    val conf = ESMysqlSpark.getSparkAccConf()
    val prop = ESMysqlSpark.getSystemProp()
    val updateScript = prop.getProperty("updateScript").replaceAll("fieldName", "browseNum")
    conf.set("es.update.script", updateScript)
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    import sparkSession.implicits._
    //browseNum
    val propMysqlLog = ESMysqlSpark.getMysqlConfLog()
    val urlDS = sparkSession.read.jdbc(propMysqlLog.getProperty("url"),
      "tbd_url_element", propMysqlLog)
    urlDS.filter("url = '/VirtualCard-en/restaurant/detail' or url = '/VirtualCard-v5/restaurant/detail' " +
      "or url = '/VirtualCard-v6/restaurant/detail'")
      .select(urlDS("param"), urlDS("create_time"))
      .filter(row => isYestoday(row.getTimestamp(1).getTime))
      .map(row => parseJson(row.getString(0)))
      .filter(each => !each._1.equals("") && !each._2.equals(""))
      .map(x => (x, 1))
      .rdd
      .map(each =>
        BrowseNum(each._1._1 + each._1._2, each._1._1, each._1._2,
          each._2)).saveToEs("userrest/userrest")
    sparkSession.stop()
  }

  def accCollectionNum(): Unit ={
    val conf = ESMysqlSpark.getSparkAccConf()
    val prop = ESMysqlSpark.getSystemProp()
    val updateScript = prop.getProperty("updateScript").replaceAll("fieldName", "collectionNum")
    conf.set("es.update.script", updateScript)
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val propMysqlBusiness = ESMysqlSpark.getMysqlConfBusiness()
    import sparkSession.implicits._
    //collectionNum
    val collectDS = sparkSession.read.jdbc(propMysqlBusiness.getProperty("url"),
      "tb_user_collect", propMysqlBusiness)
    collectDS.filter(row => isYestoday(row.getTimestamp(4).getTime))
      .map(row => ((row.getLong(1).toString, row.getString(3)), 1))
      .filter(each => each._1._2.startsWith("R"))
      .rdd
      .map(each => CollectionNum(each._1._1 + each._1._2, each._1._1, each._1._2,
        each._2)).saveToEs("userrest/userrest")
    sparkSession.stop()
  }

  def accCommentNum(): Unit ={
    val conf = ESMysqlSpark.getSparkAccConf()
    val prop = ESMysqlSpark.getSystemProp()
    val updateScript = prop.getProperty("updateScript").replaceAll("fieldName", "commentNum")
    conf.set("es.update.script", updateScript)
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val propMysqlBusiness2 = ESMysqlSpark.getMysqlConfBusiness2()
    import sparkSession.implicits._
    //commentNum
    val commentDS = sparkSession.read.jdbc(propMysqlBusiness2.getProperty("url"),
      "customer_share", propMysqlBusiness2)
    commentDS.filter(row => !row.isNullAt(1) && !row.isNullAt(5) &&
    isYestoday(row.getTimestamp(4).getTime))
      .map(row => ((row.getInt(1).toString, row.getString(5)), 1))
      .filter(each => each._1._2.startsWith("R"))
      .rdd
      .map(each => CommentNum(each._1._1 + each._1._2, each._1._1, each._1._2,
        each._2))
//      .take(200) foreach println
      .saveToEs("userrest/userrest")
    sparkSession.stop()
  }
  def main(args: Array[String]): Unit ={
    accConsumptionNum()
    accUsageCounter()
    accBrowseNum()
    accCollectionNum()
    accCommentNum()
  }

}
