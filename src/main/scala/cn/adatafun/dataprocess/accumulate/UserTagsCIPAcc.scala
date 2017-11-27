package cn.adatafun.dataprocess.accumulate

import cn.adatafun.dataprocess.conf.ESMysqlSpark
import cn.adatafun.dataprocess.usertags.UserTagsCIP.{CIPBrowseNum, CIPConsumptionNum, CIPUsageCounter}
import cn.adatafun.dataprocess.usertags.UserTagsOne._
import org.apache.spark.sql.SparkSession
import cn.adatafun.dataprocess.util.TodayGet.isYestoday
import org.elasticsearch.spark._

/**
  * Created by yanggf on 2017/11/22.
  */
object UserTagsCIPAcc {
  def accCIPBrowseNum(): Unit ={
    val conf = ESMysqlSpark.getSparkAccConf()
    val prop = ESMysqlSpark.getSystemProp()
    val updateScript = prop.getProperty("updateScript").replaceAll("fieldName", "CIPBrowseNum")
    conf.set("es.update.script", updateScript)
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    import sparkSession.implicits._
    //CIPbrowseNum
    val propMysqlLog = ESMysqlSpark.getMysqlConfLog()
    val urlDS = sparkSession.read.jdbc(propMysqlLog.getProperty("url"),
      "tbd_url_element", propMysqlLog)
    urlDS.filter("url = '/VirtualCard-v6/orderCip/listCip' or " +
      "url = '/VirtualCard-v6/trafficsite/list/cip' "
      + "or url = '/VirtualCard-v5/orderCip/getCips' or " +
      "url = '/VirtualCard-v5/orderCip/getCips'")
      .select(urlDS("param"), urlDS("create_time"))
        .filter(row => isYestoday(row.getTimestamp(1).getTime))
      .map(row => parseJson(row.getString(0)))
      .filter(each => !each.equals(""))
      .map(x => (x, 1))
      .rdd
      .map(each =>
        CIPBrowseNum(each._1, each._1,
          each._2)).saveToEs("usertags/usertags")
    sparkSession.stop()
  }

  def accCIPConsumptionNum(): Unit ={
    val conf = ESMysqlSpark.getSparkAccConf()
    val prop = ESMysqlSpark.getSystemProp()
    val updateScript = prop.getProperty("updateScript").replaceAll("fieldName", "CIPConsumptionNum")
    conf.set("es.update.script", updateScript)
    val propMysql = ESMysqlSpark.getMysqlConfBusiness()
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val orderDS = sparkSession.read.jdbc(propMysql.getProperty("url"),
      "tb_order_cip", propMysql)
    val tbOrderDS = sparkSession.read.jdbc(propMysql.getProperty("url"),
      "tb_order", propMysql)
    val togetherDS = orderDS.join(tbOrderDS,orderDS("order_no").equalTo(tbOrderDS("order_no")),
      "left_outer")
    import sparkSession.implicits._
    //CIPConsumptionNum
    togetherDS.select(tbOrderDS("user_id"), orderDS("pro_code"), tbOrderDS("cdate"))
      .filter(row => !row.isNullAt(0) && !row.isNullAt(1) && !row.isNullAt(2) &&
      isYestoday(row.getTimestamp(2).getTime))
      .map(row => (row.getLong(0).toString, 1))
      .rdd
      .map(each => CIPConsumptionNum(each._1,
        each._1,  each._2)) //take 200 foreach println
      .saveToEs("usertags/usertags")
    sparkSession.stop()
  }

  def accCIPUsageCounter(): Unit ={
    val conf = ESMysqlSpark.getSparkAccConf()
    val prop = ESMysqlSpark.getSystemProp()
    val updateScript = prop.getProperty("updateScript").replaceAll("fieldName", "CIPUsageCounter")
    conf.set("es.update.script", updateScript)
    val propMysql = ESMysqlSpark.getMysqlConfBusiness()
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val orderDS = sparkSession.read.jdbc(propMysql.getProperty("url"),
      "tb_order_cip", propMysql)
    val tbOrderDS = sparkSession.read.jdbc(propMysql.getProperty("url"),
      "tb_order", propMysql)
    import sparkSession.implicits._
    //CIPUsageCounter
    val usageDS = sparkSession.read.jdbc(propMysql.getProperty("url"),
      "tb_order_cip_record", propMysql)
    val tbOrderRenamedDS = tbOrderDS.withColumnRenamed("order_no", "order_name1")
    val orderTbDS = orderDS.join(tbOrderRenamedDS, orderDS("order_no").
      equalTo(tbOrderRenamedDS("order_name1")), "left_outer")
    val counterDS = usageDS.join(orderTbDS, usageDS("order_no").
      equalTo(orderTbDS("order_no")), "left_outer")
    counterDS.select(tbOrderDS("user_id"), orderDS("pro_code"), tbOrderDS("cdate"))
      .na.drop()
        .filter(row => isYestoday(row.getTimestamp(2).getTime))
      .map(row => (row.getAs(0).toString, 1))
      .rdd
      .map(each => CIPUsageCounter(each._1, each._1, each._2))
//  .take(200) foreach println
      .saveToEs("usertags/usertags")
    sparkSession.stop()
  }
  def main(args: Array[String]): Unit ={
    accCIPBrowseNum()
    accCIPConsumptionNum()
    accCIPUsageCounter()

  }

}
