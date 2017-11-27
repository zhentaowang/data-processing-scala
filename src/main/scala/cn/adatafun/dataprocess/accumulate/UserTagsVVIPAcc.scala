package cn.adatafun.dataprocess.accumulate

import cn.adatafun.dataprocess.conf.ESMysqlSpark
import cn.adatafun.dataprocess.usertags.UserTagsVVIP.{VVIPBrowseNum, VVIPCollectionNum, VVIPConsumptionNum, parseJson}
import org.apache.spark.sql.SparkSession
import cn.adatafun.dataprocess.util.TodayGet.isYestoday
import org.elasticsearch.spark._

/**
  * Created by yanggf on 2017/11/23.
  */
object UserTagsVVIPAcc {
  def accVVIPBrowseNum(): Unit ={
    val conf = ESMysqlSpark.getSparkAccConf()
    val prop = ESMysqlSpark.getSystemProp()
    val updateScript = prop.getProperty("updateScript").replaceAll("fieldName", "VVIPBrowseNum")
    conf.set("es.update.script", updateScript)
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    import sparkSession.implicits._
    //vvipBrowseNum
    val propMysqlLog = ESMysqlSpark.getMysqlConfLog()
    val vipUrlDS = sparkSession.read.jdbc(propMysqlLog.getProperty("url"),
      "tbd_url_element", propMysqlLog)
    vipUrlDS.filter("url = '/VirtualCard-v6/share/count/vvip' or " +
      "url = '/VirtualCard-v6/vvip/index' " +
      "or url = '/VirtualCard-v5/vvip/index' or " +
      "url = '/VirtualCard-v5/vvip/setTypes' or " +
      "url = '/VirtualCard-v5/share/getVvipShare'")
      .select("param", "create_time")
        .filter(row => isYestoday(row.getTimestamp(1).getTime))
      .map(row => parseJson(row.getString(0)))
      .filter(row => !row.equals(""))
      .map(x => (x, 1)).rdd
      .map(each => VVIPBrowseNum(each._1, each._1, each._2)) //take 200 foreach println
      .saveToEs("usertags/usertags")
  }

  def accVVIPCollectionNum(): Unit ={
    val conf = ESMysqlSpark.getSparkAccConf()
    val prop = ESMysqlSpark.getSystemProp()
    val updateScript = prop.getProperty("updateScript").replaceAll("fieldName", "VVIPCollectionNum")
    conf.set("es.update.script", updateScript)
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    import sparkSession.implicits._
    val propMysqlBusiness = ESMysqlSpark.getMysqlConfBusiness()
    //vvipCollectionNum
    val collectDS = sparkSession.read.jdbc(propMysqlBusiness.getProperty("url"),
      "tb_user_collect", propMysqlBusiness)
    collectDS.filter(row => isYestoday(row.getTimestamp(4).getTime))
      .map(row => ((row.getAs(1).toString, row.getString(3)), 1))
      .filter(each => each._1._2.startsWith("M"))
      .rdd.map(row => (row._1._1, 1))
      .map(each => VVIPCollectionNum(each._1, each._1,
        each._2)) //take 200 foreach println
      .saveToEs("usertags/usertags")
  }

  def accVVIPConsumptionNum(): Unit ={
    val conf = ESMysqlSpark.getSparkAccConf()
    val prop = ESMysqlSpark.getSystemProp()
    val updateScript = prop.getProperty("updateScript").replaceAll("fieldName", "VVIPCollectionNum")
    conf.set("es.update.script", updateScript)
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val propMysqlBusiness = ESMysqlSpark.getMysqlConfBusiness()
    val vvipOrderDS = sparkSession.read.jdbc(propMysqlBusiness.getProperty("url"),
      "tb_order_vvip", propMysqlBusiness)
    val tbOrderDS = sparkSession.read.jdbc(propMysqlBusiness.getProperty("url"),
      "tb_order", propMysqlBusiness)
    val joinDS = vvipOrderDS.join(tbOrderDS, vvipOrderDS("order_no")
      .equalTo(tbOrderDS("order_no")), "left_outer")

    import sparkSession.implicits._
    //vvipconsumptionNum
    joinDS.select(joinDS("user_id"), joinDS("actual_amount"), tbOrderDS("cdate")).na.drop()
        .filter(row => isYestoday(row.getTimestamp(2).getTime))
      .map(row => (row.getAs(0).toString, (row.getDouble(1), 1)))
      .rdd
      .filter(each => each._2._2 != 0)
      .map(each => VVIPConsumptionNum(each._1, each._1,
        each._2._2)) //take 200 foreach println
      .saveToEs("usertags/usertags")
  }

  def main(args: Array[String]): Unit ={
    accVVIPBrowseNum()
    accVVIPCollectionNum()
    accVVIPConsumptionNum()
  }

}
