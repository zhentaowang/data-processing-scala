package cn.adatafun.dataprocess.accumulate

import cn.adatafun.dataprocess.conf.ESMysqlSpark
import cn.adatafun.dataprocess.usertags.UserTagsLounge.{LoungeCollectionNum, LoungeCommentNum, LoungeConsumptionNum, LoungeUsageCounter}
import cn.adatafun.dataprocess.util.TodayGet._
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark._

/**
  * Created by yanggf on 2017/11/22.
  */
object UserTagsLoungeAcc {

  def accLoungeConsumptionNum(): Unit ={
    val conf = ESMysqlSpark.getSparkAccConf()
    val prop = ESMysqlSpark.getSystemProp()
    val updateScript = prop.getProperty("updateScript").replaceAll("fieldName", "loungeConsumptionNum")
    conf.set("es.update.script", updateScript)
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val propMysql = ESMysqlSpark.getMysqlConfBusiness()
    val bindDS = sparkSession.read.jdbc(propMysql.getProperty("url"),
      "tb_order", propMysql)
    //comsumptionNum accumulative process
    val servDS = sparkSession.read.jdbc(propMysql.getProperty("url"),
      "tb_order_loungeserv", propMysql)
    val servOrderDS = servDS.join(bindDS,
      servDS("order_no").equalTo(bindDS("order_no")), "left_outer")
    servOrderDS.select(servOrderDS("user_id"), servOrderDS("lounge_code"),
      servDS("create_date"))
      .filter(row => !row.isNullAt(0) && !row.isNullAt(1)
        && isYestoday(row.getDate(2).getTime))
      .rdd.flatMap(row => {
      val userId = row.getLong(0).toString
      val userIdList = List(row.getLong(0).toString)
      val codeList = row.getString(1).split(",").toList
      val key:List[(String, String)] = userIdList.zipAll(codeList, userId, "")
      key //这里需要分两步操作将数据映射为（（x,y）,1）要不然需要处理seriazable对象
    }).map(x => (x, 1)).map(each => (each._1._1, 1))
      .map(each => LoungeConsumptionNum(each._1,
        each._1, each._2)) //take 200 foreach println
      .saveToEs("usertags/usertags")

    sparkSession.stop()
  }

  def accLoungeCollectionNum(): Unit ={
    val conf = ESMysqlSpark.getSparkAccConf()
    val prop = ESMysqlSpark.getSystemProp()
    val updateScript = prop.getProperty("updateScript").replaceAll("fieldName", "loungeCollectionNum")
    conf.set("es.update.script", updateScript)
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val propMysql = ESMysqlSpark.getMysqlConfBusiness()
    //collectionNum process
    val collectDS = sparkSession.read.jdbc(propMysql.getProperty("url"),
      "tb_user_collect", propMysql)
    import sparkSession.implicits._
    collectDS.filter(row => isYestoday(row.getTimestamp(4).getTime))
      .map(row => ((row.getLong(1).toString, row.getString(3)), 1))
      .filter(row => row._1._2.startsWith("N")).map(each => (each._1._1, 1))
      .rdd
      .map(each => LoungeCollectionNum(
      each._1, each._1, each._2
    )) //take 200 foreach println
      .saveToEs("usertags/usertags")
    sparkSession.stop()
  }

  def accLoungeCommentNum(): Unit ={
    val conf = ESMysqlSpark.getSparkAccConf()
    val prop = ESMysqlSpark.getSystemProp()
    val updateScript = prop.getProperty("updateScript").replaceAll("fieldName", "loungeCommentNum")
    conf.set("es.update.script", updateScript)
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val propMysql = ESMysqlSpark.getMysqlConfBusiness()
    import sparkSession.implicits._
    //commentNum process
    val commentDS = sparkSession.read.jdbc(propMysql.getProperty("url"),
      "tb_customer_share", propMysql)
    commentDS.filter("user_id is not null and code is not null and score is not null and score > 2")
      .filter(row => isYestoday(row.getTimestamp(7).getTime))
      .map(row => ((row.getLong(6).toString, row.getString(17)), 1))
      .filter(each => each._1._2.startsWith("N")).map(each => (each._1._1, 1))
      .rdd.map(each => LoungeCommentNum(
      each._1, each._1, each._2
    )) //take 200 foreach println
      .saveToEs("userlounge/userlounge")
    sparkSession.stop()
  }

  def accLoungeUsageCounter(): Unit ={
    val conf = ESMysqlSpark.getSparkAccConf()
    val prop = ESMysqlSpark.getSystemProp()
    val updateScript = prop.getProperty("updateScript").replaceAll("fieldName", "loungeUsageCounter")
    conf.set("es.update.script", updateScript)
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val propMysql = ESMysqlSpark.getMysqlConfBusiness()
    val orderDS = sparkSession.read.jdbc(propMysql.getProperty("url"),
      "tblconsumerecord", propMysql)
    val bindDS = sparkSession.read.jdbc(propMysql.getProperty("url"),
      "tb_bindrecord", propMysql)
    val joinDS = orderDS.join(bindDS,
      orderDS("dragoncode").equalTo(bindDS("dragoncode")),"left_outer")
    import sparkSession.implicits._
    //usageCounter process
    val resultDS = joinDS.select(joinDS("user_id"), joinDS("loungecode"), joinDS("consumetime"))
      .filter(row => !row.isNullAt(0) && !row.isNullAt(1)
        && isYestoday(row.getTimestamp(2).getTime))
      .map(row => (row.getLong(0).toString, 1))
      .rdd
      .map(each => LoungeUsageCounter(each._1,
        each._1, each._2)) //take 200 foreach println
      .saveToEs("userlounge/userlounge")
  }

  def main(args: Array[String]): Unit ={
    accLoungeCollectionNum()
    accLoungeCommentNum()
    accLoungeConsumptionNum()
    accLoungeUsageCounter()
  }
}
