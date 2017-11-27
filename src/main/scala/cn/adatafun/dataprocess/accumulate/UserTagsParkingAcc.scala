package cn.adatafun.dataprocess.accumulate

import cn.adatafun.dataprocess.conf.ESMysqlSpark
import cn.adatafun.dataprocess.usertags.userTagsParking.{ParkingAverageOrderAmount, ParkingBrowseNum, parseJson}
import org.apache.spark.sql.SparkSession
import cn.adatafun.dataprocess.util.TodayGet.isYestoday
import org.elasticsearch.spark._

/**
  * Created by yanggf on 2017/11/23.
  */
object UserTagsParkingAcc {
  def accParkingBrowseNum(): Unit ={
    val conf = ESMysqlSpark.getSparkAccConf()
    val prop = ESMysqlSpark.getSystemProp()
    val updateScript = prop.getProperty("updateScript").replaceAll("fieldName", "parkingBrowseNum")
    conf.set("es.update.script", updateScript)
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    import sparkSession.implicits._
    //parkingBrowseNum
    val propMysqlLog = ESMysqlSpark.getMysqlConfLog()
    val parkingLogDS = sparkSession.read.jdbc(propMysqlLog.getProperty("url"),
      "tbd_url_element", propMysqlLog)
    parkingLogDS.filter("url = '/VirtualCard-v6/parking/items'")
      .select(parkingLogDS("param"), parkingLogDS("create_time"))
        .filter(row => isYestoday(row.getTimestamp(1).getTime))
      .map(row => parseJson(row.getString(0))).coalesce(8)
      .filter(each => !each.equals(""))
      .map(x => (x,1)).rdd.reduceByKey((x,y) => x+y).map(each =>
      ParkingBrowseNum(each._1, each._1, each._2))
//        .take(200) foreach println
      .saveToEs("usertags/usertags")
    sparkSession.stop()
  }

  def accParkingConsumptionNum(): Unit ={
    val conf = ESMysqlSpark.getSparkAccConf()
    val prop = ESMysqlSpark.getSystemProp()
    val updateScript = prop.getProperty("updateScript").replaceAll("fieldName", "parkingConsumptionNum")
    conf.set("es.update.script", updateScript)
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val propMysqlBusiness = ESMysqlSpark.getMysqlConfBusiness()
    val parkingOrderDS = sparkSession.read.jdbc(propMysqlBusiness.getProperty("url"),
      "tb_order_parking", propMysqlBusiness)
    val tbOrderDS = sparkSession.read.jdbc(propMysqlBusiness.getProperty("url"),
      "tb_order", propMysqlBusiness)
    val parkingJoinDS = parkingOrderDS.join(tbOrderDS, parkingOrderDS("order_no").
      equalTo(tbOrderDS("order_no")), "left_outer")
    import sparkSession.implicits._
    //parkingConsumptionNum
    parkingJoinDS.select(tbOrderDS("user_id"), parkingOrderDS("parking_code"),
      parkingOrderDS("create_date")).filter(row => !row.isNullAt(0) && !row.isNullAt(1)
      && !row.isNullAt(2) && isYestoday(row.getDate(2).getTime))
      .map(row => (row.getLong(0).toString,
        1)).rdd
      .map(each => ParkingAverageOrderAmount(each._1,
        each._1, each._2))
//        .take(200) foreach println
      .saveToEs("usertags/usertags")
    sparkSession.stop()
  }

  def main(args: Array[String]): Unit ={
    accParkingBrowseNum()
    accParkingConsumptionNum()
  }

}
