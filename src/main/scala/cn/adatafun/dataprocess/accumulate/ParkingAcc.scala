package cn.adatafun.dataprocess.accumulate

import cn.adatafun.dataprocess.conf.ESMysqlSpark
import cn.adatafun.dataprocess.parking.Parking.{ParkingAverageOrderAmount, ParkingBrowseNum, parseJson}
import org.apache.spark.sql.SparkSession
import cn.adatafun.dataprocess.util.TodayGet.isYestoday
import org.elasticsearch.spark._

/**
  * Created by yanggf on 2017/11/22.
  */
object ParkingAcc {
  def accBrowseNum(): Unit ={
    val conf = ESMysqlSpark.getSparkAccConf()
    val prop = ESMysqlSpark.getSystemProp()
    val updateScript = prop.getProperty("updateScript").replaceAll("fieldName", "browseNum")
    conf.set("es.update.script", updateScript)
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    import sparkSession.implicits._
    //parkingBrowseNum
    val propMysqlLog = ESMysqlSpark.getMysqlConfLog()
    val parkingLogDS = sparkSession.read.jdbc(propMysqlLog.getProperty("url"),
      "tbd_url_element", propMysqlLog)
    val resultDS = parkingLogDS.filter("url = '/VirtualCard-v6/parking/items'")
      .select(parkingLogDS("param"), parkingLogDS("create_time"))
      .filter(row => isYestoday(row.getTimestamp(1).getTime))
      .map(row => parseJson(row.getString(0))).coalesce(8)
      .filter(each => !each._1.equals("") && !each._2.equals(""))
      .map(x => (x,1)).rdd.map(each =>
      ParkingBrowseNum(each._1._1 + each._1._2, each._1._1, each._1._2, each._2))
      .saveToEs("userparking/userparking")
    sparkSession.stop()
  }
  def accConsumptionNum(): Unit ={
    val conf = ESMysqlSpark.getSparkAccConf()
    val prop = ESMysqlSpark.getSystemProp()
    val updateScript = prop.getProperty("updateScript").replaceAll("fieldName", "consumptionNum")
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
      && isYestoday(row.getDate(2).getTime))
      .map(row => ((row.getLong(0).toString, row.getString(1)),
        1)).rdd
      .map(each => ParkingAverageOrderAmount(each._1._1 + each._1._2,
        each._1._1, each._1._2, each._2)).saveToEs("userparking/userparking")
    sparkSession.stop()
  }

  def main(args: Array[String]): Unit ={
    accBrowseNum()
    accConsumptionNum()
  }
}
