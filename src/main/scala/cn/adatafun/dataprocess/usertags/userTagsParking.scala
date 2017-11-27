package cn.adatafun.dataprocess.usertags

import cn.adatafun.dataprocess.conf.ESMysqlSpark
import org.elasticsearch.spark._
import spray.json.JsonParser

/**
  * Created by yanggf on 2017/11/17.
  */
object userTagsParking {
  case class ParkingAverageOrderAmount(id: String, userId: String,
                                       parkingAverageOrderAmount: Double)
  case class ParkingBrowseNum(id: String, userId: String,
                              parkingBrowseNum: Int)
  case class ParkingConsumptionNum(id: String, userId: String,
                                   parkingConsumptionNum: Int)

  def parseJson(jsonStr: String): String = {
    val json = JsonParser(jsonStr).asJsObject
    try{
      json.getFields("userId")(0).toString().replace("\"","")
    } catch {
      case ex: java.lang.IndexOutOfBoundsException =>{
        ""
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val sparkSession = ESMysqlSpark.getSparkSession()
    val propMysqlBusiness = ESMysqlSpark.getMysqlConfBusiness()
    val parkingOrderDS = sparkSession.read.jdbc(propMysqlBusiness.getProperty("url"),
      "tb_order_parking", propMysqlBusiness)
    val tbOrderDS = sparkSession.read.jdbc(propMysqlBusiness.getProperty("url"),
      "tb_order", propMysqlBusiness)
    val parkingJoinDS = parkingOrderDS.join(tbOrderDS, parkingOrderDS("order_no").
      equalTo(tbOrderDS("order_no")), "left_outer")

    import sparkSession.implicits._
    //parkingAverageOrderAmount
    parkingJoinDS.select(tbOrderDS("user_id"), parkingOrderDS("parking_code"),
      tbOrderDS("actual_amount")).filter(row => !row.isNullAt(0) && !row.isNullAt(1)
      && !row.isNullAt(2))
      .map(row => (row.getLong(0).toString,
        (row.getDouble(2), 1))).rdd.reduceByKey((x, y) =>(x._1 + y._1, x._2 + y._2))
      .map(each => ParkingAverageOrderAmount(each._1,
        each._1, each._2._1/each._2._2)).saveToEs("usertags/usertags")

    //parkingBrowseNum
    val propMysqlLog = ESMysqlSpark.getMysqlConfLog()
    val parkingLogDS = sparkSession.read.jdbc(propMysqlLog.getProperty("url"),
      "tbd_url_element", propMysqlLog)
    parkingLogDS.filter("url = '/VirtualCard-v6/parking/items'")
      .select(parkingLogDS("param"))
      .map(row => parseJson(row.getString(0))).coalesce(8)
      .filter(each => !each.equals(""))
      .map(x => (x,1)).rdd.reduceByKey((x,y) => x+y).map(each =>
      ParkingBrowseNum(each._1, each._1, each._2))
      .saveToEs("usertags/usertags")

    //parkingConsumptionNum
    parkingJoinDS.select(tbOrderDS("user_id"), parkingOrderDS("parking_code"),
      tbOrderDS("actual_amount")).filter(row => !row.isNullAt(0) && !row.isNullAt(1)
      && !row.isNullAt(2))
      .map(row => (row.getLong(0).toString,
        1)).rdd.reduceByKey((x, y) => x+y)
      .map(each => ParkingAverageOrderAmount(each._1,
        each._1, each._2)).saveToEs("usertags/usertags")
  }
}
