package cn.adatafun.dataprocess.parking

import cn.adatafun.dataprocess.conf.ESMysqlSpark
import org.elasticsearch.spark._
import spray.json.JsonParser

object Parking {
  case class ParkingAverageOrderAmount(id:String, userId:String,
                                       restaurantCode:String, averageOrderAmount:Double)
  case class ParkingBrowseNum(id:String, userId:String,
                                       restaurantCode:String, browseNum:Int)
  case class ParkingConsumptionNum(id:String, userId:String,
                              restaurantCode:String, consumptionNum:Int)

  def parseJson(jsonStr: String): (String, String) = {
    val json = JsonParser(jsonStr).asJsObject
    try{
      (json.getFields("userId")(0).toString().replace("\"",""),
        json.getFields("parkingCode")(0).toString().replace("\"",""))
    } catch {
      case ex: java.lang.IndexOutOfBoundsException =>{
        ("","")
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
      .map(row => ((row.getLong(0).toString, row.getString(1)),
      (row.getDouble(2), 1))).rdd.reduceByKey((x, y) =>(x._1 + y._1, x._2 + y._2))
      .map(each => ParkingAverageOrderAmount(each._1._1 + each._1._2,
        each._1._1, each._1._2, each._2._1/each._2._2)).saveToEs("userparking/userparking")

    //parkingBrowseNum
    val propMysqlLog = ESMysqlSpark.getMysqlConfLog()
    val parkingLogDS = sparkSession.read.jdbc(propMysqlLog.getProperty("url"),
    "tbd_url_element", propMysqlLog)
    parkingLogDS.filter("url = '/VirtualCard-v6/parking/items'")
      .select(parkingLogDS("param"))
      .map(row => parseJson(row.getString(0))).coalesce(8)
      .filter(each => !each._1.equals("") && !each._2.equals(""))
      .map(x => (x,1)).rdd.reduceByKey((x,y) => x+y).map(each =>
    ParkingBrowseNum(each._1._1 + each._1._2, each._1._1, each._1._2, each._2))
      .saveToEs("userparking/userparking")

    //parkingConsumptionNum
    parkingJoinDS.select(tbOrderDS("user_id"), parkingOrderDS("parking_code"),
      tbOrderDS("actual_amount")).filter(row => !row.isNullAt(0) && !row.isNullAt(1)
      && !row.isNullAt(2))
      .map(row => ((row.getLong(0).toString, row.getString(1)),
      1)).rdd.reduceByKey((x, y) => x+y)
      .map(each => ParkingAverageOrderAmount(each._1._1 + each._1._2,
        each._1._1, each._1._2, each._2)).saveToEs("userparking/userparking")
  }

}
