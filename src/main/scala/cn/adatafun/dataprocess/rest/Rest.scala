package cn.adatafun.dataprocess.rest

import cn.adatafun.dataprocess.conf.ESMysqlSpark
import org.elasticsearch.spark._
import spray.json.JsonParser

object Rest {
  case class AverageOrderAmount(id:String, userId:String,
                                restaurantCode:String, averageOrderAmount:Double)
  case class ConsumptionNum(id:String, userId:String,
                            restaurantCode:String, consumptionNum:Int)
  case class MultitimeConsumption(id:String, userId:String,
                            restaurantCode:String, multitimeConsumption:Boolean)
  case class UsageCounter(id:String, userId:String,
                                  restaurantCode:String, usageCounter:Int)
  case class BrowseNum(id:String, userId:String,
                          restaurantCode:String, browseNum:Int)
  case class CollectionNum(id:String, userId:String,
                       restaurantCode:String, collectionNum:Int)
  case class CommentNum(id:String, userId:String,
                           restaurantCode:String, commentNum:Int)

  def parseJson(jsonStr: String): (String, String) = {
    val json = JsonParser(jsonStr).asJsObject
    try{
      (json.getFields("userId")(0).toString().replace("\"",""),
        json.getFields("code")(0).toString().replace("\"",""))
    } catch {
      case ex: java.lang.IndexOutOfBoundsException =>{
        ("","")
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val sparkSession = ESMysqlSpark.getSparkSession()
    val propMysqlBusiness = ESMysqlSpark.getMysqlConfBusiness()
    val propMysqlBusiness2 = ESMysqlSpark.getMysqlConfBusiness2()
    val restOrderDS = sparkSession.read.jdbc(propMysqlBusiness.getProperty("url"),
    "restaurant_order_detail2", propMysqlBusiness)
    val tbOrderDS = sparkSession.read.jdbc(propMysqlBusiness.getProperty("url"),
    "tb_order", propMysqlBusiness)
    val restOrderJoinRestDS = restOrderDS.join(tbOrderDS, restOrderDS("fd_code").
      equalTo(tbOrderDS("order_no")), "left_outer")

    import sparkSession.implicits._
    //averageOrderAmount
    restOrderJoinRestDS.select(tbOrderDS("user_id"), restOrderDS("fd_restaurant_code"),
      restOrderDS("xfprice")).filter(row => !row.isNullAt(0))
      .map(row => ((row.getLong(0).toString, row.getString(1)),(row.getDouble(2),1)))
      .rdd.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2)).map(each =>
    AverageOrderAmount(each._1._1 + each._1._2, each._1._1, each._1._2,
      each._2._1/each._2._2)).saveToEs("userrest/userrest")

    //consumptionNum
    restOrderJoinRestDS.select(tbOrderDS("user_id"), restOrderDS("fd_restaurant_code"),
      restOrderDS("xfprice")).filter(row => !row.isNullAt(0))
      .map(row => ((row.getLong(0).toString, row.getString(1)),1))
      .rdd.reduceByKey((x,y) => x + y).map(each =>
      ConsumptionNum(each._1._1 + each._1._2, each._1._1, each._1._2,
        each._2)).saveToEs("userrest/userrest")

    //multitimeConsumption
    restOrderJoinRestDS.select(tbOrderDS("user_id"), restOrderDS("fd_restaurant_code"),
      restOrderDS("xfprice")).filter(row => !row.isNullAt(0))
      .map(row => ((row.getLong(0).toString, row.getString(1)),1))
      .rdd.reduceByKey((x,y) => x + y).map(each =>
        ((each._1._1,each._1._2), if(each._2>1)true else false))
      .map(each =>
      MultitimeConsumption(each._1._1 + each._1._2, each._1._1, each._1._2,
        each._2)).saveToEs("userrest/userrest")

    //usageCounter
    val usageDS = sparkSession.read.jdbc(propMysqlBusiness.getProperty("url"),
    "td_restaurant_order", propMysqlBusiness)
    val usageTbOrderDS = usageDS.join(tbOrderDS,
      usageDS("fd_code").equalTo(tbOrderDS("order_no")), "left_outer")
    usageTbOrderDS.select(usageTbOrderDS("user_id"),
      usageTbOrderDS("fd_restaurant_code"))
      .filter(row => !row.isNullAt(0))
      .map(row => ((row.getLong(0).toString, row.getString(1)),1))
      .rdd.reduceByKey((x,y) => x + y)
      .map(each =>
      UsageCounter(each._1._1 + each._1._2, each._1._1, each._1._2,
        each._2)).saveToEs("userrest/userrest")

    //browseNum
    val propMysqlLog = ESMysqlSpark.getMysqlConfLog()
    val urlDS = sparkSession.read.jdbc(propMysqlLog.getProperty("url"),
    "tbd_url_element", propMysqlLog)
    urlDS.filter("url = '/VirtualCard-en/restaurant/detail' or url = '/VirtualCard-v5/restaurant/detail' " +
      "or url = '/VirtualCard-v6/restaurant/detail'")
      .select(urlDS("param"))
      .map(row => parseJson(row.getString(0)))
      .filter(each => !each._1.equals("") && !each._2.equals(""))
      .map(x => (x, 1))
      .rdd.reduceByKey((x, y) => x+y)
      .map(each =>
      BrowseNum(each._1._1 + each._1._2, each._1._1, each._1._2,
        each._2)).saveToEs("userrest/userrest")

    //collectionNum
    val collectDS = sparkSession.read.jdbc(propMysqlBusiness.getProperty("url"),
    "tb_user_collect", propMysqlBusiness)
    collectDS.map(row => ((row.getLong(1).toString, row.getString(3)), 1))
      .filter(each => each._1._2.startsWith("R"))
      .rdd.reduceByKey((x, y) => x+y)
      .map(each => CollectionNum(each._1._1 + each._1._2, each._1._1, each._1._2,
            each._2)).saveToEs("userrest/userrest")

    //commentNum
    val commentDS = sparkSession.read.jdbc(propMysqlBusiness2.getProperty("url"),
    "customer_share", propMysqlBusiness2)
    commentDS.filter(row => !row.isNullAt(1) && !row.isNullAt(5))
      .map(row => ((row.getInt(1).toString, row.getString(5)), 1))
      .filter(each => each._1._2.startsWith("R"))
      .rdd.reduceByKey((x, y) => x+y)
      .map(each => CommentNum(each._1._1 + each._1._2, each._1._1, each._1._2,
            each._2)).saveToEs("userrest/userrest")
  }
}
