package cn.adatafun.dataprocess.usertags

import cn.adatafun.dataprocess.conf.ESMysqlSpark
import spray.json.JsonParser
import org.elasticsearch.spark._

/**
  * Created by yanggf on 2017/11/17.
  */
object UserTagsRest {
  case class AverageOrderAmount(id:String, userId:String,
                                restAverageOrderAmount:Double)
  case class ConsumptionNum(id:String, userId:String,
                            restConsumptionNum:Int)
  case class MultitimeConsumption(id:String, userId:String,
                                  restMultitimeConsumption:Boolean)
  case class UsageCounter(id:String, userId:String,
                          restUsageCounter:Int)
  case class BrowseNum(id:String, userId:String,
                       restBrowseNum:Int)
  case class CollectionNum(id:String, userId:String,
                           restCollectionNum:Int)
  case class CommentNum(id:String, userId:String,
                        restCommentNum:Int)

  def parseJson(jsonStr: String):  String = {
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
      .map(row => (row.getLong(0).toString, (row.getDouble(2),1)))
      .rdd.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2)).map(each =>
      AverageOrderAmount(each._1, each._1,
        each._2._1/each._2._2)).saveToEs("usertags/usertags")

    //consumptionNum
    restOrderJoinRestDS.select(tbOrderDS("user_id"), restOrderDS("fd_restaurant_code"),
      restOrderDS("xfprice")).filter(row => !row.isNullAt(0))
      .map(row => (row.getLong(0).toString, 1))
      .rdd.reduceByKey((x,y) => x + y).map(each =>
      ConsumptionNum(each._1, each._1, each._2
        )).saveToEs("usertags/usertags")

    //multitimeConsumption
    restOrderJoinRestDS.select(tbOrderDS("user_id"), restOrderDS("fd_restaurant_code"),
      restOrderDS("xfprice")).filter(row => !row.isNullAt(0))
      .map(row => (row.getLong(0).toString,1))
      .rdd.reduceByKey((x,y) => x + y).map(each =>
      (each._1, if(each._2>1)true else false))
      .map(each =>
        MultitimeConsumption(each._1, each._1,
          each._2)).saveToEs("usertags/usertags")

    //usageCounter
    val usageDS = sparkSession.read.jdbc(propMysqlBusiness.getProperty("url"),
      "td_restaurant_order", propMysqlBusiness)
    val usageTbOrderDS = usageDS.join(tbOrderDS,
      usageDS("fd_code").equalTo(tbOrderDS("order_no")), "left_outer")
    usageTbOrderDS.select(usageTbOrderDS("user_id"),
      usageTbOrderDS("fd_restaurant_code"))
      .filter(row => !row.isNullAt(0))
      .map(row => (row.getLong(0).toString, 1))
      .rdd.reduceByKey((x,y) => x + y)
      .map(each =>
        UsageCounter(each._1, each._1,
          each._2)).saveToEs("usertags/usertags")

    //browseNum
    val propMysqlLog = ESMysqlSpark.getMysqlConfLog()
    val urlDS = sparkSession.read.jdbc(propMysqlLog.getProperty("url"),
      "tbd_url_element", propMysqlLog)
    urlDS.filter("url = '/VirtualCard-en/restaurant/detail' or url = '/VirtualCard-v5/restaurant/detail' " +
      "or url = '/VirtualCard-v6/restaurant/detail'")
      .select(urlDS("param"))
      .map(row => parseJson(row.getString(0)))
      .filter(each => !each.equals(""))
      .map(x => (x, 1))
      .rdd.reduceByKey((x, y) => x+y)
      .map(each =>
        BrowseNum(each._1, each._1,
          each._2)).saveToEs("usertags/usertags")

    //collectionNum
    val collectDS = sparkSession.read.jdbc(propMysqlBusiness.getProperty("url"),
      "tb_user_collect", propMysqlBusiness)
    collectDS.map(row => ((row.getLong(1).toString, row.getString(3)), 1))
      .filter(each => each._1._2.startsWith("R"))
      .rdd.map(each => (each._1._1, 1)).reduceByKey((x, y) => x+y)
      .map(each => CollectionNum(each._1, each._1,
        each._2)).saveToEs("usertags/usertags")

    //commentNum
    val commentDS = sparkSession.read.jdbc(propMysqlBusiness2.getProperty("url"),
      "customer_share", propMysqlBusiness2)
    commentDS.filter(row => !row.isNullAt(1) && !row.isNullAt(5))
      .map(row => ((row.getInt(1).toString, row.getString(5)), 1))
      .filter(each => each._1._2.startsWith("R"))
      .rdd.map(each => (each._1._1, 1)).reduceByKey((x, y) => x+y)
      .map(each => CommentNum(each._1, each._1,
        each._2)).saveToEs("usertags/usertags")
  }

}
