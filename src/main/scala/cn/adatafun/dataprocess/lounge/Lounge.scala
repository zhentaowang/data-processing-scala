package cn.adatafun.dataprocess.lounge

import cn.adatafun.dataprocess.conf.ESMysqlSpark
import org.elasticsearch.spark._
import spray.json.JsonParser

object Lounge {
  case class Consumption(id:String, userId:String, restaurantCode:String,
                         averageOrderAmount: Double)
  case class ConsumptionNum(id:String, userId:String, restaurantCode:String,
                            consumptionNum: Int)
  case class PeopleConsumption(id:String, userId:String, restaurantCode:String,
                               peopleConsumption: Int)
  case class UsageCounter(id:String, userId:String, restaurantCode:String,
                          usageCounter: Int)
  case class BrowseNum(id:String, userId:String, restaurantCode:String,
                        browseNum: Int)
  case class CollectionNum(id:String, userId:String, restaurantCode:String,
                           collectionNum: Int)
  case class CommentNum(id:String, userId:String, restaurantCode:String,
                        commentNum: Int)

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
    val propMysql = ESMysqlSpark.getMysqlConfBusiness()
    val orderDS = sparkSession.read.jdbc(propMysql.getProperty("url"),
      "tblconsumerecord", propMysql)
    val bindDS = sparkSession.read.jdbc(propMysql.getProperty("url"),
    "tb_bindrecord", propMysql)
    import sparkSession.implicits._
    val joinDS = orderDS.join(bindDS,
      orderDS("dragoncode").equalTo(bindDS("dragoncode")),"left_outer")
    //averageOrderAmount process
    joinDS.select(joinDS("user_id"), joinDS("loungecode"), joinDS("point"))
      .coalesce(8).filter(row => !row.isNullAt(0) && !row.isNullAt(1))
      .map(row => ((row.getLong(0).toString, row.getString(1)), (row.getLong(2), 1)))
      .rdd.reduceByKey((x,y) => (x._1+y._1, x._2+y._2))
        .map(each => ((each._1._1, each._1._2), each._2._1/each._2._2.toDouble))
      .map(each => Consumption(each._1._1 + each._1._2,
        each._1._1, each._1._2, each._2)).saveToEs("userlounge/userlounge")

    //consumptionNum process
    val servDS = sparkSession.read.jdbc(propMysql.getProperty("url"),
      "tb_order_loungeserv", propMysql)
    val servOrderDS = servDS.join(bindDS,
      servDS("dragoncode").equalTo(bindDS("dragoncode")), "left_outer")
    servOrderDS.select(servOrderDS("user_id"), servOrderDS("lounge_code"))
      .filter(row => !row.isNullAt(0) && !row.isNullAt(1))
        .rdd.flatMap(row => {
        val userId = row.getLong(0)
        val userIdList = List(row.getLong(0))
        val codeList = row.getString(1).split(",").toList
        val key:List[(Long, String)] = userIdList.zipAll(codeList,userId,"")
        key //这里需要分两步操作将数据映射为（（x,y）,1）要不然需要处理seriazable对象
        }).map(x => (x, 1)).reduceByKey((x, y) => x+y)
          .map(each => ConsumptionNum(each._1._1.toString + each._1._2.toString,
            each._1._1.toString, each._1._2.toString, each._2))
          .saveToEs("userlounge/userlounge")

    //peopleConsumption process
    joinDS.select(joinDS("user_id"), joinDS("loungecode"), joinDS("personcount"))
      .coalesce(8).filter(row => !row.isNullAt(0) && !row.isNullAt(1) &&
    row.getString(1).startsWith("N"))
      .map(row => ((row.getLong(0).toString, row.getString(1)), row.getLong(2)))
      .rdd.reduceByKey((x,y) => x+y)
      .map(each => PeopleConsumption(each._1._1 + each._1._2,
        each._1._1, each._1._2, each._2.toInt))
      .saveToEs("userlounge/userlounge")

    //browseNum process
    val logMysqlConf = ESMysqlSpark.getMysqlConfLog()
    val browseDS = sparkSession.read.jdbc(logMysqlConf.getProperty("url"),
    "tbd_url_element", logMysqlConf)
    browseDS.filter("url = '/VirtualCard-en/lounge/detail' or " +
      "url = '/VirtualCard-v5/lounge/detail' " +
      "or url = '/VirtualCard-v6/lounge/detail' " +
      "or url = '/VirtualCard-v6/lounge/buttonShow'")
      .select(browseDS("param")).coalesce(8)
      .map(row => parseJson(row.getString(0))).filter(each => !each._1.equals("") && !each._2.equals(""))
      .map(x => (x,1)).rdd.reduceByKey((x,y) => x+y)
      .map(each => BrowseNum(each._1._1 + each._1._2,
        each._1._1, each._1._2, each._2))
      .saveToEs("userlounge/userlounge")

    //collectionNum process
    val collectDS = sparkSession.read.jdbc(propMysql.getProperty("url"),
    "tb_user_collect", propMysql)
    collectDS.map(row => ((row.getLong(1).toString, row.getString(3)), 1))
      .filter(row => row._1._2.startsWith("N"))
      .rdd.reduceByKey((x, y) => x+y).map(each => CollectionNum(
      each._1._1 + each._1._2, each._1._1, each._1._2, each._2
    )).saveToEs("userlounge/userlounge")

    //commentNum process
    val commentDS = sparkSession.read.jdbc(propMysql.getProperty("url"),
    "tb_customer_share", propMysql)
    commentDS.filter("user_id is not null and code is not null and score is not null and score > 2")
      .map(row => ((row.getLong(6).toString, row.getString(17)), 1))
      .filter(each => each._1._2.startsWith("N"))
      .rdd.reduceByKey((x, y) => x+y).map(each => CommentNum(
      each._1._1 + each._1._2, each._1._1, each._1._2, each._2
    )).saveToEs("userlounge/userlounge")

    //usageCounter process
    joinDS.select(joinDS("user_id"), joinDS("loungecode"), joinDS("point"))
      .coalesce(8).filter(row => !row.isNullAt(0) && !row.isNullAt(1))
      .map(row => ((row.getLong(0).toString, row.getString(1)), 1))
      .rdd.reduceByKey((x,y) => x+y)
      .map(each => UsageCounter(each._1._1 + each._1._2,
        each._1._1, each._1._2, each._2)).saveToEs("userlounge/userlounge")
  }
}
