package cn.adatafun.dataprocess.usertags

import cn.adatafun.dataprocess.conf.ESMysqlSpark
import org.elasticsearch.spark._
import spray.json.JsonParser

/**
  * Created by yanggf on 2017/11/17.
  */
object UserTagsVVIP {
  case class VVIPAverageOrderAmount(id: String, userId: String,
                                    VVIPAverageOrderAmount: Double)
  case class VVIPBrowseNum(id: String, userId: String,
                           VVIPBrowseNum: Int)
  case class VVIPCollectionNum(id: String, userId: String,
                               VVIPCollectionNum: Int)
  case class VVIPConsumptionNum(id: String, userId: String,
                                VVIPConsumptionNum: Int)

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
    val vvipOrderDS = sparkSession.read.jdbc(propMysqlBusiness.getProperty("url"),
    "tb_order_vvip", propMysqlBusiness)
    val tbOrderDS = sparkSession.read.jdbc(propMysqlBusiness.getProperty("url"),
    "tb_order", propMysqlBusiness)
    val joinDS = vvipOrderDS.join(tbOrderDS, vvipOrderDS("order_no")
      .equalTo(tbOrderDS("order_no")), "left_outer")

    import sparkSession.implicits._
    //vvipAverageOrderAmount
    joinDS.select(joinDS("user_id"), joinDS("actual_amount")).na.drop()
      .map(row => (row.getAs(0).toString, (row.getDouble(1), 1)))
      .rdd.reduceByKey((x,y) =>(x._1 + y._1, x._2 + y._2))
      .filter(each => each._2._2 != 0)
      .map(each => VVIPAverageOrderAmount(each._1, each._1,
        each._2._1/each._2._2)).saveToEs("usertags/usertags")

    //vvipBrowseNum
    val propMysqlLog = ESMysqlSpark.getMysqlConfLog()
    val vipUrlDS = sparkSession.read.jdbc(propMysqlLog.getProperty("url"),
    "tbd_url_element", propMysqlLog)
    vipUrlDS.filter("url = '/VirtualCard-v6/share/count/vvip' or " +
      "url = '/VirtualCard-v6/vvip/index' " +
      "or url = '/VirtualCard-v5/vvip/index' or " +
      "url = '/VirtualCard-v5/vvip/setTypes' or " +
      "url = '/VirtualCard-v5/share/getVvipShare'")
      .select("param")
      .map(row => parseJson(row.getString(0)))
      .filter(row => !row.equals(""))
      .map(x => (x, 1)).rdd.reduceByKey((x,y) => x+y)
      .map(each => VVIPBrowseNum(each._1, each._1, each._2))
      .saveToEs("usertags/usertags")

    //vvipCollectionNum
    val collectDS = sparkSession.read.jdbc(propMysqlBusiness.getProperty("url"),
      "tb_user_collect", propMysqlBusiness)
    collectDS.map(row => ((row.getAs(1).toString, row.getString(3)), 1))
      .filter(each => each._1._2.startsWith("M"))
      .rdd.map(row => (row._1._1, 1)).reduceByKey((x, y) => x + y)
      .map(each => VVIPCollectionNum(each._1, each._1,
        each._2)).saveToEs("usertags/usertags")

    //vvipconsumptionNum
    joinDS.select(joinDS("user_id"), joinDS("actual_amount")).na.drop()
      .map(row => (row.getAs(0).toString, (row.getDouble(1), 1)))
      .rdd.reduceByKey((x,y) =>(x._1 + y._1, x._2 + y._2))
      .filter(each => each._2._2 != 0)
      .map(each => VVIPConsumptionNum(each._1, each._1,
        each._2._2)).saveToEs("usertags/usertags")
  }

}
