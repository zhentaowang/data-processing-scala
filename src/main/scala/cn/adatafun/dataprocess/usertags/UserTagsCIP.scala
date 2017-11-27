package cn.adatafun.dataprocess.usertags

import cn.adatafun.dataprocess.conf.ESMysqlSpark
import org.elasticsearch.spark._
import UserTagsOne.parseJson

object UserTagsCIP {
  case class Amount(id: String, userId: String,
                    val CIPAverageOrderAmount:Int)
  case class CIPBrowseNum(id: String, userId: String,
                          CIPBrowseNum: Int)
  case class CIPConsumptionNum(id: String, userId: String,
                               CIPConsumptionNum: Int)
  case class CIPUsageCounter(id: String, userId: String,
                             CIPUsageCounter: Int)

  def main(args: Array[String]): Unit = {
    val sparkSession = ESMysqlSpark.getSparkSession()
    val propMysql = ESMysqlSpark.getMysqlConfBusiness()
    val orderDS = sparkSession.read.jdbc(propMysql.getProperty("url"),
      "tb_order_cip", propMysql)
    val tbOrderDS = sparkSession.read.jdbc(propMysql.getProperty("url"),
      "tb_order", propMysql)
    val togetherDS = orderDS.join(tbOrderDS,orderDS("order_no").equalTo(tbOrderDS("order_no")),
      "left_outer")
    import sparkSession.implicits._
    //averageOrderAmount
    togetherDS.select(tbOrderDS("user_id"), orderDS("pro_code"), tbOrderDS("actual_amount"))
      .filter(row => !row.isNullAt(0) && !row.isNullAt(1) && !row.isNullAt(2))
      .map(row => (row.getLong(0).toString, (row.getDouble(2),1)))
      .rdd.reduceByKey((x,y) => (x._1+x._1, y._2+x._2))
      .map(each => Amount(each._1,
        each._1,  each._2._1.toInt/each._2._2.toInt))
      .saveToEs("usertags/usertags")

    //CIPbrowseNum
    val propMysqlLog = ESMysqlSpark.getMysqlConfLog()
    val urlDS = sparkSession.read.jdbc(propMysqlLog.getProperty("url"),
      "tbd_url_element", propMysqlLog)
    urlDS.filter("url = '/VirtualCard-v6/orderCip/listCip' or " +
      "url = '/VirtualCard-v6/trafficsite/list/cip' "
      + "or url = '/VirtualCard-v5/orderCip/getCips' or " +
      "url = '/VirtualCard-v5/orderCip/getCips'")
      .select(urlDS("param"))
      .map(row => parseJson(row.getString(0)))
      .filter(each => !each.equals(""))
      .map(x => (x, 1))
      .rdd.reduceByKey((x, y) => x+y)
      .map(each =>
        CIPBrowseNum(each._1, each._1,
          each._2)).saveToEs("usertags/usertags")

    //CIPConsumptionNum
    togetherDS.select(tbOrderDS("user_id"), orderDS("pro_code"), tbOrderDS("actual_amount"))
      .filter(row => !row.isNullAt(0) && !row.isNullAt(1) && !row.isNullAt(2))
      .map(row => (row.getLong(0).toString, 1))
      .rdd.reduceByKey((x,y) => x+y)
      .map(each => CIPConsumptionNum(each._1,
        each._1,  each._2))
      .saveToEs("usertags/usertags")

    //CIPUsageCounter
    val usageDS = sparkSession.read.jdbc(propMysql.getProperty("url"),
      "tb_order_cip_record", propMysql)
    val tbOrderRenamedDS = tbOrderDS.withColumnRenamed("order_no", "order_name1")
    val orderTbDS = orderDS.join(tbOrderRenamedDS, orderDS("order_no").
      equalTo(tbOrderRenamedDS("order_name1")), "left_outer")
    val counterDS = usageDS.join(orderTbDS, usageDS("order_no").
      equalTo(orderTbDS("order_no")), "left_outer")
    counterDS.select(tbOrderDS("user_id"), orderDS("pro_code"))
      .na.drop().map(row => (row.getAs(0).toString, 1))
      .rdd.reduceByKey((x, y) => x+y)
      .map(each => CIPUsageCounter(each._1, each._1, each._2))
      .saveToEs("usertags/usertags")
  }
}
