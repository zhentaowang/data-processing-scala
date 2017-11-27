package cn.adatafun.dataprocess.CIP

import cn.adatafun.dataprocess.conf.ESMysqlSpark
import org.elasticsearch.spark._

object CIP {
  case class CIPAverageOrderAmount(id:String,
                                   userId:String,restaurantCode:String,
                                   averageOrderAmount: Double, consumptionNum:Int)
  case class CIPUsageCounter(id:String, userId:String,
                         restaurantCode:String, usageCounter:Int)
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
    //averageOrderAmount process
    togetherDS.select(tbOrderDS("user_id"), orderDS("pro_code"), tbOrderDS("actual_amount"))
      .filter(row => !row.isNullAt(0) && !row.isNullAt(1) && !row.isNullAt(2))
      .map(row => ((row.getLong(0).toString, row.getString(1)), (row.getDouble(2),1)))
      .rdd.reduceByKey((x,y) => (x._1+x._1, y._2+x._2))
      .map(each => ((each._1._1,each._1._2), (each._2._1/each._2._2, each._2._2)))
      .map(each => CIPAverageOrderAmount(each._1._1 + each._1._2,
        each._1._1, each._1._2, each._2._1, each._2._2))
        .coalesce(8).saveToEs("usercip/usercip")

    val usageDS = sparkSession.read.jdbc(propMysql.getProperty("url"),
    "tb_order_cip_record",propMysql)
    val renamedDS = tbOrderDS.withColumnRenamed("order_no", "order_no1")
    val orderTbDS = orderDS.join(renamedDS,
      orderDS("order_no").equalTo(renamedDS("order_no1")),"left_outer")
    val counterDS = usageDS.join(orderTbDS,
      orderDS("order_no").equalTo(orderTbDS("order_no")),"left_outer")
    //usageCounter process
    counterDS.select(tbOrderDS("user_id"), orderDS("pro_code"))
      .filter(row => !row.isNullAt(0) && !row.isNullAt(1))
      .map(row => ((row.getLong(0).toString, row.getString(1)), 1))
      .rdd.reduceByKey((x,y) => x+y)
      .map(each => CIPUsageCounter(each._1._1 + each._1._2,
        each._1._1, each._1._2, each._2)).coalesce(8).saveToEs("usercip/usercip")

  }
}
