package cn.adatafun.dataprocess.restaurant

import cn.adatafun.dataprocess.conf.ESMysqlSpark
import org.apache.spark.sql.Row
import org.elasticsearch.spark.sql.EsSparkSQL

object RestaurantPriority {
  case class RestaurantPriority (id:String,restaurantCode:String,priority:Int)
  def main(args: Array[String]): Unit = {
    val sparkSession = ESMysqlSpark.getSparkSession()
    val propMysql = ESMysqlSpark.getMysqlConfBusiness()
    val tableName = "itd_restaurant"
    val urlDS = sparkSession.read.jdbc(propMysql.getProperty("url"),
      tableName, propMysql)
    import sparkSession.implicits._
    def parseRestaurantPriority(row : Row): RestaurantPriority = {
      val restaurantCode = row.getString(0)
      if (!row.isNullAt(3) && row.getLong(3) == 1)
        RestaurantPriority(restaurantCode,restaurantCode,3)
      else if (!row.isNullAt(2) && row.getLong(2) == 1)
        RestaurantPriority(restaurantCode,restaurantCode,2)
      else if (!row.isNullAt(4) && row.getLong(4) == 1)
        RestaurantPriority(restaurantCode,restaurantCode,1)
      else
        RestaurantPriority(restaurantCode,restaurantCode,0)
    }
    val resultDS = urlDS.select(urlDS("fd_code"),
      urlDS("fd_lg"), urlDS("fd_iscoupon"),
      urlDS("fd_isflashsale"), urlDS("fd_settlementdiscount"))
      .filter(row => row.getString(1) == "zh-cn")
      .map(parseRestaurantPriority)
    EsSparkSQL.saveToEs(resultDS, "restaurant/restaurant")
  }
}
