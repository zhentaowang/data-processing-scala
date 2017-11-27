package cn.adatafun.dataprocess.usertags

import cn.adatafun.dataprocess.conf.ESMysqlSpark

import org.elasticsearch.spark._


/**
  * Created by yanggf on 2017/11/17.
  */
object UserRestaurantPreferences {
  case class UserRestaurantPreferences(id: String, userId: String,
                                       restaurantPreferences: String)
  def main(args: Array[String]): Unit ={
    val sparkSession = ESMysqlSpark.getSparkSession()
    val propMysqlBusiness = ESMysqlSpark.getMysqlConfBusiness()
    val restOrderDS = sparkSession.read.jdbc(propMysqlBusiness.getProperty("url"),
    "restaurant_order_detail2", propMysqlBusiness)
      .select("fd_code","fd_restaurant_code", "conid", "fd_class")
    val tbOrderDS = sparkSession.read.jdbc(propMysqlBusiness.getProperty("url"),
    "tb_order", propMysqlBusiness)
      .select("id", "order_no", "user_id")
    val classDS = sparkSession.read.jdbc(propMysqlBusiness.getProperty("url"),
    "restaurant_class", propMysqlBusiness)

    import sparkSession.implicits._
    val restOrderClassDS = restOrderDS.join(tbOrderDS, restOrderDS("fd_code").
      equalTo(tbOrderDS("order_no")), "left_outer")
    val allDS = restOrderClassDS.join(classDS, restOrderDS("fd_class")
      .equalTo(classDS("fd_class")), "left_outer").filter("user_id is not null")
    allDS.select(restOrderClassDS("user_id"), allDS("fd_cls"))
      .filter(row => !row.isNullAt(0) && !row.isNullAt(1))
      .map(row => (row.getAs(0).toString, (row.getString(1).toDouble, 1)))
      .rdd.reduceByKey((x,y) => (x._1+y._1, x._2+y._2))
      .map(each => new UserRestaurantPreferences(each._1, each._1,
        if(each._2._1/each._2._2 >= 0.5) "中餐" else "西餐"))
      .saveToEs("usertags/usertags")
  }

}
