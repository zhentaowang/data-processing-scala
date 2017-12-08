package cn.adatafun.dataprocess.caculate

import cn.adatafun.dataprocess.conf.ESMysqlSpark

/**
  * Created by yanggf on 2017/12/5.
  */
object UserCounter {
  def main(args: Array[String]): Unit ={
    val sparkSession = ESMysqlSpark.getSparkSession()

    /*
    val propMysql = ESMysqlSpark.getMysqlConfBusiness()
    val tbOrderDS = sparkSession.read.jdbc(propMysql.getProperty("url"),
      "tb_order", propMysql)
    val orderNum = tbOrderDS.select("user_id").rdd.count()
    println(orderNum)
    val userDS = sparkSession.read.jdbc(propMysql.getProperty("url"),
      "tb_user", propMysql)
    val userNum = userDS.select("id").distinct().count()
    println(userNum)

    val collectDS = sparkSession.read.jdbc(propMysql.getProperty("url"),
      "tb_user_collect", propMysql)
    val collectNum = userDS.select("id").distinct().count()
    println(collectNum)
*/
    val propMysqlLog = ESMysqlSpark.getMysqlConfLog()
    val logDS = sparkSession.read.jdbc(propMysqlLog.getProperty("url"),
    "tbd_app_opera_path", propMysqlLog)
    println(logDS.select("id").count())
  }

}
