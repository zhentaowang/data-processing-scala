package cn.adatafun.dataprocess.accumulate

import cn.adatafun.dataprocess.CIP.CIP.CIPUsageCounter
import cn.adatafun.dataprocess.conf.ESMysqlSpark
import org.apache.spark.sql.SparkSession
import cn.adatafun.dataprocess.util.TodayGet.isYestoday
import org.elasticsearch.spark._

/**
  * Created by yanggf on 2017/11/21.
  */
object CIPAcc {
  def accUsageCounter(): Unit ={
    val conf = ESMysqlSpark.getSparkAccConf()
    val prop = ESMysqlSpark.getSystemProp()
    val updateScript = prop.getProperty("updateScript").replaceAll("fieldName", "usageCounter")
    conf.set("es.update.script", updateScript)
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val propMysql = ESMysqlSpark.getMysqlConfBusiness()
    val usageDS = sparkSession.read.jdbc(propMysql.getProperty("url"),
      "tb_order_cip_record",propMysql)
    val tbOrderDS = sparkSession.read.jdbc(propMysql.getProperty("url"),
      "tb_order", propMysql)
    val orderDS = sparkSession.read.jdbc(propMysql.getProperty("url"),
      "tb_order_cip", propMysql)
    val renamedDS = tbOrderDS.withColumnRenamed("order_no", "order_no1")
    val orderTbDS = orderDS.join(renamedDS,
      orderDS("order_no").equalTo(renamedDS("order_no1")),"left_outer")
    val counterDS = usageDS.join(orderTbDS,
      orderDS("order_no").equalTo(orderTbDS("order_no")),"left_outer")
    import sparkSession.implicits._

    //usageCounter accumulative process
    counterDS.select(tbOrderDS("user_id"), orderDS("pro_code"), usageDS("create_time"))
      .filter(row => !row.isNullAt(0) && !row.isNullAt(1)
      && isYestoday(row.getDate(2).getTime))
      .map(row => ((row.getLong(0).toString, row.getString(1)), 1))
      .rdd
      .map(each => CIPUsageCounter(each._1._1 + each._1._2,
        each._1._1, each._1._2, each._2)).coalesce(8)
//        .take(200) foreach println
      .saveToEs("usercip/usercip")
  }

  def main(args: Array[String]): Unit ={
    accUsageCounter()
  }
}
