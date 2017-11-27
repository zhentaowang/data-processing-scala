package cn.adatafun.dataprocess.accumulate

import cn.adatafun.dataprocess.conf.ESMysqlSpark
import cn.adatafun.dataprocess.rest.Rest.CollectionNum
import cn.adatafun.dataprocess.util.TodayGet._
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark._

/**
  * Created by yanggf on 2017/11/22.
  */
object VVIPAcc {
  def accCollectionNum(): Unit ={
    val conf = ESMysqlSpark.getSparkAccConf()
    val prop = ESMysqlSpark.getSystemProp()
    val updateScript = prop.getProperty("updateScript").replaceAll("fieldName", "collectionNum")
    conf.set("es.update.script", updateScript)
    val propMysqlBusiness = ESMysqlSpark.getMysqlConfBusiness()
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val collectDS = sparkSession.read.jdbc(propMysqlBusiness.getProperty("url"),
      "tb_user_collect", propMysqlBusiness)
    import sparkSession.implicits._
    //collectionNum process
    collectDS.coalesce(8)
      .filter(row => isYestoday(row.getTimestamp(4).getTime))
      .map(row => ((row.getAs(1).toString, row.getString(3)), 1))
      .filter(each => each._1._2.startsWith("M"))
      .rdd
      .map(each => CollectionNum(each._1._1 + each._1._2, each._1._1,
        each._1._2, each._2)).saveToEs("uservvip/uservvip")
  }

  def main(): Unit ={
    accCollectionNum()
  }

}
