package cn.adatafun.dataprocess.VVIP

import cn.adatafun.dataprocess.conf.ESMysqlSpark
import cn.adatafun.dataprocess.rest.Rest.CollectionNum
import org.elasticsearch.spark._

object VVIP {
  def main(args: Array[String]): Unit = {
    val sparkSession = ESMysqlSpark.getSparkSession()
    val propMysqlBusiness = ESMysqlSpark.getMysqlConfBusiness()
    val collectDS = sparkSession.read.jdbc(propMysqlBusiness.getProperty("url"),
    "tb_user_collect", propMysqlBusiness)
    import sparkSession.implicits._
    //collectionNum process
    collectDS.map(row => ((row.getAs(1).toString, row.getString(3)), 1))
      .filter(each => each._1._2.startsWith("M"))
      .rdd.reduceByKey((x, y) => x + y)
      .map(each => CollectionNum(each._1._1 + each._1._2, each._1._1,
        each._1._2, each._2)).saveToEs("uservvip/uservvip")
  }
}
