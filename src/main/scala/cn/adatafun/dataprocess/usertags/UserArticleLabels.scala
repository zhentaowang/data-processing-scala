package cn.adatafun.dataprocess.usertags

import cn.adatafun.dataprocess.conf.ESMysqlSpark
import org.elasticsearch.spark._

/**
  * Created by yanggf on 2017/11/17.
  */
object UserArticleLabels {
  case class UserArticleLabels(id: String, userId: String,
                               business:Int, student: Int,
                               eleven:Int, home: Int, crowd: Int)

  def main(args: Array[String]): Unit = {
    val sparkSession = ESMysqlSpark.getSparkSession()
    val propMysqlBusiness = ESMysqlSpark.getMysqlConfBusiness()
    val propMysqlLog = ESMysqlSpark.getMysqlConfLog()
    val visitDS = sparkSession.read.jdbc(propMysqlLog.getProperty("url"),
    "tbd_visit_url", propMysqlLog)
    val lettresDS = sparkSession.read.jdbc(propMysqlBusiness.getProperty("url"),
    "tbd_belles_lettres", propMysqlBusiness)

    import sparkSession.implicits._
    val visitFilterDS = visitDS.select(visitDS("user_id"), visitDS("url"))
      .filter(row => row.getString(1)
        .startsWith("https://m.dragonpass.com.cn/institute/"))
      .map(row => (row.getString(1).split("/")(4).split("\\?")(0),
        (row.getString(0), 1)))
    val lettresFilterDS = lettresDS.select(lettresDS("id"),
      lettresDS("person_labels"))
      .map(row => (row.getString(0), row.getString(1)))
    visitFilterDS.rdd.leftOuterJoin(lettresFilterDS.rdd)
      .map(row => (row._2._1._1, row._2._2.getOrElse("hello")))
        .filter(row => row._1 != null && row._2 != null)
      .map(row =>
        (row._1,
          (
            if(row._2.contains("商旅出行")) 1 else 0,
            if(row._2.contains("学生/背包客")) 1 else 0,
            if(row._2.contains("十一")) 1 else 0,
            if(row._2.contains("家庭出行")) 1 else 0,
            if(row._2.contains("人群标签")) 1 else 0
            )
          )
      ).reduceByKey((x,y) => (x._1+y._1,
      x._2+y._2, x._3+y._3, x._4+y._4, x._5+y._5))
      .map(each => UserArticleLabels(each._1, each._1,
        each._2._1, each._2._2, each._2._3, each._2._4, each._2._5))
      .saveToEs("usertags/usertags")
  }

}
