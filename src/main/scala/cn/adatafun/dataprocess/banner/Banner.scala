package cn.adatafun.dataprocess.banner

import java.util.Date

import cn.adatafun.dataprocess.conf.ESMysqlSpark
import org.elasticsearch.spark._

object Banner {
  def getBannerId(value: String):String = {
    val banners: Map[String, String] = Map(
      "51683051A64B2231E0530264A8C0D3BE"->"https://activity.dragonpass.com.cn/highrail/equity/membershiplistnew",
      "51D11245FC325F6EE0530264A8C07548"->"https://activity.dragonpass.com.cn/newestlounge",
      "597CFC5FA85839E7E0530264A8C0F0E6"->"http://mp.weixin.qq.com/s/jFI9UNpQhhNrZgOIj15vSQ",
      "455AC8D410E86594E0530264A8C05B8E"->"http://img.dragonpass.com.cn/activity/GD_bank",
      "59FAB1F5A2B059D2E0530264A8C080A5"->"https://img.dragonpass.com.cn/activity/easternLounge",
      "56741F60A813112FE0530264A8C0E847"->"http://activity.dragonpass.com.cn/shop/summer/membership/from",
      "5A5EC79C48D3224DE0530264A8C035D6"->"http://img.dragonpass.com.cn/activity/nDay/",
      "59EFAD543BC50CC9E0530264A8C021D5"->"http://mp.weixin.qq.com/s/KQSQgR6GBhxrOnJJDupYjQ",
      "5A72B21112676150E0530264A8C0293E"->"https://activity.dragonpass.com.cn/aacar/index/v3",
      "4389DF2BEBB07C22E0530264A8C0E0A3"->"https://activity.dragonpass.com.cn/lucky/airmealticket/index",
      "5766C676F7701850E0530164A8C09A5C"->"http://img.dragonpass.com.cn/activity/HongKongActive/",
      "57690E6FA0C93694E0530264A8C03810"->"https://activity.dragonpass.com.cn/wuhan/vvip/index"
    )
    var bannerId:String = ""
    banners.foreach(each => {
      if(value.startsWith(each._2))
        bannerId = each._1
    })
    bannerId
  }
  case class Banner(id:String, userId:String, restaurantCode:String, browseNum:Int)
  def main(args: Array[String]):Unit = {
    val yestoday = new Date().getTime() - 86400000L
    val sparkSession = ESMysqlSpark.getSparkSession()
    val propMysql = ESMysqlSpark.getMysqlConfLog()
    val tableName = "tbd_visit_url"
    val urlDS = sparkSession.read.jdbc(propMysql.getProperty("url"),
      tableName, propMysql)
    import sparkSession.implicits._

    //browseNum process
    urlDS.select(urlDS("user_id"), urlDS("url"))
      .filter(row => !getBannerId(row.getString(1)).equals(""))
      .map(row => ((row.getString(0), getBannerId(row.getString(1))), 1))
      .rdd.reduceByKey((x, y) => x+y)
      .map(each => Banner(each._1._1 + each._1._2,
        each._1._1, each._1._2, each._2)).saveToEs("userbanner/userbanner")
  }
}
