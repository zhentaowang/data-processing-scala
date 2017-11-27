package cn.adatafun.dataprocess.util

import spray.json.JsonParser

object ParseJson {
  def parseJson(jsonStr: String): (String, String) = {
    val json = JsonParser(jsonStr).asJsObject
    try{
      (json.getFields("userId")(0).toString().replace("\"",""),
        json.getFields("code")(0).toString().replace("\"",""))
    } catch {
      case ex: java.lang.IndexOutOfBoundsException =>{
        ("","")
      }
    }
  }
}
