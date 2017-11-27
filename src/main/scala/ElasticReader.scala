package cn.adatafun.dataprocess.conf

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

/**
  * Created by yanggf on 2017/11/9.
  */
object ElasticReader {
  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setMaster("local").setAppName("elastic")
      .set("es.nodes", "127.0.0.1")
      .set("es.port", "9200")
      .set("es.net.http.auth.user", "elastic")
      .set("es.net.http.auth.pass", "changeme")
      .set("es.mapping.date.rich", "false")
    val sc = new SparkContext(conf)
    val query = """{"query":{"match_all":{}}}"""
    val esRdd = sc.esRDD("user/flight", query)
    import org.apache.spark.sql.SparkSession
    val sparkSession = SparkSession.builder().config(conf)
      .getOrCreate()

    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types.{StringType, _}
    import sparkSession.implicits._

    val columns = esRdd.take(1).map{case (a,b) => b.keySet}
    val fields = columns.flatMap(colnameSet => colnameSet.toList)
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)
    val rowRdd = esRdd.map{case(a,b) => Row.fromSeq(b.values.toList.map(a => a.toString()))}
    val userDF = sparkSession.createDataFrame(rowRdd, schema)
    userDF.createOrReplaceTempView("user")
    val results = sparkSession.sql("select userId, isStop from user")
    results.map(attributes => "userID:" + attributes(0) +
    "|" + "isStop:" + attributes(1)).show(false)
    }
}
