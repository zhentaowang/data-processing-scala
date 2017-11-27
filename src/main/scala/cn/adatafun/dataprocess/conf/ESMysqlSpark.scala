package cn.adatafun.dataprocess.conf

import java.util.Properties

import org.apache.spark.{SparkConf, sql}
import org.apache.spark.sql.SparkSession

object ESMysqlSpark {
  private val props = PropertiesGetter.loadProperties()
  def getSparkSession(): SparkSession = {
    val conf = new SparkConf().setAppName(props.getProperty("appName"))
      .setMaster(props.getProperty("master"))
    conf.set("es.index.auto.create", props.getProperty("es.index.auto.create"))
    conf.set("es.resource", props.getProperty("es.resource"))
    conf.set("es.nodes", props.getProperty("es.nodes"))
    conf.set("es.port", props.getProperty("es.port"))
    conf.set("es.nodes.wan.only", props.getProperty("es.nodes.wan.only"))
    conf.set("es.mapping.id", props.getProperty("es.mapping.id"))
    conf.set("es.write.operation", props.getProperty("es.write.operation"))
    conf.set("es.write.conflict.ignore", "true")
    conf.set("es.update.retry.on.conflict", "10")
    conf.set("spark.sql.crossJoin.enabled", "true")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sparkSession = new sql.SparkSession.Builder().config(conf)
      .getOrCreate()
    sparkSession
  }

  def getSystemProp(): Properties ={
    props
  }

  def getSparkAccConf(): SparkConf = {
    val conf = new SparkConf().setAppName(props.getProperty("appName"))
      .setMaster(props.getProperty("master"))
    conf.set("es.index.auto.create", props.getProperty("es.index.auto.create"))
    conf.set("es.resource", props.getProperty("es.resource"))
    conf.set("es.nodes", props.getProperty("es.nodes"))
    conf.set("es.port", props.getProperty("es.port"))
    conf.set("es.nodes.wan.only", props.getProperty("es.nodes.wan.only"))
    conf.set("es.mapping.id", props.getProperty("es.mapping.id"))
    conf.set("es.write.operation", props.getProperty("es.write.operation"))
    conf.set("es.write.conflict.ignore", "true")
    conf.set("es.update.retry.on.conflict", "10")
    conf.set("spark.sql.crossJoin.enabled", "true")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf
  }

  def getMysqlConfBusiness(): Properties = {
    val propMysql = new Properties()
    propMysql.setProperty("url", props.getProperty("mysqlUrl3"))
    propMysql.setProperty("user", props.getProperty("user3"))
    propMysql.setProperty("password", props.getProperty("password3"))
    propMysql.setProperty("driver", "com.mysql.jdbc.Driver")
    propMysql
  }

  def getMysqlConfBusiness2(): Properties = {
    val propMysql = new Properties()
    propMysql.setProperty("url", props.getProperty("mysqlUrl"))
    propMysql.setProperty("user", props.getProperty("user"))
    propMysql.setProperty("password", props.getProperty("password"))
    propMysql.setProperty("driver", "com.mysql.jdbc.Driver")
    propMysql
  }

  def getMysqlConfLog(): Properties = {
    val propMysql = new Properties()
    propMysql.setProperty("url", props.getProperty("mysqlUrl2"))
    propMysql.setProperty("user", props.getProperty("user2"))
    propMysql.setProperty("password", props.getProperty("password2"))
    propMysql.setProperty("driver", "com.mysql.jdbc.Driver")
    propMysql
  }
}
