package cn.adatafun.dataprocess.util

import java.util.{Calendar, Date, TimeZone}

/**
  * Created by yanggf on 2017/11/20.
  */
object TodayGet {
  def getToday(): Long={
    val tz = "GMT+8"
    val curTimeZone = TimeZone.getTimeZone(tz)
    val cal = Calendar.getInstance(curTimeZone)
    val now = new Date()
    cal.setTimeInMillis(now.getTime)
    cal.set(Calendar.HOUR_OF_DAY, 0)
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.SECOND, 0)
    cal.set(Calendar.MILLISECOND, 0)
    cal.getTimeInMillis
  }

  def isYestoday(time: Long): Boolean = {
    if(time >= getToday() - 86400000L && time <= getToday())
      true
    else
      false
  }

}
