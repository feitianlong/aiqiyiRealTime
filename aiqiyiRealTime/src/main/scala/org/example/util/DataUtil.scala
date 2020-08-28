package org.example.util

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

object DataUtil {

  // 30.187.124.132 2017-11-20 00:39:26 "GETwww/2HTTP/1.0" - 302
  // 将2017-11-20 00:39:26 转换为20171120格式，即YYYYMMDD
    val LOG_TIME_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
    val TARGET_FORMAT: FastDateFormat = FastDateFormat.getInstance("yyyyMMDD")

    def  getTime(time:String) ={
      LOG_TIME_FORMAT.parse(time).getTime
    }

  def pasreToTARGET_FORMAT(time : String)={
    TARGET_FORMAT.format(new Date(getTime(time)))
  }

  def main(args: Array[String]): Unit = {
    println(pasreToTARGET_FORMAT("2020-04-15 01:10:25"))
  }
}
