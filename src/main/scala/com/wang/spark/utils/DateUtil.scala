package com.wang.spark.utils

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

/**
 * 时间的格式转换
 * 2017-12-27 00:36:56 => 20171227
 */
object DateUtil {
    val YYYYMMDDHHMMSS_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
    val TAG_FORMAT =  FastDateFormat.getInstance("yyyyMMdd")

  //当前时间转换成时间戳
  def getTime(time:String) = {
    YYYYMMDDHHMMSS_FORMAT.parse(time).getTime
  }

  def parseToMin(time:String) = {
    TAG_FORMAT.format(new Date(getTime(time)))
  }

  //测试
  def main(args: Array[String]): Unit = {
      print(parseToMin("2019-12-27 09:56:20"))
    //20191227
  }
}
