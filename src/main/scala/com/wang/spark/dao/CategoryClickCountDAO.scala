package com.wang.spark.dao

import com.wang.spark.domain.CategoryClickCount
import com.wang.spark.utils.HBaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

object CategoryClickCountDAO {

    val tableName = "category_clickcount"
    val cf = "info"
    val qualifer = "click_count"




  //保存数据
  def save(list: ListBuffer[CategoryClickCount]): Unit = {
    val table = HBaseUtils.getInstatnce.getHtable(tableName)
    for (els <- list) {
      table.incrementColumnValue(Bytes.toBytes(els.catgoryId), Bytes.toBytes(cf), Bytes.toBytes(qualifer), els.clickCount)
    }
  }

  def count(day_category: String): Long = {
    val table = HBaseUtils.getInstatnce.getHtable(tableName)
    val get = new Get(Bytes.toBytes(day_category))
    val value = table.get(get).getValue(Bytes.toBytes(cf), Bytes.toBytes(qualifer))
    if (value == null) {
      0L
    } else {
      Bytes.toLong(value)
    }
  }

  def main(args: Array[String]): Unit = {
    //    val list=new ListBuffer[CategoryClickCount]
    //    list.append(CategoryClickCount("20171122_9",300))
    //    list.append(CategoryClickCount("20171122_8",100))
    //    list.append(CategoryClickCount("20171122_7",200))
    //    save(list)

    //    print(count("20171122_9")+"----------"+count("20171122_8"))
    print(count("20181031_1"))
  }
}