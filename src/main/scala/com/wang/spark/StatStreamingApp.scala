package com.wang.spark
import com.wang.spark.dao.{CategoryClickCountDAO, CategorySearchCountDao}
import com.wang.spark.domain.{CategoryClickCount, CategorySerachCount, ClickLog}
import com.wang.spark.utils.DateUtil
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
 * flume采集定时产生的日志到kafka
 * kafka 的消费者 整合是sparkstreaming
 */
object StatStreamingApp {

  def main(args: Array[String]): Unit = {
    //创建sparkStreaming配置
    val ssc = new StreamingContext("local[*]","StatStreamingApp",Seconds(5))

    //创建kafka
    val kafkaParams = Map[String,Object](
      "bootstrap.servers" -> "hadoop1:9092,hadoop2:9092,hadoop3:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("flume")
    val logs = KafkaUtils.createDirectStream(
      ssc,
      PreferConsistent,
      Subscribe[String,String](topics,kafkaParams)
    ).map(_.value())


    //192.168.52.20 2019-12-27 09:56:30 "GET /uowww/2 HTTP/1.0" -   200
    var cleanLog = logs.map(line =>{
      var infos = line.split("\t")
      var url = infos(2).split(" ")(1)
      var categoryId = 0
      if(url.startsWith("www")){
          categoryId = url.split("/")(1).toInt
      }

      ClickLog(infos(0),DateUtil.parseToMin(infos(1)),categoryId,infos(3),infos(4).toInt)
    }).filter(log=>log.categoryId!=0)


    cleanLog.print()



    //1.用户每天的点击量 保存与数据库
    cleanLog.map(log=>{
      (log.time.substring(0,8)+log.categoryId,1)
    }).reduceByKey(_+_).foreachRDD(rdd=>{
        rdd.foreachPartition(partitions =>{
          val list = new ListBuffer[CategoryClickCount]
          partitions.foreach(pair=>{
            list.append(CategoryClickCount(pair._1,pair._2))
         })
          CategoryClickCountDAO.save(list)
        })
    })

    //2.从渠道过来的每个栏目下面的流量20171122_www.baidu.com_1 100 20171122_2 (渠道) _1(类别) 100
    //category_search_count create "category_search_count","info"
    //192.168.52.20 2019-12-27 09:56:30 "GET /uowww/2 HTTP/1.0" -   200
    //https://www.sohu.com/web?qu=王一宁 302
    cleanLog.map(log=>{
      val url = log.refer.replace("//","/")
      val splits = url.split("/")
      var host = ""
      if(splits.length >2){
          host = splits(1)
      }
      (host,log.time,log.categoryId)
    }).filter(x=>x._1 != "").map(x=>{
      (x._2.substring(0,8)+"_"+x._1+"_"+x._3,1)
    }).reduceByKey(_+_).foreachRDD(rdd=>{
      rdd.foreachPartition(partions=>{
        val list = new ListBuffer[CategorySerachCount]
        partions.foreach(pairs=>{
          list.append(CategorySerachCount(pairs._1,pairs._2))
        })
        CategorySearchCountDao.save(list)
      })
    })

    ssc.start()

    ssc.awaitTermination()
  }

}
