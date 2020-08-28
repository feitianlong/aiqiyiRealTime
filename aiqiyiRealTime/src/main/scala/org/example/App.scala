package org.example

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.example.dao.{CategarySearchClickCountDAO, CategoryClickCountDAO}
import org.example.domain.{CategoryClickCount, CategorySearchClickCount, ClickLog}
import org.example.util.DataUtil

import scala.collection.mutable.ListBuffer

/**
 * Hello world!
 *
 */
object aiqiyiRealTime extends App {

  private val conf: SparkConf = new SparkConf().setAppName("aiqiyiRealTime").setMaster("local[*]")
  private val ssc: StreamingContext = new StreamingContext(conf, Seconds(2))


  //创建组
  val group = "g001"

  // 创建topic，注意kafkaparams参数中使用的topic类型为Array
  val topic: Array[String] = Array("flumeTopic")

  // 确定brookerList
  val zk = "hdp-01:2181,hdp-04:2181,hdp-05:2181"
  val brookerList = "hdp-05:9092,hdp-04:9092,hdp-05:9092"

  val kafkaParms = Map[String, Object](
    "bootstrap.servers" -> "hdp-05:9092,hdp-04:9092,hdp-05:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> group,
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )


  // 在Kafka中读取偏移量
  val inpuStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String , String](
    ssc,
    // 位置策略
    // PreferBrokers：当Spark集群和Kafka集群属于同一组机器时使用；
    // PreferConsistent：最常用的策略，当Spark机器和Kafka机器不属于同一组机器时使用；
    // PreferFixed：当数据分布不均衡，由用户自行制定KafkaPartition和机器的关系。
    LocationStrategies.PreferConsistent,
    // 消费者策略
    //消费者策略，是控制如何创建和配制消费者对象。
    //或者对kafka上的消息进行如何消费界定，比如t1主题的分区0和1，
    //或者消费特定分区上的特定消息段。
    //该类可扩展，自行实现。
    //1.ConsumerStrategies.Assign
    //    指定固定的分区集合,指定了特别详细的方范围。
    //    def Assign[K, V](
    //          topicPartitions: Iterable[TopicPartition],
    //          kafkaParams: collection.Map[String, Object],
    //          offsets: collection.Map[TopicPartition, Long])
    //
    //2.ConsumerStrategies.Subscribe
    //    允许消费订阅固定的主题集合。
    //
    //3.ConsumerStrategies.SubscribePattern
    //    使用正则表达式指定感兴趣的主题集合。
    ConsumerStrategies.Subscribe[String,String](topic, kafkaParms)
  )

  //日志格式 156.187.29.132	2017-11-20 00:39:26	"GET /www/2 HTTP/1.0"	-	200
  // 状态码200 代表成功处理
  // searchrefer 为- 代表其他搜索方式
  // query_log = "{ip}\t{localtime}\t\"GET /{url} HTTP/1.1\"\t{refer}\t{status_code}".format(url = sample_url(),ip = sample_ip(),refer = sample_referer(),status_code = sample_status_code(),localtime=time_str)

  //迭代DStream中的RDD，将每一个时间点对于的RDD拿出来
  inpuStream.foreachRDD { rdd =>
    //获取该RDD对于的偏移量
    val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

    val logs: RDD[String] = rdd.map(_.value())
    // 清洗数据
     val clean_logs: RDD[ClickLog] = logs.map(line => {
      var infos = line.split("\t")
      var url = infos(2).split(" ")(1)
      var categoryId = 0
      if (url.startsWith("www")) {
        categoryId = url.split("/")(1).toInt
      }
      ClickLog(infos(0), DataUtil.pasreToTARGET_FORMAT(infos(1)), categoryId, infos(3), infos(4).toInt)

    }).filter(clickLog=> clickLog.categoryId != 0)

    // 收集数据保存到HBase

    //每个类别的每天的点击量 (day_categaryId,1)
    clean_logs.map(log=>{
      (log.time.substring(0,8)+log.categoryId,1)
    }).reduceByKey(_+_).foreachPartition(partitions=>{

        val list = new ListBuffer[CategoryClickCount]
        partitions.foreach(pair=>{
          list.append(CategoryClickCount(pair._1,pair._2))
        })
        CategoryClickCountDAO.save(list)

    })


    //每个栏目下面从渠道过来的流量20171122_www.baidu.com_1 100 20171122_2（渠道）_1（类别） 100
    //categary_search_count   create "categary_search_count","info"
    //124.30.187.10	2017-11-20 00:39:26	"GET www/6 HTTP/1.0"
    // 	https:/www.sogou.com/web?qu=我的体育老师	302
    clean_logs.map(log => {
      val url = log.searchRefer.replace("//", "/")
      val url_splits = url.split("/")
      var host = ""
      if(url_splits.length > 2){
        host = url_splits(1)
      }
      (host, log.time , log.categoryId)
    }).filter(line => line._1 != "").map(line => {
      (line._2.substring(0,8) + "_" + line._1 + "_" + line._3 , 1)
    }).reduceByKey(_+_).foreachPartition( partitions =>{
      val list = new ListBuffer[CategorySearchClickCount]
      partitions.foreach(pairs => {
        list.append(CategorySearchClickCount(pairs._1 , pairs._2))
      })
      CategarySearchClickCountDAO.save(list)
    })

    //更新偏移量
    inpuStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
  }
  //156.187.29.132	2017-11-20 00:39:26	"GET /www/2 HTTP/1.0"	-	200

  ssc.start()
  ssc.awaitTermination()

}
