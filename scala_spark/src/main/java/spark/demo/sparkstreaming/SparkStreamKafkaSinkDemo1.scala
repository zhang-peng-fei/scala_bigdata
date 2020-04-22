package spark.demo.sparkstreaming

import java.util.Properties
import java.util.concurrent.Future

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory


object SparkStreamKafkaSinkDemo1 {
  private val LOG = LoggerFactory.getLogger("Kafka2KafkaStreaming")
  class KafkaSink[K, V](createProducer: () => KafkaProducer[K, V]) extends Serializable {
    /* This is the key idea that allows us to work around running into
       NotSerializableExceptions. */
    lazy val producer = createProducer()

    def send(topic: String, key: K, value: V): Future[RecordMetadata] = {
      producer.send(new ProducerRecord[K, V](topic, key, value))
    }

    def send(topic: String, value: V): Future[RecordMetadata] = {
      producer.send(new ProducerRecord[K, V](topic, value))
    }
  }
  object KafkaSink {

    import scala.collection.JavaConversions._

    def apply[K, V](config: Map[String, Object]): KafkaSink[K, V] = {
      val createProducerFunc = () => {
        val producer = new KafkaProducer[K, V](config)
        sys.addShutdownHook {
          // Ensure that, on executor JVM shutdown, the Kafka producer sends
          // any buffered messages to Kafka before shutting down.
          producer.close()
        }
        producer
      }
      new KafkaSink(createProducerFunc)
    }

    def apply[K, V](config: java.util.Properties): KafkaSink[K, V] = apply(config.toMap)
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName(SparkStreamKafkaSinkDemo1.getClass.getName)
      .setMaster("local[*]")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    val context = new SparkContext(conf)
    val streamingContext = new StreamingContext(context, Seconds(5))


    // 初始化KafkaSink,并广播
    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", "192.168.78.135:9092")
        p.setProperty("key.serializer", classOf[StringSerializer].getName)
        p.setProperty("value.serializer", classOf[StringSerializer].getName)
        p
      }
      if (LOG.isInfoEnabled)
        LOG.info("kafka producer init done!")
      streamingContext.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }
    val data = "{\"apiType\":\"1\",\"backendResponseCode\":\"99999\",\"businessResponseCode\":\"99999\",\"callByte\":0,\"callEndTime\":1577264441920,\"callIp\":\"10.251.7.222\",\"callStartTime\":1577264441903,\"dayId\":25,\"errLevel\":\"3\",\"gatewayBusinessResponseCode\":\"99999\",\"gatewayResponseCode\":\"200\",\"host\":\"10.251.7.222:80\",\"hourId\":17,\"logCnt\":\"{}\",\"logId\":0,\"method\":\"POST\",\"monthId\":201912,\"rawData\":\"{\\\"seqid\\\":\\\"2df0e511b5744c07a7575478211830c2\\\",\\\"code\\\":\\\"800-DSJZX-DE-001\\\",\\\"message\\\":\\\"数据不存在\\\",\\\"flag\\\":\\\"0\\\",\\\"data\\\":{}}\\n\",\"reqCnt\":\"{method=credit_request, user_id=610, key_id=Credit_6101, service_id=101038, sign=EDA3C1A7470DFDD46204C88F8A441A59, cust_name=新疆广茂宾馆有限责任公司广电大酒店, accs_nbr=09912361600, seqid=2df0e511b5744c07a7575478211830c2, token=4a0e629698e14bbca437b103f253ca4d, timestamp=1577264442977}\",\"requestForwardTime\":1577264441904,\"requestParam\":\"{\\\"method\\\":\\\"credit_request\\\",\\\"user_id\\\":\\\"610\\\",\\\"key_id\\\":\\\"Credit_6101\\\",\\\"service_id\\\":\\\"101038\\\",\\\"sign\\\":\\\"EDA3C1A7470DFDD46204C88F8A441A59\\\",\\\"cust_name\\\":\\\"新疆广茂宾馆有限责任公司广电大酒店\\\",\\\"accs_nbr\\\":\\\"09912361600\\\",\\\"seqid\\\":\\\"2df0e511b5744c07a7575478211830c2\\\",\\\"token\\\":\\\"4a0e629698e14bbca437b103f253ca4d\\\",\\\"timestamp\\\":\\\"1577264442977\\\"}\",\"requestReceivedTime\":1577264441903,\"requestSize\":\"335\",\"responseForwardTime\":1577264441920,\"responseParam\":\"{\\\"message\\\":{\\\"errorMessage\\\":\\\"数据不存在\\\",\\\"errorCode\\\":\\\"800-DSJZX-DE-001\\\"},\\\"data\\\":{},\\\"code\\\":\\\"99999\\\",\\\"seqid\\\":\\\"2df0e511b5744c07a7575478211830c2\\\"}\",\"responseReceivedTime\":1577264441920,\"responseSize\":\"2\",\"resultFlag\":-1,\"sId\":\"1674\",\"seqId\":\"2df0e511b5744c07a7575478211830c2\",\"subTime\":17,\"traceId\":\"128.82.15772644419031943\",\"uri\":\"/ors/rest/getBussScore\",\"userAgent\":\"Java DopAPIv1 SDK Client\",\"userId\":\"730\"}"
    kafkaProducer.value.send("log_dls","key1",data)
    streamingContext.start()
    streamingContext.awaitTermination()
  }




}
