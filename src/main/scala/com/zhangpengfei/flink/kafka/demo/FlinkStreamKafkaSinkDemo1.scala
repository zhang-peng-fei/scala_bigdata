package com.zhangpengfei.flink.kafka.demo
import org.apache.flink.api.scala._

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

object FlinkStreamKafkaSinkDemo1 {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val data = "{\"apiType\":\"1\",\"backendResponseCode\":\"99999\",\"businessResponseCode\":\"99999\",\"callByte\":0,\"callEndTime\":1577264441920,\"callIp\":\"10.251.7.222\",\"callStartTime\":1577264441903,\"dayId\":25,\"errLevel\":\"3\",\"gatewayBusinessResponseCode\":\"99999\",\"gatewayResponseCode\":\"200\",\"host\":\"10.251.7.222:80\",\"hourId\":17,\"logCnt\":\"{}\",\"logId\":0,\"method\":\"POST\",\"monthId\":201912,\"rawData\":\"{\\\"seqid\\\":\\\"2df0e511b5744c07a7575478211830c2\\\",\\\"code\\\":\\\"800-DSJZX-DE-001\\\",\\\"message\\\":\\\"数据不存在\\\",\\\"flag\\\":\\\"0\\\",\\\"data\\\":{}}\\n\",\"reqCnt\":\"{method=credit_request, user_id=610, key_id=Credit_6101, service_id=101038, sign=EDA3C1A7470DFDD46204C88F8A441A59, cust_name=新疆广茂宾馆有限责任公司广电大酒店, accs_nbr=09912361600, seqid=2df0e511b5744c07a7575478211830c2, token=4a0e629698e14bbca437b103f253ca4d, timestamp=1577264442977}\",\"requestForwardTime\":1577264441904,\"requestParam\":\"{\\\"method\\\":\\\"credit_request\\\",\\\"user_id\\\":\\\"610\\\",\\\"key_id\\\":\\\"Credit_6101\\\",\\\"service_id\\\":\\\"101038\\\",\\\"sign\\\":\\\"EDA3C1A7470DFDD46204C88F8A441A59\\\",\\\"cust_name\\\":\\\"新疆广茂宾馆有限责任公司广电大酒店\\\",\\\"accs_nbr\\\":\\\"09912361600\\\",\\\"seqid\\\":\\\"2df0e511b5744c07a7575478211830c2\\\",\\\"token\\\":\\\"4a0e629698e14bbca437b103f253ca4d\\\",\\\"timestamp\\\":\\\"1577264442977\\\"}\",\"requestReceivedTime\":1577264441903,\"requestSize\":\"335\",\"responseForwardTime\":1577264441920,\"responseParam\":\"{\\\"message\\\":{\\\"errorMessage\\\":\\\"数据不存在\\\",\\\"errorCode\\\":\\\"800-DSJZX-DE-001\\\"},\\\"data\\\":{},\\\"code\\\":\\\"99999\\\",\\\"seqid\\\":\\\"2df0e511b5744c07a7575478211830c2\\\"}\",\"responseReceivedTime\":1577264441920,\"responseSize\":\"2\",\"resultFlag\":-1,\"sId\":\"1674\",\"seqId\":\"2df0e511b5744c07a7575478211830c2\",\"subTime\":17,\"traceId\":\"128.82.15772644419031943\",\"uri\":\"/ors/rest/getBussScore\",\"userAgent\":\"Java DopAPIv1 SDK Client\",\"userId\":\"730\"}"
    val stream: DataStream[String] = env.fromElements(data)

    val myProducer = new FlinkKafkaProducer010[String](
      "192.168.78.135:9092",         // broker list
      "log_dls",               // target topic
      new SimpleStringSchema)   // serialization schema

    // versions 0.10+ allow attaching the records' event timestamp when writing them to Kafka;
    // this method is not available for earlier Kafka versions
    myProducer.setWriteTimestampToKafka(true)

    stream.addSink(myProducer)
  env.execute(FlinkStreamKafkaSinkDemo1.getClass.getName)



  }
}
