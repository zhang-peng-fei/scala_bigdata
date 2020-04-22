package flink.kafka;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;


/**
 * @author 张朋飞
 */
public class ApiCallLogKafkaDeserializationSchema implements KafkaDeserializationSchema<ApiCallLog> {

    private ObjectMapper mapper;

    @Override
    public boolean isEndOfStream(ApiCallLog nextElement) {
        return false;
    }

    @Override
    public ApiCallLog deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        ApiCallLog apiCallLog = new ApiCallLog();
        if (record.value() != null) {
            apiCallLog = mapper.readValue(record.value(), ApiCallLog.class);
        }
        return apiCallLog;
    }

    @Override
    public TypeInformation<ApiCallLog> getProducedType() {
        return TypeExtractor.getForClass(ApiCallLog.class);
    }
}
