package kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class TimeInterceptor implements ProducerInterceptor<String,String> {
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        ProducerRecord<String, String> stringStringProducerRecord = new ProducerRecord<String, String>(producerRecord.topic(),
                producerRecord.partition(), producerRecord.timestamp(), producerRecord.key(),
                System.currentTimeMillis() + "," + producerRecord.value());
        return stringStringProducerRecord;
    }

    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    public void close() {

    }

    public void configure(Map<String, ?> map) {

    }
}
