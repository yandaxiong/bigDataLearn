package kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class CounterInterceptor implements ProducerInterceptor<String,String> {
    private  int errorCount = 0;
    private  int successCount = 0;
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if(exception == null){
            successCount++;
        }else{
            errorCount++;
        }
    }

    public void close() {
        System.out.println("Successful sent:"+successCount);
        System.out.println("error sent:"+errorCount);
    }

    public void configure(Map<String, ?> configs) {

    }
}
