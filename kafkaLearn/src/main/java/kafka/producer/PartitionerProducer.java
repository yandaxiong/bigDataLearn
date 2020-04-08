package kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.Producer;

import java.util.Properties;

public class PartitionerProducer {

    public static void main(String[] args) {
        Properties props = new Properties();
        // Kafka服务端的主机名和端口号
        props.put("bootstrap.servers", "xiong0002:9092");
        // 等待所有副本节点的应答
        props.put("acks", "all");
        // 消息发送最大尝试次数
        props.put("retries", 0);
        // 一批消息处理大小
        props.put("batch.size", 16384);
        // 请求延时
        props.put("linger.ms", 1);
        // 发送缓存区内存大小
        props.put("buffer.memory", 33554432);
        // key序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 自定义分区
        props.put("partitioner.class", "com.xiong.kafka.producer.CustomPartitioner");


        Producer<String,String> producer = new KafkaProducer<String, String>(props );

        for (int i=0;i<10;i++){
            producer.send(new ProducerRecord<String, String>("first", Integer.toString(i), "hello world-" + i), new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (recordMetadata != null) {
                        System.err.println(recordMetadata.partition() + "---" + recordMetadata.offset());
                    }
                }
            });
        }
        producer.close();

    }
}
