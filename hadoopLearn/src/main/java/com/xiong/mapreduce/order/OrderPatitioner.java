package com.xiong.mapreduce.order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderPatitioner extends Partitioner<OrderBean, NullWritable> {

    @Override
    public int getPartition(OrderBean key, NullWritable value, int numPartitions) {
        // 按照key的orderid的hashCode值分区
        return (key.getOrderId().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
