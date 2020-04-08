package com.xiong.HDFS2Hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ReadFruitFromHDFSMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //从HDFS中读取的数据
        String lineValue = value.toString();
        String[] values = lineValue.split("\t");

        String rowKey = values[0];
        String name = values[1];
        String color = values[2];

        ImmutableBytesWritable immutableBytesWritable = new ImmutableBytesWritable(Bytes.toBytes(rowKey));

        Put put = new Put(Bytes.toBytes(rowKey));

        put.add(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(name));
        put.add(Bytes.toBytes("info"),Bytes.toBytes("color"),Bytes.toBytes(color));

        context.write(immutableBytesWritable,put);
    }

}
