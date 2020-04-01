package com.xiong.mapreduce.order;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

    OrderBean bean = new OrderBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 读取数据
        String line = value.toString();

        // 2 切割数据
        String[] fields = line.split("\t");

        // Order_0000002	Pdt_03	522.8
        // 3 封装bean对象
        bean.setOrderId(fields[0]);
        bean.setPrice(Double.parseDouble(fields[2]));

        // 4 写出
        context.write(bean,NullWritable.get());

    }
}
