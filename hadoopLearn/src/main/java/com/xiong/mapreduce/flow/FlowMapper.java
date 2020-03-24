package com.xiong.mapreduce.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text,Text,FlowBean> {

    FlowBean flowBean = new FlowBean();
    Text t = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 获取一行数据
        String line = value.toString();
        // 2 截取字段
        String[] strs = line.split("\t");
        // 3 封装bean对象以及获取电话号
        String phone = strs[1];
        String upFlow = strs[6];
        String downFlow = strs[7];

        flowBean.set( Long.parseLong(upFlow),Long.parseLong(downFlow));
        t.set(phone);
        // 4 写出去
        context.write(t,flowBean);

    }
}
