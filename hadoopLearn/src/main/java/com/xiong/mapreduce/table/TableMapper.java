package com.xiong.mapreduce.table;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text,Text,TableBean> {

    Text text = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

//		1 获取输入文件类型
        FileSplit inputSplit = (FileSplit)context.getInputSplit();
        String name = inputSplit.getPath().getName();
//		2 获取输入数据
        String line = value.toString();
        String[] split = line.split("\t");
        TableBean tableBean = new TableBean();
//      3 不同文件分别处理
        if(name.indexOf("order")>-1){
            tableBean.setOrder_id(split[0]);
            tableBean.setP_id(split[1]);
            tableBean.setAmount(Integer.parseInt(split[2]));
            tableBean.setPname("");
            tableBean.setFlag("0");
        }
        if(name.indexOf("pd")>-1){
            tableBean.setP_id(split[0]);
            tableBean.setPname(split[1]);
            tableBean.setOrder_id("");
            tableBean.setAmount(0);
            tableBean.setFlag("1");
        }
        text.set(tableBean.getP_id());
        context.write(text,tableBean);
    }
}
