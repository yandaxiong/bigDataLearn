package com.xiong.mapreduce.filter;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FilterReducer extends Reducer<Text, NullWritable,Text,NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        //在key上加上回车和换行
        key.set(key.toString()+"\r\n");

        context.write(key,NullWritable.get());

    }
}
