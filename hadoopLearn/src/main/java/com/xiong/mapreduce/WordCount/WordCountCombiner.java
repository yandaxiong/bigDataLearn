package com.xiong.mapreduce.WordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountCombiner  extends Reducer<Text, IntWritable,Text,IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // 计算累加和
        int count = 0;

        for(IntWritable value:values){
            count += value.get();
        }

        // 写出
        context.write(key, new IntWritable(count));


    }
}
