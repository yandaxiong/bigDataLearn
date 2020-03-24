package com.xiong.mapreduce.wholefile;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class WholeMapper extends Mapper<NullWritable, BytesWritable, Text,BytesWritable> {

    Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 获取切片路径，
        FileSplit inputSplit = (FileSplit) context.getInputSplit();

        Path path = inputSplit.getPath();
        // 设置k
        k.set(path.toString());
    }

    @Override
    protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {

        context.write(k,value);

    }


}
