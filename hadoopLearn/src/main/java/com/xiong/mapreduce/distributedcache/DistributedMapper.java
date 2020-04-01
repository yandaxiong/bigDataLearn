package com.xiong.mapreduce.distributedcache;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class DistributedMapper extends Mapper<LongWritable, Text,Text, NullWritable> {

    // 缓存pd.txt数据
    private Map<String, String> pdMap = new HashMap<>();
    Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();

        // 读取pd.txt文件,并把数据存储到缓存（集合）
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(cacheFiles[0]))));

        String line ;
        while (StringUtils.isNotEmpty(line = bufferedReader.readLine())){
            // 截取
            String[] fields = line.split("\t");
            // 存储数据到缓存
            pdMap.put(fields[0],fields[1]);
        }
        bufferedReader.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 需求：要合并pd.txt和order.txt里面的内容

        // 1 获取一行 1001		01	4  pdName
        String line = value.toString();

        // 2 截取 1001		01	4
        String[] fields = line.split("\t");

        // 3 获取pdname
        String pdName = pdMap.get(fields[1]);

        // 4 拼接  1001		01	4  pdName
        k.set(line + "\t" + pdName);

        // 5 写出
        context.write(k, NullWritable.get());


    }
}
