package com.xiong.HbaseToHbase;

import com.xiong.base.HbaseUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Fruit2FruitMRJob  extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        HbaseUtils.init();
        Configuration conf = HbaseUtils.getConfig();
        Fruit2FruitMRJob f2f = new Fruit2FruitMRJob();
        int status = ToolRunner.run(conf, f2f, args);
        System.exit(status);
    }

    public int run(String[] strings) throws Exception {
        HbaseUtils.init();
        //得到Configuration
        Configuration conf = HbaseUtils.getConfig();
        //创建Job任务
        Job job = Job.getInstance(conf);

        job.setJarByClass(Fruit2FruitMRJob.class);

        //配置Job
        Scan scan = new Scan();
        scan.setCacheBlocks(false);
        scan.setCaching(500);

        //设置Mapper，注意导入的是mapreduce包下的，不是mapred包下的，后者是老版本
        TableMapReduceUtil.initTableMapperJob(
                "fruit",//Mapper操作的表名
                scan,//扫描表的对象是谁
                com.xiong.HbaseToHbase.ReadFruitMapper.class,//制定Mapper类
                ImmutableBytesWritable.class,//制定Mapper的输出key
                Put.class,//指定Mapper的输出Value
                job//指定Job
        );

        //设置Reducer
        TableMapReduceUtil.initTableReducerJob(
                "fruit_mr",
                com.xiong.HbaseToHbase.WriteFruitReducer.class,
                job
        );
        job.setNumReduceTasks(1);
        boolean isSucceed = job.waitForCompletion(true);

        return isSucceed ? 0 : 1;
    }
}
