package com.xiong.HDFS2Hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HDFS2HBaseDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        int status = ToolRunner.run(conf, new HDFS2HBaseDriver(), args);
        System.exit(status);
    }


    public int run(String[] strings) throws Exception {
        //得到Configuration
        Configuration conf = this.getConf();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        Job job = Job.getInstance(conf, this.getClass().getSimpleName());
        job.setJarByClass(HDFS2HBaseDriver.class);

        Path path = new Path("hdfs://xiong0002:8020/input_fruit/fruit.tsv");
        FileInputFormat.addInputPath(job,path);
        //设置mapper
        job.setMapperClass(ReadFruitFromHDFSMapper.class);
        job.setMapOutputValueClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        //设置reducer
        TableMapReduceUtil.initTableReducerJob("fruit_mr",
                WriteFruitMRFromTxtReducer.class,
                job
                );

        job.setNumReduceTasks(1);

        boolean b = job.waitForCompletion(true);
        if(!b){
            System.out.println("失败");
        }

        return b ? 0:1;
    }
}
