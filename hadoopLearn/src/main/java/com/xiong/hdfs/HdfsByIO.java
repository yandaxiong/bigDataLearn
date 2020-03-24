package com.xiong.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;

public class HdfsByIO {

    //文件上传
    @Test
    public void putFileToHDFS() throws Exception {
// 1 创建配置信息对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://xiong0002:8020"), conf, "root");
// 2 创建输入流
        FileInputStream ins = new FileInputStream(new File("d://xiong.txt"));
// 3 创建输出流
        FSDataOutputStream outputStream = fs.create(new Path("/user/xiong/xiong2.txt"));
// 4 流对接
        IOUtils.copyBytes(ins, outputStream, 2048, false);

        IOUtils.closeStream(ins);
        IOUtils.closeStream(outputStream);

        fs.close();

    }

    //文件下载
    @Test
    public void putFileFromHDFS() throws Exception {
// 1 创建配置信息对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://xiong0002:8020"), conf, "root");
// 2 创建输入流
        FSDataInputStream open = fs.open(new Path("/user/xiong/test3.txt"));
// 3 创建读取path
        FileOutputStream outputStream = new FileOutputStream(new File("d://xiong33.txt"));
// 4 流对接输出到控制台
        IOUtils.copyBytes(open, outputStream, 4096, false);

        IOUtils.closeStream(open);
        IOUtils.closeStream(outputStream);

        fs.close();

    }
}
