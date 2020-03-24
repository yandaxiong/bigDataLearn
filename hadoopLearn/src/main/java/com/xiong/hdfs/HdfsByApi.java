package com.xiong.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

public class HdfsByApi {

    public  FileSystem initHDFS() throws Exception {
        // 1 创建配置信息对象
        // new Configuration();的时候，它就会去加载jar包中的hdfs-default.xml
        // 然后再加载classpath下的hdfs-site.xml
        Configuration configuration = new Configuration();

        // 2 设置参数
        // 参数优先级： 1、客户端代码中设置的值  2、classpath下的用户自定义配置文件 3、然后是服务器的默认配置
//		configuration.set("fs.defaultFS", "hdfs://xiong0002:8020");
        configuration.set("dfs.replication", "3");

        // 3 获取文件系统
        FileSystem fs = FileSystem.get(new URI("hdfs://xiong0002:8020"), configuration, "root");

        // 4 打印文件系统
//        System.out.println(fs.toString());

        return  fs;
    }

//    HDFS文件上传
    @Test
    public void  putFileToHDFS() throws Exception {
        FileSystem fs = initHDFS();
        Path local =  new Path("D:\\nouse\\hadoopTestInput\\hello.txt ");
        Path  ser  = new Path("/user/xiong/hadoopInput/hello.txt");
        fs.copyFromLocalFile(local,ser);
        fs.close();
    }

    //文件下载
    @Test
    public void getFileFromHDFS() throws Exception{

        // 1 创建配置信息对象
        FileSystem fs = initHDFS();

        // boolean delSrc 指是否将原文件删除
        // Path src 指要下载的文件路径
        // Path dst 指将文件下载到的路径
        // boolean useRawLocalFileSystem 是否开启文件效验
        // 2 下载文件
        Path local =  new Path("D:\\test.txt");
        Path  ser  = new Path("hdfs://xiong0002:8020/user/xiong/xiong.txt");
        fs.copyToLocalFile(false,ser,local,true);
        fs.close();
    }

    //目录创建
    @Test
    public void  mkdirAtHDFS() throws Exception {
        FileSystem fs = initHDFS();
        fs.mkdirs(new Path("/user/xiong2"));
        fs.close();
    }

    //文件夹删除
    @Test
    public void deleteAtHDFS() throws Exception {
        FileSystem fs =  initHDFS();
        Path  ser  = new Path("/user/xiong2");
        //2 删除文件夹 ，如果是非空文件夹，参数2必须给值true
        fs.delete(ser,true);
        fs.close();
    }

    //文件名更改
    @Test
    public void renameAtHDFS() throws Exception {
        FileSystem fs = initHDFS();
        Path  ser  = new Path("/user/xiong/xiong.txt");
        Path  ser2  = new Path("/user/xiong/test2.txt");
        //重命名文件或文件夹
        fs.rename(ser,ser2);
        fs.close();
    }

    //文件详情查看
    @Test
    public  void  readListFile() throws Exception {
        FileSystem fs = initHDFS();
        Path ser = new Path("/user/xiong");
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(ser, true);

        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();

            System.out.println(fileStatus.getPath().getName());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getLen());

            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation b : blockLocations) {
                System.out.println("block-offset:" + b.getOffset());

                String[] hosts = b.getHosts();
                for (String s : hosts) {
                    System.out.println(s);
                }
            }
            System.out.println("-------------------------------------");
        }

    }

    //文件夹查看
    @Test
    public void findAtHDFS() throws Exception, IllegalArgumentException, IOException {

        // 1 创建配置信息对象
        FileSystem fs = initHDFS();
        // 2 获取查询路径下的文件状态信息
        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        // 3 遍历所有文件状态
        for (FileStatus status : listStatus) {
            if (status.isFile()) {
                System.out.println("f--" + status.getPath().getName());
            } else {
                System.out.println("d--" + status.getPath().getName());
            }
        }
    }





}
