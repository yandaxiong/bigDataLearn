package com.xiong.base;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class zookeeper1 {

    private static String connectString = "xiong0002:2181,xiong0003:2181,xiong0004:2181";
    private static int sessionTimeout = 2000;
    private ZooKeeper zkClient = null;

    //创建zookeeper客户端
    @Before
    public void init() throws  Exception{
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                // 收到事件通知后的回调函数（用户的业务逻辑）
                System.out.println(watchedEvent.getType()+"----"+watchedEvent.getPath());

                try {
                    //再次启动监听
                    zkClient.getChildren("/",true);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    //创建子节点
    @Test
    public void create() throws Exception{
        // 数据的增删改查
        // 参数1：要创建的节点的路径； 参数2：节点数据 ； 参数3：节点权限 ；参数4：节点的类型
        String nodeCreated = zkClient.create("/idea","hello zk".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    //获取子节点
    @Test
    public void getChilder() throws  Exception{

        List<String> children = zkClient.getChildren("/", true);

        for (String child:children){
            System.out.println(child);
        }
        // 延时阻塞
//        Thread.sleep(Long.MAX_VALUE);
    }

    //判断znode是否存在
    @Test
    public void  exist() throws  Exception{

        Stat stat = zkClient.exists("/idea", false);
        System.out.println(stat == null ? "not exist" : "exist");

    }






}
