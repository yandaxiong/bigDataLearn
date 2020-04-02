package com.xiong.zkcase;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DistributeClient {

    private static String connectString = "xiong0002:2181,xiong0003:2181,xiong0004:2181";
    private static int sessionTimeout = 2000;
    private ZooKeeper zkClient = null;
    private String parentNode = "/servers";
    private volatile ArrayList<String> serversList = new ArrayList<>();

    //创建到zk的客户端连接
    public void getConnect() throws IOException{
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                try {
                    getServerList();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public void getServerList() throws  Exception{

        // 获取服务器子节点信息，并且对父节点进行监听
        List<String> children = zkClient.getChildren(parentNode,true);
        ArrayList<String> servers = new ArrayList<>();

        for (String child:children){
            byte[] data = zkClient.getData(parentNode + "/" + child, false, null);
            servers.add(new String(data));
        }

        //把servers赋值给成员serverList,一提供给各业务线程使用
        serversList = servers;
        System.out.println(serversList);
    }

    public void business() throws  Exception{
        System.out.println("client is working ...");
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws Exception {

        //获取zk连接
        DistributeClient client = new DistributeClient();
        client.getConnect();

        //获取servers的子节点信息，从中获取服务器信息列表
        client.getServerList();

        //业务进程启动
        client.business();
    }


}
