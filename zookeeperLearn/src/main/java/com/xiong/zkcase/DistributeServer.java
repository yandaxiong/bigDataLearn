package com.xiong.zkcase;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.List;

//服务器端代码
public class DistributeServer {

    private static String connectString = "xiong0002:2181,xiong0003:2181,xiong0004:2181";
    private static int sessionTimeout = 2000;
    private ZooKeeper zkClient = null;
    private String parentNode = "/servers";

    //创建连接zk的客户端连接
    public void getConnect() throws IOException{
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                // 收到事件通知后的回调函数（用户的业务逻辑）
                System.out.println(event.getType() + "--" + event.getPath());

                //再次启动监听
                try {
                    List<String> children = zkClient.getChildren(parentNode, true);
                    System.out.println("监控："+children);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    //注册服务器
    public void registServer(String hostname) throws Exception{
        String create = zkClient.create(parentNode+"/server",hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        System.out.println(hostname+"is noline "+ create);

    }

    //业务功能
    public void business(String hostname) throws Exception{
        System.out.println(hostname+" is working ...");
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws Exception {
        DistributeServer server = new DistributeServer();

        server.getConnect();
        // 利用zk连接注册服务器信息
        server.registServer(args[0]);
        //启动业务功能
        server.business(args[0]);

    }






}
