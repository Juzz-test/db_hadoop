package com.bigdata.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

/**
 * 服务器代码
 */
public class MyServer {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        //创建RPC Server的构建器
        RPC.Builder builder = new RPC.Builder(conf);
        //设置构建器相关的参数
        builder.setBindAddress("localhost")//设置监听的IP或者主机名
            .setPort(1234)//设置监听的端口
            .setProtocol(MyProtocol.class)//设置RPC接口
            .setInstance(new MyProtocolImpl());//设置RPC接口的实现类

        // 构建 RPC Server
        RPC.Server server = builder.build();
        //启动
        server.start();
        System.out.println("RPC Server 启动了。。。。");
    }
}
