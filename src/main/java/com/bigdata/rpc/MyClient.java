package com.bigdata.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * 客户机代码
 */
public class MyClient {
    public static void main(String[] args) throws IOException {
        //通过socket连接RPC Server
        InetSocketAddress addr = new InetSocketAddress("localhost", 1234);
        Configuration conf = new Configuration();
        //获取RPC代理
        MyProtocol proxy = RPC.getProxy(MyProtocol.class, MyProtocol.versionID, addr, conf);
        //执行RPC接口的方法并获取结果
        String result = proxy.hello("RPC");
        System.out.println("RPC Client(客户机)收到的接口：" +result);
    }
}
