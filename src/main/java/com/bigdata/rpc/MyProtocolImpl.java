package com.bigdata.rpc;

import org.apache.hadoop.ipc.ProtocolSignature;

import java.io.IOException;

/**
 * RPC实现类
 */
public class MyProtocolImpl implements MyProtocol{
    @Override
    public String hello(String name) {
        System.out.println("我被调用了。。。。");
        return "hello" + name;
    }

    /**
     * 接口版本号
     * @param s
     * @param l
     * @return
     * @throws IOException
     */
    @Override
    public long getProtocolVersion(String s, long l) throws IOException {
        return versionID;
    }

    /**
     * 协议签名
     * @param s
     * @param l
     * @param i
     * @return
     * @throws IOException
     */
    @Override
    public ProtocolSignature getProtocolSignature(String s, long l, int i) throws IOException {
        return new ProtocolSignature();
    }
}
