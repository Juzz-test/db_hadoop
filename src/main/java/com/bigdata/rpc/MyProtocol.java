package com.bigdata.rpc;

import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * 自定义一个RPC接口
 */
public interface MyProtocol extends VersionedProtocol {
    long versionID = 123456l;
    String hello(String name);
}
