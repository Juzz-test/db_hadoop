package com.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Java代码操作hdfs
 * 文件操作：上传文件、下载文件、删除文件
 */
public class HdfsOption {
    public static void main(String[] args) throws IOException {
        //创建一个配置对象
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://bigdata01:9000");
        //获取操作HDFS对象
        FileSystem fileSystem = FileSystem.get(conf);

        //上传文件
        put(fileSystem);

        //下载文件
//        get(fileSystem);

        //删除文件
//        delete(fileSystem);
    }

    private static void delete(FileSystem fileSystem) throws IOException {
        Path path = new Path("/README.txt.p");
        //删除文件，目录也可以删除
        //如果要递归删除目录，第二个参数需要设置为true
        //如果删除的是文件或者空目录，第二个参数会被忽略
        boolean flag = fileSystem.delete(path, true);
        if (flag){
            System.out.println("删除成功");
        }else{
            System.out.println("删除失败");
        }
    }


    private static void get(FileSystem fileSystem) throws IOException {
        //获取HDFS文件系统的输入流
        Path path = new Path("/test.txt");
        FSDataInputStream fis = fileSystem.open(path);
        //获取本地文件的输出流
        FileOutputStream fos = new FileOutputStream("E:\\bigdatasoft\\test01.txt");
        //下载文件
        IOUtils.copyBytes(fis,fos,1024,true);
    }

    private static void put(FileSystem fileSystem) throws IOException {
        //获取本地文件的输入流
        FileInputStream fis = new FileInputStream("E:\\bigdatasoft\\test.txt");
        //获取HDFS文件系统的输出流
        Path path = new Path("/test.txt");
        FSDataOutputStream fos = fileSystem.create(path);
        //上传文件：通过工具类把输入流拷贝到输出流里面，实现本地文件上传到HDFS
        IOUtils.copyBytes(fis,fos,1024,true);
    }
}
