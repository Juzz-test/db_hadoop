package com.bigdata.mapreduce;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.IOException;

/**
 * 小文件解决方案之MapFile
 */
public class SmallFileMap {
    public static void main(String[] args) throws Exception {
        write("E:\\smallFile","/mapfile");
        read("/mapfile");
    }
    /**
     * 生成MapFile文件
     * @param inputDir 输入目录-windows目录
     * @param outputDir 输出目录-hdfs目录
     * @throws Exception
     */
    private static void write(String inputDir, String outputDir) throws Exception{
        //创建一个配置对象
        Configuration conf = new Configuration();
        //指定HDFS的地址
        conf.set("fs.defaultFS","hdfs://bigdata01:9000");

        //获取操作HDFS的对象
        FileSystem fileSystem = FileSystem.get(conf);

        //删除HDFS上的输出文件
        fileSystem.delete(new Path(outputDir),true);

        //构造opts数组，有两个元素
        /**
         * 第一个是key类型
         * 第二个是value类型
         */
        SequenceFile.Writer.Option[] opts = new SequenceFile.Writer.Option[]{
                MapFile.Writer.keyClass(Text.class),
                MapFile.Writer.valueClass(Text.class)
        };
        //创建一个writer实例
        MapFile.Writer writer = new MapFile.Writer(conf, new Path(outputDir), opts);

        //指定需要压缩的文件目录
        File inputDirPath = new File(inputDir);
        if (inputDirPath.isDirectory()){
            //获取目录中的文件
            File[] files = inputDirPath.listFiles();
            //迭代文件
            for (File file:files) {
                //获取文件内容
                String content = FileUtils.readFileToString(file, "UTF-8");
                //获取文件名
                String fileName = file.getName();
                Text key = new Text(fileName);
                Text value = new Text(content);
                //向SequenceFile中写入数据
                writer.append(key,value);

            }
        }
        writer.close();
    }

    /**
     * 读取MapFile文件
     * @param inputDir MapFile文件路径
     * @throws IOException
     */
    private static void read(String inputDir) throws IOException {
        //创建一个配置对象
        Configuration conf = new Configuration();
        //指定HDFS地址
        conf.set("fs.defaultFS","hdfs://bigdata01:9000");
        //创建阅读器
        MapFile.Reader reader = new MapFile.Reader(new Path(inputDir),conf);
        Text key = new Text();
        Text value = new Text();
        //循环读数据
        while(reader.next(key,value)){
            //输出文件名称
            System.out.print("文件名："+key.toString()+",");
            //输出文件内容
            System.out.println("文件内容："+value.toString());
        }
        reader.close();
    }
}
