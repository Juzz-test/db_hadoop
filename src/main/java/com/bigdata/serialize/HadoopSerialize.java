package com.bigdata.serialize;

import org.apache.hadoop.io.Writable;

import java.io.*;

/**
 * Hadoop的序列化
 */
public class HadoopSerialize {
    public static void main(String[] args) throws Exception{
        //创建student对象，并设置id和name属性
        StudentWritable studentWritable = new StudentWritable();
        studentWritable.setId(1L);
        studentWritable.setName("Hadoop");

        //将student对象的当前状态写入本地文件中
        FileOutputStream fos = new FileOutputStream("E:\\student_hadoop.txt");
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        studentWritable.write(oos);
        oos.close();
        fos.close();
    }
}

class StudentWritable implements Writable{
    private Long id;
    private String name;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.id);
        out.writeUTF(this.name);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readLong();
        this.name = in.readUTF();
    }
}
