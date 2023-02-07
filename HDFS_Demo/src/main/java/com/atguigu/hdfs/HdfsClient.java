package com.atguigu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.URL;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClient {

    private FileSystem fs;
    @Before
    public void  Innit() throws URISyntaxException, IOException, InterruptedException {
        URI uri = new URI("hdfs://hadoop102:8020");

        Configuration conf = new Configuration();

        String user = "atguigu";

        fs = FileSystem.get(uri, conf, user);
    }
    @After
    public void Closed() throws IOException {
        fs.close();
    }
    @Test
    public void TestMkdir() throws IOException, InterruptedException, URISyntaxException {

        fs.mkdirs(new Path("/output/xiaohuahua1"));

    }
    //上传
    @Test
    public void Testput() throws IOException {
        //参数一 是否删除源数据，参数二 是否允许覆盖，参数三 文件本地路径，参数四 文件集群路径
        fs.copyFromLocalFile(false,false,new Path("D:\\input\\word.txt"),new Path("/output/xiaohuahua1"));
    }
    //下载
    @Test
    public void TestGet() throws IOException {
        //参数一 是否删除源数据，参数二 HDfs文件路径，参数三 win路径，参数四 是否校验，false 校验  true 不校验
        fs.copyToLocalFile(false,new Path("/output/xiaohuahua1/word.txt"),new Path("D:\\"),false);
    }

    //删除
    @Test
    public void TestDel() throws IOException {

        fs.delete(new Path("/output"),true);
    }


}