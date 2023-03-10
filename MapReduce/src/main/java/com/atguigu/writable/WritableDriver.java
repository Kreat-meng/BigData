package com.atguigu.writable;

import com.atguigu.patition.PhonePtatition;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WritableDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setMapperClass(WritableMapper.class);
        job.setReducerClass(WritableReducer.class);

        job.setJarByClass(WritableDriver.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(WritableBean.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(WritableBean.class);

        FileInputFormat.setInputPaths(job,new Path("D:\\input\\inputflow"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\output1"));

        boolean b = job.waitForCompletion(true);
        System.exit(b? 0 : 1);
    }
}
