package com.atguigu.patition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WritableMapper extends Mapper<LongWritable, Text,Text,WritableBean> {
    private Text outk = new Text();
    private WritableBean outv = new WritableBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String[] split = line.split("\t");

        outk.set(split[1]);
        outv.setUpFlow(Long.parseLong(split[split.length - 3]));
        outv.setDownFlow(Long.parseLong(split[split.length - 2]));
        outv.setSumFlow();

        context.write(outk,outv);

    }
}
