package com.atguigu.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN,
 * VALUEIN,
 * KEYOUT,
 * VALUEOUT
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    private Text outk = new Text();
    private IntWritable outv = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        String[] s1 = s.split(" ");
        for (String s2 : s1) {
            outk.set(s2);
            outv.set(1);
            context.write(outk, outv);
        }
    }
}
