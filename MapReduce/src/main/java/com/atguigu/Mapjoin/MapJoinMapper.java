package com.atguigu.Mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

public class MapJoinMapper extends Mapper<LongWritable, Text,Text, NullWritable> {

    private HashMap<String, String> pdmap = new HashMap<>();
    private Text outK = new Text();


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        URI[] url = context.getCacheFiles();

        FileSystem fs = FileSystem.get(context.getConfiguration());

        FSDataInputStream fis = fs.open(new Path(url[0]));

        BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF8"));

        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())){

            String[] split = line.split("\t");

            pdmap.put(split[0],split[1]);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] split = line.split("\t");

        outK.set(split[0] + "\t" + pdmap.get(split[1]) + "\t" + split[2]);

        context.write(outK,NullWritable.get());


    }
}
