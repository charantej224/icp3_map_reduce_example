package com.bigdataprogramming.bfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BfsMapper
        extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        String eachLine = value.toString();
        String[] splitArray = eachLine.split("|");
        Text outputKey = new Text();
        Text outputValue = new Text();
        for(String pair : splitArray){
            String[] pairArray = pair.split(",");
            outputKey.set(pairArray[0]);
            outputValue.set(pairArray[1]);
            context.write(outputKey, outputValue);
        }
    }
}
