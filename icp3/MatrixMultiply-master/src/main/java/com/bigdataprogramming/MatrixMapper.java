package com.bigdataprogramming;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MatrixMapper
        extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        int m = Integer.parseInt(configuration.get("m"));
        int p = Integer.parseInt(configuration.get("p"));
        String eachLine = value.toString();
        // (M, i, j, Mij);
        String[] splitArray = eachLine.split(",");
        Text outputKey = new Text();
        Text outputValue = new Text();
        if (splitArray[0].equals("M")) {
            for (int k = 0; k < p; k++) {
                outputKey.set(splitArray[1] + "," + k);
                // outputKey.set(i,k);
                outputValue.set(splitArray[0] + "," + splitArray[2]
                        + "," + splitArray[3]);
                // outputValue.set(M,j,Mij);
                context.write(outputKey, outputValue);
            }
        } else {
            // (N, j, k, Njk);
            for (int i = 0; i < m; i++) {
                outputKey.set(i + "," + splitArray[2]);
                outputValue.set("N," + splitArray[1] + ","
                        + splitArray[3]);
                context.write(outputKey, outputValue);
            }
        }
    }
}
