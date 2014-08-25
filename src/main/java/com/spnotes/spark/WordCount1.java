package com.spnotes.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by user on 8/21/14.
 */
public class WordCount1 {
    public static void main(String[] argv){
        if (argv.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n",
                    WordCount.class.getSimpleName());
            return;
        }
        String inputPath = argv[0];
        String outputPath = argv[1];

        System.out.printf("Starting WordCount program with %s as input %s as output", inputPath,outputPath);


        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> file = sc.textFile(inputPath);


        JavaRDD<Integer> lineLenths = file.map(new Function<String, Integer>() {
            @Override
            public Integer call(String s) throws Exception {
                return s.length();
            }
        });


        int totalLength = lineLenths.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer first, Integer second) throws Exception {
                return first + second;
            }
        });

        System.out.println("Total length " + totalLength);
    }
}
