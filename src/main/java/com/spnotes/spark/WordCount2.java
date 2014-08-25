package com.spnotes.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by user on 8/24/14.
 */
public class WordCount2 {

    public static void main(String[] argv){
        if (argv.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n",
                    WordCount.class.getSimpleName());
            return;
        }
        String inputPath = argv[0];
        String outputPath = argv[1];

        System.out.printf("Starting WordCount program with %s as input %s as output\n", inputPath,outputPath);

        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> file = sc.textFile(inputPath);

        JavaRDD<String> words = file.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });

        words = words.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                System.out.println("Inside filter words ->" +s);
                if( s.trim().length() == 0)
                    return false;
                return true;
            }
        });

        JavaPairRDD<String, Integer> wordToCountMap = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s,1);
            }
        });

        JavaPairRDD<String, Integer> wordCounts = wordToCountMap.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer first, Integer second) throws Exception {
                return first + second;
            }
        });

        wordCounts.saveAsTextFile(outputPath);

    }
}
