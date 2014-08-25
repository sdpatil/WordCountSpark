package com.spnotes.spark;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by user on 8/21/14.
 */
public class WordCount {

    public static void main(String[] argv){
        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> data = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> distData =  sc.parallelize(data);

        Integer returnValue = distData.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer input1, Integer input2) throws Exception {
                System.out.printf("Inside distData.reduce's call %d %d \n", input1,input2);
                return input1 + input2;
            }
        });
        System.out.println("Return value " + returnValue);
    }
}
