package com.spnotes.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by user on 8/27/14.
 */
public class HelloESOutputSpark {

    public static void main(String[] argv){
        System.setProperty("hadoop.home.dir","/usr/local/hadoop");
        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local");

        conf.set("spark.serializer", org.apache.spark.serializer.KryoSerializer.class.getName());

        JavaSparkContext sc = new JavaSparkContext(conf);

        Configuration hadoopConfiguration = new Configuration();
        hadoopConfiguration.set("es.nodes","localhost:9200");
        hadoopConfiguration.set("es.resource","hadoop/contact");
    }
}
