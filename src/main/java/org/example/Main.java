package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

public class Main {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("tryingSpark").setMaster("local[*]");
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            Logger log = LogManager.getRootLogger();
            Logger.getLogger("org.apache").setLevel(Level.WARN);

            JavaRDD<String> data = sc.textFile("src/main/resources/subtitles/input.txt")
                    .flatMap(line -> List.of(line.split(" ")).iterator());

            data.foreach(el -> System.out.println(el));

        }

    }
}