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
        List<Double> inputs = List.of(35.2, 3.145, 2.83, 4.55);

        SparkConf conf = new SparkConf().setAppName("tryingSpark").setMaster("local[*]");
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            Logger log = LogManager.getRootLogger();
            Logger.getLogger("org.apache").setLevel(Level.WARN);

            JavaRDD<Double> myRDD = sc.parallelize(inputs);
            double res = myRDD.reduce(Double::sum);
            log.info("RESULT: " + res);
        }

    }
}