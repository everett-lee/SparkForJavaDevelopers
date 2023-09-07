package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

public class Exercises {
    final static SparkConf conf = new SparkConf().setAppName("exercises").setMaster("local[*]");

    static void getKeyWords(int topN) {
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            Logger log = LogManager.getRootLogger();
            Logger.getLogger("org.apache").setLevel(Level.WARN);

            sc.textFile("src/main/resources/subtitles/input.txt")
                    .map(line -> line.replaceAll("[^a-zA-Z\\s]", ""))
                    .map(String::toLowerCase)
                    .flatMap(line -> List.of(line.split(" ")).iterator())
                    .filter(el -> !el.isBlank())
                    .filter(Util::isNotBoring)
                    .mapToPair(el -> new Tuple2<>(el, 1))
                    .reduceByKey(Integer::sum)
                    .mapToPair(el -> el.swap())
                    .sortByKey(false)
                    .take(topN)
                    .forEach(System.out::println);

        }
    }
}
