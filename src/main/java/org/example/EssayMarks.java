package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class EssayMarks {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "/home/lee/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("essayMarks")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "/home/lee/sparktmp")
                .getOrCreate();

        Dataset<Row> dataSet = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
        dataSet = dataSet.groupBy("subject")
                .pivot("year")
                .agg(round(avg(col("score")),2).alias("Average score"),
                        round(stddev(col("score")), 2).alias("SDEV score"));

        dataSet.show();

        spark.close();
    }
}
