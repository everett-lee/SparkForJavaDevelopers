package org.example;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.sources.In;
import scala.Int;
import scala.Tuple2;

/**
 * This class is used in the chapter late in the course where we analyse viewing figures.
 * You can ignore until then.
 */
public class ViewingFigures 
{
	@SuppressWarnings("resource")
	public static void main(String[] args)
	{
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// Use true to use hardcoded data identical to that in the PDF guide.
		boolean testMode = false;
		
		JavaPairRDD<Integer, Integer> viewData = setUpViewDataRdd(sc, testMode);
		JavaPairRDD<Integer, Integer> chapterData = setUpChapterDataRdd(sc, testMode);
		JavaPairRDD<Integer, String> titlesData = setUpTitlesDataRdd(sc, testMode);

		// step 0
		JavaPairRDD<Integer, Integer> courseToChapterCount = chapterData.mapToPair(Tuple2::swap)
				.mapToPair(el -> new Tuple2<>(el._1, 1))
						.reduceByKey(Integer::sum);

		// Remove duplicates
		viewData = viewData.distinct();


		// Join by chapter id to get chapterId -> (courseId, userId)
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> joined = chapterData.join(
				viewData.mapToPair(Tuple2::swap)
		);


		// Convert to (courseId, userId) -> 1
		JavaPairRDD<Tuple2<Integer, Integer>, Integer> tupleKey = joined.mapToPair(el -> new Tuple2<>(el._2, 1));


		// count views by (courseId, userId)
		JavaPairRDD<Tuple2<Integer, Integer>, Integer> counts = tupleKey
				.reduceByKey((a, b) -> a + b);


		// userId no longer needed, so set courseId as key
		JavaPairRDD<Integer, Integer> courseIdToViews = counts.mapToPair(
				el -> new Tuple2<>(el._1._1, el._2)
		);


		// Join to get chapters per course
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> withChaptersPerCourse = courseIdToViews.join(
				courseToChapterCount
		);

		// Calculate views per course / total chapters
		JavaPairRDD<Integer, Double> asFraction = withChaptersPerCourse.mapToPair(
				el -> new Tuple2<>(el._1, el._2._1 / (el._2._2 * 1.0))
		);

		// Map fraction to score
		JavaPairRDD<Integer, Integer> asScore = asFraction.mapToPair(
				el -> new Tuple2<>(el._1, getScore(el._2))
		);


		// Get the total by course
		JavaPairRDD<Integer, Integer> answer = asScore.reduceByKey(
				Integer::sum
		);

		// Join to get the total
		JavaPairRDD<Integer, Tuple2<Integer, String>> withTitle = answer.join(
				titlesData
		);

		// Set count as the key and sort
		JavaPairRDD<Integer, String> countAsKey = withTitle.mapToPair(
				el -> new Tuple2<>(el._2._1, el._2._2)
		).sortByKey(false);


		countAsKey.take(50).forEach(el -> {
			System.out.println("*".repeat(100));
			System.out.println(el);

		});
		sc.close();
	}


	public static int getScore(Double percentComplete) {
		if (percentComplete > 0.9) {
			return 10;
		} else if (percentComplete > 0.5) {
			return 4;
		} else if (percentComplete > 0.25) {
			return 2;
		} else {
			return 0;
		}
	}

	private static JavaPairRDD<Integer, String> setUpTitlesDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode)
		{
			// (chapterId, title)
			List<Tuple2<Integer, String>> rawTitles = new ArrayList<>();
			rawTitles.add(new Tuple2<>(1, "How to find a better job"));
			rawTitles.add(new Tuple2<>(2, "Work faster harder smarter until you drop"));
			rawTitles.add(new Tuple2<>(3, "Content Creation is a Mug's Game"));
			return sc.parallelizePairs(rawTitles);
		}
		return sc.textFile("src/main/resources/viewing figures/titles.csv")
				                                    .mapToPair(commaSeparatedLine -> {
														String[] cols = commaSeparatedLine.split(",");
														return new Tuple2<Integer, String>(Integer.valueOf(cols[0]),cols[1]);
				                                    });
	}

	private static JavaPairRDD<Integer, Integer> setUpChapterDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode)
		{
			// (chapterId, (courseId, courseTitle))
			List<Tuple2<Integer, Integer>> rawChapterData = new ArrayList<>();
			rawChapterData.add(new Tuple2<>(96,  1));
			rawChapterData.add(new Tuple2<>(97,  1));
			rawChapterData.add(new Tuple2<>(98,  1));
			rawChapterData.add(new Tuple2<>(99,  2));
			rawChapterData.add(new Tuple2<>(100, 3));
			rawChapterData.add(new Tuple2<>(101, 3));
			rawChapterData.add(new Tuple2<>(102, 3));
			rawChapterData.add(new Tuple2<>(103, 3));
			rawChapterData.add(new Tuple2<>(104, 3));
			rawChapterData.add(new Tuple2<>(105, 3));
			rawChapterData.add(new Tuple2<>(106, 3));
			rawChapterData.add(new Tuple2<>(107, 3));
			rawChapterData.add(new Tuple2<>(108, 3));
			rawChapterData.add(new Tuple2<>(109, 3));
			return sc.parallelizePairs(rawChapterData);
		}

		return sc.textFile("src/main/resources/viewing figures/chapters.csv")
													  .mapToPair(commaSeparatedLine -> {
															String[] cols = commaSeparatedLine.split(",");
															return new Tuple2<Integer, Integer>(Integer.valueOf(cols[0]), Integer.valueOf(cols[1]));
													  	});
	}

	private static JavaPairRDD<Integer, Integer> setUpViewDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode)
		{
			// Chapter views - (userId, chapterId)
			List<Tuple2<Integer, Integer>> rawViewData = new ArrayList<>();
			rawViewData.add(new Tuple2<>(14, 96));
			rawViewData.add(new Tuple2<>(14, 97));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(14, 99));
			rawViewData.add(new Tuple2<>(13, 100));
			return  sc.parallelizePairs(rawViewData);
		}
		
		return sc.textFile("src/main/resources/viewing figures/views-*.csv")
				     .mapToPair(commaSeparatedLine -> {
				    	 String[] columns = commaSeparatedLine.split(",");
				    	 return new Tuple2<Integer, Integer>(Integer.valueOf(columns[0]), Integer.valueOf(columns[1]));
				     });
	}
}
