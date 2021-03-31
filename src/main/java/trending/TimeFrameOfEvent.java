package trending;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import util.LoadTwitterData;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static org.apache.spark.sql.functions.*;

public class TimeFrameOfEvent {
    public static void main(String[] args) throws IOException {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        ArrayList<String> stopWords = LoadTwitterData.loadStopWords();

        SparkSession spark = SparkSession.builder().appName("Project").config("spark.master", "local[*]").getOrCreate();
        SQLContext sqlContext = new SQLContext(spark);
        sqlContext.udf().register("isNotIn",
                (String s1) -> stopWords.contains(s1.toLowerCase().trim())
                        || s1.length() < 2
                        || s1.startsWith("&")
                        || s1.contains("'"),
                DataTypes.BooleanType);

        sqlContext.udf().register("hashTag", (String s1) -> s1.startsWith("#"), DataTypes.BooleanType);

        sqlContext.udf().register("mention", (String s1) -> s1.startsWith("@"), DataTypes.BooleanType);

        sqlContext.udf().register("makeInArray", (String s1) ->
                Arrays.stream(s1.split(" "))
                        .filter(s -> !stopWords.contains(s.toLowerCase().trim()))
                        .filter(s -> !(s.startsWith("https://") || s.startsWith("http://") || s.contains("https://")) && s.length() > 3)
                        .filter(s -> s.startsWith("#")).toArray(), DataTypes.createArrayType(DataTypes.StringType));
//        sqlContext.udf().register("emptyArray", (String s1) -> s1, DataTypes.BooleanType);

        sqlContext.udf().register("getTimeInHours", (String tweetTime) -> {
            DateTimeFormatter f = DateTimeFormatter.ofPattern("EEE MMM d HH:mm:ss ZZZ yyyy", Locale.US);
            ZonedDateTime zonedDateTime = ZonedDateTime.parse(tweetTime, f);
            return zonedDateTime.getHour();
        }, DataTypes.IntegerType);

        sqlContext.udf().register("getTimeInMinutes", (String tweetTime) -> {
            DateTimeFormatter f = DateTimeFormatter.ofPattern("EEE MMM d HH:mm:ss ZZZ yyyy", Locale.US);
            ZonedDateTime zonedDateTime = ZonedDateTime.parse(tweetTime, f);
            return zonedDateTime.getMinute() + (zonedDateTime.getHour() * 60);
        }, DataTypes.IntegerType);

        Dataset<Row> tweets = spark.read().json(
                "English/Twitter-Day1.json"
//                "English/Twitter-Day2.json",
//                "English/Twitter-Day3.json",
//                "English/Twitter-Day4.json"
//                "English/Twitter-Day5.json",
//                "English/Twitter-Day6.json",
//                "English/Twitter-Day7.json"
        );

        Dataset<Row> words = tweets
                .select("text", "created_at")
                .withColumn("timeInHours", callUDF("getTimeInHours", col("created_at")))
                .withColumn("timeInMinutes", callUDF("getTimeInMinutes", col("created_at")))
                .withColumn("hashTagsInTweets", callUDF("makeInArray", col("text")));

        Dataset<Row> hashTags = words.withColumn("hashTags", explode(col("hashTagsInTweets")));
        hashTags.select("timeInHours", "timeInMinutes", "hashTags")
//                .filter(col("timeInHours").between(8,10))
                .sort(col("hashTags"))
                .groupBy(col("hashTags")).count()
                .orderBy(desc("count")).limit(1).toLocalIterator().forEachRemaining(row -> {

            List<Row> counts = hashTags.filter(col("hashTags").eqNullSafe(row.get(0)))
                    .groupBy(col("created_at")).count().orderBy(desc("created_at")).collectAsList();

            if (!counts.isEmpty()) {
                Row maximum = counts.get(0);
                Row minimum = counts.get(counts.size() - 1);
                System.out.println(row.getString(0) + " started at " + minimum.getString(0) + " and ended at " + maximum.getString(0));
            }

        });

//        hashTags
//                .select("timeInHours", "timeInMinutes", "hashTags")
//                .filter(col("hashTags").equalTo(event.toString()))
//                .groupBy(col("hashTags")).max("created_at")
//                .show(1000);
//
//        hashTags
//                .select("timeInHours", "timeInMinutes", "hashTags")
//                .filter(col("hashTags").equalTo(event.toString()))
//                .groupBy(col("hashTags")).min("created_at")
//                .show(1000);

    }
}
