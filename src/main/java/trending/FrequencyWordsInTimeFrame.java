package trending;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.types.DataTypes;
import util.LoadTwitterData;


import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.spark.sql.functions.*;


public class FrequencyWordsInTimeFrame {

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

        sqlContext.udf().register("getTimeInHours", (String tweetTime) -> {
            DateTimeFormatter f = DateTimeFormatter.ofPattern("EEE MMM d HH:mm:ss ZZZ yyyy", Locale.US);
            ZonedDateTime zonedDateTime = ZonedDateTime.parse(tweetTime, f);
            return zonedDateTime.getHour();
        }, DataTypes.IntegerType);

        sqlContext.udf().register("getTimeInMinutes", (String tweetTime) -> {
            DateTimeFormatter f = DateTimeFormatter.ofPattern("EEE MMM d HH:mm:ss ZZZ yyyy", Locale.US);
            ZonedDateTime zonedDateTime = ZonedDateTime.parse(tweetTime, f);
            return zonedDateTime.getMinute();
        }, DataTypes.IntegerType);

        // Reading a JSON file
        Dataset<Row> tweets = spark.read().json("English/Twitter-Day7.json");

        Dataset<Row> tweetsWithTime = tweets.select(col("text"), col("created_at"))
                .withColumn("timeInHours", callUDF("getTimeInHours", col("created_at")))
                .withColumn("timeInMinutes", callUDF("getTimeInMinutes", col("created_at")));

//        tweetsWithTime.filter(col("timeInHours").equalTo(22)).show(10);
        Dataset<Row> tweetsWithTimeFrame = tweetsWithTime.
                filter(col("timeInHours").between(0, 23));
        Dataset<Row> tweetsWords = tweetsWithTimeFrame.withColumn("words", split(tweets.col("text"), " "));

        Dataset<Row> words = tweetsWords
                .withColumn("words_separated", explode(tweetsWords.col("words")))
                .select("words_separated");

        Dataset<Row> absentWords = words
                .withColumn("removed", callUDF("isNotIn", col("words_separated")))
                .select("words_separated", "removed");

        //
        Dataset<Row> hashTagCounts = absentWords
                .filter(col("removed").equalTo(false))
//                .filter(callUDF("hashTag", col("words_separated")))
                .groupBy(col("words_separated")).count();

        hashTagCounts.orderBy(desc("count")).show(10);

//        hashTagCounts.orderBy(desc("count")).show(50);

    }


}
