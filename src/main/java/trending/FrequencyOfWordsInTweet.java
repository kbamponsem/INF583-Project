package trending;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.omg.IOP.Encoding;
import util.LoadTwitterData;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.apache.spark.sql.functions.*;

public class FrequencyOfWordsInTweet {
    public static void main(String[] args) throws IOException {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        ArrayList<String> stopWords = LoadTwitterData.loadStopWords();

        SparkSession spark = SparkSession.builder().appName("Project").config("spark.master", "local[*]").getOrCreate();
        spark.conf().set("spark.debug.maxToStringFields", 100);
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

        sqlContext.udf().register("makeWordArray", (String s1) ->
                Arrays.stream(s1.split(" "))
                        .map(String::toLowerCase)
                        .filter(s -> !stopWords.contains(s.toLowerCase().trim()))
                        .filter(s -> !(s.startsWith("https://") || s.startsWith("http://") || s.contains("https://")))
                        .toArray(), DataTypes.createArrayType(DataTypes.StringType));
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
                "English/Twitter-Day1.json",
                "English/Twitter-Day2.json",
                "English/Twitter-Day3.json"
//                "English/Twitter-Day4.json"
//                "English/Twitter-Day5.json",
//                "English/Twitter-Day6.json",
//                "English/Twitter-Day7.json"
        );

        Dataset<Row> words = tweets
                .select("text","")
                .withColumn("wordsInTweets", callUDF("makeWordArray", col("text")));

        Dataset<Row> tweetsWithAsim = words.withColumn("wordsIndicator", explode(col("wordsInTweets"))).filter(col("wordsIndicator").contains("asim"));

        StringBuilder asimTweets = new StringBuilder();
        StringBuilder asimWords = new StringBuilder();

        tweetsWithAsim.toLocalIterator().forEachRemaining(row -> {
            asimTweets.append(row.getString(0)).append("\n");
        });

        tweetsWithAsim.withColumn("tweetWords", explode(col("wordsInTweets"))).groupBy("tweetWords").count().toLocalIterator().forEachRemaining(row -> {
            asimWords.append(row.getString(0)).append(", ").append(row.getLong(1)).append("\n");
        });

        try{
//            Files.write(Paths.get("asimTweets.txt"), asimTweets.toString().getBytes(StandardCharsets.UTF_8));
//            Files.write(Paths.get("asimWordsFrequency.txt"), asimWords.toString().getBytes(StandardCharsets.UTF_8));

        }catch (Exception e){

        }
//        wordsWithAsim.groupBy(col("words")).count().orderBy(desc("count")).toLocalIterator().forEachRemaining(row->{
//
//                output.append(row.getString(0)).append(", ").append(String.valueOf(row.getLong(1))).append("\n");
//        });
//        try {
//            Files.write(Paths.get("frequencyFor#Asim.txt"), output.toString().getBytes(StandardCharsets.UTF_8));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        hashTags.filter(col("hashTags").eqNullSafe("#Asim")).limit(1).toLocalIterator().forEachRemaining(row -> {
//            System.out.println(row.get(0));
//            HashMap<String, Long> wordsCount = new HashMap<>();
//            words.withColumn("tweetWords", explode(col("wordsInTweets"))).groupBy(col("tweetWords")).count().orderBy(desc("count")).limit(20).toLocalIterator().forEachRemaining(word -> {
//                wordsCount.put(word.getString(0), word.getLong(1));
//            });
//
//            StringBuilder output = new StringBuilder();
//            wordsCount.forEach((x,y)->{
//                output.append(x).append(", ").append(y.toString()).append("\n");
//            });
//
//            try {
//                Files.write(Paths.get("frequencyFor#Asim.txt"), output.toString().getBytes(StandardCharsets.UTF_8));
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        });


    }

}
