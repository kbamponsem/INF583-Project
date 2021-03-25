package trending;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import util.LoadTwitterData;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.apache.spark.sql.functions.*;

public class AnomalyDetection {
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

        sqlContext.udf().register("getDayAndMonth", (String tweetTime) -> {
            DateTimeFormatter f = DateTimeFormatter.ofPattern("EEE MMM d HH:mm:ss ZZZ yyyy", Locale.US);
            ZonedDateTime zonedDateTime = ZonedDateTime.parse(tweetTime, f);
            return zonedDateTime.getDayOfWeek().toString() + " " + zonedDateTime.getMonth().toString();
        }, DataTypes.StringType);

        ArrayList<String> paths = new ArrayList<>();

        paths.add("English/Twitter-Day1.json");
        paths.add("English/Twitter-Day2.json");
        paths.add("English/Twitter-Day3.json");
        paths.add("English/Twitter-Day4.json");
        paths.add("English/Twitter-Day5.json");
        paths.add("English/Twitter-Day6.json");
        paths.add("English/Twitter-Day7.json");
        generateTop10HashTags(paths, spark);



    }

    static void generateTop10HashTags( ArrayList<String> paths, SparkSession spark) {
    paths.forEach(dataFile->{

        String outputFile = dataFile.substring(dataFile.indexOf("D"),dataFile.indexOf(".")).concat("-top-10-hashtag.json");
        System.out.println(outputFile);
        Dataset<Row> tweets = spark.read().json(dataFile);

        Dataset<Row> words = tweets
                .select("text", "created_at")
                .withColumn("timeInHours", callUDF("getTimeInHours", col("created_at")))
                .withColumn("timeInMinutes", callUDF("getTimeInMinutes", col("created_at")))
                .withColumn("dayAndMonth", callUDF("getDayAndMonth", col("created_at")))
                .withColumn("hashTagsInTweets", callUDF("makeInArray", col("text")));

        Dataset<Row> hashTags = words.withColumn("hashTags", explode(col("hashTagsInTweets")));

        HashMap<String, HashMap<String, long[]>> top10HashTags = new HashMap<>();

        hashTags.groupBy(col("hashTags")).count().orderBy(desc("count")).limit(10).toLocalIterator().forEachRemaining(row -> {
            Dataset<Row> hashTagDataSet = hashTags.filter(col("hashTags").eqNullSafe(row.get(0)));
            long[] hours = new long[24];
            String hashTag = row.getString(0);

            hashTagDataSet.groupBy("dayAndMonth").count().orderBy(desc("count")).toLocalIterator().forEachRemaining(x -> {
                Dataset<Row> dayAndMonthDataSet = hashTagDataSet.filter(col("dayAndMonth").eqNullSafe(x.get(0)));
                String dayAndMonth = x.getString(0);

                dayAndMonthDataSet.groupBy(col("timeInHours")).count().toLocalIterator().forEachRemaining(y -> {
                    int hour = y.getInt(0);
                    long count = y.getLong(1);
                    hours[hour] = count;
                    HashMap<String, long[]> value = new HashMap<>();
                    value.put(dayAndMonth, hours);
                    top10HashTags.put(hashTag, value);
                });
            });
        });

        StringBuilder output = new StringBuilder();
        output.append("{").append("\n\t");
        top10HashTags.forEach((x, y) -> {
            output.append("\"").append(x).append("\": {").append("\n\t\t\t");
            y.forEach((d,c)->{
                output.append("\"").append(d).append("\": ").append(Arrays.toString(Arrays.stream(c).toArray())).append("\n");
                output.append("\t\t").append("},").append("\n");
            });
        });
        output.append("}").append("\n");

        System.out.println(output.toString());
        try {
            Files.write(Paths.get(outputFile), output.toString().getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
        }

    });

    }
}
/*
    {
        "#something": [{0:2},{1:4},...,{24:20}],
        "#somethingelse": []
    }
*/
