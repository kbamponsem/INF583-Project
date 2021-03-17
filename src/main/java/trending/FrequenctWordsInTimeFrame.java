package trending;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import twitter4j.Status;
import twitter4j.TwitterObjectFactory;
import util.LoadTwitterData;
import java.io.IOException;
import java.util.Arrays;
import java.util.Vector;


public class FrequenctWordsInTimeFrame {
    public static void main(String[] args) throws IOException {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("TwitterAppLocal");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));

        JavaSparkContext jsc = new JavaSparkContext(jssc.ssc().sc());
        // Create a datasets folder in your project . Add the sampleTweets . json in the folder
        JavaDStream<String> stream = LoadTwitterData.loadData(jsc, jssc, "French/2020-02-01.json");

        JavaDStream<String> txtTweets = stream.map(s -> {
            Status tweet = TwitterObjectFactory.createStatus(s);
            return tweet.getText();
        });

        Vector<String> stopWords = LoadTwitterData.loadStopWords();
        txtTweets = txtTweets.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        txtTweets = txtTweets.flatMap(s -> {
            if (s.contains(","))
        })
        txtTweets = txtTweets.filter(s -> !stopWords.contains(s));
        txtTweets.foreachRDD(x->{
            x.collect().stream().limit(20).forEach(System.out::println);
        });

//        JavaDStream<String> wordsTweets = txtTweets.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
//        JavaDStream<String> hashtags = wordsTweets.filter(s->s.startsWith("#"));
//
//        JavaPairDStream<String, Integer> hashtagsPair = hashtags.mapToPair(s -> new Tuple2
//                <>(s, 1));
//        JavaPairDStream<String, Integer> hashtagsOcc = hashtagsPair.reduceByKey(Integer::sum);
//        JavaPairDStream<Integer, String> hashtagsOccReverse = hashtagsOcc.mapToPair(t ->
//                new Tuple2<>(t._2, t._1));
//        JavaPairDStream<Integer, String> sortedHashtags = hashtagsOccReverse.
//                transformToPair(rdd1 -> rdd1.sortByKey(false));
//        sortedHashtags.foreachRDD(x -> {
//            x.collect().stream().limit(10).forEach(System.out::println);
//        });

        jssc.start();

        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
