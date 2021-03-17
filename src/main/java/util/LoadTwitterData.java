package util;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Vector;

public class LoadTwitterData {
    public static JavaDStream<String> loadData(JavaSparkContext jsc, JavaStreamingContext jssc, String fileName) throws IOException {
        FileReader fr = new FileReader(fileName);
        BufferedReader br = new BufferedReader(fr);
        String line;
        int count = 0;
        ArrayList<String> batch = new ArrayList<>();
        Queue<JavaRDD<String>> rdds = new LinkedList<>();

        while ((line = br.readLine()) != null) {
            count += 1;
            if (count == 10) {
                JavaRDD<String> rdd = jsc.parallelize(batch);
                rdds.add(rdd);
                batch = new ArrayList<>();
                count = 0;
            }

            batch.add(line);
        }

        JavaRDD<String> rdd = jsc.parallelize(batch);
        rdds.add(rdd);

        return jssc.queueStream(rdds, true);
    }

    public static Vector<String> loadStopWords() throws IOException {
        FileReader fr = new FileReader("stop_words_french.txt");
        BufferedReader br = new BufferedReader(fr);
        String line;
        Vector<String> words = new Vector<>();

        while ((line = br.readLine()) != null) {
            words.add(line);
        }

        return words;
    }

}
