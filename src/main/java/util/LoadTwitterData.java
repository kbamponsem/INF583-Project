package util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Vector;

public class LoadTwitterData {
    public static ArrayList<String> loadStopWords() throws IOException {
        FileReader fr = new FileReader("English/stop_words_english.txt");
        BufferedReader br = new BufferedReader(fr);
        String line;
        ArrayList<String> words = new ArrayList<>();

        while ((line = br.readLine()) != null) {
            words.add(line.toLowerCase().trim());
        }

        return words;
    }

}
