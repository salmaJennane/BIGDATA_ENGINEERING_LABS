package edu.ensias.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ReadHDFS {
    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Usage: ReadHDFS <filepath>");
            System.err.println("Exemple: ReadHDFS /user/root/input/achats.txt");
            System.exit(1);
        }

        String filepath = args[0];

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path nomcomplet = new Path(filepath);

        if (!fs.exists(nomcomplet)) {
            System.out.println("Le fichier " + filepath + " n'existe pas sur HDFS !");
            fs.close();
            System.exit(1);
        }

        System.out.println("=== Contenu du fichier: " + filepath + " ===\n");

        FSDataInputStream inStream = fs.open(nomcomplet);
        InputStreamReader isr = new InputStreamReader(inStream);
        BufferedReader br = new BufferedReader(isr);
        
        String line;
        int lineNumber = 1;
        while ((line = br.readLine()) != null) {
            System.out.println(lineNumber + ": " + line);
            lineNumber++;
        }

        br.close();
        inStream.close();
        fs.close();

        System.out.println("\n=== Fin du fichier ===");
    }
}