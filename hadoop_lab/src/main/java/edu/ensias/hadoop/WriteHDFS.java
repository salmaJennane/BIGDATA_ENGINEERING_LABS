package edu.ensias.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class WriteHDFS {
    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println("Usage: WriteHDFS <filepath> <content>");
            System.err.println("Exemple: WriteHDFS /user/root/input/test.txt \"Bonjour Hadoop!\"");
            System.exit(1);
        }

        String filepath = args[0];
        String content = args[1];

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path nomcomplet = new Path(filepath);

        // Vérifier si le fichier existe déjà
        if (fs.exists(nomcomplet)) {
            System.out.println("⚠️  Le fichier " + filepath + " existe déjà !");
            System.out.println("Voulez-vous le remplacer ? Supprimez-le d'abord avec:");
            System.out.println("hdfs dfs -rm " + filepath);
            fs.close();
            System.exit(1);
        }

        System.out.println("=== Création du fichier: " + filepath + " ===");

        // Créer le fichier et écrire le contenu
        FSDataOutputStream outStream = fs.create(nomcomplet);
        outStream.writeUTF(content);
        
        // Écrire des lignes supplémentaires si vous voulez
        outStream.writeUTF("\nFichier créé avec succès via l'API HDFS Java!");
        
        outStream.close();
        fs.close();

        System.out.println("✅ Fichier créé avec succès !");
        System.out.println("Pour le lire : hadoop jar /root/ReadHDFS.jar " + filepath);
        System.out.println("Ou avec shell : hdfs dfs -cat " + filepath);
    }
}