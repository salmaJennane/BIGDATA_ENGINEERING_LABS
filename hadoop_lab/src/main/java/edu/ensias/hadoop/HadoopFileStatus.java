package edu.ensias.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;

public class HadoopFileStatus {
    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.err.println("Usage: HadoopFileStatus <path> <filename> <newfilename>");
            System.exit(1);
        }

        String hdfsPath = args[0];
        String fileName = args[1];
        String newFileName = args[2];

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path filePath = new Path(hdfsPath + "/" + fileName);

        if (!fs.exists(filePath)) {
            System.out.println("Le fichier " + fileName + " n'existe pas !");
            return;
        }

        // Afficher les métadonnées
        FileStatus status = fs.getFileStatus(filePath);
        System.out.println("=== Informations sur le fichier ===");
        System.out.println("Nom : " + status.getPath().getName());
        System.out.println("Propriétaire : " + status.getOwner());
        System.out.println("Taille (octets) : " + status.getLen());
        System.out.println("Facteur de réplication : " + status.getReplication());
        System.out.println("Bloc size : " + status.getBlockSize());
        System.out.println("Permissions : " + status.getPermission());

        // Renommer le fichier
        Path newPath = new Path(hdfsPath + "/" + newFileName);
        if (fs.rename(filePath, newPath)) {
            System.out.println("Fichier renommé en : " + newFileName);
        } else {
            System.out.println("Erreur lors du renommage !");
        }

        fs.close();
    }
}
