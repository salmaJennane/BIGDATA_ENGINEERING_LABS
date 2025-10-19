package edu.ensias.hadoop;

import java.util.Arrays;

public class Main {
    
    public static void main(String[] args) throws Exception {
        
        if (args.length == 0) {
            afficherUsage();
            System.exit(1);
        }

        String command = args[0];
        String[] cmdArgs = Arrays.copyOfRange(args, 1, args.length);

        System.out.println("========================================");
        System.out.println("   HADOOP LAB - Big Data Engineering");
        System.out.println("========================================");
        System.out.println("Commande : " + command);
        System.out.println("========================================\n");

        switch (command.toLowerCase()) {
            case "filestatus":
                System.out.println(">>> Exécution : HadoopFileStatus");
                HadoopFileStatus.main(cmdArgs);
                break;

            case "read":
                System.out.println(">>> Exécution : ReadHDFS");
                ReadHDFS.main(cmdArgs);
                break;

            case "write":
                System.out.println(">>> Exécution : WriteHDFS");
                WriteHDFS.main(cmdArgs);
                break;

            case "wordcount":
                System.out.println(">>> Exécution : WordCount MapReduce");
                WordCount.main(cmdArgs);
                break;

            default:
                System.err.println("❌ Commande inconnue : " + command);
                afficherUsage();
                System.exit(1);
        }

        System.out.println("\n========================================");
        System.out.println("   Exécution terminée avec succès ✅");
        System.out.println("========================================");
    }

    private static void afficherUsage() {
        System.out.println("\n╔════════════════════════════════════════════════════════════════╗");
        System.out.println("║          HADOOP LAB - Guide d'utilisation                     ║");
        System.out.println("╚════════════════════════════════════════════════════════════════╝\n");
        
        System.out.println("Usage : hadoop jar hadoop-app.jar <commande> [arguments]\n");
        
        System.out.println("📋 COMMANDES DISPONIBLES :\n");
        
        System.out.println("1️⃣  filestatus <chemin> <fichier> <nouveau_nom>");
        System.out.println("   └─ Affiche les informations d'un fichier et le renomme");
        System.out.println("   └─ Exemple : hadoop jar hadoop-app.jar filestatus /user/root/input data.txt data_new.txt\n");
        
        System.out.println("2️⃣  read <chemin_complet_fichier>");
        System.out.println("   └─ Lit et affiche le contenu d'un fichier HDFS");
        System.out.println("   └─ Exemple : hadoop jar hadoop-app.jar read /user/root/input/data.txt\n");
        
        System.out.println("3️⃣  write <chemin_fichier> <contenu>");
        System.out.println("   └─ Crée un nouveau fichier sur HDFS avec le contenu spécifié");
        System.out.println("   └─ Exemple : hadoop jar hadoop-app.jar write /user/root/input/test.txt \"Bonjour Hadoop\"\n");
        
        System.out.println("4️⃣  wordcount <fichier_entree> <dossier_sortie>");
        System.out.println("   └─ Lance le job MapReduce WordCount");
        System.out.println("   └─ Exemple : hadoop jar hadoop-app.jar wordcount /user/root/input/data.txt /user/root/output\n");
        
        System.out.println("═══════════════════════════════════════════════════════════════");
        System.out.println("💡 ASTUCE : Le dossier de sortie ne doit PAS exister pour WordCount");
        System.out.println("    Utilisez : hdfs dfs -rm -r /user/root/output (si nécessaire)");
        System.out.println("═══════════════════════════════════════════════════════════════\n");
    }
}