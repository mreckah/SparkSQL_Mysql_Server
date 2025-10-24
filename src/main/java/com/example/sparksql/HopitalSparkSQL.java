package com.example.sparksql;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;

import java.util.Properties;

public class HopitalSparkSQL {
        public static void main(String[] args) {
                System.out.println("Starting HopitalSparkSQL application...");

                // 1. Créer la session Spark
                SparkSession spark = SparkSession.builder()
                                .appName("HopitalDataProcessing")
                                .master("local[*]")
                                .getOrCreate();

                System.out.println("Spark session created successfully!");

                // 2. Paramètres de connexion MySQL
                String jdbcUrl = "jdbc:mysql://localhost:4306/DB_HOPITAL?useSSL=false&serverTimezone=UTC";
                Properties connectionProps = new Properties();
                connectionProps.put("user", "root"); // Change this to your MySQL username
                connectionProps.put("password", ""); // Change this to your MySQL password
                connectionProps.put("driver", "com.mysql.cj.jdbc.Driver");
                connectionProps.put("allowPublicKeyRetrieval", "true");

                // 3. Charger les tables MySQL dans des DataFrames
                System.out.println("Loading data from MySQL...");
                Dataset<Row> patientsDF = spark.read().jdbc(jdbcUrl, "PATIENTS", connectionProps);
                Dataset<Row> medecinsDF = spark.read().jdbc(jdbcUrl, "MEDECINS", connectionProps);
                Dataset<Row> consultationsDF = spark.read().jdbc(jdbcUrl, "CONSULTATIONS", connectionProps);
                System.out.println("Data loaded successfully!");

                // Afficher les données (optionnel)
                System.out.println("\n=== PATIENTS ===");
                patientsDF.show();
                System.out.println("\n=== MEDECINS ===");
                medecinsDF.show();
                System.out.println("\n=== CONSULTATIONS ===");
                consultationsDF.show();

                // -----------------------------
                // a) Nombre de consultations par jour
                System.out.println("=== Nombre de consultations par jour ===");
                consultationsDF.groupBy("date_consultation")
                                .agg(count("*").alias("nb_consultations"))
                                .orderBy("date_consultation")
                                .show();

                // b) Nombre de consultations par médecin
                System.out.println("=== Nombre de consultations par médecin ===");
                consultationsDF.join(medecinsDF, "id_medecin")
                                .groupBy("nom", "prenom")
                                .agg(count("*").alias("NOMBRE_DE_CONSULTATION"))
                                .orderBy(desc("NOMBRE_DE_CONSULTATION"))
                                .show();

                // c) Nombre de patients distincts par médecin
                System.out.println("=== Nombre de patients distincts par médecin ===");
                consultationsDF.join(medecinsDF, "id_medecin")
                                .groupBy("nom", "prenom")
                                .agg(countDistinct("id_patient").alias("NB_PATIENTS"))
                                .orderBy(desc("NB_PATIENTS"))
                                .show();

                // 4. Stop Spark session
                spark.stop();
        }
}
