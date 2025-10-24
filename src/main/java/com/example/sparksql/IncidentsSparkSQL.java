package com.example.sparksql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class IncidentsSparkSQL {
    public static void main(String[] args) {
        System.out.println("Starting IncidentsSparkSQL application...");

        // 1. Créer SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("IncidentsParService")
                .master("local[*]")
                .getOrCreate();

        System.out.println("Spark session created successfully!");

        // 2. Lire le fichier CSV
        System.out.println("Loading incidents.csv...");
        Dataset<Row> incidents = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("src/main/resources/incident.csv");

        System.out.println("Data loaded successfully!");

        incidents.show();

        // 3. Afficher le nombre d’incidents par service
        System.out.println("=== Nombre d'incidents par service ===");
        Dataset<Row> incidentsParService = incidents.groupBy("service")
                .count()
                .orderBy(desc("count"));
        incidentsParService.show();

        // 4. Afficher les deux années avec le plus d’incidents
        System.out.println("=== Deux années avec le plus d'incidents ===");
        Dataset<Row> incidentsAvecAnnee = incidents.withColumn("annee", year(col("date")));
        Dataset<Row> topAnnees = incidentsAvecAnnee.groupBy("annee")
                .count()
                .orderBy(desc("count"))
                .limit(2);
        topAnnees.show();

        // Fermer SparkSession
        spark.stop();
    }
}
