-- ========================================
-- Script de création de la base de données et des tables
-- Lab 6 : Apache Hive - Hotel Booking
-- ========================================

-- Supprimer la base de données si elle existe
DROP DATABASE IF EXISTS hotel_booking CASCADE;

-- Créer la base de données
CREATE DATABASE hotel_booking;

-- Utiliser la base de données
USE hotel_booking;

-- Configurer les propriétés pour partitions et buckets
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=20000;
SET hive.exec.max.dynamic.partitions.pernode=20000;
SET hive.enforce.bucketing=true;

-- ========================================
-- Création des tables principales
-- ========================================

-- Table clients
CREATE TABLE clients (
    client_id INT,
    nom STRING,
    email STRING,
    telephone STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Table hotels
CREATE TABLE hotels (
    hotel_id INT,
    nom STRING,
    etoiles INT,
    ville STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Table reservations
CREATE TABLE reservations (
    reservation_id INT,
    client_id INT,
    hotel_id INT,
    date_debut STRING,
    date_fin STRING,
    prix_total DECIMAL(10,2)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- ========================================
-- Création des tables optimisées
-- ========================================

-- Table hotels partitionnée par ville
CREATE TABLE hotels_partitioned (
    hotel_id INT,
    nom STRING,
    etoiles INT
)
PARTITIONED BY (ville STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Table reservations avec buckets
CREATE TABLE reservations_bucketed (
    reservation_id INT,
    client_id INT,
    hotel_id INT,
    date_debut STRING,
    date_fin STRING,
    prix_total DECIMAL(10,2)
)
CLUSTERED BY (client_id) INTO 4 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Afficher les tables créées
SHOW TABLES;