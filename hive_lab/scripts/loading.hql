-- ========================================
-- Script de chargement des données
-- Lab 6 : Apache Hive - Hotel Booking
-- ========================================

-- Utiliser la base de données
USE hotel_booking;

-- ========================================
-- Chargement des données dans les tables principales
-- ========================================

LOAD DATA LOCAL INPATH '/shared_volume/clients.txt' INTO TABLE clients;
LOAD DATA LOCAL INPATH '/shared_volume/hotels.txt' INTO TABLE hotels;
LOAD DATA LOCAL INPATH '/shared_volume/reservations.txt' INTO TABLE reservations;

-- Vérifier le chargement
SELECT 'Nombre de clients' AS info, COUNT(*) AS total FROM clients
UNION ALL
SELECT 'Nombre hotels' AS info, COUNT(*) AS total FROM hotels
UNION ALL
SELECT 'Nombre reservations' AS info, COUNT(*) AS total FROM reservations;

-- ========================================
-- Chargement des tables optimisées
-- ========================================

-- Charger hotels_partitioned depuis hotels
INSERT INTO TABLE hotels_partitioned PARTITION(ville)
SELECT hotel_id, nom, etoiles, ville FROM hotels;

-- Charger reservations_bucketed depuis reservations
INSERT INTO TABLE reservations_bucketed
SELECT * FROM reservations;

-- Vérifier les partitions créées
SHOW PARTITIONS hotels_partitioned;

-- Afficher quelques lignes pour vérification
SELECT 'Apercu clients' AS info;
SELECT * FROM clients LIMIT 3;

SELECT 'Apercu hotels' AS info;
SELECT * FROM hotels LIMIT 3;

SELECT 'Apercu reservations' AS info;
SELECT * FROM reservations LIMIT 3;