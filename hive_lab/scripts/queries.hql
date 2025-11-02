-- ========================================
-- Script de requêtes analytiques
-- Lab 6 : Apache Hive - Hotel Booking
-- ========================================

USE hotel_booking;

-- ========================================
-- 5. Requêtes simples
-- ========================================

-- 5.1 Lister tous les clients
SELECT '5.1 - Tous les clients' AS requete;
SELECT * FROM clients;

-- 5.2 Lister tous les hôtels à Casablanca
SELECT '5.2 - Hotels a Casablanca' AS requete;
SELECT * FROM hotels WHERE ville = 'Casablanca';

-- 5.3 Lister toutes les réservations avec infos clients et hôtels
SELECT '5.3 - Reservations avec details' AS requete;
SELECT 
    r.reservation_id,
    c.nom AS client_nom,
    h.nom AS hotel_nom,
    h.ville,
    r.date_debut,
    r.date_fin,
    r.prix_total
FROM reservations r
JOIN clients c ON r.client_id = c.client_id
JOIN hotels h ON r.hotel_id = h.hotel_id;

-- ========================================
-- 6. Requêtes avec jointures
-- ========================================

-- 6.1 Nombre de réservations par client
SELECT '6.1 - Nombre de reservations par client' AS requete;
SELECT 
    c.nom,
    COUNT(r.reservation_id) AS nombre_reservations
FROM clients c
LEFT JOIN reservations r ON c.client_id = r.client_id
GROUP BY c.nom
ORDER BY nombre_reservations DESC;

-- 6.2 Clients ayant réservé plus de 2 nuitées
SELECT '6.2 - Clients avec plus de 2 nuitees' AS requete;
SELECT 
    c.nom,
    SUM(DATEDIFF(r.date_fin, r.date_debut)) AS total_nuitees
FROM clients c
JOIN reservations r ON c.client_id = r.client_id
GROUP BY c.nom
HAVING SUM(DATEDIFF(r.date_fin, r.date_debut)) > 2
ORDER BY total_nuitees DESC;

-- 6.3 Hôtels réservés par chaque client
SELECT '6.3 - Hotels reserves par client' AS requete;
SELECT 
    c.nom AS client_nom,
    COLLECT_SET(h.nom) AS hotels_reserves
FROM clients c
JOIN reservations r ON c.client_id = r.client_id
JOIN hotels h ON r.hotel_id = h.hotel_id
GROUP BY c.nom;

-- 6.4 Hôtels avec plus d'une réservation
SELECT '6.4 - Hotels avec plusieurs reservations' AS requete;
SELECT 
    h.nom,
    COUNT(r.reservation_id) AS nombre_reservations
FROM hotels h
JOIN reservations r ON h.hotel_id = r.hotel_id
GROUP BY h.nom
HAVING COUNT(r.reservation_id) > 1
ORDER BY nombre_reservations DESC;

-- 6.5 Hôtels sans réservation
SELECT '6.5 - Hotels sans reservation' AS requete;
SELECT h.nom
FROM hotels h
LEFT JOIN reservations r ON h.hotel_id = r.hotel_id
WHERE r.reservation_id IS NULL;

-- ========================================
-- 7. Requêtes imbriquées
-- ========================================

-- 7.1 Clients ayant réservé un hôtel avec plus de 4 étoiles
SELECT '7.1 - Clients avec hotels 5 etoiles' AS requete;
SELECT DISTINCT c.nom, c.email
FROM clients c
JOIN reservations r ON c.client_id = r.client_id
JOIN hotels h ON r.hotel_id = h.hotel_id
WHERE h.etoiles > 4;

-- 7.2 Total des revenus générés par chaque hôtel
SELECT '7.2 - Revenus par hotel' AS requete;
SELECT 
    h.nom,
    h.ville,
    h.etoiles,
    COALESCE(SUM(r.prix_total), 0) AS revenus_totaux
FROM hotels h
LEFT JOIN reservations r ON h.hotel_id = r.hotel_id
GROUP BY h.nom, h.ville, h.etoiles
ORDER BY revenus_totaux DESC;

-- ========================================
-- 8. Fonctions d'agrégation avec partitions et buckets
-- ========================================

-- 8.1 Revenus totaux par ville (table partitionnée)
SELECT '8.1 - Revenus par ville (partitionne)' AS requete;
SELECT 
    ville,
    SUM(r.prix_total) AS revenus_ville
FROM hotels_partitioned hp
JOIN reservations r ON hp.hotel_id = r.hotel_id
GROUP BY ville
ORDER BY revenus_ville DESC;

-- 8.2 Nombre de réservations par client (table bucketed)
SELECT '8.2 - Reservations par client (bucketed)' AS requete;
SELECT 
    c.nom,
    COUNT(rb.reservation_id) AS nombre_reservations,
    SUM(rb.prix_total) AS depenses_totales
FROM clients c
JOIN reservations_bucketed rb ON c.client_id = rb.client_id
GROUP BY c.nom
ORDER BY depenses_totales DESC;

-- ========================================
-- Statistiques avancées
-- ========================================

-- Top 3 des hôtels les plus rentables
SELECT 'Top 3 hotels les plus rentables' AS requete;
SELECT 
    h.nom,
    h.ville,
    COUNT(r.reservation_id) AS nb_reservations,
    SUM(r.prix_total) AS revenus_totaux,
    ROUND(AVG(r.prix_total), 2) AS revenu_moyen
FROM hotels h
JOIN reservations r ON h.hotel_id = r.hotel_id
GROUP BY h.nom, h.ville
ORDER BY revenus_totaux DESC
LIMIT 3;

-- Analyse par mois
SELECT 'Analyse des revenus par mois' AS requete;
SELECT 
    MONTH(date_debut) AS mois,
    COUNT(*) AS nb_reservations,
    SUM(prix_total) AS revenus_mois
FROM reservations
GROUP BY MONTH(date_debut)
ORDER BY mois;

-- Taux d'occupation par ville
SELECT 'Taux occupation par ville' AS requete;
SELECT 
    h.ville,
    COUNT(DISTINCT h.hotel_id) AS nb_hotels,
    COUNT(r.reservation_id) AS nb_reservations,
    ROUND(COUNT(r.reservation_id) * 1.0 / COUNT(DISTINCT h.hotel_id), 2) AS taux_occupation
FROM hotels h
LEFT JOIN reservations r ON h.hotel_id = r.hotel_id
GROUP BY h.ville
ORDER BY taux_occupation DESC;