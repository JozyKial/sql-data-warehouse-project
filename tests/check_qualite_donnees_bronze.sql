/*
===============================================================================
Contrôles de Qualité
===============================================================================
Objectif du script :
    Ce script effectue des contrôles de qualité afin de valider l’intégrité, 
    la cohérence et l’exactitude de la couche Gold. Ces vérifications garantissent :
    - L’unicité des clés substitutives dans les tables de dimensions.
    - L’intégrité référentielle entre les tables de faits et de dimensions.
    - La validation des relations du modèle de données à des fins analytiques.

Notes d’utilisation :
    - Enquêter et résoudre toute anomalie détectée lors des contrôles.
===============================================================================
*/

-- ====================================================================
-- Vérification de 'gold.dim_customers'
-- ====================================================================
-- Vérification de l’unicité de la clé client dans gold.dim_customers
-- Attendu : Aucun résultat

SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Vérification de 'gold.product_key'
-- ====================================================================
-- Vérification de l’unicité de la clé produit dans gold.dim_products
-- Attendu : Aucun résultat 
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Vérification de 'gold.fact_sales'
-- ====================================================================
-- Vérification de la connectivité du modèle de données entre la table de faits et les dimensions
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL  
