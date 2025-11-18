/*
===============================================================================
Vérifications de la Qualité (Quality Checks)
===============================================================================
Objectif du Script :
    Dans ce script j'effectue des vérifications de la qualité pour valider l'intégrité, la cohérence,
    et l'exactitude de la Couche Gold (Couche Or). Ces vérifications assurent :
    - L'unicité des clés de substitution (surrogate keys) dans les tables de dimension.
    - L'intégrité référentielle entre les tables de faits et les tables de dimension.
    - La validation des relations dans le modèle de données à des fins analytiques.

Notes d'Utilisation :
    - Examiner et résoudre toute incohérence trouvée lors des vérifications.
===============================================================================
*/

-- # 1. Conception de la Dimension Clients (dim_customers)
-- But : Visualiser l'ensemble des données clients agrégées en joignant les sources CRM, ERP (détails) et ERP (localisation) de la couche Silver.
SELECT
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_material_status,
	ci.cst_gndr,
	ci.cst_create_date,
	ca.bdate,
	ca.gen,
	la.cntry
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid

-- But : Vérifier l'existence de doublons sur la clé primaire 'cst_id' dans l'ensemble de données client jointes.
SELECT cst_id, COUNT(*) FROM 
(
	SELECT
		ci.cst_id,
		ci.cst_key,
		ci.cst_firstname,
		ci.cst_lastname,
		ci.cst_material_status,
		ci.cst_gndr,
		ci.cst_create_date,
		ca.bdate,
		ca.gen,
		la.cntry
	FROM silver.crm_cust_info ci
	LEFT JOIN silver.erp_cust_az12 ca
	ON ci.cst_key = ca.cid
	LEFT JOIN silver.erp_loc_a101 la
	ON ci.cst_key = la.cid
)t GROUP BY cst_id
HAVING COUNT(*)>1

-- But : Visualiser les valeurs distinctes du genre provenant du CRM et de l'ERP pour définir la logique d'harmonisation (en donnant la priorité au CRM).
SELECT DISTINCT
	ci.cst_gndr,
	ca.gen,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr 
		ELSE COALESCE(ca.gen, 'n/a')
	END AS new_gen	
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON		ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON		ci.cst_key = la.cid
ORDER BY 1,2

-- But : Définir la structure finale de la dimension Clients en renommant les colonnes, appliquant la logique d'harmonisation du genre et générant la clé de substitution (`customer_key`).
SELECT
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
	ci.cst_id				AS customer_id,
	ci.cst_key				AS customer_number,
	ci.cst_firstname		AS first_name,
	ci.cst_lastname			AS last_name,
	la.cntry				AS country,
	ci.cst_material_status	AS marital_status,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
		ELSE COALESCE(ca.gen, 'n/a')
	END AS gender,
	ci.cst_create_date		AS create_date,
	ca.bdate				AS birthday
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid

-- But : Créer ou recréer la Vue `gold.dim_customers` contenant la dimension Clients finale, nettoyée et modélisée.
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
	DROP VIEW gold.dim_customers ;
GO

CREATE VIEW gold.dim_customers AS
	SELECT
		ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,
		ci.cst_id				AS customer_id,
		ci.cst_key				AS customer_number,
		ci.cst_firstname		AS first_name,
		ci.cst_lastname			AS last_name,
		la.cntry				AS country,
		ci.cst_material_status	AS marital_status,
		CASE WHEN LOWER(ci.cst_gndr) NOT IN ('n/a','','na') THEN ci.cst_gndr
			ELSE COALESCE(ca.gen, 'n/a')
		END AS gender,
		ci.cst_create_date		AS create_date,
		ca.bdate				AS birthday
	FROM silver.crm_cust_info ci
	LEFT JOIN silver.erp_cust_az12 ca
	ON ci.cst_key = ca.cid
	LEFT JOIN silver.erp_loc_a101 la
	ON ci.cst_key = la.cid

SELECT * FROM gold.dim_customers

-- # 2. Conception de la Dimension Produits (dim_products)
-- But : Vérifier l'unicité de la clé métier 'prd_key' pour les produits qui sont actuellement actifs (prd_end_dt IS NULL), en joignant les données CRM et ERP.
SELECT prd_key, COUNT(*) FROM (
	SELECT
		pn.prd_id,
		pn.prd_key,
		pn.prd_nm,
		pn.cat_id,
		pc.cat,
		pc.subcat,
		pc.maintenance,
		pn.prd_cost,
		pn.prd_line,
		pn.prd_start_dt AS start_date
	FROM silver.crm_prd_info pn
	LEFT JOIN silver.erp_px_cat_g1v2 pc
	ON	pn.cat_id = pc.id
	WHERE pn.prd_end_dt IS NULL
)t GROUP BY prd_key
HAVING COUNT(*) > 1


-- But : Créer ou recréer la Vue `gold.dim_products` contenant la dimension Produits, en filtrant les produits historiques et en générant la clé de substitution (`product_key`).
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
	DROP VIEW gold.dim_products ;
GO

CREATE VIEW gold.dim_products AS
SELECT
	ROW_NUMBER() OVER (
		ORDER BY pn.prd_start_dt, pn.prd_key
	)			AS product_key,
	pn.prd_id	AS product_id,
	pn.prd_key	AS product_number,
	pn.prd_nm	AS product_name,
	pn.cat_id	AS category_id,
	pc.cat		AS category,
	pc.subcat	AS subcategory,
	pc.maintenance,
	pn.prd_cost AS cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON	pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL

SELECT * FROM gold.dim_products

-- But : Vérification supplémentaire de l'existence de doublons sur la clé produit pour les enregistrements qui seront inclus dans la dimension.
SELECT prd_key, COUNT(*) FROM
(
SELECT
	pn.prd_id,
	pn.cat_id,
	pn.prd_key,
	pn.prd_nm,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt,
	pc.cat,
	pc.subcat,
	pc.maintenance
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON	pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL
)t GROUP BY prd_key
HAVING COUNT(*) > 1

-- # 3. Conception de la Table de Faits Ventes (fact_sales)
-- But : Créer ou recréer la Vue `gold.fact_sales` qui constitue la table de faits Ventes, en joignant les données de transaction (Silver) aux clés de substitution des dimensions Clients et Produits (Gold).
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
	DROP VIEW gold.fact_sales ;
GO

CREATE VIEW gold.fact_sales AS
SELECT
	sd.sls_ord_num	AS order_number,
	pr.product_key,
	cu.customer_key,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt	AS shipping_date,
	sd.sls_due_dt	AS due_date,
	sd.sls_sales	AS sales_amount,
	sd.sls_quantity AS quantity,
	sd.sls_price	AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
	ON	sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
	ON	sd.sls_cust_id = cu.customer_id


SELECT * FROM gold.fact_sales

-- # 4. Vérification de l'Intégrité des Clés Étrangères (Tests)
-- But : Tester l'intégrité référentielle en identifiant les enregistrements dans la table de faits Ventes qui n'ont pas de client correspondant dans la dimension Clients.
SELECT * FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
WHERE c.customer_key IS NULL

-- But : Tester l'intégrité référentielle en identifiant les enregistrements dans la table de faits Ventes qui n'ont pas de produit correspondant dans la dimension Produits.
SELECT * FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL
