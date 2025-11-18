/*
===============================================================================
Script DDL : Création des Vues Gold
===============================================================================
Objectif du Script :
    Ce script crée les vues de la couche Gold dans l’entrepôt de données.
    La couche Gold représente les tables finales de dimensions et de faits 
    (modèle en étoile – Star Schema).

    Chaque vue effectue des transformations et combine les données de la 
    couche Silver afin de produire un jeu de données propre, enrichi 
    et prêt pour un usage métier.

Utilisation :
    - Ces vues peuvent être interrogées directement pour l’analyse 
      et le reporting.
===============================================================================
*/

-- =============================================================================
-- Creation Dimension: gold.dim_customers
-- =============================================================================

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
	DROP VIEW gold.dim_customers ;
GO

CREATE VIEW gold.dim_customers AS
	SELECT
		ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key, -- génération d'un clé de substitution pour cette table de dimension
		ci.cst_id				AS customer_id,
		ci.cst_key				AS customer_number,
		ci.cst_firstname		AS first_name,
		ci.cst_lastname			AS last_name,
		la.cntry				AS country,
		ci.cst_material_status	AS marital_status,
		CASE WHEN LOWER(ci.cst_gndr) NOT IN ('n/a','','na') THEN ci.cst_gndr -- CRM est le maitre pour les infos concernant le genre
			ELSE COALESCE(ca.gen, 'n/a')
		END AS gender,
		ci.cst_create_date		AS create_date,
		ca.bdate				AS birthday
	FROM silver.crm_cust_info ci
	LEFT JOIN silver.erp_cust_az12 ca
	ON ci.cst_key = ca.cid
	LEFT JOIN silver.erp_loc_a101 la
	ON ci.cst_key = la.cid
  
-- =============================================================================
-- Creation Dimension: gold.dim_products
-- =============================================================================

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
WHERE pn.prd_end_dt IS NULL -- filtrer toutes les données historiques

-- =============================================================================
-- Creation Fact Table: gold.fact_sales
-- =============================================================================

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


