use dbDatawarehouse

-- 1. Vérifier la présence de valeurs nulles ou de doublons dans la clé primaire
-- Attente : Aucun résultat

SELECT 
cst_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*)>1 OR cst_id IS NULL ;

SELECT 
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*)>1 OR cst_id IS NULL ;

-- 2. je trie les données de crm_cust_info dont cst_id = 29466, soupçon de doublon

SELECT
*,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
WHERE cst_id = 29466;

-- 3. vérifier la présence d’espaces indésirables
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

-- 4. normalisation et cohérence des données
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info ;

SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info ;


-- 5. nettoyage de la table avec les bonnes données
SELECT
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE	WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
		WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
		ELSE 'n/a'
END cst_material_status,
CASE	WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		ELSE 'n/a'
END cst_gndr,
cst_create_date
FROM (
	SELECT
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
) t WHERE flag_last = 1

--- 6. Après avoir nettoyer (étape 5), je peux maintenant insérer dans silver.crm_cust_info

PRINT '>> Truncatin table : silver.crm_cust_info';
TRUNCATE TABLE silver.crm_cust_info;
PRINT '>> Inserting data into : silver.crm_cust_info';
INSERT INTO silver.crm_cust_info(
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_material_status,
	cst_gndr,
	cst_create_date)
SELECT
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	CASE	WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
			WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
			ELSE 'n/a'
	END cst_material_status,
	CASE	WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			ELSE 'n/a'
	END cst_gndr,
	cst_create_date
FROM 
(
	SELECT
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
) 
t WHERE flag_last = 1

---- ====================== ----
		-- 2nd table ---
---- ====================== ----

SELECT
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-','_') AS cat_id, -- je retire 5 caractère en partant du 1er dans la chaine prd_key et je remplace - par _
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm,
	ISNULL(prd_cost,0) AS prd_cost,
	CASE UPPER(TRIM(prd_line))
		 WHEN 'M' THEN 'Mountain'
		 WHEN 'R' THEN 'Road'
		 WHEN 'S' THEN 'Other Sales'
		 WHEN 'T' THEN 'Touring'
		 ELSE 'n/a'
	END AS prd_line,
	CAST (prd_start_dt AS DATE) AS prd_start_dt,
	CAST (LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info


--- insertion dans la table

/*
Cette requête :

nettoie les dates invalides,
corrige les montants de vente incohérents,
recalcule les prix manquants ou négatifs,
puis charge les données corrigées dans la table silver.crm_sales_details.

*/

INSERT INTO silver.crm_prd_info (
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)
SELECT
	prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-','_') AS cat_id, -- je retire 5 caractère en partant du 1er dans la chaine prd_key et je remplace - par _
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm,
	ISNULL(prd_cost,0) AS prd_cost,
	CASE UPPER(TRIM(prd_line))
		 WHEN 'M' THEN 'Mountain'
		 WHEN 'R' THEN 'Road'
		 WHEN 'S' THEN 'Other Sales'
		 WHEN 'T' THEN 'Touring'
		 ELSE 'n/a'
	END AS prd_line,
	CAST (prd_start_dt AS DATE) AS prd_start_dt,
	CAST (LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info

select * from silver.crm_prd_info


-- check des dates invalide

SELECT
NULLIF(sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0
OR LEN(sls_order_dt) != 8
OR sls_order_dt > 20500101
OR sls_order_dt < 19000101

-- Check Data consistency : Between sales, quantity, and Price
-- Sales = Quantity * Price
-- Values must not be NULL, zero, or negative

SELECT DISTINCT
sls_sales AS old_sls_sales,
sls_quantity,
CASE WHEN sls_sales  IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
	THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
END AS sls_sales,
CASE WHEN sls_price IS NULL OR sls_price <= 0
	THEN sls_sales / NULLIF(sls_quantity, 0) -- au cas où la quantité est null ou 0
	ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price


-- nettoyage de la table sales_detail
-- en transformant les colones sls_ship_dt et sls_order_dt NVARCHAR en DATE
SELECT 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt,
	CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_ship_dt,
	CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END AS sls_due_dt,
	CASE WHEN sls_sales  IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
	THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
	END AS sls_sales,
	sls_quantity,
	CASE WHEN sls_price IS NULL OR sls_price <= 0
		THEN sls_sales / NULLIF(sls_quantity, 0) -- au cas où la quantité est null ou 0
		ELSE sls_price
	END AS sls_price
FROM bronze.crm_sales_details

--- Insertion
INSERT INTO silver.crm_sales_details(
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
)
SELECT 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE 
		WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt,
	CASE 
		WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_ship_dt,
	CASE 
		WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END AS sls_due_dt,
	CASE WHEN sls_sales  IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales,
	sls_quantity,
	-- On utilise sls_price tel qu'il est, mais on s'assure que le prix n'est pas NULL
	CASE 
		WHEN sls_price IS NULL OR sls_price <= 0
		THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price
	END AS sls_price
FROM bronze.crm_sales_details

-- Check data bronze

SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price


SELECT * FROM silver.crm_sales_details

-- Clean & Load : erp_cust_az12

-- identify Out-of-range dates

SELECT DISTINCT
bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

-- data standardization & consistency

SELECT DISTINCT 
gen,
CASE 
	WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12

-- Insertion

INSERT INTO silver.erp_cust_az12(cid, bdate, gen)
SELECT
CASE 
	WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- retirer le préfix 'NAS' s'il existe
	ELSE cid
END AS cid,
CASE 
	WHEN bdate > GETDATE() THEN NULL	
	ELSE bdate
END AS bdate,
CASE 
	WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	ELSE 'n/a'
END AS gen -- normaliser le genre
FROM bronze.erp_cust_az12

-- Clean & Load : erp_loc_a101

SELECT cst_key  FROM silver.crm_cust_info

-- Data standardieation é consistency

SELECT DISTINCT cntry
FROM bronze.erp_loc_a101
ORDER BY cntry


SELECT
REPLACE(cid, '-','') cid, -- retirer les '-' de cid
CASE
	WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
	WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101

-- Data Standardization é consistency
SELECT DISTINCT
cntry AS old_cntry,
CASE
	WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
	WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101
ORDER BY cntry

-- insertion

INSERT INTO silver.erp_loc_a101
(cid, cntry)
SELECT
REPLACE(cid, '-','') cid, -- retirer les '-' de cid
CASE
	WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
	WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101


-- Clean & Load : erp_px_cat_g1v2

SELECT
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2

-- Check for unwanted spaces

SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR
subcat != TRIM(subcat) OR
maintenance != TRIM(maintenance)


-- Data Standardization é consistency

INSERT INTO silver.erp_px_cat_g1v2
(id, cat, subcat, maintenance)
SELECT
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2


SELECT * FROM silver.erp_px_cat_g1v2
