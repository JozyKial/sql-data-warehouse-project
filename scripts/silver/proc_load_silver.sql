/*
===============================================================================
Procédure stockée : Chargement du niveau Silver (Bronze -> Silver)
===============================================================================
Objectif du script :
    Cette procédure stockée exécute le processus ETL (Extraction, Transformation, Chargement)
    afin d’alimenter les tables du schéma 'silver' à partir du schéma 'bronze'.
    
    Actions effectuées :
        - Vide (TRUNCATE) les tables du schéma Silver.
        - Insère dans Silver les données transformées et nettoyées provenant de Bronze.

Paramètres :
    Aucun.
    Cette procédure stockée n’accepte aucun paramètre et ne renvoie aucune valeur.

Exemple d’utilisation :
    EXEC Silver.load_silver;
===============================================================================
*/


USE dbDatawarehouse ;

CREATE OR ALTER PROC silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '===================================';
		PRINT ' Loading Silver Layer ';
		PRINT '===================================';
		---------------------------------------------
			-- 1️ CRM_CUST_INFO
		---------------------------------------------
		SET @start_time = GETDATE();
		PRINT '>> Truncatin table : silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info; 
		-- Avant insertion : on vide complètement la table
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
			TRIM(cst_firstname) AS cst_firstname,             -- Nettoyage texte
			TRIM(cst_lastname) AS cst_lastname,               -- Nettoyage texte
			CASE	WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'   -- Normalisation statut marital
					WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
					ELSE 'n/a'
			END cst_material_status,
			CASE	WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'              -- Normalisation genre
					WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
					ELSE 'n/a'
			END cst_gndr,
			cst_create_date
		FROM 
		(
			-- Récupérer uniquement le dernier enregistrement par client
			SELECT
				*,
				ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		) t 
		WHERE flag_last = 1;
		SET @end_time = GETDATE();
		PRINT '>> Le chargement a duré : '+CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'secondes';
		PRINT '------------------';
		---------------------------------------------
		-- 2️ CRM_PRD_INFO
		---------------------------------------------
		SET @start_time = GETDATE();
		PRINT '>> Truncatin table : silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info; 
		-- Avant insertion : table vidée
		PRINT '>> Inserting data into : silver.crm_prd_info';
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
			REPLACE(SUBSTRING(prd_key, 1, 5), '-','_') AS cat_id,   -- Extraction catégorie + nettoyage '-'
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,         -- Extraction du prd_key réel
			prd_nm,
			ISNULL(prd_cost,0) AS prd_cost,                         -- Coût manquant = 0
			CASE UPPER(TRIM(prd_line))                              -- Normalisation ligne produit
				 WHEN 'M' THEN 'Mountain'
				 WHEN 'R' THEN 'Road'
				 WHEN 'S' THEN 'Other Sales'
				 WHEN 'T' THEN 'Touring'
				 ELSE 'n/a'
			END AS prd_line,
			CAST (prd_start_dt AS DATE) AS prd_start_dt,            -- Conversion date
			CAST (LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS DATE) 
				AS prd_end_dt                                       -- Prd_end_dt = veille du prochain start_dt
		FROM bronze.crm_prd_info;
		SET @end_time = GETDATE();
		PRINT '>> Le chargement a duré : '+CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'secondes';
		PRINT '------------------';


		---------------------------------------------
		-- 3️ CRM_SALES_DETAILS
		---------------------------------------------
		SET @start_time = GETDATE();
		PRINT '>> Truncatin table : silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		-- Avant insertion : table vidée
		PRINT '>> Inserting data into : silver.crm_sales_details';
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
			-- Conversion et nettoyage des dates invalides
			CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
			CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
			-- Recalcul des ventes si incohérentes ou nulles
			CASE WHEN sls_sales IS NULL OR sls_sales <= 0 
					  OR sls_sales != sls_quantity * ABS(sls_price)
				 THEN sls_quantity * ABS(sls_price)
				 ELSE sls_sales
			END AS sls_sales,
			sls_quantity,
			-- Correction prix si manquant ou ≤ 0
			CASE WHEN sls_price IS NULL OR sls_price <= 0
				 THEN sls_sales / NULLIF(sls_quantity, 0)
				 ELSE sls_price
			END AS sls_price
		FROM bronze.crm_sales_details;
		SET @end_time = GETDATE();
		PRINT '>> Le chargement a duré : '+CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'secondes';
		PRINT '------------------';
		---------------------------------------------
		-- 4️ ERP_CUST_AZ12
		---------------------------------------------
		SET @start_time = GETDATE();
		PRINT '>> Truncatin table : silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		-- Avant insertion : table vidée
		PRINT '>> Inserting data into : silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
		SELECT
			CASE WHEN cid LIKE 'NAS%' 
				 THEN SUBSTRING(cid, 4, LEN(cid))                 -- Suppression préfixe NAS
				 ELSE cid
			END AS cid,
			CASE WHEN bdate > GETDATE() THEN NULL                  -- Date future → NULL
				 ELSE bdate
			END AS bdate,
			CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
				 WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
				 ELSE 'n/a'
			END AS gen                                             -- Normalisation genre
		FROM bronze.erp_cust_az12;
		SET @end_time = GETDATE();
		PRINT '>> Le chargement a duré : '+CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'secondes';
		PRINT '------------------';


		---------------------------------------------
		-- 5️ ERP_LOC_A101
		---------------------------------------------
		SET @start_time = GETDATE();
		PRINT '>> Truncatin table : silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		-- Avant insertion : table vidée
		PRINT '>> Inserting data into : silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101 (cid, cntry)
		SELECT
			REPLACE(cid, '-','') AS cid,                          -- Nettoyage des tirets
			CASE 
				WHEN TRIM(cntry) = 'DE' THEN 'Germany'            -- Normalisation pays
				WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
				WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
				ELSE TRIM(cntry)
			END AS cntry
		FROM bronze.erp_loc_a101;
		SET @end_time = GETDATE();
		PRINT '>> Le chargement a duré : '+CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'secondes';
		PRINT '------------------';


		---------------------------------------------
		-- 6️ ERP_PX_CAT_G1V2
		---------------------------------------------
		SET @start_time = GETDATE();
		PRINT '>> Truncatin table : silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		-- Avant insertion : table vidée
		PRINT '>> Inserting data into : silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
		SELECT
			id,
			cat,
			subcat,
			maintenance
			-- Pas de transformation ici
		FROM bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT '>> Le chargement a duré : '+CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' secondes';
		PRINT '------------------';

		SET @batch_end_time = GETDATE();
		PRINT '==================================================';
		PRINT 'Chargement complet de la couche Bronze';
		PRINT '	- Temps total : '+CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' secondes';

	END TRY
	BEGIN CATCH
		PRINT '==================================================';
		PRINT 'ERREUR SURVENUE LORS DU CHARGEMENT DE LA COUCHE BRONZE';
		PRINT 'Message d''erreur : ' + ERROR_MESSAGE();
		PRINT 'Numéro d''erreur : ' + CAST(ERROR_NUMBER() AS NVARCHAR(10));
		PRINT 'Gravité : ' + CAST(ERROR_SEVERITY() AS NVARCHAR(10));
		PRINT 'État : ' + CAST(ERROR_STATE() AS NVARCHAR(10));
		PRINT 'Procédure : ' + ISNULL(ERROR_PROCEDURE(), 'N/A');
		PRINT 'Ligne : ' + CAST(ERROR_LINE() AS NVARCHAR(10));
		PRINT '==================================================';
	END CATCH
END

-- Exécution de la procédure
EXEC silver.load_silver;



