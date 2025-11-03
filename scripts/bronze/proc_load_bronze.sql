/*
===============================================================================
Procédure stockée : Chargement de la couche Bronze (Source -> Bronze)
===============================================================================
Objectif du script :
    Cette procédure stockée charge les données dans le schéma 'bronze' à partir de fichiers CSV externes. 
    Elle effectue les actions suivantes :
    - Tronque (vide) les tables du schéma bronze avant le chargement des données.
    - Utilise la commande `BULK INSERT` pour charger les données des fichiers CSV dans les tables du schéma bronze.

Paramètres :
    Aucun. 
    Cette procédure stockée n’accepte aucun paramètre et ne renvoie aucune valeur.

Exemple d’utilisation :
    EXEC bronze.load_bronze;
===============================================================================
*/

--- Nous allons inserer les données au format CSV

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	-- on veut connaitre le temps d'exécution de notre requête
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;

	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '===============================================';
		PRINT 'Loading Bronze Layer';
		PRINT '===============================================';

		PRINT '-----------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-----------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncatin Table : bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

	
		PRINT '>> Inserting Data into : bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'E:\Formations\sql sources\dwh_project\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2, -- on récupère à partir de la 2e ligne, laissant l'entête
			FIELDTERMINATOR = ',', -- le délimiteur utilisé par le fichier csv
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Temps de chargement : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' secondes'; -- Je CAST (convertit le nombre en NVARCHAR)
		PRINT '>> ----------';

		SET @start_time = GETDATE();
		PRINT '>> Truncatin Table : bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting Data into : bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'E:\Formations\sql sources\dwh_project\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Temps de chargement : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' secondes'; -- Je CAST (convertit le nombre en NVARCHAR)
		PRINT '>> ----------';


		SET @start_time = GETDATE();
		PRINT '>> Truncatin Table : bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting Data into : bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'E:\Formations\sql sources\dwh_project\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Temps de chargement : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' secondes'; -- Je CAST (convertit le nombre en NVARCHAR)
		PRINT '>> ----------';

		PRINT '-----------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-----------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncatin Table : bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Inserting Data into : bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'E:\Formations\sql sources\dwh_project\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Temps de chargement : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' secondes'; -- Je CAST (convertit le nombre en NVARCHAR)
		PRINT '>> ----------';

		SET @start_time = GETDATE();
		PRINT '>> Truncatin Table : bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Inserting Data into : bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'E:\Formations\sql sources\dwh_project\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Temps de chargement : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' secondes'; -- Je CAST (convertit le nombre en NVARCHAR)
		PRINT '>> ----------';

		SET @start_time = GETDATE();
		PRINT '>> Truncatin Table : bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Inserting Data into : bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'E:\Formations\sql sources\dwh_project\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Temps de chargement : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' secondes'; -- Je CAST (convertit le nombre en NVARCHAR)
		PRINT '>> ----------';

		SET @batch_end_time = GETDATE();
		PRINT '==============================================';
		PRINT 'Le chargement de la couche Bronze est terminé.';
		PRINT ' - Temps total de chargement : ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' secondes';
		PRINT '==============================================';
	END TRY
	BEGIN CATCH
	PRINT '==================================================';
	PRINT 'ERREUR SURVENUE LORS DU CHARGEMENT DE LA COUCHE BRONZE';
	PRINT 'Error Message' + ERROR_MESSAGE();
	PRINT 'Error Message' + CAST (ERROR_MESSAGE()	AS NVARCHAR);
	PRINT 'Error Message' + CAST (ERROR_STATE()		AS NVARCHAR);
	PRINT '==================================================';
	END CATCH
END


-- SELECT COUNT(*) AS "nombre d'enregistrements" FROM bronze.crm_cust_info ;

EXEC bronze.load_bronze


