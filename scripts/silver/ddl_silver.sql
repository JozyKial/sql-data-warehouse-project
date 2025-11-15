/*
===============================================================================
Script DDL : Création des tables Silver
===============================================================================
Objectif du script :
    Ce script crée les tables dans le schéma 'bronze', en supprimant les tables 
    existantes si elles sont déjà présentes.
    Exécutez ce script pour redéfinir la structure DDL des tables du schéma 'bronze'.
===============================================================================
*/

USE dbDatawarehouse
GO


IF OBJECT_ID ('silver.crm_cust_info', 'U') IS NOT NULL --- U : user
	DROP TABLE silver.crm_cust_info;
GO

CREATE TABLE silver.crm_cust_info (
	cst_id				INT,
	cst_key				NVARCHAR(50),
	cst_firstname		NVARCHAR(50),
	cst_lastname		NVARCHAR(50),
	cst_material_status NVARCHAR(50),
	cst_gndr			NVARCHAR(50),
	cst_create_date		DATE,
	dwh_create_date		DATETIME2 DEFAULT GETDATE() -- ajout person, n'est pas la base souce. 
);
GO

IF OBJECT_ID ('silver.crm_prd_info', 'U') IS NOT NULL --- U : user
	DROP TABLE silver.crm_prd_info;
GO

CREATE TABLE silver.crm_prd_info (
	prd_id			INT,
	cat_id			NVARCHAR(50),
	prd_key			NVARCHAR(50),
	prd_nm			NVARCHAR(50),
	prd_cost		INT,
	prd_line		NVARCHAR(50),
	prd_start_dt	DATE,
	prd_end_dt		DATE,
	wh_create_date	DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID ('silver.crm_sales_details', 'U') IS NOT NULL --- U : user
	DROP TABLE silver.crm_sales_details;
GO

CREATE TABLE silver.crm_sales_details (
	sls_ord_num		NVARCHAR(50),
	sls_prd_key		NVARCHAR(50),
	sls_cust_id		INT,
	sls_order_dt	DATE,
	sls_ship_dt		DATE,
	sls_due_dt		DATE,
	sls_sales		INT,
	sls_quantity	INT,
	sls_price		INT,
	wh_create_date	DATETIME2 DEFAULT GETDATE()
);
GO

select * from silver.crm_sales_details

IF OBJECT_ID ('silver.erp_loc_a101', 'U') IS NOT NULL --- U : user
	DROP TABLE silver.erp_loc_a101;
GO

CREATE TABLE silver.erp_loc_a101 (
	cid					NVARCHAR(50),
	cntry				NVARCHAR(50),
	wh_create_date		DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID ('silver.erp_cust_az12', 'U') IS NOT NULL --- U : user
	DROP TABLE silver.erp_cust_az12;
GO

CREATE TABLE silver.erp_cust_az12 (
	cid					NVARCHAR(50),
	bdate				DATE,
	gen					NVARCHAR(50),
	wh_create_date		DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID ('silver.erp_px_cat_g1v2', 'U') IS NOT NULL --- U : user
	DROP TABLE silver.erp_px_cat_g1v2;
GO

CREATE TABLE silver.erp_px_cat_g1v2 (
	id					NVARCHAR(50),
	cat					NVARCHAR(50),
	subcat				NVARCHAR(50),
	maintenance			NVARCHAR(50),
	wh_create_date		DATETIME2 DEFAULT GETDATE()
);
GO


