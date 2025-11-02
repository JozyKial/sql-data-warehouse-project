/*
=============================================================
Création de la base de données et des schémas
=============================================================
Objectif du script :
    Ce script crée une nouvelle base de données nommée 'dbDataWarehouse' après avoir vérifié si elle existe déjà. 
    Si la base de données existe, elle est supprimée puis recréée. Le script configure ensuite trois schémas 
    dans la base de données : 'bronze', 'silver' et 'gold'.

⚠️ AVERTISSEMENT :
    L’exécution de ce script supprimera entièrement la base de données 'dbDataWarehouse' si elle existe déjà. 
    Toutes les données qu’elle contient seront définitivement perdues. 
    Veillez à effectuer une sauvegarde avant d’exécuter ce script.
*/



--- Création de la base de donnée 'dbDatawarehouse'

USE master;
GO

--- je virifie l'existance de la base de données

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'dbDatawarehouse')
BEGIN
	ALTER DATABASE dbDatawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE dbDatawarehouse;
END;
GO

--- je crée la base
CREATE DATABASE dbDatawarehouse;
GO

USE dbDatawarehouse;
GO

--- Création de schémas (bronze, silver & gold) comme dans notre architecture

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO


