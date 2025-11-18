# Catalogue de Données pour la couche Gold

## Aperçu
La couche Gold représente la donnée au niveau métier, structurée pour supporter les cas d’usage analytiques et les besoins de reporting.
Elle est composée de **tables de dimensions** et de **tables de faits**, construites autour d’indicateurs métier spécifiques.

---

### 1. **gold.dim_customers**
- **Objectif:** Stocke les informations clients enrichies avec des données démographiques et géographiques.
- **Colonnes:**

| Column Name      | Data Type     | Description                                                                                   |
|------------------|---------------|-----------------------------------------------------------------------------------------------|
| customer_key     | INT           | Clé de substitution identifiant de manière unique chaque enregistrement client dans la dimension.|
| customer_id      | INT           | Identifiant numérique unique attribué à chaque client.                                        |
| customer_number  | NVARCHAR(50)  | Identifiant alphanumérique utilisé pour le suivi et la référence du client.         |
| first_name       | NVARCHAR(50)  | Prénom du client tel qu’enregistré dans le système.                                         |
| last_name        | NVARCHAR(50)  | Nom ou nom de famille du client.                                                    |
| country          | NVARCHAR(50)  | Pays de résidence du client (ex. : « Australia »).                               |
| marital_status   | NVARCHAR(50)  | Statut matrimonial du client (ex. : « Married », « Single »).                              |
| gender           | NVARCHAR(50)  | Genre du client (ex. : « Male », « Female », « n/a »).                                  |
| birthdate        | DATE          | Date de naissance du client au format AAAA-MM-JJ (ex. : 1971-10-06).               |
| create_date      | DATE          | Date et heure de création de l’enregistrement client dans le système.|

---

### 2. **gold.dim_products**
- **Objectif:** Fournit des informations sur les produits et leurs attributs.
- **Colonnes:**

| Column Name         | Data Type     | Description                                                                                   |
|---------------------|---------------|-----------------------------------------------------------------------------------------------|
| product_key         | INT           | Clé de substitution identifiant de manière unique chaque produit dans la table de dimension.         |
| product_id          | INT           | Identifiant unique attribué au produit pour le suivi interne.            |
| product_number      | NVARCHAR(50)  | Code alphanumérique structuré représentant le produit, souvent utilisé pour la catégorisation ou l’inventaire. |
| product_name        | NVARCHAR(50)  | Nom descriptif du produit, incluant des détails comme le type, la couleur et la taille.      |
| category_id         | NVARCHAR(50)  | Identifiant unique de la catégorie du produit, lié à sa classification générale.    |
| category            | NVARCHAR(50)  | Classification générale du produit (ex. : Bikes, Components) permettant de regrouper des articles similaires.  |
| subcategory         | NVARCHAR(50)  | Classification détaillée du produit au sein de la catégorie.     |
| maintenance_required| NVARCHAR(50)  | Indique si le produit nécessite une maintenance (ex. : « Yes », « No »).                       |
| cost                | INT           | Coût ou prix de base du produit, exprimé en unités monétaires.                           |
| product_line        | NVARCHAR(50)  | Ligne ou série de produits à laquelle le produit appartient (ex. : Road, Mountain).    |
| start_date          | DATE          | Date à laquelle le produit est devenu disponible à la vente ou à l’utilisation.|

---

### 3. **gold.fact_sales**
- **Objectif:** Stocke les données transactionnelles de ventes à des fins analytiques.
- **Colonnes:**

| Column Name     | Data Type     | Description                                                                                   |
|-----------------|---------------|-----------------------------------------------------------------------------------------------|
| order_number    | NVARCHAR(50)  | Identifiant alphanumérique unique pour chaque commande (ex. : « SO54496 »).                     |
| product_key     | INT           | Clé de substitution reliant la commande à la dimension produits.                               |
| customer_key    | INT           | Clé de substitution reliant la commande à la dimension clients.                             |
| order_date      | DATE          | Date à laquelle la commande a été passée.                                                         |
| shipping_date   | DATE          | Date à laquelle la commande a été expédiée au client.                                      |
| due_date        | DATE          | Date d’échéance du paiement de la commande.                                                     |
| sales_amount    | INT           | Valeur monétaire totale de la vente pour la ligne, en unités entières (ex. : 25).   |
| quantity        | INT           | Quantité de produits commandés pour la ligne (ex. : 1).                       |
| price           | INT           | Prix unitaire du produit pour la ligne, en unités monétaires (ex. : 25).      |
