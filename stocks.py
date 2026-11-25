# Databricks notebook source
file_path_stock  ="/Volumes/workspace/default/tp1_datalake/stocks.csv"
dfstocks= spark.read.format("csv").option("header" , "true").load(file_path_stock)
dfstocks.printSchema()


# COMMAND ----------

dfstocks.createOrReplaceTempView("stocks")

# COMMAND ----------

# doublons en stcoks
import pyspark.sql.functions as F
dfstocks.groupBy("date_stock", "id_magasin", "id_produit") \
    .count() \
    .filter(F.col("count") > 1) \
    .display()

# COMMAND ----------

# Vérifier si on a des champs vides

champs_vide  = dfstocks.filter(
    (F.col("date_stock").isNull()) |
    (F.col("id_magasin").isNull()) |
    (F.col("id_produit").isNull()) |
    (F.col("quantite_stock").isNull())
 
).display()


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from stocks

# COMMAND ----------

# MAGIC %sql
# MAGIC -- recherche de doublons (ok)
# MAGIC SELECT  date_stock,  id_magasin,  id_produit, COUNT(*) AS nombre_occurrences
# MAGIC FROM stocks
# MAGIC GROUP BY date_stock, id_magasin, id_produit
# MAGIC HAVING COUNT(*) > 1
# MAGIC ORDER BY nombre_occurrences DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- rupture de stock (277) rupture de stock (ko)
# MAGIC select id_produit , quantite_stock ,id_magasin  from stocks where quantite_stock = 0
# MAGIC group by id_produit , quantite_stock , id_magasin

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from stocks
# MAGIC where quantite_stock <0

# COMMAND ----------

# référentiels pour vérifier que chaque stock pointe vers un produit et un magasin existants
chemin = "/Volumes/workspace/default/tp1_datalake/"
spark.read.format("csv").option("header", "true").load(chemin + "produits.csv").createOrReplaceTempView("produits_ref")
spark.read.format("csv").option("header", "true").load(chemin + "magasins.csv").createOrReplaceTempView("magasins_ref")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- nombre de lignes : 84 000 (ok)
# MAGIC SELECT COUNT(*) AS nombre_lignes FROM stocks

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ruptures de stock, comptage corrigé : 277 lignes à 0 (271 couples produit/magasin, certains en rupture 2 mois)
# MAGIC -- ce n'est pas une erreur de données mais une réalité métier (ok : garder et signaler, utile pour les prévisions de Léa)
# MAGIC SELECT COUNT(*) AS lignes_en_rupture,
# MAGIC        COUNT(DISTINCT id_produit, id_magasin) AS couples_en_rupture
# MAGIC FROM stocks
# MAGIC WHERE CAST(quantite_stock AS INT) = 0

# COMMAND ----------

# MAGIC %sql
# MAGIC -- types : date_stock et quantite_stock sont lus en texte (ok : tout se convertit en date et en entier)
# MAGIC SELECT SUM(CASE WHEN try_cast(date_stock AS DATE) IS NULL THEN 1 ELSE 0 END)    AS dates_illisibles,
# MAGIC        SUM(CASE WHEN try_cast(quantite_stock AS INT) IS NULL THEN 1 ELSE 0 END) AS quantites_illisibles
# MAGIC FROM stocks

# COMMAND ----------

# MAGIC %sql
# MAGIC -- min / max (ok : stock de 0 à 299, du 31/01/2025 au 30/09/2026)
# MAGIC SELECT MIN(CAST(quantite_stock AS INT)) AS stock_min,
# MAGIC        MAX(CAST(quantite_stock AS INT)) AS stock_max,
# MAGIC        MIN(to_date(date_stock))         AS premiere_date,
# MAGIC        MAX(to_date(date_stock))         AS derniere_date
# MAGIC FROM stocks

# COMMAND ----------

# MAGIC %sql
# MAGIC -- valeurs distinctes et grille complète : 21 mois x 20 magasins x 200 produits = 84 000 lignes (ok : aucune combinaison manquante)
# MAGIC SELECT COUNT(DISTINCT date_stock) AS mois,
# MAGIC        COUNT(DISTINCT id_magasin) AS magasins,
# MAGIC        COUNT(DISTINCT id_produit) AS produits,
# MAGIC        COUNT(DISTINCT date_stock) * COUNT(DISTINCT id_magasin) * COUNT(DISTINCT id_produit) AS combinaisons_attendues,
# MAGIC        COUNT(*) AS lignes
# MAGIC FROM stocks

# COMMAND ----------

# MAGIC %sql
# MAGIC -- produits ou magasins inconnus (ok)
# MAGIC SELECT
# MAGIC   (SELECT COUNT(*) FROM stocks s LEFT ANTI JOIN produits_ref p ON s.id_produit = p.id_produit) AS produits_inconnus,
# MAGIC   (SELECT COUNT(*) FROM stocks s LEFT ANTI JOIN magasins_ref m ON s.id_magasin = m.id_magasin) AS magasins_inconnus

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stocks : interpréter, décider, justifier
# MAGIC
# MAGIC | Constat | Interprétation | Décision |
# MAGIC |---|---|---|
# MAGIC | 84 000 lignes, aucun doublon sur (date, magasin, produit), aucun vide | Une photo par fin de mois, grille complète (21 × 20 × 200) | ok |
# MAGIC | Tout est lu en texte | Erreur technique de lecture : le CSV n'a pas de types | **Corriger** : convertir date_stock en date et quantite_stock en entier |
# MAGIC | Aucun stock négatif (de 0 à 299) | Valeurs plausibles | ok. Un stock négatif est physiquement impossible : **quarantaine**, sans le remplacer par 0 |
# MAGIC | 277 lignes à 0 (271 couples produit/magasin) | **Réalité métier** : ce sont des ruptures, pas des erreurs | **Garder et signaler** : c'est exactement ce que Léa veut prévoir |
# MAGIC | Aucun produit ni magasin inconnu | Intégrité respectée | ok |
# MAGIC
# MAGIC **Pour Léa** : l'historique complet, sans valeur inventée, permet de prévoir les ruptures produit par produit.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Passage en silver : on applique les décisions
# MAGIC 1. **Corriger** : convertir date_stock en date et quantite_stock en entier.
# MAGIC 2. **Mettre en quarantaine** : date illisible, stock vide ou négatif, doublon sur (date, magasin, produit). Un stock négatif n'est **pas** remplacé par 0 : cela inventerait des ruptures.
# MAGIC 3. **Garder et signaler** : les ruptures (stock = 0) avec la colonne est_rupture.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

# 1. Convertir les types (le CSV est lu en texte)
stocks_types = dfstocks \
    .withColumn("date_stock", F.expr("try_cast(date_stock AS DATE)")) \
    .withColumn("quantite_stock", F.expr("try_cast(quantite_stock AS INT)"))

# 2. Règles de quarantaine
stocks_controle = stocks_types \
    .withColumn("nb_cle", F.count("*").over(Window.partitionBy("date_stock", "id_magasin", "id_produit"))) \
    .withColumn("motif_rejet",
        F.when(F.col("date_stock").isNull(), "date illisible")
         .when(F.col("quantite_stock").isNull(), "stock vide ou illisible")
         .when(F.col("quantite_stock") < 0, "stock negatif")
         .when(F.col("nb_cle") > 1, "doublon sur la cle")) \
    .drop("nb_cle")

# 3. Garder et signaler : les ruptures de stock
stocks_controle = stocks_controle.withColumn("est_rupture", F.col("quantite_stock") == 0)

stocks_quarantaine = stocks_controle.filter(F.col("motif_rejet").isNotNull())
stocks_silver = stocks_controle.filter(F.col("motif_rejet").isNull()).drop("motif_rejet")
print("Lignes en silver :", stocks_silver.count())                 # 84 000
print("Lignes en quarantaine :", stocks_quarantaine.count())       # 0
print("Ruptures signalées :", stocks_silver.filter("est_rupture").count())   # 277
display(stocks_silver.limit(10))

# COMMAND ----------

# Conserver la table stocks nettoyée dans la couche Silver
stocks_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/Volumes/workspace/default/tp1_datalake/silver/stocks")

# Les lignes rejetées sont conservées à part, avec leur motif (quarantaine)
stocks_quarantaine.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/Volumes/workspace/default/tp1_datalake/silver/quarantaine_stocks")

# COMMAND ----------

# Vérification de la table silver
verif = spark.read.format("delta").load("/Volumes/workspace/default/tp1_datalake/silver/stocks")
verif.printSchema()
print("Lignes en silver :", verif.count())   # 84 000