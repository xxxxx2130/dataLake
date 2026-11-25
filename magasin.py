# Databricks notebook source
file_path_magasin = "/Volumes/workspace/default/tp1_datalake/magasins.csv"
dfmagasin = spark.read.format("csv").option("header", "true").load(file_path_magasin)
dfmagasin.printSchema()

# COMMAND ----------

dfmagasin.createOrReplaceTempView("magasin")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM magasin
# MAGIC --20 lignes

# COMMAND ----------

# Vérifier si on a des champs vides
from pyspark.sql import functions as F

champs_vide  = dfmagasin.filter(
    F.col("id_magasin").isNull()
    | F.col("ville").isNull()
    | F.col("nom").isNull()
    | F.col("region").isNull()
    | F.col("surface_m2").isNull()
    | F.col("ville").isNull()
 
).display()


# COMMAND ----------

# MAGIC %sql
# MAGIC --verifier si les surfaces sont valides (ok)
# MAGIC
# MAGIC select  surface_m2 from  magasin where surface_m2 < 0 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- recherche de doublons sur id_magasin (ok)
# MAGIC SELECT id_magasin, COUNT(*) AS nombre_occurrences
# MAGIC FROM magasin
# MAGIC GROUP BY id_magasin
# MAGIC HAVING COUNT(*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- types : toutes les colonnes sont lues en texte (string), surface_m2 doit être un entier (ok : toutes les surfaces se convertissent)
# MAGIC SELECT COUNT(*) AS surfaces_non_numeriques
# MAGIC FROM magasin
# MAGIC WHERE try_cast(surface_m2 AS INT) IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC -- min / max des surfaces (ok : de 498 à 2 496 m²)
# MAGIC SELECT MIN(CAST(surface_m2 AS INT)) AS surface_min,
# MAGIC        MAX(CAST(surface_m2 AS INT)) AS surface_max,
# MAGIC        ROUND(AVG(CAST(surface_m2 AS INT))) AS surface_moyenne
# MAGIC FROM magasin

# COMMAND ----------

# MAGIC %sql
# MAGIC -- valeurs distinctes : 20 magasins, 10 villes, 9 régions (ok : 2 magasins par ville)
# MAGIC SELECT COUNT(DISTINCT id_magasin) AS magasins,
# MAGIC        COUNT(DISTINCT ville)      AS villes,
# MAGIC        COUNT(DISTINCT region)     AS regions,
# MAGIC        COUNT(DISTINCT nom)        AS noms
# MAGIC FROM magasin

# COMMAND ----------

# MAGIC %sql
# MAGIC -- villes par région : Marseille et Nice sont toutes les deux en PACA (ok, réalité géographique)
# MAGIC SELECT region, COUNT(*) AS nombre_magasins, collect_set(ville) AS villes
# MAGIC FROM magasin
# MAGIC GROUP BY region
# MAGIC ORDER BY nombre_magasins DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- le magasin WEB existe-t-il dans le référentiel ? non (ko : 20 % des ventes ont id_magasin = 'WEB')
# MAGIC SELECT COUNT(*) AS magasin_web
# MAGIC FROM magasin
# MAGIC WHERE id_magasin = 'WEB'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Magasins : interpréter, décider, justifier
# MAGIC
# MAGIC | Constat | Interprétation | Décision |
# MAGIC |---|---|---|
# MAGIC | 20 magasins, aucun doublon, aucun champ vide | Référentiel propre | ok |
# MAGIC | Tout est lu en texte (surface_m2 aussi) | Erreur technique de lecture : le CSV n'a pas de types | **Corriger** : convertir surface_m2 en entier (toutes les valeurs se convertissent) |
# MAGIC | Surfaces de 498 à 2 496 m², aucune négative | Valeurs plausibles | ok, on garde le contrôle (**quarantaine** si une surface devient négative ou illisible) |
# MAGIC | 10 villes mais 9 régions | Réalité métier : Marseille et Nice sont en PACA | ok |
# MAGIC | Le magasin WEB n'existe pas dans le référentiel | Les ventes en ligne ont id_magasin = 'WEB' (20 % des ventes) | **Garder et signaler** : ajouter un membre « WEB » dans la dimension magasin du TP 3, sinon 20 % du CA disparaît des jointures |
# MAGIC
# MAGIC **Pour Sophie** : sans le membre WEB, son CA par magasin oublierait toutes les ventes du site.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Passage en silver : on applique les décisions
# MAGIC 1. **Corriger** : convertir surface_m2 en entier.
# MAGIC 2. **Mettre en quarantaine** : identifiant en double, surface vide, illisible ou négative.
# MAGIC 3. Le membre **WEB** n'est pas ajouté ici : c'est une décision de modélisation, elle se fait au TP 3 (dimension magasin).

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

# 1. Convertir les types (le CSV est lu en texte)
magasins_types = dfmagasin.withColumn("surface_m2", F.expr("try_cast(surface_m2 AS INT)"))

# 2. Règles de quarantaine
magasins_controle = magasins_types \
    .withColumn("nb_id", F.count("*").over(Window.partitionBy("id_magasin"))) \
    .withColumn("motif_rejet",
        F.when(F.col("nb_id") > 1, "id_magasin en double")
         .when(F.col("surface_m2").isNull(), "surface vide ou illisible")
         .when(F.col("surface_m2") < 0, "surface negative")) \
    .drop("nb_id")

magasins_quarantaine = magasins_controle.filter(F.col("motif_rejet").isNotNull())
magasins_silver = magasins_controle.filter(F.col("motif_rejet").isNull()).drop("motif_rejet")
print("Magasins en silver :", magasins_silver.count())            # 20
print("Magasins en quarantaine :", magasins_quarantaine.count())  # 0
display(magasins_silver)

# COMMAND ----------

# Conserver la table magasins nettoyée dans la couche Silver
magasins_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/Volumes/workspace/default/tp1_datalake/silver/magasins")

# Les lignes rejetées sont conservées à part, avec leur motif (quarantaine)
magasins_quarantaine.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/Volumes/workspace/default/tp1_datalake/silver/quarantaine_magasins")

# COMMAND ----------

# Vérification de la table silver
verif = spark.read.format("delta").load("/Volumes/workspace/default/tp1_datalake/silver/magasins")
verif.printSchema()
print("Lignes en silver :", verif.count())   # 20