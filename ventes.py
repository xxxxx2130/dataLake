# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
file_path_ventes = "/Volumes/workspace/default/tp1_datalake/ventes.csv"
dfventes = spark.read.format("csv").option("header", "true").load(file_path_ventes)
dfventes.printSchema()

# COMMAND ----------

dfventes.createOrReplaceTempView("ventes")

# les référentiels, pour vérifier que chaque vente pointe vers quelque chose qui existe
chemin = "/Volumes/workspace/default/tp1_datalake/"
spark.read.format("csv").option("header", "true").load(chemin + "produits.csv").createOrReplaceTempView("produits")
spark.read.format("csv").option("header", "true").load(chemin + "magasins.csv").createOrReplaceTempView("magasins")
spark.read.format("csv").option("header", "true").load(chemin + "clients.csv").createOrReplaceTempView("clients")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ventes
# MAGIC --301 500 lignes

# COMMAND ----------

# MAGIC %sql
# MAGIC -- lignes et identifiants : 301 500 lignes pour 300 000 id_vente (ko : 1 500 doublons)
# MAGIC SELECT COUNT(*) AS nombre_lignes,
# MAGIC        COUNT(DISTINCT id_vente) AS id_vente_distincts,
# MAGIC        COUNT(*) - COUNT(DISTINCT id_vente) AS doublons
# MAGIC FROM ventes

# COMMAND ----------

# MAGIC %sql
# MAGIC -- les doublons sont-ils des lignes 100 % identiques ? oui, 1 500 (ko : double envoi de la caisse -> corriger)
# MAGIC SELECT (SELECT COUNT(*) FROM ventes) - (SELECT COUNT(*) FROM (SELECT DISTINCT * FROM ventes)) AS doublons_exacts

# COMMAND ----------

# MAGIC %sql
# MAGIC -- même id_vente avec un contenu différent (ok : aucun conflit)
# MAGIC SELECT id_vente, COUNT(*) AS versions
# MAGIC FROM (SELECT DISTINCT * FROM ventes)
# MAGIC GROUP BY id_vente
# MAGIC HAVING COUNT(*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- poids des doublons sur le CA : 51 998 108,84 € avec, 51 747 384,62 € sans (ko : +250 724 €)
# MAGIC SELECT ROUND(SUM(CAST(montant AS DOUBLE)), 2) AS ca_avec_doublons,
# MAGIC        (SELECT ROUND(SUM(CAST(montant AS DOUBLE)), 2) FROM (SELECT DISTINCT * FROM ventes)) AS ca_sans_doublons
# MAGIC FROM ventes

# COMMAND ----------

# MAGIC %sql
# MAGIC -- champs vides par colonne : seul id_client a des vides (ventes anonymes)
# MAGIC SELECT
# MAGIC   SUM(CASE WHEN id_vente IS NULL THEN 1 ELSE 0 END)      AS id_vente_vides,
# MAGIC   SUM(CASE WHEN date_vente IS NULL THEN 1 ELSE 0 END)    AS date_vente_vides,
# MAGIC   SUM(CASE WHEN canal IS NULL THEN 1 ELSE 0 END)         AS canal_vides,
# MAGIC   SUM(CASE WHEN id_magasin IS NULL THEN 1 ELSE 0 END)    AS id_magasin_vides,
# MAGIC   SUM(CASE WHEN id_client IS NULL THEN 1 ELSE 0 END)     AS id_client_vides,
# MAGIC   SUM(CASE WHEN id_produit IS NULL THEN 1 ELSE 0 END)    AS id_produit_vides,
# MAGIC   SUM(CASE WHEN quantite IS NULL THEN 1 ELSE 0 END)      AS quantite_vides,
# MAGIC   SUM(CASE WHEN prix_unitaire IS NULL THEN 1 ELSE 0 END) AS prix_vides,
# MAGIC   SUM(CASE WHEN montant IS NULL THEN 1 ELSE 0 END)       AS montant_vides
# MAGIC FROM ventes

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ventes sans client par canal : 35 % en magasin, 12 sur le web (ko : réalité métier, client sans carte de fidélité)
# MAGIC -- les 12 ventes web anonymes sont celles du client CLI00042, effacé au TP 4 (droit à l'effacement)
# MAGIC SELECT canal, COUNT(*) AS nombre_ventes,
# MAGIC        SUM(CASE WHEN id_client IS NULL THEN 1 ELSE 0 END) AS sans_client,
# MAGIC        ROUND(100 * AVG(CASE WHEN id_client IS NULL THEN 1 ELSE 0 END), 1) AS pct_sans_client
# MAGIC FROM ventes
# MAGIC GROUP BY canal

# COMMAND ----------

# MAGIC %sql
# MAGIC -- quantités négatives (2 940, min -5) : erreur ou retour ? (ko : ce sont des retours, le montant est négatif aussi)
# MAGIC SELECT COUNT(*) AS nombre, MIN(CAST(quantite AS INT)) AS quantite_min,
# MAGIC        ROUND(SUM(CAST(montant AS DOUBLE)), 2) AS montant_total
# MAGIC FROM ventes
# MAGIC WHERE CAST(quantite AS INT) < 0

# COMMAND ----------

# MAGIC %sql
# MAGIC -- quantités nulles (ok)
# MAGIC SELECT * FROM ventes WHERE CAST(quantite AS INT) = 0

# COMMAND ----------

# MAGIC %sql
# MAGIC -- montant = quantite x prix_unitaire (ok)
# MAGIC SELECT id_vente, quantite, prix_unitaire, montant
# MAGIC FROM ventes
# MAGIC WHERE abs(CAST(montant AS DOUBLE) - round(CAST(quantite AS INT) * CAST(prix_unitaire AS DOUBLE), 2)) > 0.01

# COMMAND ----------

# MAGIC %sql
# MAGIC -- prix de vente = prix catalogue du produit (ok)
# MAGIC SELECT v.id_vente, v.id_produit, v.prix_unitaire, p.prix_unitaire AS prix_catalogue
# MAGIC FROM ventes v JOIN produits p ON v.id_produit = p.id_produit
# MAGIC WHERE CAST(v.prix_unitaire AS DOUBLE) <> CAST(p.prix_unitaire AS DOUBLE)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- produits inconnus (ok)
# MAGIC SELECT v.id_vente, v.id_produit
# MAGIC FROM ventes v LEFT ANTI JOIN produits p ON v.id_produit = p.id_produit

# COMMAND ----------

# MAGIC %sql
# MAGIC -- magasins inconnus, en dehors de WEB (ok)
# MAGIC SELECT v.id_vente, v.id_magasin
# MAGIC FROM ventes v LEFT ANTI JOIN magasins m ON v.id_magasin = m.id_magasin
# MAGIC WHERE v.id_magasin <> 'WEB'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- canal cohérent avec le magasin : WEB <=> id_magasin = 'WEB' (ok)
# MAGIC SELECT canal, id_magasin = 'WEB' AS magasin_web, COUNT(*) AS nombre
# MAGIC FROM ventes
# MAGIC GROUP BY canal, id_magasin = 'WEB'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- clients inconnus du CRM (ok)
# MAGIC SELECT v.id_vente, v.id_client
# MAGIC FROM ventes v LEFT ANTI JOIN clients c ON v.id_client = c.id_client
# MAGIC WHERE v.id_client IS NOT NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC -- achats avant la date d'inscription du client (106 296, environ la moitié) (ko : incohérence du CRM, à signaler)
# MAGIC SELECT COUNT(*) AS achats_avant_inscription
# MAGIC FROM ventes v JOIN clients c ON v.id_client = c.id_client
# MAGIC WHERE to_date(v.date_vente) < to_date(c.date_inscription)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- période couverte et dates futures (ok : du 01/01/2025 au 30/09/2026)
# MAGIC SELECT MIN(to_timestamp(date_vente)) AS premiere_vente,
# MAGIC        MAX(to_timestamp(date_vente)) AS derniere_vente,
# MAGIC        SUM(CASE WHEN to_timestamp(date_vente) > current_timestamp() THEN 1 ELSE 0 END) AS ventes_futures
# MAGIC FROM ventes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ventes : interpréter, décider, justifier
# MAGIC
# MAGIC | Constat | Erreur technique ou réalité métier ? | Décision |
# MAGIC |---|---|---|
# MAGIC | 1 500 lignes en double, identiques à 100 % | **Erreur technique** : la caisse a envoyé deux fois la même ligne | **Corriger** : on garde une seule copie |
# MAGIC | 2 940 quantités négatives (jusqu'à -5) | **Réalité métier** : ce sont des retours, le montant est négatif aussi | **Garder et signaler** (est_retour = true) : le retour diminue le CA, comme en compta |
# MAGIC | 28 % de ventes sans client, en magasin | **Réalité métier** : client sans carte de fidélité | **Garder** : client « ANONYME » dans le modèle du TP 3 |
# MAGIC | 106 296 achats avant l'inscription du client | Incohérence entre la caisse et le CRM | **Garder et signaler** au CRM : la vente a bien eu lieu |
# MAGIC | Montants, prix, produits, magasins, clients, canal, dates | Aucune anomalie aujourd'hui | ok. Si une ligne devient fausse un jour : **quarantaine** (on ne corrige pas une valeur sans savoir laquelle est juste) |
# MAGIC
# MAGIC **Pour Sophie** : après suppression des doublons, le CA passe de 51 998 108,84 € à **51 747 384,62 €**, et il inclut les retours en négatif : ce sont les mêmes chiffres que la compta.
# MAGIC **Pour Marc** : aucune vente ne pointe vers un client inconnu, donc un client peut être effacé sur demande sans rien oublier (c'est ce qu'on a fait au TP 4 avec CLI00042).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Passage en silver : on applique les décisions
# MAGIC 1. **Corriger** : supprimer les doublons exacts et convertir les types.
# MAGIC 2. **Mettre en quarantaine** les lignes invalides, avec leur motif.
# MAGIC 3. **Garder et signaler** : retours (est_retour), ventes anonymes.
# MAGIC 4. Enregistrer dans silver et vérifier le CA.

# COMMAND ----------

from pyspark.sql import functions as F

# 1. Supprimer les doublons exacts (erreur technique : double envoi de la caisse)
dfventes_sans_doublons = dfventes.dropDuplicates()
print("Lignes avant :", dfventes.count())                    # 301 500
print("Lignes après :", dfventes_sans_doublons.count())      # 300 000
print("Doublons supprimés :", dfventes.count() - dfventes_sans_doublons.count())   # 1 500

# COMMAND ----------

# 2. Convertir les types (le CSV est lu en texte) ; une valeur illisible devient vide au lieu de faire planter
dfventes_typees = dfventes_sans_doublons \
    .withColumn("date_vente", F.expr("try_cast(date_vente AS TIMESTAMP)")) \
    .withColumn("quantite", F.expr("try_cast(quantite AS INT)")) \
    .withColumn("prix_unitaire", F.expr("try_cast(prix_unitaire AS DOUBLE)")) \
    .withColumn("montant", F.expr("try_cast(montant AS DOUBLE)"))
dfventes_typees.printSchema()

# COMMAND ----------

# 3. Règles de quarantaine : une ligne fausse qu'on ne sait pas corriger est mise de côté, avec son motif
produits_ok = spark.table("produits").select("id_produit").withColumn("produit_connu", F.lit(True))
magasins_ok = spark.table("magasins").select("id_magasin").withColumn("magasin_connu", F.lit(True))
clients_ok = spark.table("clients").select("id_client").withColumn("client_connu", F.lit(True))

dfventes_controle = dfventes_typees \
    .join(produits_ok, "id_produit", "left") \
    .join(magasins_ok, "id_magasin", "left") \
    .join(clients_ok, "id_client", "left")

motif_rejet = (
    F.when(F.col("date_vente").isNull(), "date illisible")
     .when(F.col("quantite").isNull() | (F.col("quantite") == 0), "quantite nulle ou vide")
     .when(F.col("montant").isNull() | F.col("prix_unitaire").isNull(), "montant ou prix vide")
     .when(F.abs(F.col("montant") - F.round(F.col("quantite") * F.col("prix_unitaire"), 2)) > 0.01, "montant incoherent")
     .when(F.col("produit_connu").isNull(), "produit inconnu")
     .when(F.col("magasin_connu").isNull() & (F.col("id_magasin") != "WEB"), "magasin inconnu")
     .when((F.col("canal") == "WEB") != (F.col("id_magasin") == "WEB"), "canal incoherent")
     .when(F.col("id_client").isNotNull() & F.col("client_connu").isNull(), "client inconnu")
)
dfventes_controle = dfventes_controle.withColumn("motif_rejet", motif_rejet)

dfventes_quarantaine = dfventes_controle.filter(F.col("motif_rejet").isNotNull()) \
    .drop("produit_connu", "magasin_connu", "client_connu")
print("Lignes en quarantaine :", dfventes_quarantaine.count())   # 0 aujourd'hui, mais la règle est en place

# COMMAND ----------

# 4. Garder et signaler : les retours (quantité négative) restent dans les ventes, avec un indicateur
dfventes_silver = dfventes_controle.filter(F.col("motif_rejet").isNull()) \
    .withColumn("est_retour", F.col("quantite") < 0) \
    .withColumn("annee_mois", F.date_format("date_vente", "yyyy-MM")) \
    .select("id_vente", "date_vente", "canal", "id_magasin", "id_client", "id_produit",
            "quantite", "prix_unitaire", "montant", "est_retour", "annee_mois")
display(dfventes_silver.limit(10))

# COMMAND ----------

# Conserver la table ventes nettoyée dans la couche Silver
dfventes_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/Volumes/workspace/default/tp1_datalake/silver/ventes")

# Les lignes rejetées sont conservées à part, avec leur motif (quarantaine)
dfventes_quarantaine.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/Volumes/workspace/default/tp1_datalake/silver/quarantaine_ventes")

# COMMAND ----------

# Vérification de la table silver
ventes_silver = spark.read.format("delta").load("/Volumes/workspace/default/tp1_datalake/silver/ventes")
print("Lignes en silver :", ventes_silver.count())                                    # 300 000
print("Retours gardés :", ventes_silver.filter("est_retour").count())                 # 2 924 (2 940 avant suppression des doublons)
print("Ventes anonymes :", ventes_silver.filter("id_client IS NULL").count())
print("CA silver :", round(ventes_silver.agg(F.sum("montant")).first()[0], 2))       # 51 747 384,62 €

# COMMAND ----------

# MAGIC %md
# MAGIC ### Résultat du passage en silver
# MAGIC | Règle | Décision | Effet |
# MAGIC |---|---|---|
# MAGIC | Doublons exacts | **Corriger** | 1 500 lignes supprimées, 300 000 ventes restantes |
# MAGIC | Types (dates, entiers, décimaux) | **Corriger** | Conversion avec try_cast : une valeur illisible devient vide et part en quarantaine |
# MAGIC | Date illisible, quantité nulle, montant faux, produit / magasin / client inconnu, canal incohérent | **Quarantaine** | 0 ligne aujourd'hui, conservées dans silver/quarantaine_ventes avec leur motif |
# MAGIC | Quantité négative | **Garder et signaler** | est_retour = true : le retour diminue le CA, comme en compta |
# MAGIC | Vente sans client | **Garder** | Client « ANONYME » dans le modèle du TP 3 |
# MAGIC
# MAGIC **CA silver = 51 747 384,62 €** (contre 51 998 108,84 € avant suppression des doublons).