# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # TP 3 · Chaîne décisionnelle : modèle en étoile (gold) et data marts
# MAGIC **Mission** : « Sophie veut son tableau de bord du lundi matin. Concevez le modèle, construisez-le en gold, et livrez le tableau de bord. »
# MAGIC
# MAGIC **Grain de fait_ventes : une ligne = une vente** (un produit, une quantité, un canal, une date-heure), retours compris avec une quantité et un montant négatifs.
# MAGIC
# MAGIC ### Étape 1 — Concevoir avant de coder
# MAGIC | Question | Décision |
# MAGIC |---|---|
# MAGIC | Ventes du site web, sans magasin ? | Un membre **WEB** dans dim_magasin (canal = WEB) |
# MAGIC | Ventes sans client ? | Un membre **ANONYME** dans dim_client |
# MAGIC | Les retours sont-ils dans le CA ? | **Oui**, en négatif, comme en comptabilité |
# MAGIC | Le nom du client dans le modèle ? | **Non** (minimisation RGPD) : ville, tranche d'âge et consentement suffisent |

# COMMAND ----------

# Lecture des tables silver (TP 1)
silver = "/Volumes/workspace/default/tp1_datalake/silver/"
dfventes = spark.read.format("delta").load(silver + "ventes")
dfclients = spark.read.format("delta").load(silver + "clients")
dfproduits = spark.read.format("delta").load(silver + "produits")
dfmagasins = spark.read.format("delta").load(silver + "magasins")

dfventes.createOrReplaceTempView("ventes")
dfclients.createOrReplaceTempView("clients")
dfproduits.createOrReplaceTempView("produits")
dfmagasins.createOrReplaceTempView("magasins")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- pourquoi un membre WEB ? part du CA faite en ligne, sans magasin
# MAGIC SELECT canal,
# MAGIC        ROUND(SUM(montant), 2) AS ca,
# MAGIC        ROUND(SUM(montant) * 100 / (SELECT SUM(montant) FROM ventes), 1) AS part_ca_pct
# MAGIC FROM ventes
# MAGIC GROUP BY canal

# COMMAND ----------

# MAGIC %sql
# MAGIC -- pourquoi un membre ANONYME ? ventes sans client
# MAGIC SELECT COUNT(*) AS ventes,
# MAGIC        SUM(CASE WHEN id_client IS NULL THEN 1 ELSE 0 END) AS ventes_sans_client,
# MAGIC        ROUND(SUM(CASE WHEN id_client IS NULL THEN 1 ELSE 0 END) * 100 / COUNT(*), 1) AS part_pct
# MAGIC FROM ventes

# COMMAND ----------

# MAGIC %sql
# MAGIC -- les retours : combien, et combien ils retirent au CA
# MAGIC SELECT SUM(CASE WHEN est_retour THEN 1 ELSE 0 END) AS nombre_retours,
# MAGIC        ROUND(SUM(CASE WHEN est_retour THEN montant ELSE 0 END), 2) AS montant_retours
# MAGIC FROM ventes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Étape 2 — Construire les dimensions et la table de faits

# COMMAND ----------

from pyspark.sql import functions as F

# ---- dim_date : un jour par ligne, sans trou, de la première à la dernière vente
bornes = dfventes.agg(F.to_date(F.min("date_vente")).alias("debut"), F.to_date(F.max("date_vente")).alias("fin")).first()
jours = spark.sql("SELECT explode(sequence(DATE'" + str(bornes["debut"]) + "', DATE'" + str(bornes["fin"]) + "', INTERVAL 1 DAY)) AS date")
jour_iso = ((F.dayofweek("date") + 5) % 7) + 1          # lundi = 1 ... dimanche = 7
dim_date = jours.select(
    F.date_format("date", "yyyyMMdd").cast("int").alias("cle_date"),
    "date",
    F.year("date").alias("annee"),
    F.quarter("date").alias("trimestre"),
    F.month("date").alias("mois"),
    F.date_format("date", "yyyy-MM").alias("nom_mois"),
    F.weekofyear("date").alias("semaine_iso"),
    F.year(F.date_add("date", 4 - jour_iso)).alias("annee_iso"),   # l'année ISO est celle du jeudi de la semaine
    jour_iso.alias("jour_semaine"),
    (jour_iso >= 6).alias("est_weekend"))
print("dim_date :", dim_date.count(), "jours")

# COMMAND ----------

# ---- dim_magasin : les magasins + le membre WEB
membre_web = spark.createDataFrame(
    [("WEB", "UrbanMarket en ligne", "Internet", "Web", None, "WEB")],
    "id_magasin string, nom string, ville string, region string, surface_m2 int, canal string")
dim_magasin = dfmagasins.select("id_magasin", "nom", "ville", "region",
                                F.col("surface_m2").cast("int").alias("surface_m2"),
                                F.lit("MAGASIN").alias("canal")) \
    .unionByName(membre_web)
print("dim_magasin :", dim_magasin.count(), "lignes")   # 20 magasins + WEB

# ---- dim_produit
dim_produit = dfproduits.select("id_produit", "nom", "categorie",
                                F.col("prix_unitaire").cast("double").alias("prix_catalogue"))
print("dim_produit :", dim_produit.count(), "lignes")

# COMMAND ----------

# ---- dim_client : minimisée (ni nom, ni email, ni date de naissance) + le membre ANONYME
date_naissance = F.expr("try_cast(CAST(date_naissance AS STRING) AS DATE)")
age = F.floor(F.months_between(F.lit(bornes["fin"]), date_naissance) / 12)
tranche_age = F.when(age < 25, "18-24").when(age < 35, "25-34").when(age < 50, "35-49") \
               .when(age < 65, "50-64").otherwise("65+")
membre_anonyme = spark.createDataFrame(
    [("ANONYME", "Inconnue", "Inconnue", False)],
    "id_client string, ville string, tranche_age string, consentement_marketing boolean")
dim_client = dfclients.select(
        "id_client", "ville", tranche_age.alias("tranche_age"),
        F.expr("try_cast(CAST(consentement_marketing AS STRING) AS BOOLEAN)").alias("consentement_marketing")) \
    .unionByName(membre_anonyme)
print("dim_client :", dim_client.count(), "lignes")

# COMMAND ----------

# ---- fait_ventes : une ligne = une vente ; les clés pointent vers les dimensions
fait_ventes = dfventes.select(
    "id_vente",
    F.date_format("date_vente", "yyyyMMdd").cast("int").alias("cle_date"),
    "id_magasin",
    "id_produit",
    F.coalesce("id_client", F.lit("ANONYME")).alias("id_client"),   # vente sans client -> ANONYME
    "quantite",
    "montant",
    "est_retour")
print("fait_ventes :", fait_ventes.count(), "lignes")

# COMMAND ----------

# Vues temporaires pour les contrôles en SQL
dim_date.createOrReplaceTempView("dim_date")
dim_magasin.createOrReplaceTempView("dim_magasin")
dim_produit.createOrReplaceTempView("dim_produit")
dim_client.createOrReplaceTempView("dim_client")
fait_ventes.createOrReplaceTempView("fait_ventes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Étape 3 — Contrôles avant d'écrire en gold : aucune vente perdue, chaque clé trouve sa dimension

# COMMAND ----------

# MAGIC %sql
# MAGIC -- même nombre de ventes et même CA qu'en silver (ok si les écarts valent 0)
# MAGIC SELECT (SELECT COUNT(*) FROM ventes) - (SELECT COUNT(*) FROM fait_ventes) AS ecart_lignes,
# MAGIC        ROUND((SELECT SUM(montant) FROM ventes) - (SELECT SUM(montant) FROM fait_ventes), 2) AS ecart_ca

# COMMAND ----------

# MAGIC %sql
# MAGIC -- clés orphelines : une vente dont la clé n'existe pas dans la dimension (ok si tout vaut 0)
# MAGIC SELECT
# MAGIC   (SELECT COUNT(*) FROM fait_ventes f LEFT ANTI JOIN dim_date d    ON f.cle_date = d.cle_date)     AS sans_date,
# MAGIC   (SELECT COUNT(*) FROM fait_ventes f LEFT ANTI JOIN dim_magasin m ON f.id_magasin = m.id_magasin) AS sans_magasin,
# MAGIC   (SELECT COUNT(*) FROM fait_ventes f LEFT ANTI JOIN dim_produit p ON f.id_produit = p.id_produit) AS sans_produit,
# MAGIC   (SELECT COUNT(*) FROM fait_ventes f LEFT ANTI JOIN dim_client c  ON f.id_client = c.id_client)   AS sans_client

# COMMAND ----------

# MAGIC %sql
# MAGIC -- unicité des clés primaires (ok si aucune ligne)
# MAGIC SELECT 'dim_date' AS table_, cle_date AS cle, COUNT(*) AS n FROM dim_date GROUP BY cle_date HAVING COUNT(*) > 1
# MAGIC UNION ALL SELECT 'dim_magasin', id_magasin, COUNT(*) FROM dim_magasin GROUP BY id_magasin HAVING COUNT(*) > 1
# MAGIC UNION ALL SELECT 'dim_produit', id_produit, COUNT(*) FROM dim_produit GROUP BY id_produit HAVING COUNT(*) > 1
# MAGIC UNION ALL SELECT 'dim_client', id_client, COUNT(*) FROM dim_client GROUP BY id_client HAVING COUNT(*) > 1
# MAGIC UNION ALL SELECT 'fait_ventes', id_vente, COUNT(*) FROM fait_ventes GROUP BY id_vente HAVING COUNT(*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- RGPD : dim_client ne contient aucune donnée directement identifiante
# MAGIC DESCRIBE dim_client

# COMMAND ----------

# MAGIC %md
# MAGIC ## Étape 4 — Écrire le modèle dans la couche gold

# COMMAND ----------

# Conserver le modèle en étoile dans la couche Gold
gold = "/Volumes/workspace/default/tp1_datalake/gold/"
for nom, df in [("dim_date", dim_date), ("dim_magasin", dim_magasin), ("dim_produit", dim_produit),
                ("dim_client", dim_client), ("fait_ventes", fait_ventes)]:
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(gold + nom)
    print(nom, ":", spark.read.format("delta").load(gold + nom).count(), "lignes en gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Étape 5 — Data marts : les réponses toutes prêtes pour Sophie

# COMMAND ----------

# MAGIC %sql
# MAGIC -- mart 1 : CA mensuel par magasin, comparé au même mois de l'année précédente
# MAGIC CREATE OR REPLACE TEMP VIEW v_ca_mensuel_magasin AS
# MAGIC WITH ca AS (
# MAGIC   SELECT d.annee, d.mois, d.nom_mois, m.id_magasin, m.nom AS magasin, m.ville, m.region, m.canal,
# MAGIC          SUM(f.montant) AS ca, COUNT(*) AS nb_ventes
# MAGIC   FROM fait_ventes f
# MAGIC   JOIN dim_date d    ON d.cle_date = f.cle_date
# MAGIC   JOIN dim_magasin m ON m.id_magasin = f.id_magasin
# MAGIC   GROUP BY d.annee, d.mois, d.nom_mois, m.id_magasin, m.nom, m.ville, m.region, m.canal)
# MAGIC SELECT c.*, p.ca AS ca_annee_precedente,
# MAGIC        ROUND((c.ca - p.ca) / NULLIF(p.ca, 0) * 100, 1) AS evolution_pct
# MAGIC FROM ca c
# MAGIC LEFT JOIN ca p ON p.id_magasin = c.id_magasin AND p.mois = c.mois AND p.annee = c.annee - 1;
# MAGIC
# MAGIC SELECT * FROM v_ca_mensuel_magasin WHERE nom_mois = '2026-03' ORDER BY evolution_pct DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- mart 2 : top produits du mois (quantité nette = retours déduits)
# MAGIC CREATE OR REPLACE TEMP VIEW v_top_produits_mois AS
# MAGIC SELECT d.nom_mois, p.id_produit, p.nom AS produit, p.categorie,
# MAGIC        SUM(f.quantite) AS quantite_nette,
# MAGIC        ROUND(SUM(f.montant), 2) AS ca,
# MAGIC        RANK() OVER (PARTITION BY d.nom_mois ORDER BY SUM(f.montant) DESC) AS rang_ca
# MAGIC FROM fait_ventes f
# MAGIC JOIN dim_date d    ON d.cle_date = f.cle_date
# MAGIC JOIN dim_produit p ON p.id_produit = f.id_produit
# MAGIC GROUP BY d.nom_mois, p.id_produit, p.nom, p.categorie;
# MAGIC
# MAGIC SELECT * FROM v_top_produits_mois WHERE nom_mois = '2026-03' AND rang_ca <= 10 ORDER BY rang_ca

# COMMAND ----------

# MAGIC %sql
# MAGIC -- mart 3 : part du web et des magasins dans le CA, par mois
# MAGIC CREATE OR REPLACE TEMP VIEW v_parts_canal AS
# MAGIC SELECT d.nom_mois, m.canal,
# MAGIC        ROUND(SUM(f.montant), 2) AS ca,
# MAGIC        ROUND(SUM(f.montant) * 100 / SUM(SUM(f.montant)) OVER (PARTITION BY d.nom_mois), 1) AS part_pct
# MAGIC FROM fait_ventes f
# MAGIC JOIN dim_date d    ON d.cle_date = f.cle_date
# MAGIC JOIN dim_magasin m ON m.id_magasin = f.id_magasin
# MAGIC GROUP BY d.nom_mois, m.canal;
# MAGIC
# MAGIC SELECT * FROM v_parts_canal ORDER BY nom_mois, canal

# COMMAND ----------

# MAGIC %md
# MAGIC ## Étape 6 — Contrôle attendu par le cours
# MAGIC Mars 2026 : le web pèse **environ 20 %** du CA, Nice progresse d'**environ 14 %** sur un an.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- part du web en mars 2026 (attendu : environ 20 %)
# MAGIC SELECT canal, ca, part_pct FROM v_parts_canal WHERE nom_mois = '2026-03'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- évolution de Nice sur un an en mars 2026, magasin par magasin (attendu : environ +14 % pour Nice 1)
# MAGIC -- Nice a 2 magasins : Nice 1 progresse, Nice 2 recule ; la ville entière recule
# MAGIC SELECT magasin, id_magasin,
# MAGIC        ROUND(ca, 2) AS ca_mars_2026,
# MAGIC        ROUND(ca_annee_precedente, 2) AS ca_mars_2025,
# MAGIC        evolution_pct
# MAGIC FROM v_ca_mensuel_magasin
# MAGIC WHERE nom_mois = '2026-03' AND ville = 'Nice'
# MAGIC UNION ALL
# MAGIC SELECT 'Nice (ville entière)', NULL, ROUND(SUM(ca), 2), ROUND(SUM(ca_annee_precedente), 2),
# MAGIC        ROUND((SUM(ca) - SUM(ca_annee_precedente)) * 100 / SUM(ca_annee_precedente), 1)
# MAGIC FROM v_ca_mensuel_magasin
# MAGIC WHERE nom_mois = '2026-03' AND ville = 'Nice'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Synthèse : interpréter → décider → justifier
# MAGIC
# MAGIC ### 1. Interpréter (mesures du 25/11/2026)
# MAGIC | Contrôle | Résultat | Lecture |
# MAGIC |---|---|---|
# MAGIC | Part du web dans le CA | 20 % (10,3 M€ sur 51,7 M€) | (ko sans membre WEB) 1 euro sur 5 disparaîtrait des jointures avec dim_magasin |
# MAGIC | Ventes sans client | 84 273 ventes, soit 28,1 % | (ko sans membre ANONYME) plus d'une vente sur 4 serait perdue |
# MAGIC | Retours | 2 924 retours, −500 875,77 € | (ok) gardés en négatif : le CA est net, comme en comptabilité |
# MAGIC | Lignes et CA silver → gold | écart 0 ligne, 0 € | (ok) aucune vente perdue |
# MAGIC | Clés orphelines | 0 sur les 4 dimensions | (ok) chaque vente trouve sa date, son magasin, son produit et son client |
# MAGIC | Unicité des clés primaires | aucun doublon | (ok) |
# MAGIC | Tables gold | dim_date 638 jours · dim_magasin 21 (20 + WEB) · dim_produit 200 · dim_client 5 000 (4 999 + ANONYME) · fait_ventes 300 000 | (ok) conforme à l'attendu du cours |
# MAGIC | Contrôle du cours : web en mars 2026 | 20,4 % | (ok) attendu environ 20 % |
# MAGIC | Contrôle du cours : Nice sur un an | **Nice 1 : +14,3 %** · Nice 2 : −27,0 % · ville entière : −7,6 % | (ok) les +14 % du cours concernent le magasin Nice 1 ; la ville recule à cause de Nice 2 |
# MAGIC
# MAGIC ### 2. Décider
# MAGIC - Un modèle en étoile : **une table de faits** (fait_ventes) et **4 dimensions** (date, magasin, produit, client).
# MAGIC - **Grain** : une ligne = une vente, retours compris en négatif.
# MAGIC - Deux membres techniques : **WEB** dans dim_magasin et **ANONYME** dans dim_client.
# MAGIC - dim_client **minimisée** : ville, tranche d'âge, consentement ; ni nom, ni email, ni date de naissance.
# MAGIC - Le gold est écrit en **Delta**, sans partitionnement (tables petites, décision du TP 2), dans /Volumes/workspace/default/tp1_datalake/gold/.
# MAGIC - Trois data marts pour Sophie : CA mensuel par magasin avec l'évolution sur un an, top produits du mois, part web / magasins.
# MAGIC
# MAGIC ### 3. Justifier
# MAGIC - Le grain le plus fin (la vente) permet de répondre à toutes les questions (jour, semaine, mois, magasin, produit, canal) sans refaire le modèle.
# MAGIC - Les membres WEB et ANONYME évitent de perdre 20 % du CA et 28 % des ventes dans les jointures : un tableau de bord faux est pire que pas de tableau de bord.
# MAGIC - Garder les retours en négatif donne le CA net réel, et permet quand même de les isoler (est_retour).
# MAGIC - La minimisation RGPD réduit le risque : le tableau de bord n'a pas besoin de savoir **qui** achète, seulement **quel type** de client.
# MAGIC - Les contrôles (écart 0, 0 orphelin, clés uniques) sont faits **avant** d'écrire en gold : on ne publie pas un modèle incohérent.
# MAGIC - Le résultat de Nice montre l'intérêt du grain magasin : au niveau de la ville, on ne verrait pas qu'un magasin progresse et que l'autre s'effondre.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Étape 7 — Publier le gold dans le catalogue pour le tableau de bord
# MAGIC Un tableau de bord Databricks interroge des **tables du catalogue** (via le SQL warehouse), pas des dossiers.
# MAGIC On publie donc les 5 tables gold dans le schéma **workspace.gold**. La source reste le dossier gold du volume.

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.gold")
# les anciennes clés étrangères empêcheraient de remplacer les dimensions
for dim in ["dim_date", "dim_magasin", "dim_produit", "dim_client"]:
    try:
        spark.sql("ALTER TABLE workspace.gold.fait_ventes DROP CONSTRAINT IF EXISTS fk_" + dim)
    except Exception:
        pass
for nom in ["dim_date", "dim_magasin", "dim_produit", "dim_client", "fait_ventes"]:
    df = spark.read.format("delta").load("/Volumes/workspace/default/tp1_datalake/gold/" + nom)
    try:
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.gold." + nom)
    except Exception:
        spark.sql("DROP TABLE IF EXISTS workspace.gold." + nom)
        df.write.format("delta").saveAsTable("workspace.gold." + nom)
    print("workspace.gold." + nom, ":", spark.table("workspace.gold." + nom).count(), "lignes")