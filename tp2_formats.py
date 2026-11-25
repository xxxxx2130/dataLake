# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # TP 2 
# MAGIC **Mission** : « La direction veut savoir quel format et quel partitionnement utiliser pour les ventes. On doit leurs fournire des mesures »
# MAGIC
# MAGIC On prend la table **silver/ventes** du TP 1et on fait 4 expériences : **formats**, **partitionnement**, **petits fichiers**, **Delta**.

# COMMAND ----------

# Lecture de la table silver ventes (TP 1)
file_path_silver_ventes = "/Volumes/workspace/default/tp1_datalake/silver/ventes"
dfventes = spark.read.format("delta").load(file_path_silver_ventes)
dfventes.printSchema()
dfventes.createOrReplaceTempView("ventes_silver")

# Dossier de travail du TP 2 (dans notre volume)
dossier_tp2 = "/Volumes/workspace/default/tp1_datalake/tp2"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- point de départ : 300 000 ventes, de quelle période ?
# MAGIC SELECT COUNT(*) AS nombre_lignes,
# MAGIC        MIN(date_vente) AS premiere_vente,
# MAGIC        MAX(date_vente) AS derniere_vente,
# MAGIC        COUNT(DISTINCT annee_mois) AS nombre_mois
# MAGIC FROM ventes_silver;
# MAGIC

# COMMAND ----------

# Petits outils de mesure
import os, shutil, time
from pyspark.sql import functions as F

def taille_mo(chemin):
    # taille totale d'un dossier, en Mo
    return sum(os.path.getsize(os.path.join(r, f)) for r, _, fs in os.walk(chemin) for f in fs) / 1048576

def nb_fichiers(chemin, extension):
    # nombre de fichiers de données (on ignore les fichiers techniques)
    return sum(1 for _, _, fs in os.walk(chemin) for f in fs if f.endswith(extension))

def chrono(fonction, essais=3):
    # on garde le meilleur de 3 essais, en millisecondes
    meilleur = float("inf")
    for _ in range(essais):
        debut = time.perf_counter()
        fonction()
        meilleur = min(meilleur, time.perf_counter() - debut)
    return round(meilleur * 1000)

# Les 2 requêtes types des métiers (TP 1)
def requete_complete(df):
    # CA par canal : lit toutes les lignes
    return lambda: df.groupBy("canal").agg(F.sum("montant")).collect()

def requete_mars(df):
    # CA de mars 2026 par magasin : ne devrait lire qu'un mois
    return lambda: df.filter("annee_mois = '2026-03'").groupBy("id_magasin").agg(F.sum("montant")).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC # Expérience 1 — Formats : mêmes données, mêmes requêtes
# MAGIC On écrit les 300 000 ventes dans 5 formats , puis on mesure la **taille** et le **temps** des 2 requêtes.

# COMMAND ----------

# Écriture des mêmes ventes dans 5 formats
dossier_formats = dossier_tp2 + "/formats"
shutil.rmtree(dossier_formats, ignore_errors=True)

une_partie = dfventes.coalesce(1)   # un seul fichier par format
une_partie.write.mode("overwrite").option("header", "true").csv(dossier_formats + "/csv")
une_partie.write.mode("overwrite").json(dossier_formats + "/json")
une_partie.write.mode("overwrite").option("compression", "snappy").parquet(dossier_formats + "/parquet_snappy")
une_partie.write.mode("overwrite").option("compression", "zstd").parquet(dossier_formats + "/parquet_zstd")
une_partie.write.format("delta").mode("overwrite").save(dossier_formats + "/delta")
print("5 formats écrits dans", dossier_formats)

# COMMAND ----------

# Mesure : taille et temps de lecture de chaque format
schema = dfventes.schema
lecteurs = {
    "CSV":            lambda: spark.read.schema(schema).option("header", "true").csv(dossier_formats + "/csv"),
    "JSON":           lambda: spark.read.schema(schema).json(dossier_formats + "/json"),
    "Parquet snappy": lambda: spark.read.parquet(dossier_formats + "/parquet_snappy"),
    "Parquet zstd":   lambda: spark.read.parquet(dossier_formats + "/parquet_zstd"),
    "Delta":          lambda: spark.read.format("delta").load(dossier_formats + "/delta"),
}
dossiers = {"CSV": "csv", "JSON": "json", "Parquet snappy": "parquet_snappy", "Parquet zstd": "parquet_zstd", "Delta": "delta"}

mesures = []
for nom, lecteur in lecteurs.items():
    df = lecteur()
    taille = taille_mo(dossier_formats + "/" + dossiers[nom])
    mesures.append((nom, round(taille, 1), chrono(requete_complete(df)), chrono(requete_mars(df))))
    print(nom, "mesuré")

resultats_formats = spark.createDataFrame(mesures, "format string, taille_Mo double, requete_complete_ms long, requete_mars_ms long")
resultats_formats.createOrReplaceTempView("resultats_formats")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- résultats des formats, comparés au CSV
# MAGIC SELECT format,
# MAGIC        taille_Mo,
# MAGIC        ROUND(taille_Mo / (SELECT taille_Mo FROM resultats_formats WHERE format = 'CSV') * 100) AS taille_en_pct_du_csv,
# MAGIC        requete_complete_ms,
# MAGIC        requete_mars_ms
# MAGIC FROM resultats_formats
# MAGIC ORDER BY taille_Mo

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expérience 2 — Partitionnement
# MAGIC On range les ventes en Parquet de 3 façons : **aucune** partition, **par mois** (annee_mois=2026-03/), **par jour**.
# MAGIC On compte les fichiers et on mesure la requête de mars.
# MAGIC

# COMMAND ----------

INCLURE_PIEGE = False

dossier_partitions = dossier_tp2 + "/partitions"
shutil.rmtree(dossier_partitions, ignore_errors=True)
base = dfventes.withColumn("jour", F.date_format("date_vente", "yyyy-MM-dd"))

strategies = {"aucune": [], "par_mois": ["annee_mois"], "par_jour": ["jour"]}
if INCLURE_PIEGE:
    strategies["par_magasin_et_jour"] = ["id_magasin", "jour"]

mesures = []
for nom, colonnes in strategies.items():
    chemin = dossier_partitions + "/" + nom
    debut = time.perf_counter()
    if colonnes:
        # un fichier par valeur de partition, comme un chargement quotidien
        base.repartition(*colonnes).write.mode("overwrite").partitionBy(*colonnes).parquet(chemin)
    else:
        base.coalesce(1).write.mode("overwrite").parquet(chemin)
    ecriture_s = round(time.perf_counter() - debut, 1)

    df = spark.read.parquet(chemin)
    if colonnes == ["jour"]:
        mars = lambda: df.filter("jour >= '2026-03-01' AND jour <= '2026-03-31'").groupBy("id_magasin").agg(F.sum("montant")).collect()
    else:
        mars = requete_mars(df)
    n = nb_fichiers(chemin, ".parquet")
    taille = taille_mo(chemin)
    mesures.append((nom, n, round(taille, 1), round(taille * 1024 / n, 1), ecriture_s, chrono(mars), chrono(requete_complete(df))))
    print(nom, ":", n, "fichiers")

resultats_partitions = spark.createDataFrame(mesures, "strategie string, nombre_fichiers long, taille_Mo double, "
                                             "taille_moyenne_fichier_Ko double, ecriture_s double, requete_mars_ms long, requete_complete_ms long")
resultats_partitions.createOrReplaceTempView("resultats_partitions")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- résultats du partitionnement : nombre de fichiers, taille moyenne d'un fichier, vitesse
# MAGIC SELECT * FROM resultats_partitions
# MAGIC ORDER BY nombre_fichiers

# COMMAND ----------

# partitionnement par mois : un sous-dossier par mois
display(dbutils.fs.ls(dossier_partitions + "/par_mois"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expérience 3 — Petits fichiers, puis compaction (OPTIMIZE)
# MAGIC On simule 600 petits chargements « au fil de l'eau » : 600 fichiers dans une table Delta.
# MAGIC L'optimisation automatique est **désactivée** sur cette table, sinon Databricks corrigerait le problème tout seul et on ne verrait rien.

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS workspace.default.tp2_petits_fichiers")
spark.sql("""CREATE TABLE workspace.default.tp2_petits_fichiers USING DELTA
             TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'false',
                            'delta.autoOptimize.autoCompact' = 'false')
             AS SELECT * FROM ventes_silver WHERE 1 = 0""")

# 600 petits fichiers
dfventes.repartition(600).write.format("delta").mode("append").saveAsTable("workspace.default.tp2_petits_fichiers")

def mesurer(etape):
    detail = spark.sql("DESCRIBE DETAIL workspace.default.tp2_petits_fichiers").first()
    ms = chrono(lambda: spark.table("workspace.default.tp2_petits_fichiers").groupBy("id_magasin").agg(F.sum("montant")).collect())
    return (etape, detail["numFiles"], round(detail["sizeInBytes"] / 1048576, 1), ms)

avant = mesurer("1. après 600 petits chargements")
display(spark.sql("OPTIMIZE workspace.default.tp2_petits_fichiers"))
apres = mesurer("2. après OPTIMIZE")

resultats_petits_fichiers = spark.createDataFrame([avant, apres], "etape string, fichiers_actifs long, taille_Mo double, requete_ms long")
resultats_petits_fichiers.createOrReplaceTempView("resultats_petits_fichiers")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- avant / après compaction
# MAGIC SELECT * FROM resultats_petits_fichiers ORDER BY etape

# COMMAND ----------

# MAGIC %sql
# MAGIC -- VACUUM : quels anciens fichiers seraient supprimés ? (DRY RUN = on regarde sans supprimer)
# MAGIC VACUUM workspace.default.tp2_petits_fichiers DRY RUN

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expérience 4 — Ce que Delta apporte : UPDATE, MERGE, historique, schéma protégé

# COMMAND ----------

# Version 0 : une copie Delta des ventes silver
dfventes.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.default.tp2_ventes_delta")
print("Version 0 : table créée")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. UPDATE : le prix de PRD0001 était faux, on le corrige de +10 %
# MAGIC UPDATE workspace.default.tp2_ventes_delta
# MAGIC SET prix_unitaire = ROUND(prix_unitaire * 1.10, 2),
# MAGIC     montant = ROUND(quantite * ROUND(prix_unitaire * 1.10, 2), 2)
# MAGIC WHERE id_produit = 'PRD0001'

# COMMAND ----------

# 2. MERGE : un fichier arrive avec 3 ventes nouvelles ET une correction (quantité 5 saisie au lieu de 4)
from datetime import datetime
table_delta = spark.table("workspace.default.tp2_ventes_delta")
id_corrige = table_delta.filter("quantite = 5").orderBy("id_vente").first()["id_vente"]

modeles = table_delta.orderBy("id_vente").limit(3).collect()
nouvelles = spark.createDataFrame(
    [dict(r.asDict(), id_vente="V900000" + str(i + 1), date_vente=datetime(2026, 10, 1, 10), annee_mois="2026-10") for i, r in enumerate(modeles)],
    schema=table_delta.schema)
correction = table_delta.filter(F.col("id_vente") == id_corrige) \
    .withColumn("quantite", F.lit(4)) \
    .withColumn("montant", F.round(F.lit(4) * F.col("prix_unitaire"), 2))
nouvelles.unionByName(correction).createOrReplaceTempView("arrivee")
print("Vente corrigée :", id_corrige)

display(spark.sql("""MERGE INTO workspace.default.tp2_ventes_delta t
                     USING arrivee s ON t.id_vente = s.id_vente
                     WHEN MATCHED THEN UPDATE SET *
                     WHEN NOT MATCHED THEN INSERT *"""))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3. historique : chaque modification crée une version
# MAGIC DESCRIBE HISTORY workspace.default.tp2_ventes_delta

# COMMAND ----------

# Voyage dans le temps : on relit la version 0
v0 = spark.sql("SELECT * FROM workspace.default.tp2_ventes_delta VERSION AS OF 0")
actuelle = spark.table("workspace.default.tp2_ventes_delta")
print("Lignes version 0 :", v0.count(), "| version actuelle :", actuelle.count())
print(id_corrige, "quantité en version 0 :", v0.filter(F.col("id_vente") == id_corrige).first()["quantite"],
      "| maintenant :", actuelle.filter(F.col("id_vente") == id_corrige).first()["quantite"])

# COMMAND ----------

# 4. Le schéma est protégé : une colonne inattendue est refusée
intrus = spark.table("workspace.default.tp2_ventes_delta").limit(1).withColumn("colonne_inattendue", F.lit("test"))
try:
    intrus.write.format("delta").mode("append").saveAsTable("workspace.default.tp2_ventes_delta")
    print("Écriture acceptée (anormal)")
except Exception as erreur:
    print("Écriture refusée, le schéma est protégé :", type(erreur).__name__)

# COMMAND ----------

# 5. Le dossier _delta_log : le journal des transactions (un fichier JSON par version)
display(dbutils.fs.ls(dossier_formats + "/delta/_delta_log"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Synthèse : interpréter → décider → justifier
# MAGIC Mesures du 25/11/2026 sur Databricks Free Edition (serverless), 300 000 ventes (janvier 2025 → septembre 2026, 21 mois).
# MAGIC
# MAGIC ### 1. Interpréter les mesures
# MAGIC | Expérience | Ce qu'on mesure | Lecture |
# MAGIC |---|---|---|
# MAGIC | **Formats** | CSV 25,8 Mo · JSON 67,2 Mo (260 % du CSV) · Parquet snappy 5,7 Mo (22 %) · Delta 5,7 Mo (22 %) · **Parquet zstd 4,1 Mo (16 %)** | (ko) JSON répète le nom de chaque colonne sur chaque ligne. (ok) Le stockage en colonnes + compression divise la taille par 4 à 6. Delta = Parquet + un journal : même taille. |
# MAGIC | | Temps : 300 à 440 ms pour tous, CSV le plus lent (424 / 436 ms) | Sur 6 Mo, le temps est surtout le coût fixe du serverless : l'écart de vitesse grandira avec le volume, l'écart de taille est déjà là (on paie le stockage et les octets lus). |
# MAGIC | **Partitionnement** | aucune : 1 fichier · **par mois : 21 fichiers de 305 Ko, écriture 2,7 s** · par jour : 638 fichiers de 17 Ko, 10,5 Mo (+84 %), écriture 30,6 s (×18), requête complète 480 ms (la plus lente) | (ko) Par jour = sur-partitionnement : des centaines de fichiers minuscules, plus lourds et plus lents. (ok) Par mois garde des fichiers de taille correcte et permet de ne lire qu'un dossier pour « mars ». |
# MAGIC | | Requête de mars : 316 à 375 ms quelle que soit la stratégie | À 6 Mo, rien à gagner encore : le partitionnement est un choix pour le volume futur (plusieurs années, plusieurs Go). |
# MAGIC | **Petits fichiers** | 600 fichiers, 7,3 Mo, 593 ms → après OPTIMIZE : **1 fichier, 3,6 Mo, 364 ms (−39 %)** | (ko) Les chargements « au fil de l'eau » fabriquent des petits fichiers. (ok) OPTIMIZE les regroupe ; VACUUM supprime les anciens après 7 jours de rétention. |
# MAGIC | **Delta** | UPDATE : 1 459 lignes de PRD0001 corrigées · MERGE : 1 mise à jour + 3 insertions · version 0 : 300 000 lignes, version actuelle : 300 003 · V0000007 : quantité 5 en version 0, 4 maintenant · colonne inattendue refusée (AnalysisException) | (ok) Corriger, fusionner un chargement, revenir en arrière, protéger le schéma : impossible avec de simples fichiers Parquet ou CSV. |
# MAGIC
# MAGIC ### 2. Décider : tableau format / compression / partitionnement par zone
# MAGIC | Zone | Format | Compression | Partitionnement |
# MAGIC |---|---|---|---|
# MAGIC | **Bronze** | Format d'origine (CSV, JSON), dans le volume | Aucune | Par date d'arrivée (ingestion_date=) |
# MAGIC | **Silver** | **Delta** | zstd (snappy acceptable) | Ventes et stocks : **annee_mois**. Référentiels (clients, produits, magasins) : aucun |
# MAGIC | **Gold** | **Delta** | zstd | Aucun tant qu'une table fait moins de 1 Go |
# MAGIC
# MAGIC ### 3. Justifier
# MAGIC - **Bronze en format d'origine** : c'est la preuve de ce qu'on a reçu. On peut tout rejouer si une règle du TP 1 change. Ces fichiers sont rarement relus, la taille compte peu.
# MAGIC - **Silver et gold en Delta** : 4 à 6 fois plus petit que le CSV (mesure 1). Delta apporte MERGE pour les chargements, DELETE pour le droit à l'effacement RGPD (TP 4), l'historique pour auditer et revenir en arrière, et le schéma protégé contre les fichiers mal formés (mesure 4).
# MAGIC - **zstd** : 28 % plus petit que snappy (4,1 contre 5,7 Mo) pour un temps de lecture équivalent.
# MAGIC - **Ventes par annee_mois** : les métiers filtrent surtout par période (CA du mois, comparaison d'une année sur l'autre). Par mois, on a 21 fichiers de taille correcte. Par jour, on en aurait 638 de 17 Ko, avec une écriture 18 fois plus lente (mesure 2).
# MAGIC - **À ne jamais faire** : partitionner par jour, ou par magasin et par jour (environ 13 000 fichiers, plus de 35 minutes d'écriture constatées). Chaque fichier est un appel réseau facturé.
# MAGIC - **Exploitation** : planifier OPTIMIZE (ou l'optimisation prédictive) sur les tables qui reçoivent des chargements fréquents, puis VACUUM (mesure 3).