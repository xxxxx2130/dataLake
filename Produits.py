# Databricks notebook source
# Chemin du dossier contenant les fichiers
chemin_source = "/Volumes/workspace/default/tp1_datalake/"

# Lecture du fichier produits.csv
produits = (
    spark.read
    .option("header", True)
    .option("sep", ",")
    .csv(chemin_source + "produits.csv")
)

# Affichage des dix premières lignes
display(produits.limit(10))

# COMMAND ----------

from pyspark.sql import functions as F

# Compter toutes les lignes du fichier
nombre_lignes = produits.count()
print("Nombre de lignes :", nombre_lignes)

# Analyser chaque colonne
resultats = []

for nom, type_colonne in produits.dtypes:
    colonne = produits[nom]

    statistiques = produits.agg(
        F.count(F.when(colonne.isNull(), 1)).alias("nulls"),
        F.count(
            F.when(F.trim(colonne.cast("string")) == "", 1)
        ).alias("vides"),
        F.count_distinct(colonne).alias("distinctes")
    ).first()

    pourcentage_nulls = (
        round(statistiques["nulls"] * 100 / nombre_lignes, 2)
        if nombre_lignes > 0 else None
    )

    resultats.append((
        nom,
        type_colonne,
        statistiques["nulls"],
        statistiques["vides"],
        pourcentage_nulls,
        statistiques["distinctes"]
    ))

# Construire et afficher le tableau de profilage
profil = spark.createDataFrame(
    resultats,
    schema="""
        colonne STRING,
        type_lu STRING,
        nombre_nulls LONG,
        nombre_vides LONG,
        pourcentage_nulls DOUBLE,
        valeurs_distinctes LONG
    """
)

display(profil)

# COMMAND ----------

from pyspark.sql import functions as F

# Conserver le prix original et ajouter sa version numérique
produits_controle = produits.withColumn(
    "prix_numerique",
    F.expr("try_cast(prix_unitaire as DECIMAL(18, 2))")
)

# Calculer les indicateurs sur les prix
display(
    produits_controle.agg(
        F.min("prix_numerique").alias("prix_minimum"),
        F.max("prix_numerique").alias("prix_maximum"),
        F.count(
            F.when(F.col("prix_numerique").isNull(), 1)
        ).alias("prix_non_convertibles"),
        F.count(
            F.when(F.col("prix_numerique") < 0, 1)
        ).alias("prix_negatifs"),
        F.count(
            F.when(F.col("prix_numerique") == 0, 1)
        ).alias("prix_egaux_zero")
    )
)

# COMMAND ----------

chemin_source = "/Volumes/workspace/default/tp1_datalake/"

produits = (
    spark.read
    .option("header", True)
    .option("sep", ",")
    .csv(chemin_source + "produits.csv")
)

display(produits.limit(10))

# COMMAND ----------

from pyspark.sql import functions as F

# Conserver le prix original et ajouter sa version numérique
produits_controle = produits.withColumn(
    "prix_numerique",
    F.expr("try_cast(prix_unitaire as DECIMAL(18, 2))")
)

# Calculer les indicateurs sur les prix
display(
    produits_controle.agg(
        F.min("prix_numerique").alias("prix_minimum"),
        F.max("prix_numerique").alias("prix_maximum"),
        F.count(
            F.when(F.col("prix_numerique").isNull(), 1)
        ).alias("prix_non_convertibles"),
        F.count(
            F.when(F.col("prix_numerique") < 0, 1)
        ).alias("prix_negatifs"),
        F.count(
            F.when(F.col("prix_numerique") == 0, 1)
        ).alias("prix_egaux_zero")
    )
)

# COMMAND ----------

display(
    produits.groupBy("categorie")
    .count()
    .orderBy("categorie")
)

# COMMAND ----------

# Repérer les noms présents plusieurs fois
noms_repetes = (
    produits.groupBy("nom")
    .count()
    .filter(F.col("count") > 1)
)

# Afficher les produits qui portent ces noms
display(
    produits.join(
        noms_repetes.select("nom"),
        on="nom",
        how="inner"
    ).orderBy("nom", "id_produit")
)

# COMMAND ----------

from pyspark.sql import functions as F

# Lire le fichier des ventes
ventes = (
    spark.read
    .option("header", True)
    .option("sep", ",")
    .csv("/Volumes/workspace/default/tp1_datalake/ventes.csv")
)

# Connaître la taille du tableau
print("Nombre de lignes :", ventes.count())
print("Nombre de colonnes :", len(ventes.columns))

# Afficher les colonnes et un aperçu des données
ventes.printSchema()
display(ventes.limit(10))

# COMMAND ----------

from pyspark.sql import functions as F

nombre_lignes_ventes = ventes.count()
resultats_ventes = []

for nom, type_colonne in ventes.dtypes:
    colonne = ventes[nom]

    stats = ventes.agg(
        F.count(
            F.when(colonne.isNull(), 1)
        ).alias("nulls"),
        F.count(
            F.when(F.trim(colonne) == "", 1)
        ).alias("vides"),
        F.count_distinct(colonne).alias("distinctes")
    ).first()

    pourcentage = (
        round(stats["nulls"] * 100 / nombre_lignes_ventes, 2)
        if nombre_lignes_ventes > 0 else None
    )

    resultats_ventes.append((
        nom,
        type_colonne,
        stats["nulls"],
        stats["vides"],
        pourcentage,
        stats["distinctes"]
    ))

profil_ventes = spark.createDataFrame(
    resultats_ventes,
    schema="""
        colonne STRING,
        type_lu STRING,
        nombre_nulls LONG,
        nombre_vides LONG,
        pourcentage_nulls DOUBLE,
        valeurs_distinctes LONG
    """
)

display(profil_ventes)

# COMMAND ----------

ventes_id_repetes = (
    ventes.groupBy("id_vente")
    .count()
    .filter(F.col("count") > 1)
)

display(ventes_id_repetes)

# COMMAND ----------

print(
    "Copies supplémentaires de lignes entièrement identiques :",
    ventes.count() - ventes.dropDuplicates().count()
)

# COMMAND ----------

ventes_id_repetes = (
    ventes.groupBy("id_vente")
    .count()
    .filter(F.col("count") > 1)
)

display(
    ventes.join(
        ventes_id_repetes.select("id_vente"),
        on="id_vente",
        how="inner"
    )
    .orderBy("id_vente", "id_produit")
    .limit(20)
)

# COMMAND ----------

ventes_sans_doublons = ventes.dropDuplicates()

print("Avant :", ventes.count())
print("Après :", ventes_sans_doublons.count())

# COMMAND ----------

display(
    ventes_sans_doublons.groupBy("canal")
    .agg(
        F.count("*").alias("nombre_ventes"),
        F.count(
            F.when(F.col("id_client").isNull(), 1)
        ).alias("clients_manquants")
    )
    .withColumn(
        "pourcentage_clients_manquants",
        F.round(
            100 * F.col("clients_manquants")
            / F.col("nombre_ventes"),
            2
        )
    )
)

# COMMAND ----------

# Ajouter des colonnes numériques sans remplacer les originales
ventes_controle = (
    ventes_sans_doublons
    .withColumn(
        "quantite_num",
        F.expr("try_cast(quantite as DECIMAL(18, 4))")
    )
    .withColumn(
        "prix_num",
        F.expr("try_cast(prix_unitaire as DECIMAL(18, 2))")
    )
    .withColumn(
        "montant_num",
        F.expr("try_cast(montant as DECIMAL(18, 2))")
    )
)

# Afficher les indicateurs pour chaque colonne numérique
for colonne in ["quantite_num", "prix_num", "montant_num"]:
    print("Colonne analysée :", colonne)

    display(
        ventes_controle.agg(
            F.min(colonne).alias("minimum"),
            F.max(colonne).alias("maximum"),
            F.count(
                F.when(F.col(colonne).isNull(), 1)
            ).alias("non_convertibles"),
            F.count(
                F.when(F.col(colonne) < 0, 1)
            ).alias("valeurs_negatives"),
            F.count(
                F.when(F.col(colonne) == 0, 1)
            ).alias("valeurs_egales_zero")
        )
    )

# COMMAND ----------

# Recalculer le montant attendu, arrondi à deux décimales
ventes_controle = ventes_controle.withColumn(
    "montant_calcule",
    F.round(F.col("quantite_num") * F.col("prix_num"), 2)
)

# Comparer le montant du fichier au montant recalculé
ventes_controle = ventes_controle.withColumn(
    "ecart_montant",
    F.col("montant_num") - F.col("montant_calcule")
)

ecarts = ventes_controle.filter(
    F.col("ecart_montant") != 0
)

print("Lignes avec un écart de montant :", ecarts.count())

display(
    ecarts.select(
        "id_vente",
        "quantite_num",
        "prix_num",
        "montant_num",
        "montant_calcule",
        "ecart_montant"
    ).limit(20)
)

# COMMAND ----------

# Convertir le texte en date et heure
ventes_controle = ventes_controle.withColumn(
    "date_vente_ts",
    F.expr("try_cast(date_vente as TIMESTAMP)")
)

# Contrôler les conversions et la période couverte
display(
    ventes_controle.agg(
        F.min("date_vente_ts").alias("premiere_vente"),
        F.max("date_vente_ts").alias("derniere_vente"),
        F.count(
            F.when(F.col("date_vente_ts").isNull(), 1)
        ).alias("dates_non_convertibles")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC # **Table produits**
# MAGIC ## **Analyse**
# MAGIC
# MAGIC Nous avons vérifié les valeurs manquantes et l’unicité des identifiants. Aucun identifiant produit ne se répète et aucune valeur NULL ou vide n’a été détectée.
# MAGIC
# MAGIC En regroupant les produits par nom, nous avons trouvé cinq noms présents chacun deux fois. Les dix lignes concernées ont des identifiants et des prix différents. Nous ne pouvons donc pas affirmer qu’il s’agit de produits enregistrés en double.
# MAGIC
# MAGIC Nous avons aussi converti les prix en nombres et contrôlé leurs minimums et maximums. Tous les prix sont convertibles et strictement positifs. Enfin, nous avons examiné les catégories sans relever de différence d’écriture évidente.
# MAGIC ## **Traitements et justifications**
# MAGIC **Noms répétés : mise en quarantaine proposée.** Nous proposons d’isoler les dix lignes pour les faire vérifier par Sophie et Marc, sans les supprimer ni les fusionner. Le cours recommande d’isoler les cas douteux plutôt que de modifier les données sans certitude.
# MAGIC
# MAGIC **Prix et catégories : conservation.** Les contrôles réalisés ne justifient aucune correction. Nous utilisons les prix convertis en nombres pour les calculs et conservons les valeurs originales. Les catégories restent à vérifier avec la liste autorisée.
# MAGIC
# MAGIC
# MAGIC # **Table ventes**
# MAGIC ## **Analyse**
# MAGIC Nous avons regroupé les lignes par identifiant de vente, puis comparé le nombre total de lignes au nombre de lignes entièrement distinctes. Nous avons ainsi identifié 1 500 copies supplémentaires identiques sur toutes les colonnes.
# MAGIC
# MAGIC Après dédoublonnage, nous avons compté les identifiants clients manquants par canal. Ils concernent uniquement les ventes en magasin. Il peut s’agir d’achats anonymes, mais nous devons faire confirmer cette explication.
# MAGIC
# MAGIC Nous avons ensuite converti les quantités, les prix et les montants en nombres. Nous avons relevé 2 924 quantités négatives, associées à des montants négatifs. Le recalcul de chaque montant à partir de la quantité et du prix unitaire n’a montré aucun écart. Ces lignes peuvent correspondre à des retours, mais leur signification reste à vérifier.
# MAGIC
# MAGIC La conversion des dates n’a présenté aucun échec. Nous avons relevé la première et la dernière date de vente pour les comparer à la période attendue dans le sujet.
# MAGIC ## **Traitements et justifications**
# MAGIC **Copies identiques : dédoublonnage effectué.** Nous avons conservé une seule occurrence de chaque ligne identique dans le tableau de travail, qui contient désormais 300 000 lignes. Cela évite de compter certaines ventes deux fois. Le fichier source est conservé et l’opération doit être inscrite dans le journal des traitements.
# MAGIC
# MAGIC **Clients non renseignés : conservation et signalement.** Nous conservons les ventes concernées pour ne pas les exclure des analyses globales. Nous ne complétons pas les identifiants, car nous ne connaissons pas leur valeur. Le cours précise qu’une donnée manquante ne doit pas être inventée. Sophie et Marc devront confirmer si le client peut rester non renseigné pour une vente en magasin.
# MAGIC
# MAGIC **Quantités et montants négatifs : mise en quarantaine proposée.** Nous proposons d’isoler les lignes concernées en attendant leur validation par Sophie et Marc. Nous ne modifions pas les signes : si ces opérations sont des retours, leurs valeurs négatives doivent être conservées.
# MAGIC
# MAGIC **Autres valeurs numériques et dates : conservation.** Aucun échec de conversion ni écart de montant n’a été détecté. Les contrôles réalisés ne justifient pas de correction supplémentaire.
# MAGIC
# MAGIC Les mises en quarantaine proposées n’ont pas encore été réalisées. Les traitements retenus suivent les règles du cours : conserver les données d’origine, ne pas inventer de valeur, tracer les modifications et faire valider les cas douteux par le métier.

# COMMAND ----------

produits.createOrReplaceTempView("produits")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- recherche de doublons sur id_produit (ok)
# MAGIC SELECT id_produit, COUNT(*) AS nombre_occurrences
# MAGIC FROM produits
# MAGIC GROUP BY id_produit
# MAGIC HAVING COUNT(*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- prix invalides : vides, négatifs ou nuls (ok)
# MAGIC SELECT id_produit, nom, prix_unitaire
# MAGIC FROM produits
# MAGIC WHERE prix_unitaire IS NULL OR CAST(prix_unitaire AS DOUBLE) <= 0

# COMMAND ----------

# MAGIC %sql
# MAGIC -- catégories et fourchettes de prix (ok : 6 catégories, prix cohérents avec chaque catégorie)
# MAGIC SELECT categorie, COUNT(*) AS nombre_produits,
# MAGIC        MIN(CAST(prix_unitaire AS DOUBLE)) AS prix_min,
# MAGIC        MAX(CAST(prix_unitaire AS DOUBLE)) AS prix_max,
# MAGIC        ROUND(AVG(CAST(prix_unitaire AS DOUBLE)), 2) AS prix_moyen
# MAGIC FROM produits
# MAGIC GROUP BY categorie
# MAGIC ORDER BY categorie

# COMMAND ----------

# MAGIC %sql
# MAGIC -- noms de produits en double (5) (ko : à signaler, l'identifiant reste unique)
# MAGIC SELECT nom, COUNT(*) AS nombre, collect_list(id_produit) AS id_produits
# MAGIC FROM produits
# MAGIC GROUP BY nom
# MAGIC HAVING COUNT(*) > 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Produits : interpréter, décider, justifier
# MAGIC
# MAGIC | Constat | Interprétation | Décision |
# MAGIC |---|---|---|
# MAGIC | Aucun doublon sur id_produit, aucun prix vide ou ≤ 0 | Référentiel propre | ok, on garde les contrôles pour les prochaines livraisons (**quarantaine** si un prix est invalide) |
# MAGIC | 5 noms portés par 2 produits différents | Réalité métier possible (même nom, deux références) | **Garder et signaler** : on travaille toujours avec id_produit, jamais avec le nom |
# MAGIC | Prix cohérents avec la catégorie (High-tech de 41 à 399 €, Épicerie de 1 à 15 €) | – | ok |
# MAGIC
# MAGIC **Pour Sophie** : le prix catalogue sert de référence pour vérifier le montant des ventes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Passage en silver : on applique les décisions
# MAGIC 1. **Corriger** : convertir le prix en décimal.
# MAGIC 2. **Mettre en quarantaine** : identifiant en double, prix vide ou ≤ 0.
# MAGIC 3. **Garder et signaler** : les noms en double (l'identifiant reste unique).

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

# 1. Convertir les types (le CSV est lu en texte)
produits_types = produits.withColumn("prix_unitaire", F.expr("try_cast(prix_unitaire AS DOUBLE)"))

# 2. Règles de quarantaine
produits_controle = produits_types \
    .withColumn("nb_id", F.count("*").over(Window.partitionBy("id_produit"))) \
    .withColumn("motif_rejet",
        F.when(F.col("nb_id") > 1, "id_produit en double")
         .when(F.col("prix_unitaire").isNull(), "prix vide ou illisible")
         .when(F.col("prix_unitaire") <= 0, "prix negatif ou nul")) \
    .drop("nb_id")

# 3. Garder et signaler : nom porté par plusieurs produits
produits_controle = produits_controle.withColumn(
    "nom_en_double", F.count("*").over(Window.partitionBy("nom")) > 1)

produits_quarantaine = produits_controle.filter(F.col("motif_rejet").isNotNull())
produits_silver = produits_controle.filter(F.col("motif_rejet").isNull()).drop("motif_rejet")
print("Produits en silver :", produits_silver.count())            # 200
print("Produits en quarantaine :", produits_quarantaine.count())  # 0
print("Produits signalés (nom en double) :", produits_silver.filter("nom_en_double").count())   # 10
display(produits_silver.limit(10))

# COMMAND ----------

# Conserver la table produits nettoyée dans la couche Silver
produits_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/Volumes/workspace/default/tp1_datalake/silver/produits")

# Les lignes rejetées sont conservées à part, avec leur motif (quarantaine)
produits_quarantaine.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/Volumes/workspace/default/tp1_datalake/silver/quarantaine_produits")

# COMMAND ----------

# Vérification de la table silver
verif = spark.read.format("delta").load("/Volumes/workspace/default/tp1_datalake/silver/produits")
verif.printSchema()
print("Lignes en silver :", verif.count())   # 200