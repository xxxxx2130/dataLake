# Databricks notebook source
file_client_path="/Volumes/workspace/default/tp1_datalake/clients.csv"
dfclient= spark.read.format("csv").option("header" , "true").load(file_client_path)
dfclient.printSchema()

# COMMAND ----------

display(dfclient)

# COMMAND ----------

# Vérifier si on a des champs vides
from pyspark.sql import functions as F

null = dfclient.filter(
    F.col("id_client").isNull()
    | (F.trim(F.col("id_client")) == "")
    | F.col("prenom").isNull()
    | (F.trim(F.col("prenom")) == "")
    | F.col("nom").isNull()
    | (F.trim(F.col("nom")) == "")
    | F.col("email").isNull()
    | (F.trim(F.col("email")) == "")
    | F.col("ville").isNull()
    | (F.trim(F.col("ville")) == "")
    | F.col("date_naissance").isNull()
    | (F.trim(F.col("date_naissance")) == "")
    | F.col("date_inscription").isNull()
    | (F.trim(F.col("date_inscription")) == "")
    | F.col("consentement_marketing").isNull()
).count()
display(null)
#(150 champs vides)

# COMMAND ----------

#chercher des ID_client  en double
dfclient.groupBy("id_client") \
    .count() \
    .filter(F.col("count") > 1) \
    .display()

# COMMAND ----------

dfclient.createOrReplaceTempView("dfclient")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id_client,  COUNT(*) AS nombre_occurrences
# MAGIC FROM dfclient
# MAGIC GROUP BY id_client
# MAGIC HAVING COUNT(*) > 1;
# MAGIC -- 0 doublons sur id_client

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     email,
# MAGIC     COUNT(*) AS nombre_occurrences
# MAGIC FROM dfclient
# MAGIC WHERE email is not null
# MAGIC GROUP BY email
# MAGIC HAVING COUNT(*) > 1
# MAGIC
# MAGIC -- 46 doublons sur email

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_emails_vides
# MAGIC FROM dfclient
# MAGIC WHERE email IS NULL OR TRIM(email) = '';

# COMMAND ----------

#verifier les dates 
dfclient.select(
    "id_client", "date_inscription" , "date_naissance"
).display()
#verifer les dates coherente (ok)
dfclient.filter(
    F.to_date("date_inscription") < F.to_date("date_naissance")
).select(
    "id_client", "date_inscription"
).display()

# COMMAND ----------

#verifer les dates futures  (ok)
dfclient.filter(
    F.to_date("date_inscription") > F.current_date()
).select(
    "id_client", "date_inscription"
).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     consentement_marketing,
# MAGIC     COUNT(*) AS nombre
# MAGIC FROM dfclient
# MAGIC GROUP BY consentement_marketing
# MAGIC ORDER BY consentement_marketing;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS nombre_consentements_manquants
# MAGIC FROM dfclient
# MAGIC WHERE consentement_marketing IS NULL
# MAGIC    OR TRIM(consentement_marketing) = '';

# COMMAND ----------

from pyspark.sql import functions as F

# 1. Normaliser les chaînes de caractères
dfclient_silver = dfclient.select(
    F.trim(F.col("id_client")).alias("id_client"),
    F.trim(F.col("prenom")).alias("prenom"),
    F.trim(F.col("nom")).alias("nom"),

    # Normaliser les e-mails : minuscules et espaces supprimés
    F.when(
        F.col("email").isNull() | (F.trim(F.col("email")) == ""),
        F.lit(None).cast("string")
    ).otherwise(
        F.lower(F.trim(F.col("email")))
    ).alias("email"),

    F.trim(F.col("ville")).alias("ville"),

    # Conversion de la date de naissance
    F.to_date(
        F.col("date_naissance"), "yyyy-MM-dd"
    ).alias("date_naissance"),

    # Conversion de la date d'inscription
    F.to_date(
        F.col("date_inscription"), "yyyy-MM-dd"
    ).alias("date_inscription"),

    # Conversion du consentement en booléen
    F.when(
        F.lower(F.trim(F.col("consentement_marketing"))) == "true",
        F.lit(True)
    ).when(
        F.lower(F.trim(F.col("consentement_marketing"))) == "false",
        F.lit(False)
    ).otherwise(
        F.lit(None).cast("boolean")
    ).alias("consentement_marketing")
)

dfclient_silver.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # Traitement des anomalies — Table `clients`
# MAGIC
# MAGIC ## 1. E-mails en double
# MAGIC
# MAGIC **Anomalie constatée :** plusieurs clients possèdent la même adresse e-mail.
# MAGIC
# MAGIC **Traitement retenu : Dédoublonner, sous condition de vérification.**
# MAGIC
# MAGIC **Justification :** une adresse e-mail identique peut indiquer qu'un même client a été enregistré plusieurs fois. Cependant, plusieurs personnes peuvent légitimement partager une adresse e-mail. Il serait donc risqué de supprimer automatiquement des clients uniquement sur la base de leur adresse e-mail.
# MAGIC
# MAGIC **Décision :** conserver les enregistrements tant que l'identité des clients n'est pas confirmée. Si plusieurs lignes correspondent réellement au même client, fusionner les fiches en conservant les informations utiles et un identifiant de référence. Sinon, conserver les clients distincts.
# MAGIC
# MAGIC ## 2. E-mails manquants (`NULL` ou vides)
# MAGIC
# MAGIC **Anomalie constatée :** certains clients n'ont pas d'adresse e-mail renseignée.
# MAGIC
# MAGIC **Traitement retenu : Isoler.**
# MAGIC
# MAGIC **Justification :** aucune autre source de données n'est disponible pour retrouver les adresses manquantes. Il est donc impossible de compléter ces informations de manière fiable. Inventer une adresse ou supprimer les clients entraînerait une perte ou une altération des données.
# MAGIC
# MAGIC **Décision :** conserver les clients dans la table principale, maintenir les e-mails manquants à `NULL` et signaler les lignes concernées dans une table de contrôle ou de rejet pour un éventuel enrichissement ultérieur.
# MAGIC
# MAGIC ## Conclusion
# MAGIC
# MAGIC Les doublons d'e-mails sont soumis à une vérification avant tout dédoublonnage, tandis que les e-mails manquants sont isolés pour suivi sans supprimer les clients. Ces décisions permettent de préserver l'intégrité des données et d'éviter toute correction non justifiée.

# COMMAND ----------

# Conserver la table clients nettoyée dans la couche Silver
dfclient_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/Volumes/workspace/default/tp1_datalake/silver/clients")

# COMMAND ----------

dfclient.createOrReplaceTempView("clients")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- nombre de lignes : 4 999 (5 000 au départ, CLI00042 effacé au TP 4)
# MAGIC SELECT COUNT(*) AS nombre_lignes FROM clients

# COMMAND ----------

# MAGIC %sql
# MAGIC -- recherche de doublons sur id_client (ok)
# MAGIC SELECT id_client, COUNT(*) AS nombre_occurrences
# MAGIC FROM clients
# MAGIC GROUP BY id_client
# MAGIC HAVING COUNT(*) > 1
# MAGIC ORDER BY nombre_occurrences DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- champs vides par colonne : seul email a des vides (150) (ko)
# MAGIC SELECT
# MAGIC   SUM(CASE WHEN id_client IS NULL THEN 1 ELSE 0 END)              AS id_client_vides,
# MAGIC   SUM(CASE WHEN prenom IS NULL THEN 1 ELSE 0 END)                 AS prenom_vides,
# MAGIC   SUM(CASE WHEN nom IS NULL THEN 1 ELSE 0 END)                    AS nom_vides,
# MAGIC   SUM(CASE WHEN email IS NULL OR trim(email) = '' THEN 1 ELSE 0 END) AS email_vides,
# MAGIC   SUM(CASE WHEN ville IS NULL THEN 1 ELSE 0 END)                  AS ville_vides,
# MAGIC   SUM(CASE WHEN date_naissance IS NULL THEN 1 ELSE 0 END)         AS date_naissance_vides,
# MAGIC   SUM(CASE WHEN date_inscription IS NULL THEN 1 ELSE 0 END)       AS date_inscription_vides
# MAGIC FROM clients

# COMMAND ----------

# MAGIC %sql
# MAGIC -- emails partagés par plusieurs clients (45 adresses) (ko : à signaler, cas des foyers)
# MAGIC SELECT email, COUNT(*) AS nombre_clients
# MAGIC FROM clients
# MAGIC WHERE email IS NOT NULL
# MAGIC GROUP BY email
# MAGIC HAVING COUNT(*) > 1
# MAGIC ORDER BY nombre_clients DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- format des emails (ok)
# MAGIC SELECT id_client, email
# MAGIC FROM clients
# MAGIC WHERE email IS NOT NULL AND NOT email RLIKE '^[^@ ]+@[^@ ]+[.][^@ ]+$'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- dates incohérentes : inscription avant la naissance ou naissance dans le futur (ok)
# MAGIC SELECT id_client, date_naissance, date_inscription
# MAGIC FROM clients
# MAGIC WHERE to_date(date_inscription) < to_date(date_naissance)
# MAGIC    OR to_date(date_naissance) > current_date()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- clients mineurs au moment de l'inscription (40) (ko : à vérifier avec le marketing, consentement parental)
# MAGIC SELECT id_client, date_naissance, date_inscription,
# MAGIC        floor(months_between(to_date(date_inscription), to_date(date_naissance)) / 12) AS age_inscription
# MAGIC FROM clients
# MAGIC WHERE months_between(to_date(date_inscription), to_date(date_naissance)) / 12 < 18
# MAGIC ORDER BY age_inscription

# COMMAND ----------

# MAGIC %sql
# MAGIC -- valeurs du consentement marketing (ok : true / false uniquement)
# MAGIC SELECT consentement_marketing, COUNT(*) AS nombre
# MAGIC FROM clients
# MAGIC GROUP BY consentement_marketing

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clients : interpréter, décider, justifier
# MAGIC
# MAGIC | Constat | Erreur technique ou réalité métier ? | Décision |
# MAGIC |---|---|---|
# MAGIC | Aucun doublon sur id_client | – | ok, on garde la règle d'unicité (un doublon ferait compter deux fois les ventes) |
# MAGIC | 150 emails vides (3 %) | Réalité métier : l'email est facultatif | **Garder et signaler** (colonne email_manquant). Le client existe et ses achats comptent |
# MAGIC | 45 adresses partagées par 2 clients | Réalité métier possible (couple, famille) | **Garder et signaler** (email_partage) : utile pour le marketing et pour les droits RGPD de Marc |
# MAGIC | 40 clients mineurs à l'inscription | À vérifier : le CRM accepte des moins de 18 ans | **Garder et signaler** au marketing (consentement parental pour le marketing) |
# MAGIC | Dates et formats d'email | Aucune anomalie | ok |
# MAGIC
# MAGIC **Pour Sophie** : aucun client n'est supprimé, donc aucune vente ne disparaît du CA.
# MAGIC **Pour Marc** : les données personnelles (nom, prénom, email, date de naissance) restent en silver, et seront minimisées en gold (ville, tranche d'âge).