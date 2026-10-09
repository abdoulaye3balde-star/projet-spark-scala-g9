# EcommerceAnalytics — Système d'analyse de données e-commerce distribué

Pipeline Spark / Scala qui ingère quatre sources (CSV, JSON, Parquet), valide les données,
les enrichit (jointures, UDF temporelle, fonctions de fenêtrage) puis calcule des indicateurs
business (KPI marchands, rétention par cohortes, produits, paiements).

| Composant | Version |
|---|---|
| Scala | 2.12.18 |
| Apache Spark | 3.5.1 |
| JDK | 17 (11 également compatible) |
| sbt | 1.10.x (fixé dans `project/build.properties`) |

## 1. Architecture

```
src/main/scala/com/ecommerce/
├── models/Models.scala              case classes Transaction, User, Product, Merchant
├── utils/
│   ├── ConfigLoader.scala           lecture de application.conf (+ valeurs par défaut)
│   ├── SparkSessionBuilder.scala    création de la SparkSession
│   ├── DataFrameWriterUtils.scala   écriture CSV + Parquet, CSV unique, taux de rejet
│   └── StepTimer.scala              début / fin / durée de chaque étape
└── analytics/
    ├── DataIngestion.scala          lecture multi-format -> Dataset[T]
    ├── DataValidation.scala         règles de validation (valides + rejets)
    ├── DataQuality.scala            rapport de qualité (+ orphelins)
    ├── TimeFeatures.scala           UDF extractTimeFeatures
    ├── DataTransformation.scala     jointures, fenêtres, transactions suspectes
    ├── Analytics.scala              KPI marchands, cohortes, produits, paiements
    ├── SparkOptimizations.scala     cache / persist / unpersist / broadcast
    └── MainApp.scala                orchestration du pipeline
```

Flux : `ingestion -> validation -> transformation -> analytique -> écriture`.

## 2. Prérequis

1. **JDK 17** (par ex. Temurin 17). Les JDK 21 et 25 ne sont pas supportés par Spark 3.5.
   Vérifier : `java -version`.
2. **sbt 1.x** (https://www.scala-sbt.org/download). sbt télécharge lui-même Scala 2.12.18
   et les dépendances Spark : il n'est pas nécessaire d'installer Scala ni Spark pour
   compiler et exécuter en local.
3. **Apache Spark 3.5.1** (pré-compilé pour Hadoop 3) — uniquement pour la commande
   `spark-submit` de la section 5.
4. **Windows uniquement** : l'écriture de fichiers par Spark nécessite `winutils.exe` et
   `hadoop.dll` (Hadoop 3.3.x) dans `C:\hadoop\bin`, la variable d'environnement
   `HADOOP_HOME=C:\hadoop` et `C:\hadoop\bin` dans le `Path`.

## 3. Données

Les quatre fichiers sources sont dans `src/main/resources/data/` :
`transactions.csv`, `users.json`, `products.parquet/` (répertoire) et `merchants.csv`.
Les chemins sont définis dans `src/main/resources/application.conf` (jamais en dur dans le code).

## 4. Compilation et exécution locale

Depuis la racine du projet :

```bash
sbt compile                     # compilation
sbt test                        # tests unitaires
sbt assembly                    # JAR exécutable -> target/scala-2.12/ecommerce-analytics.jar
sbt run                         # pipeline complet en mode local (local[*])
sbt "run ingestion"             # une seule étape (voir ci-dessous)
```

Les résultats sont écrits dans `output/` (voir section 7).

**Exécution par étape** (argument optionnel, `all` par défaut) :

| Argument | Étapes exécutées |
|---|---|
| `ingestion` | ingestion + validation + rapport de qualité + rejets |
| `transformation` | ce qui précède + enrichissement + transactions suspectes |
| `analytics` ou `all` | pipeline complet |

Un argument inconnu affiche un message d'aide sans lever d'exception.
Chaque étape journalise son heure de début, de fin et sa durée.

## 5. Déploiement avec spark-submit

```bash
spark-submit \
  --class com.ecommerce.analytics.MainApp \
  --master local[*] \
  target/scala-2.12/ecommerce-analytics.jar all
```

Sur un cluster, remplacer `--master` (ex. `yarn --deploy-mode cluster` ou `spark://hote:7077`)
et fournir les données à un emplacement accessible à tous les nœuds (HDFS, S3, …) en
surchargeant les chemins, sans recompiler :

```bash
spark-submit --class com.ecommerce.analytics.MainApp --master yarn --deploy-mode cluster \
  --conf "spark.driver.extraJavaOptions=-Dapp.data.input.transactions=hdfs:///data/transactions.csv -Dapp.data.output.path=hdfs:///out/" \
  ecommerce-analytics.jar all
```

## 6. Configuration (`application.conf`)

Toute clé absente est remplacée par une valeur par défaut (`ConfigLoader`). Toute clé peut
être surchargée en ligne de commande : `-Dapp.optimization.enable-cache=false`.

| Clé | Rôle | Défaut |
|---|---|---|
| `app.spark.master` | master Spark (ignoré si `spark-submit` en fournit un) | `local[*]` |
| `app.spark.shuffle.partitions` | `spark.sql.shuffle.partitions` | 8 |
| `app.optimization.enable-cache` | cache() / persist() / unpersist() | true |
| `app.optimization.enable-broadcast` | broadcast() des petites tables | true |
| `app.data.input.*` | chemins des 4 sources | `src/main/resources/data/…` |
| `app.data.output.path` | répertoire de sortie | `output/` |
| `app.validation.*` | seuils d'âge, de note, de commission, longueur du timestamp | 16–100, 1–5, 0–1, 14 |
| `app.analytics.*` | fenêtre glissante, jours d'activité, seuils des transactions suspectes | 7 j, 5 j, 300 %, 5 min, 2 |

## 7. Résultats produits

Convention : `output/csv/<nom>/` (en-tête, `coalesce(1)`) et `output/parquet/<nom>/`, mode `overwrite`.
Seul le rapport de qualité est un fichier CSV unique : `output/rapport_qualite_yyyymmdd.csv`.

| Sortie | Question | Contenu |
|---|---|---|
| `rapport_qualite_yyyymmdd.csv` | Q2.4, Q2.5 | lignes lues / valides / rejetées, taux de rejet, valeurs nulles, orphelins |
| `rejets_transactions`, `rejets_users`, `rejets_products`, `rejets_merchants` | Q2.2 | lignes rejetées + `rejection_reason` |
| `transactions_enrichies` | Q3.2, Q3.3 | une ligne par transaction valide, 36 colonnes |
| `transactions_suspectes` | Q3.4 (bonus) | transactions avec au moins 2 conditions suspectes |
| `kpi_marchands` | Q4.1 | KPI, classements, commissions, CA par tranche d'âge |
| `cohortes_retention` | Q4.2 | matrice de rétention (+ CA par cohorte et par période) |
| `meilleure_cohorte_3m` | Q4.2 | cohorte avec la meilleure rétention à 3 mois |
| `top10_produits`, `ca_categorie_region`, `ca_paiement_periode` | Q4.4 (bonus) | analyses produits, catégories, paiements |

## 8. Règles de validation

| Jeu de données | Règle | Motif de rejet |
|---|---|---|
| transactions | `amount > 0` | `amount <= 0` |
| transactions | `timestamp` de 14 caractères | `timestamp_invalide` |
| users | âge entre 16 et 100 | `age_hors_intervalle` |
| users | `annual_income > 0` | `income <= 0` |
| products | `price > 0` | `price <= 0` |
| products | note entre 1 et 5 | `rating_hors_intervalle` |
| merchants | commission entre 0 et 1 | `commission_hors_intervalle` |

Une valeur manquante (null) viole la règle qui la concerne. Si plusieurs règles sont violées,
les motifs sont séparés par ` | ` (ex. `amount <= 0 | timestamp_invalide`).

## 9. Choix techniques principaux

- **Jointures** : `LEFT JOIN` de `transactions` vers `users`, `products` et `merchants`, afin de
  garder une ligne par transaction valide (justification détaillée dans `CONTRIBUTIONS.md`).
  Les trois petites tables sont diffusées avec `broadcast()`.
- **Fenêtres** : `row_number`, `count`, `sum`/`collect_set` sur `rangeBetween` (7 jours en secondes), `lag`.
- **Optimisations** : `cache()` pour les DataFrame réutilisés, `persist(MEMORY_AND_DISK_SER)` pour les
  transactions enrichies, `unpersist()` en fin de traitement, `spark.sql.shuffle.partitions` configurable.
- **Gestion d'erreurs** : chaque lecture est protégée par un `try/catch` ; une erreur fatale est affichée
  et la SparkSession est toujours arrêtée proprement.

## 10. Périmètre traité

Tronc commun complet (Parties 0 à 8). Questions bonus traitées : **2.5** (intégrité référentielle),
**3.4** (transactions suspectes, avec `taux_suspectes_pct` dans les KPI marchands),
**4.4** (analyses produits / catégories / paiements) et **6.2** (exécution modulaire par étape).
Non traitées : 4.3 (RFM) et 5.3 (mesure du gain des optimisations).
