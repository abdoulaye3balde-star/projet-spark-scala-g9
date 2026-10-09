# CONTRIBUTIONS

## 1. Répartition et relecture croisée

| Question | Responsable | Relecteur |
|---|---|---|
| 1.1 Structure SBT | Membre A | Membre C |
| 1.2 build.sbt | Membre A | Membre C |
| 1.3 README.md | Membre A | Membre C |
| 2.1 Ingestion multi-format | Membre A | Membre B |
| 2.2 Validation + rejets | Membre A | Membre B |
| 2.3 Gestion d'erreurs et résumé | Membre A | Membre B |
| 2.4 Rapport de qualité | Membre A | Membre B |
| 2.5 Intégrité référentielle (bonus) | Membre A | Membre B |
| 3.1 UDF extractTimeFeatures | Membre B | Membre A |
| 3.2 enrichTransactionData | Membre B | Membre A |
| 3.3 Fenêtres (7 jours, actif, délai) | Membre B | Membre A |
| 3.4 Transactions suspectes (bonus) | Membre B | Membre A |
| 4.1 KPI marchands | Membre C | Membre B |
| 4.2 Cohortes de rétention | Membre C | Membre B |
| 4.4 Produits / catégories / paiements (bonus) | Membre C | Membre B |
| 5.1 Optimisation du stockage | Membre C | Membres A et B |
| 5.2 Optimisation des jointures | Membre C | Membres A et B |
| 6.1 Application principale | Membre C | Intégration validée par A, B et C |
| 6.2 Exécution par étape (bonus) | Membre C | Membre A |
| 7.1 application.conf | Membre A | Membre C |

## 2. Charge de travail et difficultés (à compléter honnêtement par chaque membre)

| Membre | Heures estimées | Difficultés rencontrées |
|---|---|---|
| Membre A (Seynabou BALDE) | _… h_ | _à compléter_ |
| Membre B (Abdoulaye BALDE) | _… h_ | _à compléter_ |
| Membre C (Modou GNING) | _… h_ | _à compléter_ |

Difficultés techniques rencontrées sur le projet (à attribuer au membre concerné) :
- JDK trop récent (21/25) : Spark 3.5 impose JDK 11 ou 17 ; options `--add-opens` nécessaires sous Java 17.
- sbt 2.x (installé par défaut) change la syntaxe et les dossiers de sortie : sbt est fixé à 1.10.7 pour que les commandes documentées fonctionnent.
- Windows : `winutils.exe` et `hadoop.dll` requis pour que Spark écrive des fichiers.
- Types : `age` est lu en `bigint` depuis le JSON ; un schéma explicite (et des `cast`) évite l'échec de `.as[T]`.
- Colonnes homonymes (`name`, `category`, `price`) après jointure : renommage explicite.

## 3. Journal de relecture croisée (entrées datées, à renseigner par les relecteurs)

| Date | Module relu | Auteur | Relecteur | Remarques |
|---|---|---|---|---|
| _jj/mm/aaaa_ | _fichier_ | _membre_ | _membre_ | _remarques_ |

## 4. Décisions techniques du groupe

**Versions** : Spark 3.5.1 avec Scala 2.12.18 (Spark 3.5 est publié pour Scala 2.12 et 2.13 ; 2.12
est la version la plus répandue) sur JDK 17 (supporté par Spark 3.5, contrairement à JDK 25).
sbt est fixé à 1.10.7 (`project/build.properties`).

**JAR exécutable : `assembly`** (plutôt que `package`). `package` ne contient que nos classes et
oblige à fournir Typesafe Config à l'exécution ; `assembly` produit un JAR unique contenant nos classes
et Typesafe Config. Spark est en scope `provided` : il est fourni par `spark-submit`, ce qui garde le
JAR léger et évite les conflits de versions. Pour `sbt run` en local, `Compile / run` remet les
dépendances `provided` dans le classpath.

**Jointures (Q3.2)** — `transactions` est la table de faits ; trois `LEFT JOIN`, `broadcast()` sur les dimensions :

| Table | Jointure | Justification |
|---|---|---|
| users | `left` | Garder toute transaction valide, même si l'utilisateur est orphelin (400 cas) ou a été rejeté : le sujet demande une ligne par transaction valide ; les colonnes utilisateur restent à `null`. |
| products | `left` | Idem pour les produits orphelins (300 cas) ou rejetés. |
| merchants | `left` | Idem pour les marchands (250 cas) ; ils sont ensuite exclus des KPI marchands (`merchant_name` non nul), qui doivent avoir une ligne par marchand existant. |

Les orphelins ne sont pas perdus : ils sont comptés dans le rapport de qualité (Q2.5).

**Autres choix** :
- Format de sortie : CSV (en-tête, `coalesce(1)`) + Parquet, mode `overwrite`, écriture centralisée dans `DataFrameWriterUtils`.
- Valeur manquante : une valeur `null` viole la règle de validation concernée (donc rejet).
- Tranches d'âge : « Jeune » < 25, « Adulte » 25–44, « Âge Moyen » 45–64, « Senior » ≥ 65. Le sujet laisse 25 ans non couvert (< 25 puis 26–44) ; on le rattache à « Adulte ».
- `is_working_hours` : 1 si l'heure est entre 9h et 17h (inclus), sans distinction de jour.
- `day_period` : « Night » regroupe 22h–6h.
- Fenêtre glissante de 7 jours : `rangeBetween(-7 jours, 0)` sur l'horodatage en secondes, transaction courante incluse.
- `jours_depuis_achat_precedent` : écart en jours calendaires entre deux dates de transaction.
- Panier moyen (Q3.4) : moyenne de toutes les transactions valides de l'utilisateur ; « dépasse de plus de 300 % » signifie un écart > 300 %.
- `period_index` (cohortes) : différence en mois entre le mois de la transaction et le mois de la première transaction.
- Rangs (Q4.1) : fonction `rank()` (ex æquo possibles).
- Orphelins (Q2.5) : seuls les identifiants non nuls absents du référentiel sont comptés ; un identifiant nul est déjà compté dans `nb_valeurs_nulles`.
