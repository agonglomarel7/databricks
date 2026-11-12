# Alpenia Assurances — Data Engineering avec Databricks

## 1. Présentation du projet

Ce projet consiste à concevoir et mettre en œuvre un pipeline de données pour **Alpenia Assurances**, dans un environnement Databricks.

L'objectif est d'ingérer des données provenant de plusieurs systèmes sources, de les stocker dans des tables Delta, d'effectuer des premières transformations avec PySpark, puis de réaliser des analyses métier en SQL.

Le projet permet également de découvrir les mécanismes de gestion des données de Delta Lake, notamment le versionnement, l'historique des opérations et la restauration des données.

## 2. Objectifs

Les principaux objectifs sont :

- Ingestion de fichiers provenant de plusieurs sources et formats.
- Stockage des données dans des tables Delta.
- Organisation des données avec Unity Catalog.
- Réalisation de transformations avec PySpark.
- Contrôle de la qualité et de la cohérence des données.
- Exploitation des données avec des requêtes SQL.
- Exploration des fonctionnalités de gestion transactionnelle et d'historique de Delta Lake.

## 3. Architecture technique

Le projet repose sur une architecture simple organisée autour de Databricks et de Unity Catalog.

```text
                  SOURCES DE DONNÉES
                         |
          +--------------+--------------+
          |              |              |
        CSV           PARQUET          JSON
          |              |              |
          +--------------+--------------+
                         |
                  VOLUME LANDING
                         |
                         v
                 NOTEBOOK 02_INGESTION
                         |
                         v
                UNITY CATALOG / BRONZE
                         |
                  TABLES DELTA
                         |
                         v
              NOTEBOOK 03_TRANSFORMATIONS
                         |
              PySpark / DataFrame API
                         |
                         v
                TABLES BRONZE TRANSFORMÉES
                         |
                         v
                 NOTEBOOK 04_EXPLORATION
                         |
                      SQL
                         |
                         v
                 ANALYSES MÉTIER
```

### Environnement

| Composant | Technologie |
|---|---|
| Plateforme | Databricks Free Edition |
| Langage de programmation | Python |
| Traitement distribué | PySpark |
| Langage de requête | SQL |
| Stockage analytique | Delta Lake |
| Gouvernance et catalogue | Unity Catalog |
| Versionnement du code | Git |

## 4. Organisation des données

Le catalogue Unity Catalog utilisé est `alpenia_dev`.

Le schéma `bronze` contient le volume de fichiers sources et les six tables Delta créées à partir des données ingérées.

### Structure du catalogue

```text
alpenia_dev
└── bronze
    ├── landing/              # Fichiers sources
    ├── brz_contracts        # Contrats
    ├── brz_clients          # Clients
    ├── brz_pricing          # Tarification
    ├── brz_claims           # Sinistres
    ├── brz_web_events       # Interactions web
    └── brz_expertises       # Expertises
```

### Sources de données

| Dataset | Format | Nombre de lignes |
|---|---|---:|
| Contracts | CSV | 4 000 |
| Clients | CSV | 3 000 |
| Pricing | CSV | 6 |
| Claims | Parquet | 24 000 |
| Web Events | JSON | 90 000 |
| Expertises | JSON | 450 |
| **Total** | | **121 456** |

Les fichiers sont déposés dans le volume Unity Catalog `alpenia_dev.bronze.landing`.

## 5. Étapes de réalisation

### Étape 1 — Initialisation

Création des éléments nécessaires dans Unity Catalog :

- Catalogue `alpenia_dev`.
- Schéma `bronze`.
- Volume `landing`.

### Étape 2 — Ingestion des données

Le notebook `02_ingestion` assure la lecture des fichiers sources avec PySpark.

Les formats sont traités selon leur nature :

- CSV avec gestion de l'en-tête et inférence des types.
- Parquet pour les données de sinistres.
- JSON pour les interactions web et les expertises.
- JSON multilignes pour le fichier des expertises.

Les DataFrames obtenus sont enregistrés sous forme de tables Delta dans le schéma `bronze`.

Une fonction générique d'écriture permet de factoriser la création des six tables.

**Contrôle d'idempotence :**

Le notebook d'ingestion a été exécuté deux fois. Les volumes obtenus sont restés identiques aux volumes attendus pour les six datasets.

Cette vérification valide la reproductibilité de l'ingestion sur le jeu de fichiers utilisé.

### Étape 3 — Premières transformations PySpark

Le notebook `03_transformations` réalise les transformations demandées en utilisant exclusivement l'API DataFrame de PySpark.

Les principales opérations sont :

**Typage des données**
- Vérification des types des colonnes.
- Conversion des timestamps des interactions web.
- Conversion des dates d'ouverture et de clôture des expertises.
- Conservation des montants de tarification au format DOUBLE.

**Aplatissement des données**
- Remplacement de la structure imbriquée `quote` dans `brz_web_events` par deux colonnes :
  - `quote_product`
  - `quote_premium_eur`

**Création de colonnes dérivées**
- `settlement_days` dans `brz_claims` : délai de règlement d'un sinistre en jours, arrondi à une décimale.
- `items_count` dans `brz_expertises` : nombre d'éléments remplacés.
- `is_open` dans `brz_expertises` : indicateur booléen permettant d'identifier les expertises encore ouvertes.

Les tables transformées sont réécrites dans le schéma `bronze`, avec mise à jour explicite du schéma Delta lorsque nécessaire.

### Étape 4 — Exploration et interrogation SQL

Le notebook `04_exploration` permet d'exploiter les données transformées à travers des requêtes SQL répondant à plusieurs besoins métier :

1. Nombre de contrats actifs par ville et par produit.
2. Nombre de sinistres, montant total des sinistres et ratio de sinistralité par ville.
3. Délai moyen de règlement par nature de sinistre.
4. Identification des dix clients ayant abandonné le plus de devis dans les interactions web.
5. Coût total des expertises par type de constat et nombre d'expertises encore ouvertes.

Les résultats permettent d'étudier l'activité commerciale, la sinistralité, les comportements des clients et les coûts d'expertise.

### Étape 5 — Métadonnées et historique Delta

Les commandes SQL suivantes permettent d'examiner les caractéristiques et les opérations réalisées sur les tables Delta :

```sql
DESCRIBE DETAIL alpenia_dev.bronze.brz_contracts;

DESCRIBE HISTORY alpenia_dev.bronze.brz_contracts;
```

`DESCRIBE DETAIL` permet notamment d'identifier :

- Le format de stockage.
- L'emplacement physique de la table.
- Le nombre de fichiers.
- La taille des données.

`DESCRIBE HISTORY` permet de retracer les opérations effectuées sur une table, avec leur version, leur date et leur type.

## 6. Fonctionnalités avancées de Delta Lake

Une opération de restauration a été réalisée sur `brz_contracts`.

L'historique Delta indique une opération `RESTORE` en version 5, avec restauration de la version 2.

Cette opération illustre la capacité de Delta Lake à revenir à un état antérieur de la table tout en conservant la trace de la restauration dans son historique.

D'autres fonctionnalités peuvent être explorées dans le cadre des travaux complémentaires :

- **Time Travel** : consultation des données à une version antérieure.
- **RESTORE** : restauration d'une version précédente.
- **OPTIMIZE** : réorganisation des fichiers pour améliorer les performances de lecture.
- **VACUUM** : suppression des anciens fichiers de données qui ne sont plus nécessaires et qui ont dépassé la durée de rétention autorisée.

## 7. Organisation du dépôt

```text
alpenia-databricks/
│
├── README.md
│
├── notebooks/
│   ├── 01_setup
│   ├── 02_ingestion
│   ├── 03_transformations
│   └── 04_exploration
│
├── docs/
│   └── images/
│       └── unity_catalog_bronze.png
│
└── .gitignore
```

Les notebooks présentés correspondent aux différentes étapes du pipeline.

## 8. Compétences mobilisées

Ce projet met en pratique plusieurs compétences en ingénierie des données :

- Lecture et ingestion de données hétérogènes.
- Traitement de données avec PySpark.
- Manipulation des DataFrames.
- Gestion des tables Delta.
- Utilisation de Unity Catalog.
- Écriture de requêtes SQL analytiques.
- Contrôle de l'idempotence des traitements.
- Gestion des versions et restauration avec Delta Lake.
- Organisation et documentation d'un projet Data Engineering.

## 9. Conclusion

Ce projet a permis de mettre en œuvre un premier pipeline Data Engineering dans Databricks, depuis l'ingestion de fichiers hétérogènes jusqu'à leur exploitation analytique.

Il a également permis de mettre en pratique les concepts fondamentaux de Delta Lake, notamment la gestion du schéma, l'historisation des opérations et la restauration des données.

L'ensemble constitue une première base technique pour la mise en place de pipelines de données fiables, reproductibles et exploitables dans un environnement analytique.

---

**Projet :** Alpenia Assurances  
**Plateforme :** Databricks  
**Technologies :** Python, PySpark, SQL, Delta Lake, Unity Catalog
