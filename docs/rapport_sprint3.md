# Rapport complet — Architecture IA/Data & avancement du projet **Marché IT Maroc**

## 1) Résumé exécutif

Le projet implémente une architecture locale **Médaillon + Lambda** :

- **Bronze** : ingestion brute (PDF, presse, LinkedIn) dans MinIO
- **Speed Layer** : streaming Kafka en temps réel
- **Silver** : nettoyage/enrichissement via PySpark
- **Gold** : chargement analytique dans PostgreSQL (schéma en étoile)
- **Dashboard** : visualisation Streamlit

---

## 2) Stack technique et rôle des outils

### Stockage / Data Lake
- **MinIO (S3-compatible)** : stockage Bronze/Silver (`bronze-zone`, `silver-zone`)

### Streaming
- **Kafka + kafka-python** : diffusion d’événements temps réel (`it_market_events`)

### Data Warehouse
- **PostgreSQL** : couche Gold (`gold.dim_*`, `gold.fait_offres_emploi`, tables pont)

### Traitement Data
- **PySpark** : transformations batch Silver et chargement Gold

### Extraction/Scraping
- **requests + BeautifulSoup4** : scraping HTML
- **feedparser** : ingestion RSS
- **pdfplumber** : extraction texte + tableaux PDF

### Intégration S3
- **boto3** : upload/download MinIO

### Visualisation
- **Streamlit + pandas** : dashboard interactif

---

## 3) Ce qui a été réalisé

### A) Ingestion Bronze

#### `src/ingestion/load_pdfs.py`
- Upload brut PDF vers `bronze-zone/raw/pdfs/`
- Extraction texte et tableaux
- JSON local puis upload vers `bronze-zone/extracted/`
- Logs et gestion d’exceptions

#### Refactor qualité PDF
- Normalisation encodage UTF-8
- Filtre anti faux-tableaux :
  - minimum 2 colonnes
  - présence statistique de chiffres

---

### B) Scraping Presse

#### `src/scraping/spiders/news_spider.py`
- Classe orientée objet par source
- Sources actives : Médias24, L’Économiste, TIC Maroc
- Extraction robuste du contenu texte
- Sauvegarde locale + upload MinIO (`raw/articles/news_raw.json`)

#### `src/scraping/spiders/rss_monitor.py`
- Surveillance RSS (L’Économiste, TIC Maroc)
- Filtrage par mots-clés métier
- Extraction article + stockage Bronze (`news_rss_raw.json`)
- Intégration Kafka (Speed Layer) avec tolérance aux pannes

---

### C) Streaming Kafka

#### `src/ingestion/kafka_producer.py`
- Classe `MarketDataProducer`
- JSON serializer UTF-8
- Envoi fiable (`future.get(timeout=10)`)
- `flush + close` propre

#### `src/ingestion/test_consumer.py`
- Consumer de validation temps réel
- `auto_offset_reset='latest'`
- Affichage live des messages

---

### D) LinkedIn Jobs

#### `src/scraping/spiders/linkedin_jobs_spider.py`
- Pagination endpoint guest LinkedIn
- Parsing offres + envoi immédiat Kafka
- Anti-ban (headers + sleep aléatoire)
- Ajout Batch Layer :
  - accumulateur global
  - JSON local `linkedin_raw.json`
  - upload Bronze `raw/offres/linkedin_raw.json`

---

### E) Silver (PySpark)

#### `src/medallion/silver/clean_and_nlp.py`
- Mode Bypass S3A (Boto3 + Spark local) pour Windows
- Nettoyage localisation (UDF)
- Catégorisation `profil_tech`
- Écriture Parquet local
- Upload vers `silver-zone/offres_clean/`

---

### F) Gold (PySpark → PostgreSQL)

#### `src/medallion/gold/silver_to_gold.py`
- Configuration Spark + JDBC + S3A (Windows/Java 21+)
- Fix runtime S3A :
  - timeouts numériques (`60000`)
  - `multipart.purge.age=86400`
  - provider credentials compatible Hadoop AWS 3.3.4
- Chargement idempotent dimensions (`left_anti`) :
  - `gold.dim_source`
  - `gold.dim_poste`
  - `gold.dim_date`
  - `gold.dim_ville` (fallback `Non spécifié`)
- Ajout chargement faits :
  - jointure vers IDs dimensions
  - calcul `nb_competences`, `salaire_min`, `salaire_max`
  - écriture append dans `gold.fait_offres_emploi`
- Bloc de vérification finale JDBC

---

## 4) Problèmes rencontrés et résolutions

- **S3A `NumberFormatException` (`60s`, `24h`)**  
  Résolu via overrides explicites numériques dans `spark.hadoop.*` et `hconf.set(...)`.

- **AWS credentials provider introuvable**  
  Résolu via `org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider`.

- **Colonnes Silver absentes côté Gold**  
  Résolu en limitant les colonnes sélectionnées aux colonnes présentes.

- **Parsing date Spark strict (`SparkUpgradeException` / `DateTimeException`)**  
  Résolu avec sanitation des dates vides, parsing tolérant, `spark.sql.ansi.enabled=false`.

- **Dashboard `.dt` pandas**  
  Erreur identifiée : colonne utilisée par `.dt` non datetime.

---

## 5) État actuel des tables Gold

### Implémentées
- `gold.dim_source`
- `gold.dim_poste`
- `gold.dim_date`
- `gold.dim_ville`
- `gold.fait_offres_emploi`

### À finaliser
- `gold.dim_competence`
- `gold.pont_offres_competences`

---

## 6) Lecture architecture (rôle par couche)

- **Bronze** : conservation brute, traçabilité
- **Speed** : disponibilité immédiate des événements
- **Silver** : standardisation métier et nettoyage
- **Gold** : données analytiques prêtes BI

---

## 7) Recommandations de clôture

1. Convertir explicitement les dates pandas avant `.dt` dans Streamlit  
2. Migrer `pd.read_sql` vers SQLAlchemy engine  
3. Finaliser `dim_competence` + `pont_offres_competences`  
4. Ajouter un mécanisme d’idempotence pour la table de faits

---

## 8) Conclusion

Le projet dispose désormais d’une base solide **end-to-end** en architecture Médaillon + Lambda.  
Les composants critiques ingestion/streaming/silver/gold sont en place.  
La phase suivante concerne surtout les finitions analytiques (compétences/pont) et la robustesse dashboard.
