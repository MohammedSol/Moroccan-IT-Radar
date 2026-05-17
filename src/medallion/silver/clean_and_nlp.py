"""
Socle PySpark de la Zone Silver (Architecture Médaillon).
Traitement Multi-Domaines : Emploi (LinkedIn), Presse (RSS), Statistiques (HCP).
Stratégie Hybride : Boto3 + PySpark + Pandas.
"""

from __future__ import annotations

import os
import boto3
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lower, regexp_replace, regexp_extract

# --- LE FIX POUR JAVA 21+ SOUS WINDOWS ---
os.environ["PYSPARK_SUBMIT_ARGS"] = '--driver-java-options "-Djava.security.manager=allow" pyspark-shell'
# -----------------------------------------

class SilverLayerETL:
    def __init__(self) -> None:
        print("⏳ Initialisation de Spark (Mode Local Ultra-Léger)...")
        self.spark = SparkSession.builder.appName("SilverLayer_MultiDomain").getOrCreate()

        self.s3_client = boto3.client(
            's3',
            endpoint_url='http://localhost:9002',
            aws_access_key_id='admin123',
            aws_secret_access_key='admin123'
        )

    def process_linkedin(self) -> None:
        """Domaine 1 : Nettoyage des offres d'emploi"""
        print("\n" + "="*50)
        print("🚀 LANCEMENT DU DOMAINE 1 : OFFRES D'EMPLOI (LINKEDIN)")
        print("="*50)
        
        local_raw = "tmp_linkedin_raw.json"
        local_clean = "tmp_linkedin_clean.parquet"

        try:
            self.s3_client.download_file('bronze-zone', 'raw/offres/linkedin_raw.json', local_raw)
            df = self.spark.read.option("multiline", "true").json(local_raw)
            
            df_clean = (
                df.withColumn(
                    "localisation_propre",
                    when(lower(col("localisation")).contains("الدار البيضاء") | lower(col("localisation")).contains("casablanca") | lower(col("localisation")).contains("كازابلانكا"), "Casablanca")
                    .when(lower(col("localisation")).contains("الرباط") | lower(col("localisation")).contains("rabat"), "Rabat")
                    .otherwise("Autre/Maroc")
                )
                .withColumn(
                    "domaine_it",
                    when(lower(col("titre")).contains("ai") | lower(col("titre")).contains("machine learning") | lower(col("titre")).contains("data"), "Data & AI")
                    .when(lower(col("titre")).contains("developer") | lower(col("titre")).contains("développeur") | lower(col("titre")).contains("software") | lower(col("titre")).contains("back-end"), "Software Engineering")
                    .otherwise("Autre IT")
                )
            )

            df_clean.toPandas().to_parquet(local_clean, engine="pyarrow", index=False)
            self.s3_client.upload_file(local_clean, 'silver-zone', 'offres_clean/linkedin_clean.parquet')
            print("✅ Domaine 1 terminé avec succès.")
        except Exception as e:
            print(f"⚠️ Domaine 1 ignoré ou en erreur : {e}")
        finally:
            if os.path.exists(local_raw): os.remove(local_raw)
            if os.path.exists(local_clean): os.remove(local_clean)

    def process_news(self) -> None:
        """Domaine 2 : Nettoyage de la Presse IT"""
        print("\n" + "="*50)
        print("🚀 LANCEMENT DU DOMAINE 2 : PRESSE IT (RSS)")
        print("="*50)
        
        local_raw = "tmp_news_raw.json"
        local_clean = "tmp_news_clean.parquet"

        try:
            self.s3_client.download_file('bronze-zone', 'raw/articles/news_rss_raw.json', local_raw)
            df = self.spark.read.option("multiline", "true").json(local_raw)
            
            df_clean = (
                df.withColumn(
                    # 1. Nettoyage HTML : Remplace tout ce qui ressemble à des balises <...> par du vide
                    "texte_propre",
                    regexp_replace(col("contenu_texte"), "<[^>]+>", "")
                )
                .withColumn(
                    # 2. Extraction Mots-clés
                    "sujet_principal",
                    when(lower(col("contenu_texte")).contains("5g"), "5G")
                    .when(lower(col("contenu_texte")).contains("gitex"), "GITEX")
                    .when(lower(col("contenu_texte")).contains("startup"), "Startups")
                    .otherwise("Tech Général")
                )
            )

            df_clean.toPandas().to_parquet(local_clean, engine="pyarrow", index=False)
            self.s3_client.upload_file(local_clean, 'silver-zone', 'articles_clean/news_clean.parquet')
            print("✅ Domaine 2 terminé avec succès.")
        except Exception as e:
            print(f"⚠️ Domaine 2 ignoré ou en erreur (Fichier Bronze manquant ?) : {e}")
        finally:
            if os.path.exists(local_raw): os.remove(local_raw)
            if os.path.exists(local_clean): os.remove(local_clean)

    def process_hcp(self) -> None:
        """Domaine 3 : Structuration des données extraites des PDF"""
        print("\n" + "="*50)
        print("🚀 LANCEMENT DU DOMAINE 3 : STATISTIQUES (HCP & RAPPORTS)")
        print("="*50)
        
        files_to_process = [
            "hcp_activite_chomage_2024_extracted.json",
            "rapport_chercheur_impasse_IT_extracted.json"
        ]
        
        # On garde une trace des chemins locaux pour les supprimer à la fin
        downloaded_paths = []

        try:
            all_dfs = []
            for file_name in files_to_process:
                local_path = f"tmp_{file_name}"
                print(f"⏳ Téléchargement de {file_name}...")
                self.s3_client.download_file('bronze-zone', f'raw/pdfs/{file_name}', local_path)
                downloaded_paths.append(local_path) # On l'ajoute à la liste de nettoyage
                
                temp_df = self.spark.read.option("multiline", "true").json(local_path)
                
                # Projection des colonnes pour éviter le conflit 'tables'
                column_names = temp_df.columns
                text_col = "raw_text" if "raw_text" in column_names else "contenu_texte"
                
                temp_df = temp_df.select(
                    col(text_col).alias("texte_brut"),
                    when(col(text_col).isNotNull(), file_name).alias("source_file")
                )
                all_dfs.append(temp_df)

            # Union
            df = all_dfs[0]
            for next_df in all_dfs[1:]:
                df = df.union(next_df)

            # Nettoyage & NLP
            df_clean = df.withColumn(
                "taux_extrait",
                regexp_extract(col("texte_brut"), r"(\d+[.,]?\d*)\s*%", 1)
            )

            # --- L'ACTION QUI FORCE LA LECTURE ---
            print("⏳ Conversion et Upload...")
            pdf = df_clean.toPandas() # Ici Spark lit enfin les fichiers JSON
            
            local_parquet = "tmp_hcp_final.parquet"
            pdf.to_parquet(local_parquet, engine="pyarrow", index=False)
            self.s3_client.upload_file(local_parquet, 'silver-zone', 'statistiques_clean/hcp_rapports.parquet')
            
            print("✅ Domaine 3 terminé avec succès.")
            downloaded_paths.append(local_parquet)

        except Exception as e:
            print(f"❌ Erreur Domaine 3 : {e}")

        finally:
            # ON NETTOIE TOUT ICI, SEULEMENT APRÈS LE SUCCÈS OU L'ERREUR
            print("🧹 Nettoyage final des fichiers temporaires HCP...")
            for path in downloaded_paths:
                if os.path.exists(path):
                    os.remove(path)
    def run_all(self) -> None:
        """Orchestration des trois pipelines"""
        self.process_linkedin()
        self.process_news()
        self.process_hcp()
        print("\n🎉 PILELINE SILVER GLOBAL TERMINÉ !")

if __name__ == "__main__":
    etl = SilverLayerETL()
    etl.run_all()