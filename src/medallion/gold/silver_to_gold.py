import os
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    date_format,
    lit,
    month,
    quarter,
    regexp_extract,
    size,
    to_date,
    trim,
    when,
    year,
)

# --- HACKS WINDOWS ---
os.environ["JAVA_TOOL_OPTIONS"] = "-Djava.security.manager=allow"
if os.path.isdir(r"C:\hadoop"):
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


class SilverToGoldETL:
    def __init__(self) -> None:
        self.repo_root = Path(__file__).resolve().parents[3]
        self.jdbc_jar = self.repo_root / "postgresql-42.7.3.jar"
        if not self.jdbc_jar.exists():
            raise FileNotFoundError(f"Driver JDBC introuvable: {self.jdbc_jar}")

        self.db_url = "jdbc:postgresql://localhost:5432/marche_it_gold"
        self.db_properties = {
            "user": "postgres",
            "password": "admin",
            "driver": "org.postgresql.Driver",
        }

        self.spark = (
            SparkSession.builder
            .appName("Silver_to_Gold_ETL")
            .config("spark.jars", str(self.jdbc_jar))
            .config(
                "spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
            )
            .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9002")
            .config("spark.hadoop.fs.s3a.access.key", "admin123")
            .config("spark.hadoop.fs.s3a.secret.key", "admin123")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
            .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
            .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400")
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .config("spark.sql.ansi.enabled", "false")
            .getOrCreate()
        )
        self._override_hadoop_conf()

    def _override_hadoop_conf(self) -> None:
        hconf = self.spark.sparkContext._jsc.hadoopConfiguration()
        hconf.set("fs.s3a.endpoint", "http://localhost:9002")
        hconf.set("fs.s3a.access.key", "admin123")
        hconf.set("fs.s3a.secret.key", "admin123")
        hconf.set("fs.s3a.path.style.access", "true")
        hconf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hconf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        hconf.set("fs.s3a.connection.timeout", "60000")
        hconf.set("fs.s3a.connection.establish.timeout", "10000")
        hconf.set("fs.s3a.connection.request.timeout", "60000")
        hconf.set("fs.s3a.socket.timeout", "60000")
        hconf.set("fs.s3a.threads.keepalivetime", "60000")
        hconf.set("fs.s3a.multipart.purge", "false")
        hconf.set("fs.s3a.multipart.purge.age", "86400")

    def _read_silver(self) -> DataFrame:
        return (
            self.spark.read
            .option("recursiveFileLookup", "true")
            .parquet("s3a://silver-zone/offres_clean")
        )

    def _read_dim(self, table_name: str, columns: list[str]) -> DataFrame:
        return self.spark.read.jdbc(
            url=self.db_url,
            table=table_name,
            properties=self.db_properties,
        ).select(*columns)

    def _append_if_not_empty(self, df: DataFrame, table_name: str) -> None:
        if df.limit(1).count() == 0:
            return
        df.write.jdbc(
            url=self.db_url,
            table=table_name,
            mode="append",
            properties=self.db_properties,
        )

    def _load_dim_source(self, df_silver: DataFrame) -> None:
        candidates = (
            df_silver.select(trim(col("source")).alias("nom_source"))
            .where(col("nom_source").isNotNull() & (col("nom_source") != ""))
            .distinct()
        )
        existing = self._read_dim("gold.dim_source", ["nom_source"])
        to_insert = candidates.join(existing, on="nom_source", how="left_anti")
        self._append_if_not_empty(to_insert, "gold.dim_source")

    def _load_dim_poste(self, df_silver: DataFrame) -> None:
        candidates = (
            df_silver.select(trim(col("titre")).alias("titre_poste"))
            .where(col("titre_poste").isNotNull() & (col("titre_poste") != ""))
            .distinct()
        )
        existing = self._read_dim("gold.dim_poste", ["titre_poste"])
        to_insert = candidates.join(existing, on="titre_poste", how="left_anti")
        self._append_if_not_empty(to_insert, "gold.dim_poste")

    def _load_dim_date(self, df_silver: DataFrame) -> None:
        safe_date_raw = when(
            col("date_publication").isNull() | (trim(col("date_publication")) == ""),
            None,
        ).otherwise(trim(col("date_publication")))

        extracted_dmy = regexp_extract(safe_date_raw, r"(\d{1,2}\s+[A-Za-z]{3}\s+\d{4})", 1)
        parsed_date = coalesce(
            to_date(extracted_dmy, "dd MMM yyyy"),
            to_date(safe_date_raw, "yyyy-MM-dd"),
            to_date(safe_date_raw),
        )

        candidates = (
            df_silver.withColumn("full_date", parsed_date)
            .where(col("full_date").isNotNull())
            .select(
                date_format(col("full_date"), "yyyyMMdd").cast("int").alias("date_id"),
                col("full_date"),
                year(col("full_date")).alias("annee"),
                month(col("full_date")).alias("mois"),
                quarter(col("full_date")).alias("trimestre"),
            )
            .distinct()
        )

        existing = self._read_dim("gold.dim_date", ["date_id"])
        to_insert = candidates.join(existing, on="date_id", how="left_anti")
        self._append_if_not_empty(to_insert, "gold.dim_date")

    def _load_dim_ville(self) -> None:
        candidates = self.spark.createDataFrame([("Non spécifié",)], ["nom_ville"])
        existing = self._read_dim("gold.dim_ville", ["nom_ville"])
        to_insert = candidates.join(existing, on="nom_ville", how="left_anti")
        self._append_if_not_empty(to_insert, "gold.dim_ville")

    def _load_fact_offres_emploi(self, df_silver_base: DataFrame) -> None:
        # Dimensions de référence (déjà alimentées) pour résoudre les clés étrangères.
        sources_pg = self._read_dim("gold.dim_source", ["source_id", "nom_source"])
        postes_pg = self._read_dim("gold.dim_poste", ["poste_id", "titre_poste"])
        villes_pg = self._read_dim("gold.dim_ville", ["ville_id", "nom_ville"])
        dates_pg = self._read_dim("gold.dim_date", ["date_id"])

        # Recalcul date_id côté faits puis validation de correspondance via jointure sur dim_date.
        safe_date_raw = when(
            col("date_publication").isNull() | (trim(col("date_publication")) == ""),
            None,
        ).otherwise(trim(col("date_publication")))

        extracted_dmy = regexp_extract(safe_date_raw, r"(\d{1,2}\s+[A-Za-z]{3}\s+\d{4})", 1)
        parsed_date = coalesce(
            to_date(extracted_dmy, "dd MMM yyyy"),
            to_date(safe_date_raw, "yyyy-MM-dd"),
            to_date(safe_date_raw),
        )
        fact_base = df_silver_base.withColumn(
            "date_id_silver",
            date_format(parsed_date, "yyyyMMdd").cast("int"),
        )

        # Métriques: nb_competences, salaire_min, salaire_max selon présence/type des colonnes Silver.
        silver_types = dict(df_silver_base.dtypes)
        has_array_competences = (
            "competences" in silver_types and silver_types["competences"].lower().startswith("array")
        )
        if has_array_competences:
            fact_base = fact_base.withColumn("nb_competences", size(col("competences")))
        else:
            fact_base = fact_base.withColumn("nb_competences", lit(0))

        if "salaire_min" in silver_types:
            fact_base = fact_base.withColumn("salaire_min_value", col("salaire_min").cast("float"))
        else:
            fact_base = fact_base.withColumn("salaire_min_value", lit(None).cast("float"))

        if "salaire_max" in silver_types:
            fact_base = fact_base.withColumn("salaire_max_value", col("salaire_max").cast("float"))
        else:
            fact_base = fact_base.withColumn("salaire_max_value", lit(None).cast("float"))

        # Localisation: clé ville via localisation_propre si disponible, sinon fallback "Non spécifié".
        if "localisation_propre" in silver_types:
            fact_base = fact_base.withColumn("ville_join_value", trim(col("localisation_propre")))
        else:
            fact_base = fact_base.withColumn("ville_join_value", lit("Non spécifié"))

        fact_joined = (
            fact_base.alias("f")
            .join(sources_pg.alias("s"), trim(col("f.source")) == col("s.nom_source"), "left")
            .join(postes_pg.alias("p"), trim(col("f.titre")) == col("p.titre_poste"), "left")
            .join(villes_pg.alias("v"), col("f.ville_join_value") == col("v.nom_ville"), "left")
            .join(dates_pg.alias("d"), col("f.date_id_silver") == col("d.date_id"), "left")
        )

        fact_df = fact_joined.select(
            col("d.date_id").cast("int").alias("date_id"),
            col("p.poste_id").cast("int").alias("poste_id"),
            col("v.ville_id").cast("int").alias("ville_id"),
            col("s.source_id").cast("int").alias("source_id"),
            col("f.salaire_min_value").cast("float").alias("salaire_min"),
            col("f.salaire_max_value").cast("float").alias("salaire_max"),
            col("f.nb_competences").cast("int").alias("nb_competences"),
        )

        # Écarte les lignes inutilisables où toutes les clés étrangères sont absentes.
        fact_df = fact_df.where(
            ~(
                col("date_id").isNull()
                & col("poste_id").isNull()
                & col("ville_id").isNull()
                & col("source_id").isNull()
            )
        )

        self._append_if_not_empty(fact_df, "gold.fait_offres_emploi")

    def run(self) -> None:
        # On ne retient que les colonnes utiles aux dimensions Gold
        # pour éviter les erreurs si certaines colonnes Silver n'existent pas.
        df_silver = self._read_silver().select(
            "date_publication",
            "source",
            "titre",
        )
        df_silver_base = self._read_silver()

        self._load_dim_source(df_silver)
        self._load_dim_poste(df_silver)
        self._load_dim_date(df_silver)
        self._load_dim_ville()
        self._load_fact_offres_emploi(df_silver_base)

    def stop(self) -> None:
        self.spark.stop()


def main() -> None:
    etl = SilverToGoldETL()
    try:
        etl.run()

        # --- BLOC DE VÉRIFICATION FINALE ---
        print("\n" + "=" * 50)
        print("🔎 VÉRIFICATION DU CONTENU DE POSTGRESQL")
        print("=" * 50)

        for table_name in ["gold.dim_source", "gold.dim_poste", "gold.dim_date"]:
            print(f"\n📊 Contenu de la table {table_name} :")
            etl.spark.read.jdbc(
                url=etl.db_url,
                table=table_name,
                properties=etl.db_properties,
            ).show(5, False)
        # -------------------------

    finally:
        etl.stop()


if __name__ == "__main__":
    main()
