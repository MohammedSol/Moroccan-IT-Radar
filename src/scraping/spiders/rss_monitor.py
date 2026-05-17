"""
Ingesteur batch RSS pour L'Économiste et TIC Maroc.

Ce script est conçu pour une orchestration Apache Airflow (exécution horaire).
L'approche RSS garantit la fraîcheur des données en récupérant des liens récents,
ce qui réduit fortement le risque d'URL statiques expirées (erreurs 404).
"""

from __future__ import annotations

import json
import logging
import re
import sys
import unicodedata
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional

import boto3
import feedparser
import requests
from bs4 import BeautifulSoup, Tag
from botocore.exceptions import ClientError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from src.ingestion.kafka_producer import MarketDataProducer
except ModuleNotFoundError:
    # Permet l'import "src.ingestion" quand le script est lancé directement via son chemin fichier.
    PROJECT_ROOT = Path(__file__).resolve().parents[3]
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.append(str(PROJECT_ROOT))
    from src.ingestion.kafka_producer import MarketDataProducer


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class MinIOConfig:
    endpoint_url: str = "http://localhost:9002"
    aws_access_key_id: str = "admin123"
    aws_secret_access_key: str = "admin123"
    region_name: str = "us-east-1"
    bucket_name: str = "bronze-zone"
    object_key: str = "raw/articles/news_rss_raw.json"


@dataclass(frozen=True)
class RSSMonitorConfig:
    timeout_seconds: int = 25
    output_file: Path = Path(__file__).resolve().parents[1] / "static_data" / "news_rss_raw.json"
    rss_sources: dict[str, str] = None  # type: ignore[assignment]
    keywords: list[str] = None  # type: ignore[assignment]
    user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    )

    def __post_init__(self) -> None:
        # object.__setattr__ est requis car la dataclass est immuable (frozen=True).
        if self.rss_sources is None:
            object.__setattr__(
                self,
                "rss_sources",
                {
                    "L'Économiste": "https://www.leconomiste.com/rss",
                    "TIC Maroc": "https://www.tic-maroc.com/feeds/posts/default?alt=rss",
                },
            )
        if self.keywords is None:
            object.__setattr__(
                self,
                "keywords",
                [
                    "emploi",
                    "chômage",
                    "informatique",
                    "it",
                    "tech",
                    "technologie",
                    "ingénieur",
                    "développeur",
                    "ia",
                    "data",
                ],
            )


@dataclass
class RSSNewsArticle:
    titre: str
    source: str
    date_publication: Optional[str]
    url: str
    contenu_texte: str


class RSSNewsMonitor:
    def __init__(
        self,
        config: Optional[RSSMonitorConfig] = None,
        minio_config: Optional[MinIOConfig] = None,
    ) -> None:
        self.config = config or RSSMonitorConfig()
        self.minio_config = minio_config or MinIOConfig()
        self.session = self._build_session()
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=self.minio_config.endpoint_url,
            aws_access_key_id=self.minio_config.aws_access_key_id,
            aws_secret_access_key=self.minio_config.aws_secret_access_key,
            region_name=self.minio_config.region_name,
        )

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update(
            {
                "User-Agent": self.config.user_agent,
                "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Connection": "keep-alive",
            }
        )
        retry = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    @staticmethod
    def _clean_text(text: str) -> str:
        return re.sub(r"\s+", " ", text).strip()

    @staticmethod
    def _normalize_for_matching(text: str) -> str:
        # Normalisation Unicode + suppression des accents pour matcher "chômage" et "chomage".
        normalized = unicodedata.normalize("NFKD", text.lower())
        return "".join(char for char in normalized if not unicodedata.combining(char))

    def _is_relevant_title(self, title: str) -> bool:
        normalized_title = self._normalize_for_matching(title)
        normalized_keywords = [self._normalize_for_matching(keyword) for keyword in self.config.keywords]
        return any(keyword in normalized_title for keyword in normalized_keywords)

    def _parse_feed(self, source_name: str, feed_url: str) -> list[dict[str, str]]:
        LOGGER.info("Lecture flux RSS: %s (%s)", source_name, feed_url)
        parsed_feed = feedparser.parse(feed_url)

        entries: list[dict[str, str]] = []
        for entry in parsed_feed.entries:
            title = self._clean_text(getattr(entry, "title", ""))
            link = self._clean_text(getattr(entry, "link", ""))
            published = self._clean_text(
                getattr(entry, "published", getattr(entry, "updated", ""))
            )

            if not title or not link:
                continue
            if not self._is_relevant_title(title):
                continue

            entries.append(
                {
                    "titre": title,
                    "source": source_name,
                    "date_publication": published,
                    "url": link,
                }
            )

        LOGGER.info("Articles RSS retenus (%s): %d", source_name, len(entries))
        return entries

    def _fetch_article_text(self, article_url: str) -> str:
        try:
            response = self.session.get(article_url, timeout=self.config.timeout_seconds)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
        except requests.RequestException as exc:
            LOGGER.warning("Lecture article échouée (%s): %s", article_url, exc)
            return ""

        # Fallback robuste demandé: on essaie plusieurs conteneurs potentiels du corps d'article.
        selectors = ["div.article-content", "div.entry-content", "div.content", "article"]
        for selector in selectors:
            try:
                container = soup.select_one(selector)
                if not container:
                    continue
                # L'extraction par <p> est stable pour isoler le texte narratif de l'article.
                paragraphs: list[Tag] = container.select("p")
                if not paragraphs:
                    continue
                text = " ".join(
                    self._clean_text(paragraph.get_text(strip=True)) for paragraph in paragraphs
                ).strip()
                if text:
                    return text
            except Exception as exc:
                LOGGER.debug("Parsing contenu selector=%s url=%s: %s", selector, article_url, exc)

        LOGGER.warning("Aucun contenu texte trouvé pour: %s", article_url)
        return ""

    def collect_articles(self, producer: Optional[MarketDataProducer] = None) -> list[RSSNewsArticle]:
        articles: list[RSSNewsArticle] = []
        for source_name, feed_url in self.config.rss_sources.items():
            candidates = self._parse_feed(source_name=source_name, feed_url=feed_url)
            for candidate in candidates:
                contenu_texte = self._fetch_article_text(candidate["url"])
                titre = candidate["titre"]
                if not titre or not contenu_texte:
                    LOGGER.warning(
                        "Article ignoré (titre ou contenu vide): %s",
                        candidate["url"],
                    )
                    continue

                article = RSSNewsArticle(
                    titre=titre,
                    source=candidate["source"],
                    date_publication=candidate["date_publication"] or None,
                    url=candidate["url"],
                    contenu_texte=contenu_texte,
                )
                articles.append(article)

                if producer is not None:
                    try:
                        producer.send_event("it_market_events", asdict(article))
                    except Exception as exc:
                        LOGGER.warning("Échec d'envoi Kafka, mais on continue le Batch: %s", exc)
        LOGGER.info("Total articles collectés depuis RSS: %d", len(articles))
        return articles

    def save_json_local(self, articles: list[RSSNewsArticle]) -> Path:
        output_path = self.config.output_file
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8") as file:
            json.dump([asdict(article) for article in articles], file, ensure_ascii=False, indent=2)
        LOGGER.info("Sauvegarde locale OK: %s", output_path)
        return output_path

    def upload_to_minio(self, local_file_path: Path) -> None:
        try:
            # Dépôt en Bronze: matière première pour les futures transformations Silver/Gold.
            self.s3_client.upload_file(
                str(local_file_path),
                self.minio_config.bucket_name,
                self.minio_config.object_key,
            )
            LOGGER.info(
                "Upload MinIO OK: s3://%s/%s",
                self.minio_config.bucket_name,
                self.minio_config.object_key,
            )
        except ClientError as exc:
            LOGGER.error("Upload MinIO échoué: %s", exc)
            raise

    def run(self, producer: Optional[MarketDataProducer] = None) -> Path:
        # Méthode appelée par un scheduler (ex: Airflow DAG horaire) pour ingestion batch automatique.
        articles = self.collect_articles(producer=producer)
        local_file = self.save_json_local(articles)
        self.upload_to_minio(local_file)
        return local_file


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    monitor = RSSNewsMonitor()
    producer = MarketDataProducer()
    try:
        output = monitor.run(producer=producer)
        print(f"Fichier généré: {output}")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
