from __future__ import annotations

import json
import logging
import os
import random
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import boto3
import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from src.ingestion.kafka_producer import MarketDataProducer
except ModuleNotFoundError:
    # Permet l'import src.ingestion quand ce script est lancé directement.
    PROJECT_ROOT = Path(__file__).resolve().parents[3]
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.append(str(PROJECT_ROOT))
    from src.ingestion.kafka_producer import MarketDataProducer


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class LinkedInSpiderConfig:
    # Endpoint public paginé de LinkedIn Jobs (mode guest).
    endpoint_url: str = "https://ma.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
    # Requête métier demandée.
    keywords: str = "Data Engineer OR Artificial Intelligence"
    location: str = "Morocco"
    # Pagination LinkedIn: offset par bloc de 25.
    page_size: int = 25
    # Garde-fou pour éviter une boucle sans fin si la source change.
    max_pages: int = 20
    timeout_seconds: int = 25
    topic_name: str = "it_market_events"
    # Headers complets pour réduire le risque de blocage.
    user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    )


class LinkedInJobsSpider:
    def __init__(self, config: Optional[LinkedInSpiderConfig] = None) -> None:
        self.config = config or LinkedInSpiderConfig()
        self.session = self._build_session()
        # --- Batch Layer (Lambda): client S3/MinIO pour persister un snapshot brut en Bronze.
        self.s3_client = boto3.client(
            "s3",
            endpoint_url="http://localhost:9002",
            aws_access_key_id="admin123",
            aws_secret_access_key="admin123",
        )
        # --- Bucket Bronze partagé avec les autres pipelines d'ingestion batch.
        self.bucket_name = "bronze-zone"

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update(
            {
                "User-Agent": self.config.user_agent,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Referer": "https://ma.linkedin.com/jobs/",
            }
        )
        # Retry réseau pour tolérer erreurs transitoires (429/5xx).
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
        return " ".join(text.split()).strip()

    def _fetch_jobs_page(self, start: int) -> Optional[BeautifulSoup]:
        params = {
            "keywords": self.config.keywords,
            "location": self.config.location,
            "start": start,
        }
        try:
            response = self.session.get(
                self.config.endpoint_url,
                params=params,
                timeout=self.config.timeout_seconds,
            )
            response.raise_for_status()
            return BeautifulSoup(response.text, "html.parser")
        except requests.RequestException as exc:
            LOGGER.warning("Erreur HTTP LinkedIn start=%s: %s", start, exc)
            return None

    def _parse_job_card(self, card: Tag) -> Optional[dict[str, str]]:
        try:
            # Titre selon structure publique LinkedIn.
            title_node = card.select_one("h3.base-search-card__title")
            titre = self._clean_text(title_node.get_text(" ", strip=True)) if title_node else ""
        except Exception:
            titre = ""

        try:
            # Entreprise selon structure publique LinkedIn.
            company_node = card.select_one("h4.base-search-card__subtitle")
            entreprise = self._clean_text(company_node.get_text(" ", strip=True)) if company_node else ""
        except Exception:
            entreprise = ""

        try:
            # Localisation selon structure publique LinkedIn.
            location_node = card.select_one("span.job-search-card__location")
            localisation = self._clean_text(location_node.get_text(" ", strip=True)) if location_node else ""
        except Exception:
            localisation = ""

        try:
            # URL de l'offre depuis le lien principal de la carte.
            link_node = card.select_one("a.base-card__full-link")
            raw_url = (link_node.get("href") or "").strip() if link_node else ""
            url = urljoin("https://ma.linkedin.com", raw_url) if raw_url else ""
        except Exception:
            url = ""

        try:
            # Date de publication (attribut datetime) si disponible.
            time_node = card.select_one("time")
            date_publication = (time_node.get("datetime") or "").strip() if time_node else ""
        except Exception:
            date_publication = ""

        # On garde seulement les offres minimales valides pour Kafka.
        if not titre or not entreprise or not url:
            return None

        return {
            "titre": titre,
            "entreprise": entreprise,
            "localisation": localisation,
            "date_publication": date_publication,
            "url": url,
            "source": "LinkedIn Jobs",
        }

    def save_and_upload_to_minio(self, jobs_data: list[dict[str, str]]) -> None:
        # --- Batch Layer (Lambda): sérialise l'ensemble des offres de la session de scraping.
        static_data_dir = Path(__file__).resolve().parents[1] / "static_data"
        # --- os.makedirs garantit la création du dossier cible même sur un environnement vierge.
        os.makedirs(static_data_dir, exist_ok=True)
        # --- Fichier local brut servant de trace d'ingestion (avant transformations Silver/Gold).
        local_file_path = static_data_dir / "linkedin_raw.json"

        # --- Écriture locale UTF-8 pour conserver les accents et caractères internationaux.
        with local_file_path.open("w", encoding="utf-8") as file:
            json.dump(jobs_data, file, ensure_ascii=False, indent=2)
        LOGGER.info("Snapshot Batch local sauvegardé: %s", local_file_path)

        # --- Upload final vers la Zone Bronze du Data Lake (Batch Layer persistée).
        self.s3_client.upload_file(
            str(local_file_path),
            self.bucket_name,
            "raw/offres/linkedin_raw.json",
        )
        LOGGER.info(
            "Snapshot Batch uploadé vers MinIO: s3://%s/%s",
            self.bucket_name,
            "raw/offres/linkedin_raw.json",
        )

    def run(self) -> None:
        producer = MarketDataProducer()
        seen_urls: set[str] = set()
        # --- Accumulateur Batch Layer: agrège toutes les offres valides envoyées au fil de l'eau.
        all_jobs: list[dict[str, str]] = []
        try:
            for page_index in range(self.config.max_pages):
                start = page_index * self.config.page_size
                LOGGER.info("Scraping LinkedIn start=%s", start)

                soup = self._fetch_jobs_page(start=start)
                if soup is None:
                    continue

                cards = soup.select("li")
                if not cards:
                    LOGGER.info("Aucune offre trouvée, arrêt pagination.")
                    break

                sent_count = 0
                for card in cards:
                    job_event = self._parse_job_card(card)
                    if not job_event:
                        continue
                    if job_event["url"] in seen_urls:
                        continue

                    seen_urls.add(job_event["url"])
                    # --- Alias explicite demandé pour matérialiser l'objet métier à archiver en batch.
                    offre_dict = job_event
                    try:
                        # Speed Layer: envoi immédiat de chaque offre vers Kafka.
                        producer.send_event(self.config.topic_name, offre_dict)
                        # --- Batch Layer: conserve la même offre dans le snapshot global final.
                        all_jobs.append(offre_dict)
                        sent_count += 1
                    except Exception as exc:
                        # Tolérance aux pannes: le spider continue même si Kafka est indisponible.
                        LOGGER.warning("Échec d'envoi Kafka, on continue: %s", exc)

                LOGGER.info("Page start=%s traitée, événements envoyés=%s", start, sent_count)
                # Pause aléatoire anti-ban pour simuler un rythme humain.
                time.sleep(random.uniform(2, 5))
        finally:
            # --- Déclenchement Batch final: écrit et pousse le snapshot complet vers MinIO Bronze.
            self.save_and_upload_to_minio(all_jobs)
            producer.close()
            LOGGER.info("Producer Kafka fermé proprement.")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    LinkedInJobsSpider().run()
