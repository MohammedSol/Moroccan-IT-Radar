"""
Deep Scraper ReKrute (requests + BeautifulSoup4).

Pipeline:
1) Parcourir les pages de recherche IT
2) Extraire les liens d'offres
3) Visiter chaque offre (deep scraping) pour enrichir les données
4) Sauvegarder le JSON dans src/scraping/static_data/rekrute_enriched.json

Le module est importable pour une future intégration Kafka.
"""

from __future__ import annotations

import json
import logging
import random
import re
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Iterable, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup, Tag


LOGGER = logging.getLogger(__name__)


@dataclass
class RekruteOffer:
    """Schéma cible strict d'une offre ReKrute."""

    titre_poste: str
    entreprise: Optional[str]
    localisation: Optional[str]
    salaire_raw: Optional[str]
    competences: list[str] = field(default_factory=list)
    experience_requise: Optional[str] = None
    niveau_etudes: Optional[str] = None
    type_contrat: Optional[str] = None
    date_publication: Optional[str] = None
    source_url: str = ""
    langue_annonce: Optional[str] = None


@dataclass
class RekruteConfig:
    """Configuration du spider ReKrute."""

    base_url: str = "https://www.rekrute.com"
    search_path: str = "/offres.html?s=3&keyword=IT"
    timeout_seconds: int = 25
    max_pages: int = 3
    delay_min_seconds: float = 1.2
    delay_max_seconds: float = 2.6
    output_file: Path = (
        Path(__file__).resolve().parents[1] / "static_data" / "rekrute_enriched.json"
    )
    user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    )

    @property
    def start_url(self) -> str:
        return urljoin(self.base_url, self.search_path)


class RekruteSpider:
    """Spider deep scraping ReKrute."""

    def __init__(self, config: Optional[RekruteConfig] = None) -> None:
        self.config = config or RekruteConfig()
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": self.config.user_agent,
                "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Connection": "keep-alive",
            }
        )

    def get_page(self, url: str) -> Optional[BeautifulSoup]:
        """Récupère et parse une page HTML."""
        try:
            LOGGER.info("Connexion HTTP: %s", url)
            response = self.session.get(url, timeout=self.config.timeout_seconds)
            response.raise_for_status()
            return BeautifulSoup(response.text, "html.parser")
        except requests.RequestException as exc:
            LOGGER.error("Erreur réseau sur %s: %s", url, exc)
            return None
        except Exception as exc:
            LOGGER.error("Erreur inattendue de parsing sur %s: %s", url, exc)
            return None

    @staticmethod
    def _extract_text(container: Tag, selectors: Iterable[str]) -> Optional[str]:
        """Retourne le premier texte non vide trouvé via une liste de sélecteurs."""
        for selector in selectors:
            node = container.select_one(selector)
            if not node:
                continue
            text = node.get_text(" ", strip=True)
            if text:
                return re.sub(r"\s+", " ", text).strip()
        return None

    @staticmethod
    def _normalize_city(value: Optional[str]) -> Optional[str]:
        """Nettoie la localisation et garde une forme ville lisible."""
        if not value:
            return None
        cleaned = re.sub(r"\s+", " ", value).strip(" -")
        return cleaned.split(",")[0].strip() if "," in cleaned else cleaned

    @staticmethod
    def _detect_language(full_text: str) -> Optional[str]:
        """
        Détection simple FR/EN basée sur mots fréquents.
        Retourne 'FR', 'EN' ou None.
        """
        text = (full_text or "").lower()
        fr_markers = ("poste", "expérience", "compétence", "profil", "mission", "bac+")
        en_markers = ("position", "experience", "skills", "profile", "requirements")
        fr_score = sum(1 for token in fr_markers if token in text)
        en_score = sum(1 for token in en_markers if token in text)
        if fr_score == 0 and en_score == 0:
            return None
        return "FR" if fr_score >= en_score else "EN"

    def _extract_offer_links(self, soup: BeautifulSoup) -> list[str]:
        """Extrait les URLs des offres depuis une page de résultats."""
        selectors = [
            "a[href*='offre-emploi']",
            "h2 a",
            "h3 a",
            ".titreJob a",
            ".post-title a",
        ]
        urls: list[str] = []
        seen: set[str] = set()

        for selector in selectors:
            nodes = soup.select(selector)
            if not nodes:
                continue
            for node in nodes:
                href = (node.get("href") or "").strip()
                if not href:
                    continue
                full_url = urljoin(self.config.base_url, href)
                if "rekrute.com" not in urlparse(full_url).netloc:
                    continue
                if full_url not in seen:
                    seen.add(full_url)
                    urls.append(full_url)
            if urls:
                LOGGER.info("Sélecteur liens retenu: '%s' (%d liens)", selector, len(urls))
                break

        if not urls:
            LOGGER.warning("Aucun lien d'offre trouvé sur la page de recherche.")
        return urls

    def _get_next_search_page(self, soup: BeautifulSoup) -> Optional[str]:
        """Retourne l'URL de la page de résultats suivante."""
        selectors = ["a[rel='next']", "a.next", "li.next a", "a[aria-label='Next']"]
        for selector in selectors:
            node = soup.select_one(selector)
            href = (node.get("href") or "").strip() if node else ""
            if href:
                return urljoin(self.config.base_url, href)
        return None

    def parse_offer_detail(self, soup: BeautifulSoup, source_url: str) -> Optional[RekruteOffer]:
        """Parse la page détail d'une offre pour extraire les champs enrichis."""
        try:
            title = self._extract_text(
                soup,
                ["h1", ".post-title h1", ".detail-title", ".job-title"],
            )
            if not title:
                LOGGER.warning("Offre ignorée (titre absent): %s", source_url)
                return None

            entreprise = self._extract_text(
                soup,
                [".recruiter-name", ".company-name", ".societe", ".recruiter a", ".company"],
            )
            localisation = self._normalize_city(
                self._extract_text(
                    soup,
                    [".job-location", ".location", ".infos li", ".post-location"],
                )
            )
            salaire_raw = self._extract_text(
                soup,
                [".salary", ".salaire", "li:has(.salary)", ".infos li"],
            )
            experience_requise = self._extract_text(
                soup,
                [".experience", ".exp", ".job-experience", ".infos li"],
            )
            niveau_etudes = self._extract_text(
                soup,
                [".education", ".niveau-etudes", ".formation", ".infos li"],
            )
            type_contrat = self._extract_text(
                soup,
                [".contract", ".type-contrat", ".job-contract", ".infos li"],
            )
            date_publication = self._extract_text(
                soup,
                [".date", ".publication-date", ".job-date", "time"],
            )

            # Compétences: collecte depuis tags explicites + fallback description.
            skill_selectors = [".tags .tag", ".skills .skill", ".competences span", ".keyword"]
            competences: list[str] = []
            for selector in skill_selectors:
                tags = soup.select(selector)
                if not tags:
                    continue
                for tag in tags:
                    text = re.sub(r"\s+", " ", tag.get_text(" ", strip=True))
                    if text and text not in competences:
                        competences.append(text)
                if competences:
                    break

            # Fallback ultra-simple si les tags sont absents.
            if not competences:
                description = self._extract_text(
                    soup, [".job-description", ".description", ".offer-content", "article"]
                ) or ""
                tech_candidates = [
                    "Python",
                    "Java",
                    "Spark",
                    "Kafka",
                    "SQL",
                    "Airflow",
                    "Docker",
                    "Kubernetes",
                    "AWS",
                    "Azure",
                    "GCP",
                ]
                competences = [tech for tech in tech_candidates if tech.lower() in description.lower()]

            body_text = soup.get_text(" ", strip=True)
            langue_annonce = self._detect_language(body_text)

            return RekruteOffer(
                titre_poste=title,
                entreprise=entreprise,
                localisation=localisation,
                salaire_raw=salaire_raw,
                competences=competences,
                experience_requise=experience_requise,
                niveau_etudes=niveau_etudes,
                type_contrat=type_contrat,
                date_publication=date_publication,
                source_url=source_url,
                langue_annonce=langue_annonce,
            )
        except Exception as exc:
            LOGGER.error("Erreur parsing détail offre (%s): %s", source_url, exc)
            return None

    def scrape(self, max_pages: Optional[int] = None) -> list[RekruteOffer]:
        """
        Deep scraping:
        - collecte des liens depuis pages de recherche
        - visite détaillée de chaque annonce
        """
        page_limit = max_pages or self.config.max_pages
        all_links: list[str] = []
        seen_links: set[str] = set()

        current_url = self.config.start_url
        page_idx = 1

        while current_url and page_idx <= page_limit:
            LOGGER.info("Analyse page recherche %d/%d", page_idx, page_limit)
            search_soup = self.get_page(current_url)
            if search_soup is None:
                LOGGER.warning("Page recherche inaccessible: %s", current_url)
                break

            page_links = self._extract_offer_links(search_soup)
            for link in page_links:
                if link not in seen_links:
                    seen_links.add(link)
                    all_links.append(link)

            current_url = self._get_next_search_page(search_soup)
            page_idx += 1

        LOGGER.info("Total liens collectés: %d", len(all_links))

        offers: list[RekruteOffer] = []
        for index, detail_url in enumerate(all_links, start=1):
            LOGGER.info("Visite détail %d/%d: %s", index, len(all_links), detail_url)
            detail_soup = self.get_page(detail_url)
            if detail_soup is None:
                LOGGER.warning("Offre ignorée (page détail inaccessible): %s", detail_url)
                continue

            offer = self.parse_offer_detail(detail_soup, source_url=detail_url)
            if offer:
                offers.append(offer)

            # Délai éthique aléatoire entre pages détail.
            delay = random.uniform(
                self.config.delay_min_seconds,
                self.config.delay_max_seconds,
            )
            time.sleep(delay)

        LOGGER.info("Deep scraping terminé: %d offres enrichies.", len(offers))
        return offers

    def save_to_json(
        self,
        offers: list[RekruteOffer],
        output_path: Optional[Path] = None,
    ) -> Path:
        """Sauvegarde les offres enrichies au format JSON."""
        target_path = output_path or self.config.output_file
        try:
            target_path.parent.mkdir(parents=True, exist_ok=True)
            payload = [asdict(offer) for offer in offers]
            with target_path.open("w", encoding="utf-8") as file:
                json.dump(payload, file, ensure_ascii=False, indent=2)
            LOGGER.info("Sauvegarde JSON OK: %s (%d offres)", target_path, len(payload))
            return target_path
        except Exception as exc:
            LOGGER.error("Erreur sauvegarde JSON (%s): %s", target_path, exc)
            raise


def scrape_offers(max_pages: int = 3) -> list[dict]:
    """
    Point d'entrée importable pour un futur producer Kafka.
    Retourne directement une liste de dicts sérialisables.
    """
    spider = RekruteSpider(RekruteConfig(max_pages=max_pages))
    offers = spider.scrape(max_pages=max_pages)
    return [asdict(item) for item in offers]


def run_scraping(max_pages: int = 3) -> Path:
    """Exécute le scraping et écrit le fichier JSON de validation."""
    spider = RekruteSpider(RekruteConfig(max_pages=max_pages))
    offers = spider.scrape(max_pages=max_pages)
    return spider.save_to_json(offers)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    output = run_scraping(max_pages=3)
    print(f"Fichier généré: {output}")
