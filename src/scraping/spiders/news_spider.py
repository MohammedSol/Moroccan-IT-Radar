import requests
from bs4 import BeautifulSoup
import json
import boto3
import logging
import os
from datetime import datetime

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(name)s | %(message)s')
logger = logging.getLogger(__name__)

class NewsSpider:
    def __init__(self):
        # Session avec un User-Agent pour simuler un vrai navigateur
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        
        # Configuration MinIO
        self.s3_client = boto3.client(
            's3',
            endpoint_url='http://localhost:9002',
            aws_access_key_id='admin123',
            aws_secret_access_key='admin123'
        )
        self.bucket_name = 'bronze-zone'

    def _extract_text_with_fallbacks(self, soup, containers_to_try):
        """
        Mécanisme de résilience : teste plusieurs classes CSS pour trouver le texte.
        """
        for selector in containers_to_try:
            container = soup.select_one(selector)
            if container:
                paragraphs = container.find_all('p')
                # Concaténer tous les paragraphes non vides
                text = ' '.join([p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)])
                if text:
                    return text
                    
        # Fallback de dernier recours : extraire les premiers <p> de la page entière
        logger.warning("Fallbacks spécifiques échoués, tentative d'extraction globale.")
        paragraphs = soup.find_all('p')
        return ' '.join([p.get_text(strip=True) for p in paragraphs[:15] if p.get_text(strip=True)])

    def scrape_medias24(self, url):
        logger.info(f"Scraping Médias24: {url}")
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            # Fallbacks Titre
            title_tag = soup.select_one('h1.entry-title, h1.post-title, h1')
            titre = title_tag.get_text(strip=True) if title_tag else "Titre introuvable"

            # Fallbacks Date
            date_tag = soup.select_one('time.entry-date, meta[property="article:published_time"]')
            date_pub = date_tag.get('datetime') if date_tag and date_tag.has_attr('datetime') else (date_tag.get('content') if date_tag else datetime.now().isoformat())

            # Fallbacks Texte
            contenu = self._extract_text_with_fallbacks(soup, ['div.entry-content', 'div.post-content', 'article'])

            return {
                "titre": titre,
                "source": "Médias24",
                "date_publication": date_pub,
                "url": url,
                "contenu_texte": contenu
            }
        except Exception as e:
            logger.error(f"Erreur Médias24 sur {url}: {e}")
            return None

    def scrape_leconomiste(self, url):
        logger.info(f"Scraping L'Économiste: {url}")
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            # Fallbacks Titre
            title_tag = soup.select_one('h1.page-header, h1#page-title, h1.title, h1')
            titre = title_tag.get_text(strip=True) if title_tag else "Titre introuvable"

            # Fallbacks Date
            date_tag = soup.select_one('span.date-display-single')
            date_pub = date_tag.get_text(strip=True) if date_tag else datetime.now().isoformat()

            # Fallbacks Texte
            contenu = self._extract_text_with_fallbacks(soup, ['div.field-name-body', 'div.content', 'div.node-article'])

            return {
                "titre": titre,
                "source": "L'Économiste",
                "date_publication": date_pub,
                "url": url,
                "contenu_texte": contenu
            }
        except Exception as e:
            logger.error(f"Erreur L'Économiste sur {url}: {e}")
            return None

    def save_and_upload(self, data_list, local_filename="news_raw.json"):
        # Filtrer les échecs (None)
        valid_data = [d for d in data_list if d is not None and d['contenu_texte'] != ""]
        
        if not valid_data:
            logger.warning("Aucune donnée valide à sauvegarder.")
            return

        # Sauvegarde Locale
        local_path = os.path.join(os.path.dirname(__file__), '..', 'static_data', local_filename)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        with open(local_path, 'w', encoding='utf-8') as f:
            json.dump(valid_data, f, ensure_ascii=False, indent=2)
        logger.info(f"JSON local sauvegardé: {local_path}")

        # Upload MinIO
        s3_key = f"raw/articles/historical_{local_filename}"
        try:
            self.s3_client.upload_file(local_path, self.bucket_name, s3_key)
            logger.info(f"Upload MinIO OK: s3://{self.bucket_name}/{s3_key}")
        except Exception as e:
            logger.error(f"Erreur d'upload MinIO: {e}")

if __name__ == "__main__":
    spider = NewsSpider()
    results = []
    
    # URLs de test (liens frais)
    url_medias24 = "https://medias24.com/2025/02/03/les-chiffres-du-chomage-et-de-lemploi-en-2024/"
    url_leco = "https://www.leconomiste.com/article/1119561-maroc-digital-2030-le-gouvernement-accelere-la-cadence" # Remplace par un vrai lien si besoin
    
    # Exécution des méthodes
    res_m24 = spider.scrape_medias24(url_medias24)
    res_eco = spider.scrape_leconomiste(url_leco)
    
    if res_m24: results.append(res_m24)
    if res_eco: results.append(res_eco)
    
    # Sauvegarde
    spider.save_and_upload(results)