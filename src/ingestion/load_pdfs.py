from __future__ import annotations

import json
import logging
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import boto3
import pdfplumber
from botocore.exceptions import ClientError
from pdfminer.pdfparser import PDFSyntaxError


@dataclass(frozen=True)
class MinIOConfig:
    endpoint_url: str
    aws_access_key_id: str
    aws_secret_access_key: str
    region_name: str


class PDFIngestionPipeline:
    def __init__(
        self,
        config: MinIOConfig,
        bucket_name: str,
        output_directory: Path,
        raw_prefix: str = "raw/pdfs",
        extracted_prefix: str = "extracted",
    ) -> None:
        self.bucket_name = bucket_name
        self.raw_prefix = raw_prefix.strip("/")
        self.extracted_prefix = extracted_prefix.strip("/")
        self.output_directory = output_directory
        self.output_directory.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=config.endpoint_url,
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
            region_name=config.region_name,
        )

    def _upload_file(self, local_path: Path, object_key: str) -> None:
        try:
            self.s3_client.upload_file(str(local_path), self.bucket_name, object_key)
        except ClientError as exc:
            self.logger.error("Echec upload MinIO (%s): %s", object_key, exc)
            raise

    @staticmethod
    def _normalize_text(text: str) -> str:
        normalized = unicodedata.normalize("NFKC", text)
        replacements = {
            "\u2019": "'",  # apostrophe typographique droite
            "\u2018": "'",  # apostrophe typographique gauche
            "\u201c": '"',
            "\u201d": '"',
            "\u00a0": " ",  # espace insécable
            "\u200b": "",   # zero-width space
        }
        for source, target in replacements.items():
            normalized = normalized.replace(source, target)
        # Suppression des caractères de contrôle non imprimables hors sauts de ligne/tabulations.
        normalized = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", normalized)
        return normalized

    @staticmethod
    def _clean_table(table: list[list[Any]]) -> list[list[str]]:
        cleaned_table: list[list[str]] = []
        for row in table:
            cleaned_row = []
            for cell in row:
                if cell is None:
                    cleaned_row.append("")
                    continue
                cell_str = PDFIngestionPipeline._normalize_text(str(cell)).strip()
                if cell_str.lower() == "null":
                    cleaned_row.append("")
                else:
                    cleaned_row.append(cell_str)
            if any(value != "" for value in cleaned_row):
                cleaned_table.append(cleaned_row)
        return cleaned_table

    @staticmethod
    def _is_valid_table(table: list[list[str]], min_digit_ratio: float = 0.08) -> bool:
        if not table:
            return False

        # Règle 1 : au moins 2 colonnes non vides sur le tableau.
        max_non_empty_columns = max((sum(1 for cell in row if cell.strip()) for row in table), default=0)
        if max_non_empty_columns < 2:
            return False

        # Règle 2 : proportion minimale de chiffres pour écarter les faux positifs narratifs.
        text_blob = " ".join(cell for row in table for cell in row if cell.strip())
        if not text_blob:
            return False

        digit_count = len(re.findall(r"\d", text_blob))
        alpha_num_count = len(re.findall(r"[A-Za-zÀ-ÖØ-öø-ÿ0-9]", text_blob))
        if alpha_num_count == 0:
            return False

        digit_ratio = digit_count / alpha_num_count
        return digit_ratio >= min_digit_ratio

    def extract_data_from_pdf(self, pdf_path: Path) -> dict[str, Any]:
        text_parts: list[str] = []
        tables: list[list[list[str]]] = []

        self.logger.info("Extraction en cours : %s", pdf_path.name)
        try:
            with pdfplumber.open(str(pdf_path)) as pdf:
                # Lecture séquentielle page par page pour conserver la traçabilité.
                for page_number, page in enumerate(pdf.pages, start=1):
                    page_text = self._normalize_text(page.extract_text() or "")
                    if page_text.strip():
                        text_parts.append(page_text)

                    raw_tables = page.extract_tables() or []
                    for table in raw_tables:
                        if table:
                            cleaned_table = self._clean_table(table)
                            if cleaned_table and self._is_valid_table(cleaned_table):
                                tables.append(cleaned_table)
                            elif cleaned_table:
                                self.logger.debug(
                                    "Tableau rejeté (heuristique) page=%s, lignes=%s",
                                    page_number,
                                    len(cleaned_table),
                                )

                    self.logger.debug("Page %s traitée", page_number)
        except FileNotFoundError as exc:
            self.logger.error("Fichier PDF introuvable : %s", pdf_path)
            raise OSError(f"Fichier PDF introuvable : {pdf_path}") from exc
        except PDFSyntaxError as exc:
            self.logger.error("PDF invalide ou corrompu : %s", pdf_path)
            raise ValueError(f"PDF invalide : {pdf_path}") from exc
        except OSError as exc:
            self.logger.error("Erreur de lecture du PDF %s: %s", pdf_path, exc)
            raise

        return {
            "file_name": pdf_path.name,
            "raw_text": self._normalize_text("\n".join(text_parts)).strip(),
            "tables": tables,
        }

    def _extract_pdf_content(self, pdf_path: Path) -> dict[str, Any]:
        # Alias conservé pour compatibilité interne.
        return self.extract_data_from_pdf(pdf_path)

    def _save_json_locally(self, data: dict[str, Any], output_path: Path) -> None:
        try:
            with output_path.open("w", encoding="utf-8") as json_file:
                json.dump(data, json_file, ensure_ascii=False, indent=2)
        except OSError as exc:
            self.logger.error("Impossible d'ecrire le JSON local %s: %s", output_path, exc)
            raise

    def process_pdf(self, pdf_path: Path) -> None:
        if not pdf_path.exists():
            raise OSError(f"Fichier PDF introuvable : {pdf_path}")

        self.logger.info("Traitement du PDF : %s", pdf_path.name)

        # 1) Ingestion immuable du fichier source dans la zone RAW.
        raw_object_key = f"{self.raw_prefix}/{pdf_path.name}"
        self._upload_file(local_path=pdf_path, object_key=raw_object_key)
        self.logger.info("Upload RAW OK : s3://%s/%s", self.bucket_name, raw_object_key)

        # 2) Extraction structurée du contenu texte + tableaux.
        extracted_data = self.extract_data_from_pdf(pdf_path=pdf_path)
        output_json_name = f"{pdf_path.stem}_extracted.json"
        local_json_path = self.output_directory / output_json_name

        self._save_json_locally(data=extracted_data, output_path=local_json_path)
        self.logger.info("JSON local sauvegarde : %s", local_json_path)

        # 3) Chargement de la représentation JSON dans la couche BRONZE.
        extracted_object_key = f"{self.extracted_prefix}/{output_json_name}"
        self._upload_file(local_path=local_json_path, object_key=extracted_object_key)
        self.logger.info("Upload JSON OK : s3://%s/%s", self.bucket_name, extracted_object_key)

    def run(self, pdf_paths: list[Path]) -> None:
        for pdf_path in pdf_paths:
            try:
                self.process_pdf(pdf_path)
            except (ClientError, OSError, ValueError) as exc:
                self.logger.exception("Echec d'ingestion pour %s: %s", pdf_path.name, exc)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    src_directory = Path(__file__).resolve().parents[1]
    static_data_directory = src_directory / "scraping" / "static_data"
    pdf_directory = static_data_directory / "pdfs"

    minio_config = MinIOConfig(
        endpoint_url="http://localhost:9002",
        aws_access_key_id="admin123",
        aws_secret_access_key="admin123",
        region_name="us-east-1",
    )

    pipeline = PDFIngestionPipeline(
        config=minio_config,
        bucket_name="bronze-zone",
        output_directory=static_data_directory,
        raw_prefix="raw/pdfs",
        extracted_prefix="extracted",
    )

    pdf_files = [
        pdf_directory / "hcp_activite_chomage_2024.pdf",
        pdf_directory / "rapport_chercheur_impasse_IT.pdf",
    ]
    pipeline.run(pdf_files)


if __name__ == "__main__":
    main()
