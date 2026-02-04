#!/usr/bin/env python3
import argparse
import csv
import logging
import os
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional
from urllib.request import urlretrieve

import mysql.connector

DEFAULT_DNE_URL = "https://www2.correios.com.br/sistemas/edne/download/DNE_GU.zip"


@dataclass
class DneRecord:
    cep: str
    street: str
    city: str
    region: str
    neighborhood: str


def normalize_spaces(value: str) -> str:
    return " ".join(value.split())


def parse_logradouro_line(line: str) -> Optional[DneRecord]:
    if not line or line[0] != "D":
        return None

    region = line[1:3].strip()
    city = normalize_spaces(line[17:89])
    neighborhood_initial = normalize_spaces(line[102:174])
    neighborhood_final = normalize_spaces(line[187:259])
    tipo_logradouro = normalize_spaces(line[259:285])
    preposicao = normalize_spaces(line[285:288])
    titulo = normalize_spaces(line[288:360])
    nome_logradouro = normalize_spaces(line[374:446])
    cep = line[518:526].strip()

    if not cep:
        return None

    neighborhood = neighborhood_initial or neighborhood_final
    street_parts = [tipo_logradouro, preposicao, titulo, nome_logradouro]
    street = normalize_spaces(" ".join(part for part in street_parts if part))

    return DneRecord(
        cep=cep,
        street=street,
        city=city,
        region=region,
        neighborhood=neighborhood,
    )


def iter_logradouro_records(txt_path: Path) -> Iterator[DneRecord]:
    with txt_path.open("r", encoding="latin1", errors="ignore") as handle:
        for line in handle:
            record = parse_logradouro_line(line.rstrip("\n"))
            if record:
                yield record


def download_and_extract(url: str, temp_dir: Path) -> list[Path]:
    zip_path = temp_dir / "dne.zip"
    urlretrieve(url, zip_path)

    with zipfile.ZipFile(zip_path) as main_zip:
        nested_name = next(
            name for name in main_zip.namelist() if name.startswith("DNE_GU_") and name.endswith(".zip")
        )
        nested_path = temp_dir / nested_name
        nested_path.write_bytes(main_zip.read(nested_name))

    logradouro_files: list[Path] = []
    with zipfile.ZipFile(nested_path) as nested_zip:
        for name in nested_zip.namelist():
            if name.endswith("_LOGRADOUROS.TXT"):
                extracted = temp_dir / Path(name).name
                extracted.write_bytes(nested_zip.read(name))
                logradouro_files.append(extracted)

    return logradouro_files


def mysql_connect(args: argparse.Namespace):
    return mysql.connector.connect(
        host=args.db_host,
        user=args.db_user,
        password=args.db_password,
        database=args.db_name,
        port=args.db_port,
        autocommit=False,
        allow_local_infile=True,
    )


def sync_database(csv_path: Path, args: argparse.Namespace, logger: logging.Logger) -> None:
    stage_table = f"{args.table}_stage"

    with mysql_connect(args) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {stage_table} LIKE {args.table}
                """
            )
            cursor.execute(f"TRUNCATE TABLE {stage_table}")
            cursor.execute(
                f"""
                LOAD DATA LOCAL INFILE %s
                INTO TABLE {stage_table}
                FIELDS TERMINATED BY '\t'
                LINES TERMINATED BY '\n'
                (cep, street, city, region, neighborhood)
                """,
                (str(csv_path),),
            )
            logger.info("Carga na tabela de estágio concluída.")
            cursor.execute(
                f"""
                INSERT INTO {args.table} (cep, street, city, region, neighborhood)
                SELECT stage.cep, stage.street, stage.city, stage.region, stage.neighborhood
                FROM {stage_table} stage
                LEFT JOIN {args.table} target ON target.cep = stage.cep
                WHERE target.cep IS NULL
                """
            )
            inserted = cursor.rowcount
            logger.info("Novos CEPs inseridos: %s", inserted)
            cursor.execute(
                f"""
                DELETE target
                FROM {args.table} target
                LEFT JOIN {stage_table} stage ON stage.cep = target.cep
                WHERE stage.cep IS NULL
                """
            )
            deleted = cursor.rowcount
            logger.info("CEPs removidos: %s", deleted)
        conn.commit()
        logger.info("Sincronização concluída com sucesso.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Baixa o DNE dos Correios, gera CSV e sincroniza com o MySQL."
    )
    parser.add_argument("--dne-url", default=os.getenv("DNE_ZIP_URL", DEFAULT_DNE_URL))
    parser.add_argument("--db-host", default=os.getenv("DNE_DB_HOST", "localhost"))
    parser.add_argument("--db-user", default=os.getenv("DNE_DB_USER", "root"))
    parser.add_argument("--db-password", default=os.getenv("DNE_DB_PASSWORD", ""))
    parser.add_argument("--db-name", default=os.getenv("DNE_DB_NAME", ""))
    parser.add_argument("--db-port", type=int, default=int(os.getenv("DNE_DB_PORT", "3306")))
    parser.add_argument("--table", default=os.getenv("DNE_TABLE", "postcode_correios"))
    parser.add_argument("--keep-temp", action="store_true", help="Mantém arquivos temporários.")
    return parser


def run_sync(args: argparse.Namespace, logger: logging.Logger) -> None:
    with tempfile.TemporaryDirectory() as tmp_dir:
        temp_path = Path(tmp_dir)
        logger.info("Baixando DNE de %s", args.dne_url)
        logradouro_files = download_and_extract(args.dne_url, temp_path)

        csv_path = temp_path / "dne_logradouros.tsv"
        record_count = 0
        with csv_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle, delimiter="\t")
            for logradouro_file in logradouro_files:
                for record in iter_logradouro_records(logradouro_file):
                    writer.writerow(
                        [
                            record.cep,
                            record.street,
                            record.city,
                            record.region,
                            record.neighborhood,
                        ]
                    )
                    record_count += 1

        logger.info("Registros processados: %s", record_count)
        sync_database(csv_path, args, logger)

        if args.keep_temp:
            keep_path = Path.cwd() / "dne_tmp"
            keep_path.mkdir(exist_ok=True)
            for item in temp_path.iterdir():
                item.replace(keep_path / item.name)
            logger.info("Arquivos temporários mantidos em: %s", keep_path)


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logger = logging.getLogger("dne_sync")
    run_sync(args, logger)


if __name__ == "__main__":
    main()
