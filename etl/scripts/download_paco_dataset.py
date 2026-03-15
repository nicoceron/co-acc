#!/usr/bin/env python3
"""Download PACO public risk datasets and normalize archive exports to CSV files."""

from __future__ import annotations

import logging
import shutil
import sys
from pathlib import Path

import click

sys.path.insert(0, str(Path(__file__).parent))
from _download_utils import download_file, extract_zip

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PACO_DATASETS: dict[str, dict[str, str | bool]] = {
    "antecedentes_siri_sanciones": {
        "url": (
            "https://paco7public7info7prod.blob.core.windows.net/"
            "paco-pulic-info/antecedentes_SIRI_sanciones_Cleaned.zip"
        ),
        "archive_name": "antecedentes_siri_sanciones.zip",
        "output_name": "antecedentes_siri_sanciones.csv",
        "zipped": True,
    },
    "colusiones_en_contratacion": {
        "url": (
            "https://paco7public7info7prod.blob.core.windows.net/"
            "paco-pulic-info/colusiones_en_contratacion_SIC.csv"
        ),
        "output_name": "colusiones_en_contratacion.csv",
        "zipped": False,
    },
    "multas_secop": {
        "url": (
            "https://paco7public7info7prod.blob.core.windows.net/"
            "paco-pulic-info/multas_SECOP_Cleaned.zip"
        ),
        "archive_name": "multas_secop.zip",
        "output_name": "multas_secop.csv",
        "zipped": True,
    },
    "responsabilidades_fiscales": {
        "url": (
            "https://paco7public7info7prod.blob.core.windows.net/"
            "paco-pulic-info/responsabilidades_fiscales.csv"
        ),
        "output_name": "responsabilidades_fiscales.csv",
        "zipped": False,
    },
}


def _first_extracted_file(paths: list[Path]) -> Path:
    for path in paths:
        if path.is_file():
            return path
    raise click.ClickException("Archive extracted successfully but contained no files")


def _download_entry(
    *,
    name: str,
    config: dict[str, str | bool],
    output_dir: Path,
    timeout: int,
    force: bool,
) -> None:
    output_name = str(config["output_name"])
    target_path = output_dir / output_name
    if target_path.exists() and not force:
        logger.info("Skipping %s; already exists at %s", name, target_path)
        return

    url = str(config["url"])
    if bool(config.get("zipped")):
        archive_name = str(config["archive_name"])
        archive_path = output_dir / archive_name
        if not archive_path.exists() or force:
            if not download_file(url, archive_path, timeout=timeout):
                raise click.ClickException(f"Failed to download {name} from {url}")
        extract_dir = output_dir / f"_{name}_extract"
        if extract_dir.exists() and force:
            shutil.rmtree(extract_dir)
        extract_dir.mkdir(parents=True, exist_ok=True)
        extracted = extract_zip(archive_path, extract_dir)
        first_file = _first_extracted_file(extracted)
        shutil.copyfile(first_file, target_path)
        logger.info("Wrote %s to %s", name, target_path)
        return

    if not download_file(url, target_path, timeout=timeout):
        raise click.ClickException(f"Failed to download {name} from {url}")
    logger.info("Wrote %s to %s", name, target_path)


@click.command()
@click.option(
    "--dataset",
    "datasets",
    multiple=True,
    type=click.Choice(sorted(PACO_DATASETS)),
    help="Dataset key to download. Defaults to all supported PACO sanction feeds.",
)
@click.option(
    "--output-dir",
    default="../data/paco_sanctions",
    show_default=True,
    help="Destination directory for normalized PACO files.",
)
@click.option("--timeout", default=600, show_default=True, type=int)
@click.option("--force/--no-force", default=False, show_default=True)
def main(
    datasets: tuple[str, ...],
    output_dir: str,
    timeout: int,
    force: bool,
) -> None:
    selected = list(datasets) or list(PACO_DATASETS)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    for dataset in selected:
        _download_entry(
            name=dataset,
            config=PACO_DATASETS[dataset],
            output_dir=output_path,
            timeout=timeout,
            force=force,
        )


if __name__ == "__main__":
    main()
