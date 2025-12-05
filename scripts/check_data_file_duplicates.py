from pathlib import Path
import zipfile
from loguru import logger


def read_zip_csv(zip_path: Path, csv_name: str) -> bytes:
    """Extract CSV content from ZIP archive as bytes."""
    with zipfile.ZipFile(zip_path, "r") as z:
        with z.open(csv_name) as f:
            return f.read()


def read_file_bytes(file_path: Path) -> bytes:
    """Read entire file content as bytes."""
    return file_path.read_bytes()


def check_duplicates(directory: Path):
    """
    Scan directory for CSV and ZIP file pairs. Compare CSV inside ZIP
    with standalone CSV and log whether they are identical or different.
    """
    files = list(directory.iterdir())

    # Map base filenames (without extension) to dict of csv and zip files
    base_files = {}

    for file in files:
        if file.suffix == ".csv":
            base = file.stem
            base_files.setdefault(base, {})["csv"] = file
        elif file.suffix == ".zip":
            base = file.stem
            base_files.setdefault(base, {})["zip"] = file

    for base, paths in base_files.items():
        if "csv" in paths and "zip" in paths:
            csv_path = paths["csv"]
            zip_path = paths["zip"]
            try:
                zipped_csv_content = read_zip_csv(zip_path, csv_path.name)
            except KeyError:
                logger.warning(
                    f"CSV '{csv_path.name}' not found inside ZIP '{zip_path.name}'"
                )
                continue
            except Exception as e:
                logger.error(f"Error reading '{zip_path.name}': {e}")
                continue

            csv_content = read_file_bytes(csv_path)

            if zipped_csv_content == csv_content:
                logger.info(f"[IDENTICAL] {csv_path.name} and {zip_path.name}")
            else:
                logger.info(f"[DIFFERENT] {csv_path.name} and {zip_path.name}")
        else:
            # Skip files with no matching pair
            continue


if __name__ == "__main__":
    data_dir = Path("data/xwines")  # Update path accordingly
    logger.add("check_duplicates.log", rotation="1 MB")  # Log to file with rotation
    check_duplicates(data_dir)
