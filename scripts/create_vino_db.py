from pathlib import Path

import duckdb
from loguru import logger


class WineDatabase:
    def __init__(
        self,
        db_path: str = "xwines.duckdb",
        recreate_db: bool = False,
        sql_dir: Path = Path("sql"),
    ):
        """
        Connect to DuckDB and optionally recreate schema from SQL files.
        :param db_path: DuckDB database path or ':memory:' for in-memory DB.
        :param recreate_db: If True, drop and create tables fresh.
        :param sql_dir: Directory containing SQL files to create tables.
        """
        self.db_path = db_path
        self.conn = duckdb.connect(self.db_path)
        self.sql_dir = Path(sql_dir)
        if recreate_db:
            logger.info("Recreating database schema...")
            self._execute_sql_file(self.sql_dir / "drop_tables.sql")
            self._execute_sql_file(self.sql_dir / "create_tables.sql")

    def _execute_sql_file(self, filepath: Path):
        """Helper to execute all SQL statements in a file."""
        with open(filepath, "r") as f:
            sql_script = f.read()
        self.conn.execute(sql_script)
        logger.info(f"Executed SQL file: {filepath.name}")

    def load_data(self, wines_csv: Path, ratings_csv: Path):
        logger.info("Loading wines CSV...")
        self.conn.execute(f"""
            INSERT INTO wines
            SELECT * FROM read_csv(
                '{wines_csv}',
                auto_detect=True,
                header=True,
                strict_mode=False,
                ignore_errors=True
            );
        """)
        logger.info("Loading users from ratings CSV...")
        self.conn.execute(f"""
            INSERT INTO users
            SELECT DISTINCT UserID AS user_id FROM read_csv(
                '{ratings_csv}',
                auto_detect=True,
                header=True,
                strict_mode=False,
                ignore_errors=True
            );
        """)
        logger.info("Loading ratings CSV...")
        self.conn.execute(f"""
            INSERT INTO ratings
            SELECT * FROM read_csv(
                '{ratings_csv}',
                auto_detect=True,
                header=True,
                strict_mode=False,
                ignore_errors=True
            );
        """)
        logger.info("Data loaded successfully.")

    def close(self):
        """Close DuckDB connection."""
        self.conn.close()
        logger.info("Database connection closed.")


if __name__ == "__main__":
    db = WineDatabase(
        db_path="data/xwines.duckdb", recreate_db=True, sql_dir=Path("sql")
    )

    try:
        db.load_data(
            Path(
                "data/xwines/All-XWines_Full_100K_wines_21M_ratings/XWines_Full_100K_wines.csv"
            ),
            Path(
                "data/xwines/All-XWines_Full_100K_wines_21M_ratings/XWines_Full_21M_ratings.csv"
            ),
        )
    finally:
        db.close()
