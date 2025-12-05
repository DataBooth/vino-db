    import sys
    import time
    from pathlib import Path

    import duckdb
    from loguru import logger
    from pandas import DataFrame


    class DuckDBRunner:
        def __init__(
            self, db_path: str = ":memory:", log_file: str = None, verbose: bool = True
        ):
            self.db_path = db_path
            self.conn = None
            self.verbose = verbose
            if log_file:
                logger.add(log_file, level="INFO")

        def __enter__(self):
            try:
                self.conn = duckdb.connect(self.db_path)
                if self.verbose:
                    logger.info(f"DuckDB connected to {self.db_path}")
                return self
            except Exception as e:
                logger.error(f"Failed to connect to DuckDB at {self.db_path}: {e}")
                raise

        def __exit__(self, exc_type, exc_val, exc_tb):
            if self.conn:
                self.conn.close()
                if self.verbose:
                    logger.info("DuckDB connection closed")
                self.conn = None

        def run(
            self, sql_or_path: str, params: tuple = None, encoding: str = "utf-8"
        ) -> DataFrame:
            """
            Auto-detects if input is a .sql file or raw SQL text.
            Executes the SQL and returns a Pandas DataFrame.
            """
            try:
                if sql_or_path.strip().lower().endswith(".sql"):
                    sql_path = Path(sql_or_path)
                    if sql_path.is_file():
                        if self.verbose:
                            logger.info(f"Detected SQL file: {sql_or_path}")
                        with open(sql_path, "r", encoding=encoding) as f:
                            sql_text = f.read()
                    else:
                        logger.error(f"SQL file not found: {sql_or_path}")
                        raise FileNotFoundError(f"SQL file not found: {sql_or_path}")
                else:
                    if self.verbose:
                        logger.info("Detected raw SQL text")
                    sql_text = sql_or_path

                if not sql_text.strip():
                    logger.error("Empty SQL query provided")
                    raise ValueError("SQL query cannot be empty")

                start = time.time()
                result = (
                    self.conn.execute(sql_text, params)
                    if params
                    else self.conn.execute(sql_text)
                )
                result_df = result.df()
                duration = time.time() - start
                if self.verbose:
                    logger.info(
                        f"Executed SQL - row count: {len(result_df)}, duration: {duration:.2f}s"
                    )
                return result_df
            except Exception as e:
                logger.error(f"Error executing SQL: {e}")
                raise


    def main():
        # Configure logging to output to both console and a file
        logger.remove()  # Remove default logger
        logger.add(sys.stderr, level="INFO")  # Console output
        logger.add("duckdb_runner.log", level="INFO")  # File output

        # Define paths
        db_path = Path("data/xwines.duckdb")
        sql_file = Path("sql/rating_outliers.sql")

        # Verify that the database file exists
        if not db_path.exists():
            logger.error(f"Database file not found: {db_path}")
            sys.exit(1)

        # Verify that the SQL file exists
        if not sql_file.exists():
            logger.error(f"SQL file not found: {sql_file}")
            sys.exit(1)

        # Initialise DuckDBRunner with the database path
        with DuckDBRunner(db_path=str(db_path), log_file="duckdb_runner.log") as runner:
            try:
                # Example 1: Run a raw SQL query
                raw_sql = """
                SELECT wine_id, rating, user_id
                FROM ratings
                WHERE rating IS NOT NULL
                LIMIT 5;
                """
                logger.info("Running raw SQL query...")
                df_raw = runner.run(raw_sql)
                logger.info("Raw SQL query results:")
                print(df_raw)

                # Example 2: Run SQL from file (rating_outliers.sql)
                logger.info(f"Running SQL file: {sql_file}")
                df_outliers = runner.run(str(sql_file))
                logger.info("SQL file query results:")
                print(df_outliers.head())

            except Exception as e:
                logger.error(f"Failed to execute query: {e}")
                sys.exit(1)


    if __name__ == "__main__":
        main()
