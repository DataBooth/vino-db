import marimo

__generated_with = "0.15.0"
app = marimo.App()


@app.cell
def _():
    import marimo as mo
    return


@app.cell
def _():
    from vino_db.ddb import DuckDBRunner
    from pathlib import Path
    from loguru import logger
    import sys
    import plotly.express as px
    import pandas as pd
    import numpy as np
    import plotly.graph_objects as go
    return DuckDBRunner, Path, go, logger, np, pd, sys


@app.cell
def _(logger, sys):
    logger.remove()  # Remove default logger
    logger.add(sys.stderr, level="ERROR")  # Console output
    logger.add("duckdb_runner.log", level="INFO")  # File output
    return


@app.cell
def _(Path, logger, sql_file):
    db_path = Path("data/xwines.duckdb")
    sql_dir = Path("sql/")

    # Verify that the database file exists
    if not db_path.exists():
        logger.error(f"Database file not found: {db_path}")

    # Verify that the SQL file exists
    if not sql_dir.exists():
        logger.error(f"SQL file not found: {sql_file}")
    return db_path, sql_dir


@app.cell
def _(DuckDBRunner, db_path):
    def run_sql(sql, db_path=str(db_path), log_file="duckdb_runner.log"):
        with DuckDBRunner(db_path, log_file) as runner:
            return runner.run(sql)
    return (run_sql,)


@app.cell
def _(run_sql, sql_dir):
    # Add re-scaled rating column (once)

    import duckdb

    try:
        run_sql(str(sql_dir / "add_rescaled_rating.sql"))
    except duckdb.CatalogException as e:
        if "Column with name rescaled_rating already exists" in str(e):
            print("\nSKIPPED: Column already exists (as expected) - skipping add column step.")
        else:
            raise

    return


@app.cell
def _(run_sql):
    SAMPLE_SIZE = 500_000
    SEED = 21

    sql_sample = f"""
        SELECT *
        FROM ratings
        USING SAMPLE reservoir({SAMPLE_SIZE} ROWS) REPEATABLE ({SEED});
    """

    original_rows = run_sql("SELECT COUNT(*) FROM ratings")

    ratings_df = run_sql(sql_sample)
    return SAMPLE_SIZE, original_rows, ratings_df


@app.cell
def _(SAMPLE_SIZE, original_rows):
    print(f"Using {SAMPLE_SIZE/original_rows*100}% sample")
    return


@app.cell
def _(ratings_df):
    ratings_df.info()
    return


@app.cell
def _(go, np, pd):
    def plot_histogram(df, columns, bins=None, title=None, width=700, height=400, secondary_y=False, log_y=False, max_bin_percent=5.0):
        """
        Plot a histogram with counts and relative percentages, optionally with a secondary y-axis for percentages,
        a log-scaled y-axis, and dynamic bin adjustment to target a maximum bin percentage.

        Parameters:
        - df: pandas DataFrame containing the data.
        - columns: str or list of str, column(s) to plot.
        - bins: int, number of bins for numerical data (ignored for categorical data or if auto-adjusted).
        - title: str, custom title for the plot.
        - width, height: int, dimensions of the plot.
        - secondary_y: bool, if True, plot percentages on a secondary y-axis.
        - log_y: bool, if True, use a log-scaled y-axis for counts.
        - max_bin_percent: float or None, target maximum percentage per bin for numerical data (default 5.0).
                           If None, uses default binning (min(30, len(unique))).
        """
        if isinstance(columns, str):
            columns = [columns]

        for col in columns:
            # Validate column existence
            if col not in df.columns:
                raise ValueError(f"Column '{col}' not found in DataFrame")

            # Check if column is categorical or numerical
            is_categorical = isinstance(df[col].dtype, pd.CategoricalDtype) or df[col].dtype == 'object'

            if is_categorical:
                # For categorical data, use value_counts directly
                counts = df[col].value_counts().sort_index()
                percentages = df[col].value_counts(normalize=True).sort_index() * 100
                print(f"Percentages for categorical column '{col}':\n{percentages}")  # Debug
                if bins is not None or max_bin_percent is not None:
                    print(f"Warning: 'bins' and 'max_bin_percent' are ignored for categorical column '{col}'")
            else:
                # For numerical data, compute histogram bins
                data = df[col].dropna()
                if not data.empty:
                    if bins is None and max_bin_percent is None:
                        # Original default binning
                        bins = min(30, len(df[col].unique()))
                        counts, bin_edges = np.histogram(data, bins=bins)
                        percentages = (counts / counts.sum() * 100) if counts.sum() > 0 else np.zeros_like(counts)
                    elif bins is None and max_bin_percent is not None:
                        # Dynamic bin adjustment for max_bin_percent
                        n = len(data)
                        target_bins = max(1, int(np.ceil(n * max_bin_percent / 100)))  # Estimate bins for ~max_bin_percent
                        counts, bin_edges = np.histogram(data, bins=target_bins)
                        percentages = (counts / counts.sum() * 100) if counts.sum() > 0 else np.zeros_like(counts)
                        # Adjust bins if max percentage is too high
                        while percentages.max() > max_bin_percent and target_bins < n:
                            target_bins += 1
                            counts, bin_edges = np.histogram(data, bins=target_bins)
                            percentages = (counts / counts.sum() * 100) if counts.sum() > 0 else np.zeros_like(counts)
                        bins = target_bins
                    else:
                        # User-specified bins
                        counts, bin_edges = np.histogram(data, bins=bins)
                        percentages = (counts / counts.sum() * 100) if counts.sum() > 0 else np.zeros_like(counts)
                    counts = pd.Series(counts, index=bin_edges[:-1])
                    percentages = pd.Series(percentages, index=bin_edges[:-1])
                    print(f"Using {bins} bins for numerical column '{col}'")
                    print(f"Percentages for numerical column '{col}':\n{percentages}")  # Debug
                else:
                    raise ValueError(f"Column '{col}' contains no valid data after dropping NaNs")

            # Create figure
            fig = go.Figure()

            # Add histogram bars for counts
            if is_categorical:
                fig.add_trace(
                    go.Bar(
                        x=counts.index,
                        y=counts.values,
                        name="Count",
                        hovertemplate=f"{col}: %{{x}}<br>Count: %{{y}}<br>Percentage: %{{customdata:.2f}}%<extra></extra>",
                        customdata=percentages.values,
                        marker_color='#1f77b4'  # Distinct color for counts
                    )
                )
            else:
                fig.add_trace(
                    go.Histogram(
                        x=df[col],
                        nbinsx=bins,
                        name="Count",
                        hovertemplate=f"{col}: %{{x}}<br>Count: %{{y}}<br>Percentage: %{{customdata:.2f}}%<extra></extra>",
                        customdata=np.histogram(df[col].dropna(), bins=bins, weights=np.ones(len(df[col].dropna())) / len(df[col].dropna()) * 100)[0],
                        marker_color='#1f77b4'
                    )
                )

            # Optionally add secondary y-axis for percentages
            if secondary_y:
                fig.add_trace(
                    go.Scatter(
                        x=counts.index if is_categorical else bin_edges[:-1],
                        y=percentages.values,
                        name="Percentage",
                        yaxis="y2",
                        mode="lines+markers",
                        hovertemplate=f"{col}: %{{x}}<br>Percentage: %{{y:.2f}}%<extra></extra>",
                        line=dict(color='#ff7f0e', width=2)  # Distinct color for percentages
                    )
                )

            # Update layout
            layout = dict(
                title=title or f"Histogram of {col}",
                xaxis_title=col,
                yaxis_title="Count",
                width=width,
                height=height,
                bargap=0.1,
                template="plotly_white"
            )

            if log_y:
                layout.update(
                    yaxis=dict(
                        type="log",
                        range=[np.log10(0.1), np.log10(max(counts.max(), 1) * 1.5)]  # Ensure positive range
                    )
                )
            else:
                layout.update(
                    yaxis=dict(
                        type="linear"  # Explicitly set to linear for clarity
                    )
                )

            if secondary_y:
                layout.update(
                    yaxis2=dict(
                        title="Percentage (%)",
                        overlaying="y",
                        side="right",
                        range=[0, 100]
                    )
                )

            fig.update_layout(**layout)
            fig.show()

    # Example usage
    # plot_histogram(ratings_df, "rating", secondary_y=True, log_y=False, # max_bin_percent=None)

    return (plot_histogram,)


@app.cell
def _(plot_histogram, ratings_df):
    plot_histogram(ratings_df, "rating", log_y=True)
    # plot_histogram(ratings_df, ["rating", "rescaled_rating"], bins=15)
    return


@app.cell
def _(plot_histogram, ratings_df):
    plot_histogram(ratings_df, "rescaled_rating")
    return


@app.cell
def _(DuckDBRunner, db_path, logger, sql_dir):
    with DuckDBRunner(db_path=str(db_path), log_file="duckdb_runner.log") as runner:
        try:
            # Example: Run SQL from file (rating_outliers.sql)
            logger.info(f"Running SQL from: {sql_dir}")
            df_outliers = runner.run(str(sql_dir / "rating_outliers.sql"))
            logger.info("SQL file query results:")
            print(df_outliers.head())

        except Exception as e:
            logger.error(f"Failed to execute query: {e}")
    return (df_outliers,)


@app.cell
def _(df_outliers):
    df_outliers
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
