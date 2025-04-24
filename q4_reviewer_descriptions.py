

import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import plotly.express as px
    import polars as pl
    import polars.selectors as cs
    import plotly.graph_objects as go
    import re
    from datetime import datetime
    return


app._unparsable_cell(
    r"""
    dictionary=pl.read_parquet(\"pipeline/dictionary.parquet\")
        dictionary.head()
    """,
    name="_"
)


app._unparsable_cell(
    r"""
     artworks=pl.read_parquet(\"data/artworks.parquet\")
        artworks

        artworks_normalized=artworks.with_columns([
            pl.col(\"Medium\")
            .str.to_lowercase()
            .str.strip_chars()
            .alias(\"Medium\")
        ])
        artworks_normalized
    """,
    name="_"
)


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
