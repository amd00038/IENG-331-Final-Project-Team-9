

import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import plotly.express as px
    import re
    return (pl,)


@app.cell
def _(pl):
    taster_name = (pl.read_parquet("pipeline/taster_name.parquet"))
    taster_name
    return (taster_name,)


@app.cell
def _(pl, taster_name):
    counts = (
            taster_name.with_columns(pl.col("province"))
            .group_by("province")
            .len().drop_nulls()
        )
    counts
    return


if __name__ == "__main__":
    app.run()
