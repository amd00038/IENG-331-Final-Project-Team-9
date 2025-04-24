

import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import plotly.express as px
    return (pl,)


@app.cell
def _(pl):
    times = pl.read_parquet("pipeline/")
    return


if __name__ == "__main__":
    app.run()
