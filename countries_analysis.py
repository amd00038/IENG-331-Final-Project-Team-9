

import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import plotly.express as px
    import re
    return pl, px


@app.cell
def _(pl):
    countries_parquet = pl.read_parquet("pipeline/country.parquet")
    countries_parquet
    return (countries_parquet,)


@app.cell
def _(countries_parquet, pl, px):
    Country_price = countries_parquet.group_by(pl.col("country")).agg(pl.col("price").mean().alias("average price")).sort(by=pl.col("average price"), descending=True)
    px.bar(Country_price, x = "country", y="average price")
    return


@app.cell
def _(countries_parquet, pl, px):
    number_wines = countries_parquet.unique(subset=pl.col("title")).group_by(pl.col("country")).agg(pl.len().alias("number of wines")).sort(by=pl.col("number of wines"), descending=True)
    px.bar(number_wines, x= "country", y = "number of wines")
    return


if __name__ == "__main__":
    app.run()
