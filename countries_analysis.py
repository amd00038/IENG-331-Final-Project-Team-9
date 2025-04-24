

import marimo

__generated_with = "0.13.0"
app = marimo.App(
    width="medium",
    layout_file="layouts/countries_analysis.slides.json",
)


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import plotly.express as px
    import re
    return mo, pl, px


@app.cell
def _(mo):
    mo.md(r"""## Cleaned Data""")
    return


@app.cell
def _(mo):
    mo.md(r"""## I want this to go with the table""")
    return


@app.cell
def _(pl):
    countries_parquet = pl.read_parquet("pipeline/country.parquet").drop_nulls(subset= pl.col("price"))
    countries_parquet.head()
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


app._unparsable_cell(
    r"""
    Country_points = countries_parquet.group_by(pl.col(\"country\")).agg(pl.col(\"points\").mean().alias(\"average points
    \")).sort(by=pl.col(\"average price\"), descending=True)
    px.bar(Country_price, x = \"country\", y=\"average points\")
    """,
    name="_"
)


if __name__ == "__main__":
    app.run()
