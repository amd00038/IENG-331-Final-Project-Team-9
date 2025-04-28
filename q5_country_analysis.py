

import marimo

__generated_with = "0.13.0"
app = marimo.App(
    width="medium",
    layout_file="layouts/q5_country_analysis.slides.json",
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
def _(pl):
    countries_parquet = pl.read_parquet("pipeline/country.parquet").drop_nulls(subset= pl.col("price"))
    countries_parquet.head()
    return (countries_parquet,)


@app.cell
def _(countries_parquet, pl, px):
    Country_price = countries_parquet.group_by(pl.col("country")).agg(pl.col("price").mean().alias("Average Price")).sort(by=pl.col("Average Price"), descending=True)
    px.bar(Country_price, x = "country", y="Average Price", title = "Price vs. Country",subtitle = "The average price of a bottle of wine from the various countries included in the dataset", labels = {"country" : "Country"})
    return


@app.cell
def _(countries_parquet, pl, px):
    number_wines = countries_parquet.unique(subset=pl.col("title")).group_by(pl.col("country")).agg(pl.len().alias("Number of Wines")).sort(by=pl.col("Number of Wines"), descending=True)
    px.bar(number_wines, x= "country", y = "Number of Wines", title = "Number of Wines", subtitle = "The number of wines each country submitted", range_y = [0,6200], labels = {"country" : "Country"})
    return


@app.cell
def _(countries_parquet, pl, px):
    Country_points = countries_parquet.group_by(pl.col("country")).agg(pl.col("points").mean().alias("average points")).sort(by=pl.col("average points"), descending=True)
    px.box(countries_parquet, x = "country", y="points", title = "Point Distribution", subtitle="Box plot of each country's point distribution", labels={'country':'Country', 'points':'Points'})
    return


if __name__ == "__main__":
    app.run()
