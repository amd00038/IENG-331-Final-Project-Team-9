

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
def _(mo):
    mo.md(r"""## Analysis of Average Price and Country of Origin""")
    return


@app.cell
def _(countries_parquet, pl, px):
    Country_price = countries_parquet.group_by(pl.col("country")).agg(pl.col("price").mean().alias("Average Price")).sort(by=pl.col("Average Price"), descending=True)
    px.bar(Country_price, x = "country", y="Average Price", title = "Price vs. Country",subtitle = "The average price of a bottle of wine from the various countries included in the dataset", labels = {"country" : "Country"})
    return


@app.cell
def _(mo):
    mo.md(r"""### The highest priced bottles of wine in this dataset belong to Switzerland at around 80 dollars a bottle. The average price then falls around theirty dollars from England. There are many times in this data where the decreasing trend flattens out over a group of countries. This would suggest that those countries have similar average prices. """)
    return


@app.cell
def _(mo):
    mo.md(r"""## Quantity of Wine and Country of Origin""")
    return


@app.cell
def _(countries_parquet, pl, px):
    number_wines = countries_parquet.unique(subset=pl.col("title")).group_by(pl.col("country")).agg(pl.len().alias("Number of Wines")).sort(by=pl.col("Number of Wines"), descending=True)
    px.bar(number_wines, x= "country", y = "Number of Wines", title = "Number of Wines", subtitle = "The number of wines each country submitted", range_y = [0,6200], labels = {"country" : "Country"})
    return


@app.cell
def _(mo):
    mo.md(r"""### In conatrast to the price of wine, the United States leads with around 50,000 entries in this dataset. The next two highest countries are France and Italy around 15,000 entries. Beyond that the number of entries significantly decreases to the point where the entries are not visible on the graph when it is capped at the 4th most common country. """)
    return


@app.cell
def _(mo):
    mo.md(r"""## Point Distribution and Country of Origin""")
    return


@app.cell
def _(countries_parquet, pl, px):
    Country_points = countries_parquet.group_by(pl.col("country")).agg(pl.col("points").mean().alias("average points")).sort(by=pl.col("average points"), descending=True)
    px.box(countries_parquet, x = "country", y="points", title = "Point Distribution", subtitle="Box plot of each country's point distribution", labels={'country':'Country', 'points':'Points'})
    return


@app.cell
def _(mo):
    mo.md(r"""### This dataset has a clear disparity when it comes to point outliers and country of origin. There are distinctly a group of countries with large ranges and multiple outliers, and there is a group with a small range of point values. In a future analysis it would be worthwhile to determine how correlated the expanded ranges and number of outliers is with the number of enteries in the dataset.""")
    return


if __name__ == "__main__":
    app.run()
