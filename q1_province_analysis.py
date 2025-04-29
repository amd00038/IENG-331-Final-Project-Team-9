

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
    return mo, pl, px


@app.cell
def _(pl):
    province=pl.read_parquet("pipeline/province.parquet")
    province.head()
    return (province,)


@app.cell
def _(province):
    province_normalized=province.drop_nulls("price")
    province_normalized


    return (province_normalized,)


@app.cell
def _(mo):
    mo.md("""#Highest Average Prices by Province""")
    return


@app.cell
def _(pl, province_normalized):
    avg_price_per_province = (
        province_normalized
        .group_by("province")  # Group by province
        .agg(pl.col("price").mean().alias("avg_price"))  
        .sort("avg_price", descending=True) 
        .head(20)
    )

    avg_price_per_province
    return (avg_price_per_province,)


@app.cell
def _(avg_price_per_province, px):
    Highest_average_prices=px.bar(
        avg_price_per_province,
        x="province",
        y="avg_price",
        title="Highest Average Prices by Province"
    )
    Highest_average_prices
    return


@app.cell
def _(mo):
    mo.md("""##### We only included the top 20 provinces with the highest average prices. While the average price is really high in Colares, it does flatten out meaning that the average cost of wine likely does not differ much by province excluding the few outliers.""")
    return


@app.cell
def _():
    #Highest Average Prices by Province More than 10
    return


@app.cell
def _(pl, province_normalized):
    provinces_over_10df = (
        province_normalized
        .group_by("designation","province","price")
        .agg(pl.count().alias("designation_count"))
        .filter(pl.col("designation_count") > 10)
    )

    provinces_over_10df
    return (provinces_over_10df,)


@app.cell
def _(pl, provinces_over_10df):
    avg_price_per_province_over10 = (
        provinces_over_10df
        .group_by("province")  # Group by province
        .agg(pl.col("price").mean().alias("avg_price"))  
        .sort("avg_price", descending=True) 
        .head(20)
    )
    avg_price_per_province_over10
    return (avg_price_per_province_over10,)


@app.cell
def _(avg_price_per_province_over10, px):
    Highest_average_prices_over10=px.bar(
        avg_price_per_province_over10,
        x="province",
        y="avg_price",
        title="Highest Average Prices by Province Over 10"
    )
    Highest_average_prices_over10
    return


@app.cell
def _(mo):
    mo.md("""####California has the highest average price for wine in this scenario. Bar chart still flattens out, again showing not much difference in price for provinces with at least 10 wines.""")
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
