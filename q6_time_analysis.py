

import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import plotly.express as px
    import datetime
    return mo, pl, px


@app.cell
def _(pl):
    times = (pl.read_parquet("pipeline/data.parquet")
        .drop_nulls(subset=pl.col("title"))
        .with_columns(pl.col("title").str.extract(r"(19\d{2}|20\d{2})").alias("Date").str.to_date(format = "%Y"))).drop_nulls(subset=pl.col("Date"))
    times
    return (times,)


@app.cell
def _(pl, times):
    number_per_year = times.group_by("Date").agg(pl.col("title").len().alias("Number_of_wines")).sort(by="Number_of_wines",descending=True)
    number_per_year.head(10)
    return


@app.cell
def _(pl, times):
    Low_per_year = times.group_by("Date").agg(pl.col("title").len().alias("Number_of_wines")).sort(by="Number_of_wines",descending=False)
    Low_per_year.head(10)
    return


@app.cell
def _(px, times):
    px.box(times.sort(by = "Date", descending=False), x = "Date", y = "price", range_x = ["1998-08-01","2017-08-01"],range_y = [0,800])
    return


@app.cell
def _(px, times):
    px.box(times.sort(by = "Date", descending=False), x = "Date", y = "points", range_x = ["1998-08-01","2017-08-01"],range_y = [79,101])
    return


@app.cell
def _(mo):
    mo.md(r"""### The box plots show that wines fromw within the last 26 years have a lot of variation when it comes to price. In particular there are a lot outliers. Now this is already a zoomed inview of the data as there are later wines cut off in addition to extreme outliers in price (around $3,000) that have been cutoff as well. """)
    return


@app.cell
def _(pl, px, times):
    px.bar(times.group_by("Date").agg(pl.col("price").mean().alias("Average Price")).sort(by="Date",descending=False), x="Date", y="Average Price",range_x = ["1998-08-01","2017-08-01"],range_y = [0,60])
    return


if __name__ == "__main__":
    app.run()
