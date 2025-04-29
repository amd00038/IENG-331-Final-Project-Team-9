

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
def _(mo):
    mo.md(r"""## Cleaned Data""")
    return


@app.cell
def _(pl):
    times = (pl.read_parquet("pipeline/data.parquet")
        .drop_nulls(subset=pl.col("title"))
        .with_columns(pl.col("title").str.extract(r"(19[2-9]\d|20\d{2})").alias("Date").str.to_date(format = "%Y"))).drop_nulls(subset=pl.col("Date"))
    times
    return (times,)


@app.cell
def _(pl, times):
    number_per_year = times.group_by("Date").agg(pl.col("title").len().alias("Number_of_wines"), pl.col("price").mean().alias("Average Price"), pl.col("points").mean().alias("Average Points")).sort(by="Number_of_wines",descending=True)
    number_per_year.head(10)
    return


@app.cell
def _(pl, times):
    Low_per_year = (
        times.group_by("Date")
        .agg(
            pl.col("title").len().alias("Number_of_wines"),
            pl.col("price").mean().alias("Average Price"),
            pl.col("points").mean().alias("Average Points"),
        )
        .sort(
            by=["Number_of_wines", "Date"], descending=False
        )
    )

    Low_per_year.head(10)
    return


@app.cell
def _(mo):
    mo.md(r"""## Year and Price Comparison""")
    return


@app.cell
def _(px, times):
    px.box(times.sort(by = "Date", descending=False), x = "Date", y = "price", title = "Years and Price", subtitle = "Box plot for the distribution of prices for bottles of wine made each year", labels = {'Date' : "Year", "price" : "Price"}, range_x = ["1998-08-01","2017-08-01"],range_y = [0,300])
    return


@app.cell
def _(mo):
    mo.md(r"""### This table has been zoomed in to include years from 2017-1999 as those were the most common years as determined by the cleanded dataset. This provides a better view of the majority of this dataset. In every year except the most recent, 2017, there are an abundance of outliers that are quite far from the years' median. Additionally the medians are roughly close together sans a spike in 2004 and dip after 2014. This may indicate that price is not the best measure for differences in wines across time.""")
    return


@app.cell
def _(mo):
    mo.md(r"""## Points and Years""")
    return


@app.cell
def _(px, times):
    px.box(times.sort(by = "Date", descending=False), x = "Date", y = "points", title = "Points and Years", subtitle = "Box plots of point distributions for wines made each year", labels= {'points' : 'Points', 'Date' : 'Year'}, range_x = ["1982-08-01","2017-08-01"],range_y = [79,101])
    return


@app.cell
def _(mo):
    mo.md(r"""### There are definite trends in the points destributed to wines from different years. There is a concentration of outliers in years after 2000 that is not present in eariler years. Addiontally, the years after 2000 have medians that are close to each other where as the earlier years do not. """)
    return


@app.cell
def _(mo):
    mo.md(r"""### The box plot also shows that wines fromw within the last 26 years have a lot of variation when it comes to price. In particular there are a lot outliers. Now this is already a zoomed inview of the data as there are later wines cut off in addition to extreme outliers in price (around $3,000) that have been cutoff as well.""")
    return


if __name__ == "__main__":
    app.run()
