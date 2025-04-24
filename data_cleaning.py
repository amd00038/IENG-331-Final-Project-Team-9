

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
    data = pl.read_csv("data/winemag-data.csv", schema_overrides={"id" : pl.String})
    data.write_parquet("pipeline/data.parquet")
    return (data,)


@app.cell
def _(data, pl):
    province = data.drop_nulls(subset=pl.col("province"))
    province.write_parquet("pipeline/province.parquet")
    return


@app.cell
def _(data, pl):
    price = data.drop_nulls(subset=pl.col("price"))
    price.write_parquet("pipeline/price.parquet")
    return


@app.cell
def _(data, pl):
    taster_name = data.with_columns(pl.col("taster_name").fill_null("Anonymous"))
    taster_name.write_parquet("pipeline/taster_name.parquet")
    return


@app.cell
def _(data, pl):
    country = data.drop_nulls(subset=pl.col("country"))
    country.write_parquet("pipeline/country.parquet")
    return


@app.cell
def _(data, pl):
    description = data.drop_nulls(pl.col("description"))
    description.write_parquet("pipeline/description.parquet")
    return


if __name__ == "__main__":
    app.run()
