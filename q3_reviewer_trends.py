

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
    taster_name = (pl.read_parquet("pipeline/taster_name.parquet"))
    #taster_name
    return (taster_name,)


@app.cell
def _(pl):
    province = (pl.read_parquet("pipeline/province.parquet"))
    #province
    return (province,)


@app.cell
def _(pl, province, taster_name):
    reviewers_province = taster_name.join(
            province, on="province", how="right")
    count = (
            reviewers_province
            .group_by("province")
            .agg(
                reviewer_count = pl.col("taster_name").len(),
            )
        )
    count
    return


@app.cell
def _():
    # px.histogram(count, x="province", y="reviewer_count")
    return


if __name__ == "__main__":
    app.run()
