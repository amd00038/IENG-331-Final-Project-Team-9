

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
    taster_name = pl.read_parquet("pipeline/taster_name.parquet").select(pl.col("taster_name"),pl.col("province")).drop_nulls(subset=pl.col("province"))
    taster_name
    return


@app.cell
def _(reviewers_province):
    count =reviewers_province
            #.group_by("province")
            #.agg(
                #reviewer_count = pl.col("taster_name").len(),
            #)
        #)
    #count.head()
    return


@app.cell
def _():
    # px.histogram(count, x="province", y="reviewer_count")
    return


if __name__ == "__main__":
    app.run()
