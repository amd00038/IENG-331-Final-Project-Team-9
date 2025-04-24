

import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import plotly.express as px
    return pl, px


@app.cell
def _(pl):
    taster_name = pl.read_parquet("pipeline/taster_name.parquet").select(pl.col("taster_name"),pl.col("province")).drop_nulls(subset=pl.col("province"))
    taster_name
    return (taster_name,)


@app.cell
def _():
    # split_df = taster_name.with_columns(pl.col("province").str.split_exact("&", 1).struct.rename_fields(["first_location", "second_location"]).alias("locations").unnest("locations"))
    # split_df.head()

    return


@app.cell
def _(taster_name):
    reviewer_count = (
            taster_name.group_by('taster_name')
            .len()
            .head()
        )
    return


@app.cell
def _(px, taster_name):
    px.histogram(taster_name, x="province", y="")
    return


if __name__ == "__main__":
    app.run()
