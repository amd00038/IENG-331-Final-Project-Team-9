

import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import plotly.express as px
    import re
    return pl, px


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
def _(province, taster_name):
    reviewers_province = taster_name.join(
            province, on="province"
        )
    reviewers_province
        # artist_artwork = artist_parquet.join(artworks_Titles, on="ConstituentID").remove(pl.col("Date") == datetime(9999,1,1))
        # artworks_creation = (
        #     artist_artwork.select(pl.col("Date"))
        #     .with_columns(
        #         pl.col("Date").dt.year().max().alias("latest"),
        #         pl.col("Date").dt.year().min().alias("earliest"),
        #         pl.col("Date").dt.year().median().alias("median"),
        #     )
    # two_provinces = province.filter(pl.col("province").is_in(["&","and"]))
    # two_provinces
    # count = (
    #         taster_name
    #         .group_by("spotify_track_uri")
    #         .agg(
    #             skip_total = pl.col("skipped").sum(),
    #             play_count = pl.col("skipped").len(),
    #             skip_rate = pl.col("skipped").sum() / pl.col("skipped").len(),
    #         )
    return


@app.cell
def _(counts, px):
    px.histogram(counts, x="province", y="len")
    return


if __name__ == "__main__":
    app.run()
