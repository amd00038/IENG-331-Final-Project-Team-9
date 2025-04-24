

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
    taster_name = pl.read_parquet("pipeline/taster_name.parquet").select(pl.col("taster_name"),pl.col("province")).drop_nulls(subset = pl.col("province"))
    #taster_name
    return (taster_name,)


@app.cell
def _(pl, taster_name):
    split_df = taster_name.with_columns(pl.col("province").str.extract(r"^(.*?)(?:\s+(?:&|and)\s+|$)").alias("first_province"), pl.col("province").str.extract(r"(?:^.*?\s+(?:&|and)\s+)(.*)").alias("second_province"),
        )

    split_df
    # split_df.null_count()
    return (split_df,)


@app.cell
def _(split_df):
    two_province = split_df.drop_nulls("second_province")
    reviewer_count_two_province = (
            two_province.group_by("taster_name")
            .len("second_province"))

    return (reviewer_count_two_province,)


@app.cell
def _(px, reviewer_count_two_province):
    px.bar(reviewer_count_two_province, x="taster_name", y="second_province")
    return


@app.cell
def _(split_df):
    reviewer_count_one_province = (
            split_df.group_by("taster_name")
            .len("second_province").null_count())
    return (reviewer_count_one_province,)


@app.cell
def _(px, reviewer_count_one_province):
    px.bar(reviewer_count_one_province, x="taster_name", y="second_province")
    return


if __name__ == "__main__":
    app.run()
