

import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import plotly.express as px
    return mo, pl, px


@app.cell
def _(mo):
    mo.md(r"""## Taster Name and Province Data""")
    return


@app.cell
def _():
    # taster_name = pl.read_parquet("pipeline/taster_name.parquet").select(pl.col("taster_name"),pl.col("province")).drop_nulls(subset = pl.col("province"))
    # #taster_name
    return


@app.cell
def _():
    # split_df = taster_name.with_columns(pl.col("province").str.extract(r"^(.*?)(?:\s+(?:&|and)\s+|$)").alias("first_province"), pl.col("province").str.extract(r"(?:^.*?\s+(?:&|and)\s+)(.*)").alias("second_province"),
    #     )

    # #split_df
    return


@app.cell
def _(mo):
    mo.md(r"""## Reviewer Province Count""")
    return


@app.cell
def _():
    # two_province = split_df.drop_nulls("second_province")
    # reviewer_count_two_province = (
    #         two_province.group_by("taster_name")
    #         .len("second_province"))
    # two_province
    return


@app.cell
def _():
    #px.bar(reviewer_count_two_province, x="taster_name", y="second_province")
    return


@app.cell
def _(mo):
    mo.md(r"""There are only 4 reviewers out of 20 that specialize in two provinces. The rest of the reviewers specialize in one province. The above bar chart visualizes this.""")
    return


@app.cell
def _(mo):
    mo.md(r"""## Reviewer Varieties Data""")
    return


@app.cell
def _(pl):
    varieties = pl.read_parquet("pipeline/taster_name.parquet").select(pl.col("taster_name"),pl.col("designation")).drop_nulls(subset = pl.col("designation"))
    #varieties
    return (varieties,)


@app.cell
def _(mo):
    mo.md(
        r"""
        ## Reviewer Varieties Counts
        The data frame below counts the number of unique varieties per taster.
        """
    )
    return


@app.cell
def _(pl, varieties):
    taster_variety_counts = (
        varieties.group_by(['taster_name', 'designation'])
          .agg(pl.len().alias('count'))
    )
    #taster_variety_counts
    return (taster_variety_counts,)


@app.cell
def _(mo):
    mo.md(r"""The bar chart below visualizes the number of unique varieties per reviewer. This shows that majority of tasters reviewed a large variety of wines. They did not stick to specific varieties.""")
    return


@app.cell
def _(px, taster_variety_counts):
    px.bar(taster_variety_counts, x="taster_name", y="count")
    return


if __name__ == "__main__":
    app.run()
