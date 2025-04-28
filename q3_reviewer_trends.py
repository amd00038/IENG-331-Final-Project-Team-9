

import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import plotly.express as px
    return mo, pl


@app.cell
def _(mo):
    mo.md(r"""## Taster Name and Province Data""")
    return


@app.cell
def _(pl):
    taster_name = pl.read_parquet("pipeline/taster_name.parquet").drop_nulls(subset = pl.col("province"))
    taster_name
    return (taster_name,)


@app.cell
def _(pl, taster_name):
    # split_df = taster_name.with_columns(pl.col("province").str.extract(r"^(.*?)(?:\s+(?:&|and)\s+|$)").alias("first_province"), pl.col("province").str.extract(r"(?:^.*?\s+(?:&|and)\s+)(.*)").alias("second_province"),
    # )

    # split_df.head()
    variety = (
        taster_name.group_by(pl.col("taster_name"))
        .agg(
            pl.col("variety").unique().count().alias("Varieties"),
            pl.col("id").count().alias("Number of Entries"), 
            pl.col("price").max().alias("Max Price"), pl.col("price").min().alias("Min Price")
        ).sort(by="Varieties", descending=False)
    )
    variety
    return


@app.cell
def _(mo):
    mo.md(r"""## Reviewer Province Count""")
    return


@app.cell
def _(split_df):
    two_province = split_df.drop_nulls("second_province")
    reviewer_count_two_province = (
            two_province.group_by("taster_name")
            .len("second_province"))
    reviewer_count_two_province.head()
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
    varieties.head()
    return


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
def _():
    #total_reviews = (
        #varieties.group_by(['taster_name'])
          #.agg(pl.len().alias('count'))
    #)
    #total_reviews
    return


@app.cell
def _():
    #variety_trends = (
        #varieties.group_by(['taster_name', 'designation'])
          #.agg(pl.len().alias('count'))
    #)
    #variety_trends
    return


@app.cell
def _():
    #px.bar(variety_trends, x="taster_name", y = "count")
    return


@app.cell
def _(mo):
    mo.md(r"""The bar chart below visualizes the number of unique varieties per reviewer. This shows that majority of tasters reviewed a large variety of wines. They did not stick to specific varieties. The reviewer that had the least variety was Christina Pickard with only 2. The reviewer that had the most variety was Roger Voss with 17963. There are a total of 37,976 unique varieties. This chart also shows the reviewers with the most reviews and least. The name "Anonymous" is the fill for the null values in taster_name. Roger Voss has the most reviews at 17.963k reviews.""")
    return


@app.cell
def _():
    #px.bar(taster_variety_counts, x="taster_name", y="count")
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
