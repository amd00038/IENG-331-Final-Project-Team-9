

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
def _(pl):
    taster_name = pl.read_parquet("pipeline/taster_name.parquet").drop_nulls(subset = pl.col("province"))
    taster_name
    return (taster_name,)


@app.cell
def _(pl, taster_name):
    split_df = taster_name.with_columns(pl.col("province").str.extract(r"^(.*?)(?:\s+(?:&|and)\s+|$)").alias("first_province"), pl.col("province").str.extract(r"(?:^.*?\s+(?:&|and)\s+)(.*)").alias("second_province"),
    )

    split_df.head()
    return (split_df,)


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
    return (reviewer_count_two_province,)


@app.cell
def _(px, reviewer_count_two_province):
    px.bar(reviewer_count_two_province, x="taster_name", y="second_province")
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
def _(pl, taster_name):
    variety = (
        taster_name.group_by(pl.col("taster_name"))
        .agg(
            pl.col("variety").unique().count().alias("Varieties"),
            pl.col("id").count().alias("Number of Entries"), 
            pl.col("price").max().alias("Max Price"), pl.col("price").min().alias("Min Price")
        ).sort(by="Varieties", descending=False)
    )
    variety
    return (variety,)


@app.cell
def _(mo):
    mo.md(
        r"""
        ## Reviewer Varieties Graph
        The bar graph below shows the number of unique varieties per taster. This graph shows that no taster specialized in a single variety. The lowest number of varieties specialized in was 2. The highest amount specialized was 216.
        """
    )
    return


@app.cell
def _(px, variety):
    px.bar(variety, x="taster_name", y="Varieties")
    return


@app.cell
def _(mo):
    mo.md(r"""## Varieties by number of entries""")
    return


@app.cell
def _(px, variety):
    px.bar(variety, x="taster_name", y="Number of Entries")
    return


@app.cell
def _(mo):
    mo.md(
        r"""
        ## Taster Name by Number of Entries
        The graph above shows the number of reviews per taster. This graph also validates the statement that there were no tasters that specialized in a single variety. The most specialized taster had 6 entries and 6 varieties.
        """
    )
    return


if __name__ == "__main__":
    app.run()
