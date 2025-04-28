

import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import plotly.express as px
    import polars as pl
    import polars.selectors as cs
    import plotly.graph_objects as go
    import re
    from datetime import datetime
    return pl, px


@app.cell
def _(pl):
    descriptions=pl.read_parquet("pipeline/description.parquet")
    descriptions.head()
    return (descriptions,)


@app.cell
def _(descriptions, pl):
    descriptions_normalized=descriptions.with_columns([
            pl.col("description")
            .str.to_lowercase()
            .str.strip_chars()
            .alias("description")
    ])
    descriptions_normalized

    return (descriptions_normalized,)


@app.cell
def _(descriptions_normalized, pl):
    positive_df=descriptions_normalized.filter(pl.col("points") >= 90)
    positive_df.head()
    return (positive_df,)


@app.cell
def _(descriptions_normalized, pl):
    Bottom_5=descriptions_normalized.group_by(pl.col("points")).agg(pl.col("id").len()).sort(by="points", descending=False)
    Bottom_5.head(5)
    return


@app.cell
def _(descriptions_normalized, pl):
    points_df=descriptions_normalized.group_by(pl.col("points")).agg(pl.col("id").len()).sort(by="points", descending=False)
    top_5=descriptions_normalized.group_by(pl.col("points")).agg(pl.col("id").len()).sort(by="points", descending=True)
    top_5.head(5)
    return (points_df,)


@app.cell
def _(points_df, px):
    px.pie(points_df, values = "id", names = "points")
    return


@app.cell
def _(pl, positive_df):
    positive_separated = positive_df.with_columns([
        pl.col("description")
        .str.replace(", and", "")
        .str.replace(" and", "")
        .str.replace("and ","")
        .str.replace("and","")
        .str.replace(" and ","")
        .str.replace("and, ","")
        .str.replace(" with","")
        .str.replace("with ","")
        .str.replace("wine","")
        .str.replace(" but","")
        .str.replace("but ","")
        .str.replace(" this","")
        .str.replace("this ","")
        .str.replace(" is","")
        .str.replace("is ","")
        .str.replace(" in","")
        .str.replace("in ","")
        .str.replace(" it","")
        .str.replace("it ","")
        .str.replace(" the","")
        .str.replace("the ","")
        .str.replace(" on ","")
        .str.replace("on ","")
        .str.replace(" from","")
        .str.replace("from ","")
        .str.replace(" of","")
        .str.replace("of ","")
        .str.replace(" that","")
        .str.replace("that ","")
        .str.replace("a ","")
        .str.replace(" a","")
        .str.replace(" a ","")
        .str.replace(" the ","")
        .str.replace("the ","")
        .str.replace(" the","")
        .str.replace("the","")
        .str.replace(" to ","")
        .str.replace("to ","")
        .str.replace(" to","")
        .str.replace("to","")
        .str.replace(" of ","")
        .str.replace("of ","")
        .str.replace(" of","")
        .str.replace("of","")
        .str.replace(" by ","")
        .str.replace("by ","")
        .str.replace(" by","")
        .str.replace("by","")
        .str.replace(" big","")
        .str.replace("big,","")
        .str.replace("big ","")
        .str.replace(" lear","")
        .str.replace(" to the ","")
        #.str.replace(". ",",")
        .str.replace(r"\s+", " ")
        .str.replace(",",", ")
        .str.split(", ")
        .alias("separated_description")
    ])
    positive_separated
    return (positive_separated,)


@app.cell
def _(pl, positive_separated):
    Top_10_descriptions=(
        positive_separated
        .explode("separated_description") #https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.explode.html#polars.DataFrame.explode
        .group_by("separated_description")
        .agg(pl.len().alias("count"))
        .sort("count",descending=True)
        .head(10)
    )
    Top_10_descriptions
    return


@app.cell
def _(Top_10_materials, px):
    medium_bar=px.bar(
            Top_10_materials,
            x="Materials",
            y="count",
            title="Top 10 Art Mediums"
        )
    medium_bar
    return


@app.cell
def _(description_separated, pl):

    with_positive_reviews = description_separated.with_columns(
        positive_reviews=pl.when((pl.col("points")>=90))
        .then(pl.col("points"))
        .otherwise(None)

    )
    with_positive_reviews


    # most_common_descriptions = (
    #     with_positive_reviews
    #     .group_by(["positive_reviews", "separated_description"])
    #     .agg(pl.count().alias("count"))
    #     .sort("count", descending=True)
    # )
    # most_common_descriptions

    # Top_10_materials=(
    #     with_positive_reviews
    #     .explode("separated_descriptions") #https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.explode.html#polars.DataFrame.explode
    #     .group_by("separated_descriptions","positive_reviews")
    #     .agg(pl.len().alias("count"))
    #     .sort("count",descending=True)
    #     .head(10)
    # )
    # Top_10_materials

    # top_positive = most_common_descriptions.filter(pl.col("points") == True).head(10)
    # top_negative = most_common_descriptions.filter(pl.col("points") == False).head(10)

    # top_positive, top_negative
    return


if __name__ == "__main__":
    app.run()
