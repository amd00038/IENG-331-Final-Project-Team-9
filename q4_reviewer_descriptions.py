

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
    return (pl,)


@app.cell
def _(pl):
    descriptions=pl.read_parquet("pipeline/description.parquet")
    descriptions

    descriptions_normalized=descriptions.with_columns([
            pl.col("description")
            .str.to_lowercase()
            .str.strip_chars()
            .alias("description")
    ])
    descriptions_normalized
    return (descriptions_normalized,)


@app.cell
def _():
    return


@app.cell
def _(descriptions_normalized, pl):
    with_positive_reviews = descriptions_normalized.with_columns(
        positive_reviews=pl.when((pl.col("points")>=90))
        .then(pl.col("points"))
        .otherwise(None)
                
    )
    with_positive_reviews



    # Top_10_materials=(
    #     description_separated
    #     #.explode("separated_description") #https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.explode.html#polars.DataFrame.explode
    #     .group_by("separated_description", "positive_reviews")
    #     .agg(pl.len().alias("count"))
    #     .sort("count",descending=True)
    #     .group_by("positive_reviews")
    #     .head(10)
    #)
    #Top_10_materials
    # description_separated = description_points.with_columns([
    #         pl.col("description").str.replace(", and", ", ")
    #         .str.replace(" and ", ", ")
    #         .str.replace(",,",", ")
    #         .str.replace(",",", ")
    #         .str.replace(" but ",", ")
    #         .str.replace(" pencil on paper" ,"pencil on paper")
    #         .str.split(", ")
    #         .alias("Materials")


    return (with_positive_reviews,)


@app.cell
def _(pl, with_positive_reviews):
    positive_grouped=(
        with_positive_reviews.group_by("positive_reviews")
        .agg(pl.col("description").arr.join(" "))
    )

    description_separated = positive_grouped.with_columns([
        pl.col("description")
        .str.replace(", and", ", ")
        .str.replace(" and ", ", ")
        .str.replace(",,",", ")
        .str.replace(",",", ")
        .str.replace(" but ",", ")
        .str.replace(" this " ,", ")
        .str.replace(" is ",", ")
        .str.replace(" a ",", ")
        .str.replace(" to the ",", ")
        .str.split(", ")
        .alias("separated_description")
    ])
    description_separated

    # positive_separated=(
    #     positive_grouped
    #     .explode("description")
    #     .group_by("Description")
    #     .agg(pl.len().alias("count"))
    #     .sort("count",descending=True)
    #     .group_by("positive_reviews")
    #     .head(10)


    #stop_words_pattern=r"\b(the|is|as|a|an|and|or|in|of|to|from|with|on|at|this|that|it|for|by|be|are)\b"

    #positive_grouped = positive_grouped.with_columns(
        #pl.col("description")
        #.str.replace_all(", ", " ").str.split(" ")
    
    #)
    #positive_grouped
    #popular_words = positive_grouped.with_columns(pl.col("description").when(r'\b(this|is|as)\b'))
    return


if __name__ == "__main__":
    app.run()
