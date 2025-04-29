

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
    return mo, pl, px


@app.cell
def _():
    #Read in data
    return


@app.cell
def _(pl):
    descriptions=pl.read_parquet("pipeline/description.parquet")
    descriptions.head()
    return (descriptions,)


@app.cell
def _(mo):
    mo.md("""#Normalize data""")
    return


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
def _(mo):
    mo.md("""# Positive Reviews""")
    return


@app.cell
def _(descriptions_normalized, pl):
    positive_df=descriptions_normalized.filter(pl.col("points") >= 90)
    positive_df.head()
    return (positive_df,)


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
    Top_10_positive_descriptions=(
        positive_separated
        .explode("separated_description") #https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.explode.html#polars.DataFrame.explode
        .group_by("separated_description")
        .agg(pl.len().alias("count"))
        .sort("count",descending=True)
        .head(10)
    )
    Top_10_positive_descriptions
    return (Top_10_positive_descriptions,)


@app.cell
def _(Top_10_positive_descriptions, px):
    positive_bar=px.bar(
            Top_10_positive_descriptions,
            x="separated_description",
            y="count",
            title="Top 10 Descriptive Words for Positive Reviews"
        )
    positive_bar

    return


@app.cell
def _(mo):
    mo.md("""#### There is a decrease in the count of the most common words. Licorice is used significantly more than vanilla, although they are both considered to be in the top 10 most used words after filler words are removed. This decrease seems to level out and flatten. We can see that in this graph and the leveling out continues as more words are added. """)
    return


@app.cell
def _(mo):
    mo.md("""#Negative Reviews""")
    return


@app.cell
def _(descriptions_normalized, pl):
    negative_df=descriptions_normalized.filter(pl.col("points") < 90)
    negative_df.head()
    return (negative_df,)


@app.cell
def _(negative_df, pl):
    negative_separated = negative_df.with_columns([
        pl.col("description")
        .str.replace(", and", "")
        .str.replace(" and", ",")
        .str.replace("and ", ",")
        .str.replace("and", ",")
        .str.replace(" and ", ", ")
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
        #.str.replace("a ","")
        #.str.replace(" a","")
        .str.replace(" a ","")
        .str.replace(" the ","")
        .str.replace("the ","")
        .str.replace(" the","")
        .str.replace("the","")
        .str.replace(" to ","")
        .str.replace("to ","")
        .str.replace(" to","")
        .str.replace("to","")
        # .str.replace(" of ","")
        # .str.replace("of ","")
        # .str.replace(" of","")
        # .str.replace("of","")
        # .str.replace(" by ","")
        # .str.replace("by ","")
        # .str.replace(" by","")
        # .str.replace("by","")
        # .str.replace(" big","")
        # .str.replace("big,","")
        # .str.replace("big ","")
        .str.replace(" lear","")
        .str.replace(" to the "," ")
        #.str.replace(". ",",")
        #.str.replace(r"\s+", "")
        #.str.replace(r"\s+", " ")
        .str.replace(",",", ")
        .str.replace(" ","")
        .str.split(", ")
        .alias("separated_description")
    ])
    negative_separated
    return (negative_separated,)


@app.cell
def _(negative_separated, pl):
    Top_10_negative_descriptions=(
        negative_separated
        .explode("separated_description") #https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.explode.html#polars.DataFrame.explode
        .group_by("separated_description")
        .agg(pl.len().alias("count"))
        .sort("count",descending=True)
        .head(10)
    )
    Top_10_negative_descriptions
    return (Top_10_negative_descriptions,)


@app.cell
def _(Top_10_negative_descriptions, px):
    Negative_bar=px.bar(
            Top_10_negative_descriptions,
            x="separated_description",
            y="count",
            title="Top 10 Descriptive Words for Negative Reviews"
        )
    Negative_bar
    return


@app.cell
def _(mo):
    mo.md(
        """
        #### Counts for “negative” descriptive words were much higher than those for positive descriptive words. Most of these words are not negative despite being in the lower points range. From this we can assume that these wines were not terrible/bad wines, just less desirable than their counterparts. Can also assume that higher rating wines are more likely to be rich rather than ripe, and lower rating wines are more likely to be ripe rather than rich. Also shows flattening of the curve as we travel further right on the chart. Found that there are some words appearing in both positive and negative comments. (Ex. “ripe”, “vanilla”, cherry, “rich”. Rich appeared on both lists but appeared significantly higher on the list for positive reviews, even though the count was higher for this descriptor in the negative reviews list. Although these are descriptions from “negative” reviews, there don’t appear to be many negative words, more just neutral descriptors. Soft can be good or bad depending on the person.

        """
    )
    return


@app.cell
def _(mo):
    mo.md("""#Percentage of total of each point""")
    return


@app.cell
def _(descriptions_normalized, pl):
    Bottom_5=descriptions_normalized.group_by(pl.col("points")).agg(pl.col("id").len()).sort(by="points", descending=False)
    Bottom_5.head(5)
    return


@app.cell
def _(descriptions_normalized, pl):
    top_5=descriptions_normalized.group_by(pl.col("points")).agg(pl.col("id").len()).sort(by="points", descending=True)
    top_5.head(5)
    return


@app.cell
def _(descriptions_normalized, pl):
    points_df=descriptions_normalized.group_by(pl.col("points")).agg(pl.col("id").len()).sort(by="points", descending=False)
    return (points_df,)


@app.cell
def _(points_df, px):
    px.pie(points_df, values = "id", names = "points")
    return


@app.cell
def _(mo):
    mo.md(
        """
        ####Can see that the point rating with the highest percentage was point number 88. Over 60% of wines were rated in what we considered to be negative. This likely contributes to the increased number of descriptor counts in the negative group. Majority of wines rated in the negative range were (about 45 of the total ratings) were from 86-89. Contributes to why the “negative” descriptions are mostly not negative, but more neutral. Less than 1% of wines were above a point rating of 95. 


        """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
