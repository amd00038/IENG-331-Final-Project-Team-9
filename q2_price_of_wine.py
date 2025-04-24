

import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import plotly.express as px
    import polars.selectors as cs
    return mo, pl, px


@app.cell
def _(mo):
    mo.md(r"""### Question: Does the number of points predict the price of the wine? If so, how strong is the correlation?""")
    return


@app.cell
def _(pl):
    winedata = (
            pl.read_csv("data/winemag-data.csv", try_parse_dates=True))
    winedata
    return (winedata,)


@app.cell
def _(pl, winedata):
    result = winedata.select(pl.col("price", "points"))

    result
    return


@app.cell
def _(px, winedata):
    fig = px.scatter(
        winedata, 
        x="points", 
        y="price",
        title="Price of Wine vs Points Correlation"
    )
    fig
    return


@app.cell
def _(mo):
    mo.md(r"""### Conclusion: The number of points does not predict the price of wine, as shown in the figure above, "Price of Wine vs Points Correlation". When color-coded using the ID, there is a clear visual that each point had a wide range of prices. For example, point 95 lowest price, is twenty dollars, and the highest price is nine hundred seventy-three dollars. There is a correlation between the range of prices versus each point. As shown above, points 80 through 86 do not have a wide price range compared to points 92 through 100. For this reason, the is a strong correlation in the fact that the higher the points, the wider the price range is for wine. """)
    return


@app.cell
def _(winedata):
    winedata.group_by("points","price").max() # shows max value for each group and column
    winedata.group_by("points","price").min() # shows min value for each group and column
    return


@app.cell
def _(winedata):
    maxvalue = winedata.group_by(["price", "points"])

    maxvalue.max()
    return


@app.cell
def _(winedata):
    minvalue = winedata.group_by(["price", "points"])

    minvalue.min()
    return


if __name__ == "__main__":
    app.run()
