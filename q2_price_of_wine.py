

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
    winedata = pl.read_parquet("pipeline/price.parquet")
    winedata.head()
    return (winedata,)


@app.cell
def _(pl, winedata):
    result = winedata.select(pl.col("price", "points"))
    result
    return


@app.cell
def _(mo):
    mo.md(r"""## Price of Wine vs Points Correlation""")
    return


@app.cell
def _(px, winedata):
    px.box(
        winedata, 
        x="points", 
        y="price",
        title="Price of Wine vs Points Correlation",range_y=[0,800])
    return


@app.cell
def _(mo):
    mo.md(r"""### Conclusion: The number of points predicts the price of wine based on the points shown above in the figure "Price of Wine vs Points Correlation". The graph shows small exponential growth as points increase up to 800. By creating a price range from 0-800, the graph provides a clearer visual of the relationship. For example, from points 80-90, there is a small price range. The minimum price for 80-90 points is 32-90 dollars, whereas for points 90-100, the minimum price ranges from 80-90 dollars. The max price from 80-90 ranges from 69-510 dollars, and from points 90-100 the max range is from 510-1500 dollars. The box plots show an exponential growth for the median values for each point and a larger variance as the points increase. """)
    return


if __name__ == "__main__":
    app.run()
