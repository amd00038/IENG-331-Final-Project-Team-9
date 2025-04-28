

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
    mo.md(r"""### Conclusion: The number of points predicts the price of the wine based on the points of the wine shown above in the figure "Price of Wine vs Points Correlation". The graph shows a small expenential growth of the range of prices for prices up to 800. By creating a range to 800, the graph provides a clearer visual of the relation. For example, from points 80-90 there is a small range in prices. The minimum price for points 80-90 in from 32-90 dollars where as for points 90-100, the minimum price ranges from 80-90 dollars. The max price from 80-90 ranges from 69-510 dollars and from points 90-100 the max range is from 510-1500 dollars. The box plots shows an ezpential growth for the medium values for each point. """)
    return


if __name__ == "__main__":
    app.run()
