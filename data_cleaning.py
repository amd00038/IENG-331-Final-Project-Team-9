import marimo

__generated_with = "0.10.15"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md(r"""## Cleaning Dataset""")
    return


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import plotly.express as px
    import plotly.graph_objects as go
    import polars.selectors as cs
    import re
    return cs, go, mo, pl, px, re


if __name__ == "__main__":
    app.run()
