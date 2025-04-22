import marimo

__generated_with = "0.10.15"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import plotly as px
    return mo, pl, px


if __name__ == "__main__":
    app.run()
