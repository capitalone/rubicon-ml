import plotly.express as px

dark_blue = "rgb(40, 60, 70)"
light_blue = "rgb(200, 230, 250)"
plot_background_blue = "rgb(240, 250, 255)"
transparent = "rgba(255, 255, 255, 0)"


def get_rubicon_colorscale(num_colors, low=0.33):
    if num_colors < 2:
        num_colors = 2

    return px.colors.sample_colorscale(
        "Blues",
        num_colors,
        low=low,
    )
