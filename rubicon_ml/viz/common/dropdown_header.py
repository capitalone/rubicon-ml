import dash_bootstrap_components as dbc
from dash import html


def dropdown_header(dropdown_items, selected_item, left_header_text, right_header_text, id_prefix):
    dropdown_menu = dbc.DropdownMenu(
        [
            dbc.DropdownMenuItem(
                item,
                id={
                    "type": f"{id_prefix}-dropdown-button",
                    "index": item,
                },
            )
            for item in dropdown_items
        ],
        color="secondary",
        id=f"{id_prefix}-dropdown",
        label=selected_item,
        size="lg",
    )

    return dbc.Row(
        [
            dbc.Col(
                html.H5(left_header_text, className="header-text"),
                className="header-left-col",
                id=f"{id_prefix}-header-left-col",
                width="auto",
            ),
            dbc.Col(
                dropdown_menu,
                className="header-dropdown-col",
                id=f"{id_prefix}-header-dropdown-col",
                width="auto",
            ),
            dbc.Col(
                html.H5(
                    right_header_text,
                    className="header-text",
                    id=f"{id_prefix}-header-right-text",
                ),
                className="header-right-col",
                id=f"{id_prefix}-header-right-col",
            ),
        ],
        className="header-row",
        id=f"{id_prefix}-header-row",
    )
