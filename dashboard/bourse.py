import dash
from dash.dash_table.Format import Format, Scheme
import dash.dependencies as dde
from dash import html, dcc, dash_table, Input, Output, html, no_update, ALL
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
import pandas as pd
import sqlalchemy
from lib.constant import IS_DOCKER
from typing import Optional, Tuple
from datetime import date, timedelta, datetime
import plotly.graph_objects as go
import plotly.express as px
import plotly.subplots as ms

DATABASE_URI = (
    "timescaledb://ricou:monmdp@db:5432/bourse"
    if IS_DOCKER
    else "timescaledb://ricou:monmdp@localhost:5432/bourse"
)
engine = sqlalchemy.create_engine(DATABASE_URI)

app = dash.Dash(
    __name__,
    title="Bourse",
    suppress_callback_exceptions=True,
    external_stylesheets=[dbc.themes.ZEPHYR, dbc.themes.GRID],
)  # , external_stylesheets=external_stylesheets)

server = app.server

markets = pd.read_sql_query("SELECT * FROM markets", engine)
market_options = [
    {"label": row["name"], "value": row["id"]} for _, row in markets.iterrows()
]

companies = pd.read_sql_query("SELECT * FROM companies", engine)
companies_options = [
    {"label": row["name"], "value": row["id"]} for _, row in companies.iterrows()
]
companies_id_to_labels = {row["id"]: row["name"] for _, row in companies.iterrows()}
the_selected_companies = [
    # APPLE
    # 4534,
    # EURONEXPARIS
    # 4588,
]


@app.callback(
    [
        Output("bollinger-window-container", "style"),
        Output("bollinger-window", "disabled"),
    ],
    [Input("switch-bollinger-bands", "value")],
)
def toggle_bollinger_window(switch_value):
    if switch_value:
        return {"display": "block"}, False
    else:
        return {"display": "none"}, True


headerGraph = dbc.Row(
    [
        dbc.Col(html.Div(id="title-chart"), width=8),
        dbc.Col(
            dcc.DatePickerRange(
                id="my-date-picker-range",
                min_date_allowed=date(2019, 1, 1),
                max_date_allowed=date(2023, 12, 31),
                initial_visible_month=date(2019, 1, 1),
                start_date=date(2019, 1, 1),
                end_date=date(2023, 12, 31),
                # style={'float': 'right'},
            ),
            width=4,
            style={"text-align": "right"},
        ),
    ],
    justify="between",
    style={"align-items": "center"},
    class_name="p-4",
)


@app.callback(
    Output("title-chart", "children"),
    [
        Input("companies-dropdown", "value"),
        Input("market-dropdown", "value"),
    ],
)
def update_title_chart(companies_dropdown_value, market_dropdown_value):
    global the_selected_companies

    if not the_selected_companies:
        return ""

    company_id = the_selected_companies[0]

    market = pd.read_sql_query(
        f"SELECT * FROM markets WHERE id = (SELECT mid FROM companies WHERE id = {company_id})",
        engine,
    )

    company = pd.read_sql_query(
        f"SELECT * FROM companies WHERE id = {company_id}", engine
    )

    symbol = company.symbol.iloc[0]
    name = company.name.iloc[0]
    market_name = market.name.iloc[0]

    return f"{market_name} - {symbol} ({name})"


@app.callback(
    Output("selected-companies-output", "children"),
    [
        Input("companies-dropdown", "value"),
        Input("market-dropdown", "value"),
        Input("my-date-picker-range", "start_date"),
        Input("my-date-picker-range", "end_date"),
    ],
)
def update_selected_companies_output(
    selected_companies, selected_market, start_date, end_date
):
    filtered_companies = companies[
        (companies["id"].isin(selected_companies))
        & (companies["mid"].isin(selected_market))
    ]
    if len(selected_market) == 0:
        filtered_companies = companies[(companies["id"].isin(selected_companies))]
    if len(selected_companies) == 0:
        filtered_companies = companies[companies["mid"].isin(selected_market)]

    if len(filtered_companies) == 0:
        return html.Div("No companies selected", style={"text-align": "center", "color": "gray"}, className="mt-4")
    company_ids = filtered_companies["id"].tolist()
    columns = [
        {"name": "name", "id": "name"},
    ]
    if len(selected_companies) > 0:
        stock_data = get_stock_data(engine, start_date, end_date, company_ids)
        print("stock_data1: ", stock_data)
        stock_data = calculate_price_change_percentage(stock_data)
        print("stock_data2: ", stock_data)
        columns = [
            {"name": "name", "id": "name"},
            {
                "name": "perc %",
                "id": "price_change_percentage",
                "type": "numeric",
                "format": {
                    "specifier": "$,.1f",
                    "locale": {"symbol": ["", "%"]},
                },
            },
        ]

    # Fusionner les données des actions avec les informations des entreprises
    merged_data = (
        filtered_companies.merge(stock_data, left_on="id", right_on="cid", how="inner")
        if len(selected_companies) > 0
        else filtered_companies
    )
    print("merged_data: ", merged_data)

    return dash_table.DataTable(
        id="tbl",
        hidden_columns=["color"],
        data=merged_data.to_dict("records"),
        columns=columns,
        page_current=0,
        sort_action="native",
        page_size=20,
        style_cell={
            "color": "white",
    
        },
        style_header={
            "backgroundColor": "#2E2E33",
            "fontWeight": "bold",
            "color": "white",
        },
        css=[
            {
                "selector": ".dash-spreadsheet-menu",
                "rule": "position:absolute;bottom:-30px",
            },
            {"selector": ".show-hide", "rule": "font-family:Impact"},
        ],
        style_data_conditional=[
            {
                "if": {"filter_query": "{price_change_percentage} > 0"},
                "column_id": "price_change_percentage",
                "color": "#47BB78",
            },
            {
                "if": {"filter_query": "{price_change_percentage} < 0"},
                "column_id": "price_change_percentage",
                "color": "#F56565",
            },


        ],
    )


# BETWEEN au lieu de IN pour avoir le last et first
def get_stock_data(engine, start_date, end_date, company_ids):
    query = f"""
    WITH min_max_dates AS (
    SELECT 
        cid, 
        MIN("date") AS min_date, 
        MAX("date") AS max_date
    FROM 
        daystocks
    WHERE 
        "date" BETWEEN '{start_date}' AND '{end_date}'
        AND cid IN ({','.join(map(str, company_ids))})
    GROUP BY 
        cid
    )
    SELECT 
        ds.*
    FROM 
        daystocks ds
    JOIN 
        min_max_dates mmd 
        ON ds.cid = mmd.cid 
        AND (ds.date = mmd.min_date OR ds.date = mmd.max_date)
    ORDER BY 
        date
    """
    df = pd.read_sql_query(query, engine)
    return df


def calculate_price_change_percentage(df):
    df["price_change_percentage"] = df.groupby("cid")["close"].transform(
        lambda x: ((x.iloc[-1] - x.iloc[0]) / x.iloc[0]) * 100
    )
    # Conserver uniquement la première occurrence de chaque 'cid'
    df_unique = df.drop_duplicates(subset="cid", keep="first")
    return df_unique


@app.callback(
    Output("companies-dropdown", "value"),
    [State("companies-dropdown", "value")],
    Input("tbl", "active_cell"),
)
def move_company_to_top(selected_companies, active_cell):
    print("active_cell", active_cell)
    global the_selected_companies
    if active_cell:
        clicked_company_index = active_cell["row_id"] if active_cell else 0
        if len(the_selected_companies) == 0:
            the_selected_companies.append(clicked_company_index)
        else:
            the_selected_companies[0] = clicked_company_index
    return selected_companies


@app.callback(
    Output({"type": "company", "index": ALL}, "style"),
    [Input({"type": "company", "index": ALL}, "n_clicks")],
    [State({"type": "company", "index": ALL}, "id")],
)
def update_company_highlight(n_clicks, company_ids):
    if n_clicks is None:
        raise dash.exceptions.PreventUpdate

    style_list = [{"color": "white", "cursor": "pointer"} for _ in range(len(n_clicks))]
    if any(n_clicks):
        clicked_company_index = n_clicks.index(1)
        style_list[clicked_company_index] = {"color": "red", "cursor": "pointer"}

    return style_list


graph = dbc.Row(
    [
        dbc.Col(
            dcc.Graph(
                id="graph",
                style={"height": "80vh"},
                config={"displayModeBar": False, "scrollZoom": True},
                figure={
                    "layout": {
                        "template": "plotly_dark",
                        "plot_bgcolor": "rgba(30, 0, 0, 0)",
                        "paper_bgcolor": "rgba(30, 0, 0, 0)",
                    }
                },
            )
        )
    ]
)

end = dbc.Col(
    [
        # html.Div(style={"padding": "2px"}),  # faut automatiser le padding
        dcc.Dropdown(
            id="market-dropdown",
            options=market_options,
            value=[],
            multi=True,
            placeholder="Select market(s)",
            className="custom-dropdown m-2",
            style={"color": "black"},
        ),
        # html.Div(style={"padding": "2px"}),  # faut automatiser le padding
        dcc.Dropdown(
            options=companies_options,
            value=[],
            id="companies-dropdown",
            multi=True,
            placeholder="Select company(ies)",
            className="custom-dropdown m-2",
            style={"color": "black"},
        ),
        # html.Div(style={"padding": "10px"}),  # faut automatiser le padding
        dbc.Table(
                id="selected-companies-output", style={"margin": "auto", "display": "block"}
            )
    ],
    width=2,
    style={"align-items": "center"},
)

tab_companies = dbc.Row(
    [
        dbc.Col(
            dbc.Table(
                id="table-container", style={"margin": "auto", "display": "block"}
            )
        )
    ]
)


preference_settings = html.Div(
    [
        html.Div(
            [
                html.Div(
                    dcc.Link(
                        dbc.Col(
                            html.Img(src="assets/its.png", style={"width": "100%"}),
                            width=8,
                            class_name="my-4",
                        ),
                        href="#",
                    ),
                    id="open",
                ),
                dbc.Label("Preference Settings", className="h5"),
                dbc.Form(
                    [
                        dbc.Switch(
                            id="switch-log",
                            label="Logarithmic",
                            value=False,
                            style={"color": "white"},
                        ),
                        dbc.Switch(
                            id="switch-volume",
                            label="Volume",
                            value=False,
                            style={"color": "white"},
                        ),
                        dbc.Switch(
                            id="switch-bollinger-bands",
                            label="Bollinger Bands",
                            value=False,
                            style={"color": "white"},
                        ),
                        html.Div(
                            [
                                dbc.Label(
                                    "Bollinger Bands Window Size:",
                                    style={"color": "white"},
                                ),
                                dbc.Input(
                                    type="number",
                                    id="bollinger-window",
                                    min=1,
                                    step=1,
                                    value=20,
                                    disabled=True,
                                ),
                            ],
                            id="bollinger-window-container",
                            style={"display": "none"},
                        ),
                        html.Div(
                            [
                                dbc.Label(
                                    "Chart Type", style={"color": "white"}
                                ),
                                dbc.RadioItems(
                                    options=[
                                        {
                                            "label": "Candle Stick",
                                            "value": "Candle Stick",
                                        },
                                        {"label": "Line", "value": "Line"},
                                    ],
                                    value="Line",
                                    id="chart-type",
                                    inline=True,
                                    switch=True,
                                    style={"color": "white"},
                                    className="mt-2",
                                ),
                            ],
                            className="mt-3",
                        ),
                    ]
                ),
            ],
            className="m-4",
        )
    ]
)


@app.callback(
    Output("modal", "is_open"),
    [Input("open", "n_clicks")],
    [State("modal", "is_open")],
)
def toggle_modal(n, is_open):
    if n:
        return not is_open
    return is_open


app.layout = html.Div(
    [
        html.Script("document.documentElement.setAttribute('data-bs-theme', 'dark');"),
        dbc.Modal(
            [
                dbc.ModalBody(
                    html.Img(src="/assets/saul_anim.gif", style={"width": "100%"})
                ),
            ],
            id="modal",
        ),
        html.Div(
            [
                dbc.Col(preference_settings, width=2),
                dbc.Col(
                    html.Div([headerGraph, graph, tab_companies]),
                    width=8,
                    id="center-section",
                ),
                end,
            ],
            className="mb-3 d-flex",
            id="layout",
        ),
    ]
)


def create_candlestick_trace(df, label):
    return go.Candlestick(
        x=df["date"],
        open=df["open"],
        high=df["high"],
        low=df["low"],
        close=df["close"],
        name=f"{label}",
    )


def create_bands_trace(df):
    upper_trace = go.Scatter(
        x=df["date"],
        y=df["Upper"],
        mode="lines",
        name="Bande Supérieure",
        line=dict(color="rgba(33, 150, 243, 0.5)"),
    )
    lower_trace = go.Scatter(
        x=df["date"],
        y=df["Lower"],
        mode="lines",
        name="Bande Inférieure",
        line=dict(color="rgba(33, 150, 243, 0.5)"),
    )
    zone_trace = go.Scatter(
        x=df["date"].tolist() + df["date"].tolist()[::-1],
        y=df["Upper"].tolist() + df["Lower"].tolist()[::-1],
        fill="toself",
        fillcolor="rgba(33, 150, 243, 0.1)",
        line=dict(color="rgba(255, 255, 255, 0)"),
        name="Zone entre les bandes",
    )
    # Adding the average trace
    average_trace = go.Scatter(
        x=df["date"],
        y=df["MA"],
        mode="lines",
        name="Moyenne",
        line=dict(color="rgba(255, 109, 1, 0.5)"),
    )
    return upper_trace, lower_trace, zone_trace, average_trace


def create_volume_trace(df, label):
    df["change"] = df["close"] - df["open"]
    colors = ["#47BB78" if change > 0 else "#F56565" for change in df["change"]]
    return go.Bar(
        x=df["date"], y=df["volume"], marker_color=colors, name=f"Volume {label}"
    )


@app.callback(
    Output("graph", "figure"),
    [
        Input("companies-dropdown", "value"),
        Input("my-date-picker-range", "start_date"),
        Input("my-date-picker-range", "end_date"),
        Input("chart-type", "value"),
        Input("switch-log", "value"),
        Input("switch-volume", "value"),
        Input("switch-bollinger-bands", "value"),
        Input("bollinger-window", "value"),
    ],
)
def update_companies_chart(
    values: Optional[list[int]],
    start_date: Optional[str],
    end_date: Optional[str],
    chart_type: str = "Line",
    log_y: bool = False,
    volume: bool = False,
    bollinger_bands: bool = False,
    bollinger_window: Optional[int] = None,
):

    if volume:
        fig = ms.make_subplots(
            rows=2,
            cols=1,
            shared_xaxes=True,
            vertical_spacing=0.05,
            row_heights=[0.67, 0.33],
        )
    else:
        fig = ms.make_subplots(rows=1, cols=1, shared_xaxes=True)

    fig.update_layout(
        template="plotly_dark",
        plot_bgcolor="rgba(0, 0, 0, 0)",
        paper_bgcolor="rgba(0, 0, 0, 0)",
    )
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor="#444", zerolinecolor="#444")
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor="#444", zerolinecolor="#444")
    fig.update_layout(xaxis_rangeslider_visible=False)
    fig.update_layout(
        xaxis=dict(
            rangeselector=dict(
                buttons=[
                    dict(count=1, label="1m", step="month", stepmode="todate"),
                    dict(count=6, label="6m", step="month", stepmode="todate"),
                    dict(count=1, label="YTD", step="year", stepmode="todate"),
                    dict(step="all"),
                ]
            ),
            type="date",
        ),
        dragmode="pan",
        xaxis_rangeselector_font_color="white",
        xaxis_rangeselector_activecolor="#2E2E33",
        xaxis_rangeselector_bgcolor="#222",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )

    if not the_selected_companies:
        return fig
    if not start_date or not end_date:
        return fig
    end_date = str(datetime.fromisoformat(end_date) + timedelta(days=1))
    if chart_type == "Candle Stick":
        for i, value in enumerate(the_selected_companies):
            label = companies_id_to_labels.get(value, "")

            df = pd.read_sql_query(
                f"SELECT * FROM daystocks WHERE cid = {value} and date >= '{start_date}' and date < '{end_date}' ORDER by date",
                engine,
            )

            if i == 0:
                fig.add_trace(create_candlestick_trace(df, label))
            else:
                fig.add_trace(
                    go.Scatter(
                        x=df["date"], y=df["close"], mode="lines", name=f"{label}"
                    )
                )

            if bollinger_bands and i == 0:
                df["MA"] = df["close"].rolling(window=bollinger_window).mean()
                df["STD"] = df["close"].rolling(window=bollinger_window).std()
                df["Upper"] = df["MA"] + (df["STD"] * 2)
                df["Lower"] = df["MA"] - (df["STD"] * 2)
                upper_trace, lower_trace, zone_trace, average_trace = (
                    create_bands_trace(df)
                )
                fig.add_trace(upper_trace)
                fig.add_trace(lower_trace)
                fig.add_trace(zone_trace)
                fig.add_trace(average_trace)

    else:
        for i, value in enumerate(the_selected_companies):
            label = companies_id_to_labels.get(value, "")
            df = pd.read_sql_query(
                f"SELECT * FROM daystocks WHERE cid = {value} and date >= '{start_date}' and date < '{end_date}' ORDER by date",
                engine,
            )

            fig.add_trace(
                go.Scatter(x=df["date"], y=df["close"], mode="lines", name=f"{label}")
            )

            if bollinger_bands and i == 0:
                df["MA"] = df["close"].rolling(window=bollinger_window).mean()
                df["STD"] = df["close"].rolling(window=bollinger_window).std()
                df["Upper"] = df["MA"] + (df["STD"] * 2)
                df["Lower"] = df["MA"] - (df["STD"] * 2)
                upper_trace, lower_trace, zone_trace, average_trace = (
                    create_bands_trace(df)
                )
                fig.add_trace(upper_trace)
                fig.add_trace(lower_trace)
                fig.add_trace(zone_trace)
                fig.add_trace(average_trace)

    if log_y:
        fig.update_yaxes(type="log")

    if volume:
        value = the_selected_companies[0]
        label = companies_id_to_labels.get(value, "")
        df = pd.read_sql_query(
            f"SELECT * FROM daystocks WHERE cid = {value} and date >= '{start_date}' and date < '{end_date}' ORDER by date",
            engine,
        )
        fig.add_trace(create_volume_trace(df, label), row=2, col=1)
        fig.update_yaxes(title_text=f"Volume {label}", row=2, col=1)

    return fig


@app.callback(
    Output("table-container", "children"),
    [
        Input("companies-dropdown", "value"),
        Input("my-date-picker-range", "start_date"),
        Input("my-date-picker-range", "end_date"),
    ],
)
def update_table(selected_companies, start_date, end_date):
    if not the_selected_companies or not start_date or not end_date:
        return html.Table()

    dfs = []

    end_date = str(datetime.fromisoformat(end_date) + timedelta(days=1))

    for i, value in enumerate(the_selected_companies):
        label = companies_id_to_labels.get(value, "")
        df = pd.read_sql_query(
            f"SELECT * FROM daystocks WHERE cid = {value} and date >= '{start_date}' and date < '{end_date}' ORDER by date",
            engine,
        )
        # convertir la colonne 'date' en datetime (ne pas faire .strftime() sur la colonne 'date' si on est pas sur que cest une datetime)
        df["date"] = pd.to_datetime(df["date"])

        df["date"] = df["date"].dt.strftime("%Y-%m-%d")
        df["cid"] = label
        dfs.append(df)
    df = pd.concat(dfs)

    df.rename(columns={"cid": "name"}, inplace=True)

    # Prétraitement pour créer une nouvelle colonne 'color'
    df["color"] = df["close"].diff().apply(lambda x: "green" if x > 0 else "red")

    return dash_table.DataTable(
        id="table",
        hidden_columns=["color"],
        data=df.to_dict("records"),
        page_current=0,
        sort_action="native",  # Enable sorting on all sortable columns
        page_size=10,
        # style_cell={
        #     "textAlign": "left",
        #     "color": "white",
        #     "backgroundColor": "#1D1D22",
        #     "width": "100px",
        #     "minWidth": "100px",
        #     "maxWidth": "100px",
        #     "whiteSpace": "normal",
        #     "border": "1px solid #2E2E33",
        # },
        style_header={
            "backgroundColor": "#2E2E33",
            "fontWeight": "bold",
            "color": "white",
        },
        css=[
            {
                "selector": ".dash-spreadsheet-menu",
                "rule": "position:absolute;bottom:-30px",
            },  # move below table
            {"selector": ".show-hide", "rule": "font-family:Impact"},  # change font
        ],
        style_data_conditional=[
            {
                "if": {"filter_query": '{color} eq "green"'},
                "column_id": "!date",
                "color": "#47BB78",
            },
            {
                "if": {"filter_query": '{color} eq "red"'},
                "color": "#F56565",
                "column_id": "!date",
            },
        ],
    )


@app.callback(
    Output("companies-dropdown", "options"), [Input("market-dropdown", "value")]
)
def update_companies_options(selected_markets):
    if not selected_markets:
        return companies_options
    else:
        filtered_companies = companies[companies["mid"].isin(selected_markets)]
        filtered_options = [
            {"label": row["name"], "value": row["id"]}
            for _, row in filtered_companies.iterrows()
        ]
        return filtered_options


if __name__ == "__main__":
    app.run(debug=True)
