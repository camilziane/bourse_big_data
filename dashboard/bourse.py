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

external_stylesheets = [
    dbc.themes.ZEPHYR,
    dbc.themes.GRID,
    "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css",
]

app = dash.Dash(
    __name__,
    title="Bourse",
    suppress_callback_exceptions=True,
    external_stylesheets=external_stylesheets,
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
    4570,
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


@app.callback(
    [
        Output("ma-window-container", "style"),
        Output("ma-window", "disabled"),
    ],
    [Input("switch-ma", "value")],
)
def toggle_ma_window(switch_value):
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
    Output("compared-companies-dropdown", "value"),
    [Input("compared-companies-dropdown", "value")],
)
def add_new_companies_to_chart(compared_companies):
    global the_selected_companies
    if len(the_selected_companies) == 0:
        return compared_companies
    main_companie = the_selected_companies[0]
    the_selected_companies = [
        companie
        for companie in the_selected_companies
        if companie in compared_companies
    ]
    the_selected_companies.insert(0, main_companie)
    for companie in compared_companies:
        if (the_selected_companies[0] != companie) and (
            companie not in the_selected_companies
        ):
            the_selected_companies.append(companie)

    # delete elements that are not in the compared_companies list except the first element
    return compared_companies


@app.callback(
    Output("title-chart", "children"),
    [
        Input("the_selected_companies", "children"),
    ],
)
def update_title_chart(div_null):
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

    is_peapme = company.pea.iloc[0]

    peapme_badge = None

    if is_peapme:
        peapme_badge = dbc.Badge(
            "pea/pme",
            text_color="white",
            className="border me-1",
        )

    return html.Span(
        [
            html.B(f"{name}"),
            f" | ({symbol})",
            html.Br(),
            html.Div(
                [
                    dbc.Badge(
                        f"{market_name}",
                        color="dark",
                        text_color="white",
                        className="border me-1",
                    ),
                    peapme_badge,
                ],
                id="badges",
            ),
        ]
    )


@app.callback(
    Output("selected-companies-output", "children"),
    [
        Input("companies-dropdown", "value"),
        Input("market-dropdown", "value"),
        Input("my-date-picker-range", "start_date"),
        Input("my-date-picker-range", "end_date"),
        Input("switch-peapme", "value"),
    ],
)
def update_selected_companies_output(
    selected_companies, selected_market, start_date, end_date, switch_peapme
):
    filtered_companies = companies[
        (companies["id"].isin(selected_companies))
        & (companies["mid"].isin(selected_market))
        & (companies["pea"] == switch_peapme)
    ]

    if len(selected_market) == 0:
        filtered_companies = companies[
            (companies["id"].isin(selected_companies))
            & (companies["pea"] == switch_peapme)
        ]
    if len(selected_companies) == 0:
        filtered_companies = companies[
            companies["mid"].isin(selected_market) & (companies["pea"] == switch_peapme)
        ]

    if len(filtered_companies) == 0:
        return html.Div(
            "No companies selected",
            style={"text-align": "center", "color": "gray"},
            className="mt-4",
        )
    company_ids = filtered_companies["id"].tolist()
    columns = [
        {"name": "name", "id": "name"},
    ]
    if True:
        stock_data = get_stock_data(engine, start_date, end_date, company_ids)
        stock_data = calculate_price_change_percentage(stock_data)
        columns = [
            {"name": "name", "id": "name"},
            {
                "name": "%",
                "id": "price_change_percentage",
                "type": "numeric",
                "format": {
                    "specifier": "$,.1f",
                    "locale": {"symbol": ["", "%"]},
                },
            },
        ]

    # Fusionner les donnÃ©es des actions avec les informations des entreprises
    merged_data = (
        filtered_companies.merge(stock_data, left_on="id", right_on="cid", how="inner")
        if True
        else filtered_companies
    )

    return dash_table.DataTable(
        id="tbl",
        hidden_columns=["color"],
        data=merged_data.to_dict("records"),
        columns=columns,
        page_current=0,
        sort_action="native",
        page_size=20,
        style_cell={
            "color": "#cacaca",
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
                "if": {
                        "column_id": "price_change_percentage",
                        "filter_query": "{price_change_percentage} > 0"
                    },
                "color": "#47BB78",
            },
            {
                "if": {
                        "column_id": "price_change_percentage",
                        "filter_query": "{price_change_percentage} < 0"
                    },
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
    query2 = f"""
    SELECT l.*
    FROM companies c,
    LATERAL (
        SELECT ds.*
        FROM daystocks ds
        WHERE ds.cid = c.id  
        AND ds.date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY ds.date ASC  
        LIMIT 1
    ) l
    WHERE c.id IN ({','.join(map(str, company_ids))})
    """
    query3 = f"""
    SELECT l.*
    FROM companies c,
    LATERAL (
        SELECT ds.*
        FROM daystocks ds
        WHERE ds.cid = c.id  
        AND ds.date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY ds.date DESC  
        LIMIT 1
    ) l
    WHERE c.id IN ({','.join(map(str, company_ids))})
    """
    import concurrent.futures

    def execute_query(query):
        return pd.read_sql_query(query, engine)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        query2_future = executor.submit(execute_query, query2)
        query3_future = executor.submit(execute_query, query3)

    df = query2_future.result()
    df2 = query3_future.result()
    df = pd.concat([df, df2])
    return df


def calculate_price_change_percentage(df):
    df["price_change_percentage"] = df.groupby("cid")["close"].transform(
        lambda x: ((x.iloc[-1] - x.iloc[0]) / x.iloc[0]) * 100
    )

    # Arrondir le pourcentage à deux chiffres après la virgule
    df["price_change_percentage"] = df["price_change_percentage"].round(2)
    # Conserver uniquement la premiÃ¨re occurrence de chaque 'cid'
    df_unique = df.drop_duplicates(subset="cid", keep="first")
    return df_unique


@app.callback(
    Output("the_selected_companies", "children"),
    [State("the_selected_companies", "children")],
    Input("tbl", "active_cell"),
)
def move_company_to_top(div_null, active_cell):
    global the_selected_companies
    if active_cell:
        clicked_company_index = active_cell["row_id"] if active_cell else 0
        if len(the_selected_companies) == 0:
            the_selected_companies.append(clicked_company_index)
        else:
            the_selected_companies[0] = clicked_company_index
    return div_null


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
        html.Div(
            dbc.Switch(
                id="switch-peapme",
                label="PEA/PME",
                value=False,
                className="m-0",
                style={"color": "white"},
            ),
            className="mx-2",
        ),
        # html.Div(style={"padding": "10px"}),  # faut automatiser le padding
        dbc.Table(
            id="selected-companies-output",
            className="custom-table-companies",
            style={"margin": "auto", "display": "block"},
        ),
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
                html.Div([
                    dbc.Label("Preference Settings", className="h5"),
                ]),
                html.Div([
                    dbc.Label("Indicators", className="h6"),
                ]),
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
                                    "BB Window Size",
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
                            className="mb-3",
                            style={"display": "none"},
                        ),
                        dbc.Switch(
                            id="switch-ma",
                            label="Moving Average",
                            value=False,
                            style={"color": "white"},
                        ),
                        html.Div(
                            [
                                dbc.Label(
                                    "MA Window Size",
                                    style={"color": "white"},
                                ),
                                dbc.Input(
                                    type="number",
                                    id="ma-window",
                                    min=1,
                                    step=1,
                                    value=20,
                                    disabled=True,
                                ),
                            ],
                            id="ma-window-container",
                            style={"display": "none"},
                        ),

                        html.Div(
                            [
                                dbc.Label("Chart Type", style={"color": "white"}),
                                dbc.RadioItems(
                                    options=[
                                        {
                                            "label": "Candle Stick",
                                            "value": "Candle Stick",
                                        },
                                        {
                                            "label": "Line (Daily)",
                                            "value": "Line (Daily)",
                                        },
                                        {"label": "Line", "value": "Line"},
                                    ],
                                    value="Line (Daily)",
                                    id="chart-type",
                                    inline=True,
                                    switch=True,
                                    style={"color": "white"},
                                    # className="mt-2",
                                ),
                            ],
                            className="mt-3",
                        ),
                        html.Div(
                            [
                                dbc.Label("Compare with"),
                                dcc.Dropdown(
                                    id="compared-companies-dropdown",
                                    options=companies_options,
                                    value=[],
                                    multi=True,
                                    placeholder="Select company(ies)",
                                    className="custom-dropdown",
                                    style={"color": "black"},
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
    [Output('chart-type', 'options'),
     Output('chart-type', 'value')],
    [Input('compared-companies-dropdown', 'value')],
    [State('chart-type', 'value')]
)
def update_chart_type_options(selected_companies, current_value):
    options = [
        {"label": "Candle Stick", "value": "Candle Stick", "disabled": False},
        {"label": "Line (Daily)", "value": "Line (Daily)", "disabled": False},
        {"label": "Line", "value": "Line", "disabled": False},
    ]
    if selected_companies:
        options[0]["disabled"] = True
        if current_value == "Candle Stick":
            current_value = "Line (Daily)"
    return options, current_value

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
        html.Div(id="the_selected_companies", style={"display": "none"}),
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
        name="Upper Band",
        line=dict(color="rgba(33, 150, 243, 0.5)"),
    )
    lower_trace = go.Scatter(
        x=df["date"],
        y=df["Lower"],
        mode="lines",
        name="Lower Band",
        line=dict(color="rgba(33, 150, 243, 0.5)"),
    )
    zone_trace = go.Scatter(
        x=df["date"].tolist() + df["date"].tolist()[::-1],
        y=df["Upper"].tolist() + df["Lower"].tolist()[::-1],
        fill="toself",
        fillcolor="rgba(33, 150, 243, 0.1)",
        line=dict(color="rgba(255, 255, 255, 0)"),
        name="Middle Band",
    )
    # Adding the average trace
    average_trace = go.Scatter(
        x=df["date"],
        y=df["MA"],
        mode="lines",
        name="Mean",
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
        Input("the_selected_companies", "children"),
        Input("my-date-picker-range", "start_date"),
        Input("my-date-picker-range", "end_date"),
        Input("chart-type", "value"),
        Input("switch-log", "value"),
        Input("switch-volume", "value"),
        Input("switch-bollinger-bands", "value"),
        Input("bollinger-window", "value"),
        Input("compared-companies-dropdown", "value"),
        Input("switch-ma", "value"),
        Input("ma-window", "value"),
    ],
)
def update_companies_chart(
    div_null: Optional[list[int]],
    start_date: Optional[str],
    end_date: Optional[str],
    chart_type: str = "Line",
    log_y: bool = False,
    volume: bool = False,
    bollinger_bands: bool = False,
    bollinger_window: Optional[int] = None,
    compared_companies_dropdown: Optional[list[int]] = None,
    ma=False,
    ma_window: Optional[int]=None,
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
    from_source = (
        "daystocks"
        if chart_type == "Candle Stick" or chart_type == "Line (Daily)"
        else "stocks"
    )
    column_value = (
        "close"
        if chart_type == "Candle Stick" or chart_type == "Line (Daily)"
        else "value"
    )
    for i, value in enumerate(the_selected_companies):
        label = companies_id_to_labels.get(value, "")

        df = pd.read_sql_query(
            f"SELECT * FROM {from_source} WHERE cid = {value} and date >= '{start_date}' and date < '{end_date}' ORDER by date",
            engine,
        )

        if len(the_selected_companies) > 1:
            df[column_value] = (df[column_value] - df[column_value].iloc[0]) / df[
                column_value
            ].iloc[0]
            fig.update_layout(yaxis1_tickformat=",.1%")

            # fig.update_yaxes(title_text=f"Percentage {label}", row=1, col=1)

        if i == 0 and chart_type == "Candle Stick":
            fig.add_trace(create_candlestick_trace(df, label))
        else:
            fig.add_trace(
                go.Scatter(
                    x=df["date"], y=df[column_value], mode="lines", name=f"{label}"
                )
            )

        if bollinger_bands and i == 0:
            df["MA"] = df[column_value].rolling(window=bollinger_window).mean()
            df["STD"] = df[column_value].rolling(window=bollinger_window).std()
            df["Upper"] = df["MA"] + (df["STD"] * 2)
            df["Lower"] = df["MA"] - (df["STD"] * 2)
            upper_trace, lower_trace, zone_trace, average_trace = create_bands_trace(df)
            fig.add_trace(upper_trace)
            fig.add_trace(lower_trace)
            fig.add_trace(zone_trace)
            fig.add_trace(average_trace)
            # Add SMA
        if ma and i == 0:
            df["SMA"] = df[column_value].rolling(window=ma_window).mean()
            mean_trace = average_trace = go.Scatter(
                x=df["date"],
                y=df["SMA"],
                mode="lines",
                name="MA",
                line=dict(color="rgba(255, 99, 72,1)"),
            )
            fig.add_trace(mean_trace)

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
        Input("the_selected_companies", "children"),
        Input("my-date-picker-range", "start_date"),
        Input("my-date-picker-range", "end_date"),
    ],
)
def update_table(div_null, start_date, end_date):
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

    # PrÃ©traitement pour crÃ©er une nouvelle colonne 'color'
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
    app.run(host="0.0.0.0", debug=True)
