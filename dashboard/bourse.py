import dash
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
    external_stylesheets=[dbc.themes.DARKLY, dbc.themes.GRID],
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
    #APPLE
    4534,
    #EURONEXPARIS
    #4588,
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
    class_name="p-4"
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
        f"SELECT * FROM markets WHERE id = (SELECT mid FROM companies WHERE id = {company_id})", engine
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
    [Input("companies-dropdown", "value"),
     Input("market-dropdown", "value")]
)
def update_selected_companies_output(selected_companies, selected_market):
    if selected_companies:
        return html.Div(
            [
                html.Ul(
                    [
                        html.Li(
                            companies_id_to_labels.get(company_id, ""),
                            id={"type": "company", "index": i},
                            style={"color": "white", "cursor": "pointer"},
                            n_clicks=0
                        )
                        for i, company_id in enumerate(selected_companies)
                    ]
                )
            ]
        )
    elif selected_market:
        market_companies = companies[companies["mid"].isin(selected_market)]
        market_companies = market_companies[:20] #pour pas saturer le site
        return html.Div(
            [
                html.Ul(
                    [
                        html.Li(
                            row["name"],
                            id={"type": "company", "index": i},
                            style={"color": "white", "cursor": "pointer"},
                            n_clicks=0
                        )
                        for i, (_, row) in enumerate(market_companies.iterrows())
                    ]
                )
            ]
        )
    else:
        return html.Div()



@app.callback(
    Output("companies-dropdown", "value"),
    [Input({"type": "company", "index": ALL}, "n_clicks")],
    [Input("market-dropdown", "value")],
    [State("companies-dropdown", "value")]
)
def move_company_to_top(n_clicks, selected_markets, selected_companies):
    global the_selected_companies
    isMarket = selected_markets and not selected_companies #dans le cas ou toute les companies du market selectionné sont afficehr
    if n_clicks is None:
        raise dash.exceptions.PreventUpdate
    
    if any(n_clicks):
        clicked_company_index = n_clicks.index(1)
        if len(selected_companies) > clicked_company_index and selected_companies[clicked_company_index]:
            companie = selected_companies[clicked_company_index]
        else:
            filtered_companies = companies[companies["mid"].isin(selected_markets)]
            new_selected_companies = [
                {"label": row["name"], "value": row["id"]}
                for _, row in filtered_companies.iterrows()
            ]
            new_selected_companies = new_selected_companies[:20] # pour ne pas saturer le site
            companie = new_selected_companies[clicked_company_index]
        
        if isMarket:
            if companie["value"] in the_selected_companies: #Si la company est deja en premiere position Ou si la company na que 1 company principal, on la remplace
                the_selected_companies.remove(companie["value"])
                the_selected_companies.insert(0, companie["value"])
            else:
                 if (len(the_selected_companies) == 1):
                     the_selected_companies = [companie["value"]]
                 #else:
                     #the_selected_companies.insert(0, companie["value"])
        else:
            # Remove the company from the list and insert it at the top
            if companie in the_selected_companies: #Si la company est deja en premiere position Ou si la company na que 1 company principal, on la remplace
                the_selected_companies.remove(companie)
                the_selected_companies.insert(0, companie)
            else:
                 if (len(the_selected_companies) == 1):
                     the_selected_companies = [companie]
                 #else:
                 #the_selected_companies.insert(0, companie)
        
        
    return selected_companies


@app.callback(
    Output({"type": "company", "index": ALL}, "style"),
    [Input({"type": "company", "index": ALL}, "n_clicks")],
    [State({"type": "company", "index": ALL}, "id")]
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
        html.Div(style={"padding": "2px"}), #faut automatiser le padding
        dcc.Dropdown(
            id="market-dropdown",
            options=market_options,
            value=[14],
            multi=True,
            placeholder="Select market(s)",
            className="custom-dropdown",
            style={"color": "black"}
        ),
        html.Div(style={"padding": "2px"}), #faut automatiser le padding
        dcc.Dropdown(
            options=companies_options,
            value=[4534],
            id="companies-dropdown",
            multi=True,
            placeholder="Select company(ies)",
            className="custom-dropdown",
            style={"color": "black"}
        ),
        html.Div(style={"padding": "10px"}), #faut automatiser le padding
        html.Div(id="selected-companies-output")
    ],
    className="mb-4",
    width=2,
    style={"backgroundColor": "#2E2E33", "align-items": "center"},
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
                            html.Img(src="assets/its.png", style={"width":'100%'}), 
                            width=8, 
                            class_name="my-4"
                        ),
                        href="#"
                    ),
                    id="open"
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
                                    "Select Chart Type:", style={"color": "white"}
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
                        ),                 ]
                ),
            ],
            className="m-4"
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
        dbc.Modal(
            [
                dbc.ModalBody(
                    html.Img(src="/assets/saul_anim.gif", style={"width":'100%'})
                ),
            ],
            id="modal",
        ),
        dbc.Row(
            [
                dbc.Col(preference_settings, width=2),
                dbc.Col(
                    html.Div([headerGraph, graph, tab_companies]),
                    width=8,
                    id="center-section"
                ),
                end,
            ],
            className="mb-3",
            id="layout",
        )
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
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='#444', zerolinecolor="#444")
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='#444', zerolinecolor="#444")
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
        df["cid"] = label
        dfs.append(df)
    df = pd.concat(dfs)
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")

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
