import dash
from dash import dcc
from dash import html
import dash.dependencies as ddep
import pandas as pd
import sqlalchemy
from lib.constant import IS_DOCKER
from typing import Optional
from datetime import date, timedelta, datetime
import plotly.graph_objects as go
import plotly.express as px



# external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

DATABASE_URI = (
    "timescaledb://ricou:monmdp@db:5432/bourse"
    if IS_DOCKER
    else "timescaledb://ricou:monmdp@localhost:5432/bourse"
)
engine = sqlalchemy.create_engine(DATABASE_URI)

app = dash.Dash(
    __name__, title="Bourse", suppress_callback_exceptions=True
)  # , external_stylesheets=external_stylesheets)
server = app.server
companies = pd.read_sql_query("SELECT * FROM companies", engine)
companies_options = [
    {"label": row["name"], "value": row["id"]} for _, row in companies.iterrows()
]
app.layout = html.Div(
    [
        # dcc.Textarea(
        #     id="sql-query",
        #     value="""
        #                 SELECT * FROM pg_catalog.pg_tables
        #                     WHERE schemaname != 'pg_catalog' AND
        #                           schemaname != 'information_schema';
        #             """,
        #     style={"width": "100%", "height": 100},
        # ),
        # html.Button("Execute", id="execute-query", n_clicks=0),
        # html.Div(id="query-result"),
        dcc.DatePickerRange(
            id="my-date-picker-range",
            min_date_allowed=date(2019, 1, 1),
            max_date_allowed=date(2023, 12, 31),
            initial_visible_month=date(2019, 1, 1),
            end_date=date(2023, 12, 31),
        ),
        html.Div(id="output-container-date-picker-range"),
        dcc.Dropdown(companies_options, 4534, id="companies-dropdown"),
        dcc.RadioItems(["Candle Stick", "Line"], "Line", id="chart-type"),
        dcc.Checklist(["Logarithmic"], [False], id="log-y"),
        # html.Div(id="companies-output-container"),
        dcc.Graph(id="graph"),
    ]
)


# @app.callback(
#     ddep.Output("query-result", "children"),
#     ddep.Input("execute-query", "n_clicks"),
#     ddep.State("sql-query", "value"),
# )
# def run_query(n_clicks, query):
#     if n_clicks > 0:
#         try:
#             result_df = pd.read_sql_query(query, engine)
#             return html.Pre(result_df.to_string())
#         except Exception as e:
#             return html.Pre(str(e))
#     return "Enter a query and press execute."


@app.callback(
    ddep.Output("graph", "figure"),
    ddep.Input("companies-dropdown", "value"),
    ddep.Input("my-date-picker-range", "start_date"),
    ddep.Input("my-date-picker-range", "end_date"),
    ddep.Input("chart-type", "value"),
    ddep.Input("log-y", "value"),
)
def update_companies_chart(
    value: Optional[int],
    start_date: Optional[str],
    end_date: Optional[str],
    chart_type: str,
    log_y: bool,
):

    if not value:
        return go.Figure()
    if not start_date or not end_date:
        return go.Figure()
    end_date = str(datetime.fromisoformat(end_date) + timedelta(days=1))
    if chart_type == 'Candle Stick':
        df = pd.read_sql_query(
            f"SELECT * FROM daystocks WHERE cid = {value} and date >= '{start_date}' and date < '{end_date}' ORDER by date",
            engine,
        )
        fig = go.Figure(
            go.Candlestick(
                x=df["date"],
                open=df["open"],
                high=df["high"],
                low=df["low"],
                close=df["close"],
            )
            
        )
    
    else:
        df = pd.read_sql_query(
            f"SELECT * FROM stocks WHERE cid = {value} and date >= '{start_date}' and date < '{end_date}' ORDER by date",
            engine,
        )
        fig = px.line(df, x=df["date"], y=df["value"])
    if log_y:
        fig.update_yaxes(type='log')
    fig.update_layout(xaxis_rangeslider_visible=False)
    return fig

if __name__ == "__main__":
    app.run(debug=True)
