import dash
from dash import dcc
from dash import html
import dash.dependencies as ddep
import pandas as pd
import sqlalchemy
from lib.constant import IS_DOCKER
from typing import Optional , Tuple
from dataclasses import dataclass
from datetime import date, timedelta, datetime
import plotly.graph_objects as go
import plotly.express as px
import plotly.subplots as ms

@dataclass
class Company:
    id: int
    name: str


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
    {"label": row["name"], "value":row["id"]} for _, row in companies.iterrows()
]
companies_id_to_labels = {row["id"]: row["name"] for _, row in companies.iterrows()}
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
            start_date=date(2019, 1, 1),
            end_date=date(2023, 12, 31),
        ),
        html.Div(id="output-container-date-picker-range"),
        dcc.Dropdown(companies_options, value=[4534], id="companies-dropdown", multi=True),
        dcc.RadioItems(["Candle Stick", "Line"], "Line", id="chart-type"),
        dcc.Checklist(["Logarithmic"], [False], id="log-y"),
        # html.Div(id="companies-output-container"),
        dcc.Graph(id="graph",style={'width': '90%', 'height': '90vh'}),
        html.Div(id="table-container"),
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
    values: Optional[list[int]],
    start_date: Optional[str],
    end_date: Optional[str],
    chart_type: str,
    log_y: bool,
):
    
    fig = ms.make_subplots(rows=len(values)+1, cols=1, shared_xaxes=True, vertical_spacing=0.1)

    if not values:
        return go.Figure()
    if not start_date or not end_date:
        return go.Figure()
    end_date = str(datetime.fromisoformat(end_date) + timedelta(days=1))
    if chart_type == 'Candle Stick':
        
        for value in values:
            label = companies_id_to_labels.get(value, '')
            
            df = pd.read_sql_query(
                f"SELECT * FROM daystocks WHERE cid = {value} and date >= '{start_date}' and date < '{end_date}' ORDER by date",
                engine,
            )
            
            window = 5  # Parametre qui doit etre modifiable
            df['MA'] = df['close'].rolling(window=window).mean()
            df['STD'] = df['close'].rolling(window=window).std()
            df['Upper'] = df['MA'] + (df['STD'] * 2)
            df['Lower'] = df['MA'] - (df['STD'] * 2)
                        
            fig.add_trace(
                go.Candlestick(
                    x=df["date"],
                    open=df["open"],
                    high=df["high"],
                    low=df["low"],
                    close=df["close"],
                    name=f"{label}"
                )
            )
            
            fig.add_trace(go.Scatter(x=df['date'], y=df['Upper'], mode='lines', name='Bande Supérieure', line=dict(color='rgba(0,0,255,0.2)')))
            fig.add_trace(go.Scatter(x=df['date'], y=df['Lower'], mode='lines', name='Bande Inférieure', line=dict(color='rgba(255,0,0,0.2)')))
            
            fig.add_trace(go.Scatter(x=df['date'].tolist() + df['date'].tolist()[::-1],
                         y=df['Upper'].tolist() + df['Lower'].tolist()[::-1],
                         fill='toself', fillcolor='rgba(0,0,255,0.1)', line=dict(color='rgba(255,255,255,0)'),
                         name='Zone entre les bandes'))
        
    else:
        for value in values:
            label = companies_id_to_labels.get(value, '')
            df = pd.read_sql_query(
                f"SELECT * FROM daystocks WHERE cid = {value} and date >= '{start_date}' and date < '{end_date}' ORDER by date",
                engine,
            )
            
            window = 20  # Parametre qui doit etre modifiable
            df['MA'] = df['close'].rolling(window=window).mean()
            df['STD'] = df['close'].rolling(window=window).std()
            df['Upper'] = df['MA'] + (df['STD'] * 2)
            df['Lower'] = df['MA'] - (df['STD'] * 2)
            
            fig.add_trace(go.Scatter(x=df["date"], y=df["close"], mode='lines', name=f"{label}"))
            
            fig.add_trace(go.Scatter(x=df['date'], y=df['Upper'], mode='lines', name='Bande Supérieure', line=dict(color='rgba(0,0,255,0.2)')))
            fig.add_trace(go.Scatter(x=df['date'], y=df['Lower'], mode='lines', name='Bande Inférieure', line=dict(color='rgba(255,0,0,0.2)')))
            
            fig.add_trace(go.Scatter(x=df['date'].tolist() + df['date'].tolist()[::-1],
                         y=df['Upper'].tolist() + df['Lower'].tolist()[::-1],
                         fill='toself', fillcolor='rgba(0,0,255,0.1)', line=dict(color='rgba(255,255,255,0)'),
                         name='Zone entre les bandes'))
    
    if log_y:
        fig.update_yaxes(type='log')
        
    for i,value in enumerate(values):
        label = companies_id_to_labels.get(value, '')
        df = pd.read_sql_query( f"SELECT * FROM daystocks WHERE cid = {value} and date >= '{start_date}' and date < '{end_date}' ORDER by date", engine)
        df['change'] = df['close'] - df['open']
        colors = ['green' if change > 0 else 'red' for change in df['change']]
        fig.add_trace(go.Bar(x=df["date"], y=df["volume"],marker_color=colors) , row=2+i, col=1)
        fig.update_yaxes(title_text=f"Volume {label}", row=2+i, col=1)

    
    fig.update_layout(xaxis_rangeslider_visible=False,

                       title={
                            'text': f"Daily chart",    
                            'x': 0.5,
                            'xanchor': 'center'
                        }, 
            )
    return fig

@app.callback(
    ddep.Output("table-container", "children"),
    [
        ddep.Input("companies-dropdown", "value"),
        ddep.Input("my-date-picker-range", "start_date"),
        ddep.Input("my-date-picker-range", "end_date"),
    ],
)
def update_table(values, start_date, end_date):
    if not values or not start_date or not end_date:
        return html.Table()
    
    dfs = []

    end_date = str(datetime.fromisoformat(end_date) + timedelta(days=1))

    for i,value in enumerate(values):
        label = companies_id_to_labels.get(value, '')
        df = pd.read_sql_query(
                f"SELECT * FROM daystocks WHERE cid = {value} and date >= '{start_date}' and date < '{end_date}' ORDER by date",
                engine,
            )
        df['cid'] = label
        dfs.append(df)

    df = pd.concat(dfs)

    table_style = {
        'margin': '20px',
        'border-collapse': 'collapse',
        'width': '90%',
        'border': '3px solid #ddd',
    }

    th_style = {
        'padding-top': '12px',
        'padding-bottom': '12px',
        'text-align': 'center',
        'background-color': '#f2f2f2',
        'color': 'black',
        'border': '1px solid #ddd',
    }

    td_style = {
        'padding-top': '12px',
        'padding-bottom': '12px',
        'text-align': 'center',
        'border': '1px solid #ddd',
    }

    df.rename(columns={"cid": "name"}, inplace=True)
    
    return html.Table(
        # Header
        [html.Tr([html.Th(col, style=th_style) for col in df.columns])] +
        # Body
        [html.Tr([html.Td(df.iloc[i][col], style=td_style) for col in df.columns]) for i in range(len(df))],
        style=table_style,
    )
    
if __name__ == "__main__":
    app.run(debug=True)
