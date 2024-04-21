import dash
from dash import dcc ,html
import dash.dependencies as ddep
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
import pandas as pd
import sqlalchemy
from lib.constant import IS_DOCKER
from typing import Optional , Tuple
from datetime import date, timedelta, datetime
import plotly.graph_objects as go
import plotly.express as px
import plotly.subplots as ms


# external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']


DATABASE_URI = (
    "timescaledb://ricou:monmdp@db:5432/bourse"
    if IS_DOCKER
    else "timescaledb://ricou:monmdp@localhost:5432/bourse"
)
engine = sqlalchemy.create_engine(DATABASE_URI)

app = dash.Dash(
    __name__, title="Bourse", suppress_callback_exceptions=True, external_stylesheets=[dbc.themes.BOOTSTRAP]
)  # , external_stylesheets=external_stylesheets)

server = app.server
companies = pd.read_sql_query("SELECT * FROM companies", engine)
companies_options = [
    {"label": row["name"], "value":row["id"]} for _, row in companies.iterrows()
]
companies_id_to_labels = {row["id"]: row["name"] for _, row in companies.iterrows()}

modal = dbc.Modal(
    [
        dbc.ModalHeader(dbc.ModalTitle("Adjust Chart Settings")),
        dbc.ModalBody(
            dbc.Form([
                dbc.Switch(id="switch-log", label="Logarithmic", value=False),
                dbc.Switch(id="switch-volume", label="Volume", value=False),
                dbc.Switch(id="switch-bollinger-bands", label="Bollinger Bands", value=False),
                html.Div(
                    [
                        dbc.Label("Bollinger Bands Window Size:"),
                        dbc.Input(type="number", id="bollinger-window", min=1, step=1, value=20, disabled=True)
                    ],
                    id="bollinger-window-container",
                    style={'display': 'none'} 
                )
            ])
        ),
        dbc.ModalFooter(
            dbc.Button("Close", id="close-modal", className="ms-auto", n_clicks=0)
        ),
    ],
    id="modal-settings",
    is_open=False
)


@app.callback(
    [Output("bollinger-window-container", "style"),
     Output("bollinger-window", "disabled")],
    [Input("switch-bollinger-bands", "value")]
)
def toggle_bollinger_window(switch_value):
    if switch_value:
        return {'display': 'block'}, False 
    else:
        return {'display': 'none'}, True 

@app.callback(
    Output("modal-settings", "is_open"),
    [Input("open-settings", "n_clicks"), Input("close-modal", "n_clicks")],
    [State("modal-settings", "is_open")]
)
def toggle_modal(n1, n2, is_open):
    if n1 or n2:
        return not is_open
    return is_open

open_modal_button = dbc.Button("Open Settings", id="open-settings", n_clicks=0)

app.layout = html.Div(
    [
        open_modal_button,
        modal,
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
        dcc.Graph(id="graph",style={'width': '90%', 'height': '90vh'}),
        html.Div(id="table-container"),
    ]
)


def create_candlestick_trace(df, label):
    return go.Candlestick(
        x=df["date"],
        open=df["open"],
        high=df["high"],
        low=df["low"],
        close=df["close"],
        name=f"{label}"
    )

def create_bands_trace(df):
    upper_trace = go.Scatter(x=df['date'], y=df['Upper'], mode='lines', name='Bande Supérieure', line=dict(color='rgba(0,0,255,0.2)'))
    lower_trace = go.Scatter(x=df['date'], y=df['Lower'], mode='lines', name='Bande Inférieure', line=dict(color='rgba(255,0,0,0.2)'))
    zone_trace = go.Scatter(x=df['date'].tolist() + df['date'].tolist()[::-1],
                            y=df['Upper'].tolist() + df['Lower'].tolist()[::-1],
                            fill='toself', fillcolor='rgba(0,0,255,0.1)', line=dict(color='rgba(255,255,255,0)'),
                            name='Zone entre les bandes')
    return upper_trace, lower_trace, zone_trace

def create_volume_trace(df, label):
    df['change'] = df['close'] - df['open']
    colors = ['green' if change > 0 else 'red' for change in df['change']]
    return go.Bar(x=df["date"], y=df["volume"], marker_color=colors, name=f"Volume {label}")


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
        Input("bollinger-window", "value")
    ]
)
def update_companies_chart(
    values: Optional[list[int]],
    start_date: Optional[str],
    end_date: Optional[str],
    chart_type: str,
    log_y: bool,
    volume: bool,
    bollinger_bands: bool,
    bollinger_window: Optional[int]
):
    
    if volume:
        fig = ms.make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.1)
    else:
        fig = ms.make_subplots(rows=1, cols=1, shared_xaxes=True, vertical_spacing=0.1)


    if not values:
        return go.Figure()
    if not start_date or not end_date:
        return go.Figure()
    end_date = str(datetime.fromisoformat(end_date) + timedelta(days=1))
    if chart_type == 'Candle Stick':
        
        for i,value in enumerate(values):
            label = companies_id_to_labels.get(value, '')
            
            df = pd.read_sql_query(
                f"SELECT * FROM daystocks WHERE cid = {value} and date >= '{start_date}' and date < '{end_date}' ORDER by date",
                engine,
            )
            
            if i == 0:            
                fig.add_trace(create_candlestick_trace(df, label))
            else:
                fig.add_trace(go.Scatter(x=df["date"], y=df["close"], mode='lines', name=f"{label}"))
                
            if bollinger_bands and i == 0:
                df['MA'] = df['close'].rolling(window=bollinger_window).mean()
                df['STD'] = df['close'].rolling(window=bollinger_window).std()
                df['Upper'] = df['MA'] + (df['STD'] * 2)
                df['Lower'] = df['MA'] - (df['STD'] * 2)
                upper_trace, lower_trace, zone_trace = create_bands_trace(df)
                fig.add_trace(upper_trace)
                fig.add_trace(lower_trace)
                fig.add_trace(zone_trace)
        
        
    else:
        for i,value in enumerate(values):
            label = companies_id_to_labels.get(value, '')
            df = pd.read_sql_query(
                f"SELECT * FROM daystocks WHERE cid = {value} and date >= '{start_date}' and date < '{end_date}' ORDER by date",
                engine,
            )
            
            fig.add_trace(go.Scatter(x=df["date"], y=df["close"], mode='lines', name=f"{label}"))
            
            if bollinger_bands and i == 0:
                df['MA'] = df['close'].rolling(window=bollinger_window).mean()
                df['STD'] = df['close'].rolling(window=bollinger_window).std()
                df['Upper'] = df['MA'] + (df['STD'] * 2)
                df['Lower'] = df['MA'] - (df['STD'] * 2)
                upper_trace, lower_trace, zone_trace = create_bands_trace(df)
                fig.add_trace(upper_trace)
                fig.add_trace(lower_trace)
                fig.add_trace(zone_trace)
    
    if log_y:
        fig.update_yaxes(type='log')
        
    if volume:
        value = values[0]
        label = companies_id_to_labels.get(value, '')
        df = pd.read_sql_query( f"SELECT * FROM daystocks WHERE cid = {value} and date >= '{start_date}' and date < '{end_date}' ORDER by date", engine)
        fig.add_trace(create_volume_trace(df, label), row=2, col=1)
        fig.update_yaxes(title_text=f"Volume {label}", row=2, col=1)

        
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
