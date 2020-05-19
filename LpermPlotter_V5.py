import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import plotly.graph_objs as go
import math
import plotly
from dash.dependencies import Input, Output
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import datetime
import numpy as np
import os.path
import csv
from sklearn import datasets, linear_model
from visc import visc
from sampledata import sample_data
from sampleparser import sample
import dash_bootstrap_components as dbc


samplesheet = r"M:\Team Chaos Liquid Perm Initialization v5.xlsx"

app = dash.Dash(__name__,external_stylesheets=[dbc.themes.SIMPLEX])
app.layout = html.Div(
    html.Div([
        html.H4('LPerm Plotter V5 - Update interval: 10 minutes'),
        html.Div(id='live-update-text'),
        dcc.Graph(id='live-update-graph0'),

        #dcc.Graph(id='live-update-graph5'),
        dcc.Interval(
            id='interval-component',
            interval=600*1000,  # in milliseconds
            n_intervals=0
        )
    ])
)


@app.callback(Output('live-update-text', 'children'),
              [Input('interval-component', 'n_intervals')])
def timenow(n):
    return("last updated: " + str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
@app.callback(Output('live-update-graph0', 'figure'),
              [Input('interval-component', 'n_intervals')])
# m is the sheet name
def plot0(n):
    current_sample = sample(samplesheet, 1).sampleprop()
    df_current = sample_data(current_sample)

    xax=df_current.DateTime
    fig = go.Figure()
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    #fig = go.Figure([go.Scatter(x=df_current['DateTime'], y=df_current['Rate'])])
    fig.update_xaxes(rangeslider_visible=True)
    #print(df_current.head())
    fig.add_trace(go.Scatter(x=xax, y=df_current.Permeability,
                            name="Permeability", line_color='Blue', opacity=.5, hovertext=df_current.Comment))
    fig.add_trace(go.Scatter(x=xax, y=df_current['Upstream Pressure'],
                            name="Upstream", line_color='red', yaxis="y2"))
    fig.add_trace(go.Scatter(x=xax, y=df_current['Downstream Pressure'],
                            name="Downstream", line_color='purple', yaxis="y2"))
    #fig.add_trace(go.Scatter(x=xax, y=df_current['rollingq'],
    fig.add_trace(go.Scatter(x=xax, y=df_current.Rate,
                            name="Flow Rate", line_color='Green', yaxis="y3", opacity=.5))
    fig.add_trace(go.Scatter(x=xax, y=df_current.qdp,
                            name="q over dp", line_color='Black', yaxis="y4", opacity=.5))
    fig.add_trace(go.Scatter(x=xax, y=df_current['Confining Pressure'],
                            name="confining pressure", line_color='Orange', yaxis="y5", opacity=1))
    title = '| Sample ID: ' + current_sample['client'] + ' ' +  current_sample['sample ID'] + ' | Vessel: ' + \
        current_sample['vessel'] + ' | Pump: ' + current_sample['Pump'][-1] + ' | Temperature: '+ \
            str(current_sample['temperature']) + ' | Comments: '+current_sample['comment']
    fig.update_layout(title_text=title, xaxis_rangeslider_visible=True, xaxis=dict(domain=[0, 0.85]), hovermode='x',
                    yaxis=dict(title="Permeability - nD", titlefont=dict(color="Blue"), anchor="free",
                                tickfont=dict(color="Blue"), range=[current_sample['perm_min'], current_sample['perm_max']]),
                    yaxis2=dict(title="Pressure - psia", titlefont=dict(color="Red"), tickfont=dict(
                        color="Red"), anchor="free", overlaying="y", side="right", position=0.85, range=[0, 9500]),
                    yaxis3=dict(title="Flow rate - cc/min", titlefont=dict(color="Green"), tickfont=dict(
                        color="Green"), anchor="free", overlaying="y", side="right", position=0.89, range=[0, .005]),
                    yaxis4=dict(title="q over dp", titlefont=dict(color="Black"), tickfont=dict(
                        color="Black"), anchor="free", overlaying="y", side="right", position=0.93, range=[0, .000005]),
                    yaxis5=dict(title="confining pressure", titlefont=dict(color="Orange"), tickfont=dict(
                        color="Orange"), anchor="free", overlaying="y", side="right", position=0.97, range=[0, 9500]))
    return fig

if __name__ == '__main__':
    app.run_server(debug=True)