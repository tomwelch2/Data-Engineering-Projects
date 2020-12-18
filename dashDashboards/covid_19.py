import pandas as pd
import plotly.express as px
import json
import requests
import dash_html_components as html
import dash_core_components as dcc
from dash import Dash
from dash.dependencies import Input, Output


def make_request():
    url = 'https://covid-193.p.rapidapi.com/statistics'

    headers = \
        {'x-rapidapi-key': 'f2315dca5amshd0576ac8a8fc1a6p11bf84jsn8b7032c423ca',
         'x-rapidapi-host': 'covid-193.p.rapidapi.com'}

    response = requests.get(url, headers=headers)
    return response.text


data = json.loads(make_request())
data = data['response']
df = pd.DataFrame(data)
df = df.dropna()
app = Dash(__name__)
server = app.server
app.layout = html.Div(children=['COVID 19 Dashboard',
                      dcc.Dropdown(id='dropdown', options=[{'label': i,
                      'value': i} for i in df.continent.unique()],
                      value='Asia', style={'fontSize': 15}),
                      dcc.Graph(id='covid_graph')],
                      style={'family': 'Verdana',
                      'backgroundColor': '#cfe3ff', 'fontSize': 40})


@app.callback(Output('covid_graph', 'figure'), [Input('dropdown',
              'value')])
def update_graph(continent):
    data = df.loc[df['continent'] == continent]
    data['death_total'] = [i['total'] for i in data['deaths']]
    fig = px.bar(x=data['country'], y=data['death_total'],
                 title='Coronavirus Deaths By Continent', width=1900,
                 height=800)
    fig.update_layout(yaxis={'title': 'Total Deaths'},
                      plot_bgcolor='orange', paper_bgcolor='#cfe3ff',
                      xaxis={'title': ''})
    return fig

if __name__ == '__main__':
    app.run_server(debug=False)
