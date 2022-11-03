import dash
from dash import Dash, html, dcc, Input, Output, State
import dash_bootstrap_components as dbc
# import plotly.express as px
import pandas as pd

app = dash.Dash(external_stylesheets=[dbc.themes.BOOTSTRAP])

heading = html.Div(style={'backgroundColor': 'black', 'height': 110, 'width': '100%'}, children=[
                # Title
                html.H1(
                    children='Project 2',
                    style={'textAlign': 'center', 'color': 'white', 'padding':5}
                ),
                # Title Description
                html.Div(
                    children='Dash: A web application framework for Python.', 
                    style={'textAlign': 'center','color': 'white'
                })
            ])

sql_query_div = html.Div(children=[
                            dbc.Label("SQL Query"),
                            dcc.Textarea(
                                id='textarea-sql-query',
                                placeholder='Enter your SQL query here.',
                                style={'width': '100%', 'height': 230, 'resize': 'none'},
                            ),
                            html.Br(),
                            dbc.Button('Submit', id='query_submit_button', n_clicks=0, outline= True, color='primary', className='me-1', size='sm',
                                        style={'float': 'right'}
                                        ),
                        ], 
                            style={'width': '50%', 'height': 250},
                )

natural_language_div = html.Div(children=[
                            dbc.Label("Natural Language Description"),
                            dcc.Textarea(
                                id='textarea-natural-language',
                                value='',
                                style={'width': '100%', 'height': 230, 'resize': 'none'},
                                readOnly=True
                            ),
                            html.Br(),
                            dbc.Button('Visualize', outline= True, color='primary', className='me-1', size='sm',
                                        style={'float': 'right', 'visibility': 'hidden'}
                                        ),
                            ], 
                            style={'width': '50%', 'height': 250, 'padding-left': 20},
                        )

visualization_div = html.Div([
                        dbc.Label("Visualize Plan"),
                        dcc.Textarea(
                                id='graph-nl-steps',
                                value='show flowchart here',
                                style={'width': '100%', 'height': 230, 'resize': 'none'},
                                readOnly=True
                            ),
                        html.Br(),
                        dbc.Button('Detailed View', outline= True, color='primary', className='me-1', size='sm',
                                        style={'float': 'right', 'margin-bottom': 10}
                                        ),
                    ],
                        style={'padding-top': 50, 'width': '100%', 'padding-left': 10}
                    )

body_top_left = html.Div(children=[
                    sql_query_div,
                    natural_language_div
                ],
                style={'width': '100%', 'display': 'flex','padding-top':30, 'padding-left': 10}
                )

body_left = html.Div(children=[body_top_left, visualization_div], style={'width': '65%'})

divider = html.Div(className="vl", style={'height':'max-height', 'border-left':'2px solid black', 'padding-right': 10, 'margin-left': 10})

dropdown_question2_div = html.Div(children=[
                            dbc.Label("Enter the step"),
                            dbc.Input(
                                id="question_input",
                                type='text',
                                style={'width': '97%', 'margin-left':5}
                            ),
                            html.Br(),
                            ],
                            style = {'display': 'block'}
                        )

dropdown_div = html.Div(children=[
                dbc.Label("Question"),
                dcc.Dropdown(
                    ['Which is the most expensive step', 'Which is the least expensive step', 'What are the operator used'],
                    placeholder="Select a question",
                    style={'width': '100%'})
                ,
                html.Br(),
                dropdown_question2_div,
                dbc.Button('Select', outline= True, color='primary', className='me-1', size='sm',
                            style={'float': 'right'}
                            ),
                
                ],

                style = {'overflow': 'auto', 'width': '100%', 'height':250}
            )

dropdown_answer_div = html.Div(children=[
                            dbc.Label("Answer"),
                            dcc.Textarea(
                                id='textarea-answer',
                                value='',
                                style={'width': '100%', 'height': 230, 'resize': 'none'},
                                readOnly=True
                            ),
                            ],
                            style={'margin-top': 50}
                        )

body_right = html.Div(children=[
                dropdown_div, 
                dropdown_answer_div],
                style={'width': '30%', 'padding-top': 30})

app.layout = html.Div(children=[
                heading,
                html.Div(children=[body_left, divider, body_right],
                        style={'display': 'flex','overflow':'auto', 'background-color': '#FAFAFA'}),
            ])
                

@app.callback(
    Output('textarea-natural-language', 'value'),
    Input('query_submit_button', 'n_clicks'),
    State('textarea-sql-query', 'value')
)
def update_output(n_clicks, value):
    if n_clicks > 0:
        return "function"

if __name__ == '__main__':
    app.run_server(debug=True)