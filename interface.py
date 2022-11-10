import dash
from dash import Dash, html, dcc, Input, Output, State, dash_table
import dash_bootstrap_components as dbc
# import plotly.express as px
import pandas as pd
from pytermgui import auto, keys

dict_ = {'key 1': 'value 1', 'key 2': 'value 2', 'key 3': 'value 3'}
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
                            style={'width': '35%', 'height': 250},
                )

natural_language_div = html.Div(children=[
                            dbc.Label("Natural Language Description"),
                            html.Div([dcc.Textarea(
                                id='textarea-processed-query',
                                value='',
                                style={'width': '50%', 'height': 230, 'resize': 'none'},
                                readOnly=True
                            ),
                            dcc.Textarea(
                                id='textarea-natural-language',
                                value='',
                                style={'width': '50%', 'height': 230, 'resize': 'none'},
                                readOnly=True
                            )]),
                            # dcc.Textarea(
                            #     id='textarea-natural-language',
                            #     value='',
                            #     style={'width': '100%', 'height': 230, 'resize': 'none'},
                            #     readOnly=True
                            # ),
                            html.Br(),
                            dbc.Button('Visualize', outline= True, color='primary', className='me-1', size='sm',
                                        style={'float': 'right', 'visibility': 'hidden'}
                                        ),
                            ], 
                            style={'width': '65%', 'height': 250, 'padding-left': 20},
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
                        style={'padding-top': 50, 'width': '35%', 'padding-left': 10}
                    )
body_right = html.Div([
            dbc.Label("Natural Language Description"),
            html.Div(children=[], id="table1", style={'width': '100%'})
            ], 
            style={'width': '65%', 'height': 250, 'padding-left': 20}
)

body_top_left = html.Div(children=[
                    sql_query_div,
                    body_right
                ],
                style={'width': '100%', 'display': 'flex','padding-top':30, 'padding-left': 10, 'padding-right': 10}
                )



body_left = html.Div(children=[body_top_left, visualization_div], style={'width': '100%'})



# divider = html.Div(className="vl", style={'height':'max-height', 'border-left':'2px solid black', 'padding-right': 10, 'margin-left': 10})

# dropdown_div = html.Div(
#     children = [
#         html.Div(
#             children = "Enter ID:",
#             className = 'textDiv'
#         )

#         html.Div(
#             children = "Enter Test Pattern",
#             className = 'textDiv'
#         ),
#         dcc.Input(
#             id = 'pattern_desc',
#             type = 'text',
#             value = 'Sample',
#             size = 20),

#          html.Div(
#             children = "Enter File OutPut Path:",
#             className = 'textDiv'
#         ),
#         dcc.Input(
#             id = 'file_path',
#             type = 'text',
#             value = '',
#             size = 30),

#         html.Button(
#             id = 'submit',
#             n_clicks = 0,
#             children = 'Search'
#         ),
# html.Div(
#         id = 'tableDiv',
#         children = dash_table.DataTable(
#         id = 'table',
#         style_table={'overflowX': 'scroll'},
#         style_as_list_view=True,
#         style_header={'backgroundColor': 'white','fontWeight': 
#             'bold'},
#          ),
#         className = 'tableDiv'
#     )
# ])
  



# dropdown_answer_div = html.Div(children=[
#                             dbc.Label("Answer"),
#                             dcc.Textarea(
#                                 id='textarea-answer',
#                                 value='',
#                                 style={'width': '100%', 'height': 230, 'resize': 'none'},
#                                 readOnly=True
#                             ),
#                             ],
#                             style={'margin-top': 50}
#                         )

# body_right = html.Div(children=[
#                 dropdown_div, 
#                 dropdown_answer_div],
#                 style={'width': '30%', 'padding-top': 30})

app.layout = html.Div(children=[
                heading,
                body_left
                # html.Div(children=[body_left],
                #         style={'display': 'flex','overflow':'auto', 'background-color': '#FAFAFA'}),
            ])
                
@app.callback(
    Output('table1', 'children'),
    Input('query_submit_button', 'n_clicks'),
    State('textarea-sql-query', 'value')
)
def update_output(n_clicks, value):
    
    html_output = []
    dict_output = {'query_1': {'subqueries': 
    {'subquery_1': {'subqueries': {}, 'from partsupp P1 , supplier S , nation': ['The clause "from p1" is implemented using Seq Scan because no other option is available.', 'The clause "from s" is implemented using Seq Scan because it requires 44.62, 10351967.39 less operations than Bitmap Heap Scan, Index Scan respectively.', 'The clause "from nation_1" is implemented using Seq Scan because it requires 2.90, 277777779.19 less operations than Bitmap Heap Scan, Index Scan respectively.'], "where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY'": ['The clause "where nation_1.n_name = \'GERMANY\'" is implemented using Seq Scan because it requires 2.90, 277777779.19 less operations than Bitmap Heap Scan, Index Scan respectively.', 'The clause "where s.s_nationkey = nation_1.n_nationkey" is implemented using Hash Join.', 'The clause "where p1.ps_suppkey = s.s_suppkey" is implemented using Hash Join.']}}
    , 'from partsupp P , supplier , nation': ['The clause "from p" is implemented using Seq Scan because no other option is available.', 'The clause "from nation" is implemented using Seq Scan because no other option is available.', 'The clause "from supplier" is implemented using Seq Scan because it requires 124.21, 28818445.30 less operations than Bitmap Heap Scan, Index Scan respectively.'], "where P.ps_suppkey = s_suppkey and not s_nationkey = n_nationkey and n_name = 'GERMANY' and ps_supplycost > 20 and s_acctbal > 10": ['The clause "where p.ps_supplycost > \'20\'" is implemented using Seq Scan because no other option is available.', 'The clause "where nation.n_name = \'GERMANY\'" is implemented using Seq Scan because no other option is available.', 'The clause "where supplier.s_acctbal > \'10\'" is implemented using Seq Scan because it requires 124.21, 28818445.30 less operations than Bitmap Heap Scan, Index Scan respectively.', 'The clause "where supplier.s_nationkey <> nation.n_nationkey" is implemented using Nested Loop.', 'The clause "where p.ps_suppkey = supplier.s_suppkey" is implemented using Hash Join.'], 'order by value ;': ['The clause "order by sum((p.ps_supplycost * (p.ps_availqty) asc" is implemented using external merge Sort.']
    }
}
    # df = pd.DataFrame.from_dict(dict_output)
    dict_keys = list(dict_output.keys())
    dict_values = list(dict_output.values())
    # # print(str(dict_keys))
    # # print(str(dict_values))
    # # print(len(dict_output))
    # dict_keys_output = ""
    # dict_values_output = ""
    
    dict_array = []
    for i in range(len(dict_keys)):
        # print(str(dict_keys[i]))
        div_component = html.Div(str(dict_keys[i]) + ": ", style={"font-weight": "bold"})
        html_output.append(div_component)
        divider = html.Div(className="vl", style={'width':'max-width', 'border-top':'2px solid black', 'padding-right': 10})
        html_output.append(divider)
        # dict_object = {'Query': str(dict_keys[i]), 'Natural Language Description': str(dict_keys[i])}
        # dict_array.append(dict_object)
        nested_dict = dict_values[i]
        # print(list(nested_dict.keys()))
        nested_dict_keys = list(nested_dict.keys())
        nested_dict_values = list(nested_dict.values())
        for j in range(len(nested_dict_keys)):
            # print(nested_dict_keys[j])
            if (nested_dict_keys[j] == "subqueries"):
                nested_dict_subquery = nested_dict_values[j]
                # print(nested_dict_subquery)
                nested_dict_subquery_keys = list(nested_dict_subquery.keys())
                nested_dict_subquery_values = list(nested_dict_subquery.values())
                dict_array = []
                for k in range(len(nested_dict_subquery_keys)):
                    div_component = html.Div(str(nested_dict_subquery_keys[k] + ": "), style={"padding-left": 20})
                    html_output.append(div_component)
                    nested_subquery_values = nested_dict_subquery_values[k]
                    nested_subquery_values_keys = list(nested_subquery_values.keys())
                    nested_subquery_values_values = list(nested_subquery_values.values())
                    
                    for l in range(len(nested_subquery_values_keys)):
                        if (nested_subquery_values_keys[l] == "subqueries"):
                            nested_nested_dict_subquery = nested_subquery_values_values[l]
                            # print(nested_nested_dict_subquery)
                        else:
                            dict_temp_array = nested_subquery_values_values[l]
                            dict_temp_str = ""
                            for item in dict_temp_array:
                                dict_temp_str += item + "\n"
                            dict_object = {'Query': str(nested_subquery_values_keys[l]), 'Natural Language Description': str(dict_temp_str)}
                            dict_array.append(dict_object)
                    # print(dict_array)
                    div_component = dash_table.DataTable(data=dict_array, 
                    style_table={
                        'padding-left': 20,
                        'padding-bottom': 10,
                        'width': 'auto',
                        'max-height': '580px', 
                        'overflowY': 'auto'
                    },
                    style_data={
                    'whiteSpace': 'pre-line',
                    'height': 'auto'
                    },
                    style_cell={'textAlign': 'left'})
                    html_output.append(div_component)
                    dict_array = []

            else:
                dict_temp_array = nested_dict_values[j]
                dict_temp_str = ""
                for item in dict_temp_array:
                    dict_temp_str += item + "\n"
                
                dict_object = {'Query': str(nested_dict_keys[j]), 'Natural Language Description': str(dict_temp_str)}
                dict_array.append(dict_object)
        print(dict_array)
        div_component = dash_table.DataTable(data=dict_array, 
                    style_table={
                        'padding-left': 1,
                        'padding-bottom': 10,
                        'width': 'auto',
                        'max-height': '580px', 
                        'overflowY': 'auto'
                    },
                    style_data={
                    'whiteSpace': 'pre-line',
                    'height': 'auto'
                    },
                    style_cell={'textAlign': 'left'})
        html_output.append(div_component)  



        # print(list(nested_dict['subqueries']))
    # for i in range(len(dict_output)):
    #     dict_value = ""
    #     for j in range(len(dict_values[i])):
    #         dict_value = dict_value + str(dict_values[i][j]) + "\n"
    #     print(dict_value)
    #     dict_object = {'Query': str(dict_keys[i]), 'Natural Language Description': dict_value}
    #     print(str(dict_object))
    #     dict_array.append(dict_object)
        # dict_keys_output = dict_keys_output + str(dict_keys[i]) + "\n"
        # dict_values_output = dict_values_output + str(dict_values[i]) + "\n"
        # print(dict_keys[i])
        # print(dict_values[i])

    # value_query = str(dict_keys)
    # value_nl = str(dict_values)
    # print(str(dict_array))
    # for item in dict_array:
    #     # print(item)
    #     nl_value = item['Natural Language Description']
    #     item['Natural Language Description'] = ""
    #     nl_list = list(nl_value.split(","))
    #     for sub_item in nl_list:
    #         item['Natural Language Description'] += str(sub_item) + "\n"
    # print(dict_array)
        # for i in range(len(nl_value)):
        #     print(nl_value[i])
        # # print(nl_value)
        # for sub_item in nl_value:
        #     print(sub_item)
    if n_clicks > 0:
        # print(value_query)
        # results = "callfunction"
        # print(str(df.to_dict('records')))
        # print(str(dict_array))
        print(html_output)
        return html.Div(html_output)
        # return html.Div([html.Div("Header"), dash_table.DataTable(data=dict_array, 
        # style_table={
        #     'padding-left': 1,
        #     'width': 'auto',
        #     'height': '580px', 
        #     'overflowY': 'auto'
        # },
        # style_data={
        # 'whiteSpace': 'pre-line',
        # 'height': 'auto'
        # },
        # style_cell={'textAlign': 'left'})
        # ])
        # return dash_table.DataTable(df.to_dict('records'))
        # return dash_table.DataTable([{'query':'sojfjjf', 'nl':'efjifj'},{'query':'sss', 'nl':'jff'}])

# @app.callback(
#     [Output('textarea-processed-query', 'value'),
#     Output('textarea-natural-language', 'value')],
#     Input('query_submit_button', 'n_clicks'),
#     State('textarea-sql-query', 'value')
# )
# def update_output(n_clicks, value):
#     dict_output = {'from partsupp P1 , supplier S , nation': ['The clause "from p1" is implemented using Seq Scan because no other option is available.', 'The clause "from s" is implemented using Seq Scan because  it requires 44.62, 10351967.39 less operations than Bitmap Heap Scan, Index Scan respectively.', 'The clause "from nation_1" is implemented using Seq Scan because  it requires 2.90, 277777779.19 less operations than Bitmap Heap Scan, Index Scan respectively.'], "where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY'": ['The clause "where nation_1.n_name = \'GERMANY\'" is implemented using Seq Scan because  it requires 2.90, 277777779.19 less operations than Bitmap Heap Scan, Index Scan respectively.', 'The clause "where s.s_nationkey = nation_1.n_nationkey" is implemented using Hash Join.', 'The clause "where p1.ps_suppkey = s.s_suppkey" is implemented using Hash Join.'], 'from partsupp P , supplier , nation': ['The clause "from p" is implemented using Seq Scan because no other option is available.', 'The clause "from nation" is implemented using Seq Scan because no other option is available.', 'The clause "from supplier" is implemented using Seq Scan because  it requires 124.21, 28818445.30 less operations than Bitmap Heap Scan, Index Scan respectively.'], "where P.ps_suppkey = s_suppkey and not s_nationkey = n_nationkey and n_name = 'GERMANY' and ps_supplycost > 20 and s_acctbal > 10": ['The clause "where p.ps_supplycost > \'20\'" is implemented using Seq Scan because no other option is available.', 'The clause "where nation.n_name = \'GERMANY\'" is implemented using Seq Scan because no other option is available.', 'The clause "where supplier.s_acctbal > \'10\'" is implemented using Seq Scan because  it requires 124.21, 28818445.30 less operations than Bitmap Heap Scan, Index Scan respectively.', 'The clause "where supplier.s_nationkey <> nation.n_nationkey" is implemented using Nested Loop.', 'The clause "where p.ps_suppkey = supplier.s_suppkey" is implemented using Hash Join.'], 'group by ps_partkey': ['The clause "group by p.ps_partkey" is implemented using Aggregate.'], 'order by value ;': ['The clause "order by sum((p.ps_supplycost * (p.ps_availqty) asc" is implemented using external merge Sort.']}
#     # df = pd.DataFrame.from_dict(dict_output)
#     dict_keys = list(dict_output.keys())
#     dict_values = list(dict_output.values())
#     # print(str(dict_keys))
#     # print(str(dict_values))
#     # print(len(dict_output))
#     dict_keys_output = ""
#     dict_values_output = ""
#     for i in range(len(dict_output)):
#         dict_keys_output = dict_keys_output + str(dict_keys[i]) + "\n"
#         dict_values_output = dict_values_output + str(dict_values[i]) + "\n"
#         print(dict_keys[i])
#         print(dict_values[i])

#     value_query = str(dict_keys)
#     value_nl = str(dict_values)
    
#     if n_clicks > 0:
#         print(value_query)
#         # results = "callfunction"
#         return dict_keys_output, dict_values_output

# @app.callback(
#     Output('tableDiv', 'children'),
#     Input('submit', 'n_clicks'),
#     State('ID', 'value'),  State('pattern_desc', 'value'), 
#     State('file_path', 'value'))
# def update_table(n_clicks):
#     df = pd.DataFrame([dict_])
#     # df = "something"
#     mycolumns = [{'name': i, 'id': i} for i in df.columns]
#     return html.Div([
#             data_table.DataTable(
#         id='table',
#         columns=mycolumns,
#         data=df.to_dict("rows")
#         )
#     ])

if __name__ == '__main__':
    app.run_server(debug=True)