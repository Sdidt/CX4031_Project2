from dash import Dash, html, dcc, Input, Output, State, dash_table
import dash_bootstrap_components as dbc
import base64, time, re
# import plotly.express as px
# import pandas as pd
# from pytermgui import auto, keys

dict_ = {'key 1': 'value 1', 'key 2': 'value 2', 'key 3': 'value 3'}
app = Dash(external_stylesheets=[dbc.themes.BOOTSTRAP])

list_group = dbc.ListGroup(
    [
        dbc.ListGroupItem("Customer", id="custAttr", href="#", n_clicks=0),
        dbc.Collapse(
            dbc.Card(dbc.CardBody(
                dbc.ListGroup([
                    dbc.ListGroupItem("1. c_custkey integer"),
                    dbc.ListGroupItem("2. c_name character varying"),
                    dbc.ListGroupItem("3. c_address character varying"),
                    dbc.ListGroupItem("4. c_nationkey integer"),
                    dbc.ListGroupItem("5. c_phone character"),
                    dbc.ListGroupItem("6. c_acctbal numeric"),
                    dbc.ListGroupItem("7. c_mktsegment character"),
                    dbc.ListGroupItem("8. c_comment character varying")
                ])
                )),
            id="collapse1",
            is_open=False,
        ),
        dbc.ListGroupItem("Supplier", id="suppAttr", href="#", n_clicks=0),
        dbc.Collapse(
            dbc.Card(dbc.CardBody(
                dbc.ListGroup([
                    dbc.ListGroupItem("1. s_suppkey integer"),
                    dbc.ListGroupItem("2. s_name character"),
                    dbc.ListGroupItem("3. s_address character varying"),
                    dbc.ListGroupItem("4. s_nationkey integer"),
                    dbc.ListGroupItem("5. s_phone character"),
                    dbc.ListGroupItem("6. s_acctbal numeric"),
                    dbc.ListGroupItem("7. s_comment character varying")
                ])
                )),
            id="collapse2",
            is_open=False,
        ),
        dbc.ListGroupItem("Nation", id="nationAttr", href="#", n_clicks=0),
        dbc.Collapse(
            dbc.Card(dbc.CardBody(
                dbc.ListGroup([
                    dbc.ListGroupItem("1. n_nationkey integer"),
                    dbc.ListGroupItem("2. n_name character"),
                    dbc.ListGroupItem("3. n_regionkey integer"),
                    dbc.ListGroupItem("4. n_comment character varying")
                ])
                )),
            id="collapse3",
            is_open=False,
        ),
        dbc.ListGroupItem("Region", id="regionAttr", href="#", n_clicks=0),
        dbc.Collapse(
            dbc.Card(dbc.CardBody(
                dbc.ListGroup([
                    dbc.ListGroupItem("1. r_regionkey integer"),
                    dbc.ListGroupItem("2. r_name character"),
                    dbc.ListGroupItem("3. r_comment character varying")
                ])
                )),
            id="collapse4",
            is_open=False,
        ),
        dbc.ListGroupItem("Part", id="partAttr", href="#", n_clicks=0),
        dbc.Collapse(
            dbc.Card(dbc.CardBody(
                dbc.ListGroup([
                    dbc.ListGroupItem("1. p_partkey integer"),
                    dbc.ListGroupItem("2. p_name character varying"),
                    dbc.ListGroupItem("3. p_mfgr character"),
                    dbc.ListGroupItem("4. p_brand character"),
                    dbc.ListGroupItem("5. p_type character varying"),
                    dbc.ListGroupItem("6. p_size integer"),
                    dbc.ListGroupItem("7. p_container character"),
                    dbc.ListGroupItem("7. p_retailprice numeric"),
                    dbc.ListGroupItem("8. c_comment character varying")
                ])
                )),
            id="collapse5",
            is_open=False,
        ),
        dbc.ListGroupItem("Partsupp", id="partsuppAttr", href="#", n_clicks=0),
        dbc.Collapse(
            dbc.Card(dbc.CardBody(
                dbc.ListGroup([
                    dbc.ListGroupItem("1. ps_partkey integer"),
                    dbc.ListGroupItem("2. ps_suppkey integer"),
                    dbc.ListGroupItem("3. ps_availqty integer"),
                    dbc.ListGroupItem("4. ps_supplycost numeric"),
                    dbc.ListGroupItem("5. ps_comment character varying")
                ])
                )),
            id="collapse6",
            is_open=False,
        ),
        dbc.ListGroupItem("Orders", id="orderAttr", href="#", n_clicks=0),
        dbc.Collapse(
            dbc.Card(dbc.CardBody(
                dbc.ListGroup([
                    dbc.ListGroupItem("1. o_orderkey integer"),
                    dbc.ListGroupItem("2. o_custkey integer"),
                    dbc.ListGroupItem("3. o_orderstatus character"),
                    dbc.ListGroupItem("4. o_totalprice numeric"),
                    dbc.ListGroupItem("5. o_orderdate date"),
                    dbc.ListGroupItem("6. o_orderpriority character"),
                    dbc.ListGroupItem("7. o_clerk character"),
                    dbc.ListGroupItem("8. o_shippriority integer"),
                    dbc.ListGroupItem("9. c_comment character varying")
                ])
                )),
            id="collapse7",
            is_open=False,
        ),
        dbc.ListGroupItem("LineItem", id="lineitemAttr", href="#", n_clicks=0),
        dbc.Collapse(
            dbc.Card(dbc.CardBody(
                dbc.ListGroup([
                    dbc.ListGroupItem("1. l_orderkey integer"),
                    dbc.ListGroupItem("2. l_partkey integer"),
                    dbc.ListGroupItem("3. l_suppkey integer"),
                    dbc.ListGroupItem("4. l_linenumber integer"),
                    dbc.ListGroupItem("5. l_quantity numeric"),
                    dbc.ListGroupItem("6. l_extendedprice numeric"),
                    dbc.ListGroupItem("7. l_discount numeric"),
                    dbc.ListGroupItem("8. l_tax numeric"),
                    dbc.ListGroupItem("9. l_returnflag character"),
                    dbc.ListGroupItem("10. l_linestatus character"),
                    dbc.ListGroupItem("11. l_shipdate date"),
                    dbc.ListGroupItem("12. l_commitdate date"),
                    dbc.ListGroupItem("13. l_receipdate date"),
                    dbc.ListGroupItem("14. l_shipinstruct character"),
                    dbc.ListGroupItem("15. l_shipmode character"),
                    dbc.ListGroupItem("16. l_comment character varying")
                ])
                )),
            id="collapse8",
            is_open=False,
        ),
    ],
    flush=True,
)

modal = html.Div([
        dbc.Modal(
            [
                dbc.ModalHeader(dbc.ModalTitle("Visual Plan - Detailed View")),
                dbc.ModalBody("This is for visual plan."),
            ],
            id="modal-visual",
            size="lg",
            is_open=False,
        ),
        dbc.Modal(
            [
                dbc.ModalHeader(dbc.ModalTitle("About")),
                dbc.ModalBody("This is a CZ4032 Database System Principles Project."),
            ],
            id="modal-about",
            size="lg",
            is_open=False,
        ),
        dbc.Modal(
            [
                dbc.ModalHeader(dbc.ModalTitle("Datasets")),
                dbc.ModalBody("In this project, datasets name TPC-H are loaded into PostgreSQL RDBMS."),
                list_group
            ],
            id="modal-datasets",
            size="lg",
            is_open=False,
        ),
])

heading = dbc.NavbarSimple(
    children=[
        dbc.NavItem(dbc.NavLink("About", href="#", id="aboutLink", style={'color':'lightsteelblue', "font-family" : "Mach OT W03 Condensed Medium", "font-size" : "17px"})),
        dbc.NavItem(dbc.NavLink("Datasets", href="#", id="datasetsLink", style={'color':'lightsteelblue', "font-family" : "Mach OT W03 Condensed Medium", "font-size" : "17px"})),
    ],
    brand="PROJECT 2",
    brand_style={'color':'lightsteelblue', 'font-weight' : 'bold', "font-family" : "Mach OT W03 Condensed Medium", "font-size" : "20px"},
    color="dark"
)

test_png = 'image\info.png'
test_base64 = base64.b64encode(open(test_png, 'rb').read()).decode('ascii')

sql_query_div = html.Div(children=[
                            dbc.Label("SQL Query",
                            style={"font-family" : "Mach OT W03 Condensed Medium", "font-size" : "20px"}),
                            html.Img(src='data:image/png;base64, {}'.format(test_base64), id="sqlImage", style={'height' : 20, 'padding-left' : 5}),
                            dbc.Popover(
                                "Example : SELECT * FROM nation ",
                                target="sqlImage",
                                body=True,
                                trigger="hover",
                            ),
                            dcc.Textarea(
                                id='textarea-sql-query',
                                placeholder='Enter your SQL query here.',
                                value='SELECT * FROM customer C, orders O WHERE C.c_custkey = O.o_custkey', 
                                style={'width': '100%', 'height': 230, 'resize': 'none','font-size' :"18px"},
                            ),
                            html.Br(),
                            dbc.Button("Submit", id='query_submit_button', n_clicks=0, outline= True, color='primary', className='me-1', size='sm',
                                        style={'float': 'right', 'font-size' : '17px'}
                                        ),
                            dcc.Loading(
                                id="loading-2",
                                children=[html.Div([html.Div(id="Spinner")])],
                                type="circle",
                            ),
                        ], 
                            style={'width': '35%', 'height': 250, "padding-left" : 10},
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
                            html.Br(),
                            dbc.Button('Visualize', outline= True, color='primary', className='me-1', size='sm',
                                        style={'float': 'right', 'visibility': 'hidden'}
                                        ),
                            ], 
                            style={'width': '65%', 'height': 250, 'padding-left': 10},
                        )

visualization_div = html.Div([
                        dbc.Label("Visualize Plan",
                        style={"font-size" : "20px"}),
                        dcc.Textarea(
                                id='graph-nl-steps',
                                placeholder='show flowchart here',
                                style={'width': '100%', 'height': 230, 'resize': 'none', "font-size" : "18px"},
                                readOnly=True
                            ),
                        html.Br(),
                        dbc.Button('Detailed View', outline= True, color='primary', className='me-1', size='sm',
                                        style={'float': 'right', 'margin-bottom': 10, "font-size" : "17px"}, id="visualButton"),
                    ],
                        style={'width': '35%','padding-top': 70, 'padding-left': 30, "font-family" : "Mach OT W03 Condensed Medium"}
                    )

body_right = html.Div([
            dbc.Label("Natural Language Description", style={"font-size" : "20px"}),
            html.Div(children=[], id="table1", style={'width': '100%', "font-size" : "18px"})
            ], 
            style={'width': '60%', 'height': 250, 'padding-left': 30}
)

body_top_left = html.Div(children=[
                    sql_query_div,
                    body_right
                ],
                style={'width': '100%', 'display': 'flex','padding-top':30, 'padding-left': 20, 'padding-right': 10, 
                "font-family" : "Mach OT W03 Condensed Medium"}
                )

body_left = html.Div(children=[body_top_left, visualization_div], style={'width': '100%'})

app.layout = html.Div(children=[
                modal,
                heading,
                body_left
            ])

@app.callback([Output("button",'disabled'), Output("Spinner", "children")], Input("query_submit_button", "value"))
def input_triggers_spinner(value):
    time.sleep(1)
    return value

def toggle_modal(n1, is_open):
    if n1:
        return not is_open
    return is_open

app.callback(
    Output("collapse1", "is_open"), Input("custAttr", "n_clicks"), State("collapse1", "is_open"),
)(toggle_modal)

app.callback(
    Output("collapse2", "is_open"), Input("suppAttr", "n_clicks"), State("collapse2", "is_open"),
)(toggle_modal)

app.callback(
    Output("collapse3", "is_open"), Input("nationAttr", "n_clicks"), State("collapse3", "is_open"),
)(toggle_modal)

app.callback(
    Output("collapse4", "is_open"), Input("regionAttr", "n_clicks"), State("collapse4", "is_open"),
)(toggle_modal)

app.callback(
    Output("collapse5", "is_open"), Input("partAttr", "n_clicks"), State("collapse5", "is_open"),
)(toggle_modal)

app.callback(
    Output("collapse6", "is_open"), Input("partsuppAttr", "n_clicks"), State("collapse6", "is_open"),
)(toggle_modal)

app.callback(
    Output("collapse7", "is_open"), Input("orderAttr", "n_clicks"), State("collapse7", "is_open"),
)(toggle_modal)

app.callback(
    Output("collapse8", "is_open"), Input("lineitemAttr", "n_clicks"), State("collapse8", "is_open")
)(toggle_modal)
app.callback(
    Output("collapseNLD", "is_open"), Input("NLDQuery", "n_clicks"), State("collapseNLD", "is_open")
)(toggle_modal)

app.callback(
    Output("modal-about", "is_open"),
    Input("aboutLink", "n_clicks"),
    State("modal-about", "is_open"),
)(toggle_modal)

app.callback(
    Output("modal-datasets", "is_open"),
    Input("datasetsLink", "n_clicks"),
    State("modal-datasets", "is_open"),
)(toggle_modal) 

app.callback(
    Output("modal-visual", "is_open"),
    Input("visualButton", "n_clicks"),
    State("modal-visual", "is_open"),
)(toggle_modal) 

def recursive_display(dict_output, div_components = [], title_padding=0, table_padding=1):
    if (dict_output == {}):
        return [], []
    # print(dict_output)
    dict_keys = list(dict_output.keys())
    dict_values = list(dict_output.values())
    for i in range(len(dict_keys)):
        # print(str(dict_keys[i]))
        div_component = html.Div(
                children=[
                    dbc.ListGroupItem(str(dict_keys[i]) + ": ", style={"font-weight": "bold", "padding-left": title_padding}, id="NLDQuery", href="#", n_clicks=0)
                ])
        div_components.append(div_component)
        divider = html.Div(className="vl", style={'width':'max-width', 'border-top':'2px solid black', 'padding-right': 10})
        div_components.append(divider)
        print("After adding title and divider")
        print(div_components)
        # dict_object = {'Query': str(dict_keys[i]), 'Natural Language Description': str(dict_keys[i])}
        # dict_array.append(dict_object)
        nested_dict = dict_values[i]
        # print(list(nested_dict.keys()))
        nested_dict_keys = list(nested_dict.keys())
        nested_dict_values = list(nested_dict.values())
        dict_array = []
        subquery_dict_array = []
        for j in range(len(nested_dict_keys)):
            # print(nested_dict_keys[j])
            if (nested_dict_keys[j] == "subqueries"):
                subquery_dict_array, div_components = recursive_display(nested_dict_values[j], div_components, title_padding + 10, table_padding + 10)
                # div_component=html.Div(dbc.Collapse(dbc.Card(dbc.CardBody(div_component)), id="collapseNLD", is_open=False))
                # div_components.append(div_component)
            else:
                dict_temp_array = nested_dict_values[j]
                dict_temp_str = ""
                for item in dict_temp_array:
                    dict_temp_str += item + "\n"
                
                dict_object = {'Query': str(nested_dict_keys[j]), 'Natural Language Description': str(dict_temp_str)}
                dict_array.append(dict_object)
        dict_array.extend(subquery_dict_array)
                # dict_array.append(dict_object)
        for i in range(len(dict_array)):
            if "Natural Language Description" in dict_array[i].keys():
                val = dict_array[i]['Natural Language Description']
                print("The extracted dic is : " + val)
                res = re.findall(r'"([^"]*)"', val)
                html.Span()
                print(str(res))
        print(dict_array)
        print("before adding datatable")
        print(div_components)
        if table_padding == 0 :
            div_component = dash_table.DataTable(data=dict_array,
                    style_table={
                        'padding-left': table_padding,
                        'padding-top': 10,
                        'padding-bottom': 10,
                        'width': 'auto',
                        'max-height': '500px', 
                        'overflowY': 'auto'
                    },
                    style_header={"font-weight" : "bold"},
                    style_as_list_view=True,
                    style_data={'whiteSpace': 'pre-line', 'height': 'auto'},
                    style_cell={'textAlign': 'left', "font-family" : "Mach OT W03 Condensed Medium"},
                    )
        else : 
            div_component = html.Div(dbc.Collapse(dbc.Card(dbc.CardBody(dash_table.DataTable(data=dict_array,
                    style_table={
                        'padding-left': table_padding,
                        'padding-top': 10,
                        'padding-bottom': 10,
                        'width': 'auto',
                        'max-height': '500px', 
                        'overflowY': 'auto'
                    },
                    style_header={"font-weight" : "bold"},
                    style_as_list_view=True,
                    style_data={'whiteSpace': 'pre-line', 'height': 'auto'},
                    style_cell={'textAlign': 'left', "font-family" : "Mach OT W03 Condensed Medium"},
                    ))), id="collapseNLD", is_open=False))
        
        div_components.append(div_component) 
    print(div_components)
    return dict_array, div_components 

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
    # dict_keys = list(dict_output.keys())
    # dict_values = list(dict_output.values())
    # # # print(str(dict_keys))
    # # # print(str(dict_values))
    # # # print(len(dict_output))
    # # dict_keys_output = ""
    # # dict_values_output = ""
    print("hello there")
    lst_curr_object, div_components = recursive_display(dict_output, html_output)
    html_output.extend(div_components)
    print("done executing")
    # dict_array = []
    # for i in range(len(dict_keys)):
    #     # print(str(dict_keys[i]))
    #     div_component = html.Div(str(dict_keys[i]) + ": ", style={"font-weight": "bold"})
    #     html_output.append(div_component)
    #     divider = html.Div(className="vl", style={'width':'max-width', 'border-top':'2px solid black', 'padding-right': 10})
    #     html_output.append(divider)
    #     # dict_object = {'Query': str(dict_keys[i]), 'Natural Language Description': str(dict_keys[i])}
    #     # dict_array.append(dict_object)
    #     nested_dict = dict_values[i]
    #     # print(list(nested_dict.keys()))
    #     nested_dict_keys = list(nested_dict.keys())
    #     nested_dict_values = list(nested_dict.values())
    #     for j in range(len(nested_dict_keys)):
    #         # print(nested_dict_keys[j])
    #         if (nested_dict_keys[j] == "subqueries"):
    #             nested_dict_subquery = nested_dict_values[j]
    #             # print(nested_dict_subquery)
    #             nested_dict_subquery_keys = list(nested_dict_subquery.keys())
    #             nested_dict_subquery_values = list(nested_dict_subquery.values())
    #             dict_array = []
    #             for k in range(len(nested_dict_subquery_keys)):
    #                 div_component = html.Div(str(nested_dict_subquery_keys[k] + ": "), style={"padding-left": 20})
    #                 html_output.append(div_component)
    #                 nested_subquery_values = nested_dict_subquery_values[k]
    #                 nested_subquery_values_keys = list(nested_subquery_values.keys())
    #                 nested_subquery_values_values = list(nested_subquery_values.values())
                    
    #                 for l in range(len(nested_subquery_values_keys)):
    #                     if (nested_subquery_values_keys[l] == "subqueries"):
    #                         nested_nested_dict_subquery = nested_subquery_values_values[l]
    #                         # print(nested_nested_dict_subquery)
    #                     else:
    #                         dict_temp_array = nested_subquery_values_values[l]
    #                         dict_temp_str = ""
    #                         for item in dict_temp_array:
    #                             dict_temp_str += item + "\n"
    #                         dict_object = {'Query': str(nested_subquery_values_keys[l]), 'Natural Language Description': str(dict_temp_str)}
    #                         dict_array.append(dict_object)
    #                 # print(dict_array)
    #                 div_component = dash_table.DataTable(data=dict_array, 
    #                 style_table={
    #                     'padding-left': 20,
    #                     'padding-bottom': 10,
    #                     'width': 'auto',
    #                     'max-height': '580px', 
    #                     'overflowY': 'auto'
    #                 },
    #                 style_data={
    #                 'whiteSpace': 'pre-line',
    #                 'height': 'auto'
    #                 },
    #                 style_cell={'textAlign': 'left'})
    #                 html_output.append(div_component)
    #                 dict_array = []

    #         else:
    #             dict_temp_array = nested_dict_values[j]
    #             dict_temp_str = ""
    #             for item in dict_temp_array:
    #                 dict_temp_str += item + "\n"
                
    #             dict_object = {'Query': str(nested_dict_keys[j]), 'Natural Language Description': str(dict_temp_str)}
    #             dict_array.append(dict_object)
    #     print(dict_array)
    #     div_component = dash_table.DataTable(data=dict_array, 
    #                 style_table={
    #                     'padding-left': 1,
    #                     'padding-bottom': 10,
    #                     'width': 'auto',
    #                     'max-height': '580px', 
    #                     'overflowY': 'auto'
    #                 },
    #                 style_data={
    #                 'whiteSpace': 'pre-line',
    #                 'height': 'auto'
    #                 },
    #                 style_cell={'textAlign': 'left'})
    #     html_output.append(div_component) 
     



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