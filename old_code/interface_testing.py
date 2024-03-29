from dash import Dash, html, dcc, Input, Output, State, dash_table, ctx, dash
import dash_bootstrap_components as dbc
import time, base64

from pytermgui import background
from driver import process_query
# import plotly.express as px
# import pandas as pd
# from pytermgui import auto, keys

# dict_ = {'key 1': 'value 1', 'key 2': 'value 2', 'key 3': 'value 3'}
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


sql_query_example = html.Div(
    dbc.Accordion(
        [
            dbc.AccordionItem(
                "SELECT * FROM customer C, orders O WHERE C.c_custkey = O.o_custkey", title="Query 1"
            ),
            dbc.AccordionItem(
                "SELECT * FROM nation WHERE nation.n_nationkey = 3", title="Query 2"
            ),
            dbc.AccordionItem(
                "SELECT ps_partkey PS, sum(ps_supplycost * ps_availqty) as value FROM partsupp P, supplier, nation WHERE" +
                " P.ps_suppkey = s_suppkey AND NOT s_nationkey = n_nationkey AND n_name = 'GERMANY' AND ps_supplycost > 20" +
                " and s_acctbal > 10 GROUP BY ps_partkey HAVING sum(ps_supplycost * ps_availqty) > ( SELECT sum(ps_supplycost *" +
                " ps_availqty) * 0.0001000000 FROM partsupp P1, supplier S, nation WHERE ps_suppkey = s_suppkey AND s_nationkey" +
                " = n_nationkey AND n_name = 'GERMANY') order by value;", title="Query 3"
            ),
            dbc.AccordionItem(
                "SELECT n_nationkey FROM nation UNION SELECT s_nationkey FROM supplier", title="Query 4"
            ),
            dbc.AccordionItem(
                "SELECT * FROM part WHERE p_brand = 'Brand#13' AND p_size <> (SELECT max(p_size) FROM part);", title="Query 5"
            ),
            dbc.AccordionItem(
                "SELECT n_nationkey FROM nation WHERE n_nationkey = 3 UNION SELECT s_nationkey FROM supplier", title="Query 6"
            ),
            dbc.AccordionItem(
                "SELECT l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty, sum(l_extendedprice) AS sum_base_price," +
                " sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price, sum(l_extendedprice * (1 - l_discount) *" +
                " (1 + l_tax)) AS sum_charge FROM lineitem WHERE l_shipdate <= date '1998-12-01' GROUP BY l_returnflag," +
                " l_linestatus ORDER BY sum_disc_price, l_linestatus", title="Query 7"
            ),
            dbc.AccordionItem(
                "SELECT sum(l_extendedprice * l_discount) AS revenue FROM lineitem WHERE l_extendedprice > 100;", title="Query 8"
            ),
            dbc.AccordionItem(
                "SELECT p_brand, p_type, p_size, COUNT(DISTINCT ps_suppkey) AS supplier_cnt FROM partsupp, part WHERE p_partkey" +
                " = ps_partkey AND p_brand <> 'Brand#45' AND p_type NOT LIKE 'MEDIUM POLISHED%' AND p_size IN (49, 14, 23, 45, 19," +
                " 3, 36, 9) AND ps_suppkey NOT IN ( SELECT s_suppkey FROM supplier WHERE s_comment LIKE '%Customer%Complaints%')" +
                " GROUP BY p_brand, p_type, p_size ORDER BY supplier_cnt DESC, p_brand, p_type, p_size", title="Query 9"
            ),
        ],
        start_collapsed=True,
    ),
)

modal = html.Div([
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
        dbc.Modal(
            [
                dbc.ModalHeader(dbc.ModalTitle("Example query")),
                dbc.ModalBody("Here are some SQL query examples"),
                sql_query_example
            ],
            id="modal-example",
            size="lg",
            is_open=False,
        ),
        html.Div([dbc.Modal(
            [
                dbc.ModalHeader([dbc.ModalTitle("Loading"), dbc.Spinner(color="primary")], close_button=False)
            ],
            backdrop="static",
            id="modal-loading",
            size="lg",
            is_open=False,
        )], id="loadingdiv"),
        dbc.Modal(
            [
                dbc.ModalHeader([dbc.ModalTitle("Error")]),
                dbc.ModalBody("The database had an internal error, or the query was too complex and timed out. Kindly recheck your input SQL query!"),
            ],
            size="lg",
            is_open=False,
            id="error-message"
        ),
        dbc.Modal(
            [
                dbc.ModalHeader([dbc.ModalTitle("Error")]),
                dbc.ModalBody("The connection to the DB was not successful. Kindly check your DB connection parameter inputs!"),
            ],
            size="lg",
            is_open=False,
            id="connection-error-message"
        ),
        dbc.Modal(
            [
                dbc.ModalHeader([dbc.ModalTitle("Error")]),
                dbc.ModalBody("The port specified is incorrect. Kindly check your port parameter, which must be an integer!"),
            ],
            size="lg",
            is_open=False,
            id="port-error-message"
        ),
        dbc.Modal(
            [
                dbc.ModalHeader([dbc.ModalTitle("Disclaimer")]),
                dbc.ModalBody("Note that the QEP and AQP generation takes some time to finish. Please wait a few moments upon clicking the submit button. Thank you.")
            ],
            backdrop="static",
            id="modal-startup",
            size="lg",
            is_open=True,
        ),
        dbc.Modal(
            [
                dbc.ModalHeader(dbc.ModalTitle("Database Details")),
                dbc.ModalBody("Database details successfully submitted"),
            ],
            id="modal-database",
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

connection_details = html.Div(children=[
    dbc.CardHeader(
        children=[dbc.Label("DB Connection Details",style={"font-family" : "Mach OT W03 Condensed Medium", "font-size" : "20px"})]
    ),
    dbc.CardBody([html.Div([dbc.Label("Host:", style={'padding-right': 5}),dcc.Input(id="host-input", type="text", style={'width': '182px'}), 
                dbc.Label("Database Name:", style={'padding-left':10, 'padding-right': 5}),dcc.Input(id="dbname-input", type="text", style={'width': '182px'}),
                dbc.Label("Username:", style={'padding-left':10, 'padding-right': 5}),dcc.Input(id="username-input", type="text", style={'width': '182px'}),
                dbc.Label("Password:", style={'padding-left':10, 'padding-right': 5}),dcc.Input(id="password-input", type="text", style={'width': '182px'}),
                dbc.Label("Port:", style={'padding-left':10, 'padding-right': 5}),dcc.Input(id="port-input", type="text", style={'width': '182px'})], style={'display': 'flex', 'justify-content': 'center'}),
                html.Br(),
                dbc.Button("Submit", id='dbdetails_submit_button', n_clicks=0, outline= True, color='primary', className='me-1', size='sm',
                    style={'margin-right': 5, 'float': 'right', 'font-size' : '17px'}),
                html.Div(className="vl", style={'width':'max-width', 'border-top':'2px solid rgba(0,0,0,.25)', 'padding-right': 10, 'margin-top': 50})]),
    
])

test_png = 'image/info.png'
test_base64 = base64.b64encode(open(test_png, 'rb').read()).decode('ascii')

sql_query_div = html.Div(children=[
    dbc.CardHeader(
        children=[dbc.Label("SQL Query", style={"font-family" : "Mach OT W03 Condensed Medium", "font-size" : "20px"}),
            html.Img(src='data:image/png;base64, {}'.format(test_base64), id="sqlImage", style={'height' : 20, 'padding-left' : 5})]
    ),
    dbc.CardBody([
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
            style={'width': '100%', 'height': 280, 'resize': 'none','font-size' :"18px"},
        ),
        html.Br(),
        dbc.Button("Submit", id='query_submit_button', n_clicks=0, outline= True, color='primary', className='me-1', size='sm',
            style={'float': 'right', 'font-size' : '17px'}),
        dbc.Button("Example", id='example_button', n_clicks=0, outline= True, color='primary', className='me-1', size='sm',
            style={'float': 'right', 'font-size' : '17px'}),
    ]),
])

sql_cards = html.Div([
    dbc.Row(
        [
            dbc.Col(dbc.Card(sql_query_div, color="light")),
        ],
        className="mb-4",
    )],
    style={"width": "30rem", "height" : '50rem'}
)

# sql_query_div = html.Div(children=[
#                             dbc.Label("SQL Query",
#                             style={"font-family" : "Mach OT W03 Condensed Medium", "font-size" : "20px"}),
#                             html.Img(src='data:image/png;base64, {}'.format(test_base64), id="sqlImage", style={'height' : 20, 'padding-left' : 5}),
#                             dbc.Popover(
#                                 "Example : SELECT * FROM nation ",
#                                 target="sqlImage",
#                                 body=True,
#                                 trigger="hover",
#                             ),
#                             dcc.Textarea(
#                                 id='textarea-sql-query',
#                                 placeholder='Enter your SQL query here.',
#                                 value='SELECT * FROM customer C, orders O WHERE C.c_custkey = O.o_custkey', 
#                                 style={'width': '100%', 'height': 230, 'resize': 'none','font-size' :"18px"},
#                             ),
#                             html.Br(),
#                             dbc.Button('Submit', id='query_submit_button', n_clicks=0, outline= True, color='primary', className='me-1', size='sm',
#                                         style={'float': 'right', 'font-size' : '17px'}
#                                         ),
#                         ], 
#                             style={'width': '35%', 'height': 250, "padding-left" : 10},
#                 )

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
                            style={'width': '65%', 'height': 250, 'padding-left': 20},
                        )

body_right = html.Div([
            dbc.Label("Natural Language Description", style={"font-size" : "20px"}),
            html.Div(children=[], id="table1", style={'width': '100%', "font-size" : "18px"})
            ], 
            style={'width': '65%', 'height': 250, 'padding-left': 20}
)

body_top_left = html.Div(children=[
                    sql_cards,
                    body_right
                ],
                style={'width': '100%', 'display': 'flex','padding-top':30, 'padding-left': 10, 'padding-right': 10, 
                "font-family" : "Mach OT W03 Condensed Medium"}
                )

body_left = html.Div(children=[body_top_left], style={'width': '100%'})

spinner_boolean = html.Div(children="true", id="loading-boolean", style={'display': 'none'})
error_boolean = html.Div(children="false", id="error-boolean", style={'display': 'none'})

popup = html.Div(dbc.Spinner(color="primary"), id="spinner")

app.layout = html.Div(children=[
                modal,
                heading,
                connection_details,
                body_left,
                spinner_boolean,
                error_boolean
            ])

def recursive_display(dict_output, div_components: list = [], title_padding=0, table_padding=1):
    if (dict_output == {}):
        print("Base case")
        return [], div_components
    # print(dict_output)
    dict_keys = list(dict_output.keys())
    dict_values = list(dict_output.values())
    print(dict_keys)
    for i in range(len(dict_keys)):
        print("ITERATION {}:".format(i))
        dict_array = []
        # print(str(dict_keys[i]))
        div_component = html.Div(str(dict_keys[i]) + ": ", style={"font-weight": "bold", "padding-left": title_padding})
        div_components.append(div_component)
        divider = html.Div(className="vl", style={'width':'max-width', 'border-top':'2px solid black', 'padding-right': 10})
        div_components.append(divider)
        # print("After adding title and divider")
        # print(div_components)
        # dict_object = {'Query': str(dict_keys[i]), 'Natural Language Description': str(dict_keys[i])}
        # dict_array.append(dict_object)
        nested_dict = dict_values[i]
        # print(list(nested_dict.keys()))
        nested_dict_keys = list(nested_dict.keys())
        nested_dict_values = list(nested_dict.values())
        subquery_dict_array = []
        for j in range(len(nested_dict_keys)):
            # print(nested_dict_keys[j])
            if (nested_dict_keys[j] == "subqueries"):
                subquery_dict_array, div_components = recursive_display(nested_dict_values[j], div_components, title_padding + 10, table_padding + 10)
                # print(div_components)
            else:
                dict_temp_array = nested_dict_values[j]
                dict_temp_str = ""
                for i, item in enumerate(dict_temp_array):
                    dict_temp_str += "{}. ".format(i + 1) + item + "\n"
                
                dict_object = {'Query': str(nested_dict_keys[j]), 'Natural Language Description': str(dict_temp_str)}
                dict_array.append(dict_object)
        # dict_array.extend(subquery_dict_array)
                # dict_array.append(dict_object)
        print(dict_array)
        # print("before adding datatable")
        # print(div_components)
        div_component = dash_table.DataTable(data=dict_array, 
                    style_table={
                        'padding-left': table_padding,
                        'padding-top': 10,
                        'padding-bottom': 10,
                        'width': 'auto',
                        'max-height': '580px', 
                        'overflowY': 'auto'
                    },
                    style_data={'whiteSpace': 'pre-line','height': 'auto'},
                    style_header={"font-weight" : "bold"},
                    style_as_list_view=True,
                    style_cell={'textAlign': 'left', "font-family" : "Mach OT W03 Condensed Medium"})
        div_components.append(div_component) 
    # print(div_components)
    return dict_array, div_components 

def toggle_modal(n1, is_open):
    if n1:
        return not is_open
    return is_open

app.callback(
    Output("modal-example", "is_open"), Input("example_button", "n_clicks"), State("modal-example", "is_open"),
)(toggle_modal)

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
    Output("modal-about", "is_open"), Input("aboutLink", "n_clicks"), State("modal-about", "is_open"),
)(toggle_modal)

app.callback(
    Output("modal-datasets", "is_open"), Input("datasetsLink", "n_clicks"), State("modal-datasets", "is_open"),
)(toggle_modal)

# @app.callback(
#     Output('loading-boolean', 'children'),
#     Input('textarea-sql-query', 'value'),
# )
# def change_boolean(value):
#     return "true"

@app.callback(
    Output('modal-database', 'is_open'),
    Output('port-error-message', 'is_open'),
    Input('dbdetails_submit_button', 'n_clicks'),
    State('host-input', 'value'),
    State('dbname-input', 'value'),
    State('username-input', 'value'),
    State('password-input', 'value'),
    State('port-input', 'value')
)
def get_database_inputs(n_clicks, host, dbname, username, password, port):
    if n_clicks:
        try:
            port = int(port)
        except Exception as e:
            print("display error message: {}".format(e))
            return False, True
        return True, False

@app.callback(
    Output('modal-loading', 'is_open'),
    Input('query_submit_button', 'n_clicks'),
    Input('textarea-sql-query', 'value'),
    State('loading-boolean', 'children')
)
def open_loading(n_clicks, value, loading):
    print(ctx.inputs)
    # print(error)
    # print(str(ctx.triggered_id))
    # print(n_clicks)
    # print(loading)
    # print(value)
    if n_clicks==1 and loading=="true":
        return True
    if n_clicks>1 and ctx.triggered_id=="query_submit_button":
        print('came here')
        return True

# def load_output(n):
#     if n:
#         time.sleep(5)
#         return {'display':'none'}

@app.callback(
    Output('table1', 'children'),
    Output('loadingdiv', 'children'),
    Output('loading-boolean', 'children'),
    Output('error-message', 'is_open'),
    Output('connection-error-message', 'is_open'),
    Input('query_submit_button', 'n_clicks'),
    State('textarea-sql-query', 'value'),
    State('host-input', 'value'),
    State('dbname-input', 'value'),
    State('username-input', 'value'),
    State('password-input', 'value'),
    State('port-input', 'value')
)
def update_output(n_clicks, value, host, dbname, username, password, port):
    # print(open)
    # open_loading(n_clicks, value, "", "")
    # print(value)
    # print(n_clicks)
    # print("QUERY OUTPUT: {}".format(query_output))
    # html_output = []
    # dict_output = {'query_1': {'subqueries': 
    #     {'subquery_1': {'subqueries': {}, 'from partsupp P1 , supplier S , nation': ['The clause "from p1" is implemented using Seq Scan because no other option is available.', 'The clause "from s" is implemented using Seq Scan because it requires 44.62, 10351967.39 less operations than Bitmap Heap Scan, Index Scan respectively.', 'The clause "from nation_1" is implemented using Seq Scan because it requires 2.90, 277777779.19 less operations than Bitmap Heap Scan, Index Scan respectively.'], "where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY'": ['The clause "where nation_1.n_name = \'GERMANY\'" is implemented using Seq Scan because it requires 2.90, 277777779.19 less operations than Bitmap Heap Scan, Index Scan respectively.', 'The clause "where s.s_nationkey = nation_1.n_nationkey" is implemented using Hash Join.', 'The clause "where p1.ps_suppkey = s.s_suppkey" is implemented using Hash Join.']}}
    #     , 'from partsupp P , supplier , nation': ['The clause "from p" is implemented using Seq Scan because no other option is available.', 'The clause "from nation" is implemented using Seq Scan because no other option is available.', 'The clause "from supplier" is implemented using Seq Scan because it requires 124.21, 28818445.30 less operations than Bitmap Heap Scan, Index Scan respectively.'], "where P.ps_suppkey = s_suppkey and not s_nationkey = n_nationkey and n_name = 'GERMANY' and ps_supplycost > 20 and s_acctbal > 10": ['The clause "where p.ps_supplycost > \'20\'" is implemented using Seq Scan because no other option is available.', 'The clause "where nation.n_name = \'GERMANY\'" is implemented using Seq Scan because no other option is available.', 'The clause "where supplier.s_acctbal > \'10\'" is implemented using Seq Scan because it requires 124.21, 28818445.30 less operations than Bitmap Heap Scan, Index Scan respectively.', 'The clause "where supplier.s_nationkey <> nation.n_nationkey" is implemented using Nested Loop.', 'The clause "where p.ps_suppkey = supplier.s_suppkey" is implemented using Hash Join.'], 'order by value ;': ['The clause "order by sum((p.ps_supplycost * (p.ps_availqty) asc" is implemented using external merge Sort.']
    #     }
    # }
    # set_output = {'query_1': {'subqueries': {}, 'select': 'select n_nationkey', 'from': 'from nation', 'intersect': 'intersect query_2'}, 'query_2': {'subqueries': {}, 'select': 'select s_nationkey', 'from': 'from supplier'}}
    
    if n_clicks > 0:
        print("HOST: {}".format(host))
        print("DB NAME: {}".format(dbname))
        print("USERNAME: {}".format(username))
        print("PASSWORD: {}".format(password))
        print("PORT: {}".format(port))
        query_output = process_query(value, host, dbname, username, password, port)
        # query_output = {}
    #     query_output = {'query_1': {'subqueries': 
    #     {'subquery_1': {'subqueries': {}, 'from partsupp P1 , supplier S , nation': ['The clause "from p1" is implemented using Seq Scan because no other option is available.', 'The clause "from s" is implemented using Seq Scan because it requires 44.62, 10351967.39 less operations than Bitmap Heap Scan, Index Scan respectively.', 'The clause "from nation_1" is implemented using Seq Scan because it requires 2.90, 277777779.19 less operations than Bitmap Heap Scan, Index Scan respectively.'], "where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY'": ['The clause "where nation_1.n_name = \'GERMANY\'" is implemented using Seq Scan because it requires 2.90, 277777779.19 less operations than Bitmap Heap Scan, Index Scan respectively.', 'The clause "where s.s_nationkey = nation_1.n_nationkey" is implemented using Hash Join.', 'The clause "where p1.ps_suppkey = s.s_suppkey" is implemented using Hash Join.']}}
    #     , 'from partsupp P , supplier , nation': ['The clause "from p" is implemented using Seq Scan because no other option is available.', 'The clause "from nation" is implemented using Seq Scan because no other option is available.', 'The clause "from supplier" is implemented using Seq Scan because it requires 124.21, 28818445.30 less operations than Bitmap Heap Scan, Index Scan respectively.'], "where P.ps_suppkey = s_suppkey and not s_nationkey = n_nationkey and n_name = 'GERMANY' and ps_supplycost > 20 and s_acctbal > 10": ['The clause "where p.ps_supplycost > \'20\'" is implemented using Seq Scan because no other option is available.', 'The clause "where nation.n_name = \'GERMANY\'" is implemented using Seq Scan because no other option is available.', 'The clause "where supplier.s_acctbal > \'10\'" is implemented using Seq Scan because it requires 124.21, 28818445.30 less operations than Bitmap Heap Scan, Index Scan respectively.', 'The clause "where supplier.s_nationkey <> nation.n_nationkey" is implemented using Nested Loop.', 'The clause "where p.ps_suppkey = supplier.s_suppkey" is implemented using Hash Join.'], 'order by value ;': ['The clause "order by sum((p.ps_supplycost * (p.ps_availqty) asc" is implemented using external merge Sort.']
    #     }
    # }
        if "connection-error" in query_output:
            return html.Div([]), html.Div([]), "false", False, True
        if (query_output == {}):
            print("display error message")
            return html.Div([]), html.Div([]), "false", True, False
        else:
            
            div_components = []
            lst_curr_object, div_components = recursive_display(query_output, [])
            
            html_output = div_components
            
            # print("FINAL RESULT: {}".format(div_components))
            # print("HTML OUTPUT: {}".format(html_output))
            print("done executing")
            # print(html_output)
            return html.Div(html_output), [dbc.Modal(
                [
                    dbc.ModalHeader([dbc.ModalTitle("Loading"), dbc.Spinner(color="primary")], close_button=False)
                ],
                backdrop="static",
                id="modal-loading",
                size="lg",
                is_open=False,
            )], "false", False, False

if __name__ == '__main__':
    app.run_server(debug=True)