from node import Node

def default_define(node : Node, condition,index_column_dict):
    return "The clause " + condition + " is performed using {}".format(node.type)