from postorder import Node

def default_define(node : Node, condition,index_column_dict):
    return "The clause " + condition + " performed a {} .".format(node.type)