from postorder import Node

def default_define(node : Node, condition,index_column_dict):
    return "The {} is performed.".format(node.information["Node Type"])