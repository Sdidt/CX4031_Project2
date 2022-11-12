from node import Node

def default_define(node : Node, condition,index_column_dict):

    """    
    Returns the natural langauge description of the Unknown Node Types
    
    Paramemter node: the Node object.

    Paramemter condition: The specific SQL Query clause.

    Parameter index_column_dict: The dictionary that contains the index names and their respecitve tables and table columns.  
   
    """

    return "The clause " + condition + " is performed using {}".format(node.type)