from node import Node

def hash_join_define(node : Node, condition,index_column_dict):
    """    
    Returns the natural langauge description of the Hash Join Node Types
    
    Paramemter node: the Node object.

    Paramemter condition: The specific SQL Query clause.

    Parameter index_column_dict: The dictionary that contains the index names and their respecitve tables and table columns.  
   
    """

    result = "The clause " + condition + f" performs a join operation using Hash {node.information['Join Type']} Join based on the result from the previous hash operation"        
    join_filter = node.join_filters[0]
    result += " on the condition: {}".format(join_filter)
    if (" = " in condition):
        result += " as it is an equi-join, but also"
    return result

