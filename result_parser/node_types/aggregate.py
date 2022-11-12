from node import Node

def aggregate_define(node : Node, condition,index_column_dict):
    """ 
    
    Returns the natural langauge description of the following Node types: SortAggregate, HashAggregate and PlainAggregate
    
    Paramemter node: the Node object.

    Paramemter condition: The specific SQL Query clause.

    Parameter index_column_dict: The dictionary that contains the index names and their respecitve tables and table columns.  
   
    """

    strategy = node.information["Strategy"]
    if strategy == "Sorted":
        result = "The clause " + condition+ " rows are sorted based on their keys(s)."
        if "Group Key" in node.information:
            result += " They are aggregated by the following keys: "
            for key in node.information["Group Key"]:
                result += key + ","
            result = result[:-1]
            result += "."
        if "Filter" in node.information:
            result += " They are filtered by " + node.information["Filter"].replace("::text", "")
            result += "."
        return result
    elif strategy == "Hashed":
        result = "The clause " + condition+ " hashes all rows based on the following key(s): "
        for key in node.information["Group Key"]:
            result += key.replace("::text", "") + ", "
        result += "which are then aggregated into individual bucket based on a key."
        return result
    elif strategy == "Plain":
        result = "The Clause " + condition+ " simply aggregated as normal"
        return result
    else:
        return "The clause " + condition + " performed aggregation."

