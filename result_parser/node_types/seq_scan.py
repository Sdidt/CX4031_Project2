from node import Node

def seq_scan_define(node : Node, condition,index_column_dict):
    """ 
    
    Returns the natural langauge description of the Sequential Scan Node Types
    
    Paramemter node: the Node object.

    Paramemter condition: The specific SQL Query clause.

    Parameter index_column_dict: The dictionary that contains the index names and their respecitve tables and table columns.  
   
    """

    result = "The clause " + condition+  " performs a sequential scan on relation "
    if "Relation Name" in node.information:
        result += node.information["Relation Name"]
    if "Alias" in node.information:
        if node.information["Relation Name"] != node.information["Alias"]:
            result += " with an alias of {}".format(node.information["Alias"])
    if "Filter" in node.information:
        result += " and filtered with the condition {}".format(
            node.information["Filter"].replace("::bpchar", "")
        )

    return result