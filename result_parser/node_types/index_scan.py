from node import Node


def index_scan_define(node : Node,condition, index_column_dict):

    """    
    Returns the natural langauge description of the Index Scan Node Types
    
    Paramemter node: the Node object.

    Paramemter condition: The specific SQL Query clause.

    Parameter index_column_dict: The dictionary that contains the index names and their respecitve tables and table columns.  
   
    """

    index_information = index_column_dict[node.information["Index Name"]]
    result = ""
    result += "The clause "  + condition  + " is done using Index Scan on table " + index_information[0] + " using the created index " + node.information["Index Name"] + " on column " + index_information[1]
    if "Index Cond" in node.information:
         result += " with the following conditions: " + node.information["Index Cond"].replace("::text", "")
        
    if "Filter" in node.information:
        result += (
            " The result is then filtered by "
            + node.information["Filter"].replace("::text", "")
            + "."
        )
    return result


def index_only_scan_define(node : Node,condition, index_column_dict):

    """    
    Returns the natural langauge description of the Index Only Scan Node Types
    
    Paramemter node: the Node object.

    Paramemter condition: The specific SQL Query clause.

    Parameter index_column_dict: The dictionary that contains the index names and their respecitve tables and table columns.  
   
    """
    
    index_information = index_column_dict[node.information["Index Name"]]
    result = ""
    result += "The clause "  + condition  + " is done using Index Only Scan on table " + index_information[0] + " using the created index " + node.information["Index Name"] + " on column " + index_information[1]
    
    if "Index Cond" in node.information:
         result += " with the following conditions: " + node.information["Index Cond"].replace("::text", "")
        
    if "Filter" in node.information:
        result += (
            " The result is then filtered by "
            + node.information["Filter"].replace("::text", "")
            + "."
        )
    return result

