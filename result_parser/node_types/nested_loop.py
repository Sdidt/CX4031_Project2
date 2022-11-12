from node import Node
import re

def nested_loop_define(node : Node, condition,index_column_dict):


	""" 
    
    Returns the natural langauge description of the Nested Loop Node Types
    
    Paramemter node: the Node object.

    Paramemter condition: The specific SQL Query clause.

    Parameter index_column_dict: The dictionary that contains the index names and their respecitve tables and table columns.  
   
    """

	join_type = node.type
	join_filter = node.join_filters[0]
	index_name = node.index_name
	relation_list = re.findall("([a-zA-Z0-9]+)\." ,join_filter)
	result = "The clause " + condition + " is a " + join_type + " between table " + relation_list[0] + " and table " + relation_list[1] + " based on the filter " + join_filter
	if (index_name):
		index_information = index_column_dict[index_name]
		result += " using the created " + index_name + " on column " + index_information[1]
	if (" = " not in join_filter):
		result += " since it is a non-equi join and hash join is suboptimal, but also"

	return result
