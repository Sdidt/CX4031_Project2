from node import Node
import re

## Ideally want to identify the 2 tables that are doing the nested looped Join and what is their Join Filter.
def nested_loop_define(node : Node, condition,index_column_dict):
	join_type = node.type

	join_filter = node.join_filters[0]
	index_name = node.index_name
	relation_list = re.findall("([a-zA-Z0-9]+)\." ,join_filter)
	# print("HElloooo")
	print(relation_list)
	result = "The clause " + condition + " is a " + join_type + " between table " + relation_list[0] + " and table " + relation_list[1] + " based on the filter " + join_filter
	if (index_name):
		index_information = index_column_dict[index_name]
		result += " using the created " + index_name + " on column " + index_information[1]
	if (" = " not in join_filter):
		result += " since it is a non-equi join and hash join is suboptimal, but also"

	return result
