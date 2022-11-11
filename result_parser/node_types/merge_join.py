from postorder import Node
import re

def merge_join_define(node : Node, condition,index_column_dict):
    result = "The clause " + condition + " performs a Merge Join "
    join_filter = node.join_filters[0]

    merge_cond = node.information["Merge Cond"]
    relation_list = re.findall("([a-zA-Z0-9]+)\." ,merge_cond)
    result += "between table "+ relation_list[0] + "and table " + relation_list[1] + " with the merge condition of " + join_filter
    if (" = " not in join_filter):
        result += " since it is a non-equi join and hash join is suboptimal, but also "

    return result