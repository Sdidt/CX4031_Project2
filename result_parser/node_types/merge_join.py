from postorder import Node
import re

def merge_join_define(node : Node, condition,index_column_dict):
    result = "The clause " + condition + " performs a Merge Join "

    if "Merge Cond" in node.information:
        merge_cond = node.information["Merge Cond"]
        relation_list = re.findall("([a-zA-Z0-9]+)\." ,merge_cond)
        result += "between table "+ relation_list[0] + "and table " + relation_list[1] + " with the merge condition of " + node.information["Merge Cond"]
        
    if "Join Filter" in node.information:
        result += " and a filter condition of " + node.information["Merge Cond"].replace("::bpchar", "")

    return result