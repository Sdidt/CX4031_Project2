from postorder import Node


def merge_join_define(node : Node, condition,index_column_dict):
    result = "The results from sub-operations are joined using Merge Join"

    if "Merge Cond" in node.information:
        result += " with condition " + node.information["Merge Cond"].replace("::text", "")
        

    if "Join Type" == "Semi":
        result += " but only the row from the left relation is returned"

    result += "."
    return result