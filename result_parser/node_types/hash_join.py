from postorder import Node

def hash_join_define(node : Node, condition,index_column_dict):
    result = ""
    result += f"The result from previous operation is joined using Hash {node.information['Join Type']} Join"
    if "Hash Cond" in node.information:
        result += " on the condition: {}".format( node.information["Hash Cond"].replace("::text", ""))
    result += "."
    return result

