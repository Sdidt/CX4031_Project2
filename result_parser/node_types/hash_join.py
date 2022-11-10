from postorder import Node

def hash_join_define(node : Node, condition,index_column_dict):
    result = ""
    result += "The clause " + condition + f" performs a join operation using Hash {node.information['Join Type']} Join based on the result from the previous hash operation"
    if "Hash Cond" in node.information:
        result += " on the condition: {}".format( node.information["Hash Cond"].replace("::text", ""))
    return result

