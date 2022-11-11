from postorder import Node

def hash_join_define(node : Node, condition,index_column_dict):
    result = "The clause " + condition + f" performs a join operation using Hash {node.information['Join Type']} Join based on the result from the previous hash operation"
    join_filter = node.join_filters[0]
    result += " on the condition: {}".format(join_filter)
    return result

