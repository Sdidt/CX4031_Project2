from postorder import Node

def nested_loop_define(node : Node, condition,index_column_dict):
    join_filter = node.information["Join Filter"]
    result = "The rows are sorted based on their keys."

    if(join_filter):
        result = "The join results between the nested loop scans of the suboperations are returned as new rows."



    else : 
        result = "The join results between the nested loop scans of the suboperations are returned as new rows."


    return result