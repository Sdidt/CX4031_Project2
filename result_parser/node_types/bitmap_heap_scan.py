from node import Node

def bitmap_heap_scan_define(node : Node, condition,index_column_dict):
    index_information = index_column_dict[node.information["Index Name"]]
    result = ""
    result += "The clause " + condition + " executes a Bitmap Heap Scan after the bitmap was created by Bitmap Index Scan on relation + " + node.information["Relation Name"] +". It retrieves rows based on the bitmap"
    if "Recheck Cond" in node.information:
        result += " with the condition: {}".format( node.information["Recheck Cond"].replace("::text", ""))
    result += "."
    return result

