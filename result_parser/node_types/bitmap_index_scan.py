from postorder import Node

def bitmap_index_scan_define(node : Node, condition,index_column_dict):
    index_information = index_column_dict[node.information["Index Name"]]
    result = ""
    result += "The clause "  + condition  + "uses Bitmap Index Scan to create a bitmap of rows on table " + index_information[0] + " using the created index " + node.information["Index Name"] + " on column " + index_information[1]    
    if "Index Cond" in node.information:
         result += " with the following conditions: " + node.information["Index Cond"].replace("::text", "")
        
    if "Filter" in node.information:
        result += (
            " The result is then filtered by "
            + node.information["Filter"].replace("::text", "")
            + "."
        )
    return result

