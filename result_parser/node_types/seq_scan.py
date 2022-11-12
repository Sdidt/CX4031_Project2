from node import Node

## Add in Plan Rows or Actual Rows retrieved from the relation?? 
def seq_scan_define(node : Node, condition,index_column_dict):
    result = "The clause " + condition+  " performs a sequential scan on relation "
    if "Relation Name" in node.information:
        result += node.information["Relation Name"]
    if "Alias" in node.information:
        if node.information["Relation Name"] != node.information["Alias"]:
            result += " with an alias of {}".format(node.information["Alias"])
    if "Filter" in node.information:
        result += " and filtered with the condition {}".format(
            node.information["Filter"].replace("::bpchar", "")
        )

    return result