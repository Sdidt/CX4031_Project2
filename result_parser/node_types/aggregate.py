from postorder import Node

def aggregate_define(node : Node, condition,index_column_dict):
    # For plans of the aggregate type: SortAggregate, HashAggregate, PlainAggregate
    strategy = node.information["Strategy"]
    if strategy == "Sorted":
        result = "The clauses " + condition+ " rows are sorted based on their keys."
        if "Group Key" in node.information:
            result += " They are aggregated by the following keys: "
            for key in node.information["Group Key"]:
                result += key + ","
            result = result[:-1]
            result += "."
        if "Filter" in node.information:
            result += " They are filtered by " + node.information["Filter"].replace("::text", "")
            
            result += "."
        return result
    elif strategy == "Hashed":
        result = "The clauses " + condition+ " hashes all rows based on the following key(s): "
        for key in node.information["Group Key"]:
            result += key.replace("::text", "") + ", "
        result += "which are then aggregated into bucket given by the hashed key."
        return result
    elif strategy == "Plain":
        return "Result is simply aggregated as normal."
    else:
        raise ValueError(
            "Aggregate_explain does not work for strategy: " + strategy
        )

