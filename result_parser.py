from tree import Node

class ResultParser:
    
    def __init__(self) -> None:
        self.rules = {
            "Seq Scan": self.seq_scan_rule,
            "Aggregate": None,
            "Gather Merge": None,
            "Sort": None,
            "Hash": self.hash_rule,
        }
        self.query_annotation = {}
        self.curr_intermediate_table_count = 1

    def seq_scan_rule(self, plan_node: Node, is_final: bool):
        plan = plan_node.plan
        try:
            natural_text = "perform sequential scan on table " + plan["Relation Name"]
            plan_node.result_table_name = plan["Relation Name"]
        except KeyError:
            print("Seq Scan plan did not have \"Relation Name\" attribute. Investigate further and handle.")
            exit()
        intermediate_table_name = None
        if "Filter" in plan:
            condition = plan["Filter"]
            intermediate_table_name = "T" + str(self.curr_intermediate_table_count)
            self.curr_intermediate_table_count += 1
            if not is_final:
                natural_text += " under condition " + condition + " to obtain intermediate table " + intermediate_table_name + ". " + str(plan["Rows Removed by Filter"]) + " are removed in the process"
            else:
                natural_text += " under condition " + condition + " to obtain final result"
            plan_node.result_table_name = intermediate_table_name
        natural_text += "."
        return natural_text
    
    def hash_rule(self, plan_node: Node):
        plan = plan_node.plan
        natural_text = "hash the table "
        child: Node = plan_node.children[0]
        hashed_table_name = None
        if child.result_table_name is None:
            try:
                hashed_table_name = plan["Relation Name"]
            except KeyError:
                print("Hash plan did not have \"Relation Name\" attribute even though no result table name from previous step. Investigate further and handle.")
                exit()
        else:
            hashed_table_name = child.result_table_name
        natural_text += hashed_table_name + " to create intermediate hashed table hashed_" + hashed_table_name
        plan_node.result_table_name = "hashed_" + hashed_table_name
        return natural_text
    
    def hash_join_rule(self, plan_node: Node, is_final: bool):
        plan = plan_node.plan
        natural_text = "perform hash join on tables "
        left: Node = plan_node.children[0]
        right: Node = plan_node.children[1]
        natural_text += left.result_table_name + " and " + right.result_table_name
        intermediate_table_name = None
        if not is_final:
            intermediate_table_name = "T" + str(self.curr_intermediate_table_count)
            self.curr_intermediate_table_count += 1
            natural_text += " under condition " + plan["Hash Cond"] + " to obtain intermediate table " + intermediate_table_name
        else:
            natural_text += " under condition " + plan["Hash Cond"] + " to obtain final result"
        natural_text += "."
        plan_node.result_table_name = intermediate_table_name
        return natural_text

    
    
    
