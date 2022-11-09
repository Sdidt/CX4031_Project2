from postorder import Node
from difflib import SequenceMatcher
from connection import DB
from functools import reduce 
import operator


def getFromDict(dataDict, mapList):
    return reduce(operator.getitem, mapList, dataDict)

def setInDict(dataDict, mapList, value):
    getFromDict(dataDict, mapList[:-1])[mapList[-1]] = value

class Annotator:
    def __init__(self, query, decomposed_query, query_component_dict, db: DB) -> None:
        self.query = query
        self.db = db
        self.config_paras_scan = ["enable_bitmapscan", "enable_indexscan", "enable_indexonlyscan", "enable_seqscan", "enable_tidscan"]
        self.config_paras_join = ["enable_hashjoin", "enable_nestloop", "enable_mergejoin"]
        self.QEP: list[Node] = self.generate_QEP()
        self.AQPs: list[list[Node]] = self.generate_AQPs()
        self.component_mapping: dict = {}
        self.decomposed_query: dict = decomposed_query
        self.query_component_dict: dict = query_component_dict

    def generate_AQPs(self):
        print("\n######################################################################################################################\n")

        print("GENERATING AQPS")

        # AQPs = {k: None for k in self.config_paras_scan}
        AQPs = {}

        # isolate scans 
        for each_config in self.config_paras_scan:
            self.db.execute("set {} = false".format(each_config))

        for each_config in self.config_paras_scan:
            self.db.execute("set {} = true".format(each_config))
            AQP = self.db.execute('EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON) ' + self.query)[0][0][0]["Plan"]
            AQP = self.capture_nodes(AQP)
            AQPs[each_config] = AQP
            self.db.execute("set {} = false".format(each_config))
        
        for each_config in self.config_paras_scan:
            self.db.execute("set {} = true".format(each_config))

        # isolate joins
        for each_config in self.config_paras_join:
            self.db.execute("set {} = false".format(each_config))

        for each_config in self.config_paras_join:
            self.db.execute("set {} = true".format(each_config))
            AQP = self.db.execute('EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON) ' + self.query)[0][0][0]["Plan"]
            AQP = self.capture_nodes(AQP)
            AQPs[each_config] = AQP
            self.db.execute("set {} = false".format(each_config))

        for each_config in self.config_paras_join:
            self.db.execute("set {} = true".format(each_config))
            
        print("\n######################################################################################################################\n")

        return AQPs


    def capture_nodes(self, dct, parent=None, subquery_level=0):
        nodes = []

        cur = Node(dct, subquery_level)
        # print("Current Node Type: {}".format(type(cur)))

        if "Plans" in dct:
            plans = dct["Plans"]
            for plan in plans:

                dct = plan
                if "Subplan Name" not in dct:
                    nodes = nodes + self.capture_nodes(dct, cur, subquery_level)
                    
                else:
                    nodes = nodes + self.capture_nodes(dct, cur, subquery_level + 1)

        cur.add_parent(parent)
        nodes.append(cur)

        # update cur node as child of its parent
        if not cur.parent[0] is None:
            cur.parent[0].add_child(cur)

        return nodes

    def generate_QEP(self):
        output_plan = self.db.execute('EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON) ' + self.query)[0][0][0]["Plan"]
        return self.capture_nodes(output_plan)
        
    def traverse_and_find_best_match(self, node: Node, key, decomposed_query: dict, query_component_dict: dict, component_mapping: dict, curr_key_chain, i=0):
        relevant_info = node.keywords
        optimal_key_chain = []
        optimal_clause = None
        curr_optimal_score = 0
        print("Current key chain")
        print(curr_key_chain)
        print(decomposed_query)
        v = relevant_info[key]
        for k in decomposed_query.keys():
            relevant_decomposed_query = decomposed_query[k]
            relevant_query_component = query_component_dict[k]
            if k not in component_mapping:
                component_mapping[k] = {"subqueries": {}}
            relevant_component_mapping = component_mapping[k]
            curr_key_chain += [k]
            if i < node.subquery_level:
                if "subqueries" in relevant_decomposed_query:
                    print("Recursive subquery call")
                    print(curr_key_chain + ["subqueries"])
                    subproblem_optimal_key_chain, subproblem_optimal_clause, subproblem_optimal_score = self.traverse_and_find_best_match(node, key, relevant_decomposed_query["subqueries"], relevant_query_component["subqueries"], relevant_component_mapping["subqueries"], curr_key_chain + ["subqueries"], i + 1)
                    print("subproblem_optimal_key_chain")
                    print(subproblem_optimal_key_chain)
                    if subproblem_optimal_score > curr_optimal_score:
                        print("Replacing")
                        curr_optimal_score = subproblem_optimal_score
                        optimal_key_chain = subproblem_optimal_key_chain
                        optimal_clause = subproblem_optimal_clause
            if i == node.subquery_level:
                original_query_component = relevant_query_component.get(key, None)
                if original_query_component is None:
                    continue
                curr_optimal_score = 0
                print("\n######################################################################################################################\n")   
                print("MATCHING CLAUSE {}".format(v))   
                print(relevant_decomposed_query)      
                for clause in relevant_decomposed_query[key]:
                    curr_key_chain.append(key)
                    print("Testing clause for match : {}".format(clause))
                    curr_score = SequenceMatcher(None, clause, v).ratio()
                    if curr_optimal_score < curr_score:
                        curr_optimal_score = curr_score
                        optimal_clause = v
                        optimal_key_chain = curr_key_chain.copy()
                    print("Similarity Score: {}".format(curr_score))
                    curr_key_chain.pop()
                if curr_optimal_score > 0.65:
                    print("Fairly good match is found: {}".format(optimal_clause))
                print("\n######################################################################################################################\n")
            curr_key_chain.pop()
        return optimal_key_chain, optimal_clause, curr_optimal_score
            # print(self.component_mapping)

    def find_match_in_decomposed_query(self, node: Node, i=0):
        for k, v in node.keywords.items():
            optimal_key_chain, optimal_clause, similarity_score = self.traverse_and_find_best_match(node, k, self.decomposed_query, self.query_component_dict, self.component_mapping, [])
            print("Optimal key chain: ")
            print(optimal_key_chain)
            print("Optimal clause: ")
            print(optimal_clause)
            print("Similarity Score:")
            print(similarity_score)
            original_query_component = self.query_component_dict
            relevant_component_mapping = self.component_mapping.copy()
            last_key = None
            for j, k in enumerate(optimal_key_chain):
                original_query_component = original_query_component[k]
                if j != len(optimal_key_chain) - 1:
                    relevant_component_mapping = relevant_component_mapping[k]
                else:
                    last_key = k
            print(original_query_component)
            print("Relevant component mapping: ")
            print(relevant_component_mapping)
            print("Last Key")
            print(last_key)
            
            if last_key is not None:
                if isinstance(optimal_clause, list):
                    condition = "\"" + last_key + " " + " ".join(optimal_clause) + "\""
                else:
                    condition = "\"" + last_key + " " + optimal_clause + "\""
                # conditions.append(condition)
                # original_query_components.append(original_query_component)
                explanation = "The clause " + condition + " is implemented using " + node.type
                if "scan" in node.type.lower():
                    print("FOUND SCAN NODE IN MATCH QUERY")
                    cost_dict, choice_explanation = self.cost_comparison_scan(node, self.config_paras_scan)
                    explanation += " because " + choice_explanation
                elif "join" in node.type.lower() or "nested loop" in node.type.lower():
                    print("FOUND JOIN NODE IN MATCH QUERY")
                    cost_dict, choice_explanation = self.cost_comparison_join(node, self.config_paras_join)   
                    explanation += " because " + choice_explanation        
                else:
                    explanation += "."
                # explanations.append(explanation)
                if original_query_component not in relevant_component_mapping:
                    relevant_component_mapping[original_query_component] = []
                relevant_component_mapping[original_query_component].append(explanation)
                # getFromDict(self.component_mapping, optimal_key_chain[:-1, relevant_component_mapping])
                setInDict(self.component_mapping, optimal_key_chain[:-1], relevant_component_mapping)
                print("SELF component mapping:")
                print(self.component_mapping)
        return
    
    def annotate_nodes(self):
        for node in self.QEP:
            node.mapping()
            node.print_debug_info()
            self.find_match_in_decomposed_query(node)

    def explain_costs(self, cost_dict, qep_node_type, qep_cost):
        if len(cost_dict) == 1:
            return "no other option is available."
        ratios = {}
        anomalous_ratios = {}
        for node_type, cost in cost_dict.items():
            if node_type == qep_node_type:
                continue
            ratio = cost / qep_cost
            if ratio > 1.0:
                ratios[node_type] = ratio
            else:   
                anomalous_ratios[node_type] =  1 / ratio
        if len(ratios) == 1:
            choice_explanation = "it requires " + "{:.2f}".format(list(ratios.values())[0]) + " less operations than " + "{}".format(list(ratios.keys())[0]) + "."
        else:
            choice_explanation = "it requires " + ", ".join(["{:.2f}".format(ratio) for ratio in list(ratios.values())]) + " less operations than " + ", ".join([str(node_type) for node_type in (ratios.keys())]) + " respectively."
        if len(anomalous_ratios) == 1:
            choice_explanation += "However, surprisingly, using " + "{}".format(list(anomalous_ratios.keys())[0]) + " requires " + str(list(anomalous_ratios.values())[0]) + " less than " + node_type + "."
        elif len(anomalous_ratios) > 1:
            choice_explanation += "However, surprisingly, using " + ", ".join([str(node_type) for node_type in (anomalous_ratios.keys())]) + " requires " + ", ".join(["{:.2f}".format(ratio) for ratio in list(ratios.values())]) + " less than " + node_type + "."
        return choice_explanation

    def cost_comparison_scan(self, node: Node, config_para_for_scans):
        qep_node = node
        qep_node_type = node.type
        qep_relation = node.information["Relation Name"]
        qep_filter = node.information.get("Filter")
        if qep_filter is None:
            qep_filter = node.information.get("Index Cond")

        # children = node.children
        # total_cost_of_child = 0
        # for child in children:
        #     total_cost_of_child = total_cost_of_child + child.information["Total Cost"] * child.information["Actual Loops"]

        # qep_cost = node.information["Total Cost"] * node.information["Actual Loops"]
        # diff = qep_cost - total_cost_of_child
        qep_cost = node.get_estimated_cost()
        cost_dict = {}
        cost_dict[qep_node_type] = qep_cost
        
        # print(config_para_for_scans)

        # enable one each time
        for each_config in config_para_for_scans:
            print("\n######################################################################################################################\n")   
            print("COMPARING WITH AQP {}".format(each_config)) 
            AQP = self.AQPs[each_config]
            
            is_bitmap_index_scan = False
            bitmap_index_scan_cond = None

            for node in AQP:
                # to find anotherrr forrm of scan
                node.print_debug_info()
                aqp_node_type = node.type
                if node.type == "Bitmap Index Scan":
                    is_bitmap_index_scan = True
                    bitmap_index_scan_cond = node.information["Index Cond"]
                # if "Relation Name" not in node.information:
                #     print(node.type)
                #     print(node.information)
                if "Relation Name" not in node.information:
                    continue
                aqp_filter = None
                aqp_relation = node.information["Relation Name"]
                if qep_filter is not None:
                    if is_bitmap_index_scan:
                        aqp_filter = bitmap_index_scan_cond
                        is_bitmap_index_scan = False
                        bitmap_index_scan_cond = None
                    aqp_filter = node.information.get("Filter")
                    if aqp_filter is None:
                        aqp_filter = node.information.get("Index Cond")
                        if aqp_filter is None:
                            continue
                if qep_filter != aqp_filter:
                    print("Condition is not matching!")
                    continue
                if "scan" in aqp_node_type.lower() and aqp_relation == qep_relation:
                    # children = node.children
                    # total_cost_of_child = 0
                    # for child in children:
                    #     total_cost_of_child = total_cost_of_child + child.information["Total Cost"] * child.information["Actual Loops"]
                    # aqp_cost = node.information["Total Cost"] * node.information["Actual Loops"] 
                    # diff = aqp_cost - total_cost_of_child
                    aqp_cost = node.get_estimated_cost()
                    cost_dict[aqp_node_type] = aqp_cost
                    break
            print("\n######################################################################################################################\n")   
            # db.execute("set {} = false".format(each_config))
        print("COST COMPARISON: {}".format(cost_dict))

        print("\n######################################################################################################################\n") 
        choice_explanation = self.explain_costs(cost_dict, qep_node_type, qep_cost)
        
        return cost_dict, choice_explanation

    def cost_comparison_join(self, node: Node, config_para_for_join):
        qep_node_type = node.type
        # change this to use node.join_filter instead
        if qep_node_type == 'Hash Join':
            qep_filter = node.information["Hash Cond"]
        elif qep_node_type == 'Nested Loop':
            qep_filter = node.information["Join Filter"]
        elif qep_node_type == 'Merge Join':
            qep_filter = node.information["Join Filter"]
        else:
            qep_filter == None

        # USE THIS INSTEAD
        # qep_filter = node.join_filter

        # children = node.children
        # total_cost_of_child = 0
        # for child in children:
        #     total_cost_of_child = total_cost_of_child + child.information["Total Cost"] * child.information["Actual Loops"]

        # qep_cost = node.information["Total Cost"] * node.information["Actual Loops"]
        # diff = qep_cost - total_cost_of_child
        qep_cost = node.get_estimated_cost()
        cost_dict = {}
        cost_dict[qep_node_type] = qep_cost

        # enable one each time
        for each_config in config_para_for_join:
            print("\n######################################################################################################################\n")   
            print("COMPARING WITH AQP {}".format(each_config)) 
            AQP = self.AQPs[each_config]

            for node in AQP:
                # to find another forrm of scan
                node.print_debug_info()
                aqp_node_type = node.type
                if aqp_node_type == "Hash Join":
                    aqp_filter = node.information["Hash Cond"]
                elif aqp_node_type == "Nested Loop":
                    aqp_filter = node.information["Join Filter"]
                elif aqp_node_type == "Merge Join":
                    aqp_filter = node.information["Join Filter"]
                else:
                    aqp_filter = None
         
                if qep_filter != aqp_filter:
                    print("Condition is not matching!")
                    continue
                if "join" in aqp_node_type.lower() or "nested loop" in aqp_node_type.lower():
                    if aqp_filter == qep_filter:
                        # children = node.children
                        # total_cost_of_child = 0
                        # for child in children:
                        #     total_cost_of_child = total_cost_of_child + child.information["Total Cost"] * child.information["Actual Loops"]

                        # aqp_cost = node.information["Total Cost"] * node.information["Actual Loops"]
                        aqp_cost = node.get_estimated_cost()
                        # node_cost = aqp_cost - total_cost_of_child
                        cost_dict = {}
                        cost_dict[aqp_node_type] = aqp_cost
                        break
            print("\n######################################################################################################################\n")   
            # db.execute("set {} = false".format(each_config))
        print("COST COMPARISON: {}".format(cost_dict))
        choice_explanation = self.explain_costs(cost_dict, qep_node_type, qep_cost)
        
        return cost_dict, choice_explanation
