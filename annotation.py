from postorder import Node
from difflib import SequenceMatcher
from connection import DB
from functools import reduce 
import operator
from result_parser.result_parser import ResultParser
from result_parser.node_types.default_node import default_define



def getFromDict(dataDict, mapList):
    return reduce(operator.getitem, mapList, dataDict)

def setInDict(dataDict, mapList, value):
    getFromDict(dataDict, mapList[:-1])[mapList[-1]] = value

class Annotator:
    def __init__(self, query, decomposed_query, query_component_dict, db: DB,index_column_dict ) -> None:
        self.query = query
        self.db = db
        self.config_paras_scan = ["enable_bitmapscan", "enable_indexscan", "enable_indexonlyscan", "enable_seqscan", "enable_tidscan"]
        self.config_paras_join = ["enable_hashjoin", "enable_nestloop", "enable_mergejoin"]
        self.QEP: list[Node] = self.generate_QEP()
        self.AQPs: list[list[Node]] = self.generate_AQPs()
        self.component_mapping: dict = {}
        self.decomposed_query: dict = decomposed_query
        self.query_component_dict: dict = query_component_dict
        self.result_parser =  default_define
        self.index_column_dict = index_column_dict

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

        cur.mapping()

        return nodes

    def generate_QEP(self):
        output_plan = self.db.execute('EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON) ' + self.query)[0][0][0]["Plan"]
        return self.capture_nodes(output_plan)
        
    def traverse_and_find_best_match(self, node: Node, key, decomposed_query: dict, query_component_dict: dict, component_mapping: dict, curr_key_chain: list, i=0):
        relevant_info = node.keywords
        optimal_key_chain = []
        optimal_clause = None
        curr_optimal_score = 0
        # print("Current key chain")
        # print(curr_key_chain)
        # print(decomposed_query)
        v = relevant_info[key]
        merge_join_order_by_explanation = ""
        for k in decomposed_query.keys():
            relevant_decomposed_query = decomposed_query[k]
            relevant_query_component = query_component_dict[k]
            if k not in component_mapping:
                component_mapping[k] = {"subqueries": {}}
            relevant_component_mapping = component_mapping[k]
            curr_key_chain += [k]
            if i < node.subquery_level:
                if "subqueries" in relevant_decomposed_query:
                    # print("Recursive subquery call")
                    # print(curr_key_chain + ["subqueries"])
                    subproblem_optimal_key_chain, subproblem_optimal_clause, subproblem_optimal_score, merge_join_order_by_explanation = self.traverse_and_find_best_match(node, key, relevant_decomposed_query["subqueries"], relevant_query_component["subqueries"], relevant_component_mapping["subqueries"], curr_key_chain + ["subqueries"], i + 1)
                    # print("subproblem_optimal_key_chain: {}".format(subproblem_optimal_key_chain))
                    if subproblem_optimal_score > curr_optimal_score:
                        print("Replacing")
                        curr_optimal_score = subproblem_optimal_score
                        optimal_key_chain = subproblem_optimal_key_chain
                        optimal_clause = subproblem_optimal_clause
            if i == node.subquery_level:
                original_query_component = relevant_query_component.get(key, None)
                if original_query_component is None:
                    continue
                # curr_optimal_score = 0
                print("\n######################################################################################################################\n")   
                print("MATCHING CLAUSE {}".format(v))   
                # print(relevant_decomposed_query)      
                for clause in relevant_decomposed_query[key]:
                    curr_key_chain.append(key)
                    print("Testing clause for match : {}".format(clause))
                    curr_score = SequenceMatcher(None, clause, v).ratio()
                    if curr_optimal_score < curr_score:
                        curr_optimal_score = curr_score
                        optimal_clause = v
                        optimal_key_chain = curr_key_chain.copy()
                        if node.type == "Merge Join":
                            join_attributes = node.join_filters[0].split(" ")
                            print("JOIN ATTIRBUTES: {}".format(join_attributes))
                            for join_attribute in join_attributes:
                                if "order by" in relevant_query_component:
                                    if join_attribute in relevant_query_component["order by"]:
                                        merge_join_order_by_explanation += ". Further, sorting by the join attribute {} is performed using clause {}".format(join_attribute, relevant_query_component["order by"])
                    print("Similarity Score: {}".format(curr_score))
                    curr_key_chain.pop()
                if curr_optimal_score > 0.65:
                    print("Fairly good match is found: {}".format(optimal_clause))
                print("\n######################################################################################################################\n")
            curr_key_chain.pop()
        return optimal_key_chain, optimal_clause, curr_optimal_score, merge_join_order_by_explanation

    def find_match_in_decomposed_query(self, node: Node, i=0):
        for k, v in node.keywords.items():
            optimal_key_chain, optimal_clause, similarity_score, merge_join_order_by_explanation = self.traverse_and_find_best_match(node, k, self.decomposed_query, self.query_component_dict, self.component_mapping, [])
            print("Optimal key chain: {}".format(optimal_key_chain))
            print("Optimal clause: {}".format(optimal_clause))
            print("Similarity Score: {}".format(similarity_score))
            original_query_component = self.query_component_dict
            relevant_component_mapping = self.component_mapping.copy()
            last_key = None
            for j, k in enumerate(optimal_key_chain):
                original_query_component = original_query_component[k]
                if j != len(optimal_key_chain) - 1:
                    relevant_component_mapping = relevant_component_mapping[k]
                else:
                    last_key = k
            # print(original_query_component)
            # print("Relevant component mapping: {}".format(relevant_component_mapping))
            # print("Last Key: {}".format(last_key))
            
            if last_key is not None:
                if isinstance(optimal_clause, list):
                    condition = "\"" + last_key + " " + " ".join(optimal_clause) + "\""
                else:
                    condition = "\"" + last_key + " " + optimal_clause + "\""
                self.result_parser = ResultParser.results_map.get(node.type, default_define)
                explanation =  self.result_parser(node,condition,self.index_column_dict)
                #explanation = "The clause " + condition + " is implemented using " + node.type
                if "scan" in node.type.lower():
                    print("FOUND SCAN NODE IN MATCH QUERY")
                    cost_dict, choice_explanation = self.cost_comparison_scan(node, self.config_paras_scan)
                    explanation += choice_explanation
                elif "join" in node.type.lower() or "nested loop" in node.type.lower():
                    print("FOUND JOIN NODE IN MATCH QUERY")
                    cost_dict, choice_explanation = self.cost_comparison_join(node, self.config_paras_join)   
                    explanation += choice_explanation 
                    if node.type == "Merge Join":
                        explanation += merge_join_order_by_explanation       
                else:
                    explanation += "."
                if original_query_component not in relevant_component_mapping:
                    relevant_component_mapping[original_query_component] = []
                relevant_component_mapping[original_query_component].append(explanation)
                # getFromDict(self.component_mapping, optimal_key_chain[:-1, relevant_component_mapping])
                setInDict(self.component_mapping, optimal_key_chain[:-1], relevant_component_mapping)
                # print("SELF component mapping:")
                # print(self.component_mapping)
        return
    
    def annotate_nodes(self):
        for node in self.QEP:
            node.print_debug_info()
            self.find_match_in_decomposed_query(node)

    def explain_costs(self, cost_dict, qep_node_type, qep_cost):
        if len(cost_dict) == 1:
            return " because no other option is available."
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
        print(ratios)
        print(anomalous_ratios)
        if len(ratios) == 0:
            choice_explanation += "."
        if len(ratios) == 1:
            choice_explanation = " because it requires " + "{:.2f}".format(list(ratios.values())[0]) + " less operations than " + "{}".format(list(ratios.keys())[0]) + "."
        elif len(ratios) > 1:
            choice_explanation = " because it requires " + ", ".join(["{:.2f}".format(ratio) for ratio in list(ratios.values())]) + " less operations than " + ", ".join([str(node_type) for node_type in (ratios.keys())]) + " respectively."
        if len(anomalous_ratios) == 1:
            choice_explanation += " However, using " + "{}".format(list(anomalous_ratios.keys())[0]) + " requires " + "{:.2f}".format(list(anomalous_ratios.values())[0]) + " less operations than " + qep_node_type + "."
        elif len(anomalous_ratios) > 1:
            choice_explanation += " However, using " + ", ".join([str(node_type) for node_type in (anomalous_ratios.keys())]) + " requires " + ", ".join(["{:.2f}".format(ratio) for ratio in list(ratios.values())]) + " less operations than " + qep_node_type + "."
        return choice_explanation

    def cost_comparison_scan(self, node: Node, config_para_for_scans):
        qep_node = node
        qep_node_type = qep_node.type
        qep_relation = qep_node.information["Relation Name"]
        # qep_filter = qep_node.information.get("Filter")
        # if qep_filter is None:
        #     qep_filter = qep_node.information.get("Index Cond")
        qep_filter = qep_node.scan_filter
        print("SCAN FILTER: {}".format(qep_filter))
        qep_cost = qep_node.get_estimated_cost()
        cost_dict = {}
        cost_dict[qep_node_type] = qep_cost
        
        for each_config in config_para_for_scans:
            print("\n######################################################################################################################\n")   
            print("COMPARING WITH AQP {}".format(each_config)) 
            AQP = self.AQPs[each_config]
            
            is_bitmap_index_scan = False
            bitmap_index_scan_cond = None

            for node in AQP:
                # to find anotherrr forrm of scan
                # node.print_debug_info()
                aqp_node_type = node.type
                if node.type == "Bitmap Index Scan":
                    is_bitmap_index_scan = True
                    bitmap_index_scan_cond = node.information["Index Cond"]
                if "Relation Name" not in node.information:
                    continue
                aqp_filter = node.scan_filter
                aqp_relation = node.information["Relation Name"]
                if is_bitmap_index_scan:
                    aqp_filter = bitmap_index_scan_cond[1:-1]
                    is_bitmap_index_scan = False
                    bitmap_index_scan_cond = None
                # if qep_filter is not None:
                #     if is_bitmap_index_scan:
                #         aqp_filter = bitmap_index_scan_cond
                #         is_bitmap_index_scan = False
                #         bitmap_index_scan_cond = None
                #     aqp_filter = node.information.get("Filter")[1:-1]
                #     if aqp_filter is None:
                #         aqp_filter = node.information.get("Index Cond")[1:-1]
                #         if aqp_filter is None:
                #             continue
                print("AQP Node: {}".format(aqp_node_type))
                print("AQP filter: {}".format(aqp_filter))
                if qep_filter != aqp_filter:
                    # print("Condition is not matching!")
                    continue
                if "scan" in aqp_node_type.lower() and aqp_relation == qep_relation:
                    aqp_cost = node.get_estimated_cost()
                    cost_dict[aqp_node_type] = aqp_cost
                    break
            print("\n######################################################################################################################\n")   
        print("COST COMPARISON: {}".format(cost_dict))

        print("\n######################################################################################################################\n") 
        choice_explanation = self.explain_costs(cost_dict, qep_node_type, qep_cost)
        
        return cost_dict, choice_explanation

    def cost_comparison_join(self, node: Node, config_para_for_join):
        qep_node_type = node.type
        qep_filter = node.join_filters[0]

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
                # node.print_debug_info()
                aqp_node_type = node.type
                aqp_filters = node.join_filters
                print("JOIN FILTER: {}".format(aqp_filters))
         
                if qep_filter not in aqp_filters:
                    # print("Condition is not matching!")
                    continue
                if "join" in aqp_node_type.lower() or "nested loop" in aqp_node_type.lower():
                    if qep_filter in aqp_filters:
                        aqp_cost = node.get_estimated_cost()
                        cost_dict[aqp_node_type] = aqp_cost
                        break
            print("\n######################################################################################################################\n")   
            # db.execute("set {} = false".format(each_config))
        print("COST COMPARISON: {}".format(cost_dict))
        choice_explanation = self.explain_costs(cost_dict, qep_node_type, qep_cost)
        
        return cost_dict, choice_explanation
