from postorder import Node
from difflib import SequenceMatcher
from connection import DB
from result_parser.result_parser import ResultParser
from result_parser.node_types.default_node import default_define
class Annotator:
    def __init__(self, query, decomposed_query, query_component_dict, db: DB, index_column_dict) -> None:
        self.query = query
        self.db = db
        self.config_paras_scan = ["enable_bitmapscan", "enable_indexscan", "enable_indexonlyscan", "enable_seqscan", "enable_tidscan"]
        self.config_paras_join = ["enable_hashjoin", "enable_nestloop", "enable_mergejoin"]
        self.QEP: list[Node] = self.generate_QEP()
        self.AQPs: list[list[Node]] = self.generate_AQPs()
        self.component_mapping = {"subqueries": {}}
        self.decomposed_query = decomposed_query
        self.query_component_dict = query_component_dict
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

        for each_config in self.config_paras_scans:
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
        
    def find_match_in_decomposed_query(self, node: Node):
        conditions = []
        explanations = []
        original_query_components = []
        i = 0
        relevant_decomposed_query = self.decomposed_query
        relevant_query_component = self.query_component_dict
        relevant_component_mapping = self.component_mapping
        relevant_info = node.keywords
        while i < node.subquery_level:
            relevant_decomposed_query = relevant_decomposed_query["subqueries"]["sub_number_" + str(i + 1)]
            relevant_query_component = relevant_query_component["subqueries"]["sub_number_" + str(i + 1)]
            relevant_component_mapping = relevant_component_mapping["subqueries"]
            i += 1
        for k, v in relevant_info.items():
            optimal_clause = None
            original_query_component = relevant_query_component.get(k, None)
            if original_query_component is None:
                continue
            if isinstance(v, list):
                condition = "\"" + k + " " + " ".join(v) + "\""
            else:
                condition = "\"" + k + " " + v + "\""
            conditions.append(condition)
            original_query_components.append(original_query_component)
            print("99999999999999999999999999999999999999")
            print(node.type)
            self.result_parser = ResultParser.results_map.get(node.type, default_define)
            explanation =  self.result_parser(node,condition,self.index_column_dict)
           # explanation = "The clause " + condition + " is implemented using " + node.type
            if "scan" in node.type.lower():
                cost_dict, choice_explanation = self.cost_comparison_scan(node, self.config_paras)
                explanation += " because " + choice_explanation
            elif "join" in node.type.lower() or "nested loop" in node.type.lower():
                print("FOUND JOIN NODE IN MATCH QUERY")
                cost_dict, choice_explanation = self.cost_comparison_join(node, self.config_paras_join)           
            else:
                explanation += "."
            explanations.append(explanation)
            if original_query_component not in relevant_component_mapping:
                relevant_component_mapping[original_query_component] = []
            relevant_component_mapping[original_query_component].append(explanation)
            similarity_score = 0
            print("\n######################################################################################################################\n")   
            print("MATCHING CLAUSE {}".format(v))         
            for clause in relevant_decomposed_query[k]:
                print("Testing clause for match : {}".format(clause))
                curr_score = SequenceMatcher(None, clause, v).ratio()
                if similarity_score < curr_score:
                    similarity_score = curr_score
                    optimal_clause = v
                print("Similarity Score: {}".format(curr_score))
            if similarity_score > 0.65:
                print("Fairly good match is found: {}".format(optimal_clause))
            print("\n######################################################################################################################\n") 
        # print(self.component_mapping)
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
        qep_cost = node.information["Total Cost"] * node.information["Actual Loops"]
        cost_dict = {}
        cost_dict[qep_node_type] = qep_cost
        
        print(config_para_for_scans)

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
                    aqp_cost = node.information["Total Cost"] * node.information["Actual Loops"]
                    cost_dict[aqp_node_type] = aqp_cost
                    break
            print("\n######################################################################################################################\n")   
            # db.execute("set {} = false".format(each_config))
        print("COST COMPARISON: {}".format(cost_dict))
        choice_explanation = self.explain_costs(cost_dict, qep_node_type, qep_cost)
        
        return cost_dict, choice_explanation

    def cost_comparison_join(self, node: Node, config_para_for_join):
        qep_node = node
        qep_node_type = node.type
        placeholder = {}

        return placeholder, str(node.information)
        # qep_filter = node.information.get("Filter")
        # if qep_filter is None:
        #     qep_filter = node.information.get("Index Cond")
        # qep_cost = node.information["Total Cost"] * node.information["Actual Loops"]
        # cost_dict = {}
        # cost_dict[qep_node_type] = qep_cost
        
        # return cost_dict, choice_explanation
        # print(config_para_for_join)

        # # enable one each time
        # for each_config in config_para_for_join:
        #     print("\n######################################################################################################################\n")   
        #     print("COMPARING WITH AQP {}".format(each_config)) 
        #     AQP = self.AQPs[each_config]
            
        #     is_bitmap_index_scan = False
        #     bitmap_index_scan_cond = None

        #     for node in AQP:
        #         # to find anotherrr forrm of scan
        #         node.print_debug_info()
        #         aqp_node_type = node.type
        #         if node.type == "Bitmap Index Scan":
        #             is_bitmap_index_scan = True
        #             bitmap_index_scan_cond = node.information["Index Cond"]
        #         # if "Relation Name" not in node.information:
        #         #     print(node.type)
        #         #     print(node.information)
        #         if "Relation Name" not in node.information:
        #             continue
        #         aqp_filter = None
        #         aqp_relation = node.information["Relation Name"]
        #         if qep_filter is not None:
        #             if is_bitmap_index_scan:
        #                 aqp_filter = bitmap_index_scan_cond
        #                 is_bitmap_index_scan = False
        #                 bitmap_index_scan_cond = None
        #             aqp_filter = node.information.get("Filter")
        #             if aqp_filter is None:
        #                 aqp_filter = node.information.get("Index Cond")
        #                 if aqp_filter is None:
        #                     continue
        #         if qep_filter != aqp_filter:
        #             print("Condition is not matching!")
        #             continue
        #         if "scan" in aqp_node_type.lower() and aqp_relation == qep_relation:
        #             aqp_cost = node.information["Total Cost"] * node.information["Actual Loops"]
        #             cost_dict[aqp_node_type] = aqp_cost
        #             break
        #     print("\n######################################################################################################################\n")   
        #     # db.execute("set {} = false".format(each_config))
        # print("COST COMPARISON: {}".format(cost_dict))
        # choice_explanation = self.explain_costs(cost_dict, qep_node_type, qep_cost)
        
        # return cost_dict, choice_explanation