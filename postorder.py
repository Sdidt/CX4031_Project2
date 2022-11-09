from __future__ import annotations
# from types import NoneType
from dotenv import load_dotenv
from connection import DB
from difflib import SequenceMatcher


load_dotenv()
db = DB()


decomposed_query = {
    'subqueries': {'sub_number_1': {'subqueries': {}, 'select': ['sum(partsupp_1.ps_supplycost*partsupp_1.ps_availqty)*0.0001000000'], 'from': ['p1', 's', 'nation'], 'as': {'partsupp': 'p1', 'supplier': 's'}, 'where': ['p1.ps_suppkey = s.s_suppkey', 's.s_nationkey = nation_1.n_nationkey', "nation_1.n_name = 'GERMANY'"]}}, 'select': ['partsupp.ps_partkeyps', 'sum(partsupp.ps_supplycost*partsupp.ps_availqty)'], 'as': {'value': 'sum(partsupp.ps_supplycost*partsupp.ps_availqty)', 'partsupp': 'p'}, 'from': ['p', 'supplier', 'nation'], 'where': ['P.ps_suppkey = supplier.s_suppkey', 'supplier.s_nationkey <> nation.n_nationkey', "nation.n_name = 'GERMANY'", 'p.ps_supplycost > 20', 'supplier.s_acctbal > 10'], 'group by': ['p.ps_partkey'], 'having': ['sum ( p.ps_supplycost * p.ps_availqty ) > sub_number_1'], 'order by': ['sum(partsupp.ps_supplycost*partsupp.ps_availqty) asc']
}

query_component_dict = {
    'subqueries': {'sub_number_1': {'subqueries': {}, 'select': 'select sum ( ps_supplycost * ps_availqty ) * 0.0001000000', '': '', 'from': 'from partsupp P1 , supplier S , nation', 'where': "where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY'"}}, 'select': 'select ps_partkey PS , sum ( ps_supplycost * ps_availqty ) as value', '': '', 'from': 'from partsupp P , supplier , nation', 'where': "where P.ps_suppkey = s_suppkey and not s_nationkey = n_nationkey and n_name = 'GERMANY' and ps_supplycost > 20 and s_acctbal > 10", 'group by': 'group by ps_partkey', 'having': 'having sum ( ps_supplycost * ps_availqty ) >sub_number_1', 'order by': 'order by value ;'
    }

# decomposed_query ={
#     'subqueries': {}, 'select': ['*.*'], 'from': ['nation'], 'where': ['nation.n_nationkey = 3 ;']
# }

# query_component_dict = {
#     'subqueries': {}, 'select': 'select *', '': '', 'from': 'from nation', 'where': 'where nation.n_nationkey = 3 ;'
# }

component_mapping = {}

query = """
select
            ps_partkey PS,
            sum(ps_supplycost * ps_availqty) as value
            from
            partsupp P,
            supplier,
            nation
            where
            P.ps_suppkey = s_suppkey
            and not s_nationkey = n_nationkey
            and n_name = 'GERMANY'
            and ps_supplycost > 20
            and s_acctbal > 10
            group by
            ps_partkey having
                sum(ps_supplycost * ps_availqty) > (
                select
                    sum(ps_supplycost * ps_availqty) * 0.0001000000
                from
                    partsupp P1,
                    supplier S,
                    nation
                where
                    ps_suppkey = s_suppkey
                    and s_nationkey = n_nationkey
                    and n_name = 'GERMANY'
                ) 
            order by
            value;
"""

complex_query = """
select
      supp_nation,
      cust_nation,
      l_year,
      sum(volume) as revenue
    from
      (
        select
          n1.n_name as supp_nation,
          n2.n_name as cust_nation,
          DATE_PART('YEAR',l_shipdate) as l_year,
          l_extendedprice * (1 - l_discount) as volume
        from
          supplier,
          lineitem,
          orders,
          customer,
          nation n1,
          nation n2
        where
          s_suppkey = l_suppkey
          and o_orderkey = l_orderkey
          and c_custkey = o_custkey
          and s_nationkey = n1.n_nationkey
          and c_nationkey = n2.n_nationkey
          and (
            (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
            or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
          )
          and l_shipdate between '1995-01-01' and '1996-12-31'
          and o_totalprice > 100
          and c_acctbal > 10
      ) as shipping
    group by
      supp_nation,
      cust_nation,
      l_year
    order by
      supp_nation,
      cust_nation,
      l_year;
"""

primary_key_query = "select * from nation where nation.n_nationkey = 3;"

q20 = """
        SELECT
        s_name,
        s_address
    FROM
        supplier,
        nation
    WHERE
        s_suppkey IN (
            SELECT
                ps_suppkey
            FROM
                partsupp
            WHERE
                ps_partkey IN (
                    SELECT
                        p_partkey
                    FROM
                        part
                    WHERE
                        p_name LIKE 'forest%'
                )
                AND ps_availqty > (
                    SELECT
                        0.5 * SUM(l_quantity)
                    FROM
                        lineitem
                    WHERE
                        l_partkey = ps_partkey
                        AND l_suppkey = ps_suppkey
                        AND l_shipdate >= DATE '1-1-1994'
                        AND l_shipdate < DATE '1-1-1994' + 1
                )
        )
        AND s_nationkey = n_nationkey
        AND n_name = 'CANADA'
    ORDER BY
        s_name
        """



# query = """
# select
#       ps_partkey
#     from
#       partsupp
#     where
#       ps_supplycost
#     in (20,30,40)
#     ;
# """

# query = """
# select
#       s_nationkey
#     from
#       supplier
#     where
#       s_nationkey in (select n_nationkey from nation)
#    ;
# """
# lst_enable_disable = ["set enable_indexscan = false", "set enable_bitmapscan = false"]

# db.execute("set enable_indexscan = false")
# db.execute("set enable_bitmapscan = false")

# output_plan = db.execute('EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON) ' + query)[0][0][0]["Plan"]


class Node():

    def __init__(self, data: dict, subquery_level) -> None:

        self.type = data.get("Node Type")
        self.subquery_level = subquery_level
        self.parent = []
        self.children = []
        self.information = None
        self.keywords = None

        d = data.copy()
        d.pop("Node Type")

        if "Plans" in data:
            d.pop("Plans")

        self.information = d
        if (self.type == "Sort"):
            self.type = self.information["Sort Method"] + " Sort"

    def add_child(self, child: Node) -> None:
        self.children.append(child)

    def add_parent(self, parent: Node) -> None:
        self.parent.append(parent)

    def get_estimated_cost(self):
        # startup_cost = self.information["Startup Cost"]
        children: list[Node] = self.children
        total_cost_of_child = 0
        for child in children:
            total_cost_of_child += child.information["Total Cost"] * child.information["Actual Loops"]

        qep_cost = self.information["Total Cost"] * self.information["Actual Loops"]
        diff = qep_cost - total_cost_of_child
        return diff

    def mapping(self):

        self.keywords = {}

        if self.type == "Seq Scan":
            self.keywords = self.node_seq_scan()
        elif self.type == "Index Scan":
            self.keywords = self.node_index_scan()
        elif self.type == "Bitmap Index Scan":
            self.keywords = self.node_bitmap_index_scan()
        elif self.type == "Bitmap Heap Scan":
            self.keywords = self.node_bitmap_heap_scan()
        elif self.type == "Hash":
            self.keywords = self.node_hash()
        elif self.type == "Hash Join":
            self.keywords = self.node_hash_join()
        elif self.type == "Materialize":
            pass
        elif self.type == "Nested Loop":
            self.keywords = self.node_nested_loop()
        elif self.type == "Aggregate":
            self.keywords = self.node_aggregate()
        elif "Sort" in self.type:
            self.keywords = self.node_sort()
        elif self.type == "Gather Merge":
            pass
        else:
            pass
    
        # converting dictionary to a list of dictionaries
        lst = [{"level": self.subquery_level}]
        for key,value in self.keywords.items():
            dct = {}
            dct[key] = value
            lst.append(dct)
        return lst

    def remove_punctuations(self, condition):
        split_condition = condition.split("::", 1)
        if len(split_condition) == 1:
            split_condition = split_condition[0][1:-1]
        else:
            split_condition = split_condition[0][1:]
        return split_condition

    def node_aggregate(self):
        relevant_info = {}
        if 'Group Key' in self.information:
            group = self.information["Group Key"]
            relevant_info["group by"] = group
        return relevant_info

    # complete: BUT need to handle case when sort is used before sort merge join
    def node_sort(self):
        relevant_info = {}
        order = "desc" if self.information["Sort Key"][0][-4:] == "desc" else "asc"
        key = self.information["Sort Key"][0] if order == "asc" else self.information["Sort Key"][0][:-5]
        key = self.remove_punctuations(key)
        relevant_info["order by"] = key + " " + order
    
        return relevant_info
    
    # complete
    def node_seq_scan(self):
        relevant_info = {}
        relation = self.information.get("Alias")
        relevant_info["from"] = relation

        if "Filter" in self.information:
            filter = self.information.get("Filter")

            if "= any" not in filter:
                filter = self.remove_punctuations(filter)                
                relevant_info["where"] = filter
            else:
                filter1 = filter.split(" = any", 1)[0][1:]
                filter2 = filter.split("any (", 1)[1].split("::", 1)[0]
                relevant_info["where"] = filter1
                relevant_info["in"] = filter2
        
        # alias = self.information.get("Alias")
        # if len(alias) > len(relation):
        #     # confirm that is subquery, now need to know the level
        #     level = int(alias[-1])
        #     relevant_info["level"] = level
        # else:
        #     relevant_info["level"] = 0
        # self.find_match_in_decomposed_query(relevant_info, decomposed_query, query_component_dict)

        return relevant_info

    # complete
    def node_index_scan(self):
        relevant_info = {}
        relation = self.information.get("Alias")
        relevant_info["from"] = relation

        if "Index Cond" in self.information:
            index_cond = self.information.get("Index Cond")

            if "= any" not in index_cond:
                index_cond = self.remove_punctuations(index_cond)                
                relevant_info["where"] = index_cond
            else:
                index_cond1 = index_cond.split(" = any", 1)[0][1:]
                index_cond2 = index_cond.split("any (", 1)[1].split("::", 1)[0]
                relevant_info["where"] = index_cond1
                relevant_info["in"] = index_cond2


        return relevant_info

    def node_bitmap_index_scan(self):
        relevant_info = {}
    
        if "Index Cond" in self.information:
            index_cond = self.information.get("Index Cond")

            if "= any" not in index_cond:
                index_cond = self.remove_punctuations(index_cond)                
                relevant_info["where"] = index_cond
            else:
                index_cond1 = index_cond.split(" = any", 1)[0][1:]
                index_cond2 = index_cond.split("any (", 1)[1].split("::", 1)[0]
                relevant_info["where"] = index_cond1
                relevant_info["in"] = index_cond2


        return relevant_info

    def node_bitmap_heap_scan(self):
        relevant_info = {}
        relation = self.information.get("Alias")
        relevant_info["from"] = relation


        return relevant_info

    # complete
    def node_hash(self):
        relevant_info = {}
        return relevant_info

    # complete
    def node_hash_join(self):
        relevant_info = {}
        condition = self.information['Hash Cond'][1:-1]
        keywords = ["where", "in"]
        for keyword in keywords:
            relevant_info[keyword] = condition
        self.join_filter = ""
        return relevant_info
    
    # TODO: handle case when no join filter is present; drill down to closest index scan/bitmap index scan
    def node_nested_loop(self):
        relevant_info = {}
        print(self.information)
        condition = self.information['Join Filter'][1:-1]
        keywords = ["where", "in"]
        for keyword in keywords:
            relevant_info[keyword] = condition
        self.join_filter = ""
        return relevant_info

    #TODO: complete this function
    def node_merge_join(self):
        pass

    def print_debug_info(self):
        print("NODE TYPE: {}".format(self.type))
        print("ESTIMATED COST: {}".format(self.get_estimated_cost()))
        # print("MAPPING: {}".format(self.mapping()))
        # print("RELEVANT INFORMATION: \n{}".format(self.get_relevant_info()))
        print("OTHER INFORMATION: {}".format(self.information))

    ### DO NOT USE THIS FUNCTION FOR FINAL. THEY HAVE BEEN ABSTRACTED AWAY TO THE ANNOTATOR. 
    def find_match_in_decomposed_query(self, relevant_info, decomposed_query, query_component_dict):
        conditions = []
        explanations = []
        original_query_components = []
        i = 0
        relevant_decomposed_query = decomposed_query
        relevant_query_component = query_component_dict
        while i < self.subquery_level:
            relevant_decomposed_query = decomposed_query["subqueries"]["sub_number_" + str(i + 1)]
            relevant_query_component = relevant_query_component["subqueries"]["sub_number_" + str(i + 1)]
            i += 1
        for k, v in relevant_info.items():
            original_query_component = relevant_query_component.get(k, None)
            if original_query_component is None:
                continue
            if isinstance(v, list):
                condition = "\"" + k + " " + " ".join(v) + "\""
            else:
                condition = "\"" + k + " " + v + "\""
            conditions.append(condition)
            original_query_components.append(original_query_component)
            explanation = "The clause " + condition + " is implemented using " + self.type + " because ..."
            explanations.append(explanation)
            if original_query_component not in component_mapping:
                component_mapping[original_query_component] = []
            component_mapping[original_query_component].append(explanation)
            similarity_score = 0
            for clause in relevant_decomposed_query[k]:
                print("Testing clause for match : {}".format(clause))
                curr_score = SequenceMatcher(None, clause, v).ratio()
                if similarity_score < curr_score:
                    similarity_score = curr_score
                print("Similarity Score: {}".format(curr_score))
            if similarity_score > 0.65:
                print("Fairly good match is found")
        # print(original_query_components)
        # print(conditions)
        # print(explanations)
        # print(component_mapping)

### DO NOT USE THE FOLLOWING 3 FUNCTIONS FOR FINAL. THEY HAVE BEEN ABSTRACTED AWAY TO THE ANNOTATOR. 

def generate_AQPs(config_paras):
    
    print("\n######################################################################################################################\n")

    print("GENERATING AQPS")

    AQPs = {k: None for k in config_paras}
    
    for each_config in config_paras:
        db.execute("set {} = false".format(each_config))

    for each_config in config_paras:
        db.execute("set {} = true".format(each_config))
        AQP = db.execute('EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON) ' + query)[0][0][0]["Plan"]
        AQPs[each_config] = (AQP)
        db.execute("set {} = false".format(each_config))
        
    print("\n######################################################################################################################\n")

    return AQPs

def cost_comparison_scan(node: Node, config_para_for_scans, AQPs):

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
        
        AQP = AQPs[each_config]

        nodes = capture_nodes(AQP, None)   
        
        is_bitmap_index_scan = False
        bitmap_index_scan_cond = None

        for node in nodes:
            # to find anotherrr forrm of scan
            print(node.type)
            print(node.information)
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
                print(aqp_node_type)
                print(qep_node_type)
                print(aqp_filter)
                print(qep_filter)
                continue
            if "scan" in aqp_node_type.lower() and aqp_relation == qep_relation:
                aqp_cost = node.information["Total Cost"] * node.information["Actual Loops"]
                cost_dict[aqp_node_type] = aqp_cost
                break

        # db.execute("set {} = false".format(each_config))

    if "join" in node.type.lower():
        config_para_for_joins = ["enable_hashjoin", "enable_nestloop", "enable_mergejoin"]
        pass

    
    return cost_dict

def capture_nodes(dct, parent, subquery_level=0):

    nodes = []

    cur = Node(dct, subquery_level)
    # print("Current Node Type: {}".format(type(cur)))

    if "Plans" in dct:
        plans = dct["Plans"]
        for plan in plans:

            dct = plan
            if "Subplan Name" not in dct:
                nodes = nodes + capture_nodes(dct, cur, subquery_level)
                
            else:
                nodes = nodes + capture_nodes(dct, cur, subquery_level + 1)

    cur.add_parent(parent)
    nodes.append(cur)
    

    # update cur node as child of its parent
    if not cur.parent[0] is None:
        cur.parent[0].add_child(cur)

    return nodes


if __name__ == "__main__":
    output_plan = db.execute('EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON) ' + q20)[0][0][0]["Plan"]
    print("in main")
    print("\n######################################################################################################################\n")
    cost_of_scans_and_joins = {}

    # nodes = []
    # capture_nodes(dct, None)

    nodes: list[Node] = capture_nodes(output_plan, None)
    config_para_for_scans = ["enable_bitmapscan", "enable_indexscan", "enable_indexonlyscan", "enable_seqscan", "enable_tidscan"]
    # AQPs = generate_AQPs(config_para_for_scans)
    AQPs = []

    j = 1
    for node in nodes:

        print("STEP {}".format(j))
        j = j + 1

        node_type = node.type
        print("NODE TYPE: {}".format(node_type))
        print("ESTIMATED COST: {}".format(node.get_estimated_cost()))
        print("MAPPING: {}".format(node.mapping()))
        # print("RELEVANT INFORMATION: \n{}".format(node.get_relevant_info()))
        print("OTHER INFORMATION: {}".format(node.information))
        if not node.parent[0] is None:
            print("PARENT NODE TYPE: {}".format(node.parent[0].type))
        print("SUBQUERY LEVEL: {}".format(node.subquery_level))
        
        children = node.children
        if children:
            i = 1
            for child in children:
                print("CHILD {} NODE TYPE: {}".format(i, node.children[0].type))
                i = i + 1

        if "scan" in node_type.lower(): # or "join" in node_type.lower():
            print("cost_comparison() here")
            print("COST COMPARISON: {}".format(cost_comparison_scan(node, config_para_for_scans, AQPs)))
        
        print("\n")

    print("\n######################################################################################################################\n")

    print("COMPONENT MAPPING: {}".format(component_mapping))


    db.close()