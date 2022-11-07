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

primary_key_query = "select * from nation where nation.n_nationkey = 3;"

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
db.execute("set enable_indexscan = false")
# db.execute("set enable_bitmapscan = false")

analyze_fetched = db.execute('EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON) ' + query)

dct = analyze_fetched[0][0][0]["Plan"]


class Node():

    def __init__(self, data: dict, subquery_level) -> None:

        self.type = data.get("Node Type")
        self.subquery_level = subquery_level
        self.parent = []
        self.children = []
        self.information = None

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

    def mapping(self):

        keywords = {}

        if self.type == "Seq Scan":
            keywords = self.node_seq_scan()
        elif self.type == "Index Scan":
            keywords = self.node_index_scan()
        elif self.type == "Bitmap Index Scan":
            keywords = self.node_bitmap_index_scan()
        elif self.type == "Bitmap Heap Scan":
            keywords = self.node_bitmap_heap_scan()
        elif self.type == "Hash":
            keywords = self.node_hash()
        elif self.type == "Hash Join":
            keywords = self.node_hash_join()
        elif self.type == "Materialize":
            pass
        elif self.type == "Nested Loop":
            keywords = self.node_nested_loop()
        elif self.type == "Aggregate":
            keywords = self.node_aggregate()
        elif "Sort" in self.type:
            keywords = self.node_sort()
        elif self.type == "Gather Merge":
            pass
        else:
            pass
    
        # converting dictionary to a list of dictionaries
        lst = [{"level": self.subquery_level}]
        for key,value in keywords.items():
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
    
        self.find_match_in_decomposed_query(relevant_info, decomposed_query, query_component_dict)
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
        self.find_match_in_decomposed_query(relevant_info, decomposed_query, query_component_dict)

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

        self.find_match_in_decomposed_query(relevant_info, decomposed_query, query_component_dict)

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

        self.find_match_in_decomposed_query(relevant_info, decomposed_query, query_component_dict)

        return relevant_info

    def node_bitmap_heap_scan(self):
        relevant_info = {}
        relation = self.information.get("Alias")
        relevant_info["from"] = relation

        self.find_match_in_decomposed_query(relevant_info, decomposed_query, query_component_dict)

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
        self.find_match_in_decomposed_query(relevant_info, decomposed_query, query_component_dict)
        
        return relevant_info
    
    # complete
    def node_nested_loop(self):
        relevant_info = {}
        condition = self.information['Join Filter'][1:-1]
        keywords = ["where", "in"]
        for keyword in keywords:
            relevant_info[keyword] = condition
        self.find_match_in_decomposed_query(relevant_info, decomposed_query, query_component_dict)

        return relevant_info

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
        print(original_query_components)
        print(conditions)
        print(explanations)
        print(component_mapping)
        
        



def capture_nodes(dct, parent, subquery_level=0):

    cur = Node(dct, subquery_level)
    # print("Current Node Type: {}".format(type(cur)))

    if "Plans" in dct:
        plans = dct["Plans"]
        for plan in plans:

            dct = plan
            if "Subplan Name" not in dct:
                capture_nodes(dct, cur, subquery_level)
            else:
                capture_nodes(dct, cur, subquery_level + 1)

    cur.add_parent(parent)
    nodes.append(cur)

    # update cur node as child of its parent
    if not cur.parent[0] is None:
        cur.parent[0].add_child(cur)


print("\n######################################################################################################################")

nodes = []
capture_nodes(dct, None)

j = 1
for node in nodes:


    print("STEP {}".format(j))
    j = j + 1
    print("NODE TYPE: {}".format(node.type))
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

    print("\n")

print("\n######################################################################################################################")

db.close()