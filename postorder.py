from __future__ import annotations
# from types import NoneType
from dotenv import load_dotenv
from connection import DB
from difflib import SequenceMatcher


load_dotenv()
db = DB()


decomposed_query = {
    'subqueries': {'sub_number_1': {'subqueries': {}, 'select': ['sum(partsupp_1.ps_supplycost*partsupp_1.ps_availqty)*0.0001000000'], 'from': ['p1', 's', 'nation'], 'as': {'partsupp': 'P1', 'supplier': 'S'}, 'where': ['P1.ps_suppkey = S.s_suppkey', 'S.s_nationkey = nation_1.n_nationkey', "nation_1.n_name = 'GERMANY'"]}}, 'select': ['partsupp.ps_partkeyps', 'sum(partsupp.ps_supplycost*partsupp.ps_availqty)'], 'as': {'value': 'sum(partsupp.ps_supplycost*partsupp.ps_availqty)', 'partsupp': 'P'}, 'from': ['p', 'supplier', 'nation'], 'where': ['PS.ps_suppkey = supplier.s_suppkey', 'supplier.s_nationkey <> nation.n_nationkey', "nation.n_name = 'GERMANY'", 'P.ps_supplycost > 20', 'supplier.s_acctbal > 10'], 'group by': ['p.ps_partkey'], 'having': ['sum ( P.ps_supplycost * P.ps_availqty ) > sub_number_1'], 'order by': ['sum(partsupp.ps_supplycost*partsupp.ps_availqty)', 'asc']
    }

query_component_dict = {
    'subqueries': {'sub_number_1': {'subqueries': {}, 'select': 'select sum ( ps_supplycost * ps_availqty ) * 0.0001000000', '': '', 'from': 'from partsupp P1 , supplier S , nation', 'where': "where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY'"}}, 'select': 'select ps_partkey PS , sum ( ps_supplycost * ps_availqty ) as value', '': '', 'from': 'from partsupp P , supplier , nation', 'where': "where PS.ps_suppkey = s_suppkey and not s_nationkey = n_nationkey and n_name = 'GERMANY' and ps_supplycost > 20 and s_acctbal > 10", 'group by': 'group by ps_partkey', 'having': 'having sum ( ps_supplycost * ps_availqty ) >sub_number_1', 'order by': 'order by value ;'
    }


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

    def add_child(self, child: Node) -> None:
        self.children.append(child)

    def add_parent(self, parent: Node) -> None:
        self.parent.append(parent)

    def mapping(self):

        keywords = {}

        if self.type == "Seq Scan":
            keywords = self.node_seq_scan()
        elif self.type == "Hash":
            keywords = self.node_hash()
        elif self.type == "Hash Join":
            keywords = self.node_hash_join()
        elif self.type == "Materialize":
            pass
        elif self.type == "Nested Loop":
            pass
        elif self.type == "Aggregate":
            keywords = self.node_aggregate()
        elif self.type == "Sort":
            keywords = self.node_sort()
        elif self.type == "Gather Merge":
            pass
        else:
            pass
    
        # converting dictionary to a list of dictionaries
        lst = []
        for key,value in keywords.items():
            dct = {}
            dct[key] = value
            lst.append(dct)
        return lst

    def node_aggregate(self):
        relevant_info = {}
        if 'Group Key' in self.information:
            group = self.information["Group Key"]
            relevant_info["GROUP BY"] = group
        return relevant_info

    def node_sort(self):
        relevant_info = {}
        order = "DESC" if self.information["Sort Key"][0][-4:] == "DESC" else "ASC"
        relevant_info["ORDER BY"] = order
    
        return relevant_info
    
    def node_seq_scan(self):
        relevant_info = {}
        relation = self.information.get("Relation Name")
        relevant_info["FROM"] = relation

        if "Filter" in self.information:
            filter = self.information.get("Filter")

            if "= ANY" not in filter:
                filter = filter.split("::", 1)[0][1:]
                relevant_info["WHERE"] = filter
            else:
                filter1 = filter.split(" = ANY", 1)[0][1:]
                filter2 = filter.split("ANY (", 1)[1].split("::", 1)[0]
                relevant_info["WHERE"] = filter1
                relevant_info["IN"] = filter2
        
        alias = self.information.get("Alias")
        if len(alias) > len(relation):
            # confirm that is subquery, now need to know the level
            level = int(alias[-1])
            relevant_info["level"] = level
        else:
            relevant_info["level"] = 0

        return relevant_info

    def node_hash(self):
        relevant_info = {}
        return relevant_info

    def node_hash_join(self):
        relevant_info = {}
        condition = self.information['Hash Cond'][1:-1]
        relevant_info["where"] = condition
        original_query_component = query_component_dict['where']
        for k, v in relevant_info.items():
            print("blahblahblah")
            similarity_score = 0
            for clause in decomposed_query[k]:
                print(clause)
                if similarity_score < SequenceMatcher(None, clause, v).ratio():
                    similarity_score = SequenceMatcher(None, clause, v).ratio()
                    print(similarity_score)
            if similarity_score > 0.65:
                print("Fairly good match is found")
        print(original_query_component)
        print("The condition " + condition + " is implemented using hash join because ...")
        return relevant_info
    



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