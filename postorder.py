from __future__ import annotations
from types import NoneType
from dotenv import load_dotenv
from connection import DB


load_dotenv()
db = DB()

query = """
select
      ps_partkey,
      sum(ps_supplycost * ps_availqty) as value
    from
      partsupp,
      supplier,
      nation
    where
      ps_suppkey = s_suppkey
      and s_nationkey = n_nationkey
      and n_name = 'GERMANY'
      and ps_supplycost > 20
      and s_acctbal > 10
    group by
      ps_partkey having
        sum(ps_supplycost * ps_availqty) > (
          select
            sum(ps_supplycost * ps_availqty) * 0.0001000000
          from
            partsupp,
            supplier,
            nation
          where
            not ps_suppkey < s_suppkey
            and s_nationkey = n_nationkey
            and n_name = 'AUSTRIA'
        )
    order by
      value desc;
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

from_count = query.count("from", 0, len(query))
# since total "FROM" count is 2, possibilities are NATION and NATION_1
# assuming the deeper it goes, the higher the number at the back
current_count = from_count - 1
prev_seq_scan = False

class Node():
    def __init__(self, data: dict) -> None:
        self.type = data.get("Node Type")
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
        global current_count
        global prev_seq_scan

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
            keywords = self.node_nested_loop()
        elif self.type == "Aggregate":
            keywords = self.node_aggregate()
        elif self.type == "Sort":
            keywords = self.node_sort()
        elif self.type == "Gather Merge":
            pass
        else:
            pass
    
        # switch flag to decrement current count
        if self.type != "Seq Scan" and prev_seq_scan:
            current_count = current_count - 1
            prev_seq_scan = False

        # converting dictionary to a list of dictionaries
        lst = []
        for key,value in keywords.items():
            dct = {}
            dct[key] = value
            lst.append(dct)

        if isinstance(self.parent[0], NoneType):
            dct = {}
            dct["SELECT"] = self.information["Output"]
            lst.append(dct)

        return lst

    def node_nested_loop(self):
        relevant_info = {}
        if "Join Filter" in self.information:
            relevant_info["WHERE"] = self.information["Join Filter"]

        return relevant_info

    def node_aggregate(self):
        relevant_info = {}
        if 'Group Key' in self.information:
            group = self.information["Group Key"]
            relevant_info["GROUP BY"] = group

        output = self.information["Output"]
        for each in output:
            if "sum" in each:
                relevant_info["SUM"] = each.split("sum",1)[1]
            # below all not tested yet 
            if "max" in each:
                relevant_info["MAX"] = each.split("max",1)[1]
            if "count" in each:
                relevant_info["COUNT"] = each.split("count",1)[1]
            if "min" in each:
                relevant_info["MIN"] = each.split("min",1)[1]
            if "avg" in each:
                relevant_info["AVG"]= each.split("avg",1)[1]

        # still need tackle words like UNION and SELECT DISTINCT

        return relevant_info

    def node_sort(self):

        relevant_info = {}
        order = "DESC" if self.information["Sort Key"][0][-4:] == "DESC" else "ASC"
        key = self.information["Sort Key"][0] if order == "ASC" else self.information["Sort Key"][0][:-5]
        relevant_info["ORDER BY"] = [order, key]

        return relevant_info
    
    def node_seq_scan(self):
        global current_count
        global prev_seq_scan

        relevant_info = {}
        relation = self.information.get("Relation Name")
        relevant_info["FROM"] = relation + "_{}".format(current_count) if current_count != 0 else relation       

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
        
        # alias = self.information.get("Alias")
        # if len(alias) > len(relation):
        #     # confirm that is subquery, now need to know the level
        #     level = int(alias[-1])
        #     relevant_info["level"] = level
        # else:
        #     relevant_info["level"] = 0

        prev_seq_scan = True

        return relevant_info

    def node_hash(self):
        relevant_info = {}
        return relevant_info

    def node_hash_join(self):
        relevant_info = {}
        condition = self.information['Hash Cond'][1:-1]
        relevant_info["WHERE"] = condition
        
        return relevant_info
    

def capture_nodes(dct, parent):

    cur = Node(dct)
    # print("Current Node Type: {}".format(type(cur)))

    if "Plans" in dct:
        plans = dct["Plans"]
        for plan in plans:
            dct = plan
            capture_nodes(dct, cur)

    cur.add_parent(parent)
    nodes.append(cur)

    # update cur node as child of its parent
    if not isinstance(cur.parent[0], NoneType):
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
    if not isinstance(node.parent[0], NoneType):
        print("PARENT NODE TYPE: {}".format(node.parent[0].type))
    
    children = node.children
    if children:
        i = 1
        for child in children:
            print("CHILD {} NODE TYPE: {}".format(i, node.children[0].type))
            i = i + 1

    print("\n")

print("\n######################################################################################################################")

db.close()