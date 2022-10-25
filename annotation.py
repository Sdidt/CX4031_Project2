from dotenv import load_dotenv
from result_parser import ResultParser
from tree import Node
from connection import DB

load_dotenv()

def construct_operator_tree(actual_plan: dict):
    q = [actual_plan.copy()]
    # print(actual_plan.keys())
    actual_plan.pop("Plans")
    node_list = [Node(actual_plan.copy())]
    root = node_list[0]
    print(actual_plan)
    # print(q)
    while len(q) != 0:
        actual_plan = q.pop(0)
        parent: Node = node_list.pop(0)
        # print(parent.plan)
        # print("Actual plan:")
        # print(actual_plan)
        if "Plans" not in actual_plan:
            # print(actual_plan)
            continue
        actual_plan = actual_plan["Plans"]
        # print(len(actual_plan))
        for plan in actual_plan:
            # print(plan)
            if "Plans" in plan:
                q.append(plan.copy())
                plan.pop("Plans")
                node_list.append(Node(plan.copy()))
                parent.add_child(node_list[-1])
            else:
                parent.add_child(Node(plan.copy()))
            # print(plan)
    print(root.print_tree())
    return root

def annotate_various_nodes(root: Node):
	result_parser = ResultParser()
	level = 1
	q = [(root, level)]
	reverse_order = []
	prev_level = -1
	while len(q) != 0:
		cursor, level = q.pop(0)
		reverse_order.insert(0, (cursor, level))
		if level != prev_level:
			# print("Level " + str(level))
			prev_level = level
		# print(cursor.plan)
		q.extend([(child, level + 1) for child in cursor.children])
	while len(reverse_order) != 0:
		cursor, level = reverse_order.pop(0)
		# print(cursor.plan)
		if cursor.plan["Node Type"] == "Seq Scan":
			print(result_parser.seq_scan_rule(cursor, level == 1))
		elif cursor.plan["Node Type"] == "Hash":
			print(result_parser.hash_rule(cursor))
		elif cursor.plan["Node Type"] == "Hash Join":
			print(result_parser.hash_join_rule(cursor, level == 1))
		
db = DB()

result = db.execute("select version()")
print(result)

# conn = psycopg2.connect(database="TPC-H", user=os.getenv('DB_USERNAME'), password=os.getenv('DB_PASSWORD'), host="127.0.0.1", port=5432)

# cursor = conn.cursor()

# cursor.execute("select version()")

# data = cursor.fetchone()
# print("Connection established to: ",data)

# # cursor.execute("set enable_hashjoin = false")

# cursor.fetchall()

sql_query = 'SELECT * FROM customer C, orders O WHERE C.c_custkey = O.o_custkey'

complex_query = "select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price,sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge from lineitem where l_shipdate <= date '1998-12-01' group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus"

complex_sql_query = """
select 
      l_returnflag,
      l_linestatus,
      sum(l_quantity) as sum_qty,
      sum(l_extendedprice) as sum_base_price,
      sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
      sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
      avg(l_quantity) as avg_qty,
      avg(l_extendedprice) as avg_price,
      avg(l_discount) as avg_disc
    from
      lineitem
    where
      l_extendedprice > 100
    group by
      l_returnflag,
      l_linestatus
    order by
      l_returnflag,
      l_linestatus;
"""   

long_query = """
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
            ps_suppkey = s_suppkey
            and s_nationkey = n_nationkey
            and n_name = 'AUSTRIA'
        )
    order by
      value desc;
"""

print(sql_query)

analyze_fetched = db.execute('EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON) ' + sql_query)

actual_plan: dict = analyze_fetched[0][0][0]["Plan"]
print("Full Result:")
print(actual_plan)
print("Operator Tree:")
root = construct_operator_tree(actual_plan)

annotate_various_nodes(root)

db.close()
