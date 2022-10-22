import os
from dotenv import load_dotenv
from tree import Node

load_dotenv()
import psycopg2 

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
        print(parent.data)
        # print("Actual plan:")
        # print(actual_plan)
        if "Plans" not in actual_plan:
            print(actual_plan)
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
            print(plan)
    print(root.print_tree())


conn = psycopg2.connect(database="TPC-H", user=os.getenv('DB_USERNAME'), password=os.getenv('DB_PASSWORD'), host="127.0.0.1", port=5432)

cursor = conn.cursor()

cursor.execute("select version()")


data = cursor.fetchone()
print("Connection established to: ",data)

# cursor.execute("set enable_hashjoin = false")

cursor.fetchall()

sql_query = 'SELECT * FROM customer C, orders O WHERE C.c_custkey = O.o_custkey'

complex_sql_qeury = """
select 
      l_returnflag,
      l_linestatus,
      sum(l_quantity) as sum_qty,
      sum(l_extendedprice) as sum_base_price,
      sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
      sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
      avg(l_quantity) as avg_qty,
      avg(l_extendedprice) as avg_price,
      avg(l_discount) as avg_disc,
      count(*) as count_order
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

cursor.execute('EXPLAIN (ANALYZE, COSTS, FORMAT JSON) ' + sql_query)
analyze_fetched = cursor.fetchall()

actual_plan: dict = analyze_fetched[0][0][0]["Plan"]
print("Full Result:")
print(actual_plan)
print("Decomposed results:")
construct_operator_tree(actual_plan)

conn.close()
