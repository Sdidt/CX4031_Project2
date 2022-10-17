import os
from dotenv import load_dotenv

load_dotenv()
import psycopg2 

def retrieve_plans(actual_plan: dict):
    q = [actual_plan.copy()]
    # print(actual_plan.keys())
    actual_plan.pop("Plans")
    print(actual_plan)
    # print(q)
    while len(q) != 0:
        actual_plan = q.pop(0)
        # print("Actual plan:")
        # print(actual_plan)
        if "Plans" not in actual_plan:
            print(actual_plan)
            continue
        actual_plan = actual_plan["Plans"]
        # print(len(actual_plan))
        for plan in actual_plan:
            # print(plan.keys())
            if "Plans" in plan:
                q.extend(plan["Plans"])
            plan.pop("Plans", "No key found")
            print(plan)


conn = psycopg2.connect(database="TPC-H", user=os.getenv('DB_USERNAME'), password=os.getenv('DB_PASSWORD'), host="127.0.0.1", port=5432)

cursor = conn.cursor()

cursor.execute("select version()")

data = cursor.fetchone()
print("Connection established to: ",data)

sql_query = 'SELECT * FROM customer C, orders O WHERE C.c_custkey = O.o_custkey'

cursor.execute('EXPLAIN (ANALYZE, COSTS, FORMAT JSON) ' + sql_query)
analyze_fetched = cursor.fetchall()

actual_plan: dict = analyze_fetched[0][0][0]["Plan"]
print("Full Result:")
print(actual_plan)
print("Decomposed results:")
retrieve_plans(actual_plan)

# print(actual_plan)
# print(actual_plan.keys())
# print(actual_plan["Node Type"])

# while "Plans" in actual_plan:
#     actual_plan = actual_plan["Plans"]
#     print(len(actual_plan))
#     for plan in actual_plan:
#         print(plan.keys())
#         print(plan["Node Type"])

conn.close()
