from connection import DB
from preprocessing import PreProcessor
from annotation import Annotator

# region
# def generate_AQPs(config_paras, db: DB):
    
#     print("\n######################################################################################################################\n")

#     print("GENERATING AQPS")

#     AQPs = {k: None for k in config_paras}
    
#     for each_config in config_paras:
#         db.execute("set {} = false".format(each_config))

#     for each_config in config_paras:
#         db.execute("set {} = true".format(each_config))
#         AQP = db.execute('EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON) ' + query)[0][0][0]["Plan"]
#         AQPs[each_config] = (AQP)
#         db.execute("set {} = false".format(each_config))
        
#     print("\n######################################################################################################################\n")

#     return AQPs

# def generate_QEP(dct, parent, subquery_level=0):

#     nodes = []

#     cur = Node(dct, subquery_level)
#     # print("Current Node Type: {}".format(type(cur)))

#     if "Plans" in dct:
#         plans = dct["Plans"]
#         for plan in plans:

#             dct = plan
#             if "Subplan Name" not in dct:
#                 nodes = nodes + generate_QEP(dct, cur, subquery_level)
                
#             else:
#                 nodes = nodes + generate_QEP(dct, cur, subquery_level + 1)

#     cur.add_parent(parent)
#     nodes.append(cur)
    

#     # update cur node as child of its parent
#     if not cur.parent[0] is None:
#         cur.parent[0].add_child(cur)

#     return nodes

# def cost_comparison_scan(node: Node, config_para_for_scans, AQPs):

#     qep_node_type = node.type
#     qep_relation = node.information["Relation Name"]
#     qep_filter = node.information.get("Filter")
#     if qep_filter is None:
#         qep_filter = node.information.get("Index Cond")
#     qep_cost = node.information["Total Cost"] * node.information["Actual Loops"]
#     cost_dict = {}
#     cost_dict[qep_node_type] = qep_cost
    
#     print(config_para_for_scans)

#     # enable one each time
#     for each_config in config_para_for_scans:
        
#         AQP = AQPs[each_config]

#         nodes = generate_QEP(AQP, None)   
        
#         is_bitmap_index_scan = False
#         bitmap_index_scan_cond = None

#         for node in nodes:
#             # to find anotherrr forrm of scan
#             print(node.type)
#             print(node.information)
#             aqp_node_type = node.type
#             if node.type == "Bitmap Index Scan":
#                 is_bitmap_index_scan = True
#                 bitmap_index_scan_cond = node.information["Index Cond"]
#             # if "Relation Name" not in node.information:
#             #     print(node.type)
#             #     print(node.information)
#             if "Relation Name" not in node.information:
#                 continue
#             aqp_filter = None
#             aqp_relation = node.information["Relation Name"]
#             if qep_filter is not None:
#                 if is_bitmap_index_scan:
#                     aqp_filter = bitmap_index_scan_cond
#                     is_bitmap_index_scan = False
#                     bitmap_index_scan_cond = None
#                 aqp_filter = node.information.get("Filter")
#                 if aqp_filter is None:
#                     aqp_filter = node.information.get("Index Cond")
#                     if aqp_filter is None:
#                         continue
#             if qep_filter != aqp_filter:
#                 print("Condition is not matching!")
#                 print(aqp_node_type)
#                 print(qep_node_type)
#                 print(aqp_filter)
#                 print(qep_filter)
#                 continue
#             if "scan" in aqp_node_type.lower() and aqp_relation == qep_relation:
#                 aqp_cost = node.information["Total Cost"] * node.information["Actual Loops"]
#                 cost_dict[aqp_node_type] = aqp_cost
#                 break

#         # db.execute("set {} = false".format(each_config))

#     if "join" in node.type.lower():
#         config_para_for_joins = ["enable_hashjoin", "enable_nestloop", "enable_mergejoin"]
#         pass

    
#     return cost_dict
# endregion

if __name__ == "__main__":
    db = DB()

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

    preprocessor = PreProcessor(primary_key_query, db)

    print("\n######################################################################################################################\n")

    print("PREPROCESSING RESULTS")

    print(preprocessor.query_components)
    print(preprocessor.decomposed_query)

    annotator = Annotator(primary_key_query, preprocessor.decomposed_query, preprocessor.query_components, db, preprocessor.index_column_dict)

    annotator.annotate_nodes()

    print("\n######################################################################################################################\n")

    print("ANNOTATOR RESULTS")

    print(annotator.component_mapping)

    print("\n######################################################################################################################\n")

    db.close()


