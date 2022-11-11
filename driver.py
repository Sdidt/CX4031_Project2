from connection import DB
from preprocessing import PreProcessor
from annotation import Annotator

def process_query(query):
    db = DB()
    preprocessor = PreProcessor(query, db)

    print("\n######################################################################################################################\n")

    print("PREPROCESSING RESULTS")

    print("Query Components:")
    print()
    print(preprocessor.query_components)
    
    print()

    print("Decomposed Query:")
    print()
    print(preprocessor.decomposed_query)
    print(preprocessor.index_column_dict)
    annotator = Annotator(query, preprocessor.decomposed_query, preprocessor.query_components, db, preprocessor.index_column_dict)

    # print("TZIYU$$$$$$$$$$$$$$$$$$$$$$$$$$$")
    annotator.annotate_nodes()

    # j = 1
    # for node in annotator.QEP:
    #     print("STEP {}".format(j))
    #     j = j + 1

    #     node_type = node.type
    #     print("NODE TYPE: {}".format(node_type))
    #     print("ESTIMATED COST: {}".format(node.get_estimated_cost()))
    #     print("MAPPING: {}".format(node.mapping()))
    #     # print("RELEVANT INFORMATION: \n{}".format(node.get_relevant_info()))
    #     print("OTHER INFORMATION: {}".format(node.information))
    #     if not node.parent[0] is None:
    #         print("PARENT NODE TYPE: {}".format(node.parent[0].type))
    #     print("SUBQUERY LEVEL: {}".format(node.subquery_level))
        
    #     children = node.children
    #     if children:
    #         i = 1
    #         for child in children:
    #             print("CHILD {} NODE TYPE: {}".format(i, node.children[i-1].type))
    #             i = i + 1
    #     print("\n")
    # print("TZIYU$$$$$$$$$$$$$$$$$$$$$$$$$$$")

    print("\n######################################################################################################################\n")

    print("ANNOTATOR RESULTS")

    print(annotator.component_mapping)

    print("\n######################################################################################################################\n")

    db.close()

    return annotator.component_mapping

if __name__ == "__main__":
    
    sample_queries = [
        """
        select * from customer C,  orders O where C.c_custkey = O.o_custkey
        """,
        """
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
        """,
        """
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
        """,
        """
        select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge from lineitem where l_shipdate <= date '1998-12-01' group by l_returnflag, l_linestatus order by sum_disc_price, l_linestatus
        """,
        """
        select * from nation where nation.n_nationkey = 3;
        """
        """
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
                        AND l_shipdate >= date '1-1-1994'
                        AND l_shipdate < date '1-1-1994' + INTERVAL '1 year'
                )
        )
        AND s_nationkey = n_nationkey
        AND n_name = 'CANADA'
    ORDER BY
        s_name
        """
    ]

    process_query(sample_queries[1])
    


