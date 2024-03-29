from connection import DB
from preprocessing import PreProcessor
from annotation import Annotator
import json


def process_query(query, host, dbname, username, password, port):
    """
    Procedure invoked by the frontend to put all the pieces together:
    1. Establishes database connection
    2. Performs preprocessing
    3. Performs annotation
    4. Returns annotated results for display

    Parameter query: input SQL query string

    Parameter host: host name for DB connection

    Parameter dbname: database name for DB connection

    Parameter username: username for DB connection

    Parameter password: password for DB connection

    Parameter port: port for DB connection
    """
    try:
        db = DB(host, dbname, username, password, port)
    except Exception as e:
        print("Error: {}".format(e))
        return {"connection-error": True}
    try:
        preprocessor = PreProcessor(query, db)
    except Exception as e:
        print("Error: {}".format(e))
        return {}

    print("\n######################################################################################################################\n")

    print("PREPROCESSING RESULTS")

    print("Query Components:")
    print()
    print(preprocessor.query_components)
    
    print()

    print("Decomposed Query:")
    print()
    print(preprocessor.decomposed_query)

    print("----------------Index Column Dictionary----------------------")
    print(json.dumps(preprocessor.index_column_dict, indent=2, sort_keys=True))
    try:
        annotator = Annotator(query, preprocessor.decomposed_query, preprocessor.query_components, db, preprocessor.index_column_dict)
        
    except Exception as e:
        print(e)
        return {}

    annotator.annotate_nodes()

    print("\n######################################################################################################################\n")

    print("ANNOTATOR RESULTS")

    print(annotator.component_mapping)

    print("\n######################################################################################################################\n")

    db.close()

    return annotator.component_mapping

if __name__ == "__main__":
    
    # for testing this file individually

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
        """,
        """
        select * from part where p_brand = 'Brand#13' and p_size <> (select max(p_size) from part);
        """
        """
        select n_nationkey from nation union select s_nationkey from supplier
        """,
        """
        SELECT 
     p_brand,
     p_type,
     p_size,
     COUNT(DISTINCT ps_suppkey) AS supplier_cnt
FROM
     partsupp,
     part
WHERE
     p_partkey = ps_partkey
     AND p_brand <> 'Brand#45'
     AND p_type NOT LIKE 'MEDIUM POLISHED%'
     AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
     AND ps_suppkey NOT IN (
		SELECT
			s_suppkey
		FROM
			supplier
		WHERE
			s_comment LIKE '%Customer%Complaints%'
     )
GROUP BY
     p_brand,
     p_type,
     p_size
ORDER BY
     supplier_cnt DESC,
     p_brand,
     p_type,
     p_size
        """
    ]

    process_query(sample_queries[-1])
