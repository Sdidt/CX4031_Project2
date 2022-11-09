{'query_1': {'subqueries': 
    {'subquery_1': {'subqueries': {}, 'from partsupp P1 , supplier S , nation': ['The clause "from p1" is implemented using Seq Scan because no other option is available.', 'The clause "from s" is implemented using Seq Scan because it requires 44.62, 10351967.39 less operations than Bitmap Heap Scan, Index Scan respectively.', 'The clause "from nation_1" is implemented using Seq Scan because it requires 2.90, 277777779.19 less operations than Bitmap Heap Scan, Index Scan respectively.'], "where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY'": ['The clause "where nation_1.n_name = \'GERMANY\'" is implemented using Seq Scan because it requires 2.90, 277777779.19 less operations than Bitmap Heap Scan, Index Scan respectively.', 'The clause "where s.s_nationkey = nation_1.n_nationkey" is implemented using Hash Join.', 'The clause "where p1.ps_suppkey = s.s_suppkey" is implemented using Hash Join.']}}
    , 'from partsupp P , supplier , nation': ['The clause "from p" is implemented using Seq Scan because no other option is available.', 'The clause "from nation" is implemented using Seq Scan because no other option is available.', 'The clause "from supplier" is implemented using Seq Scan because it requires 124.21, 28818445.30 less operations than Bitmap Heap Scan, Index Scan respectively.'], "where P.ps_suppkey = s_suppkey and not s_nationkey = n_nationkey and n_name = 'GERMANY' and ps_supplycost > 20 and s_acctbal > 10": ['The clause "where p.ps_supplycost > \'20\'" is implemented using Seq Scan because no other option is available.', 'The clause "where nation.n_name = \'GERMANY\'" is implemented using Seq Scan because no other option is available.', 'The clause "where supplier.s_acctbal > \'10\'" is implemented using Seq Scan because it requires 124.21, 28818445.30 less operations than Bitmap Heap Scan, Index Scan respectively.', 'The clause "where supplier.s_nationkey <> nation.n_nationkey" is implemented using Nested Loop.', 'The clause "where p.ps_suppkey = supplier.s_suppkey" is implemented using Hash Join.'], 'order by value ;': ['The clause "order by sum((p.ps_supplycost * (p.ps_availqty) asc" is implemented using external merge Sort.']
    }
}


{'query_1': {
    'subqueries': {
        'subquery_1': 
        {
            'subqueries': {
                'subsubquery_1': {'subqueries': {}, 'SELECT': ['part_2.p_partkey'], 'FROM': ['part'], 'WHERE': ["part_2.p_name LIKE 'forest%'"]}, 'subsubquery_2': {'subqueries': {}, 'SELECT': ['0.5*sum(lineitem_2.l_quantity)'], 'FROM': ['lineitem'], 'WHERE': ['lineitem_2.l_partkey = partsupp_2.ps_partkey', 'lineitem_2.l_suppkey = partsupp_2.ps_suppkey', 'lineitem_2.l_shipdate >= MDY ( 1 , 1 , 1994 )', 'lineitem_2.l_shipdate < MDY ( 1 , 1 , 1994 ) + 1 UNITS'], 'YEAR': ['']}
                }, 'SELECT': ['partsupp_1.ps_suppkey'], 'FROM': ['partsupp'], 'WHERE': ["partsupp_1.ps_partkey IN ( SELECT part_2.p_partkey FROM part WHERE part_2.p_name LIKE 'forest%' ) AND partsupp_1.ps_availqty > subsubquery_2"]
        }
        }, 'SELECT': ['supplier.s_name', 'supplier.s_address'], 'FROM': ['supplier', 'nation'], 'WHERE': ['supplier.s_suppkey IN subquery_1', "nation.n_name = 'CANADA'"], 'ORDER BY': ['supplier.s_name']
    }
}

{
    'query_1': {
        'subqueries': {
            'subquery_1': {'subqueries': 
                {'subsubquery_1': {
                    'subqueries': {}, 'SELECT': ['part_2.p_partkey'], 'FROM': ['part'], 'WHERE': ["part_2.p_name LIKE 'forest%'"]
                    }, 
                    'subsubquery_2': {
                        'subqueries': {}, 'SELECT': ['0.5*sum(lineitem_2.l_quantity)'], 'FROM': ['lineitem'], 'WHERE': ['lineitem_2.l_partkey = partsupp_2.ps_partkey', 'lineitem_2.l_suppkey = partsupp_2.ps_suppkey', 'lineitem_2.l_shipdate >= MDY ( 1 , 1 , 1994 )', 'lineitem_2.l_shipdate < MDY ( 1 , 1 , 1994 ) + 1 UNITS'], 'YEAR': ['']
                    }
                }, 'SELECT': ['partsupp_1.ps_suppkey'], 'FROM': ['partsupp'], 'WHERE': ['partsupp_1.ps_partkey IN subsubquery_1', 'partsupp_1.ps_availqty > subsubquery_2']
            }
        }, 'SELECT': ['supplier.s_name', 'supplier.s_address'], 'FROM': ['supplier', 'nation'], 'WHERE': ['supplier.s_suppkey IN subquery_1', 'supplier.s_nationkey = nation.n_nationkey', "nation.n_name = 'CANADA'"], 'ORDER BY': ['supplier.s_name']
    }
}

{
    'query_1': {
        'subqueries': {
            'subquery_1': {
                'subqueries': {
                    'subsubquery_1': {
                        'subqueries': {}, 'SELECT': 'SELECT p_partkey', 'FROM': 'FROM part', 'WHERE': "WHERE p_name LIKE 'forest%'"
                        },
                    'subsubquery_2': {
                        'subqueries': {}, 'SELECT': 'SELECT 0.5 * SUM ( l_quantity )', 'FROM': 'FROM lineitem', 'WHERE': 'WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey AND l_shipdate >= MDY ( 1 , 1 , 1994 ) AND l_shipdate < MDY ( 1 , 1 , 1994 ) + 1 UNITS', 'YEAR': 'YEAR'
                        }
                    }, 'SELECT': 'SELECT ps_suppkey', 'FROM': 'FROM partsupp', 'WHERE': 'WHERE ps_partkey IN (subsubquery_1 ps_availqty >subsubquery_2'
                }
            }, 'SELECT': 'SELECT s_name , s_address', 'FROM': 'FROM supplier , nation', 'WHERE': "WHERE s_suppkey IN (subquery_1 s_nationkey = n_nationkey AND n_name = 'CANADA'", 'ORDER BY': 'ORDER BY s_name'
    }
}