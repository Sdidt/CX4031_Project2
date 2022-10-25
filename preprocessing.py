import sqlparse
from sql_metadata import Parser
from connection import DB

class PreProcessor:
    def __init__(self, query) -> None:
        self.query_split = sqlparse.split(query)[0] # assuming only 1 query at a time
        self.query_split = sqlparse.format(query, reindent=True, keyword_case="upper").splitlines()
        self.query = self.clean(self.query_split)
        self.parser = Parser(self.query)
        self.tokens = sqlparse.parse(self.query)[0].tokens
        self.tables = self.get_relations()
        self.columns = self.get_columns()

    def clean(self, query_split):
        reconstructed_query = ""
        for clause in query_split:
            reconstructed_query += ' {}'.format(clause.strip())
        return reconstructed_query
        
    def get_relations(self):
        return self.parser.tables

    def get_columns(self):
        columns = self.parser.columns
        # logic to assign each set of columns to each table dynamically, by querying the DB for table info
        db = DB()

        return columns


if __name__ == "__main__":
    complex_query = "select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price,sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge from lineitem where l_shipdate <= date '1998-12-01' group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus"
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
                    nation,
                    blahblah
                where
                    ps_suppkey = s_suppkey
                    and s_nationkey = n_nationkey
                    and n_name = 'GERMANY'
                ) 
            order by
            value desc;
    """
    sql_query = "SELECT * FROM customer C, orders O WHERE C.c_custkey = O.o_custkey"
    parser = Parser(long_query)
    print(parser.subqueries)
    for token in parser.tokens:
        print(token)
    # print(parser.)
    preprocessor = PreProcessor(long_query)
    print(preprocessor.tables)
    print(preprocessor.columns)
    # print(preprocessor.query)
