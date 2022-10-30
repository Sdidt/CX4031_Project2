from sql_metadata import token, Parser
import copy
from connection import DB

class PreProcessor:
    def __init__(self, query) -> None:
        # self.query_split = sqlparse.split(query)[0] # assuming only 1 query at a time
        # self.query_split = sqlparse.format(query, reindent=True, keyword_case="upper").splitlines()
        # self.query = self.clean(self.query_split)
        self.query = query
        self.parser = Parser(self.query)
        self.tokens = self.parser.tokens
        self.tables = self.parser.tables
        self.columns = self.get_columns()
        self.not_transformations = {
            "<": ">=",
            ">": "<=",
            "=": "<>",
            "<>": "=",
            "!=": "=",
            "<=": ">",
            ">=": "<"
        }
        self.curr_subquery_num = 1
        self.aggregate_list = ["count", "sum", "avg", "approx_count_distinct", "max", "min", "stdev", "stdevp", "var", "varp", "string_agg"]
        self.decomposed_query = self.extract_keywords(self.tokens, {"subqueries": {}})

    def clean(self, query_split):
        reconstructed_query = ""
        for clause in query_split:
            reconstructed_query += ' {}'.format(clause.strip())
        return reconstructed_query
        
    def get_relations(self):
        return self.parser.tables

    # TODO: Complete the logic to map columns to tables
    def get_columns(self):
        columns = self.parser.columns
        # logic to assign each set of columns to each table dynamically, by querying the DB for table info
        return columns

    def extract_where(self, tokens, clause_list: dict):
        i = 0
        last_index = i
        curr_clause_not = False
        [print(t) for t in tokens]
        while i < len(tokens):
            t: token.SQLToken = tokens[i]
            print("Value: " + str(t.value))
            if t.is_keyword and (t.value.lower() == "and" or t.value.lower() == "or"):
                tok_lst = [tok.value for tok in tokens[last_index: i]]
                print(tok_lst)
                if (curr_clause_not):
                    print("Found NOT clause")
                    tok_lst[1] = self.not_transformations[tok_lst[1].lower()]
                    curr_clause_not = False
                print(tok_lst)
                clause_list.append(" ".join(tok_lst))
                last_index = i + 1
            elif (t.is_keyword and t.value.lower() == "not"):
                curr_clause_not = True
                last_index = i + 1
            i += 1
        tok_lst = [tok.value for tok in tokens[last_index: i]]
        print(tok_lst)
        if (curr_clause_not):
            print("Found NOT clause")
            tok_lst[1] = self.not_transformations[tok_lst[1].lower()]
            curr_clause_not = False
        print(tok_lst)
        clause_list.append(" ".join(tok_lst))
        return clause_list

    def extract_keywords(self, tokens, decomposed_query, parenthesis_level=0, subquery_alias=None):
        last_keyword_token = token.SQLToken()
        # print("Incoming parenthesis level")
        # print(parenthesis_level)
        # for t in tokens:
        #     print(t.value)
        # print(decomposed_query)
        last_keyword_token.parenthesis_level = parenthesis_level
        last_comma_index = None
        i = 0
        while i < len(tokens):
            t: token.SQLToken = tokens[i]
            # print("TOKEN " + str(i))
            print("Value: " + str(t.value))
            print(t.is_keyword)
            # print("Different types: ")
            # print("Is keyword: " + str(t.is_keyword))
            # print("Is punctuation: " + str(t.is_punctuation))
            # print("Is dot: " + str(t.is_dot))
            # print("Is wildcard: " + str(t.is_wildcard))
            # print("Is integer: " + str(t.is_integer))
            # print("Is float: " + str(t.is_float))
            # print("Is comment: " + str(t.is_comment))
            # print("Is as keyword: " + str(t.is_as_keyword))
            # print("Is left parenthesis: " + str(t.is_left_parenthesis))
            # print("Is right parenthesis: " + str(t.is_right_parenthesis))
            # print("Parenthesis level: " + str(t.parenthesis_level))
            if (t.value == ","):
                if last_comma_index is not None and last_comma_index != i:
                    decomposed_query[last_keyword_token.value].append("".join([t.value.lower() for t in tokens[last_comma_index + 1: i]]))
                last_comma_index = i
                
            if (t.is_keyword):
                if last_comma_index is not None and last_comma_index != i:
                   decomposed_query[last_keyword_token.value].append("".join([t.value.lower() for t in tokens[last_comma_index + 1: i]]))
                last_comma_index = i     
                # print(t.parenthesis_level)
                # print(last_keyword_token.parenthesis_level)
                if (t.parenthesis_level == last_keyword_token.parenthesis_level):
                    decomposed_query[t.value] = []
                    last_keyword_token = t
                    if (t.value.lower() == "where"):
                        where_tokens = []
                        i += 1
                        t = tokens[i]
                        # a new keyword besides "and" or "or" marks the end of a group of where clauses
                        while ((not t.is_keyword) or (t.value.lower() in ["and", "or", "not"])):
                            where_tokens.append(t)
                            i += 1
                            if i < len(tokens):
                                t = tokens[i]
                            else:
                                break
                        i -= 1
                        print("where clause tokens: ")
                        for tok in where_tokens:
                            print("Value: " + str(tok.value))
                        decomposed_query[last_keyword_token.value] = self.extract_where(where_tokens, [])
                        last_comma_index = i + 1
                # subquery
                else:
                    print("Reached subquery case")
                    subquery_tokens = []
                    # a ')' with same parenthesis level as parent query marks end of subquery
                    while t.value != ")" or t.parenthesis_level != last_keyword_token.parenthesis_level: 
                        t = tokens[i]
                        subquery_tokens.append(t)
                        i += 1
                    print("Subquery clause tokens: ")
                    for tok in subquery_tokens:
                        print("Value: " + tok.value)
                    decomposed_query["subqueries"]["sub_number_" + str(self.curr_subquery_num)] = (self.extract_keywords(subquery_tokens[:-1], {"subqueries": {}}, t.parenthesis_level + 1))
                    self.curr_subquery_num += 1

            # aggregates
            # elif t.value.lower() in self.aggregate_list:
            #     agg = t.value.lower()
            #     i += 1
            #     t = tokens[i]
            #     # a ")" token with same parenthesis level as parent query marks end of aggregate 
            #     while t.value != ")" or t.parenthesis_level != last_keyword_token.parenthesis_level:
            #         agg += t.value.lower()
            #         i += 1
            #         t = tokens[i]
            #     i -= 1
            #     agg += t.value.lower()
            #     print("Aggregate: " + agg)
            #     decomposed_query[last_keyword_token.value].append(agg)
            
            # default
            # elif (not t.is_comment and not t.is_punctuation):
            #     print(last_keyword_token.value)
            #     print(decomposed_query)
            #     decomposed_query[last_keyword_token.value].append(t.value)
            
            i += 1
        return decomposed_query


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
                    partsupp,
                    supplier,
                    nation
                where
                    ps_suppkey = s_suppkey
                    and s_nationkey = n_nationkey
                    and n_name = 'GERMANY'
                ) as T
            order by
            value desc;
    """
    sql_query = "SELECT * FROM customer C, orders O WHERE C.c_custkey = O.o_custkey"
    preprocessor = PreProcessor(long_query)
    print(preprocessor.tables)
    print(preprocessor.columns)
    print(preprocessor.decomposed_query)
    # print(preprocessor.query)
