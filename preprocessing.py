from sql_metadata import token, Parser
import copy
from connection import DB

class PreProcessor:

    def __init__(self, query, db) -> None:
        # self.query_split = sqlparse.split(query)[0] # assuming only 1 query at a time
        # self.query_split = sqlparse.format(query, reindent=True, keyword_case="upper").splitlines()
        # self.query = self.clean(self.query_split)
        print("Inside init")
        self.query = query
        self.db = db
        self.index_column_dict = {}
        self.parser = Parser(self.query)
        self.tokens = self.parser.tokens
        self.tables = self.parser.tables
        self.get_metadata()
        self.parser_columns, self.columns = self.get_columns()
        self.column_to_table_mapping = self.get_column_to_table_mapping()
        self.not_transformations = {
            "<": ">=",
            ">": "<=",
            "=": "<>",
            "<>": "=",
            "!=": "=",
            "<=": ">",
            ">=": "<"
        }
        self.curr_query_num = {
            "query_1": {
                0: 1
            }
        }
        self.tables_aliases = self.parser.tables_aliases
        self.aggregate_list = ["count", "sum", "avg", "approx_count_distinct", "max", "min", "stdev", "stdevp", "var", "varp", "string_agg"]
        self.decomposed_query, self.query_components = self.extract_keywords(self.tokens, {"query_1": {"subqueries": {}}}, {"query_1": {"subqueries": {}}})

    def clean(self, query_split):
        reconstructed_query = ""
        for clause in query_split:
            reconstructed_query += ' {}'.format(clause.strip())
        return reconstructed_query
        
    def print_token_debug_info(self, t:token.SQLToken):
        print("Value: " + str(t.value))
        print(t.is_keyword)
        print("Different types: ")
        print("Is keyword: " + str(t.is_keyword))
        print("Is punctuation: " + str(t.is_punctuation))
        print("Is dot: " + str(t.is_dot))
        print("Is wildcard: " + str(t.is_wildcard))
        print("Is integer: " + str(t.is_integer))
        print("Is float: " + str(t.is_float))
        print("Is comment: " + str(t.is_comment))
        print("Is as keyword: " + str(t.is_as_keyword))
        print("Is left parenthesis: " + str(t.is_left_parenthesis))
        print("Is right parenthesis: " + str(t.is_right_parenthesis))
        print("Parenthesis level: " + str(t.parenthesis_level))

    def get_metadata(self):
        result = self.db.execute("select table_name from information_schema.tables where table_schema='public';")
        self.all_column_names = {}
        self.all_table_names = [res[0] for res in result]
        print(self.all_table_names)
        for table in self.all_table_names:
            result = self.db.execute("select * from information_schema.columns where table_name='{}' order by ordinal_position".format(table))
            for val in result:
                self.all_column_names[val[3]] = table
        print(self.all_column_names)
        ## Get Index columns
        index_columns = self.db.execute("SELECT tablename,indexname,indexdef FROM pg_indexes WHERE schemaname = 'public' ORDER BY tablename, indexname")
        print("----------------------RESULTS-------------------------------------")
        print(index_columns)
        for index in index_columns:
            table = index[0]
            index_name = index[1]
            create_index = index[2].split("public.",1)[1]
            mk1 = create_index.find('(') + 1
            mk2 = create_index.find(')', mk1)
            column_name = create_index[ mk1 : mk2 ]
            print("This is create_index :")
            print(column_name)
            print(table)
            print(index_name)
            self.index_column_dict[index_name] = [table,column_name]

    
    def get_relations(self):
        return self.parser.tables

    def get_columns(self):
        return self.parser.columns, [column[column.find(".") + 1:] if column.find(".") != -1 else column for column in self.parser.columns]
    
    def get_column_to_table_mapping(self):
        query_columns = {column: self.all_column_names[column] if column not in ['*', 'UNITS'] else column for column in self.columns}
        return query_columns

    def prepend_table_name_to_column(self, curr_query, t: token.SQLToken, curr_level):
        # table name already present
        if "." in t.value.lower():
            return t
        # otherwise
        if t.value.lower() in self.columns:
            column_name = t.value.lower()
            t.value = self.column_to_table_mapping[t.value.lower()]
            old_val = t.value
            print("Old value")
            t = self.replace_alias_with_expression(curr_query, t)
            if (t.value == old_val):
                if (curr_level != 0):
                    t.value += "_" + str(curr_level)
            t.value += "." + column_name
        return t

    def print_query_debug_info(self):
        for t in self.tokens:
            self.print_token_debug_info(t)

    def handle_as_keyword(self, curr_query, curr_component: list, tokens, t: token.SQLToken, i, last_keyword_token, last_comma_index):
        if t.value not in curr_query:
            curr_query[t.value.lower()] = {}
        aggregate = curr_query[last_keyword_token.value][-1]
        i += 1
        alias: token.SQLToken = tokens[i]
        curr_component.append(alias.value.lower())
        curr_query[t.value.lower()][alias.value.lower()] = aggregate
        last_comma_index = i + 1
        return curr_query, i, last_comma_index, curr_component

    # NOT IN USE
    def handle_table_name(self, curr_query, tokens, t:token.SQLToken, i, last_keyword_token:token.SQLToken, last_comma_index):
        if (t in self.tables):
            table = t
            curr_query[last_keyword_token.value.lower()].append(table.value.lower())
            # alias cases
            i += 1
            t = tokens[i]

            # case 1: explicit 'as' keyword
            if t.is_as_keyword():
                curr_query, i, last_comma_index = self.handle_as_keyword(curr_query, tokens, t, i, last_keyword_token, last_comma_index)   

    def replace_alias_with_expression(self, curr_query, t: token.SQLToken):
        if "as" not in curr_query:
            return t
        if t.value in curr_query["as"]:
            t.value = curr_query["as"][t.value.lower()]
        return t

    def extract_where_having(self, curr_query, query_components, clause_tokens, clause_list: list, last_keyword_token: token.SQLToken, query_alias, curr_level):
        i = 0
        last_index = i
        curr_clause_not = False
        contains_subquery = False
        subquery_start_index = None
        subquery_alias = None
        # query_segment = (" ".join([t.value for t in clause_tokens]))[:-1]
        raw_clause_lst = []
        raw_tok_lst = []
        raw_clause = ""
        while i < len(clause_tokens):
            t: token.SQLToken = clause_tokens[i]
            raw_tok_lst.append(t.value)
            t: token.SQLToken = self.prepend_table_name_to_column(curr_query, t, curr_level)
            t = self.replace_alias_with_expression(curr_query, t)
            # print("Value: " + str(t.value))
            if (t.value.lower() == "date"):
                i += 1
                continue
            if (t.parenthesis_level != last_keyword_token.parenthesis_level) and (t.is_keyword and t.value.lower() not in ["and", "or", "not"]):
                contains_subquery = True
                subquery_start_index = i
                # self.curr_subquery_num += 1
                # curr_query, query_components, i, subquery_alias = self.extract_subquery(curr_query, query_components, clause_tokens, i, last_keyword_token, t)
                if query_alias not in self.curr_query_num:
                    self.curr_query_num[query_alias] = {}
                if (curr_level + 1) not in self.curr_query_num[query_alias]:
                    self.curr_query_num[query_alias][curr_level + 1] = 1
                else:
                    self.curr_query_num[query_alias][curr_level + 1] += 1
                # self.curr_subquery_num += 1
                curr_query, query_components, i, subquery_alias = self.extract_subquery(curr_query, query_components, clause_tokens, i, last_keyword_token, t, self.curr_query_num[query_alias][curr_level + 1], curr_level + 1)
                i -= 1
                print("Subquery alias: " + subquery_alias)
            if t.is_keyword and (t.value.lower() == "and" or t.value.lower() == "or"):
                if not contains_subquery:
                    tok_lst = [tok.value for tok in clause_tokens[last_index: i]]
                    raw_clause += " ".join(raw_tok_lst)
                    raw_tok_lst = []
                else:
                    # print("reached this case")
                    tok_lst = [tok.value for tok in clause_tokens[last_index: subquery_start_index - 1]] + [subquery_alias]
                    raw_clause += " ".join(raw_tok_lst[:-2]) + subquery_alias
                    raw_tok_lst = []
                    contains_subquery = False
                    subquery_start_index = None
                    subquery_alias = None
                
                # print(tok_lst)
                if (curr_clause_not):
                    # print("Found NOT clause")
                    tok_lst[1] = self.not_transformations[tok_lst[1].lower()]
                    curr_clause_not = False
                # print(tok_lst)
                raw_clause_lst.append(raw_clause)
                raw_clause = ""
                clause_list.append(" ".join(tok_lst))
                last_index = i + 1
            elif (t.is_keyword and t.value.lower() == "not"):
                # raw_clause += t.value + " "
                curr_clause_not = True
                last_index = i + 1
            i += 1
        if not contains_subquery:
            tok_lst = [tok.value for tok in clause_tokens[last_index: i]]
            # print("Raw token list")
            # print(raw_tok_lst)
            raw_clause = " ".join(raw_tok_lst)
        else:
            tok_lst = [tok.value for tok in clause_tokens[last_index: subquery_start_index - 1]] + [subquery_alias]
            raw_clause = " ".join(raw_tok_lst[:-2]) + subquery_alias
            contains_subquery = False
            subquery_start_index = None
        # print(tok_lst)
        if (curr_clause_not):
            # print("Found NOT clause")
            tok_lst[1] = self.not_transformations[tok_lst[1].lower()]
            curr_clause_not = False
        # print(tok_lst)
        raw_clause_lst.append(raw_clause)
        clause_list.append(" ".join(tok_lst))
        curr_query[last_keyword_token.value.lower()] = clause_list
        # print("final raw tokens")
        # print(raw_clause_lst)
        return curr_query, raw_clause_lst

    def extract_subquery(self, curr_query, query_components, tokens, i, last_keyword_token: token.SQLToken, t: token.SQLToken, subquery_number, subquery_level):
        subquery_tokens = []
        # a ')' with same parenthesis level as parent query marks end of subquery
        while t.value != ")" or t.parenthesis_level != last_keyword_token.parenthesis_level: 
            t = tokens[i]
            subquery_tokens.append(t)
            i += 1
        # print("Subquery clause tokens: ")
        # for tok in subquery_tokens:
        #     print("Value: " + tok.value)
        # print("End of subquery tokens")
        subquery_alias = ("sub" * subquery_level) + "query_" + str(subquery_number)
        subquery_decomposed_query = {subquery_alias: {"subqueries": {}}}
        subquery_components = {subquery_alias: {"subqueries": {}}}
        subquery_tokens = subquery_tokens[:-1]
        # print(curr_query)
        # print(query_components)
        res_1, res_2 = self.extract_keywords(subquery_tokens, subquery_decomposed_query, subquery_components, t.parenthesis_level + 1, subquery_level, subquery_alias)
        curr_query["subqueries"][subquery_alias] = res_1[subquery_alias].copy()
        query_components["subqueries"][subquery_alias] = res_2[subquery_alias].copy()
        return curr_query, query_components, i, subquery_alias

    def collapse_from_last_comma(self, curr_query, last_comma_index, last_keyword_token: token.SQLToken, tokens, i, curr_level):
        if last_comma_index is not None and last_comma_index != i:
            token_lst = [self.replace_alias_with_expression(curr_query, self.prepend_table_name_to_column(curr_query, t, curr_level)).value.lower() for t in tokens[last_comma_index + 1: i]]
            curr_query[last_keyword_token.value.lower()].append("".join(token_lst))
        return curr_query, last_comma_index

    def extract_keywords(self, tokens, decomposed_query, query_components, parenthesis_level=0, curr_level=0, query_alias="query_1"):
        last_keyword_token = token.SQLToken()
        last_keyword_token.parenthesis_level = parenthesis_level
        last_comma_index = None
        i = 0
        semicolon_present = False
        curr_component = []
        curr_query = decomposed_query[query_alias]
        curr_query_components = query_components[query_alias]
        # print(decomposed_query)
        # print(query_components)
        # print(curr_query)
        # print(curr_query_components)
        set_op = False
        while i < len(tokens):
            t: token.SQLToken = tokens[i]
            curr_component.append(t.value)
            t = self.prepend_table_name_to_column(curr_query, t, curr_level)
            t = self.replace_alias_with_expression(curr_query, t)
            # print("Value: " + str(t.value))

            if (t.value.lower() == ";"):
                semicolon_present = True
                curr_query, last_comma_index = self.collapse_from_last_comma(curr_query, last_comma_index, last_keyword_token, tokens, i, curr_level)
                curr_query_components[last_keyword_token.value.lower()] = " ".join(curr_component)
                last_comma_index = i
                print("Done processing the query!")
                i += 1
                continue

            if (t.value == ","):
                curr_query, last_comma_index = self.collapse_from_last_comma(curr_query, last_comma_index, last_keyword_token, tokens, i, curr_level)
                last_comma_index = i
                
            if (t.is_keyword):
                curr_query, last_comma_index = self.collapse_from_last_comma(curr_query, last_comma_index, last_keyword_token, tokens, i, curr_level)
  
                # print(t.parenthesis_level)
                # print(last_keyword_token.parenthesis_level)

                # new keyword in same level query
                if (t.parenthesis_level == last_keyword_token.parenthesis_level):
                    
                    if (t.value.lower() == "asc" or t.value.lower() == 'desc'):
                        # print("Reached desc/asc")
                        curr_query["order by"][-1] += " " + t.value.lower()
                        if (last_keyword_token is not None):
                            curr_query_components[last_keyword_token.value.lower()] = " ".join(curr_component[:-1])
                        # print(curr_query)
                        i += 1
                        continue

                    if (t.value.lower() == "as"):
                        curr_query, i, last_comma_index, curr_component = self.handle_as_keyword(curr_query, curr_component, tokens, t, i, last_keyword_token, last_comma_index)
                        i += 1
                        continue

                    curr_query[t.value.lower()] = []
                    curr_query_components[t.value.lower()] = []

                    if (last_keyword_token is not None):
                        curr_query_components[last_keyword_token.value.lower()] = " ".join(curr_component[:-1])

                    curr_component = curr_component[-1:]
                    last_keyword_token = t
                    last_comma_index = i

                    
                    if (t.value.lower() == "where" or t.value.lower() == "having"):
                        clause = t.value.lower()
                        where_tokens = []
                        i += 1
                        t = tokens[i]
                        # a new keyword besides "and", "or", "not" or "date" marks the end of a group of where clauses
                        while ((not t.is_keyword) or (t.parenthesis_level > last_keyword_token.parenthesis_level) or (t.value.lower() in ["and", "or", "not", "date"])):
                            where_tokens.append(t)
                            i += 1
                            if i < len(tokens):
                                t = tokens[i]
                            else:
                                break
                        i -= 1
                        # print(clause + " clause tokens: ")
                        # for tok in where_tokens:
                        #     print("Value: " + str(tok.value))
                        # print("End of " + clause + " tokens")
                        curr_query, clause_list = self.extract_where_having(curr_query, curr_query_components, where_tokens, [], last_keyword_token, query_alias, curr_level)
                        curr_component.extend(clause_list)
                        last_comma_index = i + 1                                                

                    if (t.value.lower() == "union" or t.value.lower() == "intersect" or t.value.lower() == "minus"):
                        i += 1
                        set_op = t.value.lower()
                        t = tokens[i]
                        self.curr_query_num[query_alias][curr_level] += 1
                        next_query_alias = query_alias.split("_")[0] + "_" + str(self.curr_query_num[query_alias][curr_level])
                        decomposed_query[next_query_alias] = {"subqueries": {}}
                        query_components[next_query_alias] = {"subqueries": {}}
                        decomposed_query, query_components = self.extract_keywords(tokens[i:], decomposed_query, query_components, parenthesis_level, curr_level, next_query_alias)
                        curr_query[set_op] = next_query_alias
                        curr_query_components[set_op] = set_op + " " + next_query_alias
                        # print("finished recursive handling")
                        # print(decomposed_query)
                        # print(query_components)
                        break

                # subquery
                else:
                    # print("Reached subquery case")
                    if query_alias not in self.curr_query_num:
                        self.curr_query_num[query_alias] = {}
                    if (curr_level + 1) not in self.curr_query_num[query_alias]:
                        self.curr_query_num[query_alias][curr_level + 1] = 1
                    else:
                        self.curr_query_num[query_alias][curr_level + 1] += 1
                    # self.curr_subquery_num += 1
                    curr_query, curr_query_components, i, subquery_alias = self.extract_subquery(curr_query, curr_query_components, tokens, i, last_keyword_token, t, self.curr_query_num[query_alias][curr_level + 1], curr_level + 1)

            # reached a table alias; then add it to as mapping and remove
            if (t.value in self.tables_aliases):
                # print("reached table alias")
                prev_token: token.SQLToken = tokens[i - 1]
                if (prev_token.is_as_keyword):
                    prev_token = tokens[i - 2]
                
                if "as" not in curr_query:
                    curr_query["as"] = {}
                curr_query["as"][prev_token.value.lower()] = t.value.lower()
                tokens.pop(i)
                continue
            
            # region
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
            #     curr_query[last_keyword_token.value].append(agg)
            # endregion
            
            # region
            # default
            # elif (not t.is_comment and not t.is_punctuation):
            #     print(last_keyword_token.value)
            #     print(curr_query)
            #     curr_query[last_keyword_token.value].append(t.value)
            # endregion

            i += 1

        if (not semicolon_present and not set_op):
            curr_query, last_comma_index = self.collapse_from_last_comma(curr_query, last_comma_index, last_keyword_token, tokens, i, curr_level)
            last_comma_index = i
            if (last_keyword_token is not None):
                curr_query_components[last_keyword_token.value.lower()] = " ".join(curr_component)

        if ("order by" in curr_query):
            if (curr_query["order by"][-1] not in ["asc", "desc"]):
                curr_query["order by"][-1] += " asc"

        if '' in curr_query_components:
            del curr_query_components['']

        decomposed_query[query_alias] = curr_query
        query_components[query_alias] = curr_query_components

        return decomposed_query, query_components


if __name__ == "__main__":
    complex_query = "select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price,sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge from lineitem where l_shipdate <= date '1998-12-01' group by sum_disc_price, l_linestatus order by sum_disc_price, l_linestatus"
    long_query = """
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
    sql_query = "SELECT * FROM customer C, orders O WHERE C.c_custkey = O.o_custkey"
    primary_key_query = "select * from nation where nation.n_nationkey = 3;"
    set_query = "select n_nationkey from nation union select s_nationkey from supplier"
    q20 = """
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
                        AND l_shipdate >= date 1-1-1994
                        AND l_shipdate < date 1-1-1994 + '1 year'
                )
        )
        AND s_nationkey = n_nationkey
        AND n_name = 'CANADA'
    ORDER BY
        s_name
        """
    where_subquery = """
    select * from part where p_brand = 'Brand#13' and p_size <> (select max(p_size) from part);
    """

    db = DB()

    preprocessor = PreProcessor(set_query, db)
    # preprocessor.print_query_debug_info()
    print(preprocessor.tables)
    print(preprocessor.columns)
    print(preprocessor.decomposed_query)
    print(preprocessor.query_components)
    print(preprocessor.parser.columns_aliases_dict)
    print(preprocessor.parser.columns_aliases_names)
    print(preprocessor.parser.columns_aliases)
    print(preprocessor.parser.tables_aliases)
    print(preprocessor.tables)
    # print(preprocessor.query)

    db.close()


# from decomposed query has aliases due to the table name to alias mapping in as dictionary and working collapse form last comma. 