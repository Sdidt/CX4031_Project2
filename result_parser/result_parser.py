from result_parser.node_types.aggregate import aggregate_define
from result_parser.node_types.nested_loop import nested_loop_define
from result_parser.node_types.seq_scan import seq_scan_define
from result_parser.node_types.hash import hash_define
from result_parser.node_types.hash_join import hash_join_define
from result_parser.node_types.index_scan import index_scan_define
from result_parser.node_types.materialize import materialize_define
#
class ResultParser(object):
    results_map = {
        "Aggregate": aggregate_define,
        "Nested Loop": nested_loop_define,
        "Seq Scan": seq_scan_define,
        "Hash": hash_define,
        "Hash Join": hash_join_define,
        "Index Scan": index_scan_define,
        "Index Only Scan": index_scan_define,
        "Materialize": materialize_define,
       # "Gather Merge": materialize_define,   
       # "Merge Join": merge_join_define,

    }


if __name__ == "__main__":
    print(ResultParser.results_map)