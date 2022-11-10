from result_parser.node_types.aggregate import aggregate_define
from result_parser.node_types.nested_loop import nested_loop_define
from result_parser.node_types.seq_scan import seq_scan_define
from result_parser.node_types.hash import hash_define
from result_parser.node_types.hash_join import hash_join_define
from result_parser.node_types.index_scan import index_scan_define, index_only_scan_define
from result_parser.node_types.materialize import materialize_define
from result_parser.node_types.bitmap_heap_scan import bitmap_heap_scan_define
from result_parser.node_types.bitmap_index_scan import bitmap_index_scan_define
from result_parser.node_types.merge_join import merge_join_define
#
class ResultParser(object):
    results_map = {
        "Aggregate": aggregate_define,
        "Nested Loop": nested_loop_define,
        "Seq Scan": seq_scan_define,
        "Hash": hash_define,
        "Hash Join": hash_join_define,
        "Index Scan": index_scan_define,
        "Index Only Scan": index_only_scan_define,
        "Materialize": materialize_define,
        "Bitmap Index Scan" : bitmap_index_scan_define,
        "Bitmap Heap Scan" : bitmap_heap_scan_define,
        "Merge Join": merge_join_define,
        #"external merge Sort"

    }


if __name__ == "__main__":
    print(ResultParser.results_map)