from __future__ import annotations

class Node():

    def __init__(self, data: dict, subquery_level) -> None:

        self.type = data.get("Node Type")
        self.subquery_level = subquery_level
        self.parent: list[Node] = []
        self.children = []
        self.information = None
        self.keywords = None
        self.scan_filter = ""
        self.join_filters = []
        self.index_name = None

        d = data.copy()
        d.pop("Node Type")

        if "Plans" in data:
            d.pop("Plans")

        self.information = d
        if (self.type == "Sort"):
            self.type = self.information["Sort Method"] + " Sort"

    def add_child(self, child: Node) -> None:
        self.children.append(child)

    def add_parent(self, parent: Node) -> None:
        self.parent.append(parent)

    def get_estimated_cost(self):
        # startup_cost = self.information["Startup Cost"]
        qep_cost = self.information["Total Cost"] * self.information["Actual Loops"]
        if self.type in ["Merge Join", "Nested Loop", "Index Nested Loop", "Bitmap Heap Scan"]:
            return qep_cost
        children: list[Node] = self.children
        total_cost_of_child = 0
        for child in children:
            total_cost_of_child += child.information["Total Cost"] * child.information["Actual Loops"]

        diff = qep_cost - total_cost_of_child
        if diff < 0:
            return qep_cost
        return diff

    def mapping(self):

        self.keywords = {}

        if self.type == "Seq Scan":
            self.keywords = self.node_seq_scan()
        elif self.type == "Index Scan":
            self.keywords = self.node_index_scan()
        elif self.type == "Bitmap Index Scan":
            self.keywords = self.node_bitmap_index_scan()
        elif self.type == "Bitmap Heap Scan":
            self.keywords = self.node_bitmap_heap_scan()
        elif self.type == "Hash":
            self.keywords = self.node_hash()
        elif self.type == "Hash Join":
            self.keywords = self.node_hash_join()
        elif self.type == "Materialize":
            pass
        elif self.type == "Nested Loop":
            self.keywords = self.node_nested_loop()
        elif self.type == "Merge Join":
            self.keywords = self.node_merge_join()
        elif self.type == "Aggregate":
            self.keywords = self.node_aggregate()
        elif "Sort" in self.type:
            self.keywords = self.node_sort()
        elif self.type == "Gather Merge":
            pass
        else:
            pass
    
        # converting dictionary to a list of dictionaries
        lst = [{"level": self.subquery_level}]
        for key,value in self.keywords.items():
            dct = {}
            dct[key] = value
            lst.append(dct)
        return lst

    def revert_condition(self, condition: str):
        return " ".join(condition.split(" ")[::-1])

    def remove_punctuations(self, condition):
        split_condition = condition.split("::", 1)
        if len(split_condition) == 1:
            split_condition = split_condition[0][1:-1]
        else:
            split_condition = split_condition[0][1:]
        return split_condition

    def node_aggregate(self):
        relevant_info = {}
        if 'Group Key' in self.information:
            group = self.information["Group Key"]
            relevant_info["group by"] = group
        return relevant_info

    # complete: BUT need to handle case when sort is used before sort merge join
    def node_sort(self):
        relevant_info = {}
        order = "desc" if self.information["Sort Key"][0][-4:] == "desc" else "asc"
        key = self.information["Sort Key"][0] if order == "asc" else self.information["Sort Key"][0][:-5]
        key = self.remove_punctuations(key)
        relevant_info["order by"] = key + " " + order
    
        return relevant_info
    
    # complete
    def node_seq_scan(self):
        relevant_info = {}
        relation = self.information.get("Alias")
        relevant_info["from"] = relation

        if "Filter" in self.information:
            filter = self.information.get("Filter")

            if "= any" not in filter:
                filter = self.remove_punctuations(filter)                
                relevant_info["where"] = filter
                self.scan_filter = filter
            else:
                filter1 = filter.split(" = any", 1)[0][1:]
                filter2 = filter.split("any (", 1)[1].split("::", 1)[0]
                relevant_info["where"] = filter1
                relevant_info["in"] = filter2

        return relevant_info

    # complete
    def node_index_scan(self):
        relevant_info = {}
        relation = self.information.get("Alias")
        relevant_info["from"] = relation

        if "Index Cond" in self.information:
            index_cond = self.information.get("Index Cond")

            if "= any" not in index_cond:
                index_cond = self.remove_punctuations(index_cond)                
                relevant_info["where"] = index_cond
                self.scan_filter = index_cond
            else:
                index_cond1 = index_cond.split(" = any", 1)[0][1:]
                index_cond2 = index_cond.split("any (", 1)[1].split("::", 1)[0]
                relevant_info["where"] = index_cond1
                relevant_info["in"] = index_cond2


        return relevant_info

    def node_bitmap_index_scan(self):
        relevant_info = {}
    
        if "Index Cond" in self.information:
            index_cond = self.information.get("Index Cond")

            if "= any" not in index_cond:
                index_cond = self.remove_punctuations(index_cond)                
                relevant_info["where"] = index_cond
                self.scan_filter = index_cond
            else:
                index_cond1 = index_cond.split(" = any", 1)[0][1:]
                index_cond2 = index_cond.split("any (", 1)[1].split("::", 1)[0]
                relevant_info["where"] = index_cond1
                relevant_info["in"] = index_cond2


        return relevant_info

    def node_bitmap_heap_scan(self):
        relevant_info = {}
        relation = self.information.get("Alias")
        relevant_info["from"] = relation


        return relevant_info

    # complete
    def node_hash(self):
        relevant_info = {}
        return relevant_info

    # complete
    def node_hash_join(self):
        relevant_info = {}
        if 'Hash Cond' in self.information:
            condition = self.information['Hash Cond'][1:-1]
            keywords = ["where", "in"]
            for keyword in keywords:
                relevant_info[keyword] = condition

            self.join_filters = [condition, self.revert_condition(condition)]
        return relevant_info
    
    def node_nested_loop(self):
        relevant_info = {}
        # print(self.information)
        if 'Join Filter' in self.information:
            condition = self.information['Join Filter'][1:-1]
            keywords = ["where", "in"]
            for keyword in keywords:
                relevant_info[keyword] = condition

            self.join_filters = [condition, self.revert_condition(condition)]
        else:
            self.type = "Index Nested Loop"
            self.join_filters, self.index_name = self.trace_for_join()
            print("Extraced join filter: {}".format(self.join_filters))

        return relevant_info

    def node_merge_join(self):
        relevant_info = {}
        # print(self.information)
        if 'Merge Cond' in self.information:
            condition = self.information['Merge Cond'][1:-1]
            keywords = ["where", "in"]
            for keyword in keywords:
                relevant_info[keyword] = condition

            self.join_filters = condition
            print("Extraced merge condition: {}".format(self.join_filters))
        return relevant_info


    def trace_for_join(self):
        print("BEGIN TRACE")
        filter = ""
        # queue:list[Node] = self.create_queue_for_des()
        queue = [self]
        while(len(queue) != 0):
            node = queue[0]
            children = node.children
            queue.extend(children)
            # node.print_debug_info()
            if "Index Cond" in node.information:
                filter:str = node.information["Index Cond"][1:-1]
                reverted_filter = self.revert_condition(filter)
                index_name = node.information["Index Name"]
                return [filter, reverted_filter], index_name
            queue.pop(0)
        return filter, None

    def print_debug_info(self):
        print("NODE TYPE: {}".format(self.type))
        print("ESTIMATED COST: {}".format(self.get_estimated_cost()))
        # print("MAPPING: {}".format(self.mapping()))
        # print("RELEVANT INFORMATION: \n{}".format(self.get_relevant_info()))
        print("OTHER INFORMATION: {}".format(self.information))

# NOTE: check old_code/postorder_for_testing.py to quickly test out changes in this file