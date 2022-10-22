from __future__ import annotations

class Node():
    def __init__(self, data) -> None:
        self.plan = data
        self.children = [] # children in order
        self.result_table_name = None
    
    def add_child(self, obj: Node) -> None:
        self.children.append(obj)

    def print_tree(self) -> None:
        level = 1
        q = [(self, level)]
        prev_level = -1
        while len(q) != 0:
            cursor, level = q.pop(0)
            if level != prev_level:
                print("Level " + str(level))
                prev_level = level
            print(cursor.plan)
            q.extend([(child, level + 1) for child in cursor.children])
            
    