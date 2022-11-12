from node import Node

def hash_define(node : Node, condition,index_column_dict):
   result = "The clause " + condition + " retrieve rows and hash them into their respective buckets which is stored into an in-memory Hash table. "
   return result