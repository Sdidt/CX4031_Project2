from node import Node

def hash_define(node : Node, condition,index_column_dict):
   """    
    Returns the natural langauge description of the Hash Node Types
    
    Paramemter node: the Node object.

    Paramemter condition: The specific SQL Query clause.

    Parameter index_column_dict: The dictionary that contains the index names and their respecitve tables and table columns.  
   
   """

   result = "The clause " + condition + " retrieve rows and hash them into their respective buckets which is stored into an in-memory Hash table. "
  
  
   return result