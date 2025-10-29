class Node:
    def __init__(self, value, key):
        #Value is the number while key is the RID
        self.value = value
        self.key = key
        self.left = None
        self.right = None
        self.parent = None
        self.next = None
        


class Binary_Tree:
    def __init__(self, Node_List, root):
        self.Node_List = Node_List
        self.root = root
    
    def sort_node_list(self):
        self.Node_List.sort()

    def insert_Node(self, value, key):
        new_node = Node(value=value, key=key)
        self.Node_List.append(new_node)
        if self.root == None:
            self.root = [new_node]
            return
        
        curr_nodes = self.root
        while (True):

            if value > curr_nodes[0].value:
                if curr_nodes.right == None:
                    curr_nodes.right = [new_node]
                    new_node.parent = curr_nodes
                    return
                curr_nodes = curr_nodes.right
                
            elif value < curr_nodes[0].value:
                if curr_nodes.left == None:
                    curr_nodes.left = [new_node]
                    new_node.parent = curr_nodes
                    return
                curr_nodes = curr_nodes.left

            elif value == curr_nodes[0].value:
                curr_nodes.append[new_node]
                return

    def search(self, value, key):
        if self.root == None:
            return None
        
        nodes = self.root
        while (value != nodes[0].data):

            if value < nodes[0].data:
                nodes = nodes.left
            elif value > nodes[0].data:
                nodes = nodes.right
            if nodes == None:
                return None
        return nodes