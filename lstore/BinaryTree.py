class Node(object):
    def __init__(self, value):
        self.parent = None
        self.left = None
        self.right = None
        self.value = value
        self.keys = []


class Tree(object):
    def __init__(self):
        """Constructs (or initializes) the root for class Tree"""
        self.root = None

    def print(self):
        """Print the data of all nodes in order"""
        self.__print(self.root)


    def __print(self, curr_node):
        """Recursively print a subtree (in order), rooted at curr_node
        

        Parameters
        ----------
        curr_node : object of class Node
            node to be recursively printed
        """
        if curr_node is not None:
            self.__print(curr_node.left)
            print(str(curr_node.data), end=' ')  # save space
            self.__print(curr_node.right)
            

    def insert(self, data, key = None):
        """Insert a node into the tree.
        

        Parameters
        ----------
        data : int or float
            data to be turned into a node and put in the tree
        
        key : int
            The RID of the data being put into the tree
        """
        data_node = Node(data)

        if self.root == None:
            self.root = data_node
            return
        
        node = self.root
        while (True):
            if data > node.data:
                if node.right == None:
                    node.right = data_node
                    data_node.parent = node
                    return
                node = node.right
                
            elif data < node.data:
                if node.left == None:
                    node.left = data_node
                    data_node.parent = node
                    return
                node = node.left
            elif data == node.data and key != None:
                node.keys.append(key)
                return

    def min(self):
        """Returns the minimum value held in the tree"""
        if self.root == None:
            return None
        
        node = self.root
        while(node.left != None):
            node = node.left

        return node.data

    def max(self):
        """Returns the maximum value held in the tree"""
        if self.root == None:
            return None
        
        node = self.root
        while(node.right != None):
            node = node.right

        return node.data

    def find_node(self, data, key = None):
        """Returns the node with data value else returns None.
       
        
        Parameters
        ----------
        data : int or float
            data to be found within the class Tree object to return
            the node containing data
        """
        if self.root == None:
            return None
        
        node = self.root
        while (data != node.data):

            if data < node.data:
                node = node.left
            elif data > node.data:
                node = node.right

            if node == None:
                return None
        return node

    def find_node_range(self, dataRange1, dataRange2):
        all_nodes = [self.find_node(dataRange1)]
        successor = self.find_successor(dataRange1)
        while successor < dataRange2 or successor == dataRange2 or successor == None:
            all_nodes.append(successor)
            successor = self.find_successor(successor)
        if successor == dataRange2:
            all_nodes.append(successor)
        return all_nodes
  
    def contains(self, data):
        """Return True if node containing data is present in the tree.


        Parameters
        ----------
        data : int or float
            data to be found within the Tree class object
        """
        if self.find_node(data) == None:
            return False
        return True

    def find_successor(self, data):
        """Finds the successor node and Return object of 
        successor if found else return None
        
        
        Parameters
        ----------
        data : int or float
            data of node that will be used to find its successor

        Raises
        ------
        
        Node is not in the tree = None
        """
        if self.contains(data) == False:
            return None
        node = self.find_node(data)

        if node.right != None:
            node = node.right
            while node.left != None:
                node = node.left
            return node
        
        if node.parent == None:
            return None
        
        parent_node = node.parent
        while parent_node != None and node == parent_node.right:
            
            node = parent_node
            parent_node = parent_node.parent
        
        return parent_node
    
    def transplant(self, node_x, node_y):
        """Replaces subtree rooted at node_x and replaces it with
        the subtree rooted at node_y

        Parameters
        ----------
        node_x : object of class Node
            Node to be replaced by node_y
        node_y : object of class Node
            Node to replace node_x
        """
        if node_x.parent == None:
            self.root = node_y
        elif node_x == node_x.parent.left:
            node_x.parent.left = node_y
        else:
            node_x.parent.right = node_y
        
        if node_y != None:
            node_y.parent = node_x.parent

    def delete(self, data, del_key):
        """Finds the successor node and Return object of 
        successor if found else return None
        
        
        Parameters
        ----------
        data : int or float
            data of node to be deleted
        
        Raises
        ------
        
        Node is not in the tree = none
        """
        if self.contains(data) == False:
            return None
        
        node = self.find_node(data)

        if len(node.keys) == 1 and del_key == node.keys[0]:
            if node.left == None:
                self.transplant(node, node.right)
            elif node.right == None:
                self.transplant(node, node.left)
            else:
                successor = self.find_successor(node.data)
                if successor != node.right:
                    self.transplant(successor, successor.right)
                    successor.right = node.right
                    successor.right.parent = successor
                self.transplant(node, successor)
                successor.left = node.left
                successor.left.parent = successor
        else:
            node.keys.remove(del_key)
        return None
