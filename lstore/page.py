"""
The Page class provides low-level physical storage capabilities. In the provided skeleton, each
page has a fixed size of 4096 KB. This should provide optimal performance when persisting to
disk, as most hard drives have blocks of the same size. You can experiment with different sizes.
This class is mostly used internally by the Table class to store and retrieve records. While working
with this class, keep in mind that tail and base pages should be identical from the hardware’s point
of view. The config.py file is meant to act as centralized storage for all the configuration options
and the constant values used in the code. It is good practice to organize such information into a
Singleton object accessible from every file in the project. This class will find more use when
implementing persistence in the next milestone
"""
class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        if self.num_records >= 4096:
            return False
        else:
            return True

    def write(self, value):
        self.num_records += 1
        #TODO probably wrong needs to figure out how to lay things out
        if self.has_capacity():
            self.data[self.num_records] = value
        else:
            print("Page is full")