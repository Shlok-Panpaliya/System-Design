from hashlib import md5

class HashRing(object):
    def __init__(self,nodes = None, replica = 3):
    #initialises a hash ring 
    #nodes represent list of object(mem_chache server) that have string representation
    #repicas represent how many virtual server should be there for even distribution

    self.replica = replica
    self.ring = dict()
    self.sorted_keys = []
    if nodes:
        for nodes in nodes:
            self.add_node(node)

    def add_node(self,node):
        # Adds the node to the hash ring with given number of replicas
        for i in range(self.replicas):
            key = self.gen_key('%s:%s' % (node, i))
            self.ring[key] = node
            self.sorted_keys.append(key)
        self.sorted_keys.sort()

    def gen_key(self,key):
        #It returns a hashed value of the given node(server) and which represents its position on the hash ring
        #md5 is used to get the hash value
        m = md5()
        key = key.encode('utf-16')
        m.update(key)
        return m.hexdigest(key,16) 