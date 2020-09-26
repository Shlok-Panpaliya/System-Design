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
            for node in nodes:
                self.add_node(node)
     #   print(self.ring)
      #  print(self.sorted_keys)
    def add_node(self,node):
        # Adds the node to the hash ring with given number of replicas
        for i in range(self.replica):
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
        return (m.hexdigest(),16) 

    def get_node(self,string_key):
        #It returns a corresponding node(server) to the string_key

        return self.get_node_pos(string_key)

    def get_node_pos(self,string_key):

        #It returns corresponding server in the hash ring to the given request along with its position in the ring

        if not self.ring:
            return None,None
        key = self.gen_key(string_key)
        nodes = self.sorted_keys

        for i in range(len(nodes)):
            if key <= nodes[i]:       #<= because if a server is removed it will find nearest available server
                return self.ring[nodes[i]],i  #here nodes[i] is the server and i is its position
        return self.ring(nodes[0]),0

    def remove_node(self,node):
        #Removes the server and its replicas from the hash ring
        for i in range(self.replicas):

            key = self.gen_key('%s:%s' % (node, i))
            del self.ring[key]
            self.sorted_keys.remove(key)

if __name__ == "__main__":
    memcache_servers = ['192.168.0.246:11212',
                    '192.168.0.247:11212',
                    '192.168.0.249:11212']
    ring = HashRing(memcache_servers)
   # print(sorted_keys)
    s = input("Enter Request id")
    server = ring.get_node(s)
    print("server alloted",server[0],"is at position ",server[1])
