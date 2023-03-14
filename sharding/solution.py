from dslib import Context, Message, Node
from typing import List
import hashlib
import bisect

total_number = 100000000000000000


def get_hash(s):
    h = hashlib.sha256(s.encode("utf-8"))
    int_h = int(h.hexdigest(), 16)
    return int_h % total_number

def find_next(key, nodes):
    ind = bisect.bisect_left(nodes, key)
    if ind == len(nodes):
        return nodes[0]
    return nodes[ind]

def get_node_hashes(node):
    res = []
    for i in range(500):
        s = str(i) + node
        h = hashlib.sha256(s.encode("utf-8"))
        int_h = int(h.hexdigest(), 16)
        res.append(int_h % total_number)
    return res

def get_tables(nodes):
    hashes_to_nodes = {}
    hashes = []
    for n in nodes:
        for h in get_node_hashes(n):
            hashes_to_nodes[h] = n
            hashes.append(h)
    hashes = sorted(hashes)
    return hashes, hashes_to_nodes




class StorageNode(Node):
    def __init__(self, node_id: str, nodes: List[str]):
        self._id = node_id
        self._nodes = set(nodes)
        self._data = {}

        
    def on_local_message(self, msg: Message, ctx: Context):
        # Get key value.
        # Request:
        #   GET {"key": "some key"}
        # Response:
        #   GET_RESP {"key": "some key", "value": "value for this key"}
        #   GET_RESP {"key": "some key", "value": null} - if record for this key is not found
        if msg.type == 'GET':
            key = msg['key']
            hashes, hashes_to_nodes = get_tables(self._nodes)
            hashed_key = get_hash(key)
            hashed_node = find_next(hashed_key, hashes)
            node = hashes_to_nodes[hashed_node]
            if node == self._id:
                value = self._data.get(key)
                resp = Message('GET_RESP', {
                    'key': key,
                    'value': value
                })
                ctx.send_local(resp)
            else:
                get_msg = Message('GET', {
                'key': key,
                'sender_id': self._id
                })
                ctx.send(get_msg, node)


        # Store (key, value) record
        # Request:
        #   PUT {"key": "some key", "value: "some value"}
        # Response:
        #   PUT_RESP {"key": "some key", "value: "some value"}
        elif msg.type == 'PUT':
            key = msg['key']
            value = msg['value']
            hashes, hashes_to_nodes = get_tables(self._nodes)
            hashed_key = get_hash(key)
            hashed_node = find_next(hashed_key, hashes)
            node = hashes_to_nodes[hashed_node]
            if node == self._id:
                self._data[key] = value
                resp = Message('PUT_RESP', {
                'key': key,
                'value': value
                })
                ctx.send_local(resp)
            else:
                put_msg = Message('PUT', {
                'key': key,
                'value': value,
                'sender_id': self._id
                })
                ctx.send(put_msg, node)

        # Delete value for the key
        # Request:
        #   DELETE {"key": "some key"}
        # Response:
        #   DELETE_RESP {"key": "some key", "value": "some value"}
        elif msg.type == 'DELETE':
            key = msg['key']
            hashed_key = get_hash(key)
            hashes, hashes_to_nodes = get_tables(self._nodes)
            hashed_node = find_next(hashed_key, hashes)
            node = hashes_to_nodes[hashed_node]
            if node == self._id:
                value = self._data.pop(key, None)
                resp = Message('DELETE_RESP', {
                    'key': key,
                    'value': value
                })
                ctx.send_local(resp)
            else:
                delete_msg = Message('DELETE', {
                'key': key,
                'sender_id': self._id
                })
                ctx.send(delete_msg, node)

        # Notification that a new node is added to the system.
        # Request:
        #   NODE_ADDED {"id": "node id"}
        # Response:
        #   N/A
        elif msg.type == 'NODE_ADDED':
            hashes, hashes_to_nodes = get_tables(self._nodes)
            self._nodes.add(msg['id'])
            node_hashes = get_node_hashes(msg['id'])
            next_nodes = [hashes_to_nodes[find_next(n, hashes)] for n in node_hashes]
            for n in node_hashes:
                hashes_to_nodes[n] = msg['id']
            hashes += node_hashes
            hashes.sort()
            for next_node in next_nodes:
                if next_node == self._id:
                    needed = 0
                    remove = {}
                    for key, value in self._data.items():
                        hashed_key = get_hash(key)
                        next_for_key = hashes_to_nodes[find_next(hashed_key, hashes)]
                        if next_for_key == msg['id']:
                            needed = 1
                            remove[key] = value
                    
                    put_msg = Message('NODE_ADDED', {
                    'keys': remove
                    })
                    if needed:
                        ctx.send(put_msg, next_for_key)
                    
                    for key in remove:
                        self._data.pop(key, None)
                
    

        # Notification that a node is removed from the system.
        # Request:
        #   NODE_REMOVED {"id": "node id"}
        # Response:
        #   N/A
        elif msg.type == 'NODE_REMOVED':
            hashes, hashes_to_nodes = get_tables(self._nodes)
            self._nodes.remove(msg['id'])
            node_hashes = get_node_hashes(msg['id'])
            next_nodes = [hashes_to_nodes[find_next(n, hashes)] for n in node_hashes]
            for hashed_node in node_hashes:
                hashes.remove(hashed_node)
                hashes_to_nodes.pop(hashed_node, None)
            hashes.sort()
            if msg['id'] == self._id:
                needed = 0
                send = {}
                for key, value in self._data.items():
                    needed = 1
                    hashed_key = get_hash(key)
                    hashes, hashes_to_nodes = get_tables(self._nodes)
                    next_node = hashes_to_nodes[find_next(hashed_key, hashes)]
                    if next_node not in send:
                        send[next_node] = {}
                    send[next_node][key] = value
                for node in send:
                    put_msg = Message('NODE_ADDED', {
                    'keys': send[node]
                    })
                    ctx.send(put_msg, next_node)
                self._data = {}


        # Get number of records stored on the node
        # Request:
        #   COUNT_RECORDS {}
        # Response:
        #   COUNT_RECORDS_RESP {"count": 100}
        elif msg.type == 'COUNT_RECORDS':
            resp = Message('COUNT_RECORDS_RESP', {
                'count': len(self._data)
            })
            ctx.send_local(resp)

        # Get keys of records stored on the node
        # Request:
        #   DUMP_KEYS {}
        # Response:
        #   DUMP_KEYS_RESP {"keys": ["key1", "key2", ...]}
        elif msg.type == 'DUMP_KEYS':
            resp = Message('DUMP_KEYS_RESP', {
                'keys': list(self._data.keys())
            })
            ctx.send_local(resp)

    def on_message(self, msg: Message, sender: str, ctx: Context):
        if msg.type == 'GET':
            key = msg['key']
            value = self._data.get(key)
            ans_msg = Message('GET_RESP', {
                'key': key,
                'value': value
            })
            ctx.send(ans_msg, msg['sender_id'])

        if msg.type == 'GET_RESP':
            key = msg['key']
            value = msg['value']
            resp = Message('GET_RESP', {
                'key': key,
                'value': value
            })
            ctx.send_local(resp)
        
        if msg.type == 'PUT':
            key = msg['key']
            value = msg['value']
            hashes, hashes_to_nodes = get_tables(self._nodes)
            hashed_key = get_hash(key)
            next_for_key = hashes_to_nodes[find_next(hashed_key, hashes)]
            if next_for_key == self._id:
                self._data[key] = value
                ans_msg = Message('PUT_RESP', {
                'key': key,
                'value': value,
                'sender_id': self._id
                })
                ctx.send(ans_msg, msg['sender_id'])
            else:
                put_msg = Message('PUT', {
                'key': key,
                'value': value,
                'sender_id': msg['sender_id']
                })
                ctx.send(put_msg, next_for_key)

        
        if msg.type == 'DELETE':
            key = msg['key']
            value = self._data.pop(key, None)
            ans_msg = Message('DELETE_RESP', {
                'key': key,
                'value': value
            })
            ctx.send(ans_msg, msg['sender_id'])
        
        if msg.type == 'DELETE_RESP':
            key = msg['key']
            value = msg['value']
            resp = Message('DELETE_RESP', {
                'key': key,
                'value': value
            })
            ctx.send_local(resp)
        
        if msg.type == 'NODE_ADDED':
            add = msg['keys']
            hashes, hashes_to_nodes = get_tables(self._nodes)
            needed = 0
            reroute = {}
            next_node = 0
            for key, value in add.items():
                hashed_key = get_hash(key)
                next_for_key = hashes_to_nodes[find_next(hashed_key, hashes)]
                if next_for_key == self._id:
                    self._data[key] = value
                else:
                    needed = 1
                    next_node = next_for_key
                    if next_node not in reroute:
                        reroute[next_node] = {}
                    reroute[next_node][key] = value
            if needed:
                for node in reroute:
                    put_msg = Message('NODE_ADDED', {
                    'keys': reroute[node]
                    })
                    ctx.send(put_msg, node)
 
        
        if msg.type == 'PUT_RESP':
            key = msg['key']
            value = msg['value']
            resp = Message('PUT_RESP', {
            'key': key,
            'value': value
            })
            ctx.send_local(resp)
 
        
        if msg.type == 'ADD':
            key = msg['key']
            value = msg['value']
            hashes, hashes_to_nodes = get_tables(self._nodes)
            hashed_key = get_hash(key)
            next_for_key = hashes_to_nodes[find_next(hashed_key, hashes)]
            if next_for_key == self._id:
                self._data[key] = value
            else:
                put_msg = Message('ADD', {
                'key': key,
                'value': value
                })
                ctx.send(put_msg, next_for_key)
        




    def on_timer(self, timer_name: str, ctx: Context):
        pass
