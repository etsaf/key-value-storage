import hashlib
from dslib import Context, Message, Node
from typing import List


class PendingRequest:
    quorum = 0
    asked_last = 0
    asked_total = 0
    unresponsive = []
    key = ''
    value = None
    num_resp = 0
    time = -1

def cmp(curr_value, value):
    if not curr_value:
        return value
    if not value:
        return curr_value
    if curr_value[1] > value[1]:
        return curr_value
    if curr_value[1] < value[1]:
        return value
    if not curr_value[0]:
        return value
    if not value[0]:
        return curr_value
    if curr_value[0] > value[0]:
        return curr_value
    return value



class StorageNode(Node):
    def __init__(self, node_id: str, nodes: List[str]):
        self._id = node_id
        self._nodes = nodes
        self._data = {}
        self._handed_data = {}
        self._get_number = 0
        self._put_number = 0
        self._delete_number = 0
        self._pending_get = {}
        self._pending_put = {}
        self._pending_delete = {}
        self._handed_number = 0
        self._handed_values = {}


    def on_local_message(self, msg: Message, ctx: Context):
        # Get key value.
        # Request:
        #   GET {"key": "some key", "quorum": 1-3}
        # Response:
        #   GET_RESP {"key": "some key", "value": "value for this key"}
        #   GET_RESP {"key": "some key", "value": null} - if record for this key is not found
        if msg.type == 'GET':
            
            self._get_number += 1
            req_num = str(self._get_number) + str(ctx.time())
            curr_req = PendingRequest()
            key = msg['key']
            quorum = msg['quorum']
            curr_req.quorum = quorum
            curr_req.key = key
            replicas = get_key_replicas(key, len(self._nodes))
            curr_req.asked_last = replicas[2]
            curr_req.asked_total = 3
            self._pending_get[req_num] = curr_req
            value = []
            for node in replicas:
                if node == self._id:
                    value = self._data.get(key)
                    self._pending_get[req_num].value = value
                    self._pending_get[req_num].num_resp += 1
                else:
                    get_msg = Message('GET', {
                        'key': key,
                        'sender': self._id,
                        'number': req_num
                    })
                    ctx.send(get_msg, str(node))
            if value:
                value = value[0]
            if self._pending_get[req_num].num_resp >= quorum:
                resp = Message('GET_RESP', {
                    'key': key,
                    'value': value
                })
                ctx.send_local(resp)
                self._pending_get.pop(req_num)
            
            else:
                ctx.set_timer("get" + req_num, 15)
            


        # Store (key, value) record
        # Request:
        #   PUT {"key": "some key", "value: "some value", "quorum": 1-3}
        # Response:
        #   PUT_RESP {"key": "some key", "value: "some value"}
        elif msg.type == 'PUT':
            
            self._put_number += 1
            time = ctx.time()
            req_num = str(self._put_number) + str(time)
            curr_req = PendingRequest()
            key = msg['key']
            value = [msg['value'], time]
            quorum = msg['quorum']
            curr_req.quorum = quorum
            curr_req.key = key
            curr_req.value = value
            replicas = get_key_replicas(key, len(self._nodes))
            curr_req.unresponsive = [str(i) for i in replicas]
            curr_req.asked_last = replicas[2]
            curr_req.asked_total = 3
            self._pending_put[req_num] = curr_req
            for node in replicas:
                if node == self._id:
                    curr_value = self._data.get(key)
                    self._data[key] = cmp(curr_value, value)
                    self._pending_put[req_num].value = value
                    self._pending_put[req_num].num_resp += 1
                else:
                    put_msg = Message('PUT', {
                        'key': key,
                        'value': value,
                        'sender': self._id,
                        'number': req_num
                    })
                    ctx.send(put_msg, str(node))
            if self._pending_put[req_num].num_resp >= quorum:
                resp = Message('PUT_RESP', {
                    'key': key,
                    'value': value[0]
                })
                ctx.send_local(resp)
                self._pending_put.pop(req_num)
            
            else:
                ctx.set_timer("put" + req_num, 15)
            

        # Delete value for the key
        # Request:
        #   DELETE {"key": "some key", "quorum": 1-3}
        # Response:
        #   DELETE_RESP {"key": "some key", "value": "some value"}
        elif msg.type == 'DELETE':

            self._delete_number += 1
            time = ctx.time()
            req_num = str(self._delete_number) + str(time)
            curr_req = PendingRequest()
            key = msg['key']
            quorum = msg['quorum']
            curr_req.quorum = quorum
            curr_req.key = key
            curr_req.time = time
            replicas = get_key_replicas(key, len(self._nodes))
            curr_req.unresponsive = [str(i) for i in replicas]
            curr_req.asked_last = replicas[2]
            curr_req.asked_total = 3
            self._pending_delete[req_num] = curr_req
            value = [None, time]
            curr_value = None
            for node in replicas:
                if node == self._id:
                    curr_value = self._data.get(key)
                    self._data[key] = cmp(curr_value, value)
                    self._pending_delete[req_num].value = value
                    self._pending_delete[req_num].num_resp += 1
                else:
                    delete_msg = Message('DELETE', {
                        'key': key,
                        'value': value,
                        'sender': self._id,
                        'number': req_num,
                        'time': time
                    })
                    ctx.send(delete_msg, str(node))
            if curr_value:
                curr_value = curr_value[0]
            if self._pending_delete[req_num].num_resp >= quorum:
                resp = Message('DELETE_RESP', {
                    'key': key,
                    'value': curr_value
                })
                ctx.send_local(resp)
                self._pending_delete.pop(req_num)
            
            else:
                ctx.set_timer("delete" + req_num, 15)
            

    def on_message(self, msg: Message, sender: str, ctx: Context):
        if msg.type == 'GET':
            key = msg['key']
            sender = msg['sender']
            value = self._data.get(key)
            resp = Message('GET_RESP', {
                'key': key,
                'value': value,
                'number': msg['number'],
                'sender': self._id
            })
            ctx.send(resp, sender)
        
        if msg.type == 'GET_RESP':
            value = msg['value']
            key = msg['key']
            number = msg['number']
            if number not in self._pending_get:
                return
            quorum = self._pending_get[number].quorum
            curr_value = self._pending_get[number].value
            self._pending_get[number].value = cmp(curr_value, value)
            self._pending_get[number].num_resp += 1
            num_resp = self._pending_get[number].num_resp
            value = self._pending_get[number].value
            if value:
                res_value = value[0]
            else:
                res_value = value
            if num_resp >= quorum:
                ctx.cancel_timer("get" + number)
                resp = Message('GET_RESP', {
                    'key': key,
                    'value': res_value
                })
                ctx.send_local(resp)
                self._pending_get.pop(number)
                if value:
                    upd = Message('UPD', {
                        'key': key,
                        'value': value
                    })
                    replicas = get_key_replicas(key, len(self._nodes))
                    for node in replicas:
                        if node == self._id:
                            self._data[key] = value
                        else:
                            ctx.send(upd, str(node))

        
        if msg.type == 'GET_HANDED':
            key = msg['key']
            sender = msg['sender']
            value = self._handed_data.get(key)
            resp = Message('GET_RESP', {
                'key': key,
                'value': value,
                'number': msg['number']
            })
            ctx.send(resp, sender)
        
        if msg.type == 'PUT':
            key = msg['key']
            value = msg['value']
            sender = msg['sender']
            curr_value = self._data.get(key)
            self._data[key] = cmp(curr_value, value)
            resp = Message('PUT_RESP', {
                'key': key,
                'value': cmp(curr_value, value),
                'number': msg['number'],
                'sender': self._id
            })
            ctx.send(resp, sender)
        
        if msg.type == 'PUT_RESP':
            value = msg['value']
            key = msg['key']
            number = msg['number']
            if number not in self._pending_put:
                return
            quorum = self._pending_put[number].quorum
            curr_value = self._pending_put[number].value
            self._pending_put[number].value = cmp(curr_value, value)
            self._pending_put[number].num_resp += 1
            self._pending_put[number].unresponsive.remove(msg['sender'])
            num_resp = self._pending_put[number].num_resp
            value = self._pending_put[number].value
            if value:
                res_value = value[0]
            else:
                res_value = value
            if num_resp >= quorum:
                ctx.cancel_timer("put" + number)
                resp = Message('PUT_RESP', {
                    'key': key,
                    'value': res_value
                })
                ctx.send_local(resp)
                self._pending_put.pop(number)
        
        if msg.type == 'PUT_HANDED':
            key = msg['key']
            value = msg['value']
            sender = msg['sender']
            recip = msg['recipient']
            curr_value = self._handed_data.get(key)
            self._handed_data[key] = cmp(curr_value, value)
            resp = Message('PUT_RESP', {
                'key': key,
                'value': cmp(curr_value, value),
                'number': msg['number'],
                'sender': recip
            })
            ctx.send(resp, sender)
            self._handed_number += 1
            hn = self._handed_number
            self._handed_values[str(hn)] = [recip, key, self._handed_data[key]]
            ctx.set_timer("handoff" + str(hn), 15)
        
        if msg.type == 'DELETE':
            key = msg['key']
            value = msg['value']
            sender = msg['sender']
            curr_value = self._data.get(key)
            self._data[key] = cmp(curr_value, value)
            resp = Message('DELETE_RESP', {
                'key': key,
                'value': curr_value,
                'number': msg['number'],
                'sender': self._id
            })
            ctx.send(resp, sender)
        
        if msg.type == 'DELETE_RESP':
            value = msg['value']
            key = msg['key']
            number = msg['number']
            if number not in self._pending_delete:
                return
            quorum = self._pending_delete[number].quorum
            curr_value = self._pending_delete[number].value
            self._pending_delete[number].value = cmp(curr_value, value)
            self._pending_delete[number].num_resp += 1
            self._pending_delete[number].unresponsive.remove(msg['sender'])
            num_resp = self._pending_delete[number].num_resp
            value = self._pending_delete[number].value
            if value:
                value = value[0]
            if num_resp >= quorum:
                ctx.cancel_timer("delete" + number)
                resp = Message('DELETE_RESP', {
                    'key': key,
                    'value': self._pending_delete[number].value[0]
                })
                ctx.send_local(resp)
                self._pending_delete.pop(number)
        
        if msg.type == 'DELETE_HANDED':
            key = msg['key']
            value = msg['value']
            sender = msg['sender']
            recip = msg['recipient']
            curr_value = self._handed_data.get(key)
            self._handed_data[key] = cmp(curr_value, value)
            resp = Message('DELETE_RESP', {
                'key': key,
                'value': curr_value,
                'number': msg['number'],
                'sender': recip
            })
            ctx.send(resp, sender)
            if recip not in self._handed:
                self._handed[recip] = []
            self._handed[recip].append(key)
            self._handed_number += 1
            hn = self._handed_number
            self._handed_values[str(hn)] = [recip, key, self._handed_data[key]]
            ctx.set_timer("handoff" + str(hn), 15)
    
        
        if msg.type == 'UPD':
            key = msg['key']
            value = msg['value']
            curr_value = self._data.get(key)
            self._data[key] = cmp(curr_value, value)
        
        if msg.type == 'HANDOFF':
            key = msg['key']
            value = msg['value']
            number = msg['number']
            sender = msg['sender']
            curr_value = self._data.get(key)
            self._data[key] = cmp(curr_value, value)
            resp = Message('HANDOFF_ACK', {
                'number': number
            })
            ctx.send(resp, sender)
        
        if msg.type == 'HANDOFF_ACK':
            number = msg['number']
            ctx.cancel_timer("handoff" + number)
            self._handed_values.pop(number)


    def on_timer(self, timer_name: str, ctx: Context):
        if timer_name[:3] == "get":

            number = timer_name[3:]
            last = self._pending_get[number].asked_last
            total = self._pending_get[number].asked_total
            quorum = self._pending_get[number].quorum
            key = self._pending_get[number].key
            num_resp = self._pending_get[number].num_resp
            value = self._pending_get[number].value
            if value:
                res_value = value[0]
            if num_resp >= quorum:
                ctx.cancel_timer("get" + number)
                value = self._pending_get[number].value
                resp = Message('GET_RESP', {
                    'key': key,
                    'value': res_value
                })
                ctx.send_local(resp)
                self._pending_get.pop(number)
                if value:
                    upd = Message('UPD', {
                        'key': key,
                        'value': value
                    })
                    replicas = get_key_replicas(key, len(self._nodes))
                    for node in replicas:
                        if node == self._id:
                            self._data[key] = value
                        else:
                            ctx.send(upd, str(node))
                return
            if total >= len(self._nodes):
                return
            ask = get_next_replica(last, len(self._nodes))
            self._pending_get[number].asked_last = ask
            self._pending_get[number].asked_total += 1
            get_msg = Message('GET_HANDED', {
                'key': key,
                'sender': self._id,
                'number': number
            })
            ctx.send(get_msg, str(ask))
            ctx.set_timer("get" + number, 10)
        
        if timer_name[:3] == "put":
            number = timer_name[3:]
            last = self._pending_put[number].asked_last
            total = self._pending_put[number].asked_total
            quorum = self._pending_put[number].quorum
            key = self._pending_put[number].key
            value = self._pending_put[number].value
            num_resp = self._pending_put[number].num_resp
            unresp = self._pending_put[number].unresponsive
            if num_resp >= quorum:
                resp = Message('PUT_RESP', {
                    'key': key,
                    'value': value[0]
                })
                ctx.send_local(resp)
                self._pending_put.pop(number)
                return
            if total >= len(self._nodes):
                return
            ask = get_next_replica(last, len(self._nodes))
            self._pending_put[number].asked_last = ask
            self._pending_put[number].asked_total += 1
            put_msg = Message('PUT_HANDED', {
                'key': key,
                'value': value,
                'sender': self._id,
                'number': number,
                'recipient': unresp[-1]
            })
            ctx.send(put_msg, str(ask))
            ctx.set_timer("put" + number, 10)
        
        if timer_name[:6] == "delete":

            number = timer_name[6:]
            last = self._pending_delete[number].asked_last
            total = self._pending_delete[number].asked_total
            quorum = self._pending_delete[number].quorum
            key = self._pending_delete[number].key
            num_resp = self._pending_delete[number].num_resp
            value = self._pending_delete[number].value
            if value:
                res_value = value[0]
            if num_resp >= quorum:
                ctx.cancel_timer("delete" + number)
                value = self._pending_delete[number].value
                resp = Message('DELETE_RESP', {
                    'key': key,
                    'value': res_value
                })
                ctx.send_local(resp)
                self._pending_delete.pop(number)
                if value:
                    upd = Message('UPD', {
                        'key': key,
                        'value': value
                    })
                    replicas = get_key_replicas(key, len(self._nodes))
                    for node in replicas:
                        if node == self._id:
                            self._data[key] = value
                        else:
                            ctx.send(upd, str(node))
                return
            if total >= len(self._nodes):
                return
            ask = get_next_replica(last, len(self._nodes))
            self._pending_delete[number].asked_last = ask
            self._pending_delete[number].asked_total += 1
            time = self._pending_delete[number].time
            unresp = self._pending_put[number].unresponsive
            get_msg = Message('DELETE_HANDED', {
                'key': key,
                'sender': self._id,
                'number': number,
                'value': [None, time],
                'recipient': unresp[-1]
            })
            ctx.send(get_msg, str(ask))
            ctx.set_timer("delete" + number, 10)
        
        if timer_name[:7] == "handoff":
            hn = timer_name[7:]
            recip = self._handed_values[hn][0]
            key = self._handed_values[hn][1]
            value = self._handed_values[hn][2]
            get_msg = Message('HANDOFF', {
                'key': key,
                'sender': self._id,
                'number': hn,
                'value': value
            })
            ctx.send(get_msg, str(recip))
            ctx.set_timer("handoff" + hn, 15)



def get_key_replicas(key: str, node_count: int):
    replicas = []
    key_hash = int.from_bytes(hashlib.md5(key.encode('utf8')).digest(), 'little', signed=False)
    cur = key_hash % node_count
    for _ in range(3):
        replicas.append(cur)
        cur = get_next_replica(cur, node_count)
    return replicas


def get_next_replica(i, node_count: int):
    return (i + 1) % node_count
