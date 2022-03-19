import h5py
import struct
import time
import logging
import socket


# 申报单
class SimpleOrder:
    def __init__(self, x, y, z, order_id, type, price, volume):
        self.x = x
        self.y = y
        self.z = z
        self.order_id = order_id
        self.stk_code = x % 10 + 1
        self.type = type
        self.price = price
        self.volume = volume


# 成交单
class Trade:
    def __init__(self, stk_code, bid_id, ask_id, price, volume):
        self.stk_code = stk_code
        self.bid_id = bid_id  # 买方order_id
        self.ask_id = ask_id  # 卖方order_id
        self.price = price
        self.volume = volume

    def to_bytes(self):
        return struct.pack("=iiidi", self.stk_code, self.bid_id, self.ask_id, self.price, self.volume)


def getCmdPkg(order):
    data = {}
    data["stk_code"] = order.stk_code
    data["order_id"] = order.order_id
    data["direction"] = direction_mtx[order.x, order.y, order.z]
    data["type"] = type_mtx[order.x, order.y, order.z]
    data["price"] = price_mtx[order.x, order.y, order.z]
    data["volume"] = volume_mtx[order.x, order.y, order.z]
    return str(data).encode('utf-8')


def save(pkg):
    # 分割多个数据包
    datastr_list = pkg.split("}")
    for datastr in datastr_list:
        if datastr == '':
            break
        datastr += '}'

        data = eval(datastr)
        if 'bid_id' in data.keys():
            # {'stk_code': 1, 'bid_id': 109, 'ask_id': 118, 'price': 1016.79, 'volume': 146}
            trade = Trade(stk_code=data['stk_code'], bid_id=data['bid_id'], ask_id=data['ask_id'],
                          price=data['price'], volume=data['volume'])
            print("trade:",data)
            if trade.stk_code == 1:
                trade1.append(trade)
            elif trade.stk_code == 2:
                trade2.append(trade)
            elif trade.stk_code == 3:
                trade3.append(trade)
            elif trade.stk_code == 4:
                trade4.append(trade)
            elif trade.stk_code == 5:
                trade5.append(trade)
            elif trade.stk_code == 6:
                trade6.append(trade)
            elif trade.stk_code == 7:
                trade7.append(trade)
            elif trade.stk_code == 8:
                trade8.append(trade)
            elif trade.stk_code == 9:
                trade9.append(trade)
            else:
                trade10.append(trade)


trade1 = []
trade2 = []
trade3 = []
trade4 = []
trade5 = []
trade6 = []
trade7 = []
trade8 = []
trade9 = []
trade10 = []

if __name__ == '__main__':
    # order_id_path = "/data/team-19/100x100x100/order_id1.h5"
    # direction_path = "/data/team-19/100x100x100/direction1.h5"
    # price_path = "/data/team-19/100x100x100/price1.h5"
    # volume_path = "/data/team-19/100x100x100/volume1.h5"
    # type_path = "/data/team-19/100x100x100/type1.h5"
    order_id_path = "./100x100x100/order_id2.h5"
    direction_path = "./100x100x100/direction2.h5"
    price_path = "./100x100x100/price2.h5"
    volume_path = "./100x100x100/volume2.h5"
    type_path = "./100x100x100/type2.h5"

    print("===============data in start============")
    start = time.time()
    order_id_mtx = h5py.File(order_id_path, 'r')['order_id'][:]
    direction_mtx = h5py.File(direction_path, 'r')['direction'][:]
    price_mtx = h5py.File(price_path, 'r')['price'][:]
    prev_price_mtx = h5py.File(price_path, 'r')['prev_close'][:]
    volume_mtx = h5py.File(volume_path, 'r')['volume'][:]
    type_mtx = h5py.File(type_path, 'r')['type'][:]
    print("===============data in end============", time.time() - start)

    # 合理的价格变动区间，对于type=0的限价交易
    price_range = []
    for i in prev_price_mtx:
        price_range.append((round(i * 0.9, 2), round(i * 1.1, 2)))
    print("价格变动范围", price_range)

    x_len, y_len, z_len = order_id_mtx.shape
    print("本次读入数据规模是：", order_id_mtx.shape)
    queue1 = []
    queue2 = []
    queue3 = []
    queue4 = []
    queue5 = []
    queue6 = []
    queue7 = []
    queue8 = []
    queue9 = []
    queue10 = []
    # TODO 数据量大了之后，下面这个三重循环会非常慢，还没处理好
    print("===============data load start============")
    start = time.time()
    for i in range(x_len):
        for j in range(y_len):
            for k in range(z_len):

                order = SimpleOrder(i, j, k, order_id_mtx[i, j, k], type_mtx[i, j, k], price_mtx[i, j, k],
                                    volume_mtx[i, j, k])
                if order.stk_code == 1:
                    queue1.append(order)

                elif order.stk_code == 2:
                    queue2.append(order)

                elif order.stk_code == 3:
                    queue3.append(order)

                elif order.stk_code == 4:
                    queue4.append(order)

                elif order.stk_code == 5:
                    queue5.append(order)

                elif order.stk_code == 6:
                    queue6.append(order)

                elif order.stk_code == 7:
                    queue7.append(order)

                elif order.stk_code == 8:
                    queue8.append(order)

                elif order.stk_code == 9:
                    queue9.append(order)

                else:
                    queue10.append(order)

    print("===============data load end============  cost ", time.time() - start)

    print("===============data sort start============")
    start = time.time()
    # chenwei 我想的是逆序排的话，小号在后边，我们需要先发小号，可以直接pop(),O(1)复杂度，不知道有没有必要
    queue1.sort(key=lambda _: _.order_id, reverse=True)
    queue2.sort(key=lambda _: _.order_id, reverse=True)
    queue3.sort(key=lambda _: _.order_id, reverse=True)
    queue4.sort(key=lambda _: _.order_id, reverse=True)
    queue5.sort(key=lambda _: _.order_id, reverse=True)
    queue6.sort(key=lambda _: _.order_id, reverse=True)
    queue7.sort(key=lambda _: _.order_id, reverse=True)
    queue8.sort(key=lambda _: _.order_id, reverse=True)
    queue9.sort(key=lambda _: _.order_id, reverse=True)
    queue10.sort(key=lambda _: _.order_id, reverse=True)
    end = time.time()
    print("===============data sort completed============", end - start)
    print("===============data filter start============")
    start = time.time()

    # 需要处理TYPE=0时，价格超出上下10%的订单
    print("before:", len(queue1))
    queue1 = list(
        filter(lambda _: _.type != 0 or _.type == 0 and price_range[0][0] <= _.price <= price_range[0][1], queue1))
    print("after:", len(queue1))
    print("before:", len(queue2))
    queue2 = list(
        filter(lambda _: _.type != 0 or _.type == 0 and price_range[1][0] <= _.price <= price_range[1][1], queue2))
    print("after:", len(queue2))
    print("before:", len(queue3))
    queue3 = list(
        filter(lambda _: _.type != 0 or _.type == 0 and price_range[2][0] <= _.price <= price_range[2][1], queue3))
    print("after:", len(queue3))
    print("before:", len(queue4))
    queue4 = list(
        filter(lambda _: _.type != 0 or _.type == 0 and price_range[3][0] <= _.price <= price_range[3][1], queue4))
    print("after:", len(queue4))
    print("before:", len(queue5))
    queue5 = list(
        filter(lambda _: _.type != 0 or _.type == 0 and price_range[4][0] <= _.price <= price_range[4][1], queue5))
    print("after:", len(queue5))
    print("before:", len(queue6))
    queue6 = list(
        filter(lambda _: _.type != 0 or _.type == 0 and price_range[5][0] <= _.price <= price_range[5][1], queue6))
    print("after:", len(queue6))
    print("before:", len(queue7))
    queue7 = list(
        filter(lambda _: _.type != 0 or _.type == 0 and price_range[6][0] <= _.price <= price_range[6][1], queue7))
    print("after:", len(queue7))
    print("before:", len(queue8))
    queue8 = list(
        filter(lambda _: _.type != 0 or _.type == 0 and price_range[7][0] <= _.price <= price_range[7][1], queue8))
    print("after:", len(queue8))
    print("before:", len(queue9))
    queue9 = list(
        filter(lambda _: _.type != 0 or _.type == 0 and price_range[8][0] <= _.price <= price_range[8][1], queue9))
    print("after:", len(queue9))
    print("before:", len(queue10))
    queue10 = list(
        filter(lambda _: _.type != 0 or _.type == 0 and price_range[9][0] <= _.price <= price_range[9][1], queue10))
    print("after:", len(queue10))
    end = time.time()
    print("===============data filter completed============", end - start)
    trade1 = []
    server_address = 'localhost'
    server_port = 10012
    client_address = 'localhost'
    client_port = 11031
    # 要填内网ip,不能写localhost和127.0.0.1
    # address = '10.216.68.191'
    # port = 60125
    buffersize = 1024

    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.bind((client_address, client_port))
    client.connect((server_address, server_port))
    # client.setblocking(False)
    print("===============Trader connect to ================", server_address, server_port)

    while True:
        # 发送order数据
        if queue1:
            order = queue1.pop()
            client.send(getCmdPkg(order))
        # if queue2:
        #     order = queue2.pop()
        #     client.send(getCmdPkg(order))
        # if queue3:
        #     order = queue3.pop()
        #     client.send(getCmdPkg(order))
        # if queue4:
        #     order = queue4.pop()
        #     client.send(getCmdPkg(order))
        # if queue5:
        #     order = queue5.pop()
        #     client.send(getCmdPkg(order))
        # if queue6:
        #     order = queue6.pop()
        #     client.send(getCmdPkg(order))
        # if queue7:
        #     order = queue7.pop()
        #     client.send(getCmdPkg(order))
        # if queue8:
        #     order = queue8.pop()
        #     client.send(getCmdPkg(order))
        # if queue9:
        #     order = queue9.pop()
        #     client.send(getCmdPkg(order))
        # if queue10:
        #     order = queue10.pop()
        #     client.send(getCmdPkg(order))
        data = client.recv(buffersize)  # 接收一个信息，并指定接收的大小 为1024字节
        # print('recv:', data.decode())  # 输出我接收的信息
        save(data.decode())
        # 把接收到的Trade存起来
        time.sleep(1)
        # try:
        #
        # except BlockingIOError as e:
        #     pass

    client.close()  # 关闭这个链接

# 实例的大小是500x10x10  决赛是500x1000X1000
