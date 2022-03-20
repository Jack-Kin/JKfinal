import h5py
import struct
import time
import logging
import socket

import logging
now = int(round(time.time()*1000))

now_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(now / 1000))

now_str = now_str.replace(" ", "-").replace(':','')
log_file_name= './log/trade1-' + now_str + '.log'
logging.basicConfig(filename=log_file_name, level=logging.DEBUG)

f1 = open("./data/chen/trade1", "ab")
f2 = open("./data/chen/trade2", "ab")
f3 = open("./data/chen/trade3", "ab")
f4 = open("./data/chen/trade4", "ab")
f5 = open("./data/chen/trade5", "ab")
f6 = open("./data/chen/trade6", "ab")
f7 = open("./data/chen/trade7", "ab")
f8 = open("./data/chen/trade8", "ab")
f9 = open("./data/chen/trade9", "ab")
f10 = open("./data/chen/trade10", "ab")
fp = [f1, f2, f3, f4, f5, f6, f7, f8, f9, f10]


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
    # print(pkg)
    # print(datastr_list)
    for datastr in datastr_list:
        if datastr == '':
            break
        datastr += '}'

        data = eval(datastr)
        print(data)
        if 'bid_id' in data.keys():
            # {'stk_code': 1, 'bid_id': 109, 'ask_id': 118, 'price': 1016.79, 'volume': 146}
            trade = Trade(stk_code=data['stk_code'], bid_id=data['bid_id'], ask_id=data['ask_id'],
                          price=data['price'], volume=data['volume'])

            logging.debug("trade:" + str((trade.stk_code, trade.bid_id, trade.ask_id, trade.price, trade.volume)))
            print("trade:", data)
            trade_list[trade.stk_code - 1].append(trade)


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
trade_list = [trade1, trade2, trade3, trade4, trade5, trade6, trade7, trade8, trade9, trade10]

last_save_index = [0] * 10


# 存储trade数据
def serialization():
    try:
        for i in range(10):
            last_index = last_save_index[i]
            while last_index < len(trade_list[i]):
                fp[i].write(trade_list[i][last_index].to_bytes())
                last_index += 1
            last_save_index[i] = last_index
    except struct.error:
        pass


if __name__ == '__main__':

    order_id_path = "./data/100x10x10/order_id1.h5"
    direction_path = "./data/100x10x10/direction1.h5"
    price_path = "./data/100x10x10/price1.h5"
    volume_path = "./data/100x10x10/volume1.h5"
    type_path = "./data/100x10x10/type1.h5"
    hook_path = "./data/100x10x10/hook.h5"
    # order_id_path = "./data/100x100x100/order_id1.h5"
    # direction_path = "./data/100x100x100/direction1.h5"
    # price_path = "./data/100x100x100/price1.h5"
    # volume_path = "./data/100x100x100/volume1.h5"
    # type_path = "./data/100x100x100/type1.h5"
    # hook_path = "./data/100x100x100/hook.h5"
    print("===============data in start============")
    logging.debug("===============data in start============")
    start = time.time()
    order_id_mtx = h5py.File(order_id_path, 'r')['order_id'][:]
    direction_mtx = h5py.File(direction_path, 'r')['direction'][:]
    price_mtx = h5py.File(price_path, 'r')['price'][:]
    prev_price_mtx = h5py.File(price_path, 'r')['prev_close'][:]
    volume_mtx = h5py.File(volume_path, 'r')['volume'][:]
    type_mtx = h5py.File(type_path, 'r')['type'][:]
    hook_mtx = h5py.File(hook_path, 'r')['hook'][:]
    #  加速查找
    hook_map = {}
    for x in range(10):
        # x stk_code-1
        for y in range(len(hook_mtx[x][:, 0])):
            hook_map[(x + 1, hook_mtx[x][:, 0][y])] = hook_mtx[x][y]

    logging.debug("===============data in end============" + str(time.time() - start))
    print("===============data in end============", time.time() - start)

    # 合理的价格变动区间，对于type=0的限价交易
    price_range = []
    for i in prev_price_mtx:
        price_range.append((round(i * 0.9, 2), round(i * 1.1, 2)))
    logging.debug("价格变动范围" + str(price_range))
    print("价格变动范围", price_range)

    x_len, y_len, z_len = order_id_mtx.shape
    logging.debug("本次读入数据规模是：" + str(order_id_mtx.shape))
    print("本次读入数据规模是：" + str(order_id_mtx.shape))
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
    queue_list = [queue1, queue2, queue3, queue4, queue5, queue6, queue7, queue8, queue9, queue10]
    # TODO 数据量大了之后，下面这个三重循环会非常慢，还没处理好
    logging.debug("===============data load start============")
    print("===============data load start============")
    start = time.time()
    for i in range(x_len):
        for j in range(y_len):
            for k in range(z_len):
                order = SimpleOrder(i, j, k, order_id_mtx[i, j, k], type_mtx[i, j, k], price_mtx[i, j, k],
                                    volume_mtx[i, j, k])
                queue_list[order.stk_code - 1].append(order)

    logging.debug("===============data load end============  cost " + str(time.time() - start))
    print("===============data load end============  cost " + str(time.time() - start))

    logging.debug("===============data sort and filter start============")
    print("===============data sort and filter start============")
    start = time.time()
    # chenwei 我想的是逆序排的话，小号在后边，我们需要先发小号，可以直接pop(),O(1)复杂度，不知道有没有必要
    for i in range(len(queue_list)):
        queue_list[i].sort(key=lambda _: _.order_id, reverse=True)
        for _ in queue_list[i]:
            # 需要处理TYPE=0时，价格超出上下10%的订单
            if _.type == 0 and not (price_range[i][0] <= _.price <= price_range[i][1]):
                _.volume = 0
    end = time.time()
    logging.debug("===============data sort and filter completed============" + str(end - start))
    print("===============data sort and filter completed============" + str(end - start))

    server_address = '10.216.68.208'
    server_port = 10011
    client_address = '10.216.68.226'
    client_port = 11071
    # 要填内网ip,不能写localhost和127.0.0.1
    # address = '10.216.68.191'
    # port = 60125
    buffersize = 2048

    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.bind((client_address, client_port))
    client.connect((server_address, server_port))

    logging.debug(

        "===============Trader connect to ================" + str(server_address) + "port:" + str(server_port))
    print(
        "===============Trader connect to ================" + str(server_address) + "port:" + str(server_port))
    last_save_time = time.time()
    remain_str = ''
    while True:
        for i in range(len(queue_list)):
            # if i > 0:
            #     break
            if queue_list[i]:
                order = queue_list[i][-1]
                # [self_order_id, target_stk_code, target_trade_idx, arg]，有一个self_order_id代表这个特殊order的
                # order_id，这个order需要根据已有的trade序列进行判断是否进入撮合，target_stk_code代表目标
                # trade序列所属的stk_code，target_trade_idx代表需要的trade在这个trade序列中的idx（从1开始），
                # 最后的arg是一个用于判断的信号量，判断规则如下：如果目标的trade的volume值（撮合成交量）小于
                # 等于arg，那么这个order才会进入撮合。
                # 属于特殊类型  hook_mtx[i] 100*4  hook_mtx[i][j] 1*4
                key = (i + 1, order.order_id)
                # 特殊order
                if key in hook_map.keys():
                    row = hook_map[key]
                    trade_seq = trade_list[row[1] - 1]
                    # 要比较的trade还没传回来
                    if row[2] - 1 < len(trade_seq):  # 不符合条件，把volume改成0，在exchange那边直接跳过这种order，因为要保证序号连续，所以还是得发
                        # 不符合条件，把volume改成0，在exchange那边直接跳过这种order，因为要保证序号连续，所以还是得发
                        if trade_seq[row[2] - 1].volume > row[3]:
                            order.volume = 0
                        client.send(getCmdPkg(order))
                        queue_list[i].pop()

                # 不是特殊order，直接发送
                else:
                    client.send(getCmdPkg(order))
                    queue_list[i].pop()

        data = client.recv(buffersize)  # 接收一个信息，并指定接收的大小 为1024字节
        print('recv:', data.decode())  # 输出我接收的信息
        str_data = data.decode()
        boundry_index = str_data.rindex("}")

        save(remain_str + str_data[0:boundry_index+1])

        remain_str = str_data[boundry_index+1:]
        if not remain_str:
            print(remain_str)
        if time.time() - last_save_time >= 30:
            serialization()
            last_save_time = time.time()
        # 把接收到的Trade存起来
        time.sleep(1)

    client.close()  # 关闭这个链接
    for f in fp:
        f.close()
