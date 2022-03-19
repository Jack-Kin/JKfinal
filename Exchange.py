import socket
import threading
import struct
import time
import logging

logging.basicConfig(filename='exchange.log', level=logging.DEBUG)


class Order:
    def __init__(self, stk_code, order_id, direction, price, volume, type):
        self.stk_code = stk_code
        self.order_id = order_id
        self.direction = direction  # 1为买入 -1为卖出
        self.price = price
        self.volume = volume
        # 限价申报（类型0），对手方最优价格申报
        # （类型1），本方最优价格申报（类型2），最优五档即时成交剩余撤销申报（类型3），即时成交
        # 剩余撤销申报（类型4），全额成交或撤销申报（类型5）
        self.type = type


class Trade:
    def __init__(self, stk_code, bid_id, ask_id, price, volume):
        self.stk_code = stk_code
        self.bid_id = bid_id  # 买方order_id
        self.ask_id = ask_id  # 卖方order_id
        self.price = price
        self.volume = volume

    def to_bytes(self):
        return struct.pack("=iiidi", self.stk_code, self.bid_id, self.ask_id, self.price, self.volume)


def getCmdPkg(trade):
    data = {}
    data["stk_code"] = trade.stk_code
    data["bid_id"] = trade.bid_id
    data["ask_id"] = trade.ask_id
    data["price"] = trade.price
    data["volume"] = trade.volume
    return str(data).encode('utf-8')


buffersize = 1024
# 建立一个服务端
address1 = 'localhost'
port1 = 10011
server1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server1.bind((address1, port1))  # 绑定要监听的端口
server1.listen(5)  # 开始监听 表示可以使用五个链接排队
print("===========a server started=============", address1, port1)
# 建立一个服务端
address2 = 'localhost'
port2 = 10012
server2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server2.bind((address2, port2))  # 绑定要监听的端口
server2.listen(5)  # 开始监听 表示可以使用五个链接排队
print("===========a server started=============", address2, port2)
# # 建立一个服务端
# address3 = 'localhost'
# port3 = 10013
# server3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# server3.bind((address3, port3))  # 绑定要监听的端口
# server3.listen(5)  # 开始监听 表示可以使用五个链接排队
# print("===========a server started=============", address3, port3)

# order用来存储收到的order序列，处理完的移除队列
order1 = []
order2 = []
order3 = []
order4 = []
order5 = []
order6 = []
order7 = []
order8 = []
order9 = []
order10 = []
order_list = [order1, order2, order3, order4, order5, order6, order7, order8, order9, order10]


def process(pkg, socket):
    # 分割多个数据包
    datastr_list = pkg.split("}")
    for datastr in datastr_list:
        if datastr == '':
            break
        datastr += '}'
        # TODO  以下的处理，应该放到另外的线程里，不阻塞主线程
        data = eval(datastr)
        order = Order(stk_code=data['stk_code'], type=data['type'], direction=data['direction'], price=data['price'],
                      order_id=data['order_id'], volume=data['volume'])

        order_list[order.stk_code - 1].append(order)


def tcplink(socket, addr, remain_str):
    try:
        data = socket.recv(buffersize)  # 接收数据
        if data == 'exit' or not data:
            return
        str_data = data.decode('utf-8')
        print('receive:', str_data, 'from', addr)  # 打印接收到的数据

        boundry_index = str_data.rindex("}")

        process(remain_str + str_data[0:boundry_index + 1], socket)
        remain_str = str_data[boundry_index + 1:]

        if not remain_str:
            print(remain_str)


        # socket.send(data)
        dict = {'message': 'OK'}
        socket.send(str(dict).encode('utf-8'))
        # time.sleep(2)
        return remain_str

    except Exception as e:
        print("exception occurred...", e, addr)


order1_buy = []
order2_buy = []
order3_buy = []
order4_buy = []
order5_buy = []
order6_buy = []
order7_buy = []
order8_buy = []
order9_buy = []
order10_buy = []
# 买榜
order_buy_list = [order1_buy, order2_buy, order3_buy, order4_buy, order5_buy, order6_buy, order7_buy, order8_buy,
                  order9_buy, order10_buy]

order1_sell = []
order2_sell = []
order3_sell = []
order4_sell = []
order5_sell = []
order6_sell = []
order7_sell = []
order8_sell = []
order9_sell = []
order10_sell = []
# 卖榜
order_sell_list = [order1_sell, order2_sell, order3_sell, order4_sell, order5_sell, order6_sell, order7_sell,
                   order8_sell, order9_sell, order10_sell]

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
# Trade列表
trade_list = [trade1, trade2, trade3, trade4, trade5, trade6, trade7, trade8, trade9, trade10]

last_order_id = [0] * 10
last_order1_id = 0
remain_str1 = ''
remain_str2 = ''
while True:
    client_socket1, client_addr1 = server1.accept()
    print("server1 have a new connection : connecting from:", client_addr1)
    logging.debug("server1 have a new connection : connecting from:" + str(client_addr1))
    client_socket2, client_addr2 = server2.accept()
    print("server2 have a new connection : connecting from:", client_addr2)
    logging.debug("server2 have a new connection : connecting from:" + str(client_addr2))
    # client_socket3, client_addr3 = server3.accept()
    # print("server3 have a new connection : connecting from:", client_addr3)
    time_lapse = time.time()
    time_match = time.time()
    while True:
        # 传入的是旧的字符串碎片，返回的是新的
        remain_str1 = tcplink(client_socket1, client_addr1, remain_str1)
        remain_str2 = tcplink(client_socket2, client_addr2, remain_str2)
        # tcplink(client_socket3, client_addr3)
        if time.time() - time_lapse > 5:
            print("stk_1:", len(order_list[0]), "stk_2:", len(order_list[1]), "stk_3:", len(order_list[2]), "stk_4:",
                  len(order_list[3]), "stk_5:", len(order_list[4]), "stk_6:", len(order_list[5]), "stk_7:",
                  len(order_list[6]),
                  "stk_8:", len(order_list[7]), "stk_9:", len(order_list[8]), "stk_10:", len(order_list[9]))
            time_lapse = time.time()
        # 隔3s撮合一次
        print("time match:", time.time() - time_match)
        if time.time() - time_match > 3:
            # 要对数据进行撮合
            # 我们这些order列表的正式名字应该是叫
            # 集中申报簿：指交易主机某一时点有效竞价范围内按买卖方向以及价格优先、时间优先顺序排列的所有未成交申报队列
            # 处理申报之前，按申请先后顺序排序
            for i in range(10):

                order_list[i].sort(key=lambda x: x.order_id)
                # 维护一个有序不断的序列，如2,3,4,5,
                temp_order = []
                temp_last_orderid = last_order_id[i]
                index = 0
                for order in order_list[i]:
                    if temp_last_orderid + 1 == order.order_id:
                        temp_order.append(order)
                        temp_last_orderid += 1
                        index += 1
                    else:
                        break

                # 更新last_order_id
                last_order_id[i] = temp_last_orderid
                order_list[i] = order_list[i][index:]

                for order in temp_order:
                    # 是不需要处理的订单
                    if order.volume == 0:
                        continue
                    # 两个榜单排一下序，买方价格降序，卖方价格升序，下面的逻辑会往这两个榜单里加数据
                    order_buy_list[i].sort(key=lambda x: (-x.price, x.order_id))
                    order_sell_list[i].sort(key=lambda x: (x.price, x.order_id))

                    if order.type == 0:
                        # type=0 买入
                        if order.direction == 1:
                            # 如果卖方价格比我出的买价低，按照卖方的价格依次买入
                            while order_sell_list[i] and order_sell_list[i][0].price <= order.price and \
                                    order_sell_list[i][0].volume <= order.volume:
                                trade_list[i].append(
                                    Trade(order.stk_code, order.order_id, order_sell_list[i][0].order_id,
                                          order_sell_list[i][0].price,
                                          order_sell_list[i][0].volume))
                                # 减掉买到的份数
                                order.volume -= order_sell_list[i][0].volume
                                # 卖一买完了，应该删掉了
                                order_sell_list[i].pop(0)
                            # 如果卖方价格合适，并且比我的要求的份数多
                            if order_sell_list[i] and order_sell_list[i][0].price <= order.price and order_sell_list[i][
                                0].volume > order.volume:
                                trade_list[i].append(
                                    Trade(order.stk_code, order.order_id, order_sell_list[i][0].order_id,
                                          order_sell_list[i][0].price,
                                          order.volume))
                                # 更新卖一的剩余份数
                                order_sell_list[i][0].volume -= order.volume
                                order.volume = 0
                            # 剩余没有买到的部分加到买方的限价单中, 可能是sell列表空，或者价格不合适
                            if order.volume > 0:
                                order_buy_list[i].append(order)

                        # type=0 卖出
                        else:
                            # 如果买方价格比我出的卖价高，按照买方的价格依次买入
                            while order_buy_list[i] and order_buy_list[i][0].price >= order.price and order_buy_list[i][
                                0].volume <= order.volume:
                                trade_list[i].append(
                                    Trade(order.stk_code, order_buy_list[i][0].order_id, order.order_id,
                                          order_buy_list[i][0].price,
                                          order_buy_list[i][0].volume))
                                # 减掉卖出的份数
                                order.volume -= order_buy_list[i][0].volume
                                # 卖一买完了，应该删掉了
                                order_buy_list[i].pop(0)
                            # 如果买方价格合适，并且比我的要求的份数多
                            if order_buy_list[i] and order_buy_list[i][0].price >= order.price and order_buy_list[i][
                                0].volume > order.volume:
                                trade_list[i].append(
                                    Trade(order.stk_code, order_buy_list[i][0].order_id, order.order_id,
                                          order_buy_list[i][0].price,
                                          order.volume))
                                # 更新买一的剩余份数
                                order_buy_list[i][0].volume -= order.volume
                                order.volume = 0
                            # 剩余没有卖出的部分加到卖方的限价单中, 可能是buy列表空，或者价格不合适
                            if order.volume > 0:
                                order_sell_list[i].append(order)
                    # type=1 对手方最优价格申报
                    elif order.type == 1:
                        # type1 买入时，以卖一为限价
                        if order.direction == 1:
                            if order_sell_list[i]:

                                temp = order_sell_list[i][0]
                                cur_price = temp.price
                                while order_sell_list[i] and order_sell_list[i][
                                    0].price == cur_price and order.volume > 0:
                                    temp = order_sell_list[i][0]
                                    if order.volume <= temp.volume:
                                        trade_list[i].append(
                                            Trade(order.stk_code, order.order_id, temp.order_id, temp.price,
                                                  order.volume))
                                        temp.volume -= order.volume
                                        order.volume = 0
                                        break
                                    else:

                                        trade_list[i].append(
                                            Trade(order.stk_code, order.order_id, temp.order_id, temp.price,
                                                  temp.volume))

                                        order.volume -= temp.volume
                                        order_sell_list[i].pop(0)
                                # 剩余的部分按照卖一的价格挂在买方的限价单里
                                if order.volume > 0:
                                    order.price = cur_price
                                    order_buy_list[i].append(order)

                        # type1 卖出时，以买一为限价
                        else:
                            if order_buy_list[i]:
                                temp = order_buy_list[i][0]
                                cur_price = temp.price
                                while order_buy_list[i] and order_buy_list[i][
                                    0].price == cur_price and order.volume > 0:
                                    temp = order_buy_list[i][0]
                                    if order.volume <= temp.volume:
                                        trade_list[i].append(
                                            Trade(order.stk_code, temp.order_id, order.order_id, temp.price,
                                                  order.volume))
                                        temp.volume -= order.volume
                                        order.volume = 0
                                        break
                                    else:
                                        # 把卖一买完
                                        trade_list[i].append(
                                            Trade(order.stk_code, temp.order_id, order.order_id, temp.price,
                                                  temp.volume))

                                        order.volume -= temp.volume
                                        order_buy_list[i].pop(0)
                                # 剩余的部分按照买一的价格挂在卖方的限价单里
                                if order.volume > 0:
                                    order.price = cur_price
                                    order_sell_list[i].append(order)
                    # 本方最优价格申报
                    elif order.type == 2:
                        if order.direction == 1:
                            if order_buy_list[i]:
                                order.price = order_buy_list[i][0].price
                                order_buy_list[i].append(order)
                        else:
                            if order_sell_list[i]:
                                order.price = order_sell_list[i][0].price
                                order_sell_list[i].append(order)

                    # 最优五档即时成交剩余撤销申报
                    elif order.type == 3:
                        # 买入要把卖一到卖五依次成交,未成交的部分撤销
                        if order.direction == 1:
                            price_set = set()

                            while order_sell_list[i] and len(price_set) <= 5:
                                price_set.add(order_sell_list[i][0].price)
                                if order.volume >= order_sell_list[i][0].volume:
                                    trade_list[i].append(
                                        Trade(order.stk_code, order.order_id, order_sell_list[i][0].order_id,
                                              order_sell_list[i][0].price,
                                              order_sell_list[i][0].volume))
                                    order.volume -= order_sell_list[i][0].volume
                                    order_sell_list[i].pop(0)
                                else:
                                    trade_list[i].append(
                                        Trade(order.stk_code, order.order_id, order_sell_list[i][0].order_id,
                                              order_sell_list[i][0].price,
                                              order.volume))
                                    order_sell_list[i][0].volume -= order.volume
                                    break
                        # 卖出要把买一到买五依次成交,未成交的部分撤销
                        else:
                            price_set = set()
                            while order_buy_list[i] and len(price_set) <= 5:
                                price_set.add(order_buy_list[i][0].price)
                                if order.volume >= order_buy_list[i][0].volume:
                                    trade_list[i].append(
                                        Trade(order.stk_code, order_buy_list[i][0].order_id, order.order_id,
                                              order_buy_list[i][0].price,
                                              order_buy_list[i][0].volume))

                                    order.volume -= order_buy_list[i][0].volume
                                    order_buy_list[i].pop(0)
                                else:
                                    trade_list[i].append(
                                        Trade(order.stk_code, order_buy_list[i][0].order_id, order.order_id,
                                              order_buy_list[i][0].price,
                                              order.volume))
                                    order_buy_list[i][0].volume -= order.volume
                                    break
                    # 即时成交剩余撤销申报
                    elif order.type == 4:
                        # 买入要把所有卖方的单子依次成交,未成交的部分撤销
                        if order.direction == 1:

                            while order_sell_list[i]:
                                if order.volume >= order_sell_list[i][0].volume:
                                    trade_list[i].append(
                                        Trade(order.stk_code, order.order_id, order_sell_list[i][0].order_id,
                                              order_sell_list[i][0].price,
                                              order_sell_list[i][0].volume))
                                    order.volume -= order_sell_list[i][0].volume
                                    order_sell_list[i].pop(0)
                                else:
                                    trade_list[i].append(
                                        Trade(order.stk_code, order.order_id, order_sell_list[i][0].order_id,
                                              order_sell_list[i][0].price,
                                              order.volume))
                                    order_sell_list[i][0].volume -= order.volume
                                    break
                        # 卖出要把所有买方的单子依次成交,未成交的部分撤销
                        else:
                            while order_buy_list[i]:
                                if order.volume >= order_buy_list[i][0].volume:
                                    trade_list[i].append(
                                        Trade(order.stk_code, order_buy_list[i][0].order_id, order.order_id,
                                              order_buy_list[i][0].price,
                                              order_buy_list[i][0].volume))
                                    order.volume -= order_buy_list[i][0].volume
                                    order_buy_list[i].pop(0)

                                else:
                                    trade_list[i].append(
                                        Trade(order.stk_code, order_buy_list[i][0].order_id, order.order_id,
                                              order_buy_list[i][0].price,
                                              order.volume))
                                    order_buy_list[i][0].volume -= order.volume
                                    break
                    # 全额成交或撤销申报
                    elif order.type == 5:

                        if order.direction == 1:
                            if not order_sell_list[i]:
                                continue
                            temp_trade1 = []
                            count = 0
                            for sell in order_sell_list[i]:
                                if sell.volume <= order.volume:
                                    temp_trade1.append(Trade(order.stk_code, order.order_id, sell.order_id, sell.price,
                                                             sell.volume))
                                    order.volume -= sell.volume
                                    count += 1
                                else:
                                    temp_trade1.append(Trade(order.stk_code, order.order_id, sell.order_id, sell.price,
                                                             order.volume))
                                    sell.volume -= order.volume
                                    order.volume = 0
                                    count += 0.5
                                    break
                            # 全部成交了
                            if order.volume == 0:
                                trade_list[i] += temp_trade1
                                # 移除使用过的sell
                                while count > 0.5:
                                    order_sell_list[i].pop(0)
                                    count -= 1

                        else:
                            if not order_buy_list[i]:
                                continue
                            temp_trade1 = []
                            count = 0
                            for buy in order_buy_list[i]:
                                if buy.volume <= order.volume:
                                    temp_trade1.append(Trade(order.stk_code, buy.order_id, order.order_id, buy.price,
                                                             buy.volume))
                                    order.volume -= buy.volume
                                    count += 1
                                else:
                                    temp_trade1.append(Trade(order.stk_code, buy.order_id, order.order_id, buy.price,
                                                             order.volume))
                                    buy.volume -= order.volume
                                    order.volume = 0
                                    count += 0.5
                                    break
                            # 全部成交了
                            if order.volume == 0:
                                trade_list[i] += temp_trade1
                                # 移除使用过的sell
                                while count > 0.5:
                                    order_buy_list[i].pop(0)
                                    count -= 1

                    else:
                        print("unexpected order type:", order.type)

                for ele in trade_list[i]:
                    client_socket1.send(getCmdPkg(ele))
                    client_socket2.send(getCmdPkg(ele))
                trade_list[i].clear()

            print("已经执行到orderid", last_order_id)
            logging.debug("处理进度:" + str(last_order_id))
            time_match = time.time()

client_socket1.close()
client_socket2.close()

server1.close()
server2.close()
