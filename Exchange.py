import socket
import threading
import struct
import time


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


# 建立一个服务端
address1 = 'localhost'
port1 = 10011
# address = '10.216.68.191'
# port = 60125
buffersize = 1024
server1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server1.bind((address1, port1))  # 绑定要监听的端口
server1.listen(5)  # 开始监听 表示可以使用五个链接排队
print("===========a server started=============", address1, port1)
# 建立一个服务端
address2 = 'localhost'
port2 = 10012
# address = '10.216.68.191'
# port = 60125
buffersize = 1024
server2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server2.bind((address2, port2))  # 绑定要监听的端口
server2.listen(5)  # 开始监听 表示可以使用五个链接排队
print("===========a server started=============", address2, port2)
# order用来存储收到的order序列，处理完的移除队列，处理的部分还没写
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
        if order.stk_code == 1:
            order1.append(order)
        elif order.stk_code == 2:
            order2.append(order)
        elif order.stk_code == 3:
            order3.append(order)
        elif order.stk_code == 4:
            order4.append(order)
        elif order.stk_code == 5:
            order5.append(order)
        elif order.stk_code == 6:
            order6.append(order)
        elif order.stk_code == 7:
            order7.append(order)
        elif order.stk_code == 8:
            order8.append(order)
        elif order.stk_code == 9:
            order9.append(order)
        else:
            order10.append(order)


def tcplink(socket, addr):
    try:
        data = socket.recv(buffersize)  # 接收数据
        if data == 'exit' or not data:
            return
        pkg = data.decode('utf-8')
        print('receive:', pkg, 'from', addr)  # 打印接收到的数据
        process(pkg, socket)
        # socket.send(data)

        socket.send(data)
        # time.sleep(2)

    except Exception as e:
        print("exception occurred...", e, addr)


order1_buy = []
order1_sell = []
trade1 = []
last_order_id = 0
while True:
    client_socket1, client_addr1 = server1.accept()
    print("server1 have a new connection : connecting from:", client_addr1)
    # t = threading.Thread(target=tcplink, args=(client_socket1, client_addr1))
    # t.start()
    client_socket2, client_addr2 = server2.accept()
    print("server2 have a new connection : connecting from:", client_addr2)
    time_lapse = time.time()
    time_match = time.time()
    while True:
        tcplink(client_socket1, client_addr1)
        tcplink(client_socket2, client_addr2)
        if time.time() - time_lapse > 5:
            print("stk_1:", len(order1), "stk_2:", len(order2), "stk_3:", len(order3), "stk_4:",
                  len(order4), "stk_5:", len(order5), "stk_6:", len(order6), "stk_7:", len(order7),
                  "stk_8:", len(order8), "stk_9:", len(order9), "stk_10:", len(order10))
            print('order1_buy', len(order1_buy), 'order1_sell', len(order1_sell))
            time_lapse = time.time()
        # 隔3s撮合一次
        if time.time() - time_match > 3:
            # 要对数据进行撮合
            # 我们这些order列表的正式名字应该是叫
            # 集中申报簿：指交易主机某一时点有效竞价范围内按买卖方向以及价格优先、时间优先顺序排列的所有未成交申报队列
            # TODO 先处理股票1的，感觉可以用10个进程来跑
            # 处理申报之前，要看一下是否有序
            # 处理申报之前，按申请先后顺序排序
            order1.sort(key=lambda x: x.order_id)
            # 维护一个有序不断的序列，如2,3,4,5,
            temp_order1 = []
            temp_last_orderid = last_order_id
            index = 0
            for order in order1:
                if temp_last_orderid + 1 == order.order_id:
                    temp_order1.append(order)
                    temp_last_orderid += 1
                    index += 1
                else:
                    break
            # 更新last_order_id
            last_order_id = temp_last_orderid
            order1 = order1[index:]
            for order in temp_order1:
                # 两个榜单排一下序，买方价格降序，卖方价格升序，下面的逻辑会往这两个榜单里加数据
                order1_buy.sort(key=lambda x: (-x.price, x.order_id))
                order1_sell.sort(key=lambda x: (x.price, x.order_id))

                if order.type == 0:
                    # type=0 买入
                    if order.direction == 1:
                        # 如果卖方价格比我出的买价低，按照卖方的价格依次买入
                        while order1_sell and order1_sell[0].price <= order.price and \
                                order1_sell[0].volume <= order.volume:
                            trade1.append(
                                Trade(order.stk_code, order.order_id, order1_sell[0].order_id, order1_sell[0].price,
                                      order1_sell[0].volume))
                            # 减掉买到的份数
                            order.volume -= order1_sell[0].volume
                            # 卖一买完了，应该删掉了
                            order1_sell.pop(0)
                        # 如果卖方价格合适，并且比我的要求的份数多
                        if order1_sell and order1_sell[0].price <= order.price and order1_sell[
                            0].volume > order.volume:
                            trade1.append(
                                Trade(order.stk_code, order.order_id, order1_sell[0].order_id, order1_sell[0].price,
                                      order.volume))
                            # 更新卖一的剩余份数
                            order1_sell[0].volume -= order.volume
                            order.volume = 0
                        # 剩余没有买到的部分加到买方的限价单中, 可能是sell列表空，或者价格不合适
                        if order.volume > 0:
                            order1_buy.append(order)

                    # type=0 卖出
                    else:
                        # 如果买方价格比我出的卖价高，按照买方的价格依次买入
                        while order1_buy and order1_buy[0].price >= order.price and order1_buy[
                            0].volume <= order.volume:
                            trade1.append(
                                Trade(order.stk_code, order1_buy[0].order_id, order.order_id, order1_buy[0].price,
                                      order1_buy[0].volume))
                            # 减掉卖出的份数
                            order.volume -= order1_buy[0].volume
                            # 卖一买完了，应该删掉了
                            order1_buy.pop(0)
                        # 如果买方价格合适，并且比我的要求的份数多
                        if order1_buy and order1_buy[0].price >= order.price and order1_buy[
                            0].volume > order.volume:
                            trade1.append(
                                Trade(order.stk_code, order1_buy[0].order_id, order.order_id, order1_buy[0].price,
                                      order.volume))
                            # 更新买一的剩余份数
                            order1_buy[0].volume -= order.volume
                            order.volume = 0
                        # 剩余没有卖出的部分加到卖方的限价单中, 可能是buy列表空，或者价格不合适
                        if order.volume > 0:
                            order1_sell.append(order)
                # type=1 对手方最优价格申报
                elif order.type == 1:
                    # type1 买入时，以卖一为限价
                    if order.direction == 1:
                        if order1_sell:

                            temp = order1_sell[0]
                            cur_price = temp.price
                            while order1_sell and order1_sell[0].price == cur_price and order.volume > 0:
                                temp = order1_sell[0]
                                if order.volume <= temp.volume:
                                    trade1.append(
                                        Trade(order.stk_code, order.order_id, temp.order_id, temp.price,
                                              order.volume))
                                    temp.volume -= order.volume
                                    order.volume = 0
                                    break
                                else:

                                    trade1.append(
                                        Trade(order.stk_code, order.order_id, temp.order_id, temp.price,
                                              temp.volume))

                                    order.volume -= temp.volume
                                    order1_sell.pop(0)
                            # 剩余的部分按照卖一的价格挂在买方的限价单里
                            if order.volume > 0:
                                order.price = cur_price
                                order1_buy.append(order)

                    # type1 卖出时，以买一为限价
                    else:
                        if order1_buy:
                            temp = order1_buy[0]
                            cur_price = temp.price
                            while order1_buy and order1_buy[0].price == cur_price and order.volume > 0:
                                temp = order1_buy[0]
                                if order.volume <= temp.volume:
                                    trade1.append(
                                        Trade(order.stk_code, temp.order_id, order.order_id, temp.price,
                                              order.volume))
                                    temp.volume -= order.volume
                                    order.volume = 0
                                    break
                                else:
                                    # 把卖一买完
                                    trade1.append(
                                        Trade(order.stk_code, temp.order_id, order.order_id, temp.price,
                                              temp.volume))

                                    order.volume -= temp.volume
                                    order1_buy.pop(0)
                            # 剩余的部分按照买一的价格挂在卖方的限价单里
                            if order.volume > 0:
                                order.price = cur_price
                                order1_sell.append(order)
                # 本方最优价格申报
                elif order.type == 2:
                    if order.direction == 1:
                        if order1_buy:
                            order.price = order1_buy[0].price
                            order1_buy.append(order)
                    else:
                        if order1_sell:
                            order.price = order1_sell[0].price
                            order1_sell.append(order)

                # 最优五档即时成交剩余撤销申报
                elif order.type == 3:
                    # 买入要把卖一到卖五依次成交,未成交的部分撤销
                    if order.direction == 1:
                        price_set = set()

                        while order1_sell and len(price_set) <= 5:
                            price_set.add(order1_sell[0].price)
                            if order.volume >= order1_sell[0].volume:
                                trade1.append(
                                    Trade(order.stk_code, order.order_id, order1_sell[0].order_id, order1_sell[0].price,
                                          order1_sell[0].volume))
                                order.volume -= order1_sell[0].volume
                                order1_sell.pop(0)
                            else:
                                trade1.append(
                                    Trade(order.stk_code, order.order_id, order1_sell[0].order_id, order1_sell[0].price,
                                          order.volume))
                                order1_sell[0].volume-=order.volume
                                break
                    # 卖出要把买一到买五依次成交,未成交的部分撤销
                    else:
                        price_set = set()
                        while order1_buy and len(price_set) <= 5:
                            price_set.add(order1_buy[0].price)
                            if order.volume >= order1_buy[0].volume:
                                trade1.append(
                                    Trade(order.stk_code, order1_buy[0].order_id, order.order_id, order1_buy[0].price,
                                          order1_buy[0].volume))

                                order.volume -= order1_buy[0].volume
                                order1_buy.pop(0)
                            else:
                                trade1.append(
                                    Trade(order.stk_code, order1_buy[0].order_id, order.order_id, order1_buy[0].price,
                                          order.volume))
                                order1_buy[0].volume -= order.volume
                                break
                # 即时成交剩余撤销申报
                elif order.type == 4:
                    # 买入要把所有卖方的单子依次成交,未成交的部分撤销
                    if order.direction == 1:

                        while order1_sell:
                            if order.volume >= order1_sell[0].volume:
                                trade1.append(
                                    Trade(order.stk_code, order.order_id, order1_sell[0].order_id, order1_sell[0].price,
                                          order1_sell[0].volume))
                                order.volume -= order1_sell[0].volume
                                order1_sell.pop(0)
                            else:
                                trade1.append(
                                    Trade(order.stk_code, order.order_id, order1_sell[0].order_id, order1_sell[0].price,
                                          order.volume))
                                order1_sell[0].volume -= order.volume
                                break
                    # 卖出要把所有买方的单子依次成交,未成交的部分撤销
                    else:
                        while order1_buy:
                            if order.volume >= order1_buy[0].volume:
                                trade1.append(
                                    Trade(order.stk_code, order1_buy[0].order_id, order.order_id, order1_buy[0].price,
                                          order1_buy[0].volume))
                                order.volume -= order1_buy[0].volume
                                order1_buy.pop(0)

                            else:
                                trade1.append(
                                    Trade(order.stk_code, order1_buy[0].order_id, order.order_id, order1_buy[0].price,
                                          order.volume))
                                order1_buy[0].volume -= order.volume
                                break
                # 全额成交或撤销申报
                elif order.type == 5:

                    if order.direction == 1:
                        if not order1_sell:
                            continue
                        temp_trade1 = []
                        count = 0
                        for sell in order1_sell:
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
                            trade1 += temp_trade1
                            # 移除使用过的sell
                            while count > 0.5:
                                order1_sell.pop(0)
                                count -= 1

                    else:
                        if not order1_buy:
                            continue
                        temp_trade1 = []
                        count = 0
                        for buy in order1_buy:
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
                            trade1 += temp_trade1
                            # 移除使用过的sell
                            while count > 0.5:
                                order1_buy.pop(0)
                                count -= 1

                else:
                    print("unexpected order type:", order.type)

            for trade in trade1:
                client_socket1.send(getCmdPkg(trade))
                client_socket2.send(getCmdPkg(trade))
            trade1.clear()

            time_match = time.time()

client_socket1.close()
client_socket2.close()

server1.close()
server2.close()
