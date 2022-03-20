import h5py
import struct
import time

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


if __name__ == '__main__':
    # order_id_path = "./data/100x100x100/order_id2.h5"
    # direction_path = "./data/100x100x100/direction2.h5"
    # price_path = "./data/100x100x100/price2.h5"
    # volume_path = "./data/100x100x100/volume2.h5"
    # type_path = "./data/100x100x100/type2.h5"
    order_id_path = "/data/100x1000x1000/order_id2.h5"
    direction_path = "/data/100x1000x1000/direction2.h5"
    price_path = "/data/100x1000x1000/price2.h5"
    volume_path = "/data/100x1000x1000/volume2.h5"
    type_path = "/data/100x1000x1000/type2.h5"

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
    filename1 = 'queue1.npy'
    filename2 = 'queue2.npy'
    filename3 = 'queue3.npy'
    filename4 = 'queue4.npy'
    filename5 = 'queue5.npy'
    filename6 = 'queue6.npy'
    filename7 = 'queue7.npy'
    filename8 = 'queue8.npy'
    filename9 = 'queue9.npy'
    filename10 = 'queue10.npy'
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

                order = SimpleOrder(i, j, k, order_id_mtx[i, j, k],
                                    type_mtx[i, j, k], price_mtx[i, j, k],
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
