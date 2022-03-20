import h5py
import struct
import time
import warnings
import numpy as np
from mpi4py import MPI
import npy_io

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
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
    order_id_path = "./100x100x100/order_id2.h5"
    direction_path = "./100x100x100/direction2.h5"
    price_path = "./100x100x100/price2.h5"
    volume_path = "./100x100x100/volume2.h5"
    type_path = "./100x100x100/type2.h5"
    # order_id_path = "/data/100x1000x1000/order_id2.h5"
    # direction_path = "/data/100x1000x1000/direction2.h5"
    # price_path = "/data/100x1000x1000/price2.h5"
    # volume_path = "/data/100x1000x1000/volume2.h5"
    # type_path = "/data/100x1000x1000/type2.h5"

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
    filename = 'order.npy'
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

    print("===============data parallel write start============")
    start = time.time()
    # parallel write local array to file, assume array is distributed on axis 1, i.e., column
    npy_io.parallel_write_array(filename, order_id_mtx, axis=1, comm=comm)
    print("===============data parallel write end============  cost ", time.time() - start)


    print("===============data load start============")
    start = time.time()
    print("===============data load end============  cost ", time.time() - start)

