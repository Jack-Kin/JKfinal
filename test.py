import os
import numpy as np
import h5py
from mpi4py import MPI


comm = MPI.COMM_WORLD
rank = comm.rank
size = comm.size

order_id_path = "./100x100x100/order_id2.h5"
direction_path = "./100x100x100/direction2.h5"
price_path = "./100x100x100/price2.h5"
volume_path = "./100x100x100/volume2.h5"
type_path = "./100x100x100/type2.h5"


print("Hello World (from process %d)"%rank)
