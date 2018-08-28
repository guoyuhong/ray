from __future__ import absolute_import, division, print_function

import numpy as np

import ray


@ray.remote
def normal(shape):
    return np.random.normal(size=shape)
