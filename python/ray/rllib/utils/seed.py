from __future__ import absolute_import, division, print_function

import random

import numpy as np
import tensorflow as tf


def seed(np_seed=0, random_seed=0, tf_seed=0):
    np.random.seed(np_seed)
    random.seed(random_seed)
    tf.set_random_seed(tf_seed)
