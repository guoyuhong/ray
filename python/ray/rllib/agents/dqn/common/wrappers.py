from __future__ import absolute_import, division, print_function

from ray.rllib.env.atari_wrappers import wrap_deepmind
from ray.rllib.models import ModelCatalog


def wrap_dqn(env, options):
    """Apply a common set of wrappers for DQN."""

    is_atari = hasattr(env.unwrapped, "ale")

    # Override atari default to use the deepmind wrappers.
    # TODO(ekl) this logic should be pushed to the catalog.
    if is_atari and "custom_preprocessor" not in options:
        return wrap_deepmind(env, dim=options.get("dim", 84))

    return ModelCatalog.get_preprocessor_as_wrapper(env, options)
