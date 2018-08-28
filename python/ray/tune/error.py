from __future__ import absolute_import, division, print_function


class TuneError(Exception):
    """General error class raised by ray.tune."""
    pass


class TuneManagerError(TuneError):
    """Error raised in operating the Tune Manager."""
    pass
