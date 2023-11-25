try:
    from reloading import reloading
except ImportError:
    def reloading(fn_or_seq=None, every=1, forever=None):
        return fn_or_seq
