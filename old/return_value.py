class ReturnValue:

    def __init__(self, value, ok=True, errmsg=""):
        self.value  = value
        self.ok     = ok
        self.errmsg = errmsg
