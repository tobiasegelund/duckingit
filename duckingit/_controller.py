"""
https://arrow.apache.org/docs/python/ipc.html
"""


class Controller:
    """The purpose of the controller is to control the invokations of
    serverless functions, e.g. Lambda functions.

    It invokes and collects the data, as well as concatenate it altogether before it's
    delivered to the user.

    TODO:
        - Incorporate cache functionality to minimize compute power.
        - Create Temp views of data?
    """

    pass
