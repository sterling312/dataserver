from tserver import *

handlers = [
            (r'/',Main),
            (r'/orm',DBAPI),
            ]
