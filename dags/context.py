class Context:
    def __init__(self, df, results=None):
        self.df = df
        self.results = results or {}