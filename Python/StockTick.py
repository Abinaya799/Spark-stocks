class StockTick:
    def __init__(self, text_line=None, date=None, time=None, price=0.0, bid=0.0, ask=0.0):

        if text_line is not None:
            tokens = text_line.split(",")
            self.date = tokens[0]
            self.time = tokens[1]
            try:
                self.price = float(tokens[2])
                self.bid = float(tokens[3])
                self.ask = float(tokens[4])

            except:

                self.price = 0.0
                self.bid = 0.0
                self.ask = 0.0

        else:
            self.date = date
            self.time = time

            self.price = price
            self.bid = bid
            self.ask = ask

    def printing(self):
        return f"{self.date}, {self.time}, {self.price}, {self.bid}, {self.ask}"

    def reducePrints(self):
        return f"{self.price}, {self.bid}, {self.ask}"
