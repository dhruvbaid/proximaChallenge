import asyncio
import websockets
import requests
import ast
import math
from collections import deque, namedtuple, defaultdict

class orderBook:
    def __init__(self, name, max_price = 100000, start_order_id = 0):
        """
        Class representing a single order book

        name          : asset name (in this case, BNBBTC)
        max_price     : maximum price
        start_order_id: first number to use for next order
        """
        self.name = name
        self.order_id = start_order_id
        self.max_price = max_price
        self.book = defaultdict(lambda: deque())
        self.bookDepth = defaultdict(lambda: 0)
        self.bid_max = 0
        self.ask_min = max_price + 1

    # update_order : I assume this is what we are supposed to do with the events
    #                we stream from the binance site - update bids and asks to
    #                the quantities we are given, rather than actually placing
    #                orders.
    def update_book(self, side, px, qty):
        # Delete order if needed
        if float(qty) == 0:
            # Check if order book contains any volume at this price level
            if float(px) in self.book:
                del self.book[float(px)]

                # Update best bid/ask if needed
                prices = sorted(list(self.book))
                if ((side == 'buy') and (px == self.bid_max)):
                    if self.book[prices[-1]][0] == 'buy':
                        self.bid_max = prices[-1]
                    else:
                        for i in range(len(prices)):
                            if self.book[prices[i]][0] == 'sell':
                                if i == 0:
                                    self.bid_max = 0
                                else:
                                    self.bid_max = prices[i-1]
                if ((side == 'sell') and (px == self.ask_min)):
                    if self.book[prices[0]][0] == 'sell':
                        self.ask_min = prices[0]
                    else:
                        for i in range(len(prices) - 1, -1, -1):
                            if self.book[prices[i]][0] == 'buy':
                                if i == len(prices) - 1:
                                    self.ask_min = math.inf
                                else:
                                    self.ask_min = prices[i + 1]
        # Modify order (always possible)
        else:
            self.book[float(px)] = (side, float(qty))

            # Update best bid/ask accordingly
            if side == 'buy':
                self.bid_max = max(self.bid_max, float(px))
            elif side == 'sell':
                self.ask_min = min(self.ask_min, float(px))
            else:
                print("Order side must be either 'buy' or 'sell'!")
                return


DEPTH_API_URL = "https://api.binance.com/api/v3/depth?symbol=BNBBTC&limit=1000"
STREAMING_URL = "wss://stream.binance.com:9443/ws/bnbbtc@depth"


# stream : 1. Obtain the depth message (lastUpdateId) before streaming events.
#               I assume that is what we had to do, because the alternative was
#               constantly (in the while loop) obtaining a new lastUpdateId as
#               well as streaming the events, but when I did that, the events'
#               "u" fields never passed the criterion listed in the Binance
#               documentation.
#          2. If the criterion listed in the Binance documentation (relating to
#             update time and lastUpdateId) is met, process this message
async def stream(uri, orderSize):
    # Set firstRun flag
    firstRun = True
    
    # Connect to streaming websocket
    async with websockets.connect(uri) as websocket:
        while True:
            # On first run, obtain lastUpdateId and initialize orderbook based
            # on depth message
            if firstRun:
                # Get depth message and convert it to a dict
                depthMsg = ast.literal_eval(requests.get(DEPTH_API_URL).text)

                # Get lastUpdateId
                lastUpdateId = int(depthMsg["lastUpdateId"])
                
                # Get current list of bids and asks
                bids = [(float(px), float(qty)) for px, qty in depthMsg["bids"]]
                asks = [(float(px), float(qty)) for px, qty in depthMsg["asks"]]

                # Create empty orderbook
                myOB = orderBook("BNBBTC")

                # Populate orderbook with current bids and asks (from depth
                # message)
                for (px, qty) in bids:
                    myOB.update_book('buy', px, qty)
                for (px, qty) in asks:
                    myOB.update_book('sell', px, qty)

                # Set firstRun flag so that lastUpdateId is not recomputed
                firstRun = False
                
                # Obtain the message from the websocket connection
                msg = await websocket.recv()

                # Convert the (string) message into a dictionary
                msg = ast.literal_eval(msg)
            
                # Check lastUpdateId condition
                if int(msg["u"]) > lastUpdateId:
                    myOB = processMsg(msg, myOB, orderSize)

            else:
                # Obtain the message from the websocket connection
                msg = await websocket.recv()

                # Convert the (string) message into a dictionary
                msg = ast.literal_eval(msg)
            
                # Check lastUpdateId condition
                if int(msg["u"]) > lastUpdateId:
                    myOB = processMsg(msg, myOB, orderSize)
        return


# processMsg : given a message, update the input orderBook and then output the
#              average bid and ask prices for the input order size
def processMsg(msg, orderBook, orderSize):
    # Update order book
    if "b" in msg:
        for [update_px, update_qty] in msg["b"]:
            orderBook.update_book('buy', update_px, update_qty)
    if "a" in msg:
        for [update_px, update_qty] in msg["a"]:
            orderBook.update_book('sell', update_px, update_qty)

    # Get list of bids/asks
    bids = []
    asks = []
    prices = sorted(list(orderBook.book))
    pLen = len(prices)
    i = 0
    while((orderBook.book[prices[i]][0] == 'buy') and (i < pLen)):
        bids.append((prices[i], orderBook.book[prices[i]][1]))
        i += 1
    while i < pLen:
        asks.append((prices[i], orderBook.book[prices[i]][1]))
        i += 1
    
    originalOrderSize = orderSize

    # Assume order is bid to compute average bid price
    if sum([i for (_, i) in bids]) >= originalOrderSize:
        total = 0
        i = -1
        while orderSize >= 0:
            if orderSize > bids[i][1]:
                total += bids[i][0] * bids[i][1]
                orderSize -= bids[i][1]
                i -= 1
            else:
                total += bids[i][0] * min(bids[i][1], orderSize)
                break
        bidAvg = f"Bid Avg = {total/originalOrderSize}"
    else:
        bidAvg = "Insufficient bids"
    
    # Reset order size for computations
    orderSize = originalOrderSize

    # Assume order is ask to copmute average ask price
    if sum([i for (_, i) in asks]) >= originalOrderSize:
        total = 0
        i = 0
        while orderSize >= 0:
            if orderSize > asks[i][1]:
                total += asks[i][0] * asks[i][1]
                orderSize -= asks[i][1]
                i += 1
            else:
                total += asks[i][0] * min(asks[i][1], orderSize)
                break
        askAvg = f"Ask Avg = {total/originalOrderSize}"
    else:
        askAvg = "Insufficient asks"

    # Reset order size for printing
    orderSize = originalOrderSize
    print(f"For order size {orderSize}, {bidAvg}, {askAvg}", end = "\r")
    return orderBook


# main : main function which will call appropriate helpers
async def main():
    # Obtain input order size (allows floats)
    orderSize = float(input("Enter order size: "))

    # Perform computations (until manual termination, i.e. Ctrl + C)
    await stream(STREAMING_URL, orderSize)


# Run main()
asyncio.run(main())
