import alpaca_trade_api as tradeapi
import time
import requests
import math

# == Exceptions == 
class AlpacaDataFetcherException(Exception):
    def __init__(self, message):            
        super().__init__(message)

class PlaceOrderException(Exception):
    def __init__(self, message):            
        super().__init__(message)

class TradingLimitsExceeded(Exception):
    def __init__(self, message):            
        super().__init__(message)

class AlpacaDataFetcher:
    def __init__(self, api, api_key, api_secret, symbol):
        self.api_key = api_key
        self.api_secret = api_secret
        self.symbol = symbol
        self.api = api
        self.headers = {
            "accept": "application/json",
            "APCA-API-KEY-ID": api_key,
            "APCA-API-SECRET-KEY": api_secret
        }
        self.prices_fetched = 0

    def fetch_latest_bars(self):
        """
        Fetch most recent minbar data for the specified symbol.
        Returns:
            Latest bars data.
        """
        url = f"https://data.alpaca.markets/v2/stocks/bars/latest?symbols={self.symbol}&feed=iex"
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            data = response.json()
            return data['bars'][self.symbol]
        else:
            raise AlpacaDataFetcherException(f"Failed to fetch data: {response.status_code}")

class DualEWMASignal:
    # class to generate trading signals based on dual EWMA crossover strategy
    def __init__(self, short_ewma_span, long_ewma_span):
        """
        Initialize dual EWMA crossover strategy.
        Params:
            long_ewma_span: Specify fast decay in terms of span
            short_ewma_span: Specify short decay in terms of span
        """
        self.fast_ewma = None
        self.slow_ewma = None
        self.alpha_fast = 2 / (long_ewma_span + 1)
        self.alpha_slow = 2 / (short_ewma_span + 1)

    def update(self, price):
        """
        Update fast and slow EWMA and generate trading signals.
        Params:
            price: New price data point.
        Returns: 
            Trading signal (-1 = Sell, 0 = Hold, 1 = Buy).
        """
        # fast EWMA
        if self.fast_ewma is None:
            self.fast_ewma = price
        else:
            self.fast_ewma = (self.alpha_fast * price) + ((1 - self.alpha_fast) * self.fast_ewma)

        # slow ewma
        if self.slow_ewma is None:
            self.slow_ewma = price
        else:
            self.slow_ewma = (self.alpha_slow * price) + ((1 - self.alpha_slow) * self.slow_ewma)

        # Generate trading signals
        if self.fast_ewma > self.slow_ewma:
            return 1 # buy
        elif self.fast_ewma < self.slow_ewma:
            return -1 # sell
        else:
            return 0  # hold

class AlpacaTrader:
    def __init__(self, api):
        self.api = api
        self.positions = 0

    def get_order_price(self, order_id):
        """
        Get the average filled price of an order.
        Params:
            order: Order object.
        Returns: 
            Average filled price.
        """
        try:
            order_status = self.api.get_order(order_id)
            if order_status.status == 'filled':
                return float(order_status.filled_avg_price)
        except:
            raise AlpacaDataFetcherException("Failed to get order price.")

    def place_buy_order(self, symbol, qty=None, latest_close_price=None):
        """
        Place a buy (market) order for the specified symbol and quantity.
        Params:
            symbol: Stock symbol to buy.
            qty: Quantity of stock to buy, if not specified, calculate based on buying power.
        """
        if qty is None:                    
            buying_power = float(self.api.get_account().buying_power)
            qty = int((0.2*buying_power) // latest_close_price)
            if qty == 0:
                raise TradingLimitsExceeded("Insufficient buying power.")
        try:
            self.api.submit_order(
                symbol=symbol,
                qty=qty,
                side='buy',
                type='market',
                time_in_force='gtc'
            )
        except:
            raise PlaceOrderException("Failed to place buy order.")
        else:
            self.positions += qty

    def place_sell_order(self, symbol, qty=None):
        """
        Place a sell (market) order for the specified symbol and quantity.
        Params:
            symbol: Stock symbol to sell.
            qty: Quantity of stock to sell, if not specified, calculate based on current positions.
        """
        if qty is None:
            qty = math.floor(0.2*self.positions)
        try:
            self.api.submit_order(
                symbol=symbol,
                qty=qty,
                side='sell',
                type='market',
                time_in_force='gtc'
            )
        except:
            raise PlaceOrderException("Failed to place sell order.")
        else:
            self.positions -= qty

    def eod_liquidate(self, symbol):
        """
        Liquidate all positions at the end of the day.
        """
        if self.positions > 0:
            self.place_sell_order(symbol=symbol, qty=self.positions)


if __name__ == '__main__':
    # API credentials
    API_KEY = '...' # excluded for security
    API_SECRET = '...' # excluded for security
    BASE_URL = 'https://paper-api.alpaca.markets'
    API = tradeapi.REST(API_KEY, API_SECRET, base_url=BASE_URL, api_version='v2')

    # Trading parameters
    SYMBOL = 'TSLA'
    SHORT_EWMA_SPAN = 5
    LONG_EWMA_SPAN = 10

    # Initialize trading objects
    fetcher = AlpacaDataFetcher(API, API_KEY, API_SECRET, SYMBOL)
    signal_generator = DualEWMASignal(short_ewma_span=SHORT_EWMA_SPAN, long_ewma_span=LONG_EWMA_SPAN)
    trader = AlpacaTrader(API)

    while True:
        try:
            # Get Current time
            current_time = time.localtime()
            # Liquidate all positions at the end of the day
            if (current_time.tm_hour == 14) and (current_time.tm_min >= 55):
                trader.eod_liquidate(SYMBOL)
                break
            else:
                # Get the last close price
                latest_bars = fetcher.fetch_latest_bars()
                if latest_bars:
                    # generate signals/update ewma
                    fetcher.prices_fetched += 1
                    latest_close_price = latest_bars['c']
                    print(f"Latest close price: {latest_close_price}")
                    signal = signal_generator.update(latest_close_price)
                    print(f"Fast EWMA: {signal_generator.fast_ewma}, Slow EWMA: {signal_generator.slow_ewma}")
                    # Wait for enough data to calculate the EWMA and generate signals
                    if (fetcher.prices_fetched >= LONG_EWMA_SPAN):
                        # Execute trades based on signals
                        if signal == 1:
                            # Place Buy order
                            trader.place_buy_order(symbol = SYMBOL, latest_close_price=latest_close_price)
                            print("Buy order placed at {}".format(latest_close_price))
                        if signal == -1:
                            # Place Sell order
                            if trader.positions > 0:
                                trader.place_sell_order(symbol = SYMBOL)
                                print("Sell order placed at {}".format(latest_close_price))

            # wait for next minbar
            time.sleep(62)

        except (AlpacaDataFetcherException, PlaceOrderException, TradingLimitsExceeded) as e:
            print(e)

        except Exception:
            print("An unexpected error occurred.")
