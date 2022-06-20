from operator import add
from pyspark import SparkContext
from StockTick import StockTick
import pyspark.sql.udf
import datetime
import org.apache.hadoop
import pyspark.sql.dataframe
from pyspark.sql import SparkSession


def maxValuesReduce(a, b):
    price = max(a.price, b.price)
    bid = max(a.bid, b.bid)
    ask = max(a.ask, b.ask)
    return StockTick(price=price, bid=bid, ask=ask)


def minValuesReduce(a, b):
    price = min(a.price, b.price)
    bid = min(a.bid, b.bid)
    ask = min(a.ask, b.ask)
    return StockTick(price=price, bid=bid, ask=ask)


def generateSpreadsDailyKeys(tick):  ### TODO: Write Me (see below)
    date = tick.date
    key_date = datetime.date(int(date[6:10]), int(date[0:2]), int(date[3:5])).isoformat()
    ask = tick.ask
    bid = tick.bid
    spread = (ask - bid) / (2 * (ask + bid))
    return key_date, (spread, 1)


def generateSpreadsHourlyKeys(tick):  ### TODO: Write Me (see below)
    date = tick.date
    time = tick.time
    key_date = datetime.date(int(date[6:10]), int(date[0:2]), int(date[3:5])).isoformat()
    time = datetime.time(int(time[:2]), int(time[3:5])).hour
    ask = tick.ask
    bid = tick.bid
    spread = (ask - bid) / (2 * (ask + bid))
    return (date, time), (spread, 1)


def spreadsSumReduce(a, b):
    return a[0] + b[0], a[1] + b[1]


if __name__ == "__main__":
    """
    Usage: stock
    """
    sc = SparkContext(appName="StockTick")
    spark = pyspark.sql.SparkSession.builder.getOrCreate()

    # rawTickData is a Resilient Distributed Dataset (RDD)
    rawTickData = sc.textFile("/WDC_bidask1min.txt")

    tickData = rawTickData.map(lambda x: StockTick(x))
    goodTicks = tickData.filter(lambda x: x.date is not None and x.time is not None and x.price > 0 and x.bid > 0 and x.ask > 0)
    # x = goodTicks.collect()
    # for y in x:
    #     print(y.printing())

    ### TODO: store goodTicks in the in-memory cache
    goodTicks.cache()

    numTicks = goodTicks.count()

    sum_values = goodTicks.reduce(
        lambda a, b: StockTick(date=" ", time=" ", price=a.price + b.price, bid=a.bid + b.bid, ask=a.ask + b.ask))
    print(sum_values.reducePrints())

    avgDailySpreads_1 = goodTicks.map(lambda x: generateSpreadsDailyKeys(x)).reduceByKey(spreadsSumReduce) \
        .mapValues(lambda x: x[0] / x[1]).sortByKey(ascending=True)
    avgDailySpreads_1.saveAsSequenceFile("/WDC_DAILY")

    avgHourlySpreads = goodTicks.map(lambda x: generateSpreadsHourlyKeys(x)).reduceByKey(spreadsSumReduce) \
        .mapValues(lambda x: x[0] / x[1]).sortByKey(ascending=True)

    avgHourlySpreads.saveAsSequenceFile("/WDC_HOURLY")

    spark.sparkContext.sequenceFile('/WDC_DAILY').toDF(['date','spread']).show()