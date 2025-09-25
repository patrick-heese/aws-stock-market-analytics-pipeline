import boto3
import json
import decimal
import os
from datetime import datetime, timedelta
from boto3.dynamodb.conditions import Key

# AWS clients
dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")

# From SAM template (Globals -> Environment -> Variables)
TABLE_NAME = os.environ["TABLE_NAME"]
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]


def get_recent_stock_data(symbol: str, minutes: int = 5):
    """
    Fetch stock data for the last 'minutes' from DynamoDB using Query on the PK/SK.
    Uses Key(...) for KeyConditionExpression (required for keys).
    """
    table = dynamodb.Table(TABLE_NAME)
    now = datetime.utcnow()
    past_time = now - timedelta(minutes=minutes)

    try:
        resp = table.query(
            KeyConditionExpression=(
                Key("symbol").eq(symbol) &
                Key("timestamp").gte(past_time.strftime("%Y-%m-%d %H:%M:%S"))
            ),
            # True = ascending (oldest -> newest). Set False if you want newest first.
            ScanIndexForward=True
        )
        items = resp.get("Items", [])
        # Ensure chronological order, just in case:
        items.sort(key=lambda x: x["timestamp"])
        return items
    except Exception as e:
        print(f"Error fetching stock data: {e}")
        return []


def calculate_moving_average(data, period: int) -> decimal.Decimal:
    """Calculate simple moving average for given period."""
    if len(data) < period:
        return decimal.Decimal("0")
    # Convert to Decimal to be safe if types vary
    return sum(decimal.Decimal(str(d["price"])) for d in data[-period:]) / period


def lambda_handler(event, context):
    symbols = ["AAPL"]  # Add more symbols if desired

    for symbol in symbols:
        stock_data = get_recent_stock_data(symbol)

        if len(stock_data) < 20:  # Need enough points for SMA-20
            continue

        # Current SMAs
        sma_5 = calculate_moving_average(stock_data, 5)
        sma_20 = calculate_moving_average(stock_data, 20)

        # Previous SMAs (exclude the latest point)
        sma_5_prev = calculate_moving_average(stock_data[:-1], 5)
        sma_20_prev = calculate_moving_average(stock_data[:-1], 20)

        message = None
        if sma_5_prev < sma_20_prev and sma_5 > sma_20:
            message = f"{symbol} is in an **Uptrend**! Consider a buy opportunity."
        elif sma_5_prev > sma_20_prev and sma_5 < sma_20:
            message = f"{symbol} is in a **Downtrend**! Consider selling."

        if message:
            try:
                sns.publish(
                    TopicArn=SNS_TOPIC_ARN,
                    Message=message,
                    Subject=f"Stock Alert: {symbol}"
                )
            except Exception as e:
                print(f"Failed to publish SNS message: {e}")

    return {"statusCode": 200, "body": json.dumps("Trend analysis complete")}
