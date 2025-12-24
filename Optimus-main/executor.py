import pandas as pd
import time


df = pd.read_csv('snapshot.csv')


capital_size = 10_000 
max_position_pct = 0.20
take_profit = 0.03
stop_loss = 0.02
broker_fee = 0.0075

symbol_list = df['symbol']
price_list = df['price']


def position_allocation():
    length = len(symbol_list)

    if length ==0:
        return 0
    size = capital_size / length
    
    max_allowed = capital_size * max_position_pct

    if size > max_allowed:
        print(f"⚠️  Position size ${size:.2f} exceeds max ${max_allowed:.2f}")
        print(f"   Limiting to ${max_allowed:.2f} per position")
        size = max_allowed
    return size

result = position_allocation()
print(result)

def calculate_quantities():
    position_size = position_allocation()

    if position_size ==0: 
        print("no coins for allocation")
    

    for i in range(len(symbol_list)):
        symbol = symbol_list.iloc[i]
        price = price_list.iloc[i]
        quantity = position_size / price
        print(f"{symbol:12} | ${price:>10.2f} | {quantity:>12.6f} coins | ${position_size:>10,.2f}")

calculate_quantities()

quant = calculate_quantities()
print(quant)

def calculate_exis_levels():
    position_size = position_allocation()

    for i in range(len(symbol_list)):
        symbol = symbol_list.iloc[i]
        entry_price = price_list.iloc[i]

        tp = entry_price * (1 + take_profit)
        sl = entry_price * (1 - stop_loss)

        print(f"{symbol:12} | Entry: ${entry_price:>10.2f} | TP: ${tp:>10.2f} (+3%) | SL: ${sl:>10.2f} (-2%)")

exit_levels = calculate_exis_levels()
print(exit_levels)