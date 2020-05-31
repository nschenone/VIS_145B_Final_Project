#!/usr/bin/env python

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from time import sleep
from pythonosc import osc_message_builder
from pythonosc import udp_client

# Start OSC connection
client = udp_client.UDPClient("127.0.0.1", 8899)

# Get data
# ----------------------------------------------
# Electricity prices from 1980 - 2020
elec_df = pd.read_excel("data/Electricity.xlsx", header=9, index_col=0)
elec_df = elec_df[elec_df.index >= 1980]

# Gas prices from 1980 - 2020
gas_df = pd.read_excel("data/Gasoline.xlsx", header=9, index_col=0)
gas_df = gas_df[gas_df.index >= 1980]

# White Bread prices from 1980 - 2020
bread_df = pd.read_excel("data/WhiteBread.xlsx", header=9, index_col=0)
bread_df = bread_df[bread_df.index >= 1980]

# Unemployment Rate from 1980 - 2020
unemp_df = pd.read_excel("data/Unemployment.xlsx", header=11, index_col=0)
unemp_df = unemp_df[unemp_df.index >= 1980]

# Hourly Earnings rate from 1980 - 2020
earn_df = pd.read_excel("data/HourlyEarnings.xlsx", header=19)
earn_df = earn_df.drop(["Unnamed: 2", "Unnamed: 3", "Unnamed: 4", "Unnamed: 5"], axis=1)
earn_df = earn_df.append(
    {"Year": 2020, "Annual": list(earn_df[-1:]["Annual"])[0]}, ignore_index=True
)
earn_df = earn_df.set_index("Year")

# Preprocess and convert data
# ----------------------------------------------
# Date range (1980 - 2020)
date_range = np.arange(1980, 2021)

# Convert dataframe with months and years to single dimensional list
def dataframe_to_list(df, date_range):
    vals = []
    for date in date_range:
        vals.extend(df.T[date].ravel())
    vals = np.array(vals)
    vals = vals[np.isfinite(vals)]
    return list(vals)


# Convert all data to lists
elec_list = dataframe_to_list(elec_df, date_range)
gas_list = dataframe_to_list(gas_df, date_range)
bread_list = dataframe_to_list(bread_df, date_range)
unemp_list = dataframe_to_list(unemp_df, date_range)
earn_list = dataframe_to_list(earn_df, date_range)

# Convert earnings list into same resolution as other lists
new_earn = []
for earning in earn_list:
    for i in range(12):
        new_earn.append(earning)
earn_list = new_earn[:-8]

# Convert date list into same resolution as other lists
new_date_range = []
for date in date_range:
    for i in range(12):
        new_date_range.append(date)
date_range = new_date_range[:-8]

# Pad last element of elec list
elec_list.append(elec_list[-1])

# Normalize all lists
elec_list_norm = elec_list / max(elec_list)
gas_list_norm = gas_list / max(gas_list)
bread_list_norm = bread_list / max(bread_list)
unemp_list_norm = unemp_list / max(unemp_list)
earn_list_norm = earn_list / max(earn_list)

# Compile back into dataframe to stream into OSC all at once
df_osc = pd.DataFrame(
    [
        elec_list,
        gas_list,
        bread_list,
        unemp_list,
        earn_list,
        elec_list_norm,
        gas_list_norm,
        bread_list_norm,
        unemp_list_norm,
        earn_list_norm,
        date_range,
    ]
).T
df_osc.columns = [
    "elec",
    "gas",
    "bread",
    "unemp",
    "earn",
    "elec_norm",
    "gas_norm",
    "bread_norm",
    "unemp_norm",
    "earn_norm",
    "year",
]
df_osc = df_osc.dropna()

# Send data
# ----------------------------------------------
def send_osc(value, address, arg_type="f"):
    print(f"OSC Sending {value} to {address}")
    msg = osc_message_builder.OscMessageBuilder(address=f"/{address}")
    msg.add_arg(value, arg_type=arg_type)
    message = msg.build()
    client.send(message)


# Send OSC messages to Max
for i, row in df_osc.iterrows():
    # Send OSC
    send_osc(row[0], "elec")
    send_osc(row[1], "gas")
    send_osc(row[2], "bread")
    send_osc(row[3], "unemp")
    send_osc(row[4], "earn")
    send_osc(row[5], "elec_norm")
    send_osc(row[6], "gas_norm")
    send_osc(row[7], "bread_norm")
    send_osc(row[8], "unemp_norm")
    send_osc(row[9], "earn_norm")
    send_osc(int(row[10]), "year", "i")

    # Sleep for 0.2 seconds
    sleep(0.2)
