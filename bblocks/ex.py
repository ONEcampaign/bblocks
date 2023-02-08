import pandas as pd

# Create a dataframe with a column with iso3 codes for coutries and a value column which contains
# values which sometimes have text in them
df = pd.DataFrame(
    {
        "iso_code": [
            "USA",
            "GBR",
            "FRA",
            "DEU",
            "ITA",
            "ESP",
            "CAN",
            "JPN",
            "AUS",
            "CHN",
        ],
        "value": [
            "10%",
            "+12%",
            "13.4%",
            "%14.3",
            "15.3  %",
            "16%",
            "17%",
            "18%",
            "19%",
            "20%",
        ],
    }
)

from bblocks import clean_numeric_series

df['value'] = clean_numeric_series(data=df['value'], to=float) #or if dealing with integers, use to=int
