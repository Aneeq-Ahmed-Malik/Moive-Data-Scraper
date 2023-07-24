from bs4 import BeautifulSoup
import requests
import numpy as np
import pandas as pd
import concurrent.futures
import os

headers = {
"User-Agent":os.environ.get("User-Agent"),
"Accept-Language": os.environ.get("Accept-Language")
}


URL = "https://www.the-numbers.com/movie/budgets/all/"


a = np.array([["Rank", "Release Date", "Movie", "Production Budget", "Domestic Gross", "Worldwide Gross"]])


class DataBot:
    def __init__(self):

        self.data = []

    def get_data(self, pages):
        for page in pages:
            content = requests.get(URL + str(page), headers=headers).text
            soup = BeautifulSoup(content, "html.parser")

            table = soup.find(name="table")
            all_rows = table.find_all_next(name="tr")[1:]

            for row in all_rows:
                columns = row.text.strip().split("\n")
                columns = [column.replace("\xa0", "") for column in columns]
                self.data.append(columns)



bots = []
for _ in range(0, 5):
    bots.append(DataBot())

# Total Number of Pages
pages = np.arange(start=1, stop=6501, step=100)

# To Boost Up the speed of data extraction
with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    for bot, page in zip(bots, np.array_split(pages, 5)):
        executor.submit(bot.get_data, page)

# Getting data from all bots and merging it
for bot in bots:
    a = np.append(a, bot.data, axis=0)

# Saving Data to csv
df = pd.DataFrame(a)
df.to_csv("MovieData.csv", index=False, header=False)
