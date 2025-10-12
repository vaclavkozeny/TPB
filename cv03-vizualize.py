import orjson
import time
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt
import polars as pl

def load_jsonl_fast(file_path):
    return pl.read_ndjson(file_path)
    
def analyze_all_fast(df: pl.DataFrame):
    # Vyextrahujeme rok z ISO timestampu
    df = df.with_columns(
        pl.col("time")
        .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S", strict=False)
        .dt.year()
        .alias("year")
    )
    df = df.with_columns(
        pl.col("time")
        .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S", strict=False)
        .dt.date()
        .alias("date")
    )
    # Filtrování podle 'disc'
    df = df.filter(pl.col("disc") <= 13000)
    # Počet slov ve článku
    df = df.with_columns(
        pl.col("content").str.split(" ").list.len().alias("length")
    )
    # Počet článků za rok
    articles_per_year = (
        df.group_by("year").len().sort("year").to_dict(as_series=False)
    )
    articles_per_year_list = dict(zip(articles_per_year["year"], articles_per_year["len"]))
    # Počet článků podle kategorií
    articles_per_category = (
        df.select(pl.col("tags"))
        .explode("tags")
        .group_by("tags")
        .len()
        .sort("len", descending=True)
        .to_dict(as_series=False)
    )
    # Počet slov ve článcích
    df = df.with_columns(
        pl.col("content").str.split(" ").list.len().alias("word_count")
    )
    # Délka slov
    df = df.with_columns(
        pl.col("content")
        .str.split(" ") 
        .list.eval(pl.element().str.len_chars()) 
        .alias("word_lengths")
    )
    # Časová osa kovidu a vakcíny
    covid_pattern = ["covid", "covid-19", "covid19", "koronavirus", "korona", "covidu", "covidu-19", "koronaviru", "koronavirusu", "kovid", "kovid-19"]
    vaccine_pattern = ["vakcína", "vakcíny", "vakcinace"]

    covid_regex = "|".join(covid_pattern) 
    vaccine_regex = "|".join(vaccine_pattern) 

    df = df.with_columns(
        pl.col("title")
        .str.contains(covid_regex)
        .cast(pl.Int32)
        .alias("covid_count"),
        pl.col("title")
        .str.contains(vaccine_regex)
        .cast(pl.Int32)
        .alias("vaccine_count")
    )
    # Seskupení podle year_month
    
    timeline = (
        df.with_columns(
        pl.col("date").dt.strftime("%Y-%m").alias("year_month")
        )
        .group_by("year_month")
        .agg(
            covid_total=pl.col("covid_count").sum(),
            vaccine_total=pl.col("vaccine_count").sum()
        )
        .sort("year_month")
    ) 
    # Články v jednotlivých dnech
    df = df.with_columns(
        pl.col("time")
        .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S", strict=False)
        .dt.weekday()
        .alias("weekday")
    )
    articles_per_weekday = (
        df.group_by("weekday").len().sort("weekday").to_dict(as_series=False)
    )
    # Převod na slovníky
    articles_per_weekday_dict = dict(zip(articles_per_weekday["weekday"], articles_per_weekday["len"]))
    covid_dict = dict(zip(timeline["year_month"], timeline["covid_total"]))
    vaccine_dict = dict(zip(timeline["year_month"], timeline["vaccine_total"]))

    articles_per_category = dict(zip(articles_per_category["tags"], articles_per_category["len"]))
    # Výsledné listy
    length = df["length"].to_list()
    coms = df["disc"].to_list()
    #df.write_ndjson("data-for-mongo.jsonl")
    return articles_per_year_list, length, coms, articles_per_category, df["word_count"].to_list(), df.select(pl.col("word_lengths").explode())["word_lengths"].to_list(), covid_dict, vaccine_dict, articles_per_weekday_dict


def articles_added_over_time(data):
    data = sorted(data.items())
    year_articles = {}
    count = 0
    for d in data:
        year_articles[d[0]] = count + d[1]
        count = count + d[1]
    return year_articles

def main():
    start = time.time()
    data = load_jsonl_fast("./jsons/articles-clear.jsonl")

    g2,g3_l,g3_c,g4,g5,g6,g7_c,g7_v,g8 = analyze_all_fast(data)
    
    g1 = articles_added_over_time(g2)
    x = list(g1.keys())  # Convert dict_keys to list
    y = list(g1.values())  # Also convert values to list for consistency
    plt.subplot(3,3,1)
    plt.plot(x,y)
    plt.xticks(x,rotation=45)
    plt.title("Články kumulativně")  # nebo vlastní seznam hodnot

    plt.subplot(3,3,2)
    plt.bar(*zip(*g2.items()))
    plt.xticks(list(g2.keys()),rotation=45) 
    plt.title("Články podle roků")
    plt.subplot(3,3,3)
    plt.scatter(g3_l,g3_c)
    plt.title("Diskuze X délka článku")
    plt.subplot(3,3,4)
    sorted_items = sorted(g4.items(), key=lambda item: item[1], reverse=True)

    # Top 10 položek
    top_n = 5
    top_list = sorted_items[:top_n]
    others_list = sorted_items[top_n:]

    # Labels a values pro top položky
    labels = [x[0] for x in top_list]
    values = [x[1] for x in top_list]

    # Součet ostatních
    others_sum = sum(x[1] for x in others_list)
    if others_sum > 0:
        labels.append("Others")
        values.append(others_sum)

    # Vykreslení
    plt.pie(
        values,
        labels=labels,
        autopct='%1.1f%%',
        startangle=140,
        textprops={'fontsize': 8}
    )
    plt.title("Koláč kategorií")
    plt.subplot(3,3,5)
    bins = np.linspace(0, 1000, 20)
    counts, edges = np.histogram(g5, bins=bins)
    plt.bar(edges[:-1], counts, width=np.diff(edges), align='edge')
    plt.title("Histogram délky článků")
    plt.subplot(3,3,6)
    bins = np.linspace(0, 20, 20)
    counts, edges = np.histogram(g6, bins=bins)
    plt.bar(edges[:-1], counts, width=np.diff(edges), align='edge')
    plt.title("Histogram délky slov ve článcích")
    plt.subplot(3,3,7)
    dates_c = [datetime.strptime(d, "%Y-%m") for d in g7_c.keys()]
    values_c = list(g7_c.values())
    plt.plot(dates_c, values_c, color="red")

    dates_v = [datetime.strptime(d, "%Y-%m") for d in g7_v.keys()]
    values_v = list(g7_v.values())
    plt.plot(dates_v, values_v, color="blue")
    plt.title("Covid a vakcína")
    years = [datetime(year, 1, 1) for year in range(1998, 2026)]
    plt.xticks(years, [f"{year}" for year in range(1998, 2026)], rotation=45)
    plt.subplot(3,3,8)
    plt.bar(range(1, 8), [g8[i] for i in range(1, 8)])  # Bar chart
    plt.xticks(range(1, 8), ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'])
    plt.title("Histogram dní")
    end = time.time()
    print(f"Trvalo to {end - start:.2f} sekund")
    plt.show()
    
if __name__ == "__main__":
    main()