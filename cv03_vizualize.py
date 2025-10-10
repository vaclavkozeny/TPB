import orjson
import time
import re
from datetime import datetime
import matplotlib.pyplot as plt

def load_jsonl(file_path):
    with open(file_path, "rb") as f: 
        return [orjson.loads(line) for line in f]



def articles_per_year(data):
    articles_per_year_list = {}
    for d in data:
        y = datetime.fromisoformat(d["time"]).year
        if y in articles_per_year_list:
            articles_per_year_list[y] += 1
        else:
            articles_per_year_list[y] = 1
    return articles_per_year_list
def articles_added_over_time(data):
    data = sorted(data.items())
    year_articles = {}
    count = 0
    for d in data:
        year_articles[d[0]] = count + d[1]
        count = count + d[1]
    return year_articles
def art_length_number_of_disc(data):
    length = []
    coms = []
    for d in data:
        if d["disc"] > 13000:
            continue
        else:
            length.append(len(d["content"].split()))
            coms.append(d["disc"])
    return length, coms

def analyze_category(data):
    articles_per_category = {}
    for d in data:
        for c in d["tags"]:
            if c in articles_per_category:
                articles_per_category[c] += 1
            else:
                articles_per_category[c] = 1
    return articles_per_category
def histogram(data):
    word_count = []
    word_length = []
    for d in data:
        word_count.append(len(d["content"].split()))
        for w in d["content"].split():
            word_length.append(len(w))
    return word_count, word_length
def main():
    start = time.time()
    data = load_jsonl("articles-clear.jsonl")

    

    analyzed_data = articles_per_year(data)
    
    g1 = articles_added_over_time(analyzed_data)
    x = g1.keys()
    y = g1.values()
    plt.subplot(3,3,1)
    plt.plot(x,y)

    plt.subplot(3,3,2)
    plt.bar(*zip(*analyzed_data.items()))

    plt.subplot(3,3,3)
    x,y = art_length_number_of_disc(data)
    plt.scatter(x,y)

    plt.subplot(3,3,4)
    g4 = analyze_category(data)
    list = sorted(g4.items(), key=lambda item: item[1],reverse=True)[1:20]
    labels = [x[0] for x in list]
    values = [x[1] for x in list]
    plt.pie(
        values,
        labels=labels,
        autopct='%1.1f%%',      # zobrazí procenta s 1 desetinným místem
        startangle=140,         # natočení grafu
        textprops={'fontsize': 8}
    )
    plt.subplot(3,3,5)
    g5,g6 = histogram(data)
    plt.hist(g5, bins=[0, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000], edgecolor='black')
    plt.subplot(3,3,6)
    plt.hist(g6, bins=[0,2,4,6,8,10,12,14,16,18,20], edgecolor='black')
    plt.show()
    end = time.time()
    print(f"Trvalo to {end - start:.2f} sekund")
if __name__ == "__main__":
    main()