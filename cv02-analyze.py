import orjson
import time
import re
from datetime import datetime
import matplotlib.pyplot as plt

def load_jsonl(file_path):
    with open(file_path, "rb") as f: 
        return [orjson.loads(line) for line in f]

def analyze_max(data):
    oldest_article_date = datetime.now()
    oldest_article = {}
    highest_discussion = 0
    highest_discussion_article = {}
    most_images = 0
    most_images_article = {}
    all_discussion = 0
    for d in data:
        article_date = datetime.fromisoformat(d["time"])
        article_discussion = d["disc"]
        article_images = d["images"]
        all_discussion += article_discussion
        if article_date < oldest_article_date:
            oldest_article_date = article_date
            oldest_article = d
        if article_discussion > highest_discussion:
            highest_discussion = article_discussion
            highest_discussion_article = d
        if article_images > most_images:
            most_images = article_images
            most_images_article = d
    return {
        "oldest": {
            "date": oldest_article_date,
            "article": oldest_article["title"]
        },
        "highest_discussion": {
            "count": highest_discussion,
            "article": highest_discussion_article["title"]
        },
        "most_images": {
            "count": most_images,
            "article": most_images_article["title"]
        },
        "all_discussion" : all_discussion
    }
def articles_per_year(data):
    articles_per_year_list = {}
    for d in data:
        y = datetime.fromisoformat(d["time"]).year
        if y in articles_per_year_list:
            articles_per_year_list[y] += 1
        else:
            articles_per_year_list[y] = 1
    return articles_per_year_list

def analyze_category(data):
    articles_per_category = {}
    for d in data:
        for c in d["tags"]:
            if c in articles_per_category:
                articles_per_category[c] += 1
            else:
                articles_per_category[c] = 1
    return articles_per_category
def analyze_2021(data):
    words = {}
    for d in data:
        if datetime.fromisoformat(d["time"]).year != datetime.fromisocalendar(2021,1,1).year:
            continue
        else:
            for w in d["title"].split():
                if len(w) < 3:
                    continue
                if w in words:
                    words[w] += 1
                else:
                    words[w] = 1
    return sorted(words.items(), key=lambda item: item[1])[-6:-1]

def all_words(data):
    words = 0
    for d in data:
        words += len(d["content"].split())
    return words


def most_used_words(data):
    words = {}
    for d in data:
        for w in d["content"].split():
            if len(w) < 6:
                continue
            if(w in words):
                words[w] += 1
            else:
                words[w] = 1
    return sorted(words.items(), key=lambda item: item[1])[-9:-1]

def covid(data):
    occurrence = {}
    for d in data:
        if not re.search(r'covid-19', d["content"]):
            continue
        occurrence[d["title"]] = len(re.findall(r'covid-19', d["content"]))
    return sorted(occurrence.items(), key=lambda item: item[1])[-4:-1]

def longest_shortest(data):
    max = 0
    min = 10000000000
    max_title = ""
    min_title = ""
    for d in data:
        if(len(d["content"].split()) > max):
            max = len(d["content"].split())
            max_title = d["title"]
        
        if(len(d["content"].split()) < min):
            min = len(d["content"].split())
            min_title = d["title"]

    return {"max":{"title":max_title, "words":max}, "min":{"title":min_title, "words":min}}

def analyze_month(data):
    articles_per_month = {}
    for d in data:
        m = datetime.fromisoformat(d["time"]).month
        if m in articles_per_month:
            articles_per_month[m] += 1
        else:
            articles_per_month[m] = 1
    return sorted(articles_per_month.items(), key=lambda item: item[1])[0]

def average_words(data):
    words = 0
    leters = 0
    for d in data:
        art_words = d["content"].split()
        words += len(art_words)
        for w in art_words:
            leters += len(w)
    return leters/words
def main():
    start = time.time()
    data = load_jsonl("articles-clear.jsonl")

    #analyzed_data = analyze_max(data)
    #print(analyzed_data)

    #analyzed_data = articles_per_year(data)
    #print(analyzed_data)
    #plt.bar(*zip(*analyzed_data.items()))
    #plt.show()

    #articles_per_category = analyze_category(data)
    #print(sorted(articles_per_category.items(), key=lambda item: item[1])[-6:-1])
    #print(len(articles_per_category))

    #year_2021 = analyze_2021(data)
    #print(year_2021)

    #print(all_words(data))
    
    # BONUS
    print(most_used_words(data))

    print(covid(data))

    #print(longest_shortest(data))

    #print(average_words(data))

    #print(analyze_month(data))

    end = time.time()
    print(f"Trvalo to {end - start:.2f} sekund")
if __name__ == "__main__":
    main()