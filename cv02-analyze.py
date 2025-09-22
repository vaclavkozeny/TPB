import orjson
import time
import re
from datetime import datetime

def load_jsonl(file_path):
    with open(file_path, "rb") as f: 
        return [orjson.loads(line) for line in f]

def analyze(data):
    oldest_article_date = datetime.now()
    oldest_article = {}
    highest_discussion = 0
    highest_discussion_article = {}
    most_images = 0
    most_images_article = {}
    for d in data:
        article_date = datetime.fromisoformat(d["time"])
        article_discussion = d["disc"]
        article_images = d["images"]
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
        }
    }
def main():
    start = time.time()
    data = load_jsonl("articles-clear.jsonl")
    analyzed_data = analyze(data)
    print(analyzed_data)
    end = time.time()
    print(f"Trvalo to {end - start:.2f} sekund")
if __name__ == "__main__":
    main()