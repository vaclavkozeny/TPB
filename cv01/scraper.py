import requests
import re
import json
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import time

def parse_article(path):
    response = session.get(path)
    art = BeautifulSoup(response.content, 'html.parser')
    article_stats = {}
    content = ''
    title = art.find('h1',itemprop='name headline').get_text()
    time = art.find('span',class_='time-date')["content"]
    diskuze_tag = art.find('a', class_='ico-discusion')
    if diskuze_tag:
        diskuze_text = diskuze_tag.find('span').get_text()
        diskuze = int(re.search(r'\d+', diskuze_text).group())
    else:
        diskuze = 0
    text = art.find('div',class_='text')
    contents = text.find_all('p')
    for c in contents:
        content = content + " " + c.get_text(strip=True)
    images = len(text.find_all('img')) if text else 0
    tags = [t.get_text(strip=True) for t in art.find('div', class_='tag-list').find_all('a')]
    article_stats["title"] = title
    article_stats["time"] = time
    article_stats["content"] = content
    article_stats["images"] = images
    article_stats["disc"] = diskuze
    article_stats["tags"] = tags

    return article_stats

def get_urls(limit):
        page = 1
        with open("articles.jsonl", "a", encoding="utf-8") as f:
            while page < limit:
                url = f"https://www.idnes.cz/zpravy/archiv/{page}"
                r = session.get(url)
                soup = BeautifulSoup(r.text, "html.parser")
                articles = soup.find("div",class_="content").find_all("a",class_="art-link")
                if not articles:
                    break 

                for a in articles:
                    href = a["href"]
                    if href.endswith("/foto"):
                        continue
                    else:
                        data = parse_article(href)
                        if data:
                            f.write(json.dumps(data, ensure_ascii=False) + "\n")

                page += 1



session = requests.Session()

session.cookies.set('dCMP', 'mafra=1111,all=1,reklama=1,part=0,cpex=1,google=1,gemius=1,id5=1,nase=1111,groupm=1,piano=1,seznam=1,geozo=0,czaid=1,click=1,vendors=full,verze=2,')

start = time.time()
get_urls(2)
end = time.time()

print(f"Trvalo to {end - start:.2f} sekund")

#data = []

#data.append(parse_article("https://www.idnes.cz/finance/pojisteni/nemovitost-podpojisteni-riziko-kalkulacka-indexace.A250914_154213_poj_sov"))

#print(data)

#with open("articles.json", "w", encoding="utf-8") as f:
#    json.dump(data, f, ensure_ascii=False, indent=2)



