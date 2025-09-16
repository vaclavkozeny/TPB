import aiohttp
import asyncio
import re
import json
from bs4 import BeautifulSoup
import time


async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text()


async def parse_article(session, path):
    html = await fetch(session, path)
    art = BeautifulSoup(html, 'lxml')
    article_stats = {}
    content = ''

    title = art.find('h1', itemprop='name headline').get_text()
    time_val = art.find('span', class_='time-date')["content"]

    diskuze_tag = art.find('a', class_='ico-discusion')
    if diskuze_tag:
        diskuze_text = diskuze_tag.find('span').get_text()
        diskuze = int(re.search(r'\d+', diskuze_text).group())
    else:
        diskuze = 0

    text = art.find('div', class_='text')
    contents = text.find_all('p')
    for c in contents:
        content += " " + c.get_text(strip=True)

    images = len(text.find_all('img')) if text else 0
    tags = [t.get_text(strip=True) for t in art.find('div', class_='tag-list').find_all('a')]

    article_stats["title"] = title
    article_stats["time"] = time_val
    article_stats["content"] = content
    article_stats["images"] = images
    article_stats["disc"] = diskuze
    article_stats["tags"] = tags

    return article_stats


async def get_urls(limit, concurrency, file):
    connector = aiohttp.TCPConnector(limit_per_host=concurrency)
    async with aiohttp.ClientSession(connector=connector, cookies={
        'dCMP': 'mafra=1111,all=1,reklama=1,part=0,cpex=1,google=1,gemius=1,id5=1,nase=1111,groupm=1,piano=1,seznam=1,geozo=0,czaid=1,click=1,vendors=full,verze=2,'
    }) as session:

        
            for page in range(1, limit):
                url = f"https://www.idnes.cz/zpravy/archiv/{page}"
                html = await fetch(session, url)
                soup = BeautifulSoup(html, "lxml")
                articles = soup.find("div", class_="content").find_all("a", class_="art-link")
                if not articles:
                    break

                hrefs = [a["href"] for a in articles if not a["href"].endswith("/foto")]

                tasks = [parse_article(session, href) for href in hrefs]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                buffer = []
                for res in results:
                    if isinstance(res, Exception):
                        print(f"Chyba: {res}")
                    elif res:
                        buffer.append(json.dumps(res, ensure_ascii=False))
                if buffer:
                    file.write("\n".join(buffer) + "\n")

async def main():
    start = time.time()
    with open("articles.jsonl", "a", encoding="utf-8") as f:
        await get_urls(10, 100, f)
    end = time.time()
    print(f"Trvalo to {end - start:.2f} sekund")


if __name__ == "__main__":
    asyncio.run(main())
