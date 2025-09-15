import requests
import re
import json
from bs4 import BeautifulSoup

def parse_article(path):
    response = session.get(path)
    art = BeautifulSoup(response.content, 'html.parser')
    clanek = {}
    content = ''
    title = art.find('h1',itemprop='name headline').get_text()
    time = art.find('span',class_='time-date')
    diskuze = re.findall(r'\d+',art.find('a',class_='ico-discusion').find('span').get_text())
    text = art.find('div',class_='text')
    contents = text.find_all('p')
    for c in contents:
        content = content + " " + c.get_text(strip=True)
    images = len(text.find_all('img'))

    clanek["title"] = title
    clanek["time"] = time
    clanek["content"] = content
    clanek["images"] = images
    clanek["disc"] = diskuze

    return clanek

session = requests.Session()

session.cookies.set('dCMP', 'mafra=1111,all=1,reklama=1,part=0,cpex=1,google=1,gemius=1,id5=1,nase=1111,groupm=1,piano=1,seznam=1,geozo=0,czaid=1,click=1,vendors=full,verze=2,')
data = []
sitemap = session.get('https://www.idnes.cz/zpravy/sitemap')
mapa = BeautifulSoup(sitemap.content, 'html.parser')
clanky = mapa.find_all('url')
for cl in clanky:
    path = cl.loc.get_text()
    print(path)
    if re.search(r'/foto',path):
        continue
    if re.search(r'idnes-premium',path):
        continue
    d = parse_article(path)
    data.append(d)

with open("data.json", "w", encoding='utf-8') as f:
    json.dump(data, f)


