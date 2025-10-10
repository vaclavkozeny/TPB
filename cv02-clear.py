import orjson
import time
import re
from datetime import datetime

def load_jsonl(file_path):
    with open(file_path, "rb") as f: 
        return [orjson.loads(line) for line in f]

def get_duplicates(data):
    result_set = set()
    duplicates = []
    result = 0
    for d in data:
        if d["title"] in result_set:
            result += 1
            duplicates.append({d["title"], d["time"]})
        else:
            result_set.add(d["title"])
    return result, duplicates

def validate_data(data):
    if not isinstance(data["title"],str):
        return False
    if not re.match(r'^(\d{4})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])T([01]\d|2[0-3]):([0-5]\d):([0-5]\d)([+-](0\d|1[0-4]):([0-5]\d)|Z)$',data["time"]):
        return False
    if not isinstance(data["images"],int):
        return False
    if not isinstance(data["disc"],int):
        return False
    if not isinstance(data["content"],str):
        return False
    if not isinstance(data["tags"],list):
        return False
    return True

def normalize_data(data):
    data["title"] = data["title"].lower()
    t = datetime.fromisoformat(data["time"])
    #1998-09-05T00:00:00+02:00
    data["time"] = t.strftime("%Y-%m-%dT%X")
    norm_tags = []
    for t in data["tags"]:
        norm_tags.append(t.lower())
    
    data["tags"] = norm_tags
    data["content"] = data["content"].lower()

def remove_duplicates(data):
    result_set = set()
    result = []
    for d in data:
        if not d["title"] in result_set:
            result.append(d)
            result_set.add(d["title"])
    return result
def clear_data(name):
    data = load_jsonl(name)
    cleared_data = []
    [cleared_data.append(d) for d in data if validate_data(d)]
    [normalize_data(d) for d in cleared_data]
    cleared_data = remove_duplicates(cleared_data)
    with open("articles-clear.jsonl","wb") as f:
        for d in cleared_data:
            f.write(orjson.dumps(d))
            f.write(b"\n")

    
def clear_with_print():
    data = load_jsonl("articles-full.jsonl")
    start = time.time()
    d, d_list = get_duplicates(data)
    pred = len(data)
    print(f"pocet clanku {pred}")
    print(f"pocet duplicit {d}")
    new_data = []
    [new_data.append(d) for d in data if validate_data(d)]
    po = len(new_data)
    [normalize_data(d) for d in new_data]
    new_data = remove_duplicates(new_data)
    print(f"pocet clanku po {len(new_data)}")
    d, d_list = get_duplicates(new_data)
    print(f"pocet duplicit po {d}")
    print(f"procento validnich dat {pred/po}, procento nevalidnich dat {1-(pred/po)}")
    end = time.time()
    print(f"Trvalo to {end - start:.2f} sekund")

def main():
    start = time.time()
    #clear_data("articles-full.jsonl")
    clear_with_print()
    end = time.time()
    print(f"Trvalo to {end - start:.2f} sekund")
if __name__ == "__main__":
    main()