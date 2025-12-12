VSTUP = "vysledek.txt"

soucet = 0

with open(VSTUP, "r", encoding="utf-8") as f:
    for radek in f:
        _, pocet = radek.split()
        soucet += int(pocet)

print(soucet)