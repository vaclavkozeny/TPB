import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SLOZKA = os.path.join(BASE_DIR, "out/2025-12-05--17/")      # složka se vstupními txt soubory
VYSTUP = "vysledek.txt"

vysledky = {}

for soubor in os.listdir(SLOZKA):
    if soubor.endswith(".txt"):
        with open(os.path.join(SLOZKA, soubor), "r", encoding="utf-8") as f:
            for radek in f:
                pismeno, pocet = radek.split()
                pocet = int(pocet)

                if pismeno in vysledky:
                    vysledky[pismeno] += pocet
                else:
                    vysledky[pismeno] = pocet

with open(VYSTUP, "w", encoding="utf-8") as out:
    for pismeno in sorted(vysledky):
        out.write(f"{pismeno} {vysledky[pismeno]}\n")