import csv

# Jména vstupních souborů
input_files = ["2006.csv", "2007.csv", "2008.csv"]
output_file = "output.csv"

with open(output_file, "w", newline="", encoding="utf-8") as outfile:
    writer = None

    for idx, file in enumerate(input_files):
        with open(file, "r", encoding="utf-8") as infile:
            reader = csv.reader(infile)

            # Pokud zapisujeme první soubor - zapiš header
            if idx == 0:
                writer = csv.writer(outfile)
                for row in reader:
                    writer.writerow(row)
            else:
                # U ostatních souborů přeskočit header
                next(reader, None)
                for row in reader:
                    writer.writerow(row)

print("Hotovo! Výsledek je v", output_file)