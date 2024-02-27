# parkovani-praha

# Použití

## Instalace

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Stažení dat

Pro stažení dat je potřeba spustit skript `download.py`:

```bash
python download.py {typ dat}
```

Kde `{typ dat}` je jeden z následujících:
- PARKING - data o využití zón
- PARKING_PERMITS - data o vydaných parkovacích povoleních
- PARKING_SPACES - data o parkovacích místech
- HOUSES - data o registracích na jednotlivých adresách

Soubory, které jsem již stáhl, jsou k dispozici přímo v tomto projektu. Z opatrnosti vzhledem k možným citlivým údajům nenahrávám data o adresách.

Další možné parametry jsou"

- `--start-year` - rok, od kterého se mají stahovat data
- `--end-year` - rok, do kterého se mají stahovat data
- `--include-quarterly` - stáhnout i čtvrtletní data (ne jen měsíční)
- `--include-sections` - stáhnout i data o jednotlivých úsecích

Příklad:

```bash
python download.py PARKING --start-year 2020 --end-year 2023 --include-sections
```

## Analýza dat

- `analyze.py` zpracuje soubory a vytvoří výstupní soubor `data/processed/data.csv`

# Co znamenají názvy souborů?

### JS a JSON soubory

.js soubory obsahují popis JSON souborů, které se načítají do mapy.

Tyto soubory ale obsahují podobná data jako .tsv soubory, narozdíl od nich jsou ale rozděleny časově (den, noc atd.). Navzdory tomu, že popis v .js souboru odkaazuje na .tsv soubor, tak tyto soubory zřejmě nemohly být vygenerovány jen z .tsv soborů (právě proto, že v těch chybí časové rozdělení).

Název souborů se skládá z několika částí:
- typ zobrazení v mapě (OB = Obsazenost, OR = Rezidenti, RE = Respektovanost) - různá zobrazení, ale stejná data
- _
- rok (2023)
- měsíc (01-12)
- časový rámec - den/noc/víkend (D = Den, N = Noc, P = Pondělí-Pátek mimo provozní dobu, S = Sobota-Neděle mimo provozní dobu, W = Pondělí-Pátek v provozní dobu, X = Sobota-Neděle v provozní dobu)
- _
- perioda dat - měsíční/čtvrtletní (N = Měsíční, Q = Čtvrtletní, P = ???)
- úseky/základní sídelní jednotky (A, H = úseky, J = ZSJ)

NA a PH, respektive QA a QH jsou stejné, mají jen jinak definované barvy a škály. Verze PH a QH budeme ignorovat.

MPD znamená mimo pracovní dobu

### TSV soubory

P10-202308B_4.tsv

- _4.tsv - úseky
- _6.tsv - základní sídelní jednotky (oblasti)

TW_202201X_7A.tsv

- účel není jasný, nějaká definice úseků?

### Jaké soubory jsou použité pro analýzu?

Protože nás nezajímá zobrazení v mapě, ale pouze data, tak budeme používat jen jeden typ souborů - arbitrárně volíme soubory začínající OB_.

Relevantní datové soubory pro jeden měsíc tedy vypadají takto:

- OB_202311D_NA.json
- OB_202311D_NJ.json
- OB_202311D_QA.json
- OB_202311D_QJ.json
- OB_202311N_NA.json
- OB_202311N_NJ.json
- OB_202311N_QA.json
- OB_202311N_QJ.json
- OB_202311P_NA.json
- OB_202311P_NJ.json 
- OB_202311S_QA.json (možná není generován)
- OB_202311S_QJ.json (možná není generován)

některé městské části mají navíc soubory W a X, tedy měly by existovat i:

- OB_202311W_NA.json
- OB_202311W_NJ.json
- OB_202311W_QA.json
- OB_202311W_QJ.json
- OB_202311X_NA.json
- OB_202311X_NJ.json
- OB_202311X_QA.json
- OB_202311X_QJ.json