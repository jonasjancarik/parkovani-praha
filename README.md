# parkovani-praha

Sada skriptů a dat pro analýzu parkování v Praze. Data jsou získána z [portálu TSK](https://zps.tsk-praha.cz/) k zónám placeného stání (ZPS).

## Instalace

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Použití

Snažím se data v projektu průběžně aktualizovat. Pro získání souborů pro analýzu by mělo stačit následující:

- extrahovat soubor `data/processed/data_parking.csv.zip` do stejného adresáře;
- spustit `python join.py` pro vytvoření souboru `data_parking_and_permits.csv` tamtéž.

Přehled datových souborů:

- **`data_parking_and_permits.csv`** - data o jednotlivých zónách - jejich využití (parkování) a počty vydaných parkovacích povolení pro adresy, které k nim přisluší
- `data_parking.csv` - data o jednotlivých zónách - jejich využití (parkování)
- `data_permits_by_zone.csv` - data o počtech vydaných parkovacích povolení podle jednotlivých zón (úseků)
- `data_permits.csv` - data o počtech vydaných parkovacích povolení podle jednotlivých městských částí
- `data_spaces.csv` - data o počtech parkovacích míst podle jednotlivých městských částí
- `data_permits_and_spaces.csv` - kombinace předchozích dvou souborů

## Aktualizace dat

### Stažení dat

Pro stažení dat je potřeba mít přístupové údaje k portálu TSK. Ty je potřeba zadat do souboru `.env`. Použijte `.env.example` jako vzor.

Samotné stažení obstárává skript `download.py`:

```bash
python download.py
```

Soubory, které jsem již stáhl, jsou k dispozici přímo v tomto projektu. Z opatrnosti vzhledem k možným citlivým údajům nenahrávám data o adresách.

Možné parametry jsou:

- `--type-of-data` - typ dat, který se má stáhnout (pokud není uveden, stáhnou se všechny)
- `--start-year` - rok, od kterého se mají stahovat data
- `--end-year` - rok, do kterého se mají stahovat data

Typy dat jsou:

- PARKING - data o využití zón
- PARKING_PERMITS - data o vydaných parkovacích povoleních
- PARKING_SPACES - data o parkovacích místech
- BUILDINGS - data o registracích na jednotlivých adresách

Pro správný běh přípravy dat je každopádně dobré stáhnout všechny typy dat.

### Příprava dat

Pro přípravu dat je potřeba spustit skript `process.py`:

```bash
python process.py
```

Pro vytvoření souboru `data_parking_and_permits.csv`, který kombinuje data o využití parkovacích míst a data o vydaných povoleních:

```bash
python join.py
```

#### Podrobnější nastavení

Je také  možné vybrat jen určitý typ dat, který se má zpracovat:

```bash
python process.py <typ-dat>
```

- `parking` - využití parkovacích míst
- `permits_spaces` - počty oprávnění a parkovacích míst
- `permits` - vydaná oprávnění
- `spaces` - počty parkovacích míst
- `all` - všechna předchozí data

Některé výstupy jsou podobné, respektive mají překryvy. Nejužitečnější jsou první dva.

Navíc je možné zpracovat následující podkladové soubory, které nespadají pod `all`:

- `useky_na_zsj` - mapování úseků na základní sídelní jednotky 
- `domy_na_useky` - mapování domů na úseky

Výsledné soubory těchto dvou skriptů jsou ale nahrané v projektu, takže pokud nedojde například k rozšíření zón, nemělo by být potřeba je spouštět.

## Analýza dat

- `analysis.py` je rozpracovaný skript, který by měl sloužit k analýze

# Slovník zkratek

## Parkovací oprávnění (POP)

- **R** Rezidentská
- **V** Vlastnická
- **A** Abonentská
- **P** Přenosná
- **C** Carsharing
- **E** Ekologická
- **O** Ostatní
- **S** Sociální

kde:
- **E** Zahrnuje hodnoty:
  - Ekologická
  - Elektromobil-abonent
  - Elektromobil-ostatní
  - Elektromobil-rezident
  - Hybrid-abonent
  - Hybrid-ostatní
  - Hybrid-rezident
  - Osvobozená
- **O** Zahrnuje hodnoty:
  - Ostatní
  - Bezpečnostní složky
  - Integrovaný záchranný systém
  - MHMP
  - Speciální Zastupitelská
  - Zastupitelská
  - Zastupitelská-přenosná
  - Zastupitelská-přenosná-senior
- **S** Zahrnuje hodnoty:
  - Seniorská/ZTP
  - Pečovatelská / ZTP
  - ZTP
  - Pečovatelská původní
  - Pečovatelská
  - Sociální služby

# Co znamenají názvy zdrojových datových souborů?

### JS a JSON soubory

.js soubory obsahují popis JSON souborů, které se načítají do mapy.

JSON soubory obsahují podobná data jako .tsv soubory, narozdíl od nich jsou ale rozděleny časově (den, noc atd.). Navzdory tomu, že popis v .js souboru odkazuje na .tsv soubor, tak tyto soubory zřejmě nemohly být vygenerovány jen z .tsv soborů (právě proto, že v těch chybí časové rozdělení).

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

Nepotřebujeme agregovaná čtvrtletní data, protože máme měsíční data. Soubory s _Q tedy také nebudeme používat.

Relevantní datové soubory pro jeden měsíc tedy vypadají takto:

- OB_202311D_NA.json
- OB_202311D_NJ.json
- OB_202311N_NA.json
- OB_202311N_NJ.json
- OB_202311P_NA.json
- OB_202311P_NJ.json 

některé městské části mají navíc soubory W a X, tedy měly by existovat i:

- OB_202311W_NA.json
- OB_202311W_NJ.json
- OB_202311X_NA.json
- OB_202311X_NJ.json