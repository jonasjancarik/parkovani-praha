# parkovani-praha

Sada skriptů a dat pro analýzu parkování v Praze. Data jsou získána z [portálu TSK](https://zps.tsk-praha.cz/) k zónám placeného stání (ZPS).

## Instalace

Tento projekt používá `uv` pro správu závislostí a virtuálního prostředí.

1.  **Nainstalujte `uv`**: Pokud ještě nemáte `uv` nainstalovaný, postupujte podle [oficiálních instrukcí](https://github.com/astral-sh/uv#installation).
2.  **Vytvoření virtuálního prostředí a instalace závislostí**: V kořenovém adresáři projektu spusťte:
    ```bash
    uv sync
    ```
    Tento příkaz vytvoří `.venv` adresář (pokud neexistuje) a nainstaluje všechny potřebné závislosti definované v `pyproject.toml`.
3.  **Aktivace virtuálního prostředí**:

    Aktivace virtuálního prostředí je volitelná. Skripty můžete také spouštět přímo pomocí `uv run python <název_skriptu>.py`.

    ```bash
    source .venv/bin/activate
    ```
    (Pro Windows použijte `.venv\Scripts\activate`)

## Použití

Snažím se data v projektu průběžně aktualizovat. Pro získání souborů pro analýzu by mělo stačit následující:

- extrahovat soubor `data/processed/data_parking.csv.zip` do stejného adresáře;
- spustit `uv run python join.py` pro vytvoření souboru `data_parking_and_permits.csv` tamtéž.

Přehled datových souborů:

- **`data_parking_and_permits.csv`** - data o jednotlivých zónách - jejich využití (parkování) a počty vydaných parkovacích povolení pro adresy, které k nim přisluší
- `data_parking.csv` - data o jednotlivých zónách - jejich využití (parkování)
- `data_permits_by_zone.csv` - data o počtech vydaných parkovacích povolení podle jednotlivých zón (úseků)
- `data_permits.csv` - data o počtech vydaných parkovacích povolení podle jednotlivých městských částí
- `data_spaces.csv` - data o počtech parkovacích míst podle jednotlivých městských částí
- `data_permits_and_spaces.csv` - kombinace předchozích dvou souborů

## Web app (Streamlit)

Spuštění:

```bash
uv sync
uv run streamlit run web_app/app.py
```

Pro hledání podle adresy nastavte `MAPY_CZ_API_KEY`.

Adresní záložka ve web app nyní umí:
- najít referenční úsek pro zadanou adresu;
- spočítat zóny ZPS v okruhu 100–1500 m;
- zobrazit výběr, okruh a zóny na interaktivní mapě;
- vybrat bod kliknutím do mapy;
- zobrazit vývoj součtu `parkovacich_mist_v_zps` v daném okruhu a tabulku nejbližších úseků.
- zobrazit malé per-zóna grafy, aby bylo vidět změny po jednotlivých úsecích.
- omezit adresní okruh jen na úseky ze stejné městské části jako vybraná adresa.

Při načtení a zpracování dat se dočasné kapacitní režimy typu „skok nahoru a návrat zpět“
u `parkovacich_mist_v_zps` / `parkovacich_mist_celkem` konzervativně čistí na stabilní baseline.

## Sloupce ve finálním výstupu (`data_parking_and_permits.csv`)

Hodnoty jsou vždy za dané časové okno ve sledovaném období (dle `cast_dne` a `date`).

Poznámka k procentům z portálu: sloupce z grafů (domicil + typ oprávnění) a `rezidenti_do_500m`
jsou v exportu přepočtené na odhad počtu obsazených míst:
`(procento / 100) * obsazenost * parkovacich_mist_v_zps`.
`obsazenost` a `respektovanost` zůstávají jako podíl 0–1.

**Identifikace a čas**

- `kod_useku` - tarifní kód úseku (CODE; prefix `PX` značí MČ).
- `kod_zsj`, `naz_zsj` - kód a název základní sídelní jednotky (ZSJ).
- `mestska_cast` - městská část (P01, P02, ...).
- `typ_zony` - kategorie úseku (CATEGORY): `RES` rezidentní, `MIX` smíšený, `VIS` návštěvnický.
- `cast_dne` - časové okno: `den`, `noc`, `Po-Pá`, `So-Ne`, případně `Po-Pá (MPD)`/`So-Ne (MPD)`.
- `date` - první den měsíce (YYYY-MM-01).
- `year` - rok extrahovaný z `date`.
- `filename` - zdrojový soubor z portálu.

**Kapacity, obsazenost, respektovanost**

- `parkovacich_mist_celkem` - celkový počet míst v úseku (CELKEM_PS).
- `parkovacich_mist_v_zps` - počet míst podléhajících režimu ZPS (PS_ZPS).
- `obsazenost` - procento obsazenosti úseku (OBS; 0–1).
- `respektovanost` - procento respektujících (platících) vozidel (RESP; 0–1).

**Domicil vozidel (GRAF1)**

- `rezidenti_do_125m` - podíl vozidel s domicilem do 125 m.
- `rezidenti_od_126m_do_500m` - podíl vozidel s domicilem 126–500 m.
- `rezidenti_od_501m_do_2000m` - podíl vozidel s domicilem 501–2000 m.
- `rezidenti_nad_2000m` - podíl vozidel s domicilem nad 2000 m.
- `navstevnici_platici` - podíl vozidel bez známého domicilu (návštěvnické parkování; VI).
- `navstevnici_neplatici` - podíl vozidel bez parkovacího oprávnění (NR).
- `volna_mista` - podíl volných míst (FR).
- `rezidenti_do_500m` - původně ResPct (podíl držitelů oprávnění do 500 m).

**Typ parkovacího oprávnění (GRAF2)**

- `rezidentska` (R), `vlastnicka` (V), `abonentska` (A), `prenosna` (P).
- `carsharing` (C), `ekologicka` (E), `ostatni` (O), `socialni` (S).
- `navstevnici` - součet `navstevnici_platici` + `navstevnici_neplatici` + `prenosna`.
- `soucet_vsech_typu` - součet všech kategorií + `volna_mista`.

**Parkovací oprávnění v úseku (data z budov)**

- `POP_CELKEM` - celkový počet vydaných parkovacích oprávnění v úseku.
- `pop_rezidentska`, `pop_vlastnicka`, `pop_abonentska`, `pop_prenosna`.
- `pop_carsharing`, `pop_ekologicka`, `pop_ostatni`, `pop_socialni`.
  (význam zkratek viz níže ve "Slovníku zkratek")
Pozn.: `POP_*` vs `pop_*` je jen rozdíl v názvu/slovníku; význam stejný.

## Další výstupy (stručně)

**`data_parking.csv`**

- Shodné sloupce jako `data_parking_and_permits.csv`, ale bez `POP_*` a `year`.

**`data_permits_by_zone.csv`**

- `date`, `kod_useku`.
- `POP_CELKEM` a `pop_*` = počty vydaných oprávnění v úseku podle typu (R/V/A/P/C/E/O/S).

**`data_permits.csv`**

- `date`, `Oblast` (městská část / subčást s tečkou).
- `POP_R`, `POP_V`, `POP_A`, `POP_P`, `POP_C`, `POP_E`, `POP_O`, `POP_S`, `XSUM` (celkem).
- `parent district` je pomocný sloupec pro agregaci subčástí.

**`data_permits_and_spaces.csv`**

- Kombinace `data_permits.csv` + `data_spaces.csv` po MČ.
- `POP_*`, `POP_CELKEM` = počty oprávnění; `CELKEM_PS`, `PS_ZPS` = počty míst.
- `RES_PS`, `MIX_PS`, `VIS_PS`, `OTH_PS` (+ `_ZPS`) = kapacity dle kategorie.
- `POCINVST`, `PA_cnt`, `IB_cnt` = TODO: doplnit význam.

## Aktualizace dat

TLDR:

```python
uv run python download.py && uv run python process.py && uv run python join.py
```

### Stažení dat

Pro stažení dat je potřeba mít přístupové údaje k portálu TSK. Ty je potřeba zadat do souboru `.env`. Použijte `.env.example` jako vzor.

Samotné stažení obstárává skript `download.py`:

```bash
uv run python download.py
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
uv run python process.py
```

Pro vytvoření souboru `data_parking_and_permits.csv`, který kombinuje data o využití parkovacích míst a data o vydaných povoleních:

```bash
uv run python join.py
```

Kompletní aktualizaci tedy provedete takto:

```bash
uv run python download.py && uv run python process.py && uv run python join.py
```

#### Podrobnější nastavení

Je také  možné vybrat jen určitý typ dat, který se má zpracovat:

```bash
uv run python process.py <typ-dat>
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
Pro spuštění těchto skriptů (a ostatních Python skriptů v projektu) s `uv` použijte:
```bash
uv run python src/mapping.py map_zones_to_areas # Příklad pro useky_na_zsj
uv run python src/mapping.py map_buildings_to_zones # Příklad pro domy_na_useky
```

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

# Chybovost dat

Bohužel v datech se objevují zjevné chyby.

Například:

- úsek P5-1410 má po většinu roku 2022 hlášeno 250 parkovacích míst, přitom v jiných měsících jich měl 65 nebo 66.
- chybí data pro úsek P5-1414 za březen 2023
