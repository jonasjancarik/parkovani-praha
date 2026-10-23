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

Po stažení nových dat (`uv run python download.py`, který zároveň zapíše datum stažení do `data/downloaded/last_download.json` pro webovou aplikaci) spusťte `./update_data.sh`; zpracuje
data, nasadí auditované přiřazení oprávnění k úsekům, sestaví
`data_parking_and_permits.csv` a uloží aktuální tvary úseků do `zones.geojson`. Podrobnosti viz `docs/parking-data-quality-handoff.md`.

## Web app (Streamlit)

Spuštění:

```bash
uv sync
uv run streamlit run web_app/app.py
```

## React Data Explorer

The React explorer reuses the existing Python data layer through a small FastAPI API.

Start the API:

```bash
uv run uvicorn web_app.api:app --reload --port 8000
```

Start the frontend in another terminal:

```bash
cd frontend
npm install
npm run dev
```

The selected visual reference and implementation notes live in `docs/design/`.

The Grafy view preserves all nine analytical questions from the original Streamlit
overview in six editorial chart surfaces: the city trend and district ranking,
zone and parker composition, detailed type trends, local annual change and its
distribution, ZSJ parking pressure, and a 12-month district forecast. Every view
uses the same period, district, and zone filters. The time-of-day filter applies
to observations that genuinely change during the day, such as parker composition.

Permit counts, parking capacity, and the resulting permits-per-space pressure are
invariant across parts of the day. The explorer therefore calculates those measures
from one canonical row per zone and month, independently of the `Část dne` filter.
That filter applies only to parker composition and other measurements that genuinely
change during the day; this prevents changes in historical source coverage from
appearing as changes in parking capacity.

The main chart shows the monthly observations as straight segments and preserves
missing months as gaps. `Průměr z 12 měření` adds a trailing average from the 12
latest available observations, while retaining those gaps in the trend line. It
remains a visual overlay and never replaces API values or CSV exports.

### City overview

The city overview is the homepage (`/`), opened through the site name; it has no item of its own in the navigation. It needs no filtering: it
shows the latest citywide number of permits, parking spaces and permits per space
with the change against the same month a year earlier, the development of permits
per space (drawn only for zones with data in every month since permits are known,
so that new zones do not dilute the trend), every district ranked by that ratio, who is parked in the zones by day
and by night, and an address field that leads to `Okolí adresy`. The sentences on
the page are written from the data, so they stay correct after each data update.

The figures come from `/api/overview` (`web_app/city_overview.py`) and use the
explorer's default selection, so they match what the explorer shows before any
filter is changed. Districts with fewer than 1,000 paid-parking spaces are left
out of the ranking. A yearly change is reported only when the same month of the
previous year has data.

The explorer, called `Vývoj a srovnání` in the navigation, now lives at `/?page=explorer`. Explorer links shared earlier, which
carry filters but no page name, still open the explorer.

### Address report

`Okolí adresy` (`/?page=address`) shows the paid-parking zones within walking
distance of an address or of a point picked on the map. For a radius of 100–1500 m
it reports parking spaces, permits per space, occupancy and compliance, the
development of capacity, the history of permits, occupancy, parkers and permit
types for all zones in the circle added up (only months with data for every
zone), every zone in the circle with its own capacity history,
the same figures for other radii, and the history of the zone the address lies in.
The zone list downloads as CSV or GeoJSON, and the page address carries the
selection, so it can be shared.

Only zones from the same city district as the address are counted. The capacity
total covers the months for which every zone in the circle has data, so a zone that
appears later does not look like added capacity; with many zones this period can be
short, and the page states it under the chart.

The page uses three endpoints: `/api/address/options`, `/api/address/search?q=`
and `/api/address/report?lon=&lat=&radius=&cast_dne=&label=`. Address search and
naming a point picked on the map go through Mapy.cz and need `MAPY_CZ_API_KEY`;
without a working key the search is unavailable and a place can still be picked on
the map.

Zone shapes come from `data/processed/zones.geojson`, which `./update_data.sh`
rebuilds from the newest downloaded month (`uv run python web_app/geo.py` on its
own). The production image contains this file instead of the raw exports.

### Production container

Security builds require certifi 2026.10.19 or later, Pillow 13 or later, and
uv 0.13.4. Streamlit is updated as needed to support Pillow 13. The image refreshes
Debian's CA bundle, explicitly disables emSign ECC Root CA - G3, and installs
pip 26.2.2 to refresh its separate vendored trust store. Build with `--pull` when
refreshing base images; verify the actual Python, certifi, pip and system trust
stores in the replacement container before accepting the release.

The production image builds the React frontend and serves it together with the
FastAPI endpoints from one process:

```bash
docker compose up -d --build
```

Compose publishes the service on `127.0.0.1:3014`, keeping the origin private for
the Cloudflare Tunnel route. Verify it with:

```bash
curl http://127.0.0.1:3014/api/health
```

Pro hledání podle adresy nastavte `MAPY_CZ_API_KEY` v souboru `.env`; Compose ho předá kontejneru.

Adresní záložka ve web app nyní umí:
- najít referenční úsek pro zadanou adresu;
- spočítat zóny ZPS v okruhu 100–1500 m;
- zobrazit výběr, okruh a zóny na interaktivní mapě;
- vybrat bod kliknutím do mapy;
- zobrazit vývoj součtu `parkovacich_mist_v_zps` v daném okruhu a tabulku nejbližších úseků.
- respektovat boční filtry pro období, část dne, MČ, ZSJ a typ zóny;
- zobrazit tlakové metriky pro návrh parkovací politiky: oprávnění na místo, obsazenost, respektovanost a počty úseků nad/pod prahovými hodnotami;
- uložit vybraný adresní report do URL a znovu ho načíst ze sdíleného odkazu;
- porovnat stejný bod napříč více okruhy;
- stáhnout aktuální seznam úseků v CSV nebo GeoJSON;
- seřadit úseky v aktuálním filtru podle tlakového skóre;
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

#### Anomálie se opravují jen tehdy, když je chyba jednoznačně doložená

Příprava parkovacích dat odděluje opravu zdrojové chyby od analytického
vyhlazení. Hodnoty obsazenosti a složení parkujících se automaticky nepřepisují.
Detektor je porovnává jen s předchozími 12 souvislými měsíci zvlášť pro každý
úsek a část dne a podezřelé pohyby zapíše do
`data/processed/data_parking_anomaly_report.json`.

Automatická oprava se týká pouze kapacity. Vedle skoků popsaných níže opravuje
dočasné chyby jmenovatele, u kterých
souhlasí oba kapacitní sloupce, všechny dostupné části dne, souvislé období před
i po chybě, stabilní typ zóny a zachovaný počet obsazených míst. Před změnou
kapacity se uloží zdrojový počet obsazených míst a obsazenost se poté přepočítá
na opravenou kapacitu; počty parkujících se tak uměle nezmění. Opravy i
potvrzující datum jsou součástí stejného reportu.

Build se zastaví, pokud po zpracování zůstanou duplicitní měsíční záznamy,
kapacita se liší podle části dne nebo nesouhlasí odvozené součty. Případy, kdy
zdroj uvádí více míst v ZPS než celkem, report označí, ale automaticky je
nepřepisuje, protože z dat nelze bezpečně určit chybný sloupec. Přesné duplicity
a nadbytečné kopie z cizí městské části se předem odstraní; úseky bez mapování
na ZSJ zůstávají v datech označené jako `Nezařazený úsek`, místo aby se tiše
zahodily.

Pro kontrolní export bez korekcí lze použít `parking_no_anomaly`. Zapíše ho do
`data/processed/data_parking_no_anomaly.csv` a nepřepíše hlavní dataset. Tento
režim záměrně nevynucuje produkční kvalitativní brány a nemá se publikovat jako
hlavní dataset.

#### Skoky kapacity: co se opravuje a co ne

Zpracování projde každý skok kapacity úseku mezi dvěma sousedními měřeními o
alespoň 50 % a 20 míst (`src/capacity_steps.py`) a porovná dvě nezávislé věci:
počet zaparkovaných aut (kapacita × obsazenost) a tvar úseku v exportech z obou
měsíců. Skok se opraví jen tehdy, když tvar i počet aut zůstaly stejné a zároveň
platí jedno z těchto dvou:

- hodnota se později vrátila na původní (bez ohledu na to, jak dlouho trvala a
  jestli úsek mezitím změnil typ),
- při hodnotě na jedné straně skoku by obsazenost přesáhla 150 %, zatímco na druhé
  straně nepřesahuje 120 %.

Chybná hodnota se nahradí tou z druhé strany skoku a obsazenost se přepočítá, takže
počet zaparkovaných aut zůstává podle zdroje. Použité opravy s důvodem a počtem
změněných řádků jsou v `data_parking_anomaly_report.json` pod
`capacity_corrections.jump_corrections`. Kontrolní export `parking_no_anomaly` je
nepoužívá.

Ostatní skoky zůstávají tak, jak je uvádí zdroj. Překreslený úsek je skutečná změna.
Trvalá změna při stejném tvaru a stejném počtu aut může být také skutečná, například
když se část stání převede do jiného režimu, a z čísel nejde poznat, která hodnota je
správná. Obsazenost mezi 100 a 150 % je v datech běžná a jako důkaz chyby nestačí.

`uv run python -m src.capacity_steps` (spouští ho i `./update_data.sh`) zapíše do
`data/processed/data_parking_capacity_steps.csv` skoky, které ve zpracovaných datech
zůstaly, s verdiktem a podklady. Slouží jako záznam, ne jako úkol ke kontrole.

Otevřené datové problémy, blokátory publikace a doporučený cíl další práce
shrnuje [předání k datové kvalitě](docs/parking-data-quality-handoff.md).

Navíc je možné zpracovat následující podkladové soubory, které nespadají pod `all`:

- `useky_na_zsj` - mapování úseků na základní sídelní jednotky
- `domy_na_useky` - mapování domů na úseky

Výsledné soubory těchto dvou skriptů jsou ale nahrané v projektu, takže pokud nedojde například k rozšíření zón, nemělo by být potřeba je spouštět.
Pro spuštění těchto skriptů (a ostatních Python skriptů v projektu) s `uv` použijte:
```bash
uv run python src/mapping.py map_zones_to_areas # Příklad pro useky_na_zsj
uv run python src/mapping.py map_buildings_to_zones # Příklad pro domy_na_useky
```

Statické mapování domů na úseky je historický podklad a nesmí se naslepo
regenerovat do produkčních dat. Pro porovnání jednoho měsíce vytvořte kandidáta
a auditní report pouze v `/tmp`; příkaz ověří úplnost, aktivní cílové úseky,
součty všech typů oprávnění, náhradní souřadnice, remízy i vzdálenosti:

```bash
uv run python -m src.permit_zone_mapping audit \
  --month 202606 \
  --candidate-output /tmp/parkovani-permits-202606-candidate.csv \
  --mapping-output /tmp/parkovani-permits-202606-mapping.csv \
  --report-output /tmp/parkovani-permit-zone-202606-report.json \
  --dry-run --strict
```

Kandidátní data nepropagujte, dokud nejsou zkontrolované scénáře vzdáleností a
změny proti stávajícímu mapování.

Pro audit celé dostupné historie a návrh release manifestu použijte:

```bash
uv run python -m src.permit_zone_mapping audit-history \
  --candidate-output /tmp/parkovani-permits-history-candidate.csv \
  --mapping-output /tmp/parkovani-permits-history-mapping.csv \
  --manifest-output /tmp/parkovani-permits-history-manifest.json \
  --dry-run
```

Výstupní mapování obsahuje metodu a vzdálenost každého přiřazení, prostorové
remízy, zdroj náhradních souřadnic a změny proti předchozímu měsíci. Manifest
obsahuje hash vstupů a kandidátů i strojově čitelné mezery v pokrytí. Zůstává
návrhem a není určený k publikaci, dokud nejsou schválené prostorové výjimky a
postup pro měsíce bez parkovacích dat.

`join.py` navíc záměrně odmítne duplicitní klíče nebo oprávnění přiřazená k
úseku, který v daném měsíci není v parkovacích datech. Chybu mapování je nutné
vyřešit před spojením; data se už nesmí tiše zahodit.

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
