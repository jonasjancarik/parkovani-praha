# Předání: publikaci nového datasetu blokuje neověřené propojení oprávnění s parkovacími úseky

read_when: regenerating parking or permit data, changing building-to-zone mapping, joining datasets, or publishing a new app dataset.

Stav k 13. srpnu 2026. Zdrojová data sahají do června 2026.

## Vyčištěná data se zatím nesmí částečně publikovat

Zpracování parkovacích dat, detekce anomálií a zobrazení časových řad už mají
přísnější kontroly. Aplikace však stále používá dříve vytvořený soubor
`data/processed/data_parking_and_permits.csv`. Nově připravené kandidáty jsme
záměrně nevydali, protože dosud nemáme bezpečný způsob, jak pro každý měsíc
přiřadit všechna parkovací oprávnění právě jednomu platnému úseku.

Červen 2026 ukazuje, proč nelze opravit jen jednu polovinu výsledku:

| Varianta | Oprávnění | Parkovací místa | Oprávnění na místo |
| --- | ---: | ---: | ---: |
| Aktuálně zobrazovaný soubor | 218 542 | 162 976 | 1,341 |
| Vyčištěná kapacita se starým počtem oprávnění | 218 542 | 166 705 | 1,311 |
| Kontrolní přepočet úplného zdrojového součtu | 228 529 | 166 705 | 1,371 |

Prostřední řádek by byl zavádějící: zvýšil by jmenovatel o 3 729 míst, ale ponechal
by neúplný čitatel. Poslední řádek pouze dokládá velikost rozdílu; není to
schválená produkční hodnota, protože nové prostorové přiřazení ještě neprošlo
kontrolou. Kapacitu a oprávnění proto musíme publikovat společně jako jeden
ověřený release.

## Bezpečný nový release blokují první čtyři problémy; další dva vyžadují zveřejnit omezení

### 1. Statické mapování budov je zastaralé a mění součty oprávnění

Produkční zpracování stále používá `data/buildings_useky_mapping.csv`, naposledy
aktualizovaný 16. prosince 2024, přestože zdrojová data pokračují do června 2026.
Soubor má 43 847 řádků pro 43 563 budov; některé budovy tedy míří na více úseků.
Vstupní spojení nehlídá vztah jedna budova ku jednomu úseku, a může tak současně
oprávnění zahazovat i násobit.

V červnu 2026:

- 1 795 z 38 436 budov ve zdroji v mapování chybí; nesou 3 045 oprávnění,
- duplicitní stará přiřazení přidávají do mezivýsledku 1 467 oprávnění,
- 8 409 oprávnění je přiřazeno k úsekům, které v daném měsíci nejsou aktivní,
- soubor po mapování obsahuje 226 951 oprávnění, zatímco zdroj obsahuje 228 529,
- po spojení s aktivními parkovacími úseky aplikace zobrazí jen 218 542; proti
  zdroji chybí 9 987 oprávnění, tedy 4,37 %.

Napříč celou historií má zpracovaný soubor 20 023 361 oprávnění proti
19 940 273 ve zdrojových budovách. Rozdíl je +83 088 (+0,42 %) a žádný měsíc se
zdrojem přesně nesouhlasí. Malý celkový rozdíl proto není známkou správnosti;
ztráty a duplicity se navzájem částečně ruší.

Aktuální ochrana: [join.py](../join.py) nově odmítne sestavit release, pokud
oprávnění míří na neaktivní úsek. Starý sloučený soubor vznikl před touto
kontrolou, takže jej dnes nelze ze současných vstupů znovu vytvořit.

### 2. Celkem 48 051 měsíčních řádků s oprávněními nemá odpovídající aktivní úsek

Kontrola celého `data_permits_by_zone.csv` našla 48 051 kombinací úsek–měsíc,
které nejsou v parkovacích datech aktivní. Tyto řádky dohromady nesou 4 395 044
oprávnění, neboli 21,95 % všech 20 023 361 měsíčně sečtených oprávnění v tomto
souboru. Jde o součet přes celou časovou řadu, nikoli o počet oprávnění v jediném
měsíci.

Příčiny se dělí na dvě skupiny:

- 19 538 řádků a 1 330 461 oprávnění připadá na měsíce, pro které parkovací
  zdroj zcela chybí: prosinec 2024 a říjen až prosinec 2025,
- i v měsících s parkovacími pozorováními míří 28 513 řádků a 3 064 583
  oprávnění na neaktivní kódy úseků.

Chybějící měsíce se nesmějí interpolovat ani tiše přeskočit. Release musí buď
doplnit parkovací zdroj, nebo oba druhy dat pro stejné měsíce vynechat a tuto
mezeru uvést v manifestu pokrytí.

### 3. Nové měsíční mapování přesně sčítá oprávnění, ale prostorové výjimky nejsou schválené

Audit v [src/permit_zone_mapping.py](../src/permit_zone_mapping.py) pro červen
2026 používá souřadnice budov a geometrie aktivních úseků ze stejného měsíce.
Kandidát má právě jedno přiřazení pro všech 38 436 budov, nulový počet
neaktivních cílů a přesně reprodukuje všech osm typů oprávnění i celkový součet
228 529.

Strukturální správnost však ještě nedokazuje věcně správný výběr úseku:

- 5 254 z 36 641 budov společných se starým mapováním změnilo úsek (14,34 %),
- 2 budovy použily poslední dostupné starší souřadnice,
- 35 přiřazení mělo shodnou nejkratší vzdálenost k více geometriím,
- 3 895 budov s 9 881 oprávněními leží více než 100 m od přiřazeného úseku,
- 2 475 budov s 5 228 oprávněními leží více než 500 m daleko,
- 1 491 budov s 3 356 oprávněními leží více než 1 km daleko; maximum je
  9 086,99 m.

Příkaz `--strict` nyní hlídá úplné součty a aktivní cíle, ale neblokuje vysokou
vzdálenost, změny proti starému mapování, použití náhradních souřadnic ani
prostorové remízy. Audit je proto záměrně jen dry run do `/tmp`; nelze jej použít
jako automatické schválení produkčních dat.

### 4. Aktuálně zobrazovaný soubor vznikl před opravou anomálií a porušuje vlastní datové vztahy

Ve stávajícím `data_parking_and_permits.csv`:

- 159 111 řádků porušuje vztah `navstevnici = platici + neplatici + prenosna`,
- 464 549 řádků neodpovídá uloženému součtu typů parkujících,
- 175 řádků má kapacitu ZPS vyšší než celkovou kapacitu,
- 606 logických klíčů je duplicitních; jde o 1 500 řádků, tedy 894 kopií navíc.

Počty porušených početních vztahů používají stejné porovnání jako produkční
validátor (`atol=1e-8`, bez relativní tolerance), nikoli prostou shodu textového
zápisu desetinných čísel.

Nové zpracování už nejprve čistí primitivní hodnoty, následně přepočítává
odvozené sloupce a před zápisem vynucuje jedinečnost i početní identity. Tyto
opravy ale nejsou ve zobrazovaném souboru, protože samostatná publikace čisté
kapacity by vytvořila výše popsaný nesourodý poměr.

### 5. Mapování na ZSJ je staré a 93 úseků zůstává nezařazených

`data/useky_zsj_mapping.csv` byl naposledy aktualizován 22. července 2024. V
novém parkovacím kandidátovi chybí mapování pro 2 633 řádků v 93 úsecích. Řádky
se už nezahazují a zůstávají označené jako `Nezařazený úsek`, takže městské a
obvodní součty zůstávají úplné. Analýza na úrovni ZSJ je ale do regenerace a
kontroly mapování neúplná.

Toto omezení nemusí blokovat městské a obvodní metriky, musí však zůstat jasně
uvedené všude, kde se nabízí rozpad podle ZSJ.

### 6. Některé zdrojové anomálie zůstávají správně jen označené k revizi

Nový audit označuje 23 126 podezřelých dynamických pozorování a 193 řádků, kde
je kapacita ZPS vyšší než celková kapacita. Automaticky je nemění, protože ze
zdroje nelze bezpečně určit správnou náhradu. Jediná automatická kapacitní
oprava se provede jen při doloženém, dočasném a souhlasném skoku obou kapacit;
přitom zachovává původní počet obsazených míst a z něj přepočítá obsazenost.

Tento backlog není důvodem k plošnému vyhlazení nebo přepisování naměřených
hodnot. Je však třeba uchovat auditní report u release a případně jednotlivé
případy potvrdit proti zdroji.

Podle současné politiky jde o reportované zdrojové riziko, ne automatický
blokátor release: publikaci blokuje až porušení strukturálních bran nebo
chybějící auditní záznam.

## Doporučený další cíl je vytvořit publikovatelné měsíční mapování oprávnění na úseky

**Cíl:** Pro každý měsíc deterministicky vytvořit úplné a auditovatelné
přiřazení budova → aktivní parkovací úsek, z něj sestavit oprávnění po úsecích a
bez výjimek projít kontrolou celého finálního spojení. Teprve potom společně
publikovat vyčištěná parkovací data a přepočtená oprávnění.

### Historický dry run a návrh manifestu jsou implementované, schválení stále chybí

Příkaz `audit-history` nyní projde všechny dostupné měsíční exporty budov a pro
každý měsíc s parkovacími daty vytvoří mapování pouze na tehdy aktivní úseky.
Každý řádek mapování uchovává použitou metodu (`polygon_intersection` nebo
`nearest_active_zone`), vzdálenost, zdrojový měsíc souřadnice, příznak náhradní
souřadnice, prostorovou remízu a změnu proti bezprostředně předchozímu měsíci.
Algoritmus nejprve vybere průnik bodu s polygonem; nejbližší aktivní úsek používá
jen jako výslovně označený fallback. Remízu řeší deterministicky podle kódu
úseku, ale tím ji automaticky neschvaluje.

Dry run současně vytváří návrh verzovaného JSON manifestu a jeho soubor
`.sha256`. Manifest obsahuje SHA-256 všech použitých vstupů a kandidátů,
měsíční součty, pokrytí, metody, vzdálenosti, remízy, náhradní souřadnice a změny
mapování. Čtyři chybějící měsíce zůstávají strojově uvedené jako neauditované;
`release_decision.ready` je proto vždy `false` a manifest má stav
`draft_review_required`. Implementace záměrně neumí návrh sama povýšit na
produkční release bez schválené politiky pokrytí a prostorových výjimek.

Úplný dry run nad současnými vstupy prošel strukturálně ve všech 83 měsících,
které mají parkovací data: vytvořil 2 883 733 přiřazení budova–měsíc a 364 413
řádků úsek–měsíc, přesně zachoval měsíční součty, nevytvořil žádný neaktivní cíl
ani duplicitní klíč. Současně potvrdil čtyři očekávané mezery v pokrytí. Pro
prostorovou revizi eviduje 767 877 použití nejbližšího úseku, 4 218 remíz, 224
použití starší souřadnice a 138 905 přiřazení vzdálených přes 1 km. Tyto
historické počty jsou počty měsíčních přiřazení, nikoli unikátních budov, a
ukazují, že kontrolu nelze omezit jen na červen 2026.

### Navržené pořadí práce

1. **Rozšířit měsíční audit na celou historii — implementováno jako dry run.** Pro každý měsíc použít tehdejší
   souřadnice budov a aktivní geometrie úseků. Průnik s polygonem má přednost;
   nejbližší úsek je pouze označený fallback. Výsledek musí obsahovat metodu,
   vzdálenost, použití starší souřadnice a případnou remízu.
2. **Stanovit pravidla prostorové kontroly.** Odpovědný reviewer musí schválit
   prahy pro vzdálenost, remízy a změny proti předchozímu měsíci. Všech 1 491
   červnových případů nad 1 km je nutné opravit, doložit nebo jednotlivě
   prominout; libovolný nový vzdálenostní limit bez revize nestačí.
3. **Rozhodnout o čtyřech chybějících měsících.** Preferovaná varianta je získat
   a zpracovat chybějící parkovací zdroje. Pokud to není možné, manifest musí
   výslovně vynechat prosinec 2024 a říjen až prosinec 2025 z obou datových sad.
4. **Vytvořit verzovaný release manifest.** Uložit hash vstupů a kandidáta,
   verzi algoritmu, počty řádků, součty všech typů oprávnění po měsících,
   pokrytí, vzdálenostní rozdělení, fallbacky, remízy a schválené výjimky.
5. **Porovnat dopad před publikací.** Zkontrolovat město, městské části a
   reprezentativní úseky; každý podstatný skok musí být vysvětlen změnou zdroje
   nebo konkrétní opravou mapování, ne jen novým algoritmem.
6. **Publikovat oba jmenovatele i čitatele najednou.** Nahradit release artefakty,
   znovu sestavit sloučený soubor, spustit aplikační testy a teprve poté jej
   zahrnout do image/deploymentu.

### Cíl je splněn pouze tehdy, když projdou všechny release podmínky

- [ ] Každá budova s oprávněními má v daném měsíci právě jeden cíl.
- [ ] Každý cílový úsek je ve stejném měsíci aktivní.
- [ ] Každý typ oprávnění i celkový součet se po měsících přesně rovná zdroji.
- [ ] Neexistují duplicitní klíče úsek–měsíc v souboru oprávnění.
- [ ] Chybějící měsíce mají schválený a strojově čitelný coverage manifest.
- [ ] Vzdálenosti, remízy, náhradní souřadnice a změny přiřazení splňují
      schválené limity; všechny výjimky jsou dohledatelné v manifestu.
- [ ] `join.py` projde nad celým releasem s nulovým počtem neaktivních cílů.
- [ ] Finální sloučený soubor má jedinečné logické klíče a všechny odvozené
      početní vztahy jsou beze zbytku platné.
- [ ] CI obsahuje reálnou regresní ukázku změny starého a nového mapování a před
      vytvořením image spouští úplnou validační kontrolu spojení.
- [ ] Aplikace byla zkontrolována nad novým releasem včetně mezer v časové řadě,
      součtů městských částí a textu o pokrytí.

## Tyto zkratky zůstávají mimo rozsah bezpečného řešení

- Nevydávat jen vyčištěnou kapacitu se starým počtem oprávnění.
- Nevypínat kontrolu v `join.py` a nezahazovat nepřiřazená oprávnění levým
  spojením.
- Nenahrazovat naměřenou obsazenost a složení parkujících obecným vyhlazením.
- Nezvolit vzdálenostní limit bez kontroly skutečných odlehlých případů.
- Neprohlásit přesný městský součet za důkaz správného přiřazení jednotlivých
  budov.

## Audit lze zopakovat bez změny produkčních dat

Červnový kandidát se vytvoří do dočasných souborů:

```bash
uv run python -m src.permit_zone_mapping audit \
  --month 202606 \
  --candidate-output /tmp/parkovani-permits-202606-candidate.csv \
  --mapping-output /tmp/parkovani-permits-202606-mapping.csv \
  --report-output /tmp/parkovani-permit-zone-202606-report.json \
  --dry-run \
  --strict
```

Dočasné kandidáty v `/tmp` nejsou release artefakty ani trvalý auditní záznam.
Schválený workflow musí manifest a jeho hash uložit společně s výslednou sadou.
Návrh pro celou historii vznikne takto:

```bash
uv run python -m src.permit_zone_mapping audit-history \
  --candidate-output /tmp/parkovani-permits-history-candidate.csv \
  --mapping-output /tmp/parkovani-permits-history-mapping.csv \
  --manifest-output /tmp/parkovani-permits-history-manifest.json \
  --dry-run
```

Vedle manifestu vznikne
`/tmp/parkovani-permits-history-manifest.json.sha256`. Příkaz lze omezit pomocí
`--start-month` a `--end-month`. Volba `--strict` navíc vrátí nenulový stav,
dokud některý zdrojový měsíc nemá parkovací data nebo neprojde jiná strukturální
brána; při současných vstupech je tedy její selhání očekávané a kandidát se
nesmí publikovat.

Po implementaci celého cíle má závěrečná kontrola zahrnout minimálně:

```bash
uv run pytest -q
uv run python join.py
```

## K budoucímu rozhodnutí zůstávají tři otázky

1. Kdo schvaluje prostorové výjimky a jaké limity platí pro 100 m, 500 m a
   1 km?
2. Doplníme čtyři chybějící parkovací měsíce, nebo je synchronně vynecháme z
   obou datových sad?
3. Je úplné mapování ZSJ podmínkou stejného release, nebo navazující opravou s
   jasně označenou neúplností ZSJ analýzy?
