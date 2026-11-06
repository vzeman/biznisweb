# VEVO: vracajúci sa zákazníci k 5. novembru 2026

Výsledok za dostupnú históriu 3. 5. 2025 – 5. 11. 2026 je **19,39 % zákazníkov s aspoň dvoma objednávkami: 1 260 / 6 498**. Zákazníka identifikuje e-mail po odstránení okrajových medzier a zjednotení veľkých a malých písmen. Rovnaký človek s rôznymi e-mailovými adresami zostáva viacerými zákazníkmi.

Produkčný kohortový súhrn uvádza **19,2 % = 1 252 / 6 509**, pretože táto konkrétna výpočtová cesta nezjednocuje zápis e-mailov. Zjednotenie spojilo 11 skupín e-mailov a zvýšilo počet opakovane nakupujúcich o 8. Produkčný výpočet ani dáta sa v tejto analýze nemenili.

## Čo jednotlivé percentá znamenajú

| Ukazovateľ | Výsledok | Základ výpočtu |
| --- | ---: | --- |
| Zákazníci s opakovaným nákupom | 19,39 % | 1 260 / 6 498 unikátnych zákazníkov |
| Opakované objednávky | 25,21 % | 2 190 / 8 688 objednávok; druhá a každá ďalšia |
| Tržby z opakovaných objednávok | 43,78 % | 64 228,35 / 146 717,31 EUR netto |
| Opakované objednávky za posledných 30 dní | 23,56 % | 155 / 658, obdobie 7. 10. – 5. 11. 2026 |
| Zákazníci s aspoň 3 nákupmi | 469 | 7,22 % zákazníkov |
| Zákazníci s aspoň 5 nákupmi | 106 | 1,63 % zákazníkov |

Medzi zákazníkmi, ktorí sa už vrátili, je medián času do druhého nákupu **52,91 dňa**, priemer **88,77 dňa**. Tieto časy nehovoria, že sa typický nový zákazník určite vráti. Zahŕňajú iba tých, ktorí druhý nákup už uskutočnili. Sedem druhých objednávok vzniklo do 24 hodín; analýza ich bez ďalších dôkazov nevyraďuje ako duplicity.

## Návrat pri rovnakej dĺžke sledovania

Každý riadok zahŕňa iba zákazníkov, pri ktorých už uplynulo celé uvedené okno od prvého nákupu. Populácie jednotlivých riadkov sa preto líšia.

| Druhý nákup najneskôr do | Vrátilo sa | Dostatočne sledovaných zákazníkov | Percento |
| --- | ---: | ---: | ---: |
| 30 dní | 388 | 5 995 | 6,47 % |
| 60 dní | 649 | 5 577 | 11,64 % |
| 90 dní | 790 | 5 427 | 14,56 % |
| 180 dní | 930 | 4 908 | 18,95 % |
| 365 dní | 686 | 2 993 | 22,92 % |

Produkčné pole `true_retention_2nd_pct = 22.0` zahŕňa celoživotné návraty kohort starších ako 90 dní. **Nie je to podiel návratov do 90 dní** a nemá rovnaký čas pozorovania pre každú kohortu. Preto sa nepoužíva ako náhrada za vyššie vypočítaných 14,56 %.

## Kohorty podľa mesiaca prvého nákupu

„Doteraz“ znamená ľubovoľný druhý nákup do 5. 11. 2026. Staršie kohorty mali viac času. Časové okno sa zobrazí iba vtedy, ak uplynulo u všetkých členov danej kohorty. Pomlčka znamená nedozreté údaje, nie nulovú retenciu. Máj 2025 začína 3. mája; november 2026 obsahuje iba prvých päť dní.

| Kohorta | Zákazníci | Vrátilo sa | Doteraz | Do 30 dní | Do 60 dní | Do 90 dní | Do 180 dní |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2025-05 | 562 | 130 | 23,13 % | 5,16 % | 9,79 % | 12,81 % | 16,55 % |
| 2025-06 | 228 | 77 | 33,77 % | 6,58 % | 12,72 % | 18,42 % | 24,56 % |
| 2025-07 | 740 | 189 | 25,54 % | 8,78 % | 14,19 % | 17,70 % | 22,16 % |
| 2025-08 | 618 | 142 | 22,98 % | 6,31 % | 11,00 % | 13,75 % | 18,93 % |
| 2025-09 | 478 | 109 | 22,80 % | 6,69 % | 12,76 % | 15,06 % | 19,67 % |
| 2025-10 | 306 | 55 | 17,97 % | 3,92 % | 7,84 % | 8,82 % | 13,07 % |
| 2025-11 | 288 | 56 | 19,44 % | 5,56 % | 9,72 % | 12,15 % | 17,01 % |
| 2025-12 | 218 | 42 | 19,27 % | 4,59 % | 10,55 % | 12,39 % | 15,14 % |
| 2026-01 | 521 | 117 | 22,46 % | 8,45 % | 12,86 % | 15,36 % | 19,58 % |
| 2026-02 | 270 | 61 | 22,59 % | 6,30 % | 12,59 % | 17,78 % | 20,37 % |
| 2026-03 | 286 | 64 | 22,38 % | 7,69 % | 13,64 % | 16,78 % | 20,98 % |
| 2026-04 | 306 | 57 | 18,63 % | 5,88 % | 9,80 % | 13,40 % | 17,97 % |
| 2026-05 | 313 | 55 | 17,57 % | 8,31 % | 12,78 % | 14,06 % | — |
| 2026-06 | 236 | 40 | 16,95 % | 3,39 % | 8,90 % | 12,71 % | — |
| 2026-07 | 55 | 9 | 16,36 % | 9,09 % | 14,55 % | 14,55 % | — |
| 2026-08 | 113 | 12 | 10,62 % | 7,08 % | 9,73 % | — | — |
| 2026-09 | 378 | 20 | 5,29 % | 3,44 % | — | — | — |
| 2026-10 | 487 | 25 | 5,13 % | — | — | — | — |
| 2026-11 | 95 | 0 | 0,00 % | — | — | — | — |

## Vývoj objednávok a aktívnych zákazníkov

„Z predošlých mesiacov“ je počet unikátnych nakupujúcich, ktorých prvý nákup nastal pred začiatkom daného mesiaca. Je to iný ukazovateľ než opakované objednávky, ktoré zahŕňajú aj druhý nákup nového zákazníka v tom istom mesiaci.

| Mesiac | Noví zákazníci | Z predošlých mesiacov | Podiel z predošlých mesiacov | Objednávky | Opakované objednávky | Podiel opakovaných objednávok |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2025-05 | 562 | 0 | 0,00 % | 568 | 6 | 1,06 % |
| 2025-06 | 228 | 35 | 13,31 % | 270 | 42 | 15,56 % |
| 2025-07 | 740 | 41 | 5,25 % | 797 | 57 | 7,15 % |
| 2025-08 | 618 | 111 | 15,23 % | 760 | 142 | 18,68 % |
| 2025-09 | 478 | 111 | 18,85 % | 606 | 128 | 21,12 % |
| 2025-10 | 306 | 127 | 29,33 % | 439 | 133 | 30,30 % |
| 2025-11 | 288 | 98 | 25,39 % | 395 | 107 | 27,09 % |
| 2025-12 | 218 | 105 | 32,51 % | 329 | 111 | 33,74 % |
| 2026-01 | 521 | 125 | 19,35 % | 668 | 147 | 22,01 % |
| 2026-02 | 270 | 135 | 33,33 % | 423 | 153 | 36,17 % |
| 2026-03 | 286 | 124 | 30,24 % | 435 | 149 | 34,25 % |
| 2026-04 | 306 | 146 | 32,30 % | 462 | 156 | 33,77 % |
| 2026-05 | 313 | 125 | 28,54 % | 455 | 142 | 31,21 % |
| 2026-06 | 236 | 166 | 41,29 % | 407 | 171 | 42,01 % |
| 2026-07 | 55 | 120 | 68,57 % | 180 | 125 | 69,44 % |
| 2026-08 | 113 | 107 | 48,64 % | 227 | 114 | 50,22 % |
| 2026-09 | 378 | 126 | 25,00 % | 516 | 138 | 26,74 % |
| 2026-10 | 487 | 126 | 20,55 % | 635 | 148 | 23,31 % |
| 2026-11 | 95 | 21 | 18,10 % | 116 | 21 | 18,10 % |

## Interpretácia

- Podiel opakovaných objednávok v júli 2026 dosiahol 69,44 %, ale nových zákazníkov bolo iba 55. V júni ich bolo 236 a v októbri 487. Počet nakupujúcich z predošlých mesiacov bol v júli 120 a v októbri 126. Vysoký júlový podiel preto sám o sebe nepreukazuje lepšiu retenciu; výrazne ho ovplyvnilo zloženie zákazníkov.
- Kohorty 1. štvrťroka 2026 mali návrat do 90 dní 176 / 1 077 = **16,34 %**. Kohorty 2. štvrťroka mali 115 / 855 = **13,45 %**, rozdiel **−2,89 percentuálneho bodu**. Je to popisný signál oslabenia, nie dôkaz konkrétnej príčiny alebo štatistickej významnosti.
- September 2026 mal 13 / 378 = **3,44 %** návratov do 30 dní. Samostatne SK: 8 / 261 = **3,07 %**, CZ: 2 / 50 = 4,00 %, HU: 3 / 67 = 4,48 %. Slabší skorý návrat teda nemožno pripísať iba pridaniu nových zahraničných trhov.
- Jún 2025 má najvyšší doterajší návrat 33,77 %, ale dlhšie pozorovanie. Pri rovnakých 90 dňoch dosiahol 18,42 %. Októbrových 5,13 % doteraz nemožno priamo porovnávať s týmto celoživotným výsledkom; celá októbrová kohorta ešte nemá ani 30 dní sledovania.
- Opakované objednávky tvoria 25,21 % počtu objednávok, ale 43,78 % netto tržieb. Ekonomická váha návratov je teda vyššia než ich podiel na objednávkach.

Odporúčaný ďalší analytický krok je rozložiť septembrovú a júnovú kohortu podľa prvého produktu, vzorka/plné balenie a dostupného akvizičného zdroja. Pri sledovaní vývoja oddeliť podiel opakovaných objednávok od 30-/60-/90-dňového návratu nových zákazníkov. Táto analýza neurčuje príčinu poklesu ani nespúšťa marketingové zmeny.

## Zdroj, rozsah a reprodukcia

- Súkromný S3 bucket: `biznisweb-reporting-artifacts-919341186960-eu-central-1`.
- Publikácia: `daily-reports/vevo/20261106T003048Z/`, vytvorená 6. 11. 2026 o 00:30:50 UTC (01:30:50 Europe/Bratislava).
- Export: `export_20250503-20261105.csv`, SHA-256 `ffbfbff7996914de408d9c5780b995872a69d4e232f359e699c1e2c8363f8485`.
- Payload: `dashboard_payload_latest.json`, SHA-256 `8fd24787e748418fb7824bc70774a956aef3ae53403c0fbc1f753a42a1d4ef98`; zhoduje sa s publikovaným manifestom.
- Kontrolný report: `data_quality_20250503-20261105.json`, SHA-256 `4716dfcf3b12b85e80311c442dc74db0c245555cad735d16419f6cf5eb7e70d3`.
- Objednávky sú tie, ktoré existujúci reporting považuje za realizované. Nie je to register všetkých vytvorených objednávok ani výlučne doručených zásielok: 8 671 má stav Shipped, 12 Payment online - paid a 5 New order. Stav sa v analýze nemení.
- Zdroj má 15 979 položkových riadkov, deduplikovaných na 8 688 objednávok. Žiadny prázdny zákaznícky e-mail, rozporné údaje jednej objednávky ani rovnaký čas dvoch objednávok jedného normalizovaného zákazníka. Historický prvý nákup z exportu zodpovedá prvému nákupu v tomto exporte. Dáta pred 3. 5. 2025 sa nepredpokladajú.
- Kontroly zdroja: `is_partial=false`, 0 QA zlyhaní a 6 upozornení týkajúcich sa geografického priradenia, nákladov a stability marže. Počet objednávok a netto tržby presne zodpovedajú produkčnému súhrnu.
- Okná používajú presný uplynutý čas (najviac N × 24 hodín) a koniec reportu, nie dnešný systémový dátum. Timestamps sa interpretujú tak, ako sú uložené v exporte; bez domyslenej zmeny časového pásma.
- Súčet kohort, mesačných objednávok a návratov sa zhoduje s celkovými hodnotami. Nezávislý výpočet cez štandardnú knižnicu Pythonu potvrdil hlavné počty a všetkých päť okien. Test s umelými dátami overuje zjednotenie e-mailov, deduplikáciu položiek, presnú hranicu 30 dní, nedozreté kohorty a odlíšenie nových a existujúcich zákazníkov. Ruff a kontrola diffu prešli.
- Agregované výsledky bez osobných údajov: [audits/vevo-retention-20261106.json](../audits/vevo-retention-20261106.json).
- Reprodukcia: `python scripts/analyze_customer_retention.py --csv <private-export.csv> --payload <private-payload.json> --output <aggregate-result.json>`. Súkromné vstupy načítať z presných kľúčov vyššie a overiť ich hashe. Surové objednávky ani e-maily necommitovať.

Všetky vzdialené operácie v tejto analýze boli čítacie. Žiadny deploy, prepočet produkčného reportu, odoslanie e-mailu alebo zmena infraštruktúry. Žiadny lokálny server, worker ani tunel nebol spustený.

Chart validation: all19 plotted values were independently matched to the aggregate JSON, and fragment JSON/DOM references were inspected. The bundled offline checker could not execute its python3 helper on this Windows host (first EACCES, then missing Python launcher); no browser rendering claim is made. No server or persistent process was started for validation.

## Odporúčaná jedna prevádzková metrika — diskusia 6. 11. 2026

Na otázku, zlepšenie ktorej jednej metriky by malo najväčší prínos, odporúčam ako pracovnú prioritu **podiel nových zákazníkov s druhým realizovaným nákupom do 90 dní od prvého**. Toto je odporúčanie, nie používateľom schválený cieľ ani dokázaná najvyššia návratnosť investície.

Východisková hodnota je **790 / 5 427 = 14,5568 %**, pričom každý zákazník v menovateli má celé 90-dňové sledovanie. Pracovná ambícia **20 %** je interný návrh pre experiment, nie odvetvový benchmark ani prognóza. Kohorty treba naďalej oddeľovať podľa mesiaca prvého nákupu a krajiny. Štandardný princíp kohorty podľa prvého nákupu a sledovania ďalších nákupov potvrdzuje aj [Shopify Help: Customer cohort analysis](https://help.shopify.com/en/manual/reports-and-analytics/shopify-reports/report-types/default-reports/customers-reports#customer-cohort-analysis).

Dôvody vyplývajúce z rovnakého exportu po normalizácii e-mailov:

| Nákup | Počet | Priemerné netto tržby | Priemerné náklady na produkty | Rozdiel pred ostatnými nákladmi |
| --- | ---: | ---: | ---: | ---: |
| Prvý | 6 498 | 12,6945 EUR | 3,1120 EUR | 9,5826 EUR |
| Druhý | 1 260 | 27,3561 EUR | 6,2156 EUR | 21,1405 EUR |
| Druhý do 90 dní, aj v ešte nedozretých kohortách | 847 | 27,5330 EUR | 6,2698 EUR | 21,2633 EUR |

Metóda: deduplikovať položky podľa order_num, normalizovať customer_email cez strip/lower, zoradiť objednávky podľa dátumu v rámci zákazníka, prideliť poradie a priemerovať order_revenue_net. Produktový náklad objednávky je súčet total_expense jej položiek. Tretí riadok je iba opis hodnoty už pozorovaných druhých objednávok; jeho 847 zákazníkov nenahrádza 790 úspechov v plne sledovanej populácii hlavnej metriky. Rozdiel po produktových nákladoch nie je čistý zisk: chýbajú ďalšie náklady na doručenie, platby, balenie, marketing a prevádzku; náklady na produkty zároveň obsahujú existujúce reportingové odhady.

Publikovaný sample_funnel.summary uvádza **4 694 zákazníkov so vstupnou objednávkou iba vzoriek**, teda **72,2 %**, s priemernou vstupnou hodnotou **7,12 EUR netto**. Hlavná oblasť na skúmanie je preto prechod zo vzoriek na plné balenie. Z publikovaných percent sample-funnel tu neodvodzujem nový časovo očistený benchmark.

Ilustrácia pri nezmenenom počte 1 000 nových zákazníkov: rast z 14,5568 % na 20 % znamená približne **54 dodatočných zákazníkov s druhým nákupom do 90 dní**. Ak by ich druhé nákupy mali historickú priemernú hodnotu 27,3561 EUR, priniesli by asi **1 489 EUR netto tržieb** v tomto okne. Nezahŕňa to ďalšie nákupy, náklady na vyvolanie návratu ani možné presunutie nákupov z neskoršieho obdobia. Je to scenár, nie predikcia dodatočného celoživotného obratu alebo zisku.

Ekonomické podmienky: rast návratnosti musí priniesť dodatočný príspevok po zľavách, produktoch, variabilných nákladoch a nákladoch na komunikáciu/reklamu. Nesmie byť dosiahnutý obmedzením získavania nových zákazníkov alebo účelovým rozdelením jednej objednávky na dve. Všeobecný mesačný podiel opakovaných objednávok je na tento účel slabší, pretože rastie aj pri poklese akvizície, ako ukázal júl.

Limity voči alternatívam: nepoznáme cenu a dosiahnuteľnosť zmeny retencie oproti zníženiu CAC alebo zvýšeniu hodnoty prvého košíka. Pri rovnakých 1 000 nových zákazníkoch by napríklad skutočné zníženie akvizičného nákladu o 1 EUR ušetrilo 1 000 EUR, ak by zostal objem a kvalita zákazníkov rovnaká. Preto nemožno iba z historickej korelácie dokázať, že retencia má najvyššiu návratnosť realizačného úsilia. Odporúčanie treba overiť kontrolovaným experimentom s nezmenenými definičnými pravidlami a finančným vyhodnotením.

Žiadny experiment, kampaň, oslovenie zákazníkov, rozpočtová zmena ani produkčná úprava neboli vykonané. Odporúčanie je uložené pre ďalšiu diskusiu.
