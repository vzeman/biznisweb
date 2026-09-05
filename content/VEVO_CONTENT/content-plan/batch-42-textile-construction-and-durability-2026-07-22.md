# VEVO batch 42: konštrukcia a trvácnosť textilu

Date: 2026-07-22
Cluster: C09A - materiály, konštrukcia textilu a praktická starostlivosť
Planned article count: 4

## Východisková kontrola

- Kandidáti boli porovnaní s aktívnym obsahom v blokoch Blog (765), FAQ (774) a Slovník/Encyklopédia (1905).
- Širší zásobník ôsmich tém prešiel guardom, do finálnej dávky však boli zámerne vybrané iba štyri navzájom odlišné oblasti.
- Všeobecná pevnosť, rozmerová stabilita a krčivosť boli odložené, aby batch príliš tesne nesusedil s článkom o Martindale a existujúcimi návodmi o zrážaní.
- Finálny duplicate guard prešiel pre všetky štyri názvy so stavom `ok`, bez skupín duplicitných živých názvov a bez chybných živých slugov.
- Cieľové slugs musia pred vytvorením vracať 404. Existujúce interné odkazy, produkt, kategória a odborné zdroje musia vracať úspešnú odpoveď.

## Články

1. Stálofarebnosť textilu: prečo farby blednú pri praní, svetle a trení
   - Rozlišuje farebnú stálosť pri praní, mokrom a suchom trení, svetle a kontakte s potom.
   - Vysvetľuje rozdiel medzi vyblednutím samotného kusa a zafarbením susednej textílie.

2. Pevnosť šva a posun nití: prečo oblečenie praská pri švoch
   - Oddeľuje pretrhnutie šijacej nite, roztrhnutie látky, posun priadzí a zlyhanie konštrukcie.
   - Rieši strih, prídavok na šev, hustotu stehov, ihlu, smer látky, veľkosť odevu a bezpečnú opravu.

3. Zatrhávanie textilu: prečo vznikajú vytiahnuté očká a ako im predchádzať
   - Rozlišuje vytiahnutú slučku, povrchovú deformáciu, dieru, žmolok a poškodenie šva.
   - Pokrýva úplety, filamentové tkaniny, zipsy, suché zipsy, šperky, pazúry a pranie v zmiešanej náplni.

4. Počet nití pri obliečkach: čo znamená thread count a čo o kvalite nehovorí
   - Vysvetľuje osnovu, útok, počet nití na jednotku dĺžky, jemnosť priadze, zákrut a väzbu.
   - Oddeľuje počet nití od gramáže, zloženia vlákna, dotyku, priedušnosti, rozmerovej stability a kvality šitia.

## Povinné prvky

- Laická rýchla odpoveď na začiatku a následná odborná hĺbka.
- Minimálne 1 700 verejne viditeľných slov, 16 nadpisov H2, 2 tabuľky, 8 štýlovaných blokov a 5 otázok FAQ na článok.
- Interné odkazy na existujúce materiálové definície a praktické návody, plus zmysluplné prepojenie medzi článkami dávky.
- Produktová karta: Prací gél hypoalergénny Vevo Ylang Absolute 1L.
- Kategóriová karta: pracie gély.
- Žiadna fixná cena a žiadne interné redakčné, vyhľadávacie alebo marketingové pomenovania vo verejnom texte.
- Primárne alebo autoritatívne zdroje: ISO, ASTM, AATCC, GINETEX a CottonWorks.

## Publikačný postup

1. Vygenerovať článkový JSON a link preflight report.
2. Spustiť duplicate, public-content, depth a HTML safety guardy.
3. Vytvoriť každý príspevok skrytý cez repo-local VEVO content MCP.
4. Overiť presný názov, slug a bohaté HTML v admin readbacku.
5. Až potom príspevok zverejniť.
6. Overiť verejnú URL, rozsah, tabuľky, karty, odkazy a absenciu poškodených jednoznakových odsekov.
