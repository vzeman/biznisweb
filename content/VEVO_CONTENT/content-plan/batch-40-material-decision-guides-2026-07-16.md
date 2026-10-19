# VEVO batch 40: rozhodovacie sprievodce materiálmi

Date: 2026-07-16
Cluster: C09A - materiály, vlastnosti textilu a praktický výber
Planned article count: 4

## Východisková kontrola

- Kandidáti boli porovnaní s aktívnym obsahom v blokoch Blog (765), FAQ (774) a Slovník/Encyklopédia (1905).
- Duplicate guard prešiel pre všetky štyri názvy so stavom `ok`.
- Témy nerozširujú existujúce definície jednotlivých vlákien. Vytvárajú rozhodovacie návody podľa použitia a prepájajú vlastnosti textilu s údržbou.
- Cieľové slugs musia pred vytvorením vracať 404. Existujúce interné odkazy, produkt, kategória a odborné zdroje musia vracať úspešnú odpoveď.

## Články

1. Priedušnosť, savosť a rýchloschnutie: ako čítať vlastnosti textilu
   - Rozlišuje priedušnosť vzduchu, odpor proti vodnej pare, nasiakavosť, transport kvapalnej vlhkosti a čas schnutia.
   - Vysvetľuje, prečo výsledok neurčuje iba názov vlákna, ale aj priadza, väzba, hrúbka, úprava a strih.

2. Bavlna, ľan, satén alebo flanel: aké obliečky vybrať podľa sezóny a potenia
   - Porovnáva vlákno, väzbu a povrchovú úpravu bez tvrdenia, že existuje jeden univerzálne najlepší materiál.
   - Zahŕňa letnú a zimnú prevádzku, potenie, sušenie, dotyk, zrážanie a údržbu.

3. Froté, bambus alebo mikrovlákno: ktorý uterák vybrať podľa savosti a schnutia
   - Oddeľuje konštrukciu froté od zloženia vlákna a upozorňuje, že označenie bambus často znamená viskózu z bambusovej celulózy.
   - Porovnáva kúpeľňu, cestovanie, šport, vlasy a kuchyňu podľa savosti, objemu a rýchlosti schnutia.

4. Polyester, polyamid, merino alebo elastan: z čoho má byť športové oblečenie
   - Vysvetľuje úlohu základného vlákna a menšinového elastanu v zmesi.
   - Rozhoduje podľa aktivity, teploty, dĺžky výkonu, schnutia, pachu, pružnosti a starostlivosti.

## Povinné prvky

- Laická rýchla odpoveď na začiatku a následná odborná hĺbka.
- Minimálne 1 700 verejne viditeľných slov, 8 nadpisov H2, 2 tabuľky, 6 štýlovaných blokov a 5 otázok FAQ na článok.
- Interné odkazy na existujúce definície a praktické návody, plus zmysluplné prepojenie medzi článkami dávky.
- Produktová karta: Prací gél hypoalergénny Vevo Ylang Absolute 1L.
- Kategóriová karta: pracie gély.
- Žiadna fixná cena, žiadne interné redakčné alebo marketingové pomenovania vo verejnom texte.
- Primárne alebo autoritatívne zdroje: ISO, AATCC, ASTM, GINETEX, PubMed a Európska komisia.

## Publikačný postup

1. Vygenerovať článkový JSON a link preflight report.
2. Spustiť duplicate, public-content, depth a HTML safety guardy.
3. Vytvoriť každý príspevok skrytý cez repo-local VEVO content MCP.
4. Overiť presný názov, slug a bohaté HTML v admin readbacku.
5. Až potom príspevok zverejniť.
6. Overiť verejnú URL, rozsah, tabuľky, karty, odkazy a absenciu poškodených jednoznakových odsekov.
