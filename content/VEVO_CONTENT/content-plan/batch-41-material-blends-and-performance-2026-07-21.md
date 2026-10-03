# VEVO batch 41: zmesi vlákien a úžitkové vlastnosti textilu

Date: 2026-07-21
Cluster: C09A - materiály, vlastnosti textilu a praktická starostlivosť
Planned article count: 4

## Východisková kontrola

- Kandidáti boli porovnaní s aktívnym obsahom v blokoch Blog (765), FAQ (774) a Slovník/Encyklopédia (1905).
- Prvý návrh obsahoval témy o polyesteri s elastanom a viskóze s elastanom. Duplicate guard ich zastavil pre príliš blízke existujúce články; nebol použitý žiadny override.
- Náhradné témy o statickej elektrine a odolnosti proti oderu rozširujú cluster o doteraz samostatne nespracované úžitkové vlastnosti.
- Finálny duplicate guard prešiel pre všetky štyri názvy so stavom `ok` a bez skupín duplicitných živých názvov.
- Cieľové slugs musia pred vytvorením vracať 404. Existujúce interné odkazy, produkt, kategória a odborné zdroje musia vracať úspešnú odpoveď.

## Články

1. Bavlna a elastan: starostlivosť o tričká, rifle a spodnú bielizeň
   - Oddeľuje úlohu bavlny, elastanu a konštrukcie látky; vysvetľuje, prečo percento elastanu samo neurčuje pružnosť ani životnosť.
   - Rozlišuje zrazenie rozmerov od straty pružného návratu a rieši tričká, strečové rifle aj spodnú bielizeň.

2. Vlna a polyamid: prečo sa miešajú vlákna a ako to ovplyvňuje pranie
   - Vysvetľuje, ako môže polyamid zvýšiť pevnosť a odolnosť vlneného výrobku bez toho, aby automaticky zmenil vlnu na bežnú syntetiku.
   - Rozlišuje vláknové zloženie, konštrukciu priadze, úpravu proti plstnateniu a povolený spôsob prania.

3. Statická elektrina v oblečení: prečo látky priľnú a ako obmedziť iskrenie
   - Vysvetľuje elektrizovanie pri kontakte a trení, vplyv suchého vzduchu, materiálov, vrstvenia, obuvi a presušenia v sušičke.
   - Ponúka bezpečný postup od úpravy sušenia po kontrolu prostredia a oddeľuje bežné nepohodlie od prostredí s horľavinami alebo citlivou elektronikou.

4. Odolnosť textilu proti oderu: čo znamená Martindale pri oblečení a bytových látkach
   - Vysvetľuje princíp skúšky, rozdiel medzi poškodením vzorky, stratou hmotnosti a zmenou vzhľadu.
   - Upozorňuje, že počet cyklov nie je univerzálne skóre kvality a nemožno ho zamieňať so žmolkovaním, pevnosťou šva ani odolnosťou povrchovej úpravy.

## Povinné prvky

- Laická rýchla odpoveď na začiatku a následná odborná hĺbka.
- Minimálne 1 700 verejne viditeľných slov, 16 nadpisov H2, 2 tabuľky, 8 štýlovaných blokov a 5 otázok FAQ na článok.
- Interné odkazy na existujúce materiálové definície a praktické návody, plus zmysluplné prepojenie medzi článkami dávky.
- Produktová karta: Prací gél hypoalergénny Vevo Ylang Absolute 1L.
- Kategóriová karta: pracie gély.
- Žiadna fixná cena a žiadne interné redakčné, vyhľadávacie alebo marketingové pomenovania vo verejnom texte.
- Primárne alebo autoritatívne zdroje: ASTM, AATCC, ISO, GINETEX, CottonWorks, Woolmark a NIST.

## Publikačný postup

1. Vygenerovať článkový JSON a link preflight report.
2. Spustiť duplicate, public-content, depth a HTML safety guardy.
3. Vytvoriť každý príspevok skrytý cez repo-local VEVO content MCP.
4. Overiť presný názov, slug a bohaté HTML v admin readbacku.
5. Až potom príspevok zverejniť.
6. Overiť verejnú URL, rozsah, tabuľky, karty, odkazy a absenciu poškodených jednoznakových odsekov.
