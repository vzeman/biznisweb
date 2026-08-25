#!/usr/bin/env python3
"""Build and validate VEVO batch 48 underused-material articles."""

from __future__ import annotations

import json
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

from build_batch_47_distinct_materials import (
    BASE,
    FIXED_PRICE_RE,
    FORBIDDEN_PUBLIC_RE,
    WORD_RE,
    article_hrefs,
    fetch_status,
    render_article,
    visible_text,
)


PUBLISH_DATE = "2026-08-25"
CANDIDATES = Path("content/VEVO_CONTENT/batches/batch-48-candidates-2026-08-25.txt")
OUT_JSON = Path("content/VEVO_CONTENT/imports/batch-48-2026-08-25-articles.json")
OUT_PREFLIGHT = Path("content/VEVO_CONTENT/exports/batch-48-2026-08-25-link-preflight.json")

EU_FIBRE_LABEL = "https://eur-lex.europa.eu/legal-content/SK/TXT/?uri=CELEX%3A02011R1007-20180215"
GINETEX = "https://www.ginetex.net/share/article/4201/care-symbols"
AATCC_DIMENSION = "https://members.aatcc.org/store/tm135/543/"
AATCC_COLOR = "https://members.aatcc.org/store/tm61/495/"
ASTM_PILLING = "https://store.astm.org/d3512_d3512m-22.html"
ASTM_ABRASION = "https://store.astm.org/d4157-13r22.html"
ASTM_TEAR = "https://store.astm.org/d1424-25.html"
AATCC_SOIL = "https://members.aatcc.org/store/tm215/3848/"
RAMIE_SHAPE = "https://www.jstage.jst.go.jp/article/fiber1944/50/10/50_10_P569/_article/-char/en"
ASAHI_BEMBERG = "https://www.asahi-kasei.co.jp/fibers/en/products_bemberg.html"
ASAHI_BEMBERG_TECH = "https://www.asahi-kasei.co.jp/fibers/en/bemberg/pdf/archives/BBleaflet%20_low.pdf"
ASAHI_BEMBERG_SUSTAINABILITY = "https://www.asahi-kasei.co.jp/fibers/en/lp/bemberg/001/"
HARRIS_TWEED = "https://www.harristweed.org/our-role/"
WOOLMARK_CARE = "https://www.woolmark.com/care/care-for-wool/"
WOOLMARK_PILLING = "https://www.woolmark.com/globalassets/_06-new-woolmark/_industry/certification/licensee-portal/gd5407-wool-care-factsheet_pilling.pdf"
COTTONWORKS_WEAVING = "https://cottonworks.com/wp-content/uploads/2023/03/Weaving-101.pdf"

ARTICLE_COTTON = "/n/co-je-bavlna-vlastnosti-vyhody-nevyhody-a-starostlivost"
ARTICLE_LINEN = "/n/co-je-lan-preco-sa-krci-ako-ho-prat-a-ako-ho-zjemnit"
ARTICLE_LINEN_COMPARE = "/n/lan-vs-bavlna-rozdiely-v-savosti-krcivosti-a-starostlivosti"
ARTICLE_VISCOSE = "/n/co-je-viskoza-vlastnosti-krcivost-zrazanie-a-starostlivost"
ARTICLE_MODAL_COMPARE = "/n/modal-vs-lyocell-vs-viskoza-ako-sa-lisia-pri-prani-a-noseni"
ARTICLE_BLEND = "/n/co-je-zmesovy-material-preco-sa-oblecenie-zraza-alebo-sprava-inak-nez-cakate"
ARTICLE_WOOL_COAT = "/n/ako-prat-jesenny-kabat-trenckot-a-lahky-vlneny-kabat-doma"
ARTICLE_WOOL_ACRYLIC = "/n/akryl-vs-vlna-zmolkovanie-teplo-zapach-a-starostlivost"
ARTICLE_PILLING = "/n/preco-sa-oblecenie-zmolkuje-vlakna-trenie-pranie-a-susenie"
ARTICLE_ABRASION = "/n/odolnost-textilu-proti-oderu-co-znamena-martindale-pri-obleceni-a-bytovych-latkach"
ARTICLE_TEAR = "/n/pevnost-textilu-v-tahu-a-proti-roztrhnutiu-co-skusky-hovoria-o-odolnosti"
ARTICLE_SHRINKAGE = "/n/preco-sa-oblecenie-zrazi-po-prani-teplota-vlakna-susicka-a-prevencia"
ARTICLE_COLOR = "/n/stalofarebnost-textilu-preco-farby-blednu-pri-prani-svetle-a-treni"
ARTICLE_LABEL = "/n/ako-citat-stitok-na-obleceni-material-symboly-prania-a-spravny-program"
ARTICLE_STAIN = "/n/ako-odstranit-zuvacku-krv-vosk-a-ine-skvrny-z-oblecenia"
ARTICLE_OIL = "/n/ako-odstranit-olejove-a-mastne-skvrny-z-oblecenia-po-prani"
ARTICLE_GEL = "/n/ako-vybrat-praci-gel-podla-typu-bielizne"
ARTICLE_DRYING = "/n/ako-susit-bielizen-v-malom-byte-bez-zatuchnutia"


RAMIE: dict[str, object] = {
    "title": "Čo je ramia: pevné rastlinné vlákno, krčivosť a pranie",
    "link": "co-je-ramia-pevne-rastlinne-vlakno-krcivost-a-pranie",
    "meta": "Čo je ramia, ako sa líši od ľanu a bavlny a ako prať, sušiť, žehliť a skladovať košele, šaty, úplety a zmesi s ramiou.",
    "short": "Ramia je prírodné lýkové celulózové vlákno s pevnosťou, hladším leskom a skôr tuhším omakom. Nízka pružnosť vysvetľuje krčivosť aj citlivosť ostrých záhybov. Praktická starostlivosť sa musí riadiť väzbou, zmesou, farbou a štítkom hotového výrobku.",
    "name": "ramia",
    "genitive": "ramie",
    "locative": "ramii",
    "construction_summary": "lýkové vlákno získavané zo stoniek rastlín rodu Boehmeria, ktoré sa po oddelení a odglejení spriada samostatne alebo mieša s inými vláknami",
    "label_details": "podiel ramie, bavlny, viskózy, ľanu alebo syntetiky, typ tkaniny či úpletu, podšívku, výšivku, farbu, plisovanie a povolený spôsob sušenia",
    "residue_place": "medzerách väzby, tuhších záhyboch, manžetách, golieri, švoch a v miestach so zvyškom odglejovacej alebo dokončovacej úpravy",
    "friction_risk": "hladké dlhšie vlákna, vystupujúcu priadzu, ostrý prehyb alebo povrch jemnej zmesi",
    "drying_advice": "Tkaný kus po praní vyrovnajte bez násilného naťahovania a sušte podľa etikety na širokej opore alebo naplocho; mäkký úplet podoprite po celej ploche. Ostrý mokrý lom nerozťahujte silou.",
    "heat_risk": "celulózové vlákna, farbivo, zvyškové pnutie, živicovú úpravu proti krčeniu, lesk povrchu a rozmer citlivejšej zložky zmesi",
    "failure_sign": "trvalé zbelenie v lome, rozídenie nití pri šve, vyťahanie úpletu alebo prasknutie oslabenej priadze",
    "answer": "Ramia je prírodné celulózové vlákno získavané z lýka rastlín Boehmeria. Môže pripomínať ľan, býva pevná, hladšia a lesklá, ale má malú pružnosť, preto sa ľahko krčí a ostrý prehyb môže namáhať priadzu. Nie každý výrobok s ramiou sa perie rovnako: košeľa zo zmesi s bavlnou, jemný úplet s viskózou a štruktúrované šaty môžu mať odlišné limity. Pred praním čítajte celé zloženie a symboly, skontrolujte farbu a švy, použite povolený šetrný cyklus s primeranou dávkou prostriedku a kus nenechávajte po skončení stlačený v bubne. Sušte ho vyrovnaný s oporou a žehlite z rubu pri povolenej teplote, ideálne ešte mierne vlhký. Ak etiketa povoľuje iba profesionálne čistenie, všeobecná odolnosť samotného vlákna tento pokyn neprepisuje.",
    "intro": "Pri otázke ako prať ramiu vzniká zmätok už pri názve. Ramia nie je ľan ani bavlna, hoci všetky tri patria medzi rastlinné celulózové vlákna a môžu vytvoriť podobný letný vzhľad. Výsledný omak výrazne mení odglejenie, jemnosť priadze, väzba a zmes. Pevné vlákno navyše automaticky neznamená nekrčivý alebo nezničiteľný odev. Naopak, menšia pružná návratnosť môže zvýrazniť lomy a pri nevhodnom sušení zdeformovať úplet. Preto treba oddeliť vlastnosť vlákna od správania hotovej košele, šiat, obrusu či pleteniny.",
    "quick": [
        "<strong>Ramia je samostatné lýkové vlákno:</strong> nie je to obchodný názov ľanu ani druh bavlny.",
        "<strong>Pevnosť nie je pružnosť:</strong> materiál môže dobre znášať ťah, ale ostro sa pokrčiť a zle sa vrátiť do pôvodného tvaru.",
        "<strong>Odglejenie mení omak:</strong> spracovanie rastlinných zložiek ovplyvňuje jemnosť, lesk aj rovnomernosť priadze.",
        "<strong>Zmes rozhoduje o praní:</strong> elastan, viskóza, vlna alebo podšívka môžu mať nižší limit než ramia.",
        "<strong>Mokrý úplet potrebuje oporu:</strong> nezdvíhajte ho za ramená a nesnažte sa tvar opraviť prudkým ťahom.",
        "<strong>Žehlite podľa etikety:</strong> mierna zvyšková vlhkosť môže pomôcť, vysoké teplo však môže zmeniť farbu a dokončenie.",
    ],
    "overview_heading": "Čo je ramia a prečo sa zamieňa s ľanom",
    "overview": [
        "Európske pravidlá uvádzajú ramiu ako vlákno získané z lýka rastlín Boehmeria nivea a Boehmeria tenacissima. Ľan pochádza z inej rastliny a bavlna zo semenných vlákien. Spoločná celulózová podstata vysvetľuje časť podobností, nie však rovnakú jemnosť, dĺžku vlákna, spracovanie ani reakciu hotovej látky na ohyb a vlhkosť.",
        "Po mechanickom oddelení zostávajú okolo ramie látky, ktoré treba odstrániť odglejením. Rozsah spracovania ovplyvní, či priadza pôsobí pevne a sucho alebo jemnejšie a lesklejšie. Výrobca môže ramiu miešať s bavlnou pre mäkší omak, s viskózou pre splývavosť alebo so syntetikou pre inú rozmerovú stabilitu. Názov vlákna preto nie je opis celej konštrukcie.",
        "Výskum tvarovej stability ramie tkanín a úpletov ukazuje, že výsledok treba hodnotiť podľa konkrétnej konštrukcie a dokončenia. Spotrebiteľ z toho nemá odvodiť jednu bezpečnú teplotu. Praktickejšie je zmerať kus pred prvým praním, sledovať smer osnovy a útku alebo očiek a porovnať zmenu až po úplnom vysušení bez násilného napínania.",
    ],
    "table1_heading": "Ramia, ľan, bavlna a viskóza: podobný vzhľad, iné hranice",
    "table1_intro": "Porovnanie opisuje typické tendencie. Reálny výrobok môže byť zmesový, chemicky dokončený alebo skonštruovaný tak, že najcitlivejšia súčasť zmení celý postup.",
    "table1_headers": ["Vlákno", "Pôvod", "Typický omak a správanie", "Čo preveriť pred praním"],
    "table1_rows": [
        ("Ramia", "Lýko rastlín Boehmeria.", "Pevnejší, hladší až lesklý povrch, nižšia pružnosť a výrazné lomy.", "Odglejenie, zmes, väzbu, farbu a spôsob sušenia."),
        ("Ľan", "Lýko ľanu siateho.", "Chladivý omak, savosť, prirodzená krčivosť a postupné mäknutie.", "Predzrážanie, farbu, gramáž a konštrukciu švov."),
        ("Bavlna", "Vlákna zo semien bavlníka.", "Široký rozsah od jemného úpletu po pevné plátno.", "Typ priadze, väzbu, zrazenie, farbu a elastické prímesi."),
        ("Viskóza", "Regenerovaná celulóza vyrobená viskózovým procesom.", "Mäkká a splývavá, podľa konštrukcie citlivá na mokré vyťahanie.", "Mokrú pevnosť, úplet, podšívku, žmýkanie a sušenie."),
    ],
    "sections": [
        {
            "heading": "Od stonky k priadzi: prečo odglejenie mení výsledok",
            "paragraphs": [
                "Lýkové zväzky nie sú v rastline pripravenou textilnou priadzou. Mechanické oddelenie a následné odstránenie necelulózových zložiek rozhoduje o čistote, jemnosti a ohybnosti. Nerovnomerné spracovanie sa môže prejaviť tuhšími miestami, rozdielnym leskom alebo drobnými zhrubnutiami. Takýto povrch nie je automaticky chyba, ale pri trení a žehlení vyžaduje pozornosť.",
                "Domácim praním nemožno bezpečne dokončiť priemyselné odglejenie. Silná zásada alebo dlhé vyváranie môže poškodiť farbu, šev a inú zložku zmesi. Ak nový kus pôsobí nezvyčajne tuhý, najprv overte pokyny výrobcu a urobte jeden kontrolovaný cyklus. Agresívny domáci experiment môže zmeniť omak nerovnomerne a zhoršiť reklamovateľnosť.",
            ],
        },
        {
            "heading": "Ako prať košeľu, šaty a nohavice s ramiou",
            "paragraphs": [
                "Zapnite alebo bezpečne zaistite kovanie, vyprázdnite vrecká a odev obráťte podľa konštrukcie. Golier a manžety prezrite proti svetlu; mastnotu najprv lokálne ošetrite kompatibilným prostriedkom po skrytej skúške. Odev perte s podobne ľahkými hladkými kusmi, nie s uterákmi, zipsami a suchými zipsami, ktoré pridávajú lokálne trenie.",
                "Použite iba teplotu a mechaniku povolenú etiketou. Menšia, voľná náplň zlepší pohyb vody a oplach, no príliš prudké odstreďovanie môže vytlačiť ostré lomy. Po skončení odev ihneď vyberte, jemne pretraste a zarovnajte švy bez ťahania do väčšieho rozmeru. Pri saku, lepenom páse alebo pevnej podšívke postup určuje celý výrobok.",
            ],
        },
        {
            "heading": "Ako prať úplet z ramie bez vyťahania očiek",
            "paragraphs": [
                "Úplet umožňuje pohyb slučiek, takže sa môže deformovať aj z relatívne pevného vlákna. Pred praním odmerajte šírku a dĺžku na rovnej ploche a zaznamenajte tvar výstrihu. Pri povolenom praní znížte mechanickú záťaž, chráňte povrch pred zachytením a mokrý kus prenášajte podopretý oboma rukami alebo na uteráku.",
                "Sušte naplocho podľa pôvodných mier bez napínania rebier a ramien. Vešiak môže mokrú hmotnosť sústrediť do dvoch bodov a vytvoriť hrbole. Ak je úplet po vysušení širší, jeden ďalší horúci cyklus nie je bezpečná oprava. Najprv rozlíšte mokré vyťahanie, zmenu slučiek a trvalú rozmerovú zmenu zmesi.",
            ],
        },
        {
            "heading": "Krčivosť ramie a bezpečné vyrovnanie záhybov",
            "paragraphs": [
                "Krčivosť súvisí s malou pružnou návratnosťou a tým, ako sa priadza a väzba po stlačení vracajú. Jemné pokrčenie po nosení môže ustúpiť zavesením a vlhkosťou, ostrý zaschnutý lom potrebuje opatrnejšie vyrovnanie. Nestriekajte veľké množstvo vody na jedno miesto, pretože farba alebo dokončenie môže vytvoriť mapu.",
                "Ak etiketa žehlenie povoľuje, pracujte z rubu cez čistú ochrannú tkaninu a najprv skúšajte vnútorný lem. Žehličku presúvajte bez dlhého tlaku na lesklú plochu. Pri zmesi zvoľte teplotu podľa najcitlivejšieho vlákna. Prudké naparovanie podšívky, výšivky alebo živicovej úpravy môže zmeniť tvar aj vtedy, keď samotná ramia teplo znesie.",
            ],
        },
        {
            "heading": "Škvrny od potu, oleja, jedla a kozmetiky",
            "paragraphs": [
                f"Čerstvú tekutinu odsajte bez trenia a pevný zvyšok nadvihnite tupou hranou. Pri oleji pracujte po malých krokoch podľa návodu <a href=\"{ARTICLE_OIL}\">ako odstrániť mastnú škvrnu z oblečenia</a>. Tmavnutie po namočení nemusí byť trvalá škvrna; celulóza a dokončenie menia optický vzhľad, kým sú mokré. Výsledok hodnotíte až po dôkladnom oplachu a úplnom vysušení.",
                "Pot a dezodorant sa sústreďujú pri golieri a podpazuší, kde sa spája chémia s trením. Nebieľte farebnú zmes bez výslovného povolenia. Bodové ošetrenie skúšajte na prídavku šva a nešúchajte dve vrstvy o seba. Ak sa biela handrička sfarbí alebo povrch zdrsnie, zásah prerušte a zvoľte odborné posúdenie.",
            ],
            "callout": {
                "title": "Kedy ramiu nečistiť ďalším domácim pokusom",
                "items": [
                    "Farba sa pri skrytej skúške prenáša na bielu handričku.",
                    "Šev sa rozostupuje alebo je priadza v ostrom lome zbelená a oslabená.",
                    "Odev obsahuje lepenú výstuž, plisovanie alebo podšívku s neznámym zložením.",
                    "Etiketa povoľuje iba profesionálne čistenie alebo je kus hodnotný a bez návodu.",
                ],
                "background": "#f7fbf8",
                "border": "#dbe5de",
            },
        },
        {
            "heading": "Ramia v zmesi s bavlnou, viskózou, ľanom a polyesterom",
            "paragraphs": [
                f"Zmes sa nespráva ako priemer percent. Desať percent elastanu môže určiť tepelný limit, viskózový úplet môže určovať mokrú oporu a tmavé farbivo môže obmedziť bielenie. Článok <a href=\"{ARTICLE_BLEND}\">prečo sa zmesový materiál správa inak, než čakáte</a> vysvetľuje, že konštrukcia a najslabší prvok sú praktickejšie než dominantné číslo na etikete.",
                "Ramia s bavlnou môže pôsobiť mäkšie, s ľanom sucho a výrazne krčivo, s viskózou splývavejšie a so syntetikou stabilnejšie alebo rýchlejšie schnúť. Tieto tendencie nie sú zárukou. Pred prvým praním si všimnite smer, v ktorom sa látka naťahuje, a porovnajte, či sú švy spevnené primerane k tuhej alebo klzkej priadzi.",
            ],
        },
        {
            "heading": "Prečo môže byť ramia po praní tvrdá alebo fľakatá",
            "paragraphs": [
                "Tvrdosť môže pochádzať z minerálov vody, zvyšku prostriedku, zaschnutia v ostrom lome, pôvodnej úpravy alebo prirodzeného omaku. Najprv skontrolujte dávku, oplach a rovnomerné vysušenie. Nepridávajte aviváž iba podľa dotyku mokrého kusu; film môže zmeniť savosť, farbu a následné odstraňovanie škvŕn.",
                "Svetlá mapa môže byť zvyšok gélu, oder lesklého povrchu alebo zbelenie mechanicky namáhaného vlákna. Zvyšok sa pri povolení môže uvoľniť čistým oplachom, poškodenie nie. Porovnajte rub, smer nití a miesto lomu. Ak sa štruktúra zmenila alebo priadza praská, ďalšia chémia problém nevyrieši.",
            ],
        },
        {
            "heading": "Výber a skladovanie oblečenia z ramie",
            "paragraphs": [
                "Pri kúpe skontrolujte presné percentá, rovnomernosť priadze, švy a to, ako sa látka vráti po jemnom stlačení. Letný omak sám nehovorí, že odev bude nenáročný. Košeľa na cestovanie potrebuje inú krčivosť než voľné šaty; obrus zas stálofarebnosť, rozmer a odolnosť opakovaného lokálneho čistenia.",
                "Čistý a úplne suchý tkaný odev zaveste na široký vešiak alebo voľne preložte podľa hmotnosti. Úplet skladujte naplocho. Nevytvárajte dlhodobý ostrý lom pod ťažkou kopou a neuzatvárajte zvyškovú vlhkosť do nepriedušného vaku. Pred sezónnym uložením odstráňte jedlo a kozmetiku, ktoré sa časom oxidujú a lákajú škodcov.",
            ],
        },
    ],
    "table2_heading": "Ramia po praní: čo znamená zmena povrchu alebo tvaru",
    "table2_intro": "Kus najprv nechajte úplne vyschnúť pri izbových podmienkach. Až potom odlišujte dočasnú vlhkosť, zvyšok produktu, rozmerovú zmenu a mechanické poškodenie.",
    "table2_headers": ["Prejav", "Možná príčina", "Čo overiť", "Bezpečný ďalší krok"],
    "table2_rows": [
        ("Odev je tvrdší", "Nadbytok prostriedku, minerály vody, spôsob sušenia alebo pôvodný tuhší omak.", "Dávku, oplach, tvrdosť vody a rovnomernosť zmeny.", "Pri povolení zopakovať čistý oplach; nepridávať zmäkčovadlo naslepo."),
        ("Vznikol ostrý svetlý lom", "Mechanické stlačenie, oder lesku alebo poškodenie dokončenia.", "Rub, celistvosť priadze a reakciu na jemné navlhčenie.", "Nežehliť silou; pri poškodenej priadzi zásah zastaviť."),
        ("Úplet sa rozšíril", "Mokrá hmotnosť, zavesenie, voľná konštrukcia alebo zmes.", "Pôvodné miery, smer očiek a stav ramien.", "Sušiť naplocho podľa pôvodného tvaru bez prudkého tepla."),
        ("Farba je nerovnomerná", "Prenos farby, lokálny produkt, zvyšok gélu alebo rozdielne schnutie.", "Skúšku bielou handričkou a hranicu mapy.", "Nedrhnúť; pri povolení rovnomerne opláchnuť alebo vyhľadať čistiareň."),
        ("Pri šve sa rozostúpili nite", "Ťah tesného strihu, posun väzby alebo oslabený lom.", "Stehy, smer ťahu a zdravú plochu okolo šva.", "Pred ďalším praním šev stabilizovať a odborne opraviť."),
    ],
    "steps_heading": "Ako bezpečne vyprať výrobok s ramiou krok za krokom",
    "steps": [
        "Prečítajte celé zloženie a symboly a odlíšte tkaninu, úplet, podšívku, výšivku a lepenú výstuž.",
        "Zmerajte nový alebo citlivý kus, skontrolujte švy a farbu a lokálny produkt skúste na skrytom mieste.",
        "Ošetrite konkrétnu škvrnu bez drhnutia a oddeľte ramiu od zipsov, uterákov a drsných povrchov.",
        "Použite povolený šetrný program, primeranú dávku a náplň, v ktorej má voda priestor na oplach.",
        "Po skončení kus ihneď vyberte, nekrúťte ho a mokrý úplet prenášajte s plošnou oporou.",
        "Vyrovnajte švy podľa pôvodných mier a sušte na vhodnej opore mimo prudkého tepla a slnka.",
        "Žehlite iba pri povolení, z rubu, cez ochrannú tkaninu a podľa najcitlivejšej zložky.",
        "Úplne suchý kus uložte bez ostrého lomu a pri ďalšom praní porovnajte rozmer aj omak.",
    ],
    "remember": [
        "Je na etikete skutočne ramia a aký je jej podiel?",
        "Ide o tkaninu, úplet alebo štruktúrovaný odev s výstužou?",
        "Je farba stabilná pri skrytej skúške a sú švy bez rozostúpenia?",
        "Ktorá zložka zmesi má najnižší limit vody, tepla a mechaniky?",
        "Bude mať mokrý kus počas prenášania a sušenia rovnomernú oporu?",
        "Je zmena po praní zvyšok produktu, krčivosť alebo poškodenie priadze?",
    ],
    "mistakes": [
        "Považovať ramiu za iný názov ľanu a automaticky použiť rovnaký cyklus.",
        "Zavesiť mokrý úplet za ramená a neskôr ho zmršťovať prudkým teplom.",
        "Drhnúť lesklú škvrnu kefou a vytvoriť svetlejšie miesto oderom.",
        "Pridať viac gélu alebo aviváže, keď je príčinou tvrdosti slabý oplach.",
        "Žehliť ostrý lom vysokou teplotou bez skrytej skúšky farby a dokončenia.",
        "Uložiť kus vlhký alebo dlhodobo stlačený v jednom pevnom prehybe.",
    ],
    "expert_heading": "Odbornejší pohľad: pevnosť vlákna, tvarová stabilita a význam skúšky",
    "expert": [
        "Pevnosť jedného oddeleného vlákna nemožno priamo zameniť za životnosť odevu. Priadza pridáva zákrut a nerovnomernosť, tkanina väzbu a dostavu, úplet pohyb očiek a hotový výrobok švy, strih a dokončenie. Ramia môže byť pevná v ťahu, no citlivá na ostrý opakovaný ohyb alebo na deformáciu vo voľnom úplete.",
        "Peer-reviewed práca o tvarovej stabilite ramie tkanín ukazuje, že konštrukcia a úprava významne menia tvarové správanie. Výsledok jednej vzorky sa preto nemá meniť na sľub pre všetky košele a šaty. Pre domácnosť je užitočný kontrolný rozmer, rovnaký spôsob sušenia a porovnanie po viacerých cykloch, nie okamžité hodnotenie mokrého kusu.",
        "AATCC TM135 definuje podmienky na hodnotenie rozmerových zmien po domácom praní a GINETEX vysvetľuje symboly ako maximálne povolené zaobchádzanie. Ani jedna informácia neurčuje univerzálny recept pre ramiu. Metóda pomáha porovnávať, etiketa riadi konkrétny výrobok a domáci používateľ musí navyše sledovať zmes, farbu a konštrukčné detaily.",
    ],
    "source_intro": "Zdroje podporujú identitu vlákna, tvarovú stabilitu a čítanie ošetrovacích symbolov. Neurčujú jednu teplotu alebo jeden prací prostriedok pre každý výrobok s ramiou.",
    "sources": [
        ("EÚ 1007/2011: ramia v zozname textilných vlákien", EU_FIBRE_LABEL),
        ("J-STAGE: Stability of Shape on Ramie Fabrics", RAMIE_SHAPE),
        ("AATCC TM135: rozmerové zmeny po domácom praní", AATCC_DIMENSION),
        ("AATCC TM61: zrýchlená stálofarebnosť pri praní", AATCC_COLOR),
        ("GINETEX: význam symbolov ošetrovania", GINETEX),
    ],
    "product_intro": "Prací gél je vhodný len pre výrobok, ktorého etiketa povoľuje domáce pranie. Pri zmesi s vlnou, lepenom saku alebo profesionálne čistenom odeve treba zvoliť presne určený postup.",
    "product_text": "Hypoalergénny prací gél Vevo Ylang Absolute je konkrétna možnosť pre kompatibilnú bežnú bielizeň s ramiou. Dávkujte ho podľa náplne, tvrdosti vody a znečistenia a zabezpečte dôkladný oplach.",
    "product_limit": "Produkt nenahrádza prostriedok na vlnu ani profesionálne čistenie, neobnoví poškodený lom a nie je automaticky vhodný pre každú zmes, farbu alebo povrchovú úpravu.",
    "category_intro": "Pri porovnaní pracích gélov sledujte určenie produktu, dávkovanie a kompatibilitu s najcitlivejšou zložkou výrobku, nie iba názov hlavného vlákna.",
    "category_text": "V kategórii pracích gélov nájdete možnosti pre bežné domáce pranie. Pri ramii vyberajte až po kontrole etikety a pred prvým použitím overte farbu aj oplach na konkrétnom kuse.",
    "related": [
        ("Čo je ľan a ako sa oň starať", ARTICLE_LINEN),
        ("Ľan verzus bavlna", ARTICLE_LINEN_COMPARE),
        ("Čo je bavlna", ARTICLE_COTTON),
        ("Ako fungujú zmesové materiály", ARTICLE_BLEND),
        ("Prečo sa oblečenie zráža", ARTICLE_SHRINKAGE),
        ("Ako čítať štítok na oblečení", ARTICLE_LABEL),
    ],
    "faq_title": "ramia, vlastnosti a starostlivosť",
    "faq": [
        ("Čo je ramia?", "Prírodné celulózové lýkové vlákno z rastlín rodu Boehmeria. Nie je to ľan ani bavlna."),
        ("Je ramia to isté ako ľan?", "Nie. Obe sú lýkové celulózové vlákna, ale pochádzajú z iných rastlín a líšia sa spracovaním aj typickým omakom."),
        ("Môže sa ramia prať v práčke?", "Iba ak to povoľuje etiketa celého výrobku. Zvoľte povolenú mechaniku, primeranú náplň a dôkladný oplach."),
        ("Na koľko stupňov prať ramiu?", "Univerzálna teplota neexistuje. Rozhoduje zmes, farba, väzba, dokončenie a symbol na konkrétnom kuse."),
        ("Prečo sa ramia krčí?", "Má menšiu pružnú návratnosť a záhyb zostáva v priadzi a konštrukcii výraznejšie viditeľný."),
        ("Ako sušiť úplet s ramiou?", "S plošnou oporou naplocho podľa pôvodných mier, ak etiketa neurčuje inak. Mokré ramená nenechávajte visieť."),
        ("Môže ísť ramia do sušičky?", "Len pri výslovnom symbole. Teplo a mechanika môžu zmeniť rozmer, farbu, lom aj citlivejšiu zložku zmesi."),
        ("Ako zmäkčiť ramiu po praní?", "Najprv skontrolujte dávku, oplach a spôsob sušenia. Nepridávajte aviváž bez overenia kompatibility a požadovanej funkcie."),
        ("Ako žehliť ramiu?", "Pri povolení z rubu cez ochrannú tkaninu, podľa najcitlivejšej zložky a po skrytej skúške lesku a farby."),
    ],
}


CUPRO: dict[str, object] = {
    "title": "Čo je cupro: regenerované celulózové vlákno, splývavosť a starostlivosť",
    "link": "co-je-cupro-regenerovane-celulozove-vlakno-splyvavost-a-starostlivost",
    "meta": "Čo je cupro a Bemberg, ako sa líši od viskózy, modalu a hodvábu a ako prať, sušiť a žehliť cupro podšívky, šaty a blúzky.",
    "short": "Cupro je regenerované celulózové vlákno vyrábané cupro-amoniakálnym procesom. Často sa používa na hladké podšívky a splývavé odevy. O domácom praní však rozhoduje etiketa celého výrobku, mokrá hmotnosť, farba, podšívka a povrchová úprava.",
    "name": "cupro",
    "genitive": "cupro materiálu",
    "locative": "cupro materiáli",
    "construction_summary": "regenerované celulózové vlákno vytvorené meďnato-amoniakálnym procesom, z ktorého sa vyrábajú hladké filamenty alebo striž pre tkaniny a úplety",
    "label_details": "podiel cupra, zloženie vrchnej látky a podšívky, lepenú výstuž, farbu, potlač, plisovanie, švy a výslovné povolenie domáceho prania",
    "residue_place": "hladkej väzbe, rubovej podšívke, švoch, záhyboch, miestach pri podpazuší a na jemne fibrilovanom povrchu",
    "friction_risk": "jemný filament, lesklý povrch, voľný šev alebo fibrilovanú vrstvu",
    "drying_advice": "Ľahký prateľný odev vyrovnajte a sušte na opore určenej etiketou; úplet alebo ťažšie mokré šaty podoprite naplocho. Podšívku saka nesušte oddelene od vrchnej konštrukcie prudkým teplom.",
    "heat_risk": "celulózové vlákno, sýte farbivo, lesk, fibrilovanú úpravu, mokré pnutie, podšívku, lepidlo a rozmer vrchnej látky",
    "failure_sign": "trvalé zmatnenie trením, vytiahnutie podšívky, rozídenie šva alebo nepravidelné zdrsnenie povrchu",
    "answer": "Cupro je regenerované celulózové vlákno získané meďnato-amoniakálnym procesom. Môže byť hladké, jemné, splývavé a dobre pracovať s telesnou vlhkosťou, preto sa používa na podšívky, šaty, blúzky aj domáci textil. Nie je to hodváb a značka Bemberg nie je všeobecný názov všetkého cupra. Pred praním rozlíšte samotnú látku od celého saka alebo šiat: vrchný materiál, výstuž, lepidlo, farba a švy môžu vyžadovať profesionálne čistenie, hoci samostatná cupro tkanina existuje aj v prateľnej úprave. Ak etiketa domáce pranie povoľuje, použite šetrnú mechaniku, primeranú dávku, dôkladný oplach a mokrý kus prenášajte s oporou. Nesušte ho prudkým teplom a lesklý povrch nedrhnite. Žehlite iba podľa symbolu, z rubu a po skrytej skúške.",
    "intro": "Cupro sa v obchodoch často opisuje ako hodvábne, prírodné alebo bavlnené, čo môže vytvoriť nesprávne očakávanie. Surovinou značkového Bembergu sú bavlnené linters, ale výsledkom je priemyselne regenerované celulózové vlákno s vlastným zákonným názvom. Na dotyk môže pripomínať hodváb a chemickou rodinou stojí blízko viskózy, no výrobný proces a parametre nie sú totožné. Starostlivosť navyše neurčuje abstraktné vlákno, ale konkrétna priadza, tkanina, farbenie a konštrukcia odevu. Najväčším praktickým rozdielom býva to, či držíte samostatnú prateľnú blúzku alebo podšívku pevne vystuženého saka.",
    "quick": [
        "<strong>Cupro je regenerovaná celulóza:</strong> v európskom označovaní má vlastný názov a nie je synonymom hodvábu.",
        "<strong>Bemberg je značka cupra:</strong> obchodná značka sa nemá zamieňať s celou kategóriou vlákna.",
        "<strong>Hladký omak neznamená rovnakú údržbu:</strong> podšívka saka a prateľné šaty môžu potrebovať opačný postup.",
        "<strong>Mokrá hmotnosť mení tvar:</strong> jemný odev prenášajte s oporou a nekrúťte ho.",
        "<strong>Trenie môže zmatniť povrch:</strong> škvrnu odsávajte a netrite ju kefou ani dvoma vrstvami o seba.",
        "<strong>O farbe rozhoduje hotová látka:</strong> sýty odtieň skúšajte na skrytom mieste a oddeľte ho od svetlej bielizne.",
    ],
    "overview_heading": "Čo je cupro a aký je vzťah medzi cuprom a Bembergom",
    "overview": [
        "Nariadenie EÚ definuje cupro ako regenerované celulózové vlákno získané meďnato-amoniakálnym procesom. Regenerované znamená, že prírodná celulóza sa rozpustí a znovu vytvorí vo forme vlákna. Táto kategória sa preto nemá označovať ako surová bavlna, hoci celulózová surovina konkrétneho produktu môže pochádzať z bavlnených linters.",
        "Asahi Kasei uvádza, že Bemberg je jeho značka cupra vyrábaná z krátkych vlákien okolo bavlníkových semien. Technický materiál výrobcu opisuje hladký prierez, prijímanie a uvoľňovanie vlhkosti, nižšiu tvorbu statického náboja v uvedených skúšobných podmienkach a výraznú splývavosť. Ide o údaje ku konkrétnemu značkovému vláknu, nie automatický certifikát každého hotového odevu.",
        "Vlákno možno spriasť alebo použiť ako filament a ďalej tkať, pliesť, farbiť či povrchovo upravovať. Jemná podšívková tkanina sa bude pri mokrom zaťažení správať inak než úplet alebo zmes s polyesterom. Pri nákupe preto oddeľte percento cupra od konštrukcie a od pokynu na ošetrovanie.",
    ],
    "table1_heading": "Cupro, viskóza, modal, lyocell a hodváb",
    "table1_intro": "Materiály môžu mať podobný lesk alebo splývavosť, ale názov opisuje iný pôvod alebo výrobný proces. Etiketa hotového výrobku zostáva rozhodujúca.",
    "table1_headers": ["Názov", "Čo označuje", "Typické použitie", "Riziko pri zámene"],
    "table1_rows": [
        ("Cupro", "Regenerovanú celulózu z meďnato-amoniakálneho procesu.", "Podšívky, šaty, blúzky, úplety a domáci textil.", "Považovať každé cupro za hodváb alebo automaticky prateľnú bavlnu."),
        ("Viskóza", "Regenerovanú celulózu z viskózového procesu.", "Šaty, blúzky, úplety, podšívky a zmesi.", "Preniesť mokrú pevnosť a rozmerovú stabilitu z inej viskózovej konštrukcie."),
        ("Modal", "Regenerované celulózové vlákno s definovanou vysokou pevnosťou a modulom za mokra.", "Spodná bielizeň, úplety, tričká a zmesi.", "Predpokladať, že názov modal povoľuje rovnakú sušičku pri každom kuse."),
        ("Lyocell", "Regenerované celulózové vlákno vyrobené procesom s organickým rozpúšťadlom.", "Odevy, denim, posteľný textil a zmesi.", "Ignorovať fibriláciu, farbu a povrchovú úpravu konkrétnej látky."),
        ("Hodváb", "Prírodné proteínové vlákno z kokónov hodvábnika.", "Jemné odevy, šatky, podšívky a luxusný textil.", "Použiť na cupro alebo na hodváb rovnakú chémiu iba podľa podobného lesku."),
    ],
    "sections": [
        {
            "heading": "Prečo je cupro hladké, splývavé a príjemné ako podšívka",
            "paragraphs": [
                "Jemný filament s hladkým povrchom znižuje drsné zachytávanie o rukáv a môže vytvoriť rovnomerný lesk. Splývavosť však vzniká až spojením jemnosti, hmotnosti, priadze a väzby. Dve tkaniny s rovnakým percentom cupra nemusia kopírovať postavu rovnako, ak má jedna hustejšiu väzbu alebo tuhšiu dokončovaciu úpravu.",
                "Podšívka musí kĺzať, prijímať zaťaženie pri obliekaní a pracovať s vrchnou látkou. Ak je pri šve príliš napnutá, môže prasknúť skôr než pevný vonkajší materiál. Pri kúpe saka skontrolujte rezervu podšívky, čistotu švov a to, či sa pri pohybe neťahá v podpazuší. Pranie už poškodené pnutie neopraví.",
            ],
        },
        {
            "heading": "Ako prať cupro šaty, blúzku alebo sukňu",
            "paragraphs": [
                "Najprv overte, či etiketa povoľuje vodu. Zapnite bezpečné uzávery, oddeľte odev od zipsov a drsných textílií a škvrnu skúšajte na vnútornom šve. Pri povolenom ručnom praní použite dostatočný objem vody a materiál jemne premiestňujte bez trenia. Pri povolenej práčke zvoľte predpísaný šetrný režim a ľahkú náplň.",
                "Mokrý odev nezdvíhajte za jedno ramienko ani ho neskrúcajte. Vodu nechajte odtiecť a kus preneste na uteráku alebo s oporou viacerých miest. Švy vyrovnajte podľa pôvodného tvaru. Ak povrch po navlhčení stmavne, nehodnoťte farbu, kým nie je rovnomerne a úplne suchý.",
            ],
        },
        {
            "heading": "Cupro podšívka v saku, kabáte a nohaviciach",
            "paragraphs": [
                "Pri saku neurčuje postup samotná podšívka. Vrchná vlna, lepená výstuž, vypchávky, tvarované chlopne, gombíky a farebné medzivrstvy môžu vodu vylúčiť. Domáce ponorenie môže zraziť jednu vrstvu, rozpustiť lepidlo alebo vytvoriť zvlnenie predného dielu, aj keď oddelená cupro tkanina by šetrný kúpeľ zniesla.",
                "Podšívku po nosení vetrajte spolu s celým odevom, vrecká vyprázdnite a pot pri podpazuší nenechávajte dlhodobo uzavretý v obale. Prach odstráňte jemne a lokálnu škvrnu odsávajte bez presakovania do výstuže. Pri symbole profesionálneho čistenia odneste celý odev, nie iba informáciu, že podšívka je z cupra.",
            ],
            "callout": {
                "title": "Kedy cupro odev neponárať doma",
                "items": [
                    "Etiketa celého saka, kabáta alebo šiat povoľuje iba profesionálne čistenie.",
                    "Odev má lepenú výstuž, tvarované ramená, plisovanie alebo neznámu podšívku.",
                    "Sýta farba sa prenáša pri skrytej skúške alebo sa povrch pri navlhčení nerovnomerne zdrsní.",
                    "Šev je napnutý, podšívka sa vyťahuje alebo je jemný filament už mechanicky poškodený.",
                ],
                "background": "#f7fbf8",
                "border": "#dbe5de",
            },
        },
        {
            "heading": "Cupro, pot, vlhkosť a statická elektrina",
            "paragraphs": [
                "Technické materiály Bembergu opisujú prijímanie a uvoľňovanie vodnej pary a nižší statický náboj v konkrétnych porovnávacích skúškach. To neznamená, že podšívka zostane hygienicky čistá bez údržby. Pot prináša vodu, soli, maz a kozmetiku, ktoré sa ukladajú pri golieri, páse a podpazuší a môžu časom meniť farbu.",
                "Statika závisí aj od relatívnej vlhkosti vzduchu, druhej vrstvy, trenia a dokončenia. Cupro podšívka pod polyesterovým odevom sa môže správať inak než samostatná vzorka. Nestriekajte na ňu náhodný antistatický produkt bez skúšky, pretože môže vytvoriť mapu. Najprv vetrajte, odstráňte zvyšky a skontrolujte kombináciu vrstiev.",
            ],
        },
        {
            "heading": "Fibrilácia, zmatnenie a poškodenie hladkého povrchu",
            "paragraphs": [
                "Jemné celulózové fibrily sa môžu pri cielenej úprave využiť na vytvorenie broskyňového povrchu, no náhodné lokálne trenie môže pôsobiť ako matná mapa. Rozdiel je v rovnomernosti a zámere. Ak škvrnu silno vydrhnete, lesklé okolie ostane odlišné aj po odstránení nečistoty.",
                "Povrch po praní najprv opláchnite a úplne vysušte. Klzký film môže byť zvyšok prostriedku, kým chĺpkovité svetlé miesto sledujúce smer trenia skôr ukazuje mechanickú zmenu. Neholte ani neleštite jemný filament bez poznania konštrukcie. Pri drahom odeve je lokálna zmena dôvodom na odborné posúdenie.",
            ],
        },
        {
            "heading": "Cupro verzus viskóza, modal a lyocell pri praní",
            "paragraphs": [
                f"Všetky štyri názvy patria k regenerovaným celulózovým vláknam, ale definujú odlišné procesy alebo vlastnosti. Článok <a href=\"{ARTICLE_MODAL_COMPARE}\">modal verzus lyocell verzus viskóza</a> ukazuje, prečo nemožno preniesť jeden režim medzi rôznymi úpletmi a tkaninami. Cupro pridáva vlastné priadzové a povrchové varianty.",
                "Pri porovnaní odevov sledujte mokré predĺženie, rozmer po sušení, sklon k fibrilácii, farbu a konštrukciu švov. Percento vlákna je iba začiatok. Prateľná zmes cupra s polyesterom môže byť stabilnejšia, ale teplo stále ovplyvní syntetiku a lesk; zmes s vlnou môže vyžadovať prostriedok a mechaniku vhodnú pre vlnu.",
            ],
        },
        {
            "heading": "Ako odstrániť škvrnu z cupra bez vodnej mapy",
            "paragraphs": [
                f"Tekutinu odsajte bielou handričkou od okraja do stredu a pevný zvyšok nadvihnite. Typ škvrny identifikujte skôr, než pridáte vodu; všeobecný rozhodovací postup ponúka návod <a href=\"{ARTICLE_STAIN}\">ako odstraňovať rôzne škvrny z oblečenia</a>. Produkt skúšajte na vnútornom prídavku šva a sledujte farbu aj lesk.",
                "Malý mokrý kruh môže po vysušení zanechať ostrú hranicu z presunutého farbiva, dokončenia alebo nečistoty. Ak etiketa povoľuje pranie, býva bezpečnejšie rovnomerné ošetrenie celej kompatibilnej časti než opakované premočenie jedného bodu. Pri podšívke saka však voda môže preniknúť do výstuže, preto bez povolenia nepresycujte vrstvy.",
            ],
        },
        {
            "heading": "Sušenie, žehlenie a skladovanie cupra",
            "paragraphs": [
                "Cupro odev vyrovnajte bez ťahania a nechajte schnúť s prúdením vzduchu mimo radiátora a prudkého slnka. Kontrolujte hrubšie švy, pás a miesta, kde sú vrstvy pri sebe. Sušičku použite iba pri výslovnom symbole. Rýchle povrchové vyschnutie neznamená, že podšívka alebo viacnásobný lem je suchý.",
                "Ak etiketa povoľuje žehlenie, pracujte z rubu cez ochrannú tkaninu, s čistou plochou žehličky a krátkym kontaktom. Pri lesklom povrchu môže tlak vytvoriť odlišný odraz. Úplne suché šaty zaveste na širokú oporu, úplet uložte naplocho a podšívku nestláčajte medzi drsné zipsy či koráliky.",
            ],
        },
    ],
    "table2_heading": "Cupro po čistení: diagnostika tvaru, lesku a povrchu",
    "table2_intro": "Najprv rozlíšte dočasné stmavnutie mokrej celulózy od zvyšku produktu, prenosu farby, fibrilácie a zmeny konštrukcie.",
    "table2_headers": ["Prejav", "Možná príčina", "Domáca kontrola", "Ďalší krok"],
    "table2_rows": [
        ("Matná mapa", "Lokálne trenie, fibrilácia, zvyšok produktu alebo nerovnomerné schnutie.", "Smer zmeny, rub, oplach a stav po úplnom vysušení.", "Nedrhnúť ani neleštiť; pri povolení rovnomerne opláchnuť."),
        ("Podšívka sa vyťahuje", "Mokrá hmotnosť, tesný šev, rozdielne zrazenie vrstiev alebo poškodený steh.", "Rezervu podšívky, švy a vrchnú konštrukciu.", "Ďalšie pranie zastaviť a odev opraviť ako celok."),
        ("Povrch je klzký", "Nadbytok gélu, avivážový film alebo nedostatočný oplach.", "Dávku, penu v čistej vode a rovnomernosť omaku.", "Pri povolení zopakovať čistý šetrný oplach."),
        ("Farba púšťa", "Nestálofarebné farbivo, nevhodný produkt alebo teplota.", "Bielu handričku na skrytom mieste a prenos do podšívky.", "Oddeliť, nefixovať teplom a zvoliť odborné posúdenie."),
        ("Odev je dlhší alebo širší", "Mokré vyťahanie, zavesenie, voľná väzba alebo rozdielna zmes.", "Pôvodné miery a smer deformácie.", "Sušiť s oporou podľa pôvodného tvaru; nezmršťovať naslepo."),
    ],
    "steps_heading": "Ako bezpečne ošetriť cupro výrobok krok za krokom",
    "steps": [
        "Overte názov cupro, percentá ostatných vlákien a symboly celého výrobku vrátane podšívky a výstuže.",
        "Prezrite švy, lesk, farbu a zmenu pri skrytej skúške bielou handričkou.",
        "Lokálnu škvrnu odsajte bez trenia a oddeľte odev od zipsov, uterákov a drsných ozdôb.",
        "Použite len povolené domáce alebo profesionálne čistenie, správnu dávku a nízku mechanickú záťaž.",
        "Mokrý kus nekrúťte a prenášajte ho s plošnou oporou, aby sa švy a podšívka nevyťahali.",
        "Sušte v tieni s prúdením vzduchu a kontrolujte lem, pás, podšívku a viacvrstvové miesta.",
        "Žehlite iba pri povolení z rubu cez ochrannú tkaninu a bez dlhého tlaku na lesklú plochu.",
        "Úplne suchý odev uložte na hladkú oporu alebo naplocho a chráňte ho pred zachytením.",
    ],
    "remember": [
        "Je na etikete cupro alebo iba marketingové prirovnanie k hodvábu?",
        "Ide o samostatný prateľný odev alebo podšívku štruktúrovaného saka?",
        "Obsahuje výrobok lepidlo, výstuž, plisovanie alebo inú citlivú vrstvu?",
        "Je farba a lesk stabilný pri skrytej skúške?",
        "Bude mať mokrý kus pri prenášaní a sušení rovnomernú oporu?",
        "Je matná plocha zvyšok produktu alebo mechanicky zmenený filament?",
    ],
    "mistakes": [
        "Označiť cupro za hodváb a prevziať postup iba podľa podobného lesku.",
        "Vyprať celé sako preto, že jeho samostatná cupro podšívka môže existovať v prateľnej verzii.",
        "Drhnúť lokálnu škvrnu a vytvoriť trvalú matnú mapu.",
        "Zdvíhať mokré šaty za ramienka alebo podšívku a vyťahať švy.",
        "Použiť sušičku alebo horúcu žehličku bez výslovného symbolu.",
        "Skladovať jemný povrch pri zipse, flitroch alebo drsnom vešiaku.",
    ],
    "expert_heading": "Odbornejší pohľad: názov vlákna, značkové dáta a hotový výrobok",
    "expert": [
        "Právna definícia cupra opisuje spôsob vzniku regenerovaného celulózového vlákna. Neopisuje jemnosť filamentu, typ priadze, väzbu, farbivo ani konečnú úpravu. Preto nemožno z názvu na etikete vypočítať priedušnosť, životnosť alebo povolenú práčku bez údajov o konkrétnej látke a odeve.",
        "Technický materiál Bemberg uvádza merania hladkosti, vlhkostného správania, statického náboja a splývavosti pre vlastné vzorky a definované podmienky. Tieto dáta sú užitočné na pochopenie mechanizmu, ale nejde o nezávislé porovnanie každého produktu na trhu. V článku sa preto vlastnosti uvádzajú ako charakteristika značkového vlákna, nie ako univerzálny zdravotný alebo výkonnostný sľub.",
        "Pri údržbe je rozhodujúci súbeh mokrej hmotnosti, trenia a rozdielov medzi vrstvami. Podšívka môže prijímať vlhkosť príjemne pri nosení, no voda pri celoplošnom praní zaťaží jej švy a výstuž saka inak. Normované skúšky a symboly pomáhajú tieto podmienky oddeliť; domáca podobnosť na dotyk ich nenahrádza.",
    ],
    "source_intro": "Zdroje rozlišujú právny názov cupro, vlastnosti konkrétneho vlákna Bemberg a symboly starostlivosti. Údaje výrobcu sa neprekladajú na univerzálny pokyn pre všetky hotové odevy.",
    "sources": [
        ("EÚ 1007/2011: definícia vlákna cupro", EU_FIBRE_LABEL),
        ("Asahi Kasei: Bemberg ako cupro z bavlnených linters", ASAHI_BEMBERG),
        ("Asahi Kasei: technické vlastnosti Bemberg", ASAHI_BEMBERG_TECH),
        ("Asahi Kasei: surovina a výrobný systém Bemberg", ASAHI_BEMBERG_SUSTAINABILITY),
        ("AATCC TM135: rozmerové zmeny po domácom praní", AATCC_DIMENSION),
        ("GINETEX: význam symbolov ošetrovania", GINETEX),
    ],
    "product_intro": "Bežný prací gél prichádza do úvahy iba pri cupro odeve, ktorý je podľa etikety určený na domáce pranie. Pri saku, kabáte alebo zmesi s vlnou má prednosť presný návod.",
    "product_text": "Hypoalergénny prací gél Vevo Ylang Absolute je konkrétna možnosť pre kompatibilnú bežnú bielizeň. Pri cupre použite primeranú dávku a ľahkú náplň, aby sa jemný povrch dôkladne opláchol bez zbytočného trenia.",
    "product_limit": "Produkt nie je automaticky vhodný na podšívku saka, hodváb, vlnu, lepenú výstuž ani profesionálne čistený odev. Neopraví fibriláciu, poškodený lesk alebo vyťahaný šev.",
    "category_intro": "Výber pracieho gélu začína symbolom prania a celou skladbou výrobku. Hladký cupro povrch nie je dôvodom ignorovať vrchnú látku, farbu alebo výstuž.",
    "category_text": "Kategória pracích gélov ponúka možnosti pre bežné prateľné textílie. Pri cupre porovnajte určenie produktu, dávkovanie a požiadavky najcitlivejšej vrstvy.",
    "related": [
        ("Čo je viskóza", ARTICLE_VISCOSE),
        ("Modal, lyocell a viskóza", ARTICLE_MODAL_COMPARE),
        ("Ako fungujú zmesové materiály", ARTICLE_BLEND),
        ("Stálofarebnosť textilu", ARTICLE_COLOR),
        ("Prečo sa oblečenie zráža", ARTICLE_SHRINKAGE),
        ("Ako čítať štítok na oblečení", ARTICLE_LABEL),
    ],
    "faq_title": "cupro, Bemberg a starostlivosť",
    "faq": [
        ("Čo je cupro?", "Regenerované celulózové vlákno získané meďnato-amoniakálnym procesom."),
        ("Je cupro prírodný materiál?", "Má celulózovú surovinu prírodného pôvodu, ale vlákno vzniká priemyselnou regeneráciou. Presnejší názov je regenerované celulózové vlákno."),
        ("Je cupro to isté ako Bemberg?", "Bemberg je značka cupra od Asahi Kasei. Cupro je všeobecný zákonný názov vlákna."),
        ("Je cupro hodváb?", "Nie. Môže mať podobný lesk a omak, ale hodváb je prírodné proteínové vlákno a cupro regenerovaná celulóza."),
        ("Môže sa cupro prať v práčke?", "Iba pri výslovnom povolení etikety celého výrobku. Podšívka saka sama o sebe postup neurčuje."),
        ("Na koľko stupňov prať cupro?", "Univerzálna teplota neexistuje. Rozhoduje farba, priadza, konštrukcia, zmes a ošetrovací symbol."),
        ("Prečo má cupro po praní matné miesto?", "Môže ísť o zvyšok produktu, lokálne trenie, fibriláciu alebo nerovnomerné schnutie. Povrch nedrhnite."),
        ("Ako sušiť cupro šaty?", "S oporou podľa etikety, bez krútenia, radiátora a prudkého slnka. Ťažší mokrý kus nenechávajte visieť za úzke ramienka."),
        ("Môže ísť cupro do sušičky?", "Len pri výslovnom symbole na konkrétnom výrobku."),
    ],
}


TWEED: dict[str, object] = {
    "title": "Čo je tvíd: hrubšia vlnená tkanina, žmolky a čistenie",
    "link": "co-je-tvid-hrubsia-vlnena-tkanina-zmolky-a-cistenie",
    "meta": "Čo je tvíd, aký je rozdiel medzi tvídom a Harris Tweedom a ako vetrať, kefovať, čistiť, sušiť a skladovať tvídové sako, kabát či sukňu.",
    "short": "Tvíd je rodina tkanín, tradične z vlnenej priadze, nie jeden vláknový názov ani jeden vzor. Hrubší povrch zachytáva prach a môže žmolkovať. Pri saku alebo kabáte však spôsob čistenia určuje aj podšívka, výstuž, tvarovanie a etiketa celého odevu.",
    "name": "tvíd",
    "genitive": "tvídu",
    "locative": "tvíde",
    "construction_summary": "hrubšiu tkaninu s viditeľnou priadzou a textúrou, tradične z vlny a často v keprovej, rybej kosti alebo inej tkanej štruktúre",
    "label_details": "percento vlny a ďalších vlákien, podšívku, výstuž, vypchávky, tvarované chlopne, kožené detaily, gombíky, povrchovú úpravu a povolené profesionálne či domáce čistenie",
    "residue_place": "nerovnomernom povrchu priadze, medzi vystupujúcimi väzbovými bodmi, pri golieri, vreckách, manžetách, švoch a pod podšívkou",
    "friction_risk": "vlnené vlákna na povrchu, mäkkú priadzu, slučku, voľný koniec alebo vystupujúci farebný nopok",
    "drying_advice": "Vlhký tkaný odev položte na čistú savú oporu alebo použite široký tvarovaný vešiak podľa konštrukcie a etikety. Sako podoprite tak, aby ramená, chlopne a podšívka schli v rovnakom tvare bez radiátora.",
    "heat_risk": "vlnené vlákna, splstnatenie, tvar chlopní, lepenú výstuž, podšívku, farbu, kožený detail a lisovanie švov",
    "failure_sign": "splstnatené tuhé miesto, predratá priadza, odchlípená výstuž alebo zvlnená chlopňa",
    "answer": "Tvíd je rodina hrubších textúrovaných tkanín, tradične z vlnenej priadze. Nie je to jeden vzor a nie každý tvíd je Harris Tweed. Pred čistením skontrolujte zloženie aj celý odev: vlnený povrch môže byť spojený s podšívkou, lepenou výstužou, vypchávkami, kožou a tvarovanými chlopňami, ktoré domáce ponorenie neznášajú. Bežnú údržbu začnite vetraním, odpočinkom odevu a jemným kefovaním tkaného povrchu v jednom smere. Škvrnu odsajte bez drhnutia a postupujte podľa etikety. Práčku použite iba pri výslovnom povolení pre celý výrobok a s prostriedkom vhodným pre najcitlivejšiu zložku. Vlhký tvíd nesušte na radiátore ani prudkom slnku. Žmolky nevytrhávajte prstami a sako neukladajte so zvyškami jedla alebo kožného mazu, ktoré priťahujú textilných škodcov.",
    "intro": "Pri otázke ako prať tvídové sako sa často preskočí najdôležitejší krok: väčšina sák nie je iba kus tvídovej metráže. Strih drží vďaka výstuži, lisovaniu a podšívke, takže voda môže zmeniť vzájomný rozmer vrstiev a vytvoriť bubliny či zvlnenie. Tvíd navyše opisuje vzhľad a konštrukciu, nie garantovaných sto percent vlny. Moderný výrobok môže obsahovať polyamid, polyester, akryl alebo bavlnu. Správna starostlivosť preto rozlišuje pravidelné vetranie a kefovanie, lokálnu škvrnu, povolené pranie samostatnej sukne a profesionálne čistenie štruktúrovaného saka alebo kabáta.",
    "quick": [
        "<strong>Tvíd je tkanina, nie vlákno:</strong> zloženie môže byť vlnené, zmesové alebo celkom odlišné.",
        "<strong>Harris Tweed je chránené označenie:</strong> nie každý tvíd alebo vzor rybej kosti spĺňa jeho zákonnú definíciu.",
        "<strong>Najprv vetrajte a kefujte:</strong> tkaný vlnený odev často nepotrebuje po každom nosení celý mokrý cyklus.",
        "<strong>Sako je viacvrstvový výrobok:</strong> podšívka a výstuž môžu vylúčiť domáce pranie.",
        "<strong>Žmolky sú dôsledkom trenia:</strong> sledujte lakte, boky, popruh tašky a vnútorné strany rukávov.",
        "<strong>Vlhkosť a teplo držte pod kontrolou:</strong> radiátor môže zmeniť vlnu aj tvar lisovaného odevu.",
    ],
    "overview_heading": "Čo je tvíd a čo tento názov nehovorí",
    "overview": [
        "Tvíd sa tradične spája s vlnenou tkaninou, viditeľnou priadzou, tlmene miešanými farbami a väzbami, ktoré dobre fungujú v sakách, kabátoch, sukniach a doplnkoch. Môže mať keprovú čiaru, rybiu kosť, káro alebo jednoduchšiu štruktúru. Vzor však nie je chemické zloženie a podobný vzhľad možno vytvoriť aj zo zmesových či syntetických priadzí.",
        "Harris Tweed Authority uvádza presnú zákonnú definíciu Harris Tweedu: látka musí byť ručne tkaná obyvateľmi na ich domovoch vo Vonkajších Hebridách, dokončená vo Vonkajších Hebridách a vyrobená z čistej novej vlny, ktorá bola v tejto oblasti farbená a spriadaná. Certifikačný znak preto potvrdzuje pôvod a proces, nie univerzálny domáci prací program pre hotové sako.",
        "Textúra tvídu môže dočasne skryť prach, vlas a drobné žmolky, ale zároveň ich zachytáva medzi priadzami. Preventívna údržba znižuje potrebu celoplošného čistenia. Odev po nosení nechajte odpočinúť, vyprázdnite vrecká, odstráňte povrchový prach a skontrolujte miesta trenia skôr, než sa voľná priadza pretrhne.",
    ],
    "table1_heading": "Tvíd, Harris Tweed, rybia kosť a pletený tvídový vzhľad",
    "table1_intro": "Názvy sa v predaji miešajú, no opisujú inú vlastnosť. Pred čistením potrebujete poznať vláknové zloženie aj konštrukciu.",
    "table1_headers": ["Označenie", "Čo znamená", "Čo neznamená", "Dôsledok pre starostlivosť"],
    "table1_rows": [
        ("Tvíd", "Rodinu textúrovaných tkanín, tradične vlnených.", "Jedno zloženie, jednu väzbu alebo jeden povolený cyklus.", "Čítať percentá vlákien a etiketu celého odevu."),
        ("Harris Tweed", "Chránenú látku spĺňajúcu zákonný pôvod a výrobný proces.", "Že každé hotové sako možno prať doma.", "Overiť znak a potom samostatne ošetrovací štítok výrobku."),
        ("Rybia kosť", "Vizuálny smerový vzor vytvorený väzbou.", "Automaticky pravý tvíd alebo čistú vlnu.", "Sledovať smer kefovania, švy, priadzu a zloženie."),
        ("Tweed-look úplet", "Pletenú alebo efektnú konštrukciu napodobňujúcu tvíd.", "Stabilitu tkaného saka.", "Mokrý úplet podoprieť a chrániť očká pred vyťahaním."),
    ],
    "sections": [
        {
            "heading": "Ako vetrať a kefovať tvíd medzi čisteniami",
            "paragraphs": [
                "Po nosení vyprázdnite vrecká, rozopnite odev a nechajte ho odpočívať v suchom prúdiacom vzduchu mimo priameho slnka. Vetranie pomáha odviesť vodnú paru a prchavé pachy, neodstráni však mastnú škvrnu alebo nános pri golieri. Odev neuzatvárajte ihneď do tesného vaku a neprekrývajte pach silnou vôňou.",
                "Woolmark pri tkaných vlnených odevoch odporúča jemné kefovanie pozdĺžne po nosení, aby sa povrchová nečistota nestala škvrnou. Použite čistú mäkkú odevnú kefu, malý tlak a konzistentný smer. Nezachytávajte vystupujúce nopy a voľnú priadzu. Kefu pravidelne čistite, aby ste mastnotu neprenášali späť.",
            ],
        },
        {
            "heading": "Ako vyčistiť tvídové sako alebo kabát",
            "paragraphs": [
                "Najprv prečítajte etiketu a skontrolujte vrchnú látku, podšívku, výstuž, ramená, chlopne, kožené detaily a gombíky. Pri profesionálnom symbole nevykonávajte skúšobné ponorenie jedného rukáva. Rozdielne zrazenie vrstiev môže pokaziť tvar ešte predtým, než je povrch viditeľne čistý.",
                "Čerstvú škvrnu odsajte bielou handričkou bez roztierania do väzby. Pevný zvyšok nechajte zaschnúť iba vtedy, ak ho možno potom bezpečne nadvihnúť; mastnotu nenechávajte oxidovať. Odbornej čistiarni opíšte pôvod škvrny a predchádzajúce pokusy. Nežehlite škvrnu, pretože teplo môže zafixovať bielkovinu, tuk alebo farbivo.",
            ],
            "callout": {
                "title": "Kedy tvídový odev patrí do odbornej čistiarne",
                "items": [
                    "Etiketa povoľuje iba profesionálne čistenie alebo je odev bez čitateľného návodu.",
                    "Sako má lepenú výstuž, tvarované chlopne, vypchávky alebo kožené detaily.",
                    "Farba sa prenáša, škvrna zasiahla viac vrstiev alebo nepoznáte použitú chémiu.",
                    "Vlna je splstnatená, priadza predratá alebo sa výstuž oddeľuje od vrchnej látky.",
                ],
                "background": "#f7fbf8",
                "border": "#dbe5de",
            },
        },
        {
            "heading": "Môže sa tvíd prať v práčke alebo ručne",
            "paragraphs": [
                "Odpoveď závisí od hotového výrobku. Jednoduchá nevystužená sukňa alebo šál môže mať povolený vlnený cyklus, zatiaľ čo sako z rovnakej metráže vyžaduje profesionálne čistenie. Symbol vaničky musí patriť celému kusu. Bez neho nemožno vybrať práčku iba podľa percenta vlny.",
                "Pri povolenom praní použite neutrálne jemné pranie určené pre vlnu alebo presný produkt odporúčaný výrobcom, nízku mechaniku a stabilnú povolenú teplotu. Vlnené vlákna zbytočne netrite, nekrúťte a nevystavujte prudkej zmene podmienok. Kus prenášajte s oporou a sušte podľa tvaru, nie zavesený za úzky bod.",
            ],
        },
        {
            "heading": "Žmolky na tvíde: čo je bežné a čo už signalizuje problém",
            "paragraphs": [
                f"Žmolok vzniká, keď sa vlákna trením uvoľnia na povrch, zamotajú a jadro zostane prichytené. Podrobnejší mechanizmus vysvetľuje článok <a href=\"{ARTICLE_PILLING}\">prečo sa oblečenie žmolkuje</a>. Na tvíde sledujte lakte, boky, manžety, miesto pod popruhom a vnútorný kontakt rukávov.",
                "Woolmark zdôrazňuje trenie ako hlavný spúšťač žmolkovania. Jednotlivé voľné chumáčiky nevytrhávajte, lebo môžete vytiahnuť ďalšiu priadzu. Pri pevnej hladkej ploche možno po skúške použiť nástroj určený pre odev, no na hrubom noppovom tvíde môže holiaci strojček odstrániť zámernú textúru. Najprv rozlíšte žmolok od priadze a dizajnového nopku.",
            ],
        },
        {
            "heading": "Prach, chlpy a omrvinky v textúrovanom povrchu",
            "paragraphs": [
                "Prach sa zachytáva medzi vystupujúcimi priadzami a časom tlmí farbu. Začnite mäkkou kefou a postupujte po celej ploche pri dobrom svetle. Lepivý valček môže na voľnom povrchu ťahať vlákna alebo zanechať lepidlo, preto ho skúšajte na nenápadnom mieste. Vysávač používajte iba cez ochrannú sieťku a pri nízkom kontrolovanom ťahu.",
                "Chlpy neodstraňujte mokrou rukou, ak tým vytvoríte lokálnu mapu. Gumový nástroj môže zvýšiť trenie a vytiahnuť povrch. Pri omrvinkách najprv odev prevráťte a jemne vytraste, potom kefujte. Vrecká a záhyby kontrolujte samostatne, aby zvyšok jedla neostal pri sezónnom skladovaní.",
            ],
        },
        {
            "heading": "Voda, dážď a bezpečné sušenie tvídu",
            "paragraphs": [
                "Mierne zmoknutý kabát najprv straste bez prudkého krútenia, vytvarujte ramená a nechajte schnúť pri izbovej teplote s prúdením vzduchu. Silno nasiaknutý odev je ťažký a potrebuje plošnejšiu oporu. Radiátor môže presušiť povrch, zmeniť vlnu a výstuž, kým podšívka zostane vlhká.",
                "Po vysušení skontrolujte pach, chlopne, švy a miesta pri vreckách. Ak vznikla tvrdá hranica alebo bublina, ďalšia para nemusí problém vyriešiť a môže aktivovať lepidlo. Profesionálne lisovanie pracuje s kontrolovanou vlhkosťou, teplom a tvarom; domáce silné pritlačenie žehličkou túto kontrolu nemá.",
            ],
        },
        {
            "heading": "Mole, dlhodobé uloženie a čistota pred sezónou",
            "paragraphs": [
                "Potravinové škvrny, kožný maz a pot zvyšujú atraktivitu uloženého odevu pre textilných škodcov. Pred sezónnym balením musí byť kus čistý a úplne suchý. Skontrolujte golier, manžety, podpazušie, vrecká a záhyby. Samotná vôňa v skrini neodstráni zdroj a môže oddialiť zistenie problému.",
                "Tkané sako zaveste na široký tvarovaný vešiak s dostatočným priestorom, ak to hmotnosť dovoľuje. Ťažký alebo dlhodobo ukladaný kus možno chrániť priedušným obalom podľa odporúčaní výrobcu. Pravidelne kontrolujte drobný prach, larválne obaly a nové poškodenie. Repelent neprikladajte priamo na tkaninu bez pokynu.",
            ],
        },
        {
            "heading": "Ako vybrať kvalitný tvíd na sako, kabát alebo sukňu",
            "paragraphs": [
                "Pozrite sa na zloženie, rovnomernosť väzby, hmotnosť, rezervu švov a reakciu látky na jemný ohyb. Zámerné farebné nopky nemajú byť voľnými koncami. Pri saku skontrolujte plynulosť vzoru cez švy a to, či podšívka neťahá. Certifikácia Harris Tweed potvrdzuje zákonný pôvod látky, nie kvalitu každého krajčírskeho detailu.",
                "Na každodenný kabát je dôležitá odolnosť proti oderu, opravitelnosť a čistiteľnosť; na mäkkú sukňu zas omak a pád. Laboratórne číslo oderu nepredpovedá samo životnosť, pretože výsledok ovplyvňuje metóda, priadza, tlak a konkrétne použitie. Pýtajte sa na test, nie iba na počet cyklov bez kontextu.",
            ],
        },
    ],
    "table2_heading": "Tvíd po nosení alebo čistení: ako čítať zmenu povrchu",
    "table2_intro": "Rozlíšte odstrániteľný prach, žmolok, splstnatenie, poškodenie priadze a deformáciu výstuže. Každý prejav potrebuje iný zásah.",
    "table2_headers": ["Prejav", "Pravdepodobná príčina", "Čo skontrolovať", "Bezpečný ďalší krok"],
    "table2_rows": [
        ("Povrch je matný", "Prach, jemné vlákna, oder alebo zvyšok lokálneho produktu.", "Smer kefovania, rub a reakciu na jemnú suchú kefu.", "Najprv kefovať; mokré čistenie voliť až podľa etikety."),
        ("Vznikli guľaté žmolky", "Trenie, voľné povrchové vlákna a charakter priadze.", "Miesta kontaktu, jadro žmolku a okolité nopy.", "Nevytrhávať; použiť vhodný kontrolovaný nástroj iba po skúške."),
        ("Miesto je tvrdé a zhutnené", "Splstnatenie vlny, teplo, vlhkosť a mechanika.", "Rozdiel v hrúbke a priechodnosti väzby.", "Ďalšie trenie zastaviť; domáce rozčesávanie môže poškodiť priadzu."),
        ("Na chlopni sú bubliny", "Oddeľovanie lepeného plátna alebo rozdielna rozmerová zmena vrstiev.", "Symetriu, podšívku a správanie po vychladnutí.", "Nežehliť naplocho silou; zvoliť krajčírske alebo čistiace posúdenie."),
        ("Priamo v priadzi je diera", "Oder, škodca alebo prasknutie nite.", "Okraje, ďalšie drobné otvory a stav uloženia.", "Izolovať, vyčistiť sklad a dieru opraviť pred ďalším nosením."),
    ],
    "steps_heading": "Ako sa starať o tvídový odev krok za krokom",
    "steps": [
        "Prečítajte zloženie a symboly celého odevu vrátane podšívky, výstuže a kožených detailov.",
        "Po nosení vyprázdnite vrecká, nechajte odev odpočinúť a vetrajte ho mimo priameho slnka.",
        "Tkaný povrch jemne kefujte v jednom smere čistou mäkkou kefou bez vyťahovania nopkov.",
        "Škvrnu identifikujte, odsajte a nežehlite; pri profesionálnom symbole ju zverte čistiarni.",
        "Domáce pranie použite iba pri výslovnom povolení pre celý kus a s postupom vhodným pre vlnu alebo zmes.",
        "Vlhký odev prenášajte s oporou, vytvarujte ramená a sušte pri izbovej teplote bez radiátora.",
        "Po vysušení skontrolujte žmolky, švy, chlopne, podšívku a zmenu povrchu pri rovnakom svetle.",
        "Čistý suchý odev uložte na široký vešiak alebo podľa hmotnosti a pravidelne kontrolujte škodcov.",
    ],
    "remember": [
        "Je výrobok skutočne tkaný tvíd a aké má vláknové zloženie?",
        "Má certifikáciu Harris Tweed alebo iba podobný vzor?",
        "Obsahuje sako podšívku, lepenú výstuž, vypchávky alebo kožu?",
        "Stačí vetranie a kefovanie, alebo ide o skutočnú škvrnu?",
        "Je povrchový útvar žmolok, zámerný nopok alebo vytiahnutá priadza?",
        "Je odev pred uložením čistý, úplne suchý a bez zvyškov jedla?",
    ],
    "mistakes": [
        "Považovať každý vzor rybej kosti za Harris Tweed alebo čistú vlnu.",
        "Ponoriť celé sako podľa vlastnosti vrchnej látky a ignorovať výstuž.",
        "Drhnúť škvrnu tvrdou kefou a splstiť alebo zosvetliť povrch.",
        "Vytrhávať žmolky a spolu s nimi vytiahnuť priadzu z väzby.",
        "Sušiť nasiaknutý kabát na radiátore alebo úzkom vešiaku.",
        "Uložiť tvíd s mastným golierom, omrvinkami alebo zvyškovou vlhkosťou.",
    ],
    "expert_heading": "Odbornejší pohľad: tvídová konštrukcia, vlnený povrch a skúšky oderu",
    "expert": [
        "Tvídový vzhľad vzniká priadzou, farbou a väzbou. Vlnené vlákna môžu na povrchu migrovať, zamotať sa alebo sa pri kombinácii vlhkosti, tepla a mechaniky splstiť. Zmes so silnejším syntetickým vláknom môže držať žmolok dlhšie, pretože jadro sa neodlomí. Rovnaký vizuálny prejav tak môže mať iný mechanizmus.",
        "ASTM pri skúškach oderu upozorňuje, že výsledok ovplyvňuje vlákno, priadza, konštrukcia, dokončenie, abradant, tlak aj hodnotenie. Počet cyklov z jednej metódy nie je univerzálna predpoveď rokov nosenia. Na saku navyše rozhodujú lakte, popruh, švy, strih a možnosť opravy, ktoré plochá laboratórna vzorka neobsahuje.",
        "Harris Tweed Authority rieši pravosť a zákonný proces látky, Woolmark praktickú starostlivosť o vlnu a GINETEX maximálne povolené postupy na etikete. Tieto tri vrstvy informácií sa dopĺňajú, ale nezastupujú. Pravý pôvod tvídu neznamená, že podšívka a lepená výstuž hotového odevu znesú vodu.",
    ],
    "source_intro": "Zdroje oddeľujú pravosť Harris Tweedu, údržbu vlneného odevu, mechanizmus žmolkovania a limity laboratórneho oderu. Etiketa hotového odevu má pri čistení prednosť.",
    "sources": [
        ("Harris Tweed Authority: zákonná definícia a certifikácia", HARRIS_TWEED),
        ("Woolmark: starostlivosť o vlnené odevy", WOOLMARK_CARE),
        ("Woolmark: vznik žmolkov na vlne", WOOLMARK_PILLING),
        ("ASTM D3512: hodnotenie žmolkovania", ASTM_PILLING),
        ("ASTM D4157: oder tkanín a hranice predikcie životnosti", ASTM_ABRASION),
        ("EÚ 1007/2011: označovanie textilných vlákien", EU_FIBRE_LABEL),
        ("GINETEX: význam symbolov ošetrovania", GINETEX),
    ],
    "product_intro": "Bežný prací gél nie je predvolenou voľbou na vlnený tvíd ani na štruktúrované sako. Produktová karta je relevantná iba pre prateľnú zmes, ktorú etiketa výslovne povoľuje prať bežným vhodným prostriedkom.",
    "product_text": "Hypoalergénny prací gél Vevo Ylang Absolute je možnosť pre kompatibilnú bežnú bielizeň. Pri tvíde ho použite iba vtedy, ak zloženie a etiketa nepotrebujú špecializovaný prostriedok na vlnu alebo profesionálne čistenie.",
    "product_limit": "Produkt nie je určený ako univerzálny prostriedok na vlnu a nenahrádza profesionálne čistenie saka, kabáta, lepenú výstuž ani opravu splstnatenia a predratej priadze.",
    "category_intro": "Pri tvíde sa kategória pracích gélov používa až po potvrdení, že celý výrobok je určený na domáce pranie a nepotrebuje špecializovanú starostlivosť o vlnu.",
    "category_text": "V kategórii pracích gélov nájdete riešenia pre bežnú bielizeň. Pred použitím na tvíd porovnajte etiketu, vlnený podiel, podšívku, výstuž a odporúčanie výrobcu.",
    "related": [
        ("Ako sa starať o ľahký vlnený kabát", ARTICLE_WOOL_COAT),
        ("Akryl verzus vlna", ARTICLE_WOOL_ACRYLIC),
        ("Prečo sa oblečenie žmolkuje", ARTICLE_PILLING),
        ("Odolnosť textilu proti oderu", ARTICLE_ABRASION),
        ("Ako čítať štítok na oblečení", ARTICLE_LABEL),
        ("Ako sušiť bielizeň bez zatuchnutia", ARTICLE_DRYING),
    ],
    "faq_title": "tvíd, žmolky a čistenie",
    "faq": [
        ("Čo je tvíd?", "Rodina textúrovaných tkanín, tradične vyrábaných z vlnenej priadze. Nie je to jeden vzor ani jedno povinné zloženie."),
        ("Je každý tvíd Harris Tweed?", "Nie. Harris Tweed musí spĺňať presnú zákonnú definíciu pôvodu a výroby a niesť príslušnú certifikáciu."),
        ("Môže sa tvídové sako prať v práčke?", "Iba pri výslovnom symbole pre celý odev. Väčšina štruktúrovaných sák má vrstvy, ktoré domáce ponorenie nemusí zniesť."),
        ("Ako odstrániť prach z tvídu?", "Jemnou čistou odevnou kefou pozdĺžne a s malým tlakom, po skúške na nenápadnom mieste."),
        ("Ako odstrániť žmolky z tvídu?", "Najprv odlíšte žmolok od zámerného nopku a priadze. Nevytrhávajte ho; vhodný nástroj použite iba kontrolovane na pevnej ploche."),
        ("Ako sušiť mokrý tvídový kabát?", "Pri izbovej teplote s oporou tvaru, mimo radiátora a priameho slnka. Kontrolujte podšívku a výstuž."),
        ("Ako často čistiť tvíd?", "Podľa znečistenia a etikety. Vetranie a kefovanie medzi noseniami môžu znížiť potrebu častého celoplošného čistenia."),
        ("Ako skladovať tvíd proti moliam?", "Čistý a úplne suchý, bez potravinových škvŕn, v kontrolovanom obale alebo priestore a s pravidelnou kontrolou."),
        ("Je rybia kosť vždy tvíd?", "Nie. Rybia kosť je vzor väzby a možno ju vyrobiť z rôznych vlákien aj v látke, ktorá sa ako tvíd nepredáva."),
    ],
}


CANVAS: dict[str, object] = {
    "title": "Čo je canvas: pevné plátno, škvrny a správne pranie",
    "link": "co-je-canvas-pevne-platno-skvrny-a-spravne-pranie",
    "meta": "Čo je canvas alebo pevné plátno a ako čistiť canvas tašku, tenisky, zásteru, pracovný odev či poťah bez poškodenia farby, náteru a tvaru.",
    "short": "Canvas je pevná plátnová alebo príbuzná tkanina, nie jedno vlákno. Môže byť bavlnený, syntetický, zmesový, voskovaný alebo potiahnutý. Pred praním preto treba rozlíšiť metráž od celej tašky, topánky či poťahu a overiť, či výrobok vôbec smie byť ponorený.",
    "name": "canvas",
    "genitive": "canvasu",
    "locative": "canvase",
    "construction_summary": "pevnejšiu tkaninu najčastejšie v plátnovej väzbe alebo jej blízkej odvodenine, vyrobenú z hrubších a hustejších priadzí pre vyššiu stabilitu a odolnosť",
    "label_details": "vláknové zloženie, gramáž, väzbu, potlač, pigmentové farbenie, vosk alebo polymérny náter, výstuž, podšívku, lepené diely, kovanie a povolenie ponorenia",
    "residue_place": "hustej plátnovej väzbe, pri švoch, pod popruhmi, v rohoch, okolo kovania, vo vrstve náteru a v hrubom leme",
    "friction_risk": "povrchovú farbu, vystupujúcu priadzu, okraj náteru, potlač alebo šev pri kovaní",
    "drying_advice": "Prateľnú samostatnú látku alebo odev vyrovnajte podľa švov a sušte s prúdením vzduchu. Tašku, tenisku alebo vystužený poťah vytvarujte bez preplnenia a nechajte vyschnúť zvnútra aj zvonka mimo radiátora.",
    "heat_risk": "bavlnené zrazenie, syntetické vlákno, pigment, potlač, vosk, náter, lepidlo, penovú výstuž, gumu a tvar kovania",
    "failure_sign": "odlupovanie náteru, prasknutie pri prehybe, rozpad lepidla, posun nití pri šve alebo trvalá deformácia výstuže",
    "answer": "Canvas je pevná tkanina, zvyčajne v plátnovej väzbe alebo jej blízkej konštrukcii, nie názov jedného vlákna. Môže byť z bavlny, polyesteru, polyamidu alebo zo zmesi a môže mať vosk, vodeodolný náter, pigmentovú potlač, podšívku či lepenú výstuž. Preto nemožno jedným postupom prať canvas zásteru, tašku, tenisky a markízovú látku. Najprv odstráňte voľný prach, identifikujte škvrnu a prečítajte etiketu celého výrobku. Do práčky patrí iba kus, ktorý ju výslovne povoľuje. Ťažký canvas perte v primerane malej náplni, aby sa opláchol, ale neudieral do bubna spolu s kovovým kovaním. Potiahnuté, voskované, lepené alebo vystužené predmety čistite spôsobom výrobcu, často iba povrchovo. Sušte ich vytvarované, zvnútra aj zvonka, bez radiátora a prudkého slnka.",
    "intro": "Vyhľadávanie ako vyprať canvas tenisky alebo ako vyčistiť canvas tašku zvádza k jednému receptu so sódou a kefou. Práve to je riziko. Pevné plátno môže mechanicky pôsobiť odolne, no farba, lepidlo podrážky, kožený lem, vosk alebo polyuretánový náter môžu mať oveľa nižší limit. Hustá tkanina navyše drží mastnotu a zvyšky pracieho prostriedku hlbšie pri švoch a schne pomalšie v preložených rohoch. Bezpečný postup preto začína určením výrobku, nie silou materiálu. Samostatná bavlnená zástera a vystužený batoh potrebujú odlišnú vodu, mechaniku aj sušenie.",
    "quick": [
        "<strong>Canvas je konštrukcia, nie jedno vlákno:</strong> vždy čítajte bavlnu, polyester, polyamid alebo zmes na etikete.",
        "<strong>Pevný povrch môže mať citlivú úpravu:</strong> vosk, náter, pigment a lepidlo menia povolené čistenie.",
        "<strong>Voľnú špinu odstráňte pred vodou:</strong> piesok a prach sa pri drhnutí správajú ako abrazívum.",
        "<strong>Ťažká tkanina potrebuje oplach:</strong> preplnený bubon drží gél v lemoch a rohoch.",
        "<strong>Taška a teniska nie sú metráž:</strong> podšívka, výstuž, guma, koža a kovanie môžu vylúčiť ponorenie.",
        "<strong>Sušte celý objem:</strong> suchý líc neznamená suchý roh, jazyk topánky alebo penovú výstuž.",
    ],
    "overview_heading": "Čo je canvas a prečo slovo plátno nestačí na výber prania",
    "overview": [
        "CottonWorks uvádza canvas medzi bežnými príkladmi plátnovej väzby, v ktorej sa osnovné a útkové nite pravidelne križujú nad a pod sebou. Vysoký počet väzbových bodov podporuje stabilitu, no výslednú pevnosť stále mení hrúbka priadze, dostava, gramáž, zloženie a dokončenie. Canvas môže byť mäkký odevný aj veľmi tuhý technický.",
        "V angličtine sa používa aj výraz duck canvas, ktorý zvyčajne označuje husto tkané pevné plátno. Obchodné názvy sa však na spotrebiteľských výrobkoch používajú voľne. Teniska môže mať canvasový zvršok, ale gumenú podrážku, lepidlo, penový jazyk a syntetickú podšívku. Taška môže obsahovať kartónovú alebo plastovú výstuž. Voda zasiahne celý systém.",
        "Odolnosť proti oderu a pevnosť proti roztrhnutiu sú rozdielne vlastnosti. Látka môže dlho znášať povrchové trenie, ale po prepichnutí sa trhlina šíriť pozdĺž priadze. Laboratórna skúška navyše platí pre definovanú vzorku a podmienky. Domáci používateľ musí sledovať švy, prehyby, kovanie a náter, kde vzniká miestne napätie.",
    ],
    "table1_heading": "Druhy canvasu a prvý bezpečný krok",
    "table1_intro": "Rovnaký názov sa používa na samostatnú látku aj zložité výrobky. Najprv určte zloženie a povrchovú úpravu.",
    "table1_headers": ["Variant", "Typická skladba", "Hlavné riziko", "Prvý krok"],
    "table1_rows": [
        ("Bavlnený canvas", "Hustá bavlnená tkanina, niekedy pigmentovo farbená alebo predzrážaná.", "Zrazenie, blednutie, tvrdý lom a pomalý oplach.", "Skontrolovať etiketu, farbu a rozmery."),
        ("Syntetický canvas", "Polyesterová alebo polyamidová tkanina, často s náterom.", "Teplo, odlupovanie náteru, statika a zadržiavanie mastnoty.", "Overiť presné vlákno a povolenie ponorenia."),
        ("Voskovaný canvas", "Tkanina s voskovou impregnáciou.", "Vymytie alebo nerovnomerné premiestnenie vosku.", "Čistiť iba podľa pokynov výrobcu, spravidla povrchovo."),
        ("Canvas taška alebo batoh", "Tkanina, podšívka, popruhy, výstuž, zips a kovanie.", "Deformácia, korózia, farbenie a dlhé schnutie rohov.", "Vyprázdniť, vysať a overiť, či smie byť celý predmet namočený."),
        ("Canvas tenisky", "Tkaný zvršok, guma, lepidlo, výstuž a stielka.", "Rozpad lepidla, deformácia a žlté mapy pri schnutí.", "Vybrať šnúrky a stielky podľa návodu a čistiť po častiach."),
    ],
    "sections": [
        {
            "heading": "Ako vyčistiť canvas tašku alebo batoh",
            "paragraphs": [
                "Tašku úplne vyprázdnite, otvorte všetky vrecká a voľný prach vysajte úzkym nadstavcom bez zachytenia podšívky. Skontrolujte kartónovú výstuž, penové panely, kožené lemy, kovanie a farebnú podšívku. Ak výrobca povoľuje iba povrchové čistenie, neponárajte predmet podľa toho, že viditeľný vonkajší canvas je bavlnený.",
                "Škvrnu odsajte a pracujte na malej ploche mäkkou handričkou. Vodu nedovoľte stekať pod výstuž. Po čistení vytvarujte rohy a otvorte zipsy pre prúdenie vzduchu. Tašku neplňte novinami, ak môže tlačová farba migrovať; použite čistú farebne stálu oporu, ktorá nezablokuje schnutie.",
            ],
            "callout": {
                "title": "Čo skontrolovať pred ponorením canvasového výrobku",
                "items": [
                    "Povoľuje etiketa vodu pre celý predmet, nielen pre vrchnú tkaninu?",
                    "Je vo vnútri kartón, pena, lepená výstuž, koža, kov alebo nestálofarebná podšívka?",
                    "Je povrch voskovaný, laminovaný, pogumovaný alebo pigmentovo potlačený?",
                    "Dokážete predmet po čistení otvoriť a vysušiť vo všetkých rohoch a vrstvách?",
                ],
                "background": "#f7fbf8",
                "border": "#dbe5de",
            },
        },
        {
            "heading": "Ako vyčistiť canvas tenisky bez poškodenia lepidla",
            "paragraphs": [
                "Odstráňte suché blato mäkkou kefou, vyberte šnúrky a stielky iba podľa konštrukcie a skontrolujte oddeľovanie podrážky. Čistiaci roztok skúšajte pri vnútornom okraji. Tkaninu nepresycujte, ak výrobca nepovoľuje ponorenie. Tvrdá kefa môže poškodiť pigment a zatlačiť špinu hlbšie k švu.",
                "Po čistení opláchnite zvyšok z povrchu kontrolovanou vlhkou handričkou a topánku vytvarujte. Sušte pri izbovej teplote s prúdením vzduchu, nie na radiátore. Pri bielej gumovej hrane použite iba kompatibilný postup a nedovoľte agresívnemu produktu preniknúť do farebného canvasu alebo lepeného spoja.",
            ],
        },
        {
            "heading": "Ako prať canvas zásteru, pracovné nohavice alebo poťah",
            "paragraphs": [
                "Samostatný prateľný textil prezrite pri švoch, vreckách a miestach s kovovým kovaním. Zipsy zaistite, odnímateľné výstuže vyberte podľa návodu a škvrny predbežne ošetrite podľa typu. Ťažký canvas nekombinujte s jemným oblečením ani s veľkou náplňou, v ktorej sa hrubé lemy neopláchnu.",
                "Program vyberte podľa vlákna a etikety, nie podľa slova pracovný. Bavlna môže zmeniť rozmer, polyester citlivo reagovať na vysoké teplo a náter sa poškodiť mechanikou. Po praní kus ihneď vyberte, zarovnajte švy a sušte tak, aby sa v dvojitom leme alebo vrecku nedržala voda.",
            ],
        },
        {
            "heading": "Mastnota, blato, tráva a pigmentové škvrny",
            "paragraphs": [
                f"Suché blato nechajte zaschnúť a mechanicky ho odstráňte bez brúsenia povrchu. Mastnotu riešte absorbovaním a kompatibilným tenzidom; podrobný postup je v článku <a href=\"{ARTICLE_OIL}\">ako odstrániť olejovú a mastnú škvrnu</a>. Nezahrievajte ju skôr, než zmizne, a nespoliehajte sa na to, že tmavý canvas stopu skryje.",
                "Tráva a farebné pigmenty môžu vyžadovať inú chémiu než tuk. Kombinovanie octu, zásady, bielidla a odmasťovača bez oplachu môže poškodiť farbu alebo náter a vytvoriť nebezpečnú zmes. Použite jeden overený krok, opláchnite ho a výsledok hodnotíte po vysušení. Pri potlači pracujte z rubu iba vtedy, ak to konštrukcia povoľuje.",
            ],
        },
        {
            "heading": "Voskovaný a potiahnutý canvas sa neperie ako obyčajné plátno",
            "paragraphs": [
                "Vosk vypĺňa časť povrchu a mení odpudzovanie vody aj vzhľad ohybov. Horúca voda, detergent a mechanika ho môžu vymyť alebo premiestniť. Výrobca môže odporučiť suché kefovanie, vlhkú handričku a neskoršiu obnovu kompatibilným voskom. Univerzálny prací gél preto nie je správny prvý krok.",
                "Polymérny náter alebo laminácia môže praskať pri opakovanom ohybe a oddeľovať sa pri nevhodnej chémii. Lepkavý, prášivý alebo bublinkový povrch je materiálové poškodenie, nie bežná škvrna. Nezakrývajte ho olejom ani silikónom bez pokynu. Pri bezpečnostnom alebo vodotesnom výrobku posúďte aj funkciu, nie iba čistý vzhľad.",
            ],
        },
        {
            "heading": "Oder, pretrhnutie a poškodenie pri šve",
            "paragraphs": [
                f"Canvas môže dobre znášať plošné trenie, no pri prepichnutí alebo poškodenom šve sa napätie sústredí do malej oblasti. Článok <a href=\"{ARTICLE_TEAR}\">pevnosť textilu v ťahu a proti roztrhnutiu</a> vysvetľuje, prečo ide o odlišné mechanizmy. Dieru pred praním stabilizujte, aby sa okraj v bubne ďalej netrhal.",
                "Popruh, roh tašky, koleno a miesto pri kovovom oku sú typické zóny oderu. Svetlejšie miesto nemusí byť škvrna, ale strata pigmentu alebo povrchových vlákien. Drhnutím ho nevrátite. Skontrolujte rub a hrúbku. Ak je priadza stenčená, uprednostnite opravu a rozloženie záťaže pred ďalším intenzívnym čistením.",
            ],
        },
        {
            "heading": "Prečo canvas po praní stvrdne, zmenší sa alebo má mapy",
            "paragraphs": [
                "Tvrdosť môže pochádzať z prirodzene hustej väzby, minerálov vody, zvyšku gélu alebo prudkého vysušenia. Zrazenie súvisí so zložením, predchádzajúcim spracovaním a uvoľnením pnutí. Kým je tkanina mokrá, meranie nie je porovnateľné. Kus nechajte úplne vyschnúť a potom ho zmerajte bez naťahovania.",
                "Žltá alebo biela mapa na topánke môže vzniknúť presunom nečistôt a rozpustených zložiek k okraju mokrej oblasti. Na farebnom plátne zas nerovnomerný oplach vytvorí svetlé šmuhy. Opakované bodové premočenie môže hranicu posúvať. Ak výrobok povoľuje rovnomerné opláchnutie, urobte ho kontrolovane; pri lepidle alebo výstuži zásah zastavte.",
            ],
        },
        {
            "heading": "Ako vybrať canvas podľa použitia a údržby",
            "paragraphs": [
                "Na zásteru sledujte prateľnosť a uvoľňovanie olejových škvŕn, na tašku švy, rohy, výstuž a farbenie, na poťah rozmer a odnímateľnosť. Vyššia gramáž môže zvýšiť hmotnosť a predĺžiť schnutie, nie automaticky vyriešiť trhanie pri zlom šve. Pri outdoorovom výrobku skontrolujte náter a možnosť jeho obnovy.",
                "Pred prvým použitím si odfoťte farbu a zmerajte prateľný kus. Pri tmavom pigmentovom canvase počítajte s možným estetickým blednutím a oddeľte ho od svetlej bielizne. Kvalitný výrobok má jasný návod, primerané spevnenie namáhaných miest a konštrukciu, ktorú možno vysušiť a opraviť.",
            ],
        },
    ],
    "table2_heading": "Canvas po čistení: škvrna, zvyšok produktu alebo poškodenie",
    "table2_intro": "Hustý povrch môže skryť zvyškovú vlhkosť aj mechanickú zmenu. Pred ďalším zásahom skontrolujte rub, švy, náter a úplné vysušenie.",
    "table2_headers": ["Prejav", "Možná príčina", "Čo overiť", "Ďalší krok"],
    "table2_rows": [
        ("Biela šmuha", "Zvyšok gélu, minerály vody, oder pigmentu alebo presunutá nečistota.", "Či sa mení po navlhčení a či je povrch stenčený.", "Pri povolení opláchnuť; mechanickú stratu farby nedrhnúť."),
        ("Tkanina je tvrdá", "Hustá väzba, prudké sušenie, minerály alebo zvyšok produktu.", "Dávku, oplach, náter a omak na rube.", "Nepridávať aviváž naslepo; najprv vyriešiť oplach a spôsob sušenia."),
        ("Náter sa odlupuje", "Starnutie, ohyb, teplo alebo nekompatibilná chémia.", "Okraje odlupovania a funkciu vodeodolnosti.", "Pranie zastaviť a postupovať podľa možnosti opravy výrobcu."),
        ("Taška stratila tvar", "Premočená výstuž, dlhé stlačenie alebo prudké odstreďovanie.", "Vnútorné panely, rohy a lepené miesta.", "Sušiť vytvarovanú; násilné teplo ani preplnenie tvar neobnoví."),
        ("Pri šve vznikla trhlina", "Koncentrované zaťaženie, poškodená priadza alebo kovanie.", "Rub, stehy a zdravú rezervu látky.", "Pred ďalším používaním opraviť a rozložiť záťaž."),
    ],
    "steps_heading": "Ako bezpečne vyčistiť canvasový výrobok krok za krokom",
    "steps": [
        "Určte typ výrobku, vláknové zloženie, náter, vosk, výstuž, lepidlo, podšívku a kovanie.",
        "Odstráňte voľný prach, piesok a blato nasucho, aby pri mokrom drhnutí nepôsobili ako abrazívum.",
        "Identifikujte škvrnu a kompatibilný produkt vyskúšajte na skrytom mieste vrátane farby a povrchu.",
        "Ponorte alebo vložte do práčky iba výrobok, ktorý to výslovne povoľuje, a oddeľte ťažké kovanie.",
        "Použite primeranú dávku a náplň, aby sa hrubé lemy, rohy a vrecká mohli dôkladne opláchnuť.",
        "Po čistení kus ihneď vytvarujte, otvorte dutiny a sušte zvnútra aj zvonka s prúdením vzduchu.",
        "Po úplnom vysušení skontrolujte farbu, náter, lepidlo, švy, rohy a zvyškový pach.",
        "Pred ďalším používaním opravte trhlinu alebo uvoľnené kovanie a obnovujte iba kompatibilnú úpravu.",
    ],
    "remember": [
        "Je canvas bavlnený, syntetický alebo zmesový?",
        "Je povrch obyčajný, pigmentovaný, voskovaný, pogumovaný alebo laminovaný?",
        "Obsahuje predmet lepidlo, výstuž, penu, kožu, gumu alebo kovanie?",
        "Povoľuje etiketa ponorenie alebo iba povrchové čistenie?",
        "Dokáže voda a oplach prejsť cez hrubé lemy, rohy a vrecká?",
        "Bude sa predmet sušiť otvorený a vytvarovaný až do úplného preschnutia?",
    ],
    "mistakes": [
        "Hodiť tašku alebo tenisky do práčky iba preto, že vrchný materiál je canvas.",
        "Drhnúť piesok a suché blato mokrou tvrdou kefou do pigmentu a priadze.",
        "Použiť horúcu vodu a detergent na voskovaný alebo potiahnutý povrch.",
        "Prepchať bubon ťažkým plátnom a nechať gél v hrubých lemoch.",
        "Sušiť lepenú topánku či vystuženú tašku na radiátore.",
        "Považovať svetlý oder za škvrnu a ďalej ho mechanicky zosvetľovať.",
    ],
    "expert_heading": "Odbornejší pohľad: plátnová väzba, oder a uvoľňovanie škvŕn",
    "expert": [
        "Plátnová väzba má časté križovanie osnovy a útku, čo podporuje stabilitu, ale nevytvára jednu úroveň pevnosti. Hrubšia priadza, vyššia dostava a dokončenie môžu zvýšiť hmotnosť aj tuhosť. Pri ohybe sa však povrchová vrstva a vonkajšie vlákna opakovane naťahujú; pri nátere vzniká ďalšie rozhranie, ktoré môže praskať.",
        "ASTM D4157 výslovne upozorňuje, že laboratórny oder závisí od vlákna, priadze, konštrukcie, dokončenia, abradantu, tlaku a hodnotenia a nemusí predpovedať skutočnú životnosť. ASTM D1424 meria šírenie už začatého roztrhnutia iným princípom. Čísla z týchto skúšok sa preto nesmú zamieňať alebo uvádzať bez metódy.",
        "AATCC TM215 hodnotí schopnosť prateľných tkanín uvoľňovať typické domáce nečistoty pri definovanom praní a nie je skúškou účinnosti konkrétneho komerčného gélu. Táto hranica je praktická aj doma: škvrna na potiahnutej taške nevypovedá o sile detergentu rovnako ako škvrna na samostatnej bavlnenej zástere.",
    ],
    "source_intro": "Zdroje vysvetľujú plátnovú väzbu, oder, šírenie trhliny, uvoľňovanie domácich nečistôt a symboly. Neoprávňujú ponoriť vystužený, voskovaný alebo lepený výrobok bez jeho návodu.",
    "sources": [
        ("CottonWorks: plátnová väzba a canvas", COTTONWORKS_WEAVING),
        ("ASTM D4157: oder tkanín a hranice interpretácie", ASTM_ABRASION),
        ("ASTM D1424: pevnosť pri šírení roztrhnutia", ASTM_TEAR),
        ("AATCC TM215: uvoľňovanie domácich nečistôt", AATCC_SOIL),
        ("AATCC TM135: rozmerové zmeny po domácom praní", AATCC_DIMENSION),
        ("EÚ 1007/2011: označovanie textilných vlákien", EU_FIBRE_LABEL),
        ("GINETEX: význam symbolov ošetrovania", GINETEX),
    ],
    "product_intro": "Prací gél má miesto pri prateľnom canvas odeve alebo samostatnom domácom textile. Voskovaná taška, lepená teniska, náter alebo výstuž môžu vyžadovať iba povrchové čistenie.",
    "product_text": "Hypoalergénny prací gél Vevo Ylang Absolute je konkrétna možnosť pre kompatibilný prateľný canvas bez citlivej úpravy. Dávku prispôsobte ťažšej náplni a zabezpečte, aby sa hrubé lemy dôkladne opláchli.",
    "product_limit": "Produkt nie je automaticky vhodný na vosk, vodeodolný náter, kožu, gumu, lepidlo alebo penovú výstuž a neopraví odlupovanie, pretrhnutie ani stratu pigmentu.",
    "category_intro": "Pri výbere pracieho gélu odlíšte samostatnú prateľnú tkaninu od zložitého výrobku. Najcitlivejší detail určuje, či sa kategória pracích produktov vôbec použije.",
    "category_text": "V kategórii pracích gélov nájdete možnosti pre bežné prateľné odevy a domáci textil. Pri canvase porovnajte dávkovanie, zloženie, náter a pokyn celého výrobku.",
    "related": [
        ("Čo je bavlna", ARTICLE_COTTON),
        ("Odolnosť textilu proti oderu", ARTICLE_ABRASION),
        ("Pevnosť textilu v ťahu a proti roztrhnutiu", ARTICLE_TEAR),
        ("Ako odstrániť mastné škvrny", ARTICLE_OIL),
        ("Prečo sa oblečenie zráža", ARTICLE_SHRINKAGE),
        ("Ako čítať štítok na oblečení", ARTICLE_LABEL),
    ],
    "faq_title": "canvas, plátno a čistenie",
    "faq": [
        ("Čo je canvas?", "Pevná tkanina, zvyčajne v plátnovej väzbe alebo príbuznej konštrukcii. Môže byť z rôznych vlákien a s rôznymi úpravami."),
        ("Je canvas vždy bavlna?", "Nie. Môže byť bavlnený, polyesterový, polyamidový alebo zmesový."),
        ("Môže sa canvas prať v práčke?", "Iba ak to povoľuje etiketa celého výrobku. Taška alebo teniska obsahuje aj výstuž, lepidlo, gumu a kovanie."),
        ("Ako vyčistiť canvas tašku?", "Vyprázdnite ju, odstráňte prach, overte výstuž a podšívku a použite povrchové alebo celkové čistenie presne podľa výrobcu."),
        ("Ako vyčistiť canvas tenisky?", "Odstráňte suché blato, čistite jemne po častiach a sušte pri izbovej teplote. Práčku nepoužívajte bez povolenia."),
        ("Ako odstrániť olej z canvasu?", "Odsajte prebytok, použite kompatibilný tenzid po skrytej skúške a škvrnu nezahrievajte, kým nezmizne."),
        ("Prečo canvas po praní stvrdol?", "Príčinou môže byť hustá väzba, zvyšok gélu, minerály vody, prudké sušenie alebo zmena povrchovej úpravy."),
        ("Ako sušiť canvas tašku?", "Otvorenú a vytvarovanú, zvnútra aj zvonka, s prúdením vzduchu a bez radiátora."),
        ("Čo je voskovaný canvas?", "Canvas s voskovou úpravou. Zvyčajne sa neperie ako obyčajné plátno a ošetruje sa podľa systému výrobcu."),
    ],
}


ARTICLES: list[dict[str, object]] = [RAMIE, CUPRO, TWEED, CANVAS]


def preflight_links(articles: list[dict[str, object]]) -> dict[str, object]:
    target_urls = {f"{BASE}/n/{article['link']}" for article in articles}
    outgoing_urls = {
        urljoin(BASE, href) if href.startswith("/") else href
        for article in articles
        for href in article_hrefs(str(article["long"]))
    }
    with ThreadPoolExecutor(max_workers=6) as executor:
        checks = list(executor.map(fetch_status, sorted(target_urls | outgoing_urls)))
    for check in checks:
        if check["url"] in target_urls:
            check["expected_status"] = 404
            check["ok"] = check["status"] == 404
    report = {
        "batch": "batch-48",
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "target_count": len(target_urls),
        "outgoing_count": len(outgoing_urls),
        "check_count": len(checks),
        "failure_count": sum(not check["ok"] for check in checks),
        "checks": checks,
    }
    OUT_PREFLIGHT.parent.mkdir(parents=True, exist_ok=True)
    OUT_PREFLIGHT.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return report


def main() -> None:
    candidate_titles = [line.strip() for line in CANDIDATES.read_text(encoding="utf-8-sig").splitlines() if line.strip()]
    article_by_title = {str(article["title"]): article for article in ARTICLES}
    if len(article_by_title) != len(ARTICLES) or set(candidate_titles) != set(article_by_title):
        raise SystemExit("Candidate titles and article definitions do not match exactly")
    slugs = [str(article["link"]) for article in ARTICLES]
    if len(slugs) != len(set(slugs)):
        raise SystemExit("Batch contains duplicate slugs")

    rendered: list[dict[str, object]] = []
    metrics: list[dict[str, object]] = []
    for article in ARTICLES:
        body = render_article(article)
        old_damage = f"Prach alebo piesok možno často odstrániť nasucho, škvrna vyžaduje lokálne ošetrenie, pach potrebuje odstrániť zdroj a {article['failure_sign']} je mechanická chyba."
        new_damage = f"Prach alebo piesok možno často odstrániť nasucho, škvrna vyžaduje lokálne ošetrenie a pach potrebuje odstrániť zdroj. Prejav ako {article['failure_sign']} je mechanické poškodenie."
        old_stop = f"Zásah zastavte, ak sa uvoľňuje farba, povrch sa lepí, vrstva sa oddeľuje, {article['failure_sign']} alebo etiketa vyžaduje odborný postup."
        new_stop = f"Zásah zastavte, ak sa uvoľňuje farba, povrch sa lepí alebo vrstva sa oddeľuje, ak vidíte {article['failure_sign']}, alebo ak etiketa vyžaduje odborný postup."
        body = body.replace(old_damage, new_damage).replace(old_stop, new_stop)
        body = body.replace(
            f"Pri {article['genitive']} navyše",
            f"Pri {article['locative']} navyše",
        )
        body = body.replace(
            f"Pri {article['name']} je dôležité",
            f"Pri {article['locative']} je dôležité",
        )
        body = body.replace(
            f"pri {article['name']} zostať",
            f"pri {article['locative']} zostať",
        )
        public_text = f"{article['title']} {article['short']} {body}"
        visible = visible_text(body)
        if FORBIDDEN_PUBLIC_RE.search(public_text):
            raise SystemExit(f"Forbidden public wording in {article['title']}")
        if FIXED_PRICE_RE.search(visible_text(public_text)):
            raise SystemExit(f"Fixed price found in {article['title']}")
        one_character_paragraphs = [
            visible_text(value).strip()
            for value in re.findall(r"<p\b[^>]*>(.*?)</p>", body, flags=re.IGNORECASE | re.DOTALL)
            if len(visible_text(value).strip()) == 1
        ]
        metric = {
            "title": article["title"],
            "slug": article["link"],
            "words": len(WORD_RE.findall(visible)),
            "h2": len(re.findall(r"<h2\b", body, re.IGNORECASE)),
            "tables": len(re.findall(r"<table\b", body, re.IGNORECASE)),
            "responsive_tables": len(re.findall(r'<div\b[^>]*style="[^"]*overflow-x:\s*auto', body, re.IGNORECASE)),
            "styled_blocks": len(re.findall(r"<div\b[^>]*style=", body, re.IGNORECASE)),
            "action_buttons": len(re.findall(r'<a\b[^>]*style="[^"]*display:\s*inline-block', body, re.IGNORECASE)),
            "one_character_paragraphs": len(one_character_paragraphs),
        }
        if metric["words"] < 2800:
            raise SystemExit(f"Article is too short: {article['title']} ({metric['words']} words)")
        if metric["h2"] < 24 or metric["tables"] < 2 or metric["responsive_tables"] != metric["tables"]:
            raise SystemExit(f"Article structure is incomplete: {article['title']} ({metric})")
        if metric["styled_blocks"] < 10 or metric["action_buttons"] < 2 or metric["one_character_paragraphs"]:
            raise SystemExit(f"Article visual blocks are incomplete: {article['title']} ({metric})")
        metrics.append(metric)
        rendered.append({
            "title": article["title"],
            "short": article["short"],
            "long": body,
            "link": article["link"],
            "date_posted": PUBLISH_DATE,
            "time_posted": "15:00:00",
            "commenting": False,
            "title_tag": article["title"],
            "description": article["meta"],
        })

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(rendered, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    report = preflight_links(rendered)
    if report["failure_count"]:
        failed = [check for check in report["checks"] if not check["ok"]]
        print(json.dumps({"failed_links": failed}, ensure_ascii=False, indent=2))
        raise SystemExit("Batch 48 link preflight failed")
    print(json.dumps({"article_count": len(rendered), "metrics": metrics, "link_preflight": True}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
