#!/usr/bin/env python3
"""Build and validate VEVO batch 52 distinct textile-system articles."""

from __future__ import annotations

import json
import re
from pathlib import Path

import build_batch_51_woven_surfaces_and_yarns as batch51
from build_batch_51_woven_surfaces_and_yarns import (
    FIXED_PRICE_RE,
    FORBIDDEN_PUBLIC_RE,
    WORD_RE,
    jaccard,
    preflight_links,
    render_article,
    seven_word_shingles,
    visible_text,
)


PUBLISH_DATE = "2026-08-27"
CANDIDATES = Path("content/VEVO_CONTENT/batches/batch-52-candidates-2026-08-27.txt")
OUT_JSON = Path("content/VEVO_CONTENT/imports/batch-52-2026-08-27-articles.json")
OUT_PREFLIGHT = Path("content/VEVO_CONTENT/exports/batch-52-2026-08-27-link-preflight.json")

EU_FIBRE_LABEL = "https://eur-lex.europa.eu/eli/reg/2011/1007/oj"
GINETEX = "https://www.ginetex.net/share/article/4201/care-symbols"
AATCC_DIMENSION = "https://members.aatcc.org/store/tm135/543/"
AATCC_COLOR = "https://members.aatcc.org/store/tm61/495/"
COTTONWORKS_WEAVES = "https://cottonworks.com/learning-hub/weaving/basic-woven-fabric-designs/"
COTTONWORKS_WEAVING = "https://cottonworks.com/wp-content/uploads/2023/03/Weaving-101.pdf"
COTTONWORKS_FINISHING = "https://cottonworks.com/learning-hub/finishing/mechanical-finishing/"
ASTM_AIR = "https://store.astm.org/standards/d737"
ASTM_COUNT = "https://store.astm.org/d3775-17r23.html"
ASTM_ABRASION = "https://store.astm.org/d4966-22r26.html"
ASTM_SEAM = "https://store.astm.org/d1683_d1683m-17e01.html"
IDFL_TESTS = "https://idfl.com/info/explanation-of-down-and-feather-tests/"
IDFL_DOWNPROOF = "https://idfl.com/info/finished-products-downproofness-issues/"
IDFL_TEXTILES = "https://idfl.com/services/textile-testing/"
IDFL_THREAD_COUNT = "https://www.idfl.com/wp-content/uploads/2021/05/IDFL_Textiles_-_Downproofness_vs_Thread_Count.pdf"
FAO_ABACA = "https://www.fao.org/economic/futurefibres/fibres/abaca0/es/?hl=en-PH"
FAO_HARD_FIBRES = "https://www.fao.org/markets-and-trade/commodities-overview/fibres/jute-and-hard-fibres/en"
PHILFIDA_MANUAL = "https://philfida.da.gov.ph/images/Publications/abacasustainabilitymanual/ASM.pdf"
PHILFIDA_GUIDE = "https://philfida.da.gov.ph/images/Publications/Technoguides/abaca-technoguide-2024.pdf"
PENN_SINAMAY = "https://collections.penn.museum/collections/object/524905"
FOWLER_FIBRES = "https://fowler.ucla.edu/exhibitions/material-choices-bast-and-leaf-fiber-textiles/"
SNL_MOLESKIN = "https://snl.no/moleskin"

ARTICLE_LABEL = "/n/ako-citat-stitok-na-obleceni-material-symboly-prania-a-spravny-program"
ARTICLE_STAIN = "/n/ako-odstranit-zuvacku-krv-vosk-a-ine-skvrny-z-oblecenia"
ARTICLE_COLOR = "/n/stalofarebnost-textilu-preco-farby-blednu-pri-prani-svetle-a-treni"
ARTICLE_SHRINKAGE = "/n/preco-sa-oblecenie-zrazi-po-prani-teplota-vlakna-susicka-a-prevencia"
ARTICLE_ODOR = "/n/preco-moje-oblecenie-zapacha-po-prani-priciny-a-riesenia"
ARTICLE_DRYING = "/n/ako-susit-bielizen-v-malom-byte-bez-zatuchnutia"
ARTICLE_IRONING = "/n/ako-vyzehlit-koselu-kompletny-sprievodca-pre-dokonaly-vysledok"
ARTICLE_PILLING = "/n/preco-sa-oblecenie-zmolkuje-vlakna-trenie-pranie-a-susenie"
ARTICLE_SNAGGING = "/n/zatrhavanie-textilu-preco-vznikaju-vytiahnute-ocka-a-ako-im-predchadzat"
ARTICLE_POPLIN = "/n/co-je-popelin-hladka-koselova-tkanina-vlastnosti-a-starostlivost"
ARTICLE_CHAMBRAY = "/n/co-je-chambray-farebna-osnova-svetly-utok-a-spravne-pranie"
ARTICLE_PANAMA = "/n/co-je-panamova-vazba-kosikova-tkanina-posun-niti-a-pranie"
ARTICLE_COTTON = "/n/co-je-bavlna-vlastnosti-vyhody-nevyhody-a-starostlivost"
ARTICLE_FLANNEL = "/n/co-je-flanel-preco-hreje-ako-sa-perie-a-preco-moze-zmolkovat"
ARTICLE_CORDUROY = "/n/co-je-mansester-rebrovana-latka-prach-v-rebrach-a-spravne-pranie"
ARTICLE_SATIN = "/n/co-je-saten-nie-je-to-vzdy-hodvab-a-ako-ho-spravne-prat"
ARTICLE_CANVAS = "/n/co-je-canvas-pevne-platno-skvrny-a-spravne-pranie"
ARTICLE_PERCALE = "/n/co-je-perkal-husta-tkanina-na-obliecky-vlastnosti-a-pranie"
ARTICLE_FEATHER_PILLOW = "/n/ako-vyprat-paperovy-vankus-kompletny-sprievodca"
ARTICLE_DUVET = "/n/ako-prat-paplon-a-prikryvku-velkost-bubna-vypln-a-susenie-bez-zapachu"
ARTICLE_PILLOWS = "/n/ako-prat-vankuse-dute-vlakno-perie-pamaetova-pena-a-vona-po-prani"
ARTICLE_KAPOK = "/n/co-je-kapok-lahka-rastlinna-vypln-vlhkost-a-starostlivost"
ARTICLE_SISAL = "/n/co-je-sisal-pevne-listove-vlakno-skvrny-od-vody-a-cistenie-kobercov"
ARTICLE_RAMIE = "/n/co-je-ramia-pevne-rastlinne-vlakno-krcivost-a-pranie"

LAUNDRY_PRODUCT_NAME = "Prací gél hypoalergénny Vevo Ylang Absolute 1L"
LAUNDRY_PRODUCT_URL = "/p-1627/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l"
LAUNDRY_CATEGORY_NAME = "Pracie gély"
LAUNDRY_CATEGORY_URL = "/c/vevo-home-care/pranie/praci-gel"
CLEANING_PRODUCT_NAME = "The Pink Stuff odstraňovač škvŕn na koberce 500 ml"
CLEANING_PRODUCT_URL = "/p-649/the-pink-stuff-odstranovac-skvrn-na-koberce-500-ml"
CLEANING_CATEGORY_NAME = "Čistiace prostriedky"
CLEANING_CATEGORY_URL = "/c/vevo-home-care/upratovanie/cistiace-prostriedky"


def add_laundry_cards(article: dict[str, object], noun: str, limit: str) -> None:
    article.update(
        {
            "product_heading": f"Prací gél použite iba na prateľný {noun}",
            "product_intro": (
                "Ak ošetrovací štítok povoľuje domáce pranie a zloženie je kompatibilné, "
                "tekutý gél možno odmerať podľa tvrdosti vody, veľkosti náplne a znečistenia."
            ),
            "product_name": LAUNDRY_PRODUCT_NAME,
            "product_url": LAUNDRY_PRODUCT_URL,
            "product_text": (
                "Tekutý prostriedok sa dá dávkovať bez sypkého zvyšku. Koncentrát nelejte "
                "priamo na suchú farebnú plochu, brúsený povrch, golier ani poškodený šev."
            ),
            "product_limit": limit,
            "category_heading": "Prostriedok vyberajte podľa vlákna a celého výrobku",
            "category_intro": (
                "Názov tkaniny sám neurčuje vhodnú receptúru. Overte percentá vlákien, "
                "farbenie, povrchovú úpravu, výstuž, podšívku aj symboly ošetrovania."
            ),
            "category_name": LAUNDRY_CATEGORY_NAME,
            "category_url": LAUNDRY_CATEGORY_URL,
            "category_text": (
                "V kategórii nájdete gély pre rôzne potreby prateľnej bielizne. Vyberte "
                "kompatibilný výrobok, neprekračujte dávku a ponechajte priestor na oplach."
            ),
        }
    )


OXFORD: dict[str, object] = {
    "title": "Čo je oxfordská tkanina: košeľová väzba, golier a správne pranie",
    "link": "co-je-oxfordska-tkanina-koselova-vazba-golier-a-spravne-pranie",
    "meta": "Čo je oxfordská tkanina, ako sa líši od popelínu, chambray a panamovej väzby a ako prať, sušiť a žehliť Oxford košeľu bez poškodenia goliera.",
    "short": "Oxfordská košeľovina je zrnitá tkanina s väzbou odvodenou od košíkovej konštrukcie, často s dvojicou osnovných nití cez jeden útok. Názov neurčuje vláknové zloženie ani povolený program prania.",
    "name": "oxfordská tkanina",
    "locative": "oxfordskej košeľovine",
    "identity_heading": "Oxford opisuje väzbu a charakter košeľoviny, nie jedno vlákno",
    "identity_detail": "Pri bežnom Oxforde sa často združujú dve osnovné nite proti jednému hrubšiemu útku, čím vzniká mäkší objem a jemne zrnitý povrch odvodený od košíkovej väzby.",
    "identity_boundary": "Košeľa z čistej bavlny, bavlny s elastanom alebo syntetickej zmesi môže niesť rovnaký názov, ale bude mať inú savosť, zrážanie, návratnosť aj tepelný limit.",
    "label_focus": "presné percentá vlákien, elastan, podlep goliera a manžiet, nite, gombíky, výšivku, nekrčivú úpravu, farbenie a povolené sušenie a žehlenie",
    "missing_label": "Pri metráži si vyžiadajte technický list a pred ušitím ju skúšobne predperte; pri hotovej košeli bez etikety neurčujte teplotu iba podľa bielej farby alebo pevného omaku.",
    "dry_check": "mastný golier, zvyšok dezodorantu, odreté manžety, rozostúpené nite, zvlnenú výstuž, prasknutý gombík, svetlé lomy, vytiahnutú priadzu a lesk po žehlení",
    "damage_boundary": "Kožný maz a pigment možno čistiť, no mechanicky odretá hrana, zrazená výstuž, posunutá priadza alebo teplom vytvorený lesk nie sú odstrániteľná škvrna.",
    "test_focus": "Na farebnej košeli porovnajte prenos z líca, rubu, šva a vyšívanej časti; odlišné priadze a výstuže nemusia reagovať rovnako.",
    "combined_risk": "napučania priadzí, trenia zrnitého povrchu, rozdielneho zrážania košeľoviny a podlepenej výstuže a tlaku na golier",
    "chemistry_boundary": "Pot, maz, make-up, atrament a jedlo potrebujú odlišný prvý krok; silné lokálne bielenie môže vytvoriť svetlý oblúk výraznejší než pôvodná stopa.",
    "drying_detail": "Rozopnite manžety, narovnajte légu a nechajte vzduch preniknúť pod golier aj dvojité vrstvy; ťažkú mokrú košeľu nevešajte za jediný bod.",
    "heat_boundary": "Vysoké teplo môže zraziť bavlnu, oslabiť elastan, zvlniť lepidlo v golieri, zataviť syntetickú niť alebo sploštiť typickú zrnitú kresbu.",
    "stop_signs": "silný prenos farby, rastúca svetlá mapa, otváranie väzby, zvlnenie goliera, lepkavá úprava, praskanie potlače alebo deformácia gombíka",
    "professional_boundary": "Bežnú prateľnú Oxford košeľu možno ošetrovať doma podľa etikety, no podšité sako, hodvábna zmes, historický odev alebo kus určený na profesionálne čistenie potrebuje individuálny postup.",
    "answer": "Oxfordská tkanina je najčastejšie zrnitá košeľovina s väzbou odvodenou od košíkovej konštrukcie. Bežný Oxford často spája dve osnovné nite s jedným hrubším útkom, kým jemnejší pinpoint používa jemnejšie priadze. Názov neurčuje vláknové zloženie ani jednu teplotu. Pred praním prečítajte celý štítok, skontrolujte golier, manžety a farebnú stálosť, škvrny ošetrite lokálne bez tvrdej kefy a košeľu perte s voľným priestorom. Po cykle ju hneď vyberte, urovnajte švy, sušte v tieni a žehlite z rubu pri teplote najcitlivejšej zložky. Oxford z potiahnutého polyesteru na batohoch alebo poťahoch sa nesmie automaticky prať ako košeľa.",
    "intro": "Otázka ako prať Oxford košeľu sa často zjednoduší na radu pre bavlnu. Oxford však pomenúva najmä spôsob utkania a vzhľad povrchu; pod rovnakým označením sa predávajú košele z rôznych zmesí aj pevné syntetické materiály na tašky. Pri košeli rozhodujú priadze, farbivo, podlep goliera, výšivka a hotové spracovanie. Pri batohu navyše povlak, pena, lepidlo, zips a výstuž. Bez rozlíšenia výrobku môže správna rada pre jednu verziu poškodiť druhú.",
    "quick": [
        "<strong>Oxford nie je druh bavlny:</strong> označuje najmä charakter tkaniny a väzby.",
        "<strong>Povrch býva zrnitý:</strong> združené nite vytvárajú mäkšiu a plnšiu kresbu než hladký popelín.",
        "<strong>Golier je samostatné riziko:</strong> viac vrstiev a podlep môžu meniť rozmer inak než košeľovina.",
        "<strong>Škvrny nedrhnite kefou:</strong> exponovaná hrana sa môže vyhladiť alebo zosvetliť.",
        "<strong>Pinpoint je jemnejší variant:</strong> obchodný názov však stále nenahrádza etiketu.",
        "<strong>Taškový Oxford je iný systém:</strong> syntetická tkanina s povlakom potrebuje návod výrobcu konkrétneho predmetu.",
    ],
    "overview_heading": "Ako sa Oxford tká a prečo má typickú zrnitú kresbu",
    "overview": [
        "V tkanej látke vedie osnova pozdĺžne a útok sa vkladá priečne. Košíková väzba združuje susedné nite alebo ich účinok, preto sa väzné body javia väčšie než pri jednoduchom striedaní jedna nad jednou. CottonWorks opisuje Oxford ako konštrukciu často založenú na pomere dvoch osnovných nití proti jednému útku. Hrubší útok môže pridať objem a farebná kombinácia osnovy a útku jemný melír.",
        "Basic Oxford, pinpoint Oxford a Royal Oxford nie sú iba tri marketingové stupne jednej hrúbky. Líšia sa jemnosťou priadzí, organizáciou väzby a výsledným povrchom. Pinpoint býva jemnejší a kompaktnejší; Royal Oxford môže mať zložitejšiu výraznejšiu kresbu. Z názvu nemožno bezpečne dopočítať hustotu, pevnosť, krčivosť ani povolenie sušičky.",
        "Hotová košeľa už nie je iba kus metráže. Golier, manžety a léga obsahujú viac vrstiev, často aj lepenú alebo vloženú výstuž. Šijacia niť, gombíky a výšivka môžu mať odlišné zloženie. Preto môže látka zostať rozmerovo stabilná, kým sa golier zvlní, šev skrúti alebo sa okraj manžety mechanicky vyhladí.",
    ],
    "table1_heading": "Oxford, pinpoint, popelín, chambray a panamová väzba",
    "table1_intro": "Porovnanie pomáha rozpoznať konštrukciu, nie určiť program prania. Teplotu a mechaniku vždy nastavte podľa etikety hotového výrobku.",
    "table1_headers": ["Označenie", "Typický princíp", "Povrch", "Praktická hranica"],
    "table1_rows": [
        ("Oxford", "Často dve osnovné nite proti jednému hrubšiemu útku.", "Zrnitý, mäkší a plnší než hladká košeľovina.", "Chrániť golier, manžety, farbu a polohu priadzí."),
        ("Pinpoint Oxford", "Jemnejšie priadze podobnej veľkosti v drobnej košíkovej kresbe.", "Jemnejší a kompaktnejší bodkovaný efekt.", "Nižšia hmotnosť neznamená automaticky vyššiu teplotnú odolnosť."),
        ("Popelín", "Hustá plátnová väzba z jemných priadzí.", "Hladší a čistejší povrch.", "Viditeľné lomy, krčivosť a citlivosť povrchovej úpravy."),
        ("Chambray", "Najčastejšie plátnová väzba s farebnou a svetlou sústavou.", "Jemný krížový melír bez dominantnej zrnitosti.", "Prenos farby, výstuž košele a zrazenie."),
        ("Panamová väzba", "Širšie združené skupiny osnovy aj útku.", "Výraznejšie košíky a voľnejšia kresba.", "Vyššie riziko posunu priadzí a zachytenia."),
    ],
    "sections": [
        {
            "heading": "Ako rozoznať Oxford košeľovinu doma bez poškodenia",
            "paragraphs": [
                f"Položte látku na svetlý podklad a prezrite ju lupou z líca aj rubu. Hľadajte združené osnovné body a priečny útok, nie diagonálne rebro typické pre keper. Porovnanie s <a href=\"{ARTICLE_POPLIN}\">hladkým popelínom</a>, <a href=\"{ARTICLE_CHAMBRAY}\">farebne melírovaným chambray</a> a <a href=\"{ARTICLE_PANAMA}\">výraznejšou panamovou väzbou</a> pomôže pomenovať rozdiel bez vyťahovania nití.",
                "Dotyk a fotografia nestačia na určenie zloženia. Bavlnená a polyesterová zmes môžu mať podobnú kresbu, no inú savosť a reakciu na teplo. Nepárajte šev ani nerobte skúšku horením na hotovom odeve. Percentá čítajte na etikete; pri metráži žiadajte technický list a vzorku predperte za podmienok plánovaných pre hotový výrobok.",
            ],
        },
        {
            "heading": "Golier a manžety: prečo sa čistia inak než plocha košele",
            "paragraphs": [
                "Golier zachytáva kožný maz, pot, parfum, make-up aj vlasové produkty. Nečistota preniká do zrnitého povrchu a pri okraji sa spája s mechanickým trením o pokožku. Najprv odsajte voľný prebytok, naneste iba kompatibilné množstvo prípravku a nechajte ho pôsobiť podľa návodu. Tvrdá kefa môže vyhladiť vrcholy priadzí a vytvoriť trvalý svetlejší oblúk.",
                f"Manžety skontrolujte aj zvnútra, pri gombíku a na zalomení. Ak ide o neurčitú stopu, pomôže postup podľa pôvodu škvrny z <a href=\"{ARTICLE_STAIN}\">sprievodcu odstraňovaním škvŕn</a>. Koncentrát nelejte priamo na suchú košeľu a lokálne miesto nepreťažujte produktom; zvyšok uzavretý v dvojitej vrstve sa môže horšie opláchnuť a po vyschnutí stuhnúť.",
            ],
        },
        {
            "heading": "Ako prať Oxford košeľu krok za krokom",
            "paragraphs": [
                f"Zapnite voľné háčiky, vyberte obsah vreciek, rozopnite manžety a podľa konštrukcie otočte košeľu naruby. Triedenie prispôsobte farbe a uvoľňovaniu farbiva. Symboly vyhodnoťte podľa <a href=\"{ARTICLE_LABEL}\">návodu na čítanie ošetrovacieho štítka</a>. Zvoľte cyklus, teplotu a odstreďovanie povolené pre celý odev, nie najodolnejšie vlákno v zmesi.",
                "Bubon neprepĺňajte. Košeľa potrebuje priestor na rovnomerné navlhčenie, pohyb a oplach; stlačená náplň vytvára ostré lomy a drží produkt pri švoch. Dávku prispôsobte tvrdosti vody a skutočnej hmotnosti náplne. Dodatočný oplach má zmysel pri preukázanom zvyšku, nie ako náhrada správnej dávky a primeraného naplnenia.",
            ],
        },
        {
            "heading": "Farebný Oxford, biele košele a bezpečné riešenie máp",
            "paragraphs": [
                f"Pri tmavom alebo kontrastnom Oxforde najprv overte prenos farby. Mechanizmy blednutia pri praní, svetle a trení vysvetľuje <a href=\"{ARTICLE_COLOR}\">sprievodca stálofarebnosťou</a>. Optický zjasňovač, bielidlo alebo silný odmasťovač nepoužívajte automaticky iba preto, že košeľa pôsobí sivšie; zmena môže byť v povrchu, farbive alebo v usadenine.",
                "Na bielej košeli rozlíšte všeobecné zašednutie od lokálneho žltého goliera a od poškodenej výstuže. Zvoľte prostriedok povolený výrobcom a vykonajte skúšku na skrytom leme. Nemiešajte chlórové bielidlo s inými čističmi. Ak mapa ostane aj po oplachu a mení sa s uhlom svetla, môže ísť o vyhladenie povrchu, nie o nečistotu.",
            ],
        },
        {
            "heading": "Sušenie bez skrútených švov a zvlneného goliera",
            "paragraphs": [
                f"Košeľu vyberte hneď po skončení cyklu, jemne ju pretrepte a urovnajte bočné švy bez násilného ťahania. Golier a légu vytvarujte do prirodzenej polohy, no mokrú lepenú výstuž neprelamujte. Pri sušení v interiéri zabezpečte prúdenie vzduchu podľa zásad v článku <a href=\"{ARTICLE_DRYING}\">ako sušiť bielizeň bez zatuchnutia</a>.",
                f"Sušičku použite iba pri výslovnom symbole a zvoľte povolené nastavenie. Teplo môže zvýšiť rozdiel medzi košeľovinou, niťou a výstužou; súvislosti opisuje článok <a href=\"{ARTICLE_SHRINKAGE}\">prečo sa oblečenie po praní zráža</a>. Skrútený šev hodnoťte až po úplnom vysušení a nenarovnávajte ho prudkým napínaním za mokra.",
            ],
        },
        {
            "heading": "Ako žehliť Oxford košeľu a nezničiť zrnitý povrch",
            "paragraphs": [
                f"Začnite na skrytej časti pri teplote najcitlivejšieho vlákna. Košeľu žehlite mierne vlhkú alebo použite primeranú paru, ak ju etiketa povoľuje. Podrobnú postupnosť goliera, manžiet, rukávov a trupu nájdete v návode <a href=\"{ARTICLE_IRONING}\">ako vyžehliť košeľu</a>. Na tmavom kuse pracujte z rubu alebo cez čistú ochrannú tkaninu.",
                "Silný tlak nemusí zrnitú košeľovinu zlepšiť. Môže sploštiť vystupujúce body, vytvoriť lesk a pretlačiť švové rezervy na líc. Golier žehlite od špičiek smerom k stredu, aby sa prebytočný materiál nehromadil na rohoch. Ak sa podlep zvlnil alebo oddeľuje, ďalšia para a tlak môžu poruchu zafixovať.",
            ],
        },
        {
            "heading": "Keď sa priadza posunie, povrch zatrhne alebo okraj vyhladí",
            "paragraphs": [
                f"Združené nite môžu pri bodovom zaťažení zmeniť polohu. Otvor pri šve, drobný posun a vytiahnutá slučka nie sú to isté ako diera z pretrhnutia. <a href=\"{ARTICLE_SNAGGING}\">Sprievodca zatrhávaním textilu</a> vysvetľuje, prečo sa voľný koniec nestrihá naslepo. Najprv odstráňte zdroj zachytávania, napríklad ostrý zips alebo poškodený bubon.",
                "Lesklý okraj goliera môže vznikať kombináciou telesného mazu a mechanického vyhladenia. Po šetrnom odmastení posúďte povrch pri bočnom svetle. Ak lesk ostáva bez hmatateľného zvyšku, ďalšie drhnutie nepomôže. Oprava šva alebo odborné prepodloženie goliera je vtedy rozumnejšie než opakované agresívne pranie.",
            ],
        },
        {
            "heading": "Oxford na batohoch, poťahoch a outdoorových doplnkoch nie je košeľovina",
            "paragraphs": [
                "V názvoch tašiek sa Oxford často používa pre pevnú polyesterovú alebo nylonovú tkaninu s označením hustoty, napríklad 600D, a s polyuretánovým či PVC povlakom. Číslo nehovorí, že predmet smie ísť do práčky. O správaní rozhoduje aj vodoodpudivá úprava, penová výstuž, lepidlo, podšívka, popruhy a kovanie.",
                "Taký predmet čistite iba podľa návodu výrobcu. Najprv odstráňte suchú nečistotu, vykonajte skrytú skúšku a obmedzte množstvo vody. Ponorenie môže oddeliť povlak, preniesť farbu alebo zadržať vlhkosť v pene. Rada pre bavlnenú košeľu sa na potiahnutý batoh nevzťahuje ani vtedy, keď obe etikety obsahujú slovo Oxford.",
            ],
        },
        {
            "heading": "Ako Oxford košeľu skladovať a predĺžiť životnosť exponovaných miest",
            "paragraphs": [
                "Košeľu ukladajte čistú a úplne suchú. Zvyšok potu a produktu na golieri pri dlhom skladovaní oxiduje, žltne a priťahuje nečistotu. Vhodne široký vešiak podoprie ramená; pri skladaní nevytvárajte zakaždým ostrý lom na rovnakom mieste. Tmavé kusy chráňte pred priamym svetlom.",
                "Životnosť nezvyšuje maximálna intenzita každého cyklu. Menej preplnený bubon, presná dávka, rýchle vybratie, prirodzené urovnanie a včasná oprava gombíka znižujú opakované trenie. Košele striedajte, aby golier a manžety medzi noseniami úplne vyschli a aby jeden kus neprechádzal zbytočne častými núdzovými zásahmi.",
            ],
        },
    ],
    "table2_heading": "Problém na Oxford košeli: diagnóza pred ďalším praním",
    "table2_intro": "Rovnaký vzhľad môže mať viac príčin. Najprv rozlíšte nečistotu, zvyšok produktu a mechanickú alebo tepelnú zmenu.",
    "table2_headers": ["Pozorovanie", "Pravdepodobná príčina", "Bezpečný prvý krok", "Kedy zastaviť"],
    "table2_rows": [
        ("Mastný tmavší golier", "Kožný maz a kozmetika v kontaktnej zóne.", "Odsatie, kompatibilný lokálny postup a nízke trenie.", "Farba sa prenáša alebo okraj bledne."),
        ("Biela tvrdá mapa", "Nadmerná dávka, slabý oplach alebo lokálny koncentrát.", "Po etikete primeraný oplach bez ďalšieho produktu.", "Miesto je lepkavé, odfarbené alebo deformované."),
        ("Lesklý svetlý oblúk", "Vyhladenie trením, kefou alebo horúcou žehličkou.", "Posúdiť po odmastení pri bočnom svetle.", "Bez zvyšku ostáva zmena povrchu."),
        ("Zvlnený golier", "Rozdielne zrazenie alebo oddelenie podlepenej výstuže.", "Vysušiť v prirodzenom tvare a zdokumentovať.", "Vznikajú bubliny alebo praská spoj."),
        ("Otvor pri šve", "Posun priadzí, tesný steh alebo mechanické napätie.", "Prestať nosiť a skontrolovať šev z rubu.", "Otvor sa zväčšuje alebo je priadza pretrhnutá."),
    ],
    "steps_heading": "Bezpečný pracovný postup pre Oxford košeľu",
    "steps": [
        "Prečítajte vláknové zloženie a všetky symboly celého odevu.",
        "Skontrolujte golier, manžety, gombíky, výstuž, výšivku a poškodené švy.",
        "Určte pôvod škvŕn a na skrytom mieste overte farbu aj prípravok.",
        "Rozopnite manžety, vyprázdnite vrecká a košeľu pripravte podľa etikety.",
        "Triedte podľa farby a nepridávajte predmety s ostrými zipsami alebo suchým zipsom.",
        "Odmerajte kompatibilný prostriedok podľa vody, náplne a znečistenia.",
        "Zvoľte povolený cyklus, teplotu a odstreďovanie a bubon neprepĺňajte.",
        "Po cykle košeľu hneď vyberte, narovnajte švy a vytvarujte golier.",
        "Sušte s prúdením vzduchu mimo priameho prudkého tepla.",
        "Žehlite podľa etikety z rubu alebo cez ochrannú tkaninu a skladujte až úplne suchú.",
    ],
    "remember": [
        "Oxford je väzba alebo obchodné označenie tkaniny, nie automaticky stopercentná bavlna.",
        "Golier, manžety a léga majú viac vrstiev a môžu meniť rozmer odlišne.",
        "Farebnú stálosť, lokálny prostriedok a teplotu overte pred zásahom.",
        "Košeľa potrebuje priestor na pohyb aj oplach.",
        "Potiahnutý Oxford na batohu sa neošetruje podľa návodu na košeľu.",
    ],
    "mistakes": [
        "Predpokladať bavlnu a vysokú teplotu iba podľa slova Oxford.",
        "Drhnúť golier tvrdou kefou, kým sa zrnitý povrch vyhladí.",
        "Naliať koncentrovaný gél priamo na suchú farebnú manžetu.",
        "Nechať mokrú košeľu hodiny stlačenú v bubne a potom lomy prehrievať.",
        "Vyprať potiahnutý batoh v práčke bez výslovného povolenia výrobcu.",
    ],
    "expert_heading": "Odbornejší pohľad: košíková odvodenina, skúšky rozmeru a šva",
    "expert": [
        "CottonWorks opisuje Oxford medzi základnými tkanými dizajnmi ako košeľovinu často vytvorenú pomerom dvoch osnovných nití k jednému útku. Základný variant môže kombinovať jemnejšiu osnovu s hrubším útkom, zatiaľ čo pinpoint používa jemnejšie priadze podobnej veľkosti. Táto konštrukcia vysvetľuje povrch, ale neurčuje chemickú úpravu ani správanie hotového goliera.",
        "AATCC TM135 hodnotí rozmerové zmeny textílií po definovaných domácich postupoch a ASTM D1683 správanie šitých švov. Ide o samostatné vlastnosti. Košeľovina môže mať prijateľný rozmer, no šev sa môže otvárať pre hustotu stehu, rezervu alebo polohu priadzí; podlep môže zlyhať bez pretrhnutia základnej látky.",
        "AATCC TM61 posudzuje stálofarebnosť za presne opísaných podmienok. Výsledok nemožno zameniť za všeobecné povolenie ľubovoľného prostriedku alebo teploty. Pre používateľa je najdôležitejšia etiketa konkrétnej košele, návod výrobcu a pozorovanie zmeny po prvých cykloch, nie samotný názov väzby.",
    ],
    "source_intro": "Zdroje podporujú opis Oxfordu ako košíkovej odvodenej košeľoviny a význam rozmeru, stálofarebnosti a šva. Nepodporujú jednu teplotu ani jeden postup pre všetky výrobky označené Oxford.",
    "sources": [
        ("CottonWorks: základné tkané dizajny vrátane Oxfordu", COTTONWORKS_WEAVES),
        ("CottonWorks: princípy tkania a väzby", COTTONWORKS_WEAVING),
        ("AATCC TM135: rozmerové zmeny po domácich postupoch", AATCC_DIMENSION),
        ("AATCC TM61: stálofarebnosť pri praní", AATCC_COLOR),
        ("ASTM D1683: správanie šitých švov", ASTM_SEAM),
        ("EÚ 1007/2011: označovanie textilných vlákien", EU_FIBRE_LABEL),
        ("GINETEX: význam symbolov ošetrovania", GINETEX),
    ],
    "related": [
        ("Čo je popelín", ARTICLE_POPLIN),
        ("Čo je chambray", ARTICLE_CHAMBRAY),
        ("Čo je panamová väzba", ARTICLE_PANAMA),
        ("Ako čítať štítok na oblečení", ARTICLE_LABEL),
        ("Prečo farby blednú", ARTICLE_COLOR),
        ("Ako vyžehliť košeľu", ARTICLE_IRONING),
    ],
    "faq_title": "Oxford košeľovina, košele a potiahnuté tkaniny",
    "faq": [
        ("Čo je oxfordská tkanina?", "Tkanina so zrnitou košíkovou odvodenou väzbou, často používaná na košele; názov neurčuje jedno vlákno."),
        ("Je Oxford vždy bavlna?", "Nie. Môže byť bavlnený, zmesový alebo syntetický, preto treba čítať zloženie."),
        ("Aký je rozdiel medzi Oxfordom a popelínom?", "Oxford býva zrnitejší a objemnejší, popelín hladší a kompaktnejší v plátnovej väzbe."),
        ("Čo je pinpoint Oxford?", "Jemnejší variant s drobnou košíkovou kresbou a jemnejšími priadzami; stále potrebuje vlastnú etiketu."),
        ("Na koľko stupňov prať Oxford košeľu?", "Jedna teplota neexistuje. Rozhoduje zloženie, farbenie, výstuž a symbol celého odevu."),
        ("Ako vyčistiť mastný golier?", "Prebytok odsajte, urobte skrytú skúšku a použite kompatibilný lokálny postup bez tvrdej kefy."),
        ("Môže ísť Oxford košeľa do sušičky?", "Iba pri výslovnom symbole; teplo môže zmeniť rozmer košeľoviny aj výstuže."),
        ("Prečo sa golier po praní zvlnil?", "Môže ísť o rozdielne zrazenie vrstiev alebo poruchu lepeného podlepu, nie o nedostatok žehlenia."),
        ("Ako žehliť Oxford?", "Podľa najcitlivejšej zložky, ideálne z rubu alebo cez ochrannú tkaninu a bez nadmerného tlaku."),
        ("Púšťa farebný Oxford farbu?", "Môže, najmä pri tmavom alebo špeciálne farbenom kuse. Overte pokyny a perte s podobnými farbami."),
        ("Je Oxford 600D vhodný do práčky?", "Číslo ani názov pranie nepovoľujú. Rozhoduje povlak, výstuž a návod výrobcu celého predmetu."),
        ("Ako odstrániť lesk z goliera?", "Najprv odstráňte maz. Ak ostáva hladký lesk bez zvyšku, ide skôr o mechanickú zmenu povrchu."),
        ("Ako Oxford košeľu skladovať?", "Čistú, úplne suchú, chránenú pred svetlom a na vešiaku alebo voľne zloženú bez ostrého dlhodobého lomu."),
    ],
}

add_laundry_cards(
    OXFORD,
    "Oxford odev",
    "Gél nie je automaticky vhodný na hodvábnu alebo vlnenú zmes, nestálofarebný kus, lepené sako ani potiahnutý batoh. Neopraví odretý golier, posunutú priadzu ani zvlnenú výstuž.",
)


SYPKOVINA: dict[str, object] = {
    "title": "Čo je sypkovina: prečo uniká perie, kontrola švov a bezpečné čistenie",
    "link": "co-je-sypkovina-preco-unika-perie-kontrola-svov-a-bezpecne-cistenie",
    "meta": "Čo je sypkovina, prečo z vankúša uniká perie a ako rozlíšiť problém tkaniny, šva a výplne pred praním, opravou alebo výmenou obalu.",
    "short": "Sypkovina je husto konštruovaná tkanina určená na obmedzenie prenikania páperia a pier cez obal. O jej funkcii nerozhoduje iba počet nití, ale aj priadza, väzba, úprava, šev a stav celého výrobku.",
    "name": "sypkovina",
    "locative": "sypkovine",
    "identity_heading": "Sypkovina je funkčný obal výplne, nie bežná obliečka",
    "identity_detail": "Jej úlohou je prepúšťať primerané množstvo vzduchu a súčasne obmedziť prenikanie jemného páperia, ostrých brkov a vláken cez plochu a švy.",
    "identity_boundary": "Hustá bavlnená tkanina môže vyzerať ako perkál, no funkciu sypkoviny neurčuje samotný počet nití; rozhoduje kombinácia priadze, väzby, dokončenia, priedušnosti, šitia a typu výplne.",
    "label_focus": "zloženie obalu aj výplne, povolenie prania celého vankúša alebo paplóna, rozmery bubna, švové spracovanie, sušenie, profesionálnu údržbu a prípadný zákaz poškodeného výrobku prať",
    "missing_label": "Starý perový výrobok bez údajov najprv skontrolujte proti svetlu a pri švoch; ak tkanina práši, praská alebo prepúšťa veľa brkov, bezpečnejšia je odborná výmena obalu než skúšobné pranie celého kusa.",
    "dry_check": "drobné brká, chumáče páperia, prachový povlak, otvorené stehy, pretrhnuté rohy, mastné mapy, žlté plochy, stvrdnutú tkaninu, rednúce miesta a zápach z vnútra",
    "damage_boundary": "Povrchovú škvrnu možno ošetriť, no rednúca tkanina, prerezanie brkom, otvorený šev a degradovaná výplň sa praním neopravia; voda môže slabé miesto zväčšiť.",
    "test_focus": "Sledujte nielen farbu, ale aj vznik tmavého okraja, tvrdnutie povrchovej úpravy a prechod vlhkosti k výplni; malá škvrna na obale neospravedlňuje premočenie celého vankúša.",
    "combined_risk": "napučania obalovej tkaniny, migrácie jemnej výplne, mechanického namáhania mokrého šva a veľmi pomalého vysychania zle rozloženej náplne",
    "chemistry_boundary": "Prostriedok musí byť kompatibilný s obalom aj výplňou a úplne sa vypláchnuť; aviváž, nadmerná dávka alebo lokálny koncentrát môžu zmeniť omak, priedušnosť a zhlukovanie.",
    "drying_detail": "Výplň priebežne rozdeľujte iba spôsobom povoleným výrobcom a kontrolujte rohy, švy aj stred; povrch suchý na dotyk neznamená suché páperie v jadre.",
    "heat_boundary": "Príliš vysoká teplota môže zraziť obal, poškodiť úpravu, oslabiť šev alebo prehriať výplň, zatiaľ čo príliš nízke a pomalé sušenie podporí zatuchnutie.",
    "stop_signs": "rastúci otvor, masívny únik výplne, praskanie tkaniny, silný prenos farby, tmavnutie bez vysychania, zatuchnutý pach alebo studené vlhké jadro aj po dlhom vetraní",
    "professional_boundary": "Odborná renovácia má zmysel pri starej alebo hodnotnej perovej výplni: prevádzka ju môže oddeliť od poškodeného obalu, vyčistiť podľa svojho procesu a naplniť do novej vhodnej sypkoviny.",
    "answer": "Sypkovina je hustá tkanina tvoriaca vnútorný obal perového alebo páperového vankúša, paplóna a inej výplne. Má obmedziť únik jemných častíc aj ostrých brkov, no zároveň nesmie byť posudzovaná iba podľa vysokého počtu nití. Ak z výrobku vychádza perie, najprv zistite, či uniká cez šev, bodové poškodenie alebo celú rednúcu plochu. Otvorený šev opravte pred praním; oslabený obal a degradovanú výplň dajte posúdiť na renováciu. Celý výrobok perte len pri výslovnom povolení, v dostatočne veľkom bubne a s plánom úplného vysušenia. Lokálnu škvrnu na zdravom obale riešte s minimom vlhkosti, aby ste zbytočne nepremočili výplň.",
    "intro": "Keď z vankúša vychádza perie, bežná rada znie kúpiť hustejšiu obliečku alebo výrobok vyprať. Ani jedno nemusí riešiť príčinu. Vonkajšia obliečka nie je funkčná sypkovina a pranie nezacelí poškodený šev, dieru po brku ani rednúcu priadzu. Navyše mokrá perová výplň výrazne oťažie a potrebuje dôkladné, rovnomerné sušenie. Správny postup preto začína diagnostikou úniku, posúdením pevnosti obalu a až potom rozhodnutím medzi lokálnym čistením, praním celého výrobku, opravou šva alebo výmenou sypkoviny.",
    "quick": [
        "<strong>Nie je to obliečka:</strong> sypkovina uzatvára výplň a je súčasťou funkcie vankúša alebo paplóna.",
        "<strong>Únik hľadajte pri šve:</strong> podľa IDFL býva šitie častým zdrojom problémov, až potom plocha tkaniny.",
        "<strong>Počet nití nestačí:</strong> o odolnosti proti prenikaniu výplne rozhoduje viac parametrov naraz.",
        "<strong>Brko môže vytvoriť bodový otvor:</strong> nevytiahnite ho prudko cez líc, aby sa diera nezväčšila.",
        "<strong>Poškodený kus neperte naslepo:</strong> mokrá hmotnosť zaťaží oslabené švy a tkaninu.",
        "<strong>Sušenie musí zasiahnuť jadro:</strong> suchý povrch môže skrývať vlhké zhluky vo vnútri.",
    ],
    "overview_heading": "Ako sypkovina zadržiava perie a pritom prepúšťa vzduch",
    "overview": [
        "Sypkovina vytvára bariéru pomocou tesne usporiadaných priadzí, vhodnej väzby a dokončenia povrchu. Medzery musia byť natoľko kontrolované, aby jemné páperie a konce pier neprenikali pri bežnom stláčaní. Úplne nepriedušná fólia by však zmenila komfort a správanie výplne, preto sa hodnotí aj priepustnosť vzduchu. Rovnováha medzi bariérou a priedušnosťou je vlastnosť konkrétneho systému.",
        "IDFL pri problémoch hotových výrobkov oddeľuje únik cez šitie od úniku cez tkaninu. Ihla vytvorí otvory, niť a hustota stehu ovplyvnia ich uzavretie a okraj musí mať dostatočnú rezervu. Aj kvalitná plocha môže zlyhať pri nevhodnom šve. Naopak jednotlivé brká na povrchu nemusia dokazovať plošné zlyhanie, ak prenikli cez konkrétny bod alebo steh.",
        "Počet nití je iba počet osnovných a útkových priadzí na definovanú dĺžku. Nehovorí sám o priemere priadze, zákrute, väzbe, kalandrovaní, pevnosti ani o veľkosti a tvare výplne. Technická analýza IDFL ukazuje, že vyšší počet nití automaticky nezaručuje lepšiu odolnosť proti prenikaniu páperia. Pri nákupe preto žiadajte údaje o hotovom výrobku, nie iba jedno veľké číslo.",
    ],
    "table1_heading": "Sypkovina, obliečka, perkál a ochranný poťah",
    "table1_intro": "Jednotlivé vrstvy majú rozdielnu úlohu. Zámena vrstiev vedie k nesprávnej diagnóze úniku aj k nevhodnému čisteniu.",
    "table1_headers": ["Vrstva alebo pojem", "Hlavná úloha", "Kontakt s výplňou", "Dôležité pri údržbe"],
    "table1_rows": [
        ("Sypkovina", "Zadržať páperie a perie v jadre výrobku.", "Priamy a trvalý.", "Stav plochy, švy, priedušnosť, poškodenie a úplné sušenie."),
        ("Vymeniteľná obliečka", "Chrániť vankúš pred potom a bežným znečistením.", "Nepriamy.", "Perie cez poškodenú sypkovinu nezastaví spoľahlivo."),
        ("Perkál", "Hustá plátnová tkanina často používaná na posteľnú bielizeň.", "Závisí od výrobku.", "Názov ani počet nití automaticky nepotvrdzuje funkciu sypkoviny."),
        ("Ochranný poťah", "Obmedziť prenikanie nečistôt alebo alergénov podľa konštrukcie.", "Obvykle cez pôvodný vankúš.", "Zips, membrána a vlastný návod môžu meniť priedušnosť a pranie."),
        ("Dekoračný povlak", "Vzhľad a kontakt s interiérom.", "Spravidla nepriamy.", "Nenahrádza opravu vnútorného obalu."),
    ],
    "sections": [
        {
            "heading": "Ako zistiť, kadiaľ perie skutočne uniká",
            "paragraphs": [
                "Výrobok položte na čistú tmavú plochu, jemne ho stlačte dlaňou a sledujte švy, rohy, prešívanie a plochu. Nevyvolávajte extrémny tlak. Označte miesto, kde sa objaví nový chumáč, a prezrite ho lupou. Jeden otvor pri stehu vyžaduje inú opravu než rovnomerné prášenie celej plochy.",
                "Ostré brko môže smerovať von. Neťahajte ho silou, pretože hrubší koniec zväčší otvor. Ak to konštrukcia umožňuje, jemne ho zatlačte späť cez obal a miesto sledujte. Pri opakovaných bodoch, rednúcej látke alebo praskajúcom rohu prestaňte s domácimi skúškami; obal už nemusí bezpečne niesť mokrú hmotnosť.",
            ],
        },
        {
            "heading": "Šev, ihlový otvor a plošná priedušnosť nie sú ten istý problém",
            "paragraphs": [
                "Pri šve vznikajú otvory zámerne vpichom ihly. Ich výsledné správanie závisí od hrúbky ihly, nite, hustoty stehu, napätia a dokončenia. Príliš hustý steh môže perforovať oslabenú tkaninu; príliš riedky nechá väčšie úseky bez opory. Dočasné prelepenie zvonka môže zmeniť omak a skomplikovať neskoršiu profesionálnu opravu.",
                "Plošná priedušnosť sa skúša ako prechod vzduchu cez definovanú plochu pri stanovenom rozdiele tlaku. Nehovorí priamo, koľko brkov prejde cez konkrétny šev počas rokov používania. Preto sa pri reklamácii oplatí zdokumentovať miesto úniku, počet uvoľnených častíc, stav stehu a podmienky používania namiesto všeobecného tvrdenia, že látka dýcha príliš veľa.",
            ],
        },
        {
            "heading": "Kedy stačí lokálne čistenie sypkoviny",
            "paragraphs": [
                f"Malú čerstvú škvrnu na pevnom nepoškodenom obale najprv odsajte bez trenia. Pôvod škvrny určite podľa postupu v <a href=\"{ARTICLE_STAIN}\">sprievodcovi čistením rôznych škvŕn</a>. Použite minimálne množstvo kompatibilného roztoku, pracujte od okraja ku stredu a nedovoľte, aby tekutina zbytočne prenikla k výplni.",
                "Pod čistené miesto vložte savú bielu vrstvu iba vtedy, ak sa k rubu bezpečne dostanete bez otvorenia sypkoviny. Inak vlhkosť odsávajte z povrchu a sušte s prúdením vzduchu. Veľká mastná mapa, moč, biologické znečistenie alebo pach z jadra nie sú vhodné na povrchové maskovanie; treba posúdiť celý výrobok a hygienickú hranicu.",
            ],
        },
        {
            "heading": "Ako vyprať perový vankúš bez poškodenia sypkoviny",
            "paragraphs": [
                f"Celý výrobok perte iba vtedy, keď to dovoľuje etiketa a obal nemá otvor, rednúce miesto ani oslabený šev. Skontrolujte, či má bubon dostatočný objem a či výrobok po nasiaknutí nebude preťažovať práčku. Konkrétny postup rozoberá článok <a href=\"{ARTICLE_FEATHER_PILLOW}\">ako vyprať páperový vankúš</a>; pri paplóne použite <a href=\"{ARTICLE_DUVET}\">návod pre prikrývky a veľkosť bubna</a>.",
                "Použite iba množstvo prostriedku a cyklus povolené výrobcom. Veľmi penivá alebo nadmerná dávka sa z hustého obalu a výplne ťažšie odstraňuje. Kus v bubne rozložte rovnomerne a po cykle skontrolujte švy ešte pred sušením. Ak sa objavil otvor, nepokračujte v prudkom prevaľovaní, pri ktorom by sa výplň uvoľnila do spotrebiča.",
            ],
        },
        {
            "heading": "Prečo perová výplň po praní zapácha alebo tvorí hrudky",
            "paragraphs": [
                f"Páperie a perie sa po navlhčení zhlukujú. Kým medzi nimi ostáva voda, výrobok môže pôsobiť ťažký, studený a mať výraznejší prirodzený pach. Zatuchnutie však signalizuje nedostatočné vysušenie alebo predchádzajúce znečistenie. Všeobecné príčiny pachu po praní vysvetľuje článok <a href=\"{ARTICLE_ODOR}\">prečo textil zapácha po praní</a>.",
                "Sušte presne podľa etikety a pravidelne kontrolujte stred, rohy a komory. Povolené šetrné mechanické rozdeľovanie môže pomôcť obnoviť objem, no nesmie nahrádzať čas a prúdenie vzduchu. Výrobok neobliekajte do nepriedušného poťahu ani neuložte do skrine, kým je vnútro chladnejšie, zhluknuté alebo po krátkom uzavretí znovu zapácha.",
            ],
        },
        {
            "heading": "Kedy sypkovinu opraviť a kedy ju radšej vymeniť",
            "paragraphs": [
                "Malý mechanický problém na zdravom šve môže opraviť skúsený krajčír vhodnou niťou a stehom bez ďalšieho perforovania. Záplata musí zniesť tlak, trenie a budúcu údržbu. Lepiaca páska na mäkkej ploche môže vytvoriť tuhý okraj, zachytávať nečistotu a po praní sa oddeliť, preto nie je univerzálnym dlhodobým riešením.",
                "Ak sa tkanina trhá pri miernom pohybe, je plošne redšia, púšťa prach alebo má viac opráv, výmena celého obalu býva bezpečnejšia. Špecializovaná renovácia môže vyprázdniť výplň kontrolovaným spôsobom a oddeliť použiteľný materiál od nečistôt. Domáce páranie plného vankúša rozptýli jemné častice a sťažuje hygienické aj množstevné posúdenie.",
            ],
        },
        {
            "heading": "Ako vyberať nový perový výrobok bez slepej viery v počet nití",
            "paragraphs": [
                f"Pýtajte sa na zloženie obalu a výplne, pôvod a klasifikáciu výplne, spôsob členenia komôr, údržbu a dostupnosť opravárenského servisu. <a href=\"{ARTICLE_PERCALE}\">Počet nití pri hustom perkále</a> opisuje inú spotrebiteľskú otázku než odolnosť proti prenikaniu páperia. Vysoké číslo bez údajov o priadzi, dokončení a šve nie je úplná informácia.",
                "Prezrite rovnomernosť švov, rohy a miesta, kde sa komory spájajú. Nový výrobok môže uvoľniť ojedinelé vlákno zachytené pri výrobe, no opakovaný únik z jedného bodu zdokumentujte. Dodržujte ochrannú obliečku a pravidelné vetranie, aby sa sypkovina menej znečisťovala a celé jadro nemuselo prechádzať zbytočne častým mokrým procesom.",
            ],
        },
        {
            "heading": "Sypkovina pri paplóne, vankúši a komorovom výrobku",
            "paragraphs": [
                "Vankúš má veľkú súvislú plochu a vysoké bodové stláčanie hlavou. Paplón býva rozdelený prešívaním alebo prepážkami, ktoré bránia presunu výplne. Komorové švy pridávajú ďalšie možné miesta úniku a pri praní ovplyvňujú rozloženie mokrej hmotnosti. Rovnaká tkanina sa preto v dvoch výrobkoch nezaťažuje rovnako.",
                f"Pred zásahom si overte, či problém patrí obalu, výplni alebo celému výrobku. Porovnanie rôznych jadier nájdete v článku <a href=\"{ARTICLE_PILLOWS}\">ako prať vankúše podľa typu výplne</a> a rastlinnú ľahkú výplň rozoberá samostatný text <a href=\"{ARTICLE_KAPOK}\">čo je kapok</a>. Rada pre perie sa nesmie automaticky preniesť na penu, duté vlákno alebo kapok.",
            ],
        },
        {
            "heading": "Hygiena, alergény a hranica domáceho zásahu",
            "paragraphs": [
                "Prach na povrchu nemusí pochádzať iba z rozpadnutej výplne; môže ísť o bežný domáci prach, kožné častice alebo zvyšok povrchového produktu. Bez analýzy nemožno z vzhľadu určiť alergén. Pravidelná výmena prateľnej obliečky, vetranie a ochrana pred vlhkosťou znižujú znečistenie bez častého namáčania jadra.",
                "Pri astme, silnej alergii, plesni, fekálnom alebo rozsiahle biologickom znečistení má zdravotná bezpečnosť prednosť pred zachovaním starej výplne. Domáci parfum ani povrchový sprej problém neodstránia. Výrobok izolujte bez rozptyľovania prachu a konzultujte primeranú profesionálnu dekontamináciu alebo bezpečné vyradenie.",
            ],
        },
    ],
    "table2_heading": "Prečo zo sypkoviny uniká výplň",
    "table2_intro": "Miesto a spôsob úniku pomáhajú rozhodnúť, či stačí oprava, treba nový obal alebo je problém v celej výplni.",
    "table2_headers": ["Prejav", "Možná príčina", "Prvý krok", "Nevhodná skratka"],
    "table2_rows": [
        ("Perie pri jednom stehu", "Ihlový otvor, uvoľnená niť alebo lokálne napätie.", "Označiť miesto a dať skontrolovať šev.", "Zaliať šev neznámym lepidlom."),
        ("Brko z jedného bodu", "Ostrý koniec prepichol alebo roztiahol medzeru.", "Jemne zatlačiť späť a sledovať otvor.", "Prudko vytiahnuť hrubý koniec cez líc."),
        ("Jemné páperie po celej ploche", "Nevhodná alebo degradovaná bariéra, prípadne extrémny tlak.", "Posúdiť tkaninu a podmienky používania.", "Pridať iba ďalšiu dekoratívnu obliečku."),
        ("Prach a praskanie látky", "Starnutie priadze, chemická alebo svetelná degradácia.", "Prestať používať a riešiť renováciu.", "Vyprať na intenzívnom programe."),
        ("Únik po praní", "Oslabený šev, zrazenie alebo mechanické zaťaženie mokrého kusa.", "Zastaviť sušenie s pohybom a bezpečne zachytiť výplň.", "Pokračovať v prudkom prevaľovaní."),
    ],
    "steps_heading": "Rozhodovací postup pri sypkovine a perovej výplni",
    "steps": [
        "Odstráňte vonkajšiu obliečku a výrobok položte na čistú kontrastnú plochu.",
        "Prezrite švy, rohy, prešívanie a plochu bez extrémneho stláčania.",
        "Označte presné miesto úniku a odlíšte jednotlivé brko od plošného prášenia.",
        "Prečítajte zloženie obalu, typ výplne a všetky symboly ošetrovania.",
        "Poškodený šev alebo otvor opravte pred akýmkoľvek praním.",
        "Pri lokálnej škvrne vykonajte skrytú skúšku a použite minimum vlhkosti.",
        "Celý kus perte iba pri výslovnom povolení a v dostatočne veľkom bubne.",
        "Použite primeranú dávku kompatibilného prostriedku a dôkladný povolený oplach.",
        "Sušte podľa etikety, priebežne kontrolujte rozloženie a vlhkosť jadra.",
        "Pri rednúcej tkanine, zápachu alebo opakovanom úniku zvoľte odbornú renováciu.",
    ],
    "remember": [
        "Sypkovina je vnútorný funkčný obal výplne, nie vymeniteľná obliečka.",
        "Najprv skontrolujte švy a bodové poškodenia, až potom celú plochu.",
        "Vyšší počet nití sám nepotvrdzuje odolnosť proti prenikaniu páperia.",
        "Poškodený výrobok môže pri praní zlyhať pod hmotnosťou mokrej výplne.",
        "Úplne suché jadro je podmienkou bezpečného uloženia a používania.",
    ],
    "mistakes": [
        "Zamieňať sypkovinu za bežnú obliečku alebo hustý perkál.",
        "Prudko vytiahnuť brko a zväčšiť otvor hrubším koncom.",
        "Vyprať starý praskajúci obal bez kontroly švov a pevnosti.",
        "Použiť veľa prostriedku, ktorý zostane v obale a výplni.",
        "Uložiť výrobok, keď je povrch suchý, ale jadro ešte chladné a zhluknuté.",
    ],
    "expert_heading": "Odbornejší pohľad: priedušnosť, šev a skúška odolnosti proti úniku",
    "expert": [
        "IDFL rozlišuje problémy hotového výrobku pri šití a pri samotnej tkanine a odkazuje na skúšky odolnosti proti prenikaniu výplne aj priepustnosti vzduchu. ASTM D737 meria rýchlosť prechodu vzduchu cez textil za definovaných podmienok. Nízka alebo vysoká hodnota sama nevysvetlí konkrétny otvor pri stehu ani stav starej priadze.",
        "ASTM D3775 určuje počet osnovných a útkových nití, ale IDFL vo svojej analýze upozorňuje, že samotný thread count nekoreluje spoľahlivo s downproofness. Priemer, zákrut a ochlpenie priadze, väzba, kalandrovanie a mechanické spracovanie menia veľkosť aj charakter ciest, ktorými môže jemná výplň prechádzať.",
        "Laboratórny výsledok platí pre konkrétnu vzorku a metódu. Starý vankúš pridáva roky stláčania, kožný maz, opakované vlhčenie, švy a lokálne poškodenia. Spotrebiteľská diagnóza preto musí spájať dokumentáciu miesta úniku, etiketu a stav celého výrobku; jedno číslo z reklamného popisu nestačí na rozhodnutie o praní ani renovácii.",
    ],
    "source_intro": "Zdroje podporujú rozlíšenie úniku cez šev a tkaninu, význam priedušnosti, počtu nití a skúšok hotového výrobku. Nepodporujú tvrdenie, že každá hustá látka alebo vysoký počet nití automaticky tvorí kvalitnú sypkovinu.",
    "sources": [
        ("IDFL: vysvetlenie skúšok páperia a peria", IDFL_TESTS),
        ("IDFL: problémy odolnosti hotových výrobkov proti úniku", IDFL_DOWNPROOF),
        ("IDFL: skúšky textílií pre výplňové výrobky", IDFL_TEXTILES),
        ("IDFL: počet nití verzus odolnosť proti prenikaniu páperia", IDFL_THREAD_COUNT),
        ("ASTM D737: priepustnosť vzduchu textilom", ASTM_AIR),
        ("ASTM D3775: počet osnovných a útkových nití", ASTM_COUNT),
        ("ASTM D1683: správanie šitých švov", ASTM_SEAM),
        ("GINETEX: význam symbolov ošetrovania", GINETEX),
    ],
    "related": [
        ("Ako vyprať páperový vankúš", ARTICLE_FEATHER_PILLOW),
        ("Ako prať paplón a prikrývku", ARTICLE_DUVET),
        ("Ako prať vankúše podľa výplne", ARTICLE_PILLOWS),
        ("Čo je perkál", ARTICLE_PERCALE),
        ("Čo je kapok", ARTICLE_KAPOK),
        ("Prečo textil zapácha po praní", ARTICLE_ODOR),
    ],
    "faq_title": "sypkovina, únik peria a starostlivosť o výplň",
    "faq": [
        ("Čo je sypkovina?", "Hustá funkčná tkanina, ktorá tvorí vnútorný obal peria alebo páperia a obmedzuje jeho únik."),
        ("Je sypkovina to isté ako obliečka?", "Nie. Obliečka je vymeniteľná vonkajšia vrstva, sypkovina priamo uzatvára výplň."),
        ("Je každý perkál vhodný na perie?", "Nie. Hustota je iba jeden parameter; potrebná je overená tkanina a vhodné švové spracovanie."),
        ("Prečo z vankúša vychádza perie?", "Môže unikať cez ihlový otvor, uvoľnený šev, bodové prepichnutie alebo degradovanú plochu."),
        ("Mám vytiahnuť brko, ktoré trčí?", "Nie prudko. Hrubý koniec môže otvor zväčšiť; ak to ide bezpečne, jemne ho zatlačte späť."),
        ("Dá sa sypkovina prať?", "Iba ako súčasť výrobku, ktorého etiketa pranie povoľuje a ktorého obal aj švy sú nepoškodené."),
        ("Ako vyčistiť malú škvrnu?", "Na pevnom obale ju odsajte, urobte skrytú skúšku a použite minimum kompatibilného roztoku."),
        ("Prečo vankúš po praní zapácha?", "Najčastejšie nie je úplne suché jadro alebo vo výplni ostalo znečistenie či produkt."),
        ("Stačí vysoký počet nití?", "Nie. Rozhoduje priadza, väzba, dokončenie, priedušnosť, šev a typ výplne."),
        ("Kedy treba sypkovinu vymeniť?", "Pri plošnom rednutí, praskaní, mnohých otvoroch alebo opakovanom úniku po opravách."),
        ("Môžem dieru prelepiť páskou?", "Nie ako univerzálne riešenie. Tuhý spoj môže zlyhať pri praní a skomplikovať odbornú opravu."),
        ("Ako dlho sušiť perový výrobok?", "Kým nie je suchý v celom jadre; čas závisí od veľkosti, výplne, metódy a podmienok."),
        ("Pomôže ďalšia obliečka proti úniku?", "Môže zachytiť časť častíc, ale neopraví poškodenú sypkovinu ani šev."),
    ],
}

add_laundry_cards(
    SYPKOVINA,
    "výrobok so sypkovinou",
    "Gél použite iba vtedy, keď etiketa povoľuje pranie celého výrobku a obal aj švy sú nepoškodené. Nie je určený na opravu úniku a nadmerná dávka sa z hustého obalu a výplne ťažko vyplachuje.",
)


def add_cleaning_cards(article: dict[str, object]) -> None:
    article.update(
        {
            "product_heading": "Odstraňovač škvŕn iba na výrobcom povolený prateľný povrch",
            "product_intro": (
                "Pri odnímateľnom, stálofarebnom a vodou čistiteľnom bytovom textile možno "
                "po skrytej skúške použiť prípravok určený na škvrny z kobercov a textilných povrchov."
            ),
            "product_name": CLEANING_PRODUCT_NAME,
            "product_url": CLEANING_PRODUCT_URL,
            "product_text": (
                "Prípravok je určený na lokálnu prácu s kompatibilným textilným povrchom. "
                "Dodržte návod, kontaktný čas, odsatie a úplné vysušenie bez presýtenia vrstiev."
            ),
            "product_limit": (
                "Nepoužívajte ho na tvarovaný, škrobený, živicou spevnený alebo lepený sinamay, "
                "klobúk, historický predmet, nelakované dekorácie ani výrobok bez povolenia mokrého čistenia."
            ),
            "category_heading": "Čistenie vyberajte podľa hotového predmetu, nie iba podľa vlákna",
            "category_intro": (
                "Abaka môže byť voľná tkanina, povraz, rohož, papierový kompozit aj tvarovaný doplnok. "
                "Každý systém má inú hranicu vody, trenia, pH a sušenia."
            ),
            "category_name": CLEANING_CATEGORY_NAME,
            "category_url": CLEANING_CATEGORY_URL,
            "category_text": (
                "V kategórii nájdete riešenia pre rôzne čistiteľné povrchy v domácnosti. Pred použitím "
                "vždy overte určenie produktu, návod predmetu a výsledok skrytej skúšky."
            ),
        }
    )


ABACA: dict[str, object] = {
    "title": "Čo je abaka: pevné listové vlákno, vlhkosť a starostlivosť",
    "link": "co-je-abaka-pevne-listove-vlakno-vlhkost-a-starostlivost",
    "meta": "Čo je abaka, ako sa líši od konope a sisalu a ako bezpečne čistiť sinamay, rohože, košíky a tvarované doplnky bez straty tvaru.",
    "short": "Abaka je pevné listové vlákno z rastliny Musa textilis, známe aj ako manilské konope. Nie je to pravé konope ani automaticky prateľný banánový textil; údržbu určuje spracovanie hotového predmetu.",
    "name": "abaka",
    "locative": "abake",
    "identity_heading": "Abaka pochádza z listových pošiev Musa textilis, nie z pravého konope",
    "identity_detail": "Vlákno sa získava z pošiev listov rastliny Musa textilis a technicky patrí medzi listové tvrdé vlákna; názov manilské konope je historický obchodný názov, nie botanická príbuznosť s rodom Cannabis.",
    "identity_boundary": "Surové vlákno, jemná sinamay tkanina, hrubý povraz, papier, živicový kompozit a hotový klobúk môžu všetky obsahovať abaku, no reagujú na vodu, ohyb a chémiu zásadne odlišne.",
    "label_focus": "podiel abaky a ďalších vlákien, škrob, živicu, farbivo, lak, lepidlo, drôt, výstuž, podšívku, povolenie mokrého čistenia, spôsob sušenia a zákaz ponorenia",
    "missing_label": "Neoznačený tvarovaný predmet považujte za konštrukčne citlivý; najprv odstráňte suchý prach a pri škvrne sa obráťte na výrobcu alebo odborníka, namiesto skúšobného namočenia celej plochy.",
    "dry_check": "zlomené vlákna, biele lomové čiary, zvlnený okraj, uvoľnený výplet, vodné mapy, prenos farby, praskajúci lak, lepkavé lepidlo, koróziu drôtu a zatuchnutie v spojoch",
    "damage_boundary": "Prach a časť povrchovej škvrny možno odstrániť, ale polámané zväzky, stratené škrobenie, zvlnený tvar alebo oddelený lepený spoj sa čistením neobnovia.",
    "test_focus": "Skúšku po vyschnutí ohodnoťte pri bočnom svetle a jemnom ohybe; sledujte prenos farby, vznik ostrého okraja, stratu tuhosti, lepivosť a rozdiel medzi osnovou a útkom.",
    "combined_risk": "kapilárneho šírenia vody pozdĺž zväzkov, napučania prímesí, straty tvarovacej úpravy, lomu pri ohýbaní za mokra a korózie skrytého drôtu",
    "chemistry_boundary": "Prírodný pôvod neznamená odolnosť voči kyseline, zásade, chlóru ani rozpúšťadlu; prostriedok môže poškodiť farbivo, škrob, živicu alebo lepidlo skôr než samotné vlákno.",
    "drying_detail": "Tvarovaný predmet podoprite v pôvodnej geometrii a nechajte vzduch obtekať rub, lem aj spoje; rohož neukladajte na nepriedušnú podlahu, kým je spodná strana chladná.",
    "heat_boundary": "Horúci fén, radiátor a priame prudké slnko môžu spôsobiť nerovnomerné zmrštenie, krehnutie povrchovej úpravy, zvlnenie okraja alebo farebný rozdiel.",
    "stop_signs": "uvoľňovanie farby, mäknutie tvaru, praskanie pri miernom ohybe, lepkavosť, rastúca vodná mapa, oddeľovanie vrstiev, korózna stopa alebo pach z vnútra spoja",
    "professional_boundary": "Tvarovaný klobúk, historický sinamay, kombinácia s perím, kožou, drôtom či lepidlom a umelecký predmet patria odborníkovi, ktorý vie čistenie prispôsobiť konštrukcii a zachovať tvar.",
    "answer": "Abaka je pevné rastlinné vlákno získavané z listových pošiev Musa textilis. Často sa nazýva manilské konope, ale nejde o pravé konope a hotový výrobok sa nesmie automaticky prať ako bavlna. Abaka sa používa na laná, papier, rohože, košíky aj jemnú tkaninu sinamay na klobúky a dekorácie. Najprv preto určte, či drží tvar samotná väzba, škrob, živica, lepidlo alebo drôt. Voľný prach odstráňte nasucho mäkkou kefou alebo regulovaným vysávaním cez ochrannú sieťku. Malú škvrnu čistite iba po skrytej skúške a s minimom vlhkosti. Tvarovaný, lepený, lakovaný alebo neoznačený predmet neponárajte. Údaj, že vlákno odoláva poškodeniu slanou vodou, nie je povolením domáceho prania hotového výrobku.",
    "intro": "Pri abake sa stretávajú tri časté omyly. Prvý zamieňa manilské konope s konope siatym, druhý považuje všetky vlákna z rodu Musa za rovnaký banánový textil a tretí prenáša odolnosť surového vlákna na hotový klobúk alebo košík. V praxi však o údržbe rozhoduje jemnosť zväzkov, väzba, farbivo, spevnenie, podšívka, kovová kostra a lepidlo. Jemný sinamay môže stratiť tvar po krátkom navlhčení, kým robustná nefarbená rohož s povoleným čistením znesie opatrný lokálny zásah.",
    "quick": [
        "<strong>Botanický zdroj je Musa textilis:</strong> vlákno sa získava z listových pošiev.",
        "<strong>Manilské konope nie je pravé konope:</strong> názov opisuje obchodnú tradíciu, nie rod Cannabis.",
        "<strong>Sinamay býva tvarovaný:</strong> klobúk môže držať pomocou škrobu, živice, drôtu a lepidla.",
        "<strong>Najprv čistite nasucho:</strong> obmedzíte vodné mapy, stratu tuhosti a koróziu skrytých prvkov.",
        "<strong>Odolné vlákno neznamená prateľný predmet:</strong> najslabšou časťou môže byť farbivo alebo spoj.",
        "<strong>Sušte v pôvodnom tvare:</strong> neohýbajte mokrý výplet a neurychľujte proces bodovým teplom.",
    ],
    "overview_heading": "Odkiaľ abaka pochádza a prečo sa označuje ako tvrdé listové vlákno",
    "overview": [
        "Abaka sa získava z pošiev listov Musa textilis, rastliny pestovanej najmä v tropických oblastiach. Dlhé vláknité zväzky sa mechanicky oddeľujú, čistia, sušia a triedia. FAO ju zaraďuje medzi jutu a tvrdé vlákna a opisuje jej tradičné aj priemyselné využitie. Vysoká pevnosť a dĺžka boli dôležité pri lodných lanách, no dnes sa abaka spracúva aj na špeciálne papiere, kompozity a dekoratívny textil.",
        "Označenie tvrdé vlákno neznamená, že každý výrobok je na dotyk tvrdý. Ide o skupinu a pôvod suroviny. Jemne rozdelené a tkané zväzky môžu vytvoriť priesvitný sinamay, zatiaľ čo menej jemné vlákna tvoria robustný povraz alebo rohož. Spracovanie mení ohybnosť, povrch, pórovitosť aj spôsob, akým voda postupuje medzi vláknami.",
        "FAO uvádza odolnosť abakového vlákna voči poškodeniu slanou vodou ako jednu z vlastností, ktorá podporila námorné použitie. To je informácia o vlákne a konkrétnom druhu zaťaženia, nie o všetkých zložkách spotrebiteľského výrobku. Farbivo môže migrovať, škrob sa rozpustiť, drôt korodovať a lepidlo zmäknúť, hoci samotný vláknitý zväzok ostane celistvý.",
    ],
    "table1_heading": "Abaka, sisal, ramia, konope a bežné banánové vlákno",
    "table1_intro": "Názvy sa v predaji zamieňajú. Rozlíšenie rastlinnej časti a hotovej formy pomáha určiť realistické riziká, nie univerzálny program.",
    "table1_headers": ["Materiál", "Zdroj vlákna", "Typická forma", "Hranica pri údržbe"],
    "table1_rows": [
        ("Abaka", "Listové pošvy Musa textilis.", "Lano, papier, sinamay, rohož, kompozit.", "Rozhoduje farbivo, tvarovanie, živica, drôt a lepidlo."),
        ("Sisal", "Listy Agave sisalana a príbuzných agáv.", "Povraz, koberce, škrabadlá a výplet.", "Vodné mapy, tuhosť a čistenie podkladu."),
        ("Ramia", "Lykové vlákno zo stonky Boehmeria nivea.", "Jemnejšia priadza a zmesi v odevoch.", "Krčivosť, ostré lomy a zloženie zmesi."),
        ("Pravé konope", "Lykové vlákno zo stonky Cannabis sativa.", "Odevy, plátno, povraz a zmesi.", "Manilské konope nie je botanicky tento materiál."),
        ("Iné banánové vlákno", "Rôzne časti iných druhov alebo hybridov Musa.", "Papier, remeselný materiál, priadza.", "Obchodný názov nemusí potvrdiť Musa textilis ani rovnaké vlastnosti."),
    ],
    "sections": [
        {
            "heading": "Ako rozpoznať abaku bez domácej skúšky horením",
            "paragraphs": [
                "Spoľahlivé určenie začína etiketou, technickým listom, pôvodom a názvom výrobcu. Vizuálne možno vidieť dlhé svetlé zväzky a nepravidelný rastlinný povrch, no podobne môžu vyzerať sisal, rafia, papierová priadza aj syntetická imitácia. Skúška horením na hotovom predmete je nebezpečná a nerozlíši lepidlo, lak ani zmes.",
                f"Porovnávajte funkciu a spracovanie. <a href=\"{ARTICLE_SISAL}\">Sisalové koberce a rohože</a> často používajú hrubšie agávové vlákno, kým <a href=\"{ARTICLE_RAMIE}\">ramia</a> je lykové vlákno spracúvané aj do jemnejších odevných zmesí. Ak deklarácia uvádza iba plant fibre alebo banana fibre, nie je korektné doplniť druh bez dokumentácie.",
            ],
        },
        {
            "heading": "Čo je sinamay a prečo klobúk nesmie ísť pod vodu",
            "paragraphs": [
                "Sinamay je ľahká otvorenejšia tkanina tradične spájaná s abakovým vláknom. Pri klobúkoch, fascinátoroch a dekoráciách sa často tvaruje pomocou škrobu alebo živice a kombinuje s drôtom, stuhou, perím, podšívkou a lepidlom. Viditeľná tkanina môže vodu zniesť inak než spoj, ktorý drží okraj alebo ozdobu.",
                "Prach odstraňujte mäkkým čistým štetcom v smere výpletu alebo veľmi slabým regulovaným vysávaním cez sieťku bez dotyku dýzy. Klobúk držte za podopretú korunu, nie za krehký okraj. Pri škvrne najprv kontaktujte výrobcu alebo klobučníka. Ponorenie, para zblízka a mokré pretvarovanie môžu uvoľniť apretúru a vytvoriť nevratnú asymetriu.",
            ],
        },
        {
            "heading": "Ako čistiť abakovú rohož, košík alebo prestieranie",
            "paragraphs": [
                "Najprv výrobok vyneste na suché miesto, podoprite a odstráňte voľný prach mäkkou kefou alebo vhodným vysávaním. Neudierajte ním o hranu; vysušené rastlinné zväzky môžu pri bodovom lome prasknúť. Skontrolujte rub, lem, výplet a prípadnú protišmykovú vrstvu. Tá môže mať nižšiu toleranciu vody než líc.",
                "Ak výrobca povoľuje vlhké čistenie, pracujte po malých zónach s minimom roztoku a okamžitým odsávaním. Rohož nesaturujte a neskladajte za mokra. Obe strany musia schnúť v prúdiacom vzduchu. Košík podoprite zvnútra čistým tvarom bez farbiaceho papiera, aby sa pri schnutí neprepadol, no nevyvíjajte tlak na navlhnuté spoje.",
            ],
        },
        {
            "heading": "Vodné mapy na abake: prečo vznikajú a ako ich nezväčšiť",
            "paragraphs": [
                "Kvapalina sa môže šíriť kapilárne pozdĺž zväzkov a pri odparovaní preniesť rozpustenú nečistotu k okraju. Vznikne tmavší alebo svetlejší prstenec, hoci pôvodná kvapka bola malá. Minerály z vody, farbivo, zvyšok prostriedku a prach menia vzhľad okraja. Opakované bodové namáčanie bez odsatia mapu často rozšíri.",
                f"Čerstvú tekutinu odsávajte bielou savou handričkou bez trenia. Pred ďalším krokom určte jej zloženie podľa <a href=\"{ARTICLE_STAIN}\">návodu pre rôzne škvrny</a> a urobte skúšku na skrytom mieste. Ak sa farba prenáša alebo tvar mäkne, zastavte. Zaschnutý okraj na hodnotnom sinamay nevyrovnávajte domácim premočením celej plochy.",
            ],
        },
        {
            "heading": "Pleseň a zatuchnutie: najprv odstráňte vlhkostnú príčinu",
            "paragraphs": [
                "Zatuchnutý pach signalizuje prostredie s nedostatočným vysychaním, nie potrebu silnejšej parfumácie. Skontrolujte podlahu pod rohožou, stenu za dekoráciou, vnútro koša a všetky lepené spoje. Predmet premiestnite do suchého vetraného priestoru bez rozptyľovania viditeľného nánosu do interiéru.",
                "Rozsiahly mikrobiálny rast, zdravotne citlivá domácnosť alebo zapojenie porézneho lepidla sú hranicou pre odborné posúdenie. Chlór, ocot a iné domáce zmesi nemiešajte a nepoužívajte bez overenia. Môžu odfarbiť prírodné vlákno, poškodiť kov a nezabezpečiť odstránenie kontaminácie z vnútra viacvrstvového predmetu.",
            ],
        },
        {
            "heading": "Farbená abaka a ochrana pred svetlom a trením",
            "paragraphs": [
                f"Farbivo môže byť vnesené do vlákna alebo nanesené na hotový výplet a jeho stálosť sa líši. Pri tmavých a sýtych odtieňoch skúšajte prenos navlhčenou bielou handričkou bez trenia. Základné mechanizmy rozoberá článok <a href=\"{ARTICLE_COLOR}\">prečo farby blednú pri svetle a čistení</a>. Výsledok jednej malej skúšky sa nevzťahuje automaticky na inú ozdobu alebo lem.",
                "Priame slnko môže meniť farbivo aj prirodzený odtieň vlákna. Predmet pravidelne otáčajte iba vtedy, ak to neohrozí jeho tvar, a neskladujte ho pri okne pod bodovým svetlom. Trenie kabelky, ruky alebo nábytku vytvára lokálne hladšie a svetlejšie miesto; ďalšie mokré čistenie mechanický oder nevráti.",
            ],
        },
        {
            "heading": "Sušenie tvarovaných a plošných výrobkov z abaky",
            "paragraphs": [
                "Plošnú rohož po povolenom čistení sušte vodorovne alebo podľa návodu tak, aby vzduch dosiahol aj rub. Mokrý výrobok nevešajte za jeden roh a neprekladajte cez tenkú tyč, pretože hmotnosť vytvorí ostrý lom. Podklad pravidelne kontrolujte a vymeňte za suchý, ak zadržiava vlhkosť.",
                "Klobúk alebo košík podoprite v pôvodnom tvare materiálom, ktorý nepúšťa farbu a neuzavrie vnútro. Nepoužívajte horúci fén ani radiátor. Pred uložením skontrolujte švy, lem a spoje dotykom aj pachom. Ak ostávajú chladné alebo lepkavé, sušenie nie je ukončené a uzavretá krabica by problém zhoršila.",
            ],
        },
        {
            "heading": "Oprava zlomeného výpletu, lemu a dekorácie",
            "paragraphs": [
                "Zlomené vlákno nestrihajte zarovno bez posúdenia, pretože môže byť nosnou súčasťou výpletu. Na košíku uvoľnite zaťaženie a miesto dočasne chráňte pred zachytením. Bežné univerzálne lepidlo môže vytvoriť tmavú škvrnu, tuhý okraj a neskôr prasknúť. Oprava má rešpektovať pôvodnú väzbu a pružnosť.",
                "Pri klobúku odborník vie rozlíšiť, či zlyhala abaka, apretúra, drôt alebo spoj ozdoby. Pretvarovanie parou bez skúseností môže meniť väčšiu zónu než poškodenie. Historický predmet nečistite ani neopravujte podľa remeselného návodu pre nový materiál; starnúce farbivá a predchádzajúce zásahy vyžadujú konzervátora.",
            ],
        },
        {
            "heading": "Ako abaku skladovať bez vlhkosti, tlaku a škodcov",
            "paragraphs": [
                "Predmet uložte čistý, úplne suchý a podopretý. Klobúk potrebuje dostatočne veľkú krabicu, aby sa okraj neopieral o stenu; rohož neskladajte do ostrého lomu a košík nepreťažujte inými predmetmi. Obal má chrániť pred prachom, no nesmie uzavrieť zvyškovú vlhkosť.",
                "Pravidelne kontrolujte tmavé spoje, dno a miesta pri stene. Nové drobné čiastočky, otvory alebo vlákna môžu signalizovať mechanické rozpadanie alebo škodcu; najprv oddeľte predmet od ostatných a zdokumentujte stav. Preventívne nestriekajte insekticíd priamo na porézny materiál bez odborného odporúčania.",
            ],
        },
    ],
    "table2_heading": "Typ výrobku z abaky a bezpečný prvý zásah",
    "table2_intro": "Rovnaké vlákno neznamená rovnakú údržbu. Najslabšia súčasť hotového predmetu určuje hranicu vody a mechaniky.",
    "table2_headers": ["Výrobok", "Čo drží funkciu alebo tvar", "Bezpečný prvý krok", "Čomu sa vyhnúť"],
    "table2_rows": [
        ("Sinamay klobúk", "Apretúra, tvarovanie, drôt, lepidlo a podšívka.", "Mäkký štetec a odborná konzultácia škvrny.", "Ponorenie, para zblízka a horúci fén."),
        ("Rohož", "Výplet, lem a prípadný protišmykový podklad.", "Suché čistenie a kontrola návodu.", "Presýtenie vodou a sušenie na nepriedušnej podlahe."),
        ("Košík", "Priestorový výplet a spoje.", "Oprášiť a podoprieť pri lokálnom zásahu.", "Stlačenie alebo skladanie za mokra."),
        ("Kabelka", "Výplet, podšívka, výstuž, kovanie a lepidlo.", "Vyprázdniť, oprášiť a skúšať každú vrstvu osobitne.", "Práčka a spoločné namáčanie všetkých materiálov."),
        ("Voľná tkanina", "Priadza, väzba, farba a dokončenie.", "Technický list a skúšobné predpranie vzorky.", "Preniesť skúšku metráže na hotový vystužený predmet."),
    ],
    "steps_heading": "Bezpečný postup pri neznámom výrobku z abaky",
    "steps": [
        "Overte deklaráciu abaky, výrobcu, účel predmetu a dostupný návod.",
        "Určte, či ide o voľnú tkaninu, výplet, tvarovaný, lepený alebo lakovaný systém.",
        "Skontrolujte farbu, zlomené vlákna, drôt, lepidlo, podšívku a rub.",
        "Voľný prach odstráňte mäkkým štetcom alebo regulovaným vysávaním cez sieťku.",
        "Určte pôvod škvrny a na skrytom mieste otestujte farbu aj tuhosť.",
        "Mokrý postup použite iba pri výslovnom povolení celého predmetu.",
        "Naneste minimum roztoku bez presýtenia a priebežne ho odsávajte.",
        "Tvarovaný kus podoprite; plošný materiál neohýbajte za mokra.",
        "Sušte zo všetkých strán bez radiátora, horúceho fénu a prudkého slnka.",
        "Pred uložením overte suchosť spojov a pri deformácii alebo lepkavosti zastavte.",
    ],
    "remember": [
        "Abaka je listové vlákno Musa textilis, nie pravé konope.",
        "Sinamay môže držať tvar pomocou materiálov citlivejších než samotné vlákno.",
        "Údaj o odolnosti surového vlákna voči slanej vode nepovoľuje pranie klobúka.",
        "Prach odstraňujte pred vodou a každú farebnú časť skúšajte osobitne.",
        "Mokrý výplet podoprite a sušte bez bodového tepla.",
    ],
    "mistakes": [
        "Zamieňať manilské konope za pravé konope alebo každé banánové vlákno za abaku.",
        "Ponoriť tvarovaný sinamay klobúk, pretože rastlinné vlákno pôsobí pevne.",
        "Použiť veľa vody na malú škvrnu a vytvoriť široký kapilárny okraj.",
        "Sušiť košík na radiátore alebo rohož na nepriedušnej mokrej podlahe.",
        "Zlepiť zlomený výplet univerzálnym lepidlom bez skúšky a znalosti konštrukcie.",
    ],
    "expert_heading": "Odbornejší pohľad: listové zväzky, lignín a rozdiel medzi vláknom a výrobkom",
    "expert": [
        "FAO opisuje abaku ako vlákno z listových pošiev Musa textilis s dlhými a pevnými vláknitými zväzkami a uvádza významný podiel lignínu. PhilFIDA dokumentuje pestovanie, zber, mechanické oddeľovanie a triedenie. Kvalita hotového materiálu preto závisí od časti rastliny, extrakcie, čistenia, sušenia a následného spracovania, nie iba od názvu druhu.",
        "Vlákno je hierarchický prírodný systém, ktorý prijíma a odovzdáva vlhkosť cez povrch aj vnútorné cesty. V hotovom výplete sa pridávajú kontakty medzi zväzkami a kapilárne medzery; farbivo, škrob alebo živica menia zmáčanie. Preto voda môže migrovať ďaleko od škvrny a po odparení zanechať materiál prenesený k okraju.",
        "Múzejné zbierky dokumentujú sinamay z abaky aj kombinácie s bavlnou. Také predmety dokazujú šírku spracovania, nie jeden domáci návod. Konzervátorské rozhodnutie zohľadňuje vek, farbivo, predchádzajúcu opravu, historickú hodnotu a zamýšľanú funkciu; moderná rohož s návodom na čistenie a historický klobúk sa nesmú posudzovať rovnakým rizikom.",
    ],
    "source_intro": "Zdroje podporujú botanický pôvod abaky, jej zaradenie medzi listové tvrdé vlákna, spracovanie a použitie sinamay. Nepodporujú prenos vlastností surového vlákna na univerzálne mokré čistenie všetkých hotových predmetov.",
    "sources": [
        ("FAO: prehľad juty a tvrdých vlákien", FAO_HARD_FIBRES),
        ("PhilFIDA: príručka udržateľnosti abaky", PHILFIDA_MANUAL),
        ("PhilFIDA: technická príručka pestovania a spracovania", PHILFIDA_GUIDE),
        ("Penn Museum: sinamay z abaky a bavlny", PENN_SINAMAY),
        ("Fowler Museum: textílie z lykových a listových vlákien", FOWLER_FIBRES),
        ("EÚ 1007/2011: názvy textilných vlákien", EU_FIBRE_LABEL),
        ("GINETEX: význam symbolov ošetrovania", GINETEX),
    ],
    "related": [
        ("Čo je sisal", ARTICLE_SISAL),
        ("Čo je ramia", ARTICLE_RAMIE),
        ("Čo je canvas", ARTICLE_CANVAS),
        ("Ako čítať štítok", ARTICLE_LABEL),
        ("Ako postupovať pri škvrnách", ARTICLE_STAIN),
        ("Prečo farby blednú", ARTICLE_COLOR),
    ],
    "faq_title": "abaka, sinamay a domáca starostlivosť",
    "faq": [
        ("Čo je abaka?", "Pevné listové vlákno získavané z pošiev rastliny Musa textilis."),
        ("Je abaka konope?", "Nie. Manilské konope je historický názov; botanicky nejde o rod Cannabis."),
        ("Je abaka banánové vlákno?", "Patrí do rodu Musa, ale presný názov abaka označuje Musa textilis, nie každé vlákno z banánovníka."),
        ("Čo je sinamay?", "Ľahká tkanina tradične vyrábaná z abaky, často tvarovaná na klobúky a dekorácie."),
        ("Dá sa sinamay prať?", "Tvarovaný výrobok spravidla neponárajte; riaďte sa návodom výrobcu a konštrukciou celého predmetu."),
        ("Ako odstrániť prach z abaky?", "Mäkkým štetcom alebo regulovaným vysávaním cez ochrannú sieťku bez lámania výpletu."),
        ("Ako vyčistiť škvrnu na abakovom košíku?", "Najprv skrytá skúška; potom minimum povoleného roztoku, odsatie a sušenie v podopretom tvare."),
        ("Prečo po vode vznikla mapa?", "Kvapalina preniesla farbivo, minerály alebo nečistotu pozdĺž zväzkov k okraju schnutia."),
        ("Môže ísť abaka na radiátor?", "Nie ako všeobecný postup. Bodové teplo môže zvlniť tvar a poškodiť úpravu alebo lepidlo."),
        ("Je abaka to isté ako sisal?", "Nie. Abaka pochádza z Musa textilis, sisal z listov agávy."),
        ("Ako skladovať sinamay klobúk?", "Úplne suchý, podopretý v dostatočne veľkej krabici, mimo tlaku, vlhkosti a priameho svetla."),
        ("Dá sa zlomený výplet zlepiť?", "Univerzálne lepidlo môže vytvoriť škvrnu a tuhý lom; vhodnejšia je remeselná oprava."),
        ("Prečo pevná abaka praskla?", "Vysušený alebo tvarovaný zväzok môže byť pevný v ťahu, ale citlivý na ostrý bodový ohyb."),
    ],
}

add_cleaning_cards(ABACA)


MOLESKIN: dict[str, object] = {
    "title": "Čo je moleskin: hustá brúsená bavlna, lesk a správne pranie",
    "link": "co-je-moleskin-husta-brusena-bavlna-lesk-a-spravne-pranie",
    "meta": "Čo je moleskin, ako sa líši od flanelu, menčestru a zamatu a ako prať, sušiť, žehliť a čistiť hustú brúsenú bavlnu bez lesku a máp.",
    "short": "Textilný moleskin je hustá pevná tkanina, tradične bavlnená, s krátko brúseným alebo počesaným povrchom podobným semišu. Nie je to koža a nie je to samolepiaca zdravotnícka náplasť.",
    "name": "moleskin",
    "locative": "moleskine",
    "identity_heading": "Textilný moleskin je hustá brúsená tkanina, nie zvieracia koža",
    "identity_detail": "Tradičný odevný moleskin používa hustú útkovo dominantnú konštrukciu a mechanické brúsenie alebo počesanie, po ktorom sa povrch zastrihne na krátky rovnomerný vlas podobný semišu.",
    "identity_boundary": "Moderný výrobok môže obsahovať elastan alebo syntetické vlákno a slovo moleskin sa používa aj pre samolepiaci materiál proti otlakom; tieto výrobky nemajú spoločný návod na pranie.",
    "label_focus": "percentá bavlny, elastanu a syntetiky, smer krátkeho vlasu, farbenie, živicovú úpravu, podšívku, výstuž, kožené detaily, lepidlo, povolenie sušičky a teplotu žehlenia",
    "missing_label": "Pri nohaviciach bez etikety nepredpokladajte vysokú teplotu iba podľa hmotnosti látky; pri metráži skúšobne predperte dostatočne veľkú vzorku a zmerajte rozmer aj zmenu povrchu.",
    "dry_check": "lesklé kolená a sed, svetlé zalomenia, mastné manžety, prach v brúsenom povrchu, žmolky, vyhladené hrany, rozostúpený šev, poškodený zips a rozdiel smeru vlasu",
    "damage_boundary": "Prach, maz a časť škvŕn možno odstrániť, ale vyhladený vlas, odreté farbivo, zlomený ostrý lom a lesk po horúcom tlaku nie sú zvyšok pracieho produktu.",
    "test_focus": "Po úplnom vyschnutí povrch jemne prečešte jedným smerom a porovnajte lesk z viacerých uhlov; mokrý alebo obrátený vlas môže dočasne vyzerať ako farebná škvrna.",
    "combined_risk": "napučania hustej bavlny, zle vypláchnutého produktu, sploštenia krátkeho vlasu trením a teplom a rozdielneho zrazenia elastanu, nite a podšívky",
    "chemistry_boundary": "Olej, blato, vosk a pigment sa neošetrujú rovnakým spôsobom; silný odmasťovač alebo bielidlo môže na matnom povrchu vytvoriť ostrý svetlý kruh.",
    "drying_detail": "Nohavice otočte a urovnajte podľa etikety, otvorte vrecká a pás, aby husté vrstvy schli rovnomerne; pred uložením skontrolujte sed, švy, manžety aj vnútro vreciek.",
    "heat_boundary": "Vysoké teplo môže zraziť bavlnu, poškodiť elastan, zataviť syntetickú prímes, sploštiť vlas alebo vytvoriť lesklú stopu, ktorú ďalšie pranie neodstráni.",
    "stop_signs": "silný prenos farby, rastúca svetlá mapa, lepkavý povrch, praskanie pri ohybe, otváranie šva, zvlnenie podšívky alebo trvalý rozdiel vlasu po vyschnutí",
    "professional_boundary": "Bežné nekomplikované nohavice s povoleným praním možno ošetrovať doma, no podšitá bunda, odev s kožou, voskovanou úpravou, historický kus alebo značka iba profesionálneho čistenia potrebuje odborný postup.",
    "answer": "Moleskin je hustá tkanina s krátko brúseným alebo počesaným povrchom, tradične z bavlny. Jemný vlas jej dáva matný semišový omak, ale pri trení a horúcom tlaku sa môže vyhladiť a začať lesknúť. Pred praním odlíšte textilný moleskin od samolepiacej zdravotníckej náplasti a prečítajte zloženie celého odevu. Prach najprv odstráňte jemnou kefou, škvrny riešte podľa ich pôvodu a farbu skúste na skrytom mieste. Perte iba pri povolení výrobcu, s podobnými farbami, primeranou dávkou a voľným priestorom. Po cykle kus urovnajte, sušte bez prudkého tepla a žehlite z rubu cez ochrannú tkaninu pri nízkom tlaku. Lesklé kolená po dlhom nosení sú často mechanicky vyhladený vlas, nie nečistota.",
    "intro": "Moleskin sa v slovenčine objavuje menej často než menčester alebo flanel, no používa sa na pracovné nohavice, bundy, vesty a pevné voľnočasové odevy. Jeho komfort vytvára spojenie hustej konštrukcie a veľmi krátkeho upraveného povrchu. Práve táto dvojica spôsobuje typické problémy: látka zadrží viac vody a produktu, kým vlas sa na kolenách, sedacej časti a okrajoch vyhladí. Správna starostlivosť preto chráni nielen bavlnené vlákno a rozmer, ale aj smer a rovnomernosť povrchu.",
    "quick": [
        "<strong>Nie je to koža:</strong> názov odkazuje na hladký jemný omak, nie na živočíšny pôvod.",
        "<strong>Povrch vzniká mechanicky:</strong> brúsenie alebo počesanie vytvorí veľmi krátky vlas.",
        "<strong>Lesk často znamená oder:</strong> vyhladené kolená sa ďalším drhnutím nezmatnia.",
        "<strong>Hustá látka potrebuje oplach:</strong> nepreplňte bubon a neprekračujte dávku.",
        "<strong>Žehlite z rubu:</strong> tlak z líca môže sploštiť vlas a obtlačiť švy.",
        "<strong>Zmes mení limity:</strong> elastan, podšívka, voskovanie alebo kožené detaily majú vlastné hranice.",
    ],
    "overview_heading": "Ako vzniká krátky semišový povrch moleskinu",
    "overview": [
        "Historické technické opisy spájajú moleskin s ťažkou, pevnou bavlnenou tkaninou s veľmi hustým útkom a útkovo dominantným lícom. Väzba môže mať saténový charakter, vďaka ktorému je na povrchu viac dlhších útkových úsekov. Následné brúsenie, počesanie a zastrihnutie vytvorí nízky rovnomerný vlas. Výsledok pôsobí mäkšie než neupravená tkanina rovnakej hmotnosti.",
        "CottonWorks opisuje sueding ako mechanické obrusovanie povrchu jemnými abrazívnymi valcami alebo pásmi a napping ako vyťahovanie koncov vlákien pomocou háčikov. Shearing potom povrch zastrihuje. Konkrétny výrobca môže kombinovať kroky inak, preto sa moleskin pohybuje od takmer hladkého matu po zreteľne mäkký povrch.",
        "Brúsenie zámerne naruší časť povrchových vlákien, aby vznikol omak. To neznamená, že je látka nekvalitná, ale povrch sa opotrebúva inak než hladké plátno. Trenie pri sedení, kľačaní a nosení predmetov vo vreckách vlas postupne ukladá jedným smerom alebo vyhladí. Svetlo sa potom odráža súvislejšie a miesto pôsobí tmavšie alebo lesklejšie.",
    ],
    "table1_heading": "Moleskin, flanel, menčester, velúr a canvas",
    "table1_intro": "Materiály môžu pôsobiť teplo a pevne, ale ich povrch vzniká odlišne. To mení spôsob zachytávania prachu, oderu aj žehlenia.",
    "table1_headers": ["Materiál", "Ako vzniká povrch", "Typický vzhľad", "Hlavné riziko"],
    "table1_rows": [
        ("Moleskin", "Hustá tkanina sa jemne brúsi alebo počesáva a zastrihuje.", "Krátky rovnomerný matný vlas bez rebier.", "Vyhladenie, lesk, zvyšok produktu a zrazenie."),
        ("Flanel", "Povrch jednej alebo oboch strán sa počesáva do mäkkého vlasu.", "Mäkší a chlpatejší omak.", "Žmolkovanie, zľahnutie vlasu a zrazenie."),
        ("Menčester", "Vlas sa vytvára a strihá v pozdĺžnych rebrách.", "Zreteľné prúžky a žliabky.", "Prach v rebrách, sploštenie a zachytenie."),
        ("Velúr alebo zamat", "Samostatný vlasový systém alebo rezané slučky podľa konštrukcie.", "Výrazná zmena odtieňa podľa smeru vlasu.", "Tlak, voda, smer vlasu a citlivá podkladová konštrukcia."),
        ("Canvas", "Pevná hladšia plátnová konštrukcia bez zámerného krátkeho vlasu.", "Suchší zrnitý povrch.", "Ostré lomy, povlak a pomalé sušenie hrubých vrstiev."),
    ],
    "sections": [
        {
            "heading": "Ako rozoznať moleskin od flanelu, menčestru a zamatu",
            "paragraphs": [
                "Pozrite sa pri bočnom svetle a prejdite dlaňou v oboch smeroch. Moleskin má spravidla veľmi krátky súvislý matný povrch bez pozdĺžnych rebier. Flanel býva chlpatejší, menčester má zreteľné rebrá a vlasový zamat reaguje na smer výraznejšou zmenou odtieňa. Samotný dotyk však neurčí zloženie.",
                f"Na rube môže byť väzba čitateľnejšia a bez rovnakej úpravy. Útkovo dominantné úseky môžu pripomínať <a href=\"{ARTICLE_SATIN}\">saténovú väzbu</a>, no výsledný brúsený povrch nie je lesklý satén. Nevyťahujte niť z hotového šva a nerobte skúšku horením; elastan, farbivo a povrchová úprava vyžadujú údaje výrobcu.",
            ],
        },
        {
            "heading": "Suché čistenie prachu pred každým mokrým zásahom",
            "paragraphs": [
                "Krátky vlas zachytáva jemný prach, ktorý po navlhčení môže vytvoriť sivú pastu a mapu. Odev najprv vytraste vonku bez úderov o ostrú hranu a prečešte mäkkou čistou kefou jedným smerom. Vrecká obráťte iba vtedy, ak to dovolí konštrukcia, a odstráňte piesok zo švov, aby pri praní nefungoval ako abrazívum.",
                "Pri vysávaní použite nízky regulovaný výkon a čistý textilný nadstavec bez ostrých hrán. Poškodenú alebo uvoľnenú plochu chráňte sieťkou a nedotýkajte sa dýzou. Suchý krok zároveň ukáže, či tmavé koleno bolo iba prach, mastnota alebo zmena smeru vlasu. Až potom má zmysel vyberať lokálny prostriedok.",
            ],
        },
        {
            "heading": "Lesklé kolená a sed: škvrna alebo vyhladený vlas",
            "paragraphs": [
                "Mastnota mení lom svetla a zlepuje krátke vlákna, preto môže povrch pôsobiť tmavšie a hladšie. Po šetrnom odmastení a úplnom vyschnutí ho jemne prečešte. Ak sa vzhľad mení so smerom a vracia sa mat, problém bol aspoň sčasti v znečistení alebo uložení vlasu. Ak ostáva hladká súvislá plocha, ide skôr o mechanický oder.",
                "Vyhladené miesto nebrúste šmirgľom, tvrdou kefou ani pemzou. Taký zásah odstráni ďalšie vlákna, zoslabí tkaninu a vytvorí nepravidelný svetlý ostrov. Pri pracovných nohaviciach možno akceptovať patinu; pri hodnotnom odeve sa poraďte o odbornom naparení alebo úprave, ktorá rešpektuje konkrétne farbivo a zloženie.",
            ],
        },
        {
            "heading": "Ako odstrániť blato, mastnotu a lokálne škvrny",
            "paragraphs": [
                f"Blato nechajte zaschnúť a voľnú zeminu odstráňte nasucho, aby ste pigment nezatlačili do vlasu. Mastný prebytok odsajte bez rozmazania. Vosk, krv, žuvačka a farbivá potrebujú odlišné postupy; začnite preto <a href=\"{ARTICLE_STAIN}\">sprievodcom podľa pôvodu škvrny</a>. Vždy skúšajte aj zmenu vlasu, nie iba farbu.",
                "Roztok nanášajte na handričku alebo podľa návodu výrobcu produktu, nie automaticky priamo na suchý odev. Pracujte po malých krokoch od okraja a priebežne odsávajte. Prudké krúživé trenie rozhádže smer vlasu a vytvorí lem. Ak sa farba prenáša alebo miesto po vyschnutí tvrdne, nepokračujte silnejším prípravkom.",
            ],
        },
        {
            "heading": "Ako prať moleskin nohavice alebo bundu",
            "paragraphs": [
                f"Najprv použite <a href=\"{ARTICLE_LABEL}\">návod na čítanie štítka</a> a skontrolujte podšívku, elastan, zips, cvoky, kožené nášivky a voskovanú úpravu. Zapnite kovové prvky, vyprázdnite vrecká a kus podľa etikety otočte naruby, aby sa líc menej trel o bubon. Triedte podľa farby a hmotnosti; ťažké zipsy iných odevov môžu brúsený povrch poškriabať.",
                "Zvoľte povolenú teplotu, mechaniku a odstreďovanie. Hustý moleskin potrebuje priestor na navlhčenie aj oplach, preto neplňte bubon na maximum a dávku neodhadujte podľa objemu suchých nohavíc. Aviváž nepoužívajte automaticky; môže meniť povrch, savosť a správanie zmesi. Pri prvom tmavom kuse overte prenos farby.",
            ],
        },
        {
            "heading": "Prečo je moleskin po praní tvrdý, fľakatý alebo lepkavý",
            "paragraphs": [
                "Tvrdosť môže pochádzať zo zvyšku produktu, minerálov z vody, zaschnutia vlasu v jednom smere alebo zmeny povrchovej úpravy. Najprv nechajte odev úplne vyschnúť, jemne ho prečešte a skontrolujte, či má bielu mapu alebo lepkavý omak. Ďalšia dávka prostriedku problém so zvyškom zhorší.",
                "Ak etiketa povoľuje pranie a príčinou je pravdepodobne nadmerná dávka, môže pomôcť primeraný cyklus bez ďalšieho produktu a s voľným priestorom na oplach. Nevykonávajte ho pri odfarbenej, zrazenej alebo lepidlom poškodenej časti. Tieto zmeny voda neodstráni a opakovanie cyklu pridá ďalšie mechanické namáhanie.",
            ],
        },
        {
            "heading": "Sušenie bez zrazenia, ostrých lomov a zatuchnutia",
            "paragraphs": [
                f"Mokrý moleskin je ťažší než ľahká košeľovina. Nohavice urovnajte podľa švov a podoprite tak, aby sa pás ani kolená nenaťahovali za jeden bod. Vrecká a viacvrstvové manžety otvorte prúdeniu vzduchu. Pri sušení v byte pomôže <a href=\"{ARTICLE_DRYING}\">postup proti zatuchnutiu</a> bez uzavretia hrubých miest pri stene.",
                f"Sušička je možná len pri výslovnom symbole. Teplo a prevaľovanie môžu zvýšiť zrazenie, oder a sploštenie; súvislosti vysvetľuje článok <a href=\"{ARTICLE_SHRINKAGE}\">prečo sa oblečenie zráža</a>. Kus vyberte včas a nevytvárajte ostrý sklad, kým je ešte vlhký. Pred skriňou skontrolujte švy, pás a vnútro vreciek.",
            ],
        },
        {
            "heading": "Ako žehliť alebo napariť moleskin bez lesku",
            "paragraphs": [
                "Najbezpečnejší je rub, čistá mäkká podložka a ochranná tkanina. Začnite nízkym tlakom a teplotou povolenou pre najcitlivejšiu zložku. Silné stlačenie z líca uloží krátky vlas, pretlačí rezervy švov a vytvorí lesklý obrys vrecka. Na tmavom kuse je taká stopa obzvlášť viditeľná.",
                "Para nie je univerzálna oprava. Môže pomôcť uvoľniť záhyb, ale pri živicovej, voskovanej alebo lepidlom spojenej časti zmení dokončenie. Žehličku nedržte dlho na mieste a odev nechajte po práci úplne vychladnúť a vyschnúť bez tlaku. Až potom vlas jemne zjednoťte mäkkou kefou.",
            ],
        },
        {
            "heading": "Žmolky, zachytenia a oder na hustej brúsenej ploche",
            "paragraphs": [
                f"Uvoľnené povrchové vlákna sa môžu pri trení zamotať do žmolkov, najmä v zmesi s pevnejšou syntetikou. Mechanizmus rozoberá článok <a href=\"{ARTICLE_PILLING}\">prečo sa oblečenie žmolkuje</a>. Odžmolkovač používajte iba pri povolení a na rovnej podopretej ploche; príliš hlboký rez môže zachytiť základnú tkaninu.",
                f"Ostrý suchý zips, hrana kovania alebo poškodený bubon môže vytiahnuť priadzu. Podľa <a href=\"{ARTICLE_SNAGGING}\">návodu pri zatrhnutí</a> voľný koniec nestrihajte skôr, než zistíte, či je súčasťou nosnej väzby. Odev perte oddelene od abrazívnych predmetov a poškodený šev opravte pred ďalším zaťažením.",
            ],
        },
        {
            "heading": "Textilný moleskin verzus samolepiaci moleskin proti otlakom",
            "paragraphs": [
                "V lekárňach a športových potrebách sa moleskin používa aj ako názov mäkkého samolepiaceho materiálu, ktorý chráni pokožku pred trením. Obsahuje lepiacu vrstvu a je určený na iný účel než nohavicová tkanina. Pokyny pre pranie odevu sa naň nevzťahujú a jeho zvyšok na textile sa rieši ako lepidlo.",
                "Ak sa samolepiaci kus zachytil na odeve, nežehlite ho a nedávajte do sušičky, kým sa neodstráni. Teplo môže lepidlo zatlačiť hlbšie a preniesť na bubon. Overte odporúčanie výrobcu lepidla a odevu, vykonajte skrytú skúšku a pracujte bez rozpúšťadla, ktoré by odfarbilo alebo vyhladilo brúsený povrch.",
            ],
        },
    ],
    "table2_heading": "Zmena na moleskine: čo pravdepodobne vidíte",
    "table2_intro": "Povrch mení odtieň podľa smeru a svetla. Diagnózu robte až po suchom očistení a úplnom vyschnutí.",
    "table2_headers": ["Prejav", "Možná príčina", "Bezpečný prvý krok", "Čomu sa vyhnúť"],
    "table2_rows": [
        ("Lesklé kolená", "Maz a mechanicky vyhladený krátky vlas.", "Šetrne odmastiť, vysušiť a jemne prečesať.", "Brúsenie šmirgľom alebo tvrdou kefou."),
        ("Biela tvrdá mapa", "Zvyšok prostriedku alebo minerálov.", "Posúdiť dávku a pri povolení primerane opláchnuť.", "Pridať ďalší koncentrát na suchú plochu."),
        ("Tmavý pruh po daždi", "Zlepený vlas, prenesená nečistota alebo farbivo.", "Vysušiť, prečesať a urobiť skrytú skúšku.", "Horúci fén a krúživé drhnutie."),
        ("Ostrý svetlý lom", "Odrenie, zlomenie povrchových vlákien alebo tlak žehličky.", "Zastaviť trenie a posúdiť poškodenie.", "Opakované mokré čistenie bez zvyšku."),
        ("Žmolky pri stehnách", "Trenie uvoľnených vlákien a prípadná syntetická zložka.", "Znížiť oder a opatrne ošetriť podľa etikety.", "Hlboké holenie nestabilnej plochy."),
    ],
    "steps_heading": "Bezpečný postup prania moleskinového odevu",
    "steps": [
        "Overte, že ide o textilný moleskin, a prečítajte zloženie aj všetky symboly.",
        "Skontrolujte podšívku, elastan, voskovanie, kožené detaily, zipsy a švy.",
        "Mäkkou kefou odstráňte suchý prach a piesok v smere vlasu.",
        "Škvrny rozlíšte podľa pôvodu a vykonajte skúšku farby aj povrchu.",
        "Zapnite kovanie, vyprázdnite vrecká a kus pripravte podľa etikety.",
        "Perte s podobnými farbami a bez ostrých alebo abrazívnych predmetov.",
        "Odmerajte kompatibilný gél a nechajte v bubne priestor na oplach.",
        "Po cykle odev hneď vyberte, urovnajte a otvorte hrubé vrstvy.",
        "Sušte bez bodového tepla a pred uložením overte suchosť švov a vreciek.",
        "Žehlite z rubu cez ochrannú tkaninu s nízkym tlakom a po vychladnutí vlas jemne zjednoťte.",
    ],
    "remember": [
        "Moleskin je názov konštrukcie a úpravy, nie koža ani záruka čistej bavlny.",
        "Prach odstráňte pred vodou, aby nevznikla sivá mapa v krátkom vlase.",
        "Lesk môže byť mechanické vyhladenie, ktoré ďalšie drhnutie zhorší.",
        "Hustý odev potrebuje voľný priestor na oplach a úplné sušenie hrubých miest.",
        "Žehlite z rubu bez silného tlaku na brúsený líc.",
    ],
    "mistakes": [
        "Považovať každý moleskin za stopercentnú bavlnu vhodnú na vysokú teplotu.",
        "Drhnúť lesklé kolená tvrdou kefou alebo brúsnym materiálom.",
        "Prepchať bubon ťažkými nohavicami a zvýšiť dávku bez priestoru na oplach.",
        "Žehliť tmavý líc vysokým tlakom a vytlačiť obrys švov.",
        "Zahriať samolepiaci moleskin na odeve v sušičke alebo pod žehličkou.",
    ],
    "expert_heading": "Odbornejší pohľad: útkové líce, brúsenie a skúšanie oderu",
    "expert": [
        "Technické opisy moleskinu ho spájajú s hustou útkovo dominantnou bavlnenou tkaninou a krátko zastrihnutým brúseným povrchom. Útkové väzné úseky poskytujú plochu pre mechanické dokončenie. Obchodný názov však nezaručuje jednu väzbu ani zloženie; moderný elastický moleskin môže mať odlišnú hustotu a dokončenie.",
        "CottonWorks oddeľuje sueding, sanding, napping, shearing a brushing ako mechanické dokončovacie procesy. Každý mení povrch iným kontaktom a hĺbkou. Starostlivosť má chrániť rovnomernosť vytvoreného vlasu: bodový tlak, abrazívny zips a vysoké teplo môžu zmeniť odraz bez toho, aby sa pretrhla celá tkanina.",
        "ASTM D4966 používa Martindaleov princíp na laboratórne hodnotenie odolnosti textílií proti oderu za definovaných podmienok. Výsledok nemožno priamo preložiť na počet rokov nosenia, pretože koleno, sed, pracie cykly, telesný maz a strih vytvárajú iné kombinácie. Užitočné je odlíšiť hmotnostné alebo vizuálne poškodenie od iba zmeneného smeru vlasu.",
    ],
    "source_intro": "Zdroje podporujú technické rozlíšenie hustej tkaniny a mechanicky vytvoreného krátkeho povrchu aj význam kontrolovaného skúšania oderu. Nepodporujú univerzálnu teplotu pre každý moderný výrobok označený moleskin.",
    "sources": [
        ("CottonWorks: mechanické dokončovanie textílií", COTTONWORKS_FINISHING),
        ("CottonWorks: princípy tkania", COTTONWORKS_WEAVING),
        ("ASTM D4966: odolnosť textílií proti oderu", ASTM_ABRASION),
        ("AATCC TM135: rozmerové zmeny po domácich postupoch", AATCC_DIMENSION),
        ("AATCC TM61: stálofarebnosť pri praní", AATCC_COLOR),
        ("Store norske leksikon: moleskin ako bavlnená tkanina", SNL_MOLESKIN),
        ("EÚ 1007/2011: označovanie textilných vlákien", EU_FIBRE_LABEL),
        ("GINETEX: význam symbolov ošetrovania", GINETEX),
    ],
    "related": [
        ("Čo je bavlna", ARTICLE_COTTON),
        ("Čo je canvas", ARTICLE_CANVAS),
        ("Čo je satén", ARTICLE_SATIN),
        ("Prečo sa oblečenie žmolkuje", ARTICLE_PILLING),
        ("Ako predchádzať zatrhávaniu textilu", ARTICLE_SNAGGING),
        ("Ako čítať štítok", ARTICLE_LABEL),
    ],
    "faq_title": "moleskin nohavice, bundy a brúsený povrch",
    "faq": [
        ("Čo je moleskin?", "Hustá tkanina, tradične bavlnená, s krátko brúseným alebo počesaným povrchom podobným semišu."),
        ("Je moleskin koža?", "Nie. Textilný názov opisuje hladký omak, nie materiál zo zvieraťa."),
        ("Je moleskin vždy bavlna?", "Nie. Moderné výrobky môžu obsahovať elastan alebo syntetické vlákna."),
        ("Aký je rozdiel medzi moleskinom a flanelom?", "Moleskin býva hustejší a má kratší rovnomernejší brúsený povrch; flanel je mäkšie počesaný."),
        ("Na koľko stupňov prať moleskin?", "Jedna teplota neexistuje. Rozhoduje zloženie, dokončenie a etiketa celého odevu."),
        ("Prečo sa moleskin leskne na kolenách?", "Krátky vlas sa trením a mazom vyhladí, takže odráža svetlo súvislejšie."),
        ("Dá sa lesk odstrániť?", "Mastnotu možno šetrne odstrániť, ale mechanicky opotrebovaný vlas sa praním neobnoví."),
        ("Môže ísť moleskin do sušičky?", "Len pri výslovnom symbole; teplo a prevaľovanie môžu zraziť odev a sploštiť povrch."),
        ("Ako žehliť moleskin?", "Z rubu cez ochrannú tkaninu, pri povolenej teplote a s nízkym tlakom."),
        ("Ako odstrániť blato?", "Nechajte ho zaschnúť, odstráňte nasucho a zvyšok riešte kompatibilným lokálnym postupom."),
        ("Prečo je odev po praní tvrdý?", "Môže obsahovať zvyšok produktu alebo minerálov, prípadne sa zmenil smer vlasu či úprava."),
        ("Je zdravotnícky moleskin prateľný textil?", "Nie. Ide o samolepiaci ochranný materiál s iným účelom a návodom."),
        ("Ako moleskin skladovať?", "Čistý a úplne suchý, bez ostrého tlaku na vlas a mimo priameho svetla a vlhkosti."),
    ],
}

add_laundry_cards(
    MOLESKIN,
    "moleskin odev",
    "Gél nie je automaticky vhodný na voskovaný povrch, kožené detaily, nestálofarebný kus ani odev určený na profesionálne čistenie. Neobnoví mechanicky vyhladený vlas ani lesk po horúcom tlaku.",
)


ARTICLES: list[dict[str, object]] = [OXFORD, SYPKOVINA, ABACA, MOLESKIN]


def main() -> None:
    candidate_titles = [
        line.strip()
        for line in CANDIDATES.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    article_titles = [str(article["title"]) for article in ARTICLES]
    if candidate_titles != article_titles:
        raise SystemExit("Candidate titles and article titles differ or are out of order")

    rendered: list[dict[str, object]] = []
    metrics: list[dict[str, object]] = []
    for index, article in enumerate(ARTICLES):
        body = render_article(article)
        visible = visible_text(body)
        one_character_paragraphs = [
            value.strip()
            for value in re.findall(r"<p(?:\s[^>]*)?>(.*?)</p>", body, re.I | re.S)
            if len(visible_text(value).strip()) == 1
        ]
        if FORBIDDEN_PUBLIC_RE.search(visible):
            raise SystemExit(f"Forbidden public wording: {article['title']}")
        if FIXED_PRICE_RE.search(visible):
            raise SystemExit(f"Fixed price found: {article['title']}")
        metric = {
            "title": article["title"],
            "slug": article["link"],
            "characters": len(body),
            "words": len(WORD_RE.findall(visible)),
            "h2": len(re.findall(r"<h2\b", body, re.IGNORECASE)),
            "tables": len(re.findall(r"<table\b", body, re.IGNORECASE)),
            "responsive_tables": len(
                re.findall(r'<div\b[^>]*style="[^"]*overflow-x:\s*auto', body, re.I)
            ),
            "styled_blocks": len(re.findall(r"<div\b[^>]*style=", body, re.I)),
            "action_buttons": len(
                re.findall(r'<a\b[^>]*style="[^"]*display:\s*inline-block', body, re.I)
            ),
            "faq_questions": len(article["faq"]),
            "one_character_paragraphs": len(one_character_paragraphs),
        }
        if metric["words"] < 2800:
            raise SystemExit(f"Article is too short: {article['title']} ({metric['words']} words)")
        if metric["h2"] < 24 or metric["tables"] < 2 or metric["responsive_tables"] != metric["tables"]:
            raise SystemExit(f"Article structure is incomplete: {article['title']} ({metric})")
        if metric["styled_blocks"] < 10 or metric["action_buttons"] < 2 or metric["faq_questions"] < 11 or metric["one_character_paragraphs"]:
            raise SystemExit(f"Article visual blocks are incomplete: {article['title']} ({metric})")
        metrics.append(metric)
        rendered.append(
            {
                "title": article["title"],
                "short": article["short"],
                "long": body,
                "link": article["link"],
                "date_posted": PUBLISH_DATE,
                "time_posted": f"{18 + index:02d}:00:00",
                "commenting": False,
                "title_tag": article["title"],
                "description": article["meta"],
            }
        )

    overlaps: list[dict[str, object]] = []
    for index, left in enumerate(rendered):
        for right in rendered[index + 1 :]:
            score = jaccard(
                seven_word_shingles(visible_text(str(left["long"]))),
                seven_word_shingles(visible_text(str(right["long"]))),
            )
            overlaps.append({"left": left["title"], "right": right["title"], "score": round(score, 4)})
            if score >= 0.13:
                raise SystemExit(
                    f"Article bodies overlap too much: {left['title']} / {right['title']} ({score:.4f})"
                )

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(rendered, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    batch51.OUT_PREFLIGHT = OUT_PREFLIGHT
    report = preflight_links(rendered)
    if report["failure_count"]:
        failed = [check for check in report["checks"] if not check["ok"]]
        print(json.dumps({"failed_links": failed}, ensure_ascii=False, indent=2))
        raise SystemExit("Batch 52 link preflight failed")
    print(
        json.dumps(
            {
                "article_count": len(rendered),
                "metrics": metrics,
                "seven_word_shingle_overlaps": overlaps,
                "link_preflight": True,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
