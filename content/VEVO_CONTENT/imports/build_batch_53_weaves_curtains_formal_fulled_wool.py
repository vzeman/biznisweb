#!/usr/bin/env python3
"""Build and validate VEVO batch 53 textile decision guides."""

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
    seven_word_shingles,
    visible_text,
)
from build_batch_52_distinct_textile_systems import render_article


PUBLISH_DATE = "2026-08-27"
CANDIDATES = Path("content/VEVO_CONTENT/batches/batch-53-candidates-2026-08-27.txt")
OUT_JSON = Path("content/VEVO_CONTENT/imports/batch-53-2026-08-27-articles.json")
OUT_PREFLIGHT = Path("content/VEVO_CONTENT/exports/batch-53-2026-08-27-link-preflight.json")

EU_FIBRE_LABEL = "https://eur-lex.europa.eu/eli/reg/2011/1007/oj"
GINETEX = "https://www.ginetex.net/share/article/4201/care-symbols"
AATCC_DIMENSION = "https://members.aatcc.org/store/tm135/543/"
AATCC_COLOR = "https://members.aatcc.org/store/tm61/495/"
ASTM_ABRASION = "https://store.astm.org/d4966-22r26.html"
ASTM_DISTORTION = "https://store.astm.org/d1336-07r25e01.html"
ASTM_SEAM = "https://store.astm.org/d1683_d1683m-17e01.html"
COTTONWORKS_BROKEN_TWILL = "https://cottonworks.com/encyclopedia-item/broken-twill/"
COTTONWORKS_WEAVES = "https://cottonworks.com/learning-hub/weaving/basic-woven-fabric-designs/"
COTTONWORKS_WEAVING_PDF = "https://cottonworks.com/wp-content/uploads/2023/03/Weaving-101.pdf"
GETTY_VOILE = "https://www.getty.edu/vow/AATFullDisplay?find=&logic=AND&note=&subjectid=300417858"
PRATT_WOVENS = "https://textileresearchlab.pratt.edu/construction/wovens"
LACMA_SILK_TAFFETA = "https://collections.lacma.org/object/130818"
LACMA_BLEND_TAFFETA = "https://collections.lacma.org/object/131310"
MCI_TAFFETA = "https://mci.si.edu/node/1317439"
WOOLMARK_WASHABLE = "https://www.woolmark.com/industry/product-development/processing-innovations/machine-washable-wool/"
WOOLMARK_SYMBOLS = "https://www.woolmark.com/care/washing-instruction-symbols-explained"
WOOLMARK_DRY = "https://www.woolmark.com/care/how-to-dry-wool-sweater/"
IWTO_PROCESSING = "https://iwto.org/wool-supply-chain/chemicals-wool-processing/"
IWTO_DURABILITY = "https://iwto.org/the-science-of-wool-durability/"

ARTICLE_LABEL = "/n/ako-citat-stitok-na-obleceni-material-symboly-prania-a-spravny-program"
ARTICLE_STAIN = "/n/ako-odstranit-zuvacku-krv-vosk-a-ine-skvrny-z-oblecenia"
ARTICLE_COLOR = "/n/stalofarebnost-textilu-preco-farby-blednu-pri-prani-svetle-a-treni"
ARTICLE_SHRINKAGE = "/n/preco-sa-oblecenie-zrazi-po-prani-teplota-vlakna-susicka-a-prevencia"
ARTICLE_ODOR = "/n/preco-moje-oblecenie-zapacha-po-prani-priciny-a-riesenia"
ARTICLE_DRYING = "/n/ako-susit-bielizen-v-malom-byte-bez-zatuchnutia"
ARTICLE_IRONING = "/n/ako-vyzehlit-koselu-kompletny-sprievodca-pre-dokonaly-vysledok"
ARTICLE_PILLING = "/n/preco-sa-oblecenie-zmolkuje-vlakna-trenie-pranie-a-susenie"
ARTICLE_SNAGGING = "/n/zatrhavanie-textilu-preco-vznikaju-vytiahnute-ocka-a-ako-im-predchadzat"
ARTICLE_TWILL = "/n/co-je-keper-alebo-twill-sikma-vazba-odolnost-a-pranie"
ARTICLE_TWEED = "/n/co-je-tvid-hrubsia-vlnena-tkanina-zmolky-a-cistenie"
ARTICLE_CURTAINS = "/n/ako-prat-zaclony"
ARTICLE_CURTAIN_IRON = "/n/ako-spravne-vyzehlit-zaclonu-kompletny-sprievodca"
ARTICLE_CURTAIN_ALLERGY = "/n/ako-prat-zaclony-v-spalni-pri-alergii-na-prach"
ARTICLE_CURTAIN_KITCHEN = "/n/ako-prat-zaclony-v-kuchyni-od-mastnoty-a-pachov"
ARTICLE_CURTAIN_WHITE = "/n/ako-prat-zaclony-aby-zostali-biele-a-vonave"
ARTICLE_CURTAIN_TRACK = "/n/ako-vycistit-garnizu-a-kolajnice-zaclon-prach-mastnota-a-zasekavanie"
ARTICLE_SATIN = "/n/co-je-saten-nie-je-to-vzdy-hodvab-a-ako-ho-spravne-prat"
ARTICLE_FORMAL_DRESS = "/n/ako-prat-spolocenske-saty-doma-a-kedy-zvolit-cistiaren"
ARTICLE_VISCOSE = "/n/co-je-viskoza-vlastnosti-krcivost-zrazanie-a-starostlivost"
ARTICLE_WOOL_COAT = "/n/ako-prat-jesenny-kabat-trenckot-a-lahky-vlneny-kabat-doma"
ARTICLE_WOOL_BLANKET = "/n/ako-prat-vlneny-pled-a-deku-bez-zrazenia"
ARTICLE_WOOL_SWEATER = "/n/ako-prat-vlneny-sveter-ked-zapacha-po-noseni"

LAUNDRY_PRODUCT_NAME = "Prací gél hypoalergénny Vevo Ylang Absolute 1L"
LAUNDRY_PRODUCT_URL = "/p-1627/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l"
LAUNDRY_CATEGORY_NAME = "Pracie gély"
LAUNDRY_CATEGORY_URL = "/c/vevo-home-care/pranie/praci-gel"


def add_cards(
    article: dict[str, object],
    *,
    product_heading: str,
    product_intro: str,
    product_text: str,
    product_limit: str,
    category_heading: str,
    category_intro: str,
    category_text: str,
) -> None:
    article.update(
        {
            "product_heading": product_heading,
            "product_intro": product_intro,
            "product_name": LAUNDRY_PRODUCT_NAME,
            "product_url": LAUNDRY_PRODUCT_URL,
            "product_text": product_text,
            "product_limit": product_limit,
            "category_heading": category_heading,
            "category_intro": category_intro,
            "category_name": LAUNDRY_CATEGORY_NAME,
            "category_url": LAUNDRY_CATEGORY_URL,
            "category_text": category_text,
        }
    )


HERRINGBONE: dict[str, object] = {
    "title": "Čo je rybia kosť: lámaný keper, smer vzoru a správne pranie",
    "link": "co-je-rybia-kost-lamany-keper-smer-vzoru-a-spravne-pranie",
    "meta": "Čo je vzor rybia kosť, ako sa líši od chevronu a obyčajného kepru a ako prať, sušiť, žehliť a vyberať herringbone odevy bez skrútenia a lesku.",
    "short": "Rybia kosť je tkaný vzor odvodený od kepru, pri ktorom sa smer diagonály pravidelne obracia s viditeľným posunom. Nie je to samostatné vlákno ani automaticky vlnený tvíd.",
    "name": "rybia kosť",
    "locative": "rybej kosti",
    "identity_heading": "Rybia kosť opisuje organizáciu keprovej línie, nie vlákno",
    "identity_detail": "Pri typickej rybej kosti sa smer keprovej diagonály v pravidelných pásoch obracia a miesto obratu je odsadené, takže línia pôsobí prerušená podobne ako kostra ryby.",
    "identity_boundary": "Rovnaký efekt možno utkať z bavlny, vlny, polyesteru, viskózy alebo zmesi a môže sa objaviť na košeli, nohaviciach, kabáte, obruse aj čalúnení.",
    "label_focus": "presné zloženie, elastan, podšívku, podlep, vlasový povrch, smer strihu, švy, potlač, farbenie priadze a povolené žehlenie",
    "missing_label": "Pri metráži si vyžiadajte technický list a pred strihaním urobte skúšobné predpranie; pri saku bez etikety neodvodzujte prateľnosť z pevného keprového vzhľadu.",
    "dry_check": "zrkadlové napojenie vzoru, skrútené švy, posunuté nite, vytiahnuté slučky, lesklé hrany, žmolky, rozstrapkaný lem, bubliny výstuže a rozdiel medzi lícom a rubom",
    "damage_boundary": "Prach a škvrnu možno čistiť, no nesprávne zostrihnutý smer, rozídený motív pri šve, teplom vytvorený lesk alebo trvalo skosená tkanina nie sú nečistota.",
    "test_focus": "Skúšku sledujte v oboch smeroch diagonály, pretože trenie, kefovanie a žehlenie môžu na susedných pásoch zmeniť odraz odlišne.",
    "combined_risk": "napučania priadzí, skosenia keprovej konštrukcie, trenia na zlomoch motívu a rozdielneho rozmeru podšívky a vrchnej látky",
    "chemistry_boundary": "Mastný golier, blato, pot a prenesené farbivo potrebujú rozdielny prvý krok; tvrdé lokálne drhnutie môže opticky prerušiť presnú kresbu aj bez straty farby.",
    "drying_detail": "Odev urovnajte podľa osnovy, útku a švov bez násilného napínania šikmých línií; sako podoprite v ramenách a hrubý vlnený kus otvorte pri vreckách, chlopniach a podšívke.",
    "heat_boundary": "Silný tlak môže sploštiť vlnu, vytvoriť lesk na tmavej diagonále, poškodiť elastan, odtlačiť švové rezervy alebo uvoľniť lepenú výstuž.",
    "stop_signs": "silný prenos farby, rastúce skosenie, otváranie šva, vytiahnutá nosná priadza, bubliny podlepenej vrstvy, splstnatené tvrdé miesto alebo lepkavá úprava",
    "professional_boundary": "Prateľnú bavlnenú košeľu možno ošetriť doma podľa etikety, kým vlnené sako, podšitý kabát, čalúnenie a historický herringbone potrebujú postup pre celý výrobok.",
    "answer": "Rybia kosť je vzor odvodený od keprovej väzby. Šikmé línie pravidelne menia smer, pričom pri pravom herringbone je obrat odsadený a línia sa viditeľne láme; chevron sa zbieha do súvislého špicatého písmena V. Názov nehovorí, či je látka bavlnená, vlnená alebo syntetická. Pred praním preto čítajte zloženie a symboly celého výrobku, skontrolujte podšívku, výstuž, smer motívu a poškodené švy. Prateľný kus perte s podobnými farbami a voľným priestorom, bez drsných zipsov. Po cykle ho urovnajte bez ťahania za diagonálu, sušte podľa etikety a žehlite z rubu cez ochrannú tkaninu s nízkym tlakom. Vlnené sako alebo podšitý kabát neperte podľa rady pre bavlnenú košeľu.",
    "intro": "Vzory rybia kosť, herringbone a chevron sa v predaji často zamieňajú a zároveň sa spájajú s predstavou vlny alebo tvídu. V skutočnosti opisujú geometriu väzby, nie jednu surovinu. Rovnakú kresbu môže mať ľahká košeľovina, ťažké nohavice, vlnený kabát aj polyesterový záves. Pri starostlivosti preto treba oddeliť tri otázky: ako sú nite previazané, z akých vlákien sú vyrobené a aké ďalšie vrstvy obsahuje hotový výrobok. Až táto kombinácia určí vodu, pohyb, sušenie a žehlenie.",
    "quick": [
        "<strong>Herringbone nie je chevron:</strong> rybia kosť má v mieste obratu odsadenú, prerušenú diagonálu; chevron vytvára súvislý špic.",
        "<strong>Vzor nie je vlákno:</strong> môže byť bavlnený, vlnený, syntetický aj zmesový.",
        "<strong>Smer strihu je viditeľný:</strong> nesprávne otočený diel alebo vrecko naruší nadväznosť motívu.",
        "<strong>Keper sa môže skosiť:</strong> šikmá väzba a napätie pri dokončení ovplyvňujú smer švov po praní.",
        "<strong>Žehlite s malým tlakom:</strong> tmavé a vlnené pásy sa môžu vyhladiť a začať lesknúť.",
        "<strong>Sako je viac než tkanina:</strong> podšívka, výstuž a tvarovanie často vylučujú domáce pranie.",
    ],
    "overview_heading": "Ako vzniká herringbone a prečo sa diagonála v strede prerušuje",
    "overview": [
        "Keprová väzba vytvára diagonálnu líniu tým, že sa väzné body v každom nasledujúcom riadku posúvajú. Pri rybej kosti sa smer tohto posunu po určitom počte nití obráti. CottonWorks opisuje herringbone ako variant pravostranného kepru, pri ktorom sa navlečenie osnovy pri zmene smeru odsadí. Práve tento posun vytvorí viditeľný zlom namiesto dokonale uzavretého špicu.",
        "Chevron používa tiež obrátenie smeru, ale navlečenie sa vracia k spoločnému bodu a vytvára pravidelné V. V bežnom obchode sa oba názvy môžu použiť voľne, preto je užitočné pozrieť sa lupou na miesto obratu. Rozlíšenie pomáha pri strihaní, nadväzovaní švov a opise výrobku, no samo neurčuje pevnosť, hmotnosť ani povolenie práčky.",
        "Látka môže byť jednofarebná a vzor ukáže odrazom, alebo môže kombinovať dve farby priadzí. Šírku pásov mení počet nití v opakovaní a výsledný dojem mení jemnosť priadze, hustota, plstenie, počesanie aj lisovanie. Preto jemná bavlnená rybia kosť a hrubý vlnený herringbone nie sú dva stupne tej istej údržby.",
    ],
    "table1_heading": "Rybia kosť, chevron, obyčajný keper a tvíd",
    "table1_intro": "Porovnanie opisuje konštrukciu a vzhľad. Program prania musí vždy vychádzať zo zloženia a symbolov hotového výrobku.",
    "table1_headers": ["Označenie", "Princíp vzoru", "Miesto obratu", "Praktický dôsledok"],
    "table1_rows": [
        ("Rybia kosť / herringbone", "Keprová diagonála sa pravidelne obracia.", "Línia je odsadená a vizuálne prerušená.", "Treba kontrolovať smer dielov, napojenie švov a skosenie."),
        ("Chevron", "Diagonály sa zbiehajú a opäť rozbiehajú.", "Vzniká súvislý špic alebo písmeno V.", "Pri strihaní je výrazná stredová os a symetria."),
        ("Jednosmerný keper", "Diagonála pokračuje rovnakým smerom.", "Bez pravidelného obratu.", "Sledujte pravý alebo ľavý smer a možné skosenie."),
        ("Tvíd", "Rodina textúrovaných tkanín, často vlnených.", "Môže, ale nemusí používať rybiu kosť.", "Vláknový povrch a podšívka sú dôležitejšie než samotný motív."),
        ("Potlač rybej kosti", "Motív je nanesený farbou na inú základnú väzbu.", "Zlom je iba grafický.", "Údržbu určuje podklad a stálofarebnosť tlače."),
    ],
    "sections": [
        {
            "heading": "Ako rozoznať pravú rybiu kosť od potlače a chevronu",
            "paragraphs": [
                f"Položte látku na svetlý podklad a pozrite sa na ňu lupou z líca aj rubu. Pri tkanej rybej kosti budú väzné body meniť smer a kresba zostane čitateľná aj na rube, hoci môže mať opačný odraz. Základ jednosmernej diagonály vysvetľuje článok <a href=\"{ARTICLE_TWILL}\">čo je keper alebo twill</a>. Pri potlači sa farba môže nachádzať najmä na líci bez zodpovedajúcej zmeny väzby.",
                "V mieste obratu sledujte, či sa šikmé línie stretávajú v čistom bode, alebo či je jedna vetva oproti druhej posunutá. Čistý bod smeruje k chevronu, odsadený zlom k herringbone. Nevyťahujte niť z hotového odevu a nerobte skúšku horením. Zloženie, elastan a povrchovú úpravu spoľahlivo potvrdzuje iba etiketa alebo technický list.",
            ],
        },
        {
            "heading": "Smer vzoru pri košeli, nohaviciach, saku a čalúnení",
            "paragraphs": [
                "Na hotovom odeve porovnajte stred predného dielu, golier, vrecká, chlopne a bočné švy. Kvalitné napojenie neznamená, že každý zlom musí byť neviditeľný, pretože strih má záševky a zakrivenie, ale zrkadlovo otočený jediný diel môže pôsobiť rušivo. Pri kúpe si motív prezrite postojačky aj v pohybe, keď sa látka ohýba cez koleno alebo lakeť.",
                "Na sedačke alebo závesoch je dôležitá orientácia veľkých plôch. Slnečné svetlo a trenie môžu zvýrazniť jednu vetvu viac než druhú. Otočený vankúš preto môže vyzerať farebne odlišne, hoci má rovnakú priadzu. Pred čistením si smer označte fotografiou a kefujte či vysávajte konzistentne, aby sa na susedných dieloch nevytvoril opačný povrchový odraz.",
            ],
        },
        {
            "heading": "Prečo sa herringbone môže po praní skosiť alebo skrútiť",
            "paragraphs": [
                f"Keprové tkaniny môžu niesť vnútorné napätie z tkania a dokončenia. Pri navlhčení a sušení sa uvoľní a prejaví ako zmena uhla, skrútenie nohavice alebo posun bočného šva. Mechanizmus rozmerovej zmeny dopĺňa článok <a href=\"{ARTICLE_SHRINKAGE}\">prečo sa oblečenie po praní zráža</a>. Rybia kosť môže časť vizuálneho skosenia vyvažovať, ale nezaručuje nulovú deformáciu.",
                "Nový kus pred prvým praním odmerajte medzi pevnými bodmi a odfoťte polohu švov. Po cykle ho neťahajte za jednu diagonálu, kým je mokrý. Urovnajte ho podľa osnovy, útku a konštrukčných línií a zmenu posudzujte až úplne suchú. Ak sa nový odev výrazne skrúti napriek dodržaniu etikety, výsledok zdokumentujte pre predajcu.",
            ],
        },
        {
            "heading": "Ako prať bavlnenú košeľu alebo nohavice so vzorom rybej kosti",
            "paragraphs": [
                f"Najprv použite <a href=\"{ARTICLE_LABEL}\">návod na čítanie ošetrovacieho štítka</a>. Skontrolujte golier, manžety, elastan, výšivku, gombíky a vrecká. Farebný kus oddeľte podľa stálofarebnosti a obráťte naruby, ak to konštrukcia dovoľuje. Zipsy iných odevov zapnite alebo ich z dávky odstráňte, aby sa nezachytili o diagonálne väzné úseky.",
                "Bubon naplňte tak, aby sa odev mohol rozvinúť a opláchnuť, ale nepoužívajte zbytočne prudký program pre jediný ľahký kus. Prostriedok odmerajte podľa vody, hmotnosti a znečistenia; koncentrát nelejte na suchý farebný motív. Po cykle odev ihneď vyberte, urovnajte švy a nechajte schnúť bez ostrého preloženia cez jednu vetvu vzoru.",
            ],
        },
        {
            "heading": "Vlnená rybia kosť, tvíd a podšité sako",
            "paragraphs": [
                f"Rybia kosť sa často spája s vlneným tvídom, ale <a href=\"{ARTICLE_TWEED}\">tvíd je širšia rodina textúrovaných tkanín</a> a môže mať aj inú väzbu. Vlnený povrch zachytáva prach a pri trení môže tvoriť žmolky alebo sa zhutniť. Medzi noseniami preto najprv vetrajte a používajte čistú mäkkú odevnú kefu v smere látky, nie náhodné mokré čistenie celej plochy.",
                "Sako obsahuje podšívku, výstuž chlopní, ramenné diely, nite a tvarovanie parou. Aj keby samotná vlnená metráž zniesla kontrolované ručné pranie, hotový odev sa môže zvlniť alebo stratiť architektúru. Symbol profesionálneho čistenia rešpektujte. Lokálnu škvrnu čistiarni presne opíšte a neprežehľujte ju pred odovzdaním.",
            ],
        },
        {
            "heading": "Škvrny na rybej kosti: vzor môže problém skryť aj zvýrazniť",
            "paragraphs": [
                f"Najprv odstráňte prebytok bez rozotierania a určte pôvod podľa <a href=\"{ARTICLE_STAIN}\">sprievodcu rôznymi škvrnami</a>. Mastnota môže stmaviť jednotlivé diagonály, zatiaľ čo zaschnutý prostriedok vytvorí svetlý okraj. Pri dvojfarebnej tkanine skúšajte obe priadze a šev, pretože nemusia mať rovnakú stálosť ani povrchovú úpravu.",
                "Pracujte od okraja ku stredu a miesto navlhčite rovnomerne iba v rozsahu, ktorý dokážete opláchnuť alebo odsať. Tvrdá kefka prechádzajúca naprieč zlomom môže zdrsniť jednu vetvu a vyhladiť druhú. Výsledok posudzujte až suchý z viacerých uhlov; meniaci sa lesk môže byť mechanická zmena, nie zvyšná škvrna.",
            ],
        },
        {
            "heading": "Ako sušiť herringbone bez vytiahnutia a ostrých lomov",
            "paragraphs": [
                f"Mokrý kus podoprite a rozložte jeho hmotnosť. Košeľu možno sušiť na primeranom vešiaku, nohavice podľa etikety za pás alebo naplocho a vlnený úplet naplocho. V interiéri zabezpečte prúdenie vzduchu podľa návodu <a href=\"{ARTICLE_DRYING}\">ako sušiť bielizeň bez zatuchnutia</a>. Vrecká, manžety a podšívku otvorte, aby nezostali vlhké.",
                "Sušičku používajte iba pri výslovnom symbole. Prevaľovanie zvyšuje trenie vystupujúcich bodov a teplo môže zmeniť elastan, vlnu, podšívku aj výstuž. Odev neprehadzujte cez úzku hranu presne v jednom zlome motívu a na viditeľnú plochu nedávajte kolík. Tvar upravujte jemne, bez ťahania šikmým smerom.",
            ],
        },
        {
            "heading": "Ako žehliť rybiu kosť bez lesklých pásov",
            "paragraphs": [
                f"Žehlenie začnite z rubu pri teplote najcitlivejšej zložky. Praktické poradie dielov približuje <a href=\"{ARTICLE_IRONING}\">návod na žehlenie košele</a>, no pri rybej kosti navyše používajte ochrannú tkaninu a malý tlak. Švové rezervy podložte, aby sa ich obrys nepretlačil cez tmavé a svetlé diagonály.",
                "Vlnený povrch skôr naparujte s odstupom a tvarujte podľa pokynu výrobcu než silno pritláčajte žehličku. Skúšobné miesto nechajte vychladnúť a pozrite pri bočnom svetle. Ak sa objaví hladký lesklý pás alebo rozdielny odraz v jednej vetve, ďalšie teplo nepridávajte. Na podlepené sako nepoužívajte paru ako pokus o opravu bublín.",
            ],
        },
        {
            "heading": "Zatrhnutie, rozostúpenie pri šve a rozstrapkaný okraj",
            "paragraphs": [
                f"Vytiahnutú slučku neodstrihujte skôr, než zistíte jej úlohu. <a href=\"{ARTICLE_SNAGGING}\">Návod pri zatrhnutí textilu</a> vysvetľuje rozdiel medzi povrchovou slučkou a porušením nosnej priadze. Rybia kosť môže vďaka zmene smeru skryť malý posun, no pri napnutí sa chyba otvorí. Odev prestaňte zaťažovať a šev skontrolujte z rubu.",
                "Strihaný okraj keprovej tkaniny sa môže strapkať, ak nie je správne začistený. Pri metráži skúšajte švovú rezervu, steh a zažehlenie na odstrižku, nie na hotovom prednom diele. Lepidlo nanesené z líca vytvorí tvrdú tmavú mapu. Pri drahom saku alebo čalúnení je vhodnejšia krajčírska oprava, ktorá rozloží napätie.",
            ],
        },
        {
            "heading": "Ako vybrať herringbone metráž alebo hotový odev",
            "paragraphs": [
                "Pri metráži si vypýtajte zloženie, plošnú hmotnosť, šírku, odporúčané predpranie, rozmerovú zmenu a smer vzoru. Skontrolujte, či máte rezervu na napájanie pruhov a či sú okraje rovné voči osnove. Výrazný veľký motív spotrebuje viac látky než drobná rybia kosť, pretože diely treba posúvať pre nadväznosť.",
                "Pri hotovom odeve porovnajte symetriu, polohu vreciek, skrútenie nohavíc a spôsob čistenia. Pevný omak v predajni nemusí znamenať odolnosť proti žmolkom ani dobrú rozmerovú stabilitu. Užitočnejšia je jasná etiketa, rovné švy, vhodná podšívka a možnosť opravy. Luxusný názov bez údajov nehovorí, ako sa výrobok zmení po vode a teple.",
            ],
        },
    ],
    "table2_heading": "Rybia kosť po praní alebo nosení: čo pravdepodobne vidíte",
    "table2_intro": "Odev posudzujte úplne suchý pri rovnakom svetle a bez napínania. Vzor môže opticky zväčšiť aj skryť zmenu.",
    "table2_headers": ["Prejav", "Pravdepodobná príčina", "Čo overiť", "Bezpečný ďalší krok"],
    "table2_rows": [
        ("Bočný šev sa stáča", "Skosenie tkaniny, vnútorné napätie alebo rozdielne zrazenie.", "Polohu šva pred praním, smer osnovy a zmenu rozmeru.", "Nenapínať mokrý; zdokumentovať a pri novom kuse riešiť s predajcom."),
        ("Jedna diagonála je lesklejšia", "Tlak žehličky, trenie alebo zmena smeru povrchu.", "Rub, bočné svetlo a omak bez ďalšieho drhnutia.", "Znížiť tlak a teplo; mechanický lesk neprať opakovane."),
        ("Pri šve vznikla svetlá medzera", "Posun nití alebo poškodený steh.", "Či sú priadze celé a či sa otvor pri ťahu zväčšuje.", "Prestať nosiť a dať šev odborne stabilizovať."),
        ("Motív na vrecku nenadväzuje", "Smer alebo poloha dielu pri strihaní.", "Či ide o konštrukčný zámer alebo chybu výroby.", "Čistenie nepomôže; pri novom kuse posúdiť reklamáciu."),
        ("Vlnené miesto je tvrdé", "Lokálne splstnatenie kombináciou vlhkosti, tepla a trenia.", "Hrúbku, omak a priechodnosť väzby.", "Zastaviť pohyb a nečesať silou; zvoliť odborné posúdenie."),
    ],
    "steps_heading": "Bezpečný postup pre prateľný herringbone odev",
    "steps": [
        "Určte, či ide o rybiu kosť, chevron alebo iba potlač, a odfoťte smer motívu.",
        "Prečítajte zloženie, symboly, podšívku, elastan, výstuž a ozdobné prvky.",
        "Skontrolujte švy, vytiahnuté nite, žmolky, škvrny a predchádzajúci lesk.",
        "Otestujte farbu a lokálny prostriedok na skrytom mieste vrátane úplného vyschnutia.",
        "Odev oddeľte od zipsov, suchých zipsov, hrubých uterákov a nestálofarebných kusov.",
        "Použite povolený cyklus, presnú dávku a dostatok priestoru na pohyb aj oplach.",
        "Po cykle kus hneď podoprite, urovnajte švy a nenapínajte ho za diagonálu.",
        "Sušte podľa etikety s prúdením vzduchu a bez prudkého lokálneho tepla.",
        "Žehlite z rubu cez ochrannú tkaninu a výsledok kontrolujte pri bočnom svetle.",
        "Úplne suchý odev uložte bez ostrého lomu a pravidelne kontrolujte švy a povrch.",
    ],
    "remember": [
        "Je zlom diagonály odsadený ako rybia kosť, alebo sa zbieha do čistého chevronu?",
        "Aké vlákno, podšívka, elastan a výstuž určujú najnižší limit?",
        "Sú švy rovné a motív správne orientovaný ešte pred praním?",
        "Je svetlé miesto škvrna, zvyšok produktu, oder alebo lesk po tlaku?",
        "Má odev pri praní priestor a pri sušení rovnomernú oporu?",
        "Je sako vôbec určené na domáce pranie ako celý výrobok?",
    ],
    "mistakes": [
        "Považovať každý herringbone za vlnený tvíd alebo za chevron.",
        "Vyprať podšité sako podľa vlastnosti vrchnej metráže.",
        "Drhnúť škvrnu naprieč motívom tvrdou kefou.",
        "Napínať skrútený mokrý šev za šikmú líniu.",
        "Žehliť tmavú rybiu kosť vysokým tlakom priamo z líca.",
        "Odstrihnúť vytiahnutú priadzu bez kontroly väzby a rubu.",
    ],
    "expert_heading": "Odbornejší pohľad: lámaný keper, skosenie a hodnotenie tkaniny",
    "expert": [
        "CottonWorks zaraďuje herringbone medzi broken twills a pri porovnaní s chevronom ukazuje rozdiel v navlečení osnovy: pri herringbone sa zmena smeru odsadí, pri chevrone sa vracia k bodu. Tento rozdiel je konštrukčný. Obchodný názov však nepredpisuje pomer väzby, hustotu, vlákno ani dokončenie konkrétneho výrobku.",
        "ASTM D1336 hodnotí deformácie typu bow a skew za definovaných podmienok, ASTM D1683 správanie šitého šva a ASTM D4966 oder plochej vzorky. Ide o tri rôzne vlastnosti. Výsledok oderu nepredpovedá skrútenie nohavice a pevnosť tkaniny sama nezaručuje správne napojenie motívu alebo stabilnú výstuž saka.",
        "AATCC TM61 sleduje stálofarebnosť pri definovanom zrýchlenom praní a TM135 rozmerové zmeny po konkrétnych domácich postupoch. Bez metódy, počtu cyklov, orientácie vzorky a spôsobu sušenia nie je číslo porovnateľné. Pre spotrebiteľa ostáva rozhodujúca etiketa hotového odevu a zdokumentovaná zmena po dodržanom postupe.",
    ],
    "source_intro": "Zdroje podporujú rozlíšenie rybej kosti, chevronu a obyčajného kepru aj samostatné hodnotenie skosenia, šva, oderu, farby a rozmeru. Nepodporujú jednu univerzálnu teplotu pre každý herringbone výrobok.",
    "sources": [
        ("CottonWorks: broken twill a herringbone", COTTONWORKS_BROKEN_TWILL),
        ("CottonWorks: herringbone verzus chevron", COTTONWORKS_WEAVES),
        ("CottonWorks: Weaving 101", COTTONWORKS_WEAVING_PDF),
        ("ASTM D1336: bow a skew v tkaninách", ASTM_DISTORTION),
        ("ASTM D1683: zlyhanie šitých švov", ASTM_SEAM),
        ("ASTM D4966: oder metódou Martindale", ASTM_ABRASION),
        ("AATCC TM61: stálofarebnosť pri praní", AATCC_COLOR),
        ("AATCC TM135: rozmerové zmeny po praní", AATCC_DIMENSION),
        ("EÚ 1007/2011: označovanie textilných vlákien", EU_FIBRE_LABEL),
        ("GINETEX: význam symbolov ošetrovania", GINETEX),
    ],
    "related": [
        ("Čo je keper alebo twill", ARTICLE_TWILL),
        ("Čo je tvíd a ako sa oň starať", ARTICLE_TWEED),
        ("Ako čítať štítok na oblečení", ARTICLE_LABEL),
        ("Prečo sa oblečenie po praní zráža", ARTICLE_SHRINKAGE),
        ("Prečo farby blednú pri praní a trení", ARTICLE_COLOR),
        ("Ako riešiť zatrhnutú priadzu", ARTICLE_SNAGGING),
    ],
    "faq_title": "rybia kosť, herringbone a chevron",
    "faq": [
        ("Čo je rybia kosť?", "Tkaný vzor odvodený od kepru, pri ktorom sa smer diagonály pravidelne obracia a v mieste zmeny je odsadený."),
        ("Je herringbone to isté ako rybia kosť?", "Áno, herringbone je anglický názov typickej rybej kosti."),
        ("Je rybia kosť to isté ako chevron?", "Nie úplne. Chevron sa zbieha do čistého špicu, rybia kosť má diagonálu v obrate odsadenú."),
        ("Je rybia kosť vždy vlnená?", "Nie. Vzor možno utkať z bavlny, vlny, polyesteru, viskózy aj zmesí."),
        ("Je každý herringbone tvíd?", "Nie. Tvíd je širšia rodina textúrovaných tkanín a rybia kosť je iba jeden možný vzor."),
        ("Môže sa herringbone prať v práčke?", "Iba ak to povoľuje etiketa celého výrobku vrátane podšívky, výstuže a ozdôb."),
        ("Na koľko stupňov prať rybiu kosť?", "Jedna teplota neexistuje. Rozhoduje vláknové zloženie, farbenie, dokončenie a symbol konkrétneho kusu."),
        ("Prečo sa nohavica po praní skrútila?", "Príčinou môže byť skosenie keprovej tkaniny, vnútorné napätie alebo rozdielna rozmerová zmena vrstiev."),
        ("Ako žehliť herringbone?", "Z rubu, cez ochrannú tkaninu, pri povolenej teplote a s nízkym tlakom, najmä pri tmavej alebo vlnenej látke."),
        ("Ako odstrániť lesk na rybej kosti?", "Najprv odlíšte mastnotu od mechanicky vyhladeného povrchu. Trvalý lesk po tlaku sa ďalším praním nemusí obnoviť."),
        ("Čo s vytiahnutou niťou?", "Neodstrihujte ju naslepo. Odev položte, skontrolujte rub a pri nosnej priadzi zvoľte odbornú opravu."),
        ("Ako skladovať vlnené sako s rybou kosťou?", "Čisté a úplne suché na širokom vešiaku, mimo svetla, vlhkosti a potravinových zvyškov, s kontrolou škodcov."),
        ("Ako vyberať metráž s veľkou rybou kosťou?", "Počítajte s rezervou na napájanie motívu, overte smer osnovy, rozmerovú zmenu a odporúčané predpranie."),
    ],
}

add_cards(
    HERRINGBONE,
    product_heading="Prací gél použite iba na prateľný herringbone odev",
    product_intro="Ak etiketa celej košele, nohavíc alebo iného bežného kusu povoľuje domáce pranie, tekutý gél možno presne odmerať a rovnomerne opláchnuť.",
    product_text="Gél použite podľa tvrdosti vody, veľkosti náplne a znečistenia. Nenalievajte ho priamo na suchú farebnú diagonálu, mastný golier ani vytiahnutú priadzu.",
    product_limit="Nie je automaticky vhodný na vlnené sako, hodvábnu zmes, lepenú výstuž ani kus určený na profesionálne čistenie. Neopraví skosenie, zlé napojenie vzoru alebo lesk po žehlení.",
    category_heading="Prostriedok vyberajte podľa vlákna, nie podľa vzoru",
    category_intro="Dve látky s rovnakou rybou kosťou môžu mať úplne iné zloženie a ošetrovací limit. Kategóriu preto prezerajte až po kontrole etikety.",
    category_text="V kategórii nájdete pracie gély pre rôzne potreby bežnej prateľnej bielizne. Pri vlne, hodvábe a štruktúrovanom odeve rešpektujte špecializovaný pokyn výrobcu.",
)


VOILE: dict[str, object] = {
    "title": "Čo je voálová záclona: priesvitná tkanina, prach a pranie bez pokrčenia",
    "link": "co-je-voalova-zaclona-priesvitna-tkanina-prach-a-pranie-bez-pokrcenia",
    "meta": "Čo je voálová záclona, ako sa líši od organzy a sieťoviny a ako ju vyprať, vysušiť a prípadne vyžehliť bez zašednutia, pokrčenia a zatrhnutia.",
    "short": "Voálová záclona je ľahká priesvitná tkanina, najčastejšie v plátnovej väzbe. Názov neurčuje vlákno: moderný voál býva často polyesterový, no existujú aj bavlnené, hodvábne a zmesové varianty.",
    "name": "voálová záclona",
    "locative": "voálovej záclone",
    "identity_heading": "Voál opisuje ľahkú priesvitnú tkaninu, nie jedno vlákno",
    "identity_detail": "Typický voál má jednoduchú plátnovú väzbu, jemné priadze a dostatok otvorenosti na rozptýlenie svetla, pričom je spravidla mäkší a splývavejší než tuhá organza.",
    "identity_boundary": "Polyesterový záclonový voál, bavlnený odevný voál a historický hodvábny voile môžu vyzerať príbuzne, ale reagujú odlišne na vodu, bielenie, teplo a žehlenie.",
    "label_focus": "vláknové zloženie, farebný alebo biely variant, riasiacu pásku, háčiky, krúžky, olovko alebo inú záťaž, výšivku, potlač, lem a povolené odstreďovanie a žehlenie",
    "missing_label": "Pri záclone na mieru si vyžiadajte údaje k metráži, páske aj záťažovému lemu; bez nich nezačínajte vysokou teplotou alebo bielidlom iba preto, že látka je biela.",
    "dry_check": "uvoľnené háčiky, ostré krúžky, poškodenú riasiacu pásku, rozostúpené švy, zatrhnuté nite, prach pri hornom leme, mastnotu pri kuchynskom okne, žlté okraje a rozdielnu dĺžku dielov",
    "damage_boundary": "Prach, dymový povlak alebo mastnotu možno čistiť, no pretrhnutá priadza, vytiahnutý šev, teplom zvlnený polyester a slnkom zoslabnutý pás nie sú škvrny.",
    "test_focus": "Na bielej záclone sledujte po vyschnutí žltnutie, sivý krúžok a zmenu priehľadnosti; na farebnej navyše prenos farbiva z lemu, pásky a výšivky.",
    "combined_risk": "zachytenia jemnej väzby o kovanie, stlačenia veľkej plochy v malom bubne, nerovnomerného oplachu v záhyboch a tepelnej deformácie syntetickej priadze",
    "chemistry_boundary": "Prach, kuchynský tuk, nikotínový povlak, plesňová mapa a prirodzené starnutie potrebujú rozdielne riešenia; silné bielenie neobnoví oslabené vlákno ani pôvodný optický odtieň každej zmesi.",
    "drying_detail": "Záclonu rozložte cez celú šírku alebo ju zaveste podľa etikety tak, aby horná páska, sklady, bočné lemy a spodná záťaž mali prístup vzduchu a neniesli mokrú hmotnosť v jednom bode.",
    "heat_boundary": "Horúca sušička, radiátor alebo žehlička môže polyester zvlniť či zataviť, bavlnený voál zraziť, výšivku zmeniť a plastové háčiky deformovať.",
    "stop_signs": "rastúce zatrhnutie, trhajúci sa horný lem, silný prenos farby, zvlnenie pri nízkom teple, tvrdnúca mapa, zápach z nedoschnutých záhybov alebo známky plesne v podklade a stene",
    "professional_boundary": "Bežnú polyesterovú záclonu možno často prať doma podľa etikety, no hodvábny voál, starý slnkom oslabený diel, kombinácia s podšívkou alebo rozmerná dekorácia môže vyžadovať odborné čistenie.",
    "answer": "Voálová záclona je ľahká priesvitná tkanina, najčastejšie utkaná jednoduchou plátnovou väzbou. Moderné záclony bývajú často polyesterové, ale názov voál nezaručuje jedno zloženie ani jednu teplotu. Pred praním odstráňte alebo bezpečne zaistite háčiky a kovanie, záclonu jemne vyprášte alebo povysávajte s nízkym výkonom, skontrolujte hornú pásku, lemy a zatrhnutia a prečítajte etiketu. Perte ju samostatne alebo s podobne ľahkými hladkými textíliami, v nepreplnenom bubne, s nízkou mechanikou a presnou dávkou. Po cykle ju hneď vyberte, bez krútenia urovnajte a pri povolení zaveste ešte rovnomerne vlhkú. Žehličku používajte iba vtedy, keď záhyby ostanú, a vždy pri teplote najcitlivejšej zložky. Bielidlo nevyberajte iba podľa bielej farby.",
    "intro": "Pri voálových záclonách sa stretávajú dve protichodné rady: jedni ich skladajú do vrecka, druhí ich perú voľne; jedni ich nikdy nežehlia, iní bez žehličky nevedia odstrániť lomy. Obe skúsenosti môžu byť pravdivé pre odlišnú metráž, riasiacu pásku, veľkosť bubna a odstreďovanie. Voál navyše zbiera prach z veľkej plochy a pri okne reaguje na slnko, kuchynské výpary aj vykurovanie. Bezpečný postup preto nevyberá univerzálny trik, ale posudzuje zloženie, kovanie, znečistenie, množstvo látky, oplach a spôsob zavesenia.",
    "quick": [
        "<strong>Voál nie je vždy polyester:</strong> zloženie prečítajte na etikete alebo v údajoch k metráži.",
        "<strong>Prach odstráňte pred vodou:</strong> menej usadenín sa zmení na sivú suspenziu v záhyboch.",
        "<strong>Háčiky a závažia skontrolujte:</strong> ostrý alebo voľný prvok môže roztrhnúť jemnú plochu.",
        "<strong>Bubon neprepĺňajte:</strong> veľká záclona potrebuje vodu medzi vrstvami a priestor na oplach.",
        "<strong>Vlhké zavesenie často pomôže:</strong> vlastná hmotnosť vyrovná ľahké lomy, nie však každú pokrčenú metráž.",
        "<strong>Biela farba nie je povolenie na chlór:</strong> vlákno, výšivka, páska a úprava môžu mať nižší limit.",
    ],
    "overview_heading": "Ako je voál utkaný a prečo prepúšťa svetlo",
    "overview": [
        "Voál sa tradične opisuje ako ľahká priesvitná tkanina v plátnovej väzbe. Každá osnovná niť sa pravidelne kríži s útkom, no jemné priadze a ich rozostup nechávajú medzi väznými bodmi dostatok priestoru na priechod svetla. Povrch preto nepôsobí ako hustý popelín ani ako tkanina s výraznou diagonálou. Splývavosť však ovplyvňuje vlákno, zákrut, hustota a dokončenie.",
        "Getty Art & Architecture Thesaurus opisuje voál ako tenkú, ľahkú, priesvitnú alebo polopriesvitnú tkaninu z bavlny, hodvábu, vlny či syntetických vlákien. Pratt Textile Research Lab ho zaraďuje medzi príklady plátnovej väzby. Z týchto znakov však nemožno odvodiť zloženie konkrétnej záclony. Bavlnený, hodvábny a polyesterový voál potrebujú rozdielne teplo a chémiu.",
        "Hotová záclona obsahuje viac než metráž. Riasiaca páska môže byť hustejšia, spodný lem môže niesť záťaž, okraje môžu mať výšivku a zavesenie plastové či kovové prvky. Práve tieto miesta sa pri praní zachytávajú, schnú pomalšie a menia rozmer inak než priesvitná plocha. Ošetrovací symbol sa preto musí vzťahovať na celý hotový diel.",
    ],
    "table1_heading": "Voál, organza, sieťovina, tyl a závesovina",
    "table1_intro": "Pojmy sa pri predaji záclon miešajú. Porovnanie pomáha určiť stavbu, no teplotu a spôsob čistenia naďalej určuje konkrétny výrobok.",
    "table1_headers": ["Materiál alebo konštrukcia", "Typický povrch", "Splývavosť", "Dôležité pri údržbe"],
    "table1_rows": [
        ("Voál", "Jemná priesvitná plátnová tkanina.", "Mäkká až stredne splývavá podľa vlákna a úpravy.", "Prach, záhyby, háčiky, nízka mechanika a opatrné teplo."),
        ("Organza", "Veľmi priesvitná, hladká a zvyčajne tuhšia.", "Drží objem a ostré línie.", "Citlivosť na lomy, zachytenie a teplo podľa vlákna."),
        ("Tyl alebo sieťovina", "Otvory tvoria slučky alebo sieťovú štruktúru.", "Od mäkkej po tuhú.", "Háčiky sa môžu zachytiť priamo v otvoroch."),
        ("Žakárová záclona", "Vzor vzniká zložitejšou väzbou a hustotou plôch.", "Nerovnomerná podľa motívu.", "Rôzne zóny môžu držať vodu a napätie odlišne."),
        ("Hustý záves", "Nepriesvitná alebo zatemňovacia viacvrstvová látka.", "Ťažšia, často podšitá alebo povrchovo upravená.", "Nedá sa automaticky prať rovnakým postupom ako ľahký voál."),
    ],
    "sections": [
        {
            "heading": "Čo urobiť pred zvesením voálovej záclony",
            "paragraphs": [
                f"Odfoťte spôsob riasenia, počet háčikov a polohu dielov. Potom odstráňte prach z garniže a koľajnice podľa návodu <a href=\"{ARTICLE_CURTAIN_TRACK}\">ako vyčistiť závesný systém</a>, aby sa čistá záclona nevrátila na mastný alebo zaprášený podklad. Pri práci vo výške používajte stabilnú oporu a záclonu nesťahujte prudko za jeden kraj.",
                "Háčiky, krúžky a spony vyberte, ak to konštrukcia umožňuje. Ak ich výrobca prikazuje ponechať, bezpečne ich uzavrite v ochrannom riešení bez ostrých výstupkov. Spodnú záťaž, olovko alebo reťaz kontrolujte osobitne; poškodený lem sa pri mokrej hmotnosti môže otvoriť. Každé zatrhnutie a uvoľnený šev opravte pred cyklom.",
            ],
        },
        {
            "heading": "Prach na voáli: prečo ho najprv odstrániť nasucho",
            "paragraphs": [
                "Záclona filtruje pohybujúci sa vzduch a na veľkej ploche zachytáva jemný prach, peľ, sadze a aerosóly z domácnosti. Pri okamžitom namočení sa voľné častice rozptýlia vo vode a môžu sa zachytiť v riasiacej páske alebo hustých záhyboch. Záclonu preto vonku jemne pretrepte alebo použite nízke sanie cez čistú ochrannú sieťku, ak je tkanina pevná.",
                f"V spálni alergika je dôležitá aj bezpečná manipulácia s prachom; podrobnejšie ju rieši článok <a href=\"{ARTICLE_CURTAIN_ALLERGY}\">ako prať záclony pri alergii na prach</a>. Silné vysávanie priamo na oslabenú plochu môže vytiahnuť niť. Pri starom slnkom krehkom diele pracujte po malých úsekoch a ak sa vlákna uvoľňujú, mokré čistenie zastavte.",
            ],
        },
        {
            "heading": "Ako prať voálovú záclonu v práčke",
            "paragraphs": [
                f"Zloženie a symboly vyhodnoťte podľa <a href=\"{ARTICLE_LABEL}\">návodu na čítanie etikety</a>. Záclonu vložte voľne alebo podľa odporúčania do dostatočne veľkého pracieho vaku; cieľom nie je natlačiť ju do malej gule. Perte samostatne alebo s podobne ľahkými hladkými dielmi. Uteráky, zipsy a háčiky zvyšujú oder a zachytenie.",
                "Zvoľte teplotu, cyklus a odstreďovanie z etikety. Pri veľmi veľkej ploche môže byť domáci bubon objemovo nevhodný aj vtedy, keď suchá záclona váži málo. Voda musí prejsť medzi záhyby a oplach odstrániť uvoľnený prach aj prostriedok. Nadbytočná dávka nepomôže zašednutej látke, iba zvýši riziko tuhých máp.",
            ],
        },
        {
            "heading": "Prací vak, obliečka alebo voľné pranie: čo chráni a čo môže zhoršiť",
            "paragraphs": [
                "Prací vak môže oddeliť jemnú záclonu od bubna a zachytiť zvyšný háčik, no musí byť dostatočne veľký. Keď je látka pevne stlačená, voda a oplach nepreniknú rovnomerne cez všetky sklady. Malá obliečka nie je automaticky vhodnejšia: hustý materiál môže obmedziť výmenu vody a farbivo z obliečky sa môže preniesť.",
                "Voľné pranie dáva ploche viac priestoru, ale vyžaduje čistý bubon bez poškodenia a dávku bez ostrých prvkov. Rozhodujte podľa veľkosti záclony, stavu švov, prítomnosti kovania a návodu výrobcu. Ochranná pomôcka nemôže zrušiť nevhodne prudký program, preplnenie ani otvorený háčik.",
            ],
        },
        {
            "heading": "Ako vyprať sivý alebo zažltnutý voál bez slepého bielenia",
            "paragraphs": [
                f"Najprv rozlíšte voľný prach, mastný film, usadeninu z vody, nikotín, zmenu optického zjasnenia a starnutie vlákna. Všeobecný postup pre bielu záclonu nájdete v článku <a href=\"{ARTICLE_CURTAIN_WHITE}\">ako prať záclony, aby zostali biele</a>. Pri voáli navyše overte zloženie, výšivku, pásku a farebnú niť. Biela plocha môže obsahovať syntetiku citlivú na teplo aj chémiu.",
                "Použite iba bielidlo povolené symbolom a návodom výrobcu produktu. Nemiešajte chlórové prípravky s kyselinami, amoniakom ani inými čističmi. Výrazne oslabené, slnkom skrehnuté alebo farebne nerovnomerné miesto neobnovíte vyššou koncentráciou. Skúšku nechajte úplne vyschnúť, pretože mokrá priesvitná látka vyzerá tmavšia a odtieň sa hodnotí nespoľahlivo.",
            ],
        },
        {
            "heading": "Kuchynská mastnota, pach a škvrny pri okne",
            "paragraphs": [
                f"Kuchynský voál zachytáva aerosól tuku, ktorý na seba viaže prach a vytvára sivý lepivý film najmä pri hornom leme a okrajoch. Samostatný postup rozoberá článok <a href=\"{ARTICLE_CURTAIN_KITCHEN}\">ako prať záclony v kuchyni od mastnoty a pachov</a>. Pred cyklom odsajte voľný prach, potom na skrytom mieste overte kompatibilný odmasťovací krok.",
                f"Jednotlivú škvrnu riešte podľa pôvodu pomocou <a href=\"{ARTICLE_STAIN}\">sprievodcu čistením škvŕn</a>. Koncentrovaný odmasťovač môže na polyesterovej alebo farebnej metráži vytvoriť svetlý kruh a tvrdé trenie roztiahnuť otvorenú väzbu. Pach hodnotíte až po úplnom vysušení; parfumovanie mokrého filmu neodstráni mastnotu ani zdroj pri okne.",
            ],
        },
        {
            "heading": "Nízke odstreďovanie, žiadne odstreďovanie a mokrá hmotnosť",
            "paragraphs": [
                "Nižšie otáčky obmedzujú tvorbu ostrých lomov a zaťaženie jemných švov, no záclona ostane ťažšia a môže kvapkať. Úplné vypnutie odstreďovania nie je automaticky najšetrnejšie, ak potom treba nasiaknutú plochu dvíhať za jeden roh alebo krútiť rukami. Zvoľte najnižšie nastavenie, ktoré povoľuje etiketa a umožní bezpečnú manipuláciu.",
                "Po cykle podoprite záclonu zospodu a nechajte krátko odkvapkať bez stáčania do povrazu. Vodu nevytláčajte silným žmýkaním, pretože sa napätie sústredí pri leme a páske. Ak musí prejsť cez domácnosť, preneste ju v čistej nádobe. Mokrá plocha sa nesmie dotýkať špinavej podlahy, radiátora ani kovu, ktorý môže zanechať stopu.",
            ],
        },
        {
            "heading": "Zavesiť vlhkú alebo sušiť rozloženú",
            "paragraphs": [
                f"Mnohé moderné polyesterové voály sa po nízkom odstreďovaní vyrovnajú vlastnou rovnomernou hmotnosťou, keď sa zavesia ešte vlhké. Všeobecné zásady vysvetľuje článok <a href=\"{ARTICLE_CURTAINS}\">ako prať záclony</a>. Zavesenie však použite iba na pevný lem a čistý systém; voľný háčik alebo poškodená páska môže pod mokrou záťažou prasknúť.",
                "Bavlnený, hodvábny, starší alebo tvarovo nestabilný voál môže potrebovať rovnejšiu oporu a kontrolu rozmeru. Pri sušení rozloženej veľkej plochy zabezpečte čistotu podkladu a vzduch z oboch strán. Diely sa nesmú prekrývať v mokrej vrstve. Pred zatiahnutím okna nechajte vyschnúť aj hornú pásku, spodnú záťaž a preloženia.",
            ],
        },
        {
            "heading": "Kedy treba voál žehliť a ako nepoškodiť polyester",
            "paragraphs": [
                f"Ak po zavesení a úplnom vyschnutí ostanú ostré sklady, postupujte podľa symbolu a návodu <a href=\"{ARTICLE_CURTAIN_IRON}\">ako správne vyžehliť záclonu</a>. Začnite na skrytom bočnom leme, z rubu a pri najnižšej účinnej teplote. Použite čistú ochrannú tkaninu a žehličku nenechávajte stáť na priesvitnej ploche.",
                "Polyester sa môže lokálne zvlniť, získať lesk alebo sa pri vysokej teplote zataviť. Bavlnený voál môže potrebovať viac vlhkosti, no lokálne striekanie vytvorí mapu, ak je na povrchu zvyšok prostriedku. Para nie je vhodná na každý farebný, hodvábny alebo upravený diel. Výsledok kontrolujte po vychladnutí a bez napínania.",
            ],
        },
        {
            "heading": "Ako voál skladovať, ak záclony striedate",
            "paragraphs": [
                "Záclonu ukladajte iba úplne čistú a suchú. Široké voľné sklady sú bezpečnejšie než dlhodobé stlačenie v malej taške. Háčiky a kovanie skladujte oddelene v označenej nádobe, aby nevytvorili hrdzavú stopu alebo zatrhnutie. Medzi bielu záclonu a farebný papier či textil nevkladajte neoverený materiál s možným prenosom farby.",
                "Suchý priedušný obal chráni pred prachom, ale nevyrovná vlhkú skriňu. Pred opätovným zavesením skontrolujte lem, žlté línie na skladoch a pach. Ak je látka po rokoch krehká, nezačínajte automaticky praním. Najprv posúďte pevnosť na skrytom kraji a podľa hodnoty zvážte odborné čistenie alebo výmenu.",
            ],
        },
    ],
    "table2_heading": "Voálová záclona po praní: diagnóza pred ďalším cyklom",
    "table2_intro": "Záclonu hodnotíte až úplne suchú pri dennom rozptýlenom svetle. Mokrá priesvitná látka dočasne tmavne a ukazuje každý sklad.",
    "table2_headers": ["Prejav", "Možná príčina", "Čo skontrolovať", "Bezpečný ďalší krok"],
    "table2_rows": [
        ("Ostali sivé pásy", "Prach alebo produkt uzavretý v záhyboch, prípadne nerovnomerné starnutie.", "Dávku, naplnenie, riasiacu pásku a zmenu podľa svetla.", "Pri povolení šetrne opláchnuť bez ďalšej dávky; mechanické poškodenie nedrhnúť."),
        ("Záclona je veľmi pokrčená", "Preplnený bubon, vysoké otáčky, dlhé státie po cykle alebo vlastnosť metráže.", "Symbol žehlenia, zloženie a stav po vlhkom zavesení.", "Najprv rovnomerne navlhčiť podľa etikety; až potom nízke teplo na skúške."),
        ("Pri hornom leme je diera", "Háčik, oslabená páska alebo mokrá hmotnosť v jednom bode.", "Kovanie, okraje diery a pevnosť susedných miest.", "Pred zavesením opraviť a rozložiť zaťaženie."),
        ("Biela látka zožltla", "Teplo, nevhodná chémia, starý film, svetlo alebo starnutie úpravy.", "Zloženie, použitý produkt a symetriu pri okne.", "Nepridávať silnejšie bielidlo bez kompatibility a skrytej skúšky."),
        ("Po usušení ostal pach", "Mastnota, nedoschnutá páska, znečistená garniža alebo vlhkosť pri okne.", "Horný lem, spodný sklad, stenu a koľajnicu.", "Odstrániť zdroj a zabezpečiť úplné sušenie; vôňou problém neprekrývať."),
    ],
    "steps_heading": "Ako vyprať voálovú záclonu krok za krokom",
    "steps": [
        "Odfoťte zavesenie, označte diely a bezpečne odstráňte záclonu bez ťahania za jeden roh.",
        "Vyberte alebo zaistite háčiky, skontrolujte záťažový lem, pásku, výšivku a poškodené švy.",
        "Voľný prach jemne odstráňte nasucho a mastné či farebné škvrny označte.",
        "Prečítajte zloženie a všetky symboly vrátane bielenia, odstreďovania, sušenia a žehlenia.",
        "Zvoľte dostatočne veľký vak alebo voľnú samostatnú náplň bez zipsov a drsných textílií.",
        "Použite povolený jemný cyklus, presnú dávku a nepreplnený bubon s priestorom na oplach.",
        "Po skončení záclonu hneď podoprite a vyberte bez krútenia a ťahania za lem.",
        "Podľa etikety ju zaveste rovnomerne vlhkú alebo rozložte na čistú oporu s prúdením vzduchu.",
        "Žehličku použite až po vyschnutí, ak lomy ostali, na skrytej skúške a pri nízkej teplote.",
        "Čistú suchú záclonu zaveste na vyčistenú garnižu a skontrolujte rovnomerné rozloženie háčikov.",
    ],
    "remember": [
        "Aké vlákno, páska, výšivka, záťaž a háčiky tvorí celý diel?",
        "Je sivý vzhľad prach, mastnota, usadenina, zmena úpravy alebo starnutie?",
        "Zmestí sa záclona do bubna s priestorom na vodu a oplach?",
        "Chráni prací vak látku bez toho, aby ju stlačil do pevnej gule?",
        "Unesie horný lem rovnomerne vlhkú záclonu pri zavesení?",
        "Je pred žehlením potvrdený symbol a nízka skúšobná teplota?",
    ],
    "mistakes": [
        "Prať záclonu s ponechanými voľnými kovovými háčikmi.",
        "Natlačiť veľký voál do malého vaku alebo preplneného bubna.",
        "Použiť chlórové bielidlo iba preto, že je záclona biela.",
        "Krútiť nasiaknutú plochu a zdvíhať ju za poškodený lem.",
        "Žehliť polyesterovú záclonu vysokou teplotou priamo z líca.",
        "Zavesiť čistý voál na mastnú garnižu alebo ho odložiť s vlhkou páskou.",
    ],
    "expert_heading": "Odbornejší pohľad: priesvitnosť, otvorenosť väzby a skúšky po praní",
    "expert": [
        "Priesvitnosť voálu vzniká kombináciou jemnosti priadze, hustoty, plátnovej väzby, hrúbky a optických vlastností použitých vlákien. Rovnaký počet nití preto nemusí vytvoriť rovnaké svetelné správanie. Getty dokumentuje širšiu vláknovú rodinu voálu a Pratt jeho plátnovú konštrukciu, no ani jeden všeobecný záznam nie je dôkazom zloženia konkrétnej polyesterovej záclony.",
        "AATCC TM135 hodnotí rozmerovú zmenu po definovaných domácich postupoch a TM61 stálofarebnosť pri zrýchlenom praní. Výsledok musí uvádzať podmienky a konkrétny materiál. Záclona navyše obsahuje pásku, niť a záťaž, ktoré plochá vzorka nemusí reprezentovať. Domáce odporúčanie preto nemožno postaviť na jednom čísle bez výrobkového návodu.",
        "GINETEX symboly stanovujú maximálny povolený postup pre hotový výrobok. Symbol jemného prania neznamená maximálne naplnený bubon a povolenie žehlenia neznamená najvyššiu teplotu. Pri veľkej ľahkej ploche je objem náplne, mechanika, oplach a okamžité vybratie rovnako dôležité ako číslo na termostate.",
    ],
    "source_intro": "Zdroje podporujú plátnovú a priesvitnú identitu voálu, význam zloženia, rozmerovej zmeny, farby a symbolov. Nepodporujú jednu teplotu, bielidlo alebo spôsob žehlenia pre všetky voálové záclony.",
    "sources": [
        ("Getty Art & Architecture Thesaurus: definícia voálu", GETTY_VOILE),
        ("Pratt Textile Research Lab: voál ako plátnová tkanina", PRATT_WOVENS),
        ("CottonWorks: základné tkané väzby", COTTONWORKS_WEAVES),
        ("AATCC TM61: stálofarebnosť pri praní", AATCC_COLOR),
        ("AATCC TM135: rozmerové zmeny po praní", AATCC_DIMENSION),
        ("ASTM D1683: správanie šitých švov", ASTM_SEAM),
        ("EÚ 1007/2011: označovanie textilných vlákien", EU_FIBRE_LABEL),
        ("GINETEX: význam symbolov ošetrovania", GINETEX),
    ],
    "related": [
        ("Ako prať záclony", ARTICLE_CURTAINS),
        ("Ako vyžehliť záclonu", ARTICLE_CURTAIN_IRON),
        ("Ako prať záclony pri alergii na prach", ARTICLE_CURTAIN_ALLERGY),
        ("Ako prať kuchynské záclony", ARTICLE_CURTAIN_KITCHEN),
        ("Ako prať biele záclony", ARTICLE_CURTAIN_WHITE),
        ("Ako vyčistiť garnižu a koľajnicu", ARTICLE_CURTAIN_TRACK),
    ],
    "faq_title": "voálové záclony, pranie a pokrčenie",
    "faq": [
        ("Čo je voálová záclona?", "Ľahká priesvitná tkanina, spravidla v plátnovej väzbe, použitá ako mäkko splývavá okenná dekorácia."),
        ("Je voál vždy polyester?", "Nie. Moderné záclony bývajú často polyesterové, ale existujú bavlnené, hodvábne a zmesové voály."),
        ("Aký je rozdiel medzi voálom a organzou?", "Voál býva mäkší a splývavejší, organza spravidla hladšia a tuhšia. Zloženie môže byť pri oboch rôzne."),
        ("Na koľko stupňov prať voál?", "Jedna teplota neexistuje. Riaďte sa zložením a symbolom konkrétnej záclony."),
        ("Treba vybrať háčiky pred praním?", "Áno, ak to konštrukcia umožňuje. Inak ich treba bezpečne zaistiť podľa pokynu výrobcu."),
        ("Má sa voál prať v pracom vaku?", "Iba v dostatočne veľkom vaku, ktorý látku chráni bez stlačenia a umožní vode aj oplachu prejsť celou plochou."),
        ("Môže sa voál odstreďovať?", "Podľa etikety. Nízke otáčky často znižujú pokrčenie, no úplne mokrá záclona sa potom musí bezpečne preniesť."),
        ("Treba voál po praní žehliť?", "Nie vždy. Mnohé moderné voály sa po vlhkom zavesení vyrovnajú, iné potrebujú nízke žehlenie podľa symbolu."),
        ("Môžem voál zavesiť mokrý?", "Ak to povoľuje etiketa a horný lem je zdravý. Záclona má byť rovnomerne vlhká, nie kvapkajúca a ťažká."),
        ("Ako vybieliť sivý voál?", "Najprv určte zloženie a príčinu. Použite iba bielidlo povolené symbolom a návodom, po skrytej skúške."),
        ("Prečo záclona po praní zapácha?", "Môže zostať vlhká páska alebo záhyb, prípadne ostala mastnota či je znečistená garniža a okolie okna."),
        ("Čo s dierou pri hornom leme?", "Záclonu znovu nevešajte pod záťažou. Skontrolujte háčik, pásku a dajte miesto opraviť."),
        ("Ako skladovať voálové záclony?", "Úplne čisté a suché, vo voľných skladoch, bez kovania a v priedušnom suchom obale."),
    ],
}

add_cards(
    VOILE,
    product_heading="Prací gél dávkujte iba pri voáli s povoleným praním",
    product_intro="Pri bežnej prateľnej polyesterovej alebo kompatibilnej zmesovej záclone môže tekutý gél pomôcť uvoľniť prach a bežný film bez sypkého zvyšku.",
    product_text="Odmerajte ho podľa tvrdosti vody, skutočnej náplne a znečistenia. Veľká plocha potrebuje dostatok priestoru na oplach; viac gélu nenahrádza voľný bubon.",
    product_limit="Gél nie je automaticky vhodný na hodvábny voál, nestálofarebnú výšivku ani diel určený na profesionálne čistenie. Nevybieli starnutie a neopraví zatrhnutie či tepelné zvlnenie.",
    category_heading="Prací prostriedok vyberte až po kontrole zloženia záclony",
    category_intro="Voál môže byť polyesterový, bavlnený, hodvábny alebo zmesový a dopĺňa ho páska, niť aj záťaž. Kategória preto nie je náhradou etikety.",
    category_text="V kategórii nájdete gély pre rôzne potreby bežnej prateľnej bielizne. Pri bielení, hodvábe a špeciálnej úprave použite iba výrobcom potvrdený postup.",
)


TAFFETA: dict[str, object] = {
    "title": "Čo je taft: šušťavá tkanina, vodné mapy a bezpečné čistenie",
    "link": "co-je-taft-sustava-tkanina-vodne-mapy-a-bezpecne-cistenie",
    "meta": "Čo je taft, prečo šuští a drží tvar, ako sa líši od saténu a faille a ako čistiť, sušiť a žehliť taft bez vodných máp, lomov a lesku.",
    "short": "Taft je husto tkaná plátnová látka s hladkým až jemne priečne rebrovaným povrchom, výrazným leskom, tuhším omakom a typickým šušťaním. Môže byť hodvábny, polyesterový, nylonový, acetátový aj zmesový.",
    "name": "taft",
    "locative": "tafte",
    "identity_heading": "Taft je konštrukcia a omak, nie synonymum hodvábu",
    "identity_detail": "Taft používa plátnovú väzbu a často jemnejšiu hustú osnovu s plnším útkom, čo môže vytvoriť priečne rebro, vysoký odraz, pevnejší papierový omak a charakteristický zvuk pri pohybe.",
    "identity_boundary": "Hodvábny taft na šatách, polyesterový taft na sukni, nylonová podšívka a acetátová dekorácia môžu vyzerať podobne, no majú rozdielnu mokrú pevnosť, teplotu aj reakciu na škvrny.",
    "label_focus": "presné vlákna líca aj podšívky, výstuž, podlep, kostice, flitre, koráliky, potlač, dúhové priadze, povrchovú apretúru, záhyby, riasenie a povolené profesionálne čistenie",
    "missing_label": "Pri spoločenských alebo svadobných šatách bez jasného návodu nezačínajte celoplošným namáčaním; pri metráži si vyžiadajte technický list a skúšobne overte vodu, paru, šev aj rozmer.",
    "dry_check": "ostré biele lomy, vodné krúžky, pot v podpazuší, poškodenú výstuž, pretlačené švové rezervy, vytiahnuté nite, praskajúcu potlač, kovové ozdoby, rozstrapkaný okraj a rozdiel odtieňa pri pohľade z dvoch smerov",
    "damage_boundary": "Povrchovú nečistotu a čerstvú škvrnu možno čistiť, no zlomená alebo odretá priadza, trvalý lom, teplom zvlnený syntetický taft a oddelená výstuž nie sú škvrny.",
    "test_focus": "Skúšku pozorujte spredu, zboku aj po úplnom vyschnutí, pretože vysoký lesk a dvojfarebná osnova s útkom môžu zmenu odrazu zameniť za mapu alebo vyblednutie.",
    "combined_risk": "migrácie farbiva a apretúry pri nerovnomernom navlhčení, tvorby ostrých lomov, tlaku na hladký povrch a rozdielnej reakcie podšívky, výstuže a ozdôb",
    "chemistry_boundary": "Voda, pot, deodorant, víno, mastnota a make-up potrebujú odlišný prvý krok; náhodné rozpúšťadlo alebo silná zásada môže vytiahnuť farbu, zmeniť tuhosť alebo zanechať väčší kruh.",
    "drying_detail": "Šaty podoprite cez viac bodov, otvorte podšívku, záhyby a vystužené časti a nechajte vzduch prechádzať bez toho, aby mokrá sukňa visela za úzke ramienko alebo jediný šev.",
    "heat_boundary": "Horúca žehlička môže vytvoriť lesklú platňu, roztaviť syntetické vlákno, odtlačiť šev, zmeniť apretúru, poškodiť acetát a zafixovať zvyšok škvrny.",
    "stop_signs": "šíriaca sa vodná mapa, prenos farby, zmena tuhosti, bubliny výstuže, lepkavosť, zvlnenie pri miernom teple, uvoľnenie korálikov alebo praskanie starého ostrého lomu",
    "professional_boundary": "Jednoduchý prateľný polyesterový taft možno niekedy ošetriť doma podľa etikety, kým hodvábne, podšité, vystužené, zdobené, svadobné a historické šaty spravidla potrebujú profesionálne posúdenie.",
    "answer": "Taft je husto tkaná látka v plátnovej väzbe, ktorá býva hladká až jemne priečne rebrovaná, lesklá, tuhšia a pri pohybe šuští. Nie je to automaticky hodváb: vyrába sa aj z polyesteru, nylonu, acetátu, viskózy a zmesí. Pred čistením preto prečítajte etiketu celého odevu, skontrolujte podšívku, výstuž, kostice, ozdoby, farbu a staré lomy. Vodnú alebo potnú mapu nepretierajte veľkým mokrým kruhom; najprv odsajte prebytok, urobte skúšku na skrytom mieste a zvoľte postup podľa škvrny a vlákna. Celé šaty perte len pri výslovnom povolení. Po navlhčení ich podoprite, sušte bez prudkého tepla a žehlite z rubu cez ochrannú tkaninu pri najnižšej účinnej teplote. Štruktúrované spoločenské alebo hodvábne šaty zverte čistiarni.",
    "intro": "Taft priťahuje pozornosť leskom, objemom a zvukom, ale rovnaké vlastnosti zviditeľnia každý ostrý lom, kvapku a odtlačok šva. Preto sa rada utrieť miesto vodou môže skončiť väčším krúžkom a rada prežehliť záhyb lesklou platňou. Riziko ešte rastie pri hotových šatách: pod taftom býva podšívka, sieťovina, kostice, podlep, ozdoby a tvarované švy. Bezpečná starostlivosť najprv určí vlákno a konštrukciu, potom odlíši odstrániteľnú nečistotu od trvalého mechanického lomu a až následne vyberie lokálne čistenie, domáce pranie alebo čistiareň.",
    "quick": [
        "<strong>Taft nie je satén:</strong> taft má typicky plátnovú väzbu a tuhší šušťavý omak, satén dlhšie väzné úseky a hladký odraz.",
        "<strong>Nie je vždy hodvábny:</strong> moderný taft môže byť polyesterový, nylonový, acetátový, viskózový alebo zmesový.",
        "<strong>Voda môže vytvoriť kruh:</strong> pri nerovnomernom schnutí migruje nečistota, farbivo alebo povrchová úprava k okraju.",
        "<strong>Ostrý lom môže byť trvalý:</strong> zlomenú alebo odretú priadzu žehlenie nevráti do pôvodného stavu.",
        "<strong>Šaty posudzujte ako celok:</strong> podšívka, kostice a ozdoby často určujú prísnejší postup než vrchný taft.",
        "<strong>Teplo skúšajte z rubu:</strong> syntetika a acetát sa môžu poškodiť skôr, než sa záhyb vyrovná.",
    ],
    "overview_heading": "Ako vzniká lesk, rebro a typické šušťanie taftu",
    "overview": [
        "Taft patrí medzi plátnovo tkané látky: osnovné a útkové nite sa pravidelne striedajú nad a pod sebou. Keď je osnova jemnejšia a hustejšia a útok plnší, na povrchu sa môže objaviť jemné priečne rebro. Pevnejšie priadze a dokončenie obmedzia splývanie, takže látka pri ohybe vytvára ostré línie a vydáva charakteristický suchý zvuk.",
        "LACMA dokumentuje taft ako plátnovú konštrukciu pri historickom hodvábnom odeve aj pri moderných šatách z rayonu a polyesteru. Museum Conservation Institute zároveň uvádza, že taft môže byť z hodvábu, viskózového rayonu, nylonu, acetátu, polyesteru alebo kombinácií a že apretúry prinášajú riziko kruhov, krčenia a farebných zmien. Názov teda nepredpovedá jednu bezpečnú chémiu.",
        "Dvojfarebný alebo changeant taft používa rozdielnu farbu osnovy a útku. Pri pohybe sa mení pomer svetla odrazeného z oboch sústav, preto látka pôsobí dúhovo. Lokálne vyhladenie, voda alebo otočenie dielu potom vytvoria nápadný farebný rozdiel aj bez chemického vyblednutia. Hodnotiť ho treba z viacerých uhlov.",
    ],
    "table1_heading": "Taft, satén, faille, organza a podšívkovina",
    "table1_intro": "Rozlíšenie pomáha pochopiť vzhľad a riziká. Obchodné názvy sa používajú voľne a konkrétnu starostlivosť vždy určuje etiketa celého výrobku.",
    "table1_headers": ["Označenie", "Typická väzba alebo stavba", "Omak a odraz", "Praktické riziko"],
    "table1_rows": [
        ("Taft", "Hustá plátnová väzba, často jemná osnova a plnší útok.", "Tuhší, hladký až priečne rebrovaný, šušťavý a lesklý.", "Vodné kruhy, ostré lomy, pretlačenie švov a teplo."),
        ("Satén", "Saténová väzba s dlhšími väznými úsekmi.", "Veľmi hladký, súvislo lesklý a často splývavejší.", "Zatrhnutie dlhších úsekov, lesk po tlaku a vláknový limit."),
        ("Faille", "Výraznejšia priečne rebrovaná plátnová odvodenina.", "Zreteľnejšie rebrá a pevný omak.", "Oder vrcholov rebier a odtlačenie pri žehlení."),
        ("Organza", "Ľahká priesvitná plátnová tkanina.", "Tuhá, ale podstatne priesvitnejšia.", "Zachytenie, lomy a vysoká citlivosť podľa vlákna."),
        ("Hladká podšívkovina", "Rôzne väzby a syntetické či celulózové vlákna.", "Ľahká a klzká, nie vždy tuhá.", "Podšívka môže mať nižší mokrý a tepelný limit než vrchný diel."),
    ],
    "sections": [
        {
            "heading": "Ako rozoznať taft od saténu, organzy a podšívky",
            "paragraphs": [
                f"Pozrite sa lupou na väzbu z líca aj rubu. Pravidelné striedanie jedna nad a jedna pod smeruje k plátnovej konštrukcii; dlhšie hladké väzné úseky k saténu. Rozdiel medzi väzbou a vláknom vysvetľuje článok <a href=\"{ARTICLE_SATIN}\">čo je satén</a>. Taft býva tuhší a pri ohnutí vydáva suchší zvuk, no omak upraveného polyesteru a hodvábu sa môže prekrývať.",
                "Organza je zvyčajne priehľadnejšia a ľahšia, faille má výraznejšie priečne rebro. Hladká podšívkovina môže v predaji dostať široký názov taft, ale nemusí mať rovnakú hustotu ani tuhosť ako spoločenská metráž. Rozlíšenie dotykom nikdy nenahrádza zloženie. Nepárajte šev a nerobte skúšku horením na hotovom odeve.",
            ],
        },
        {
            "heading": "Hodvábny, polyesterový, nylonový, acetátový a viskózový taft",
            "paragraphs": [
                "Hodvábny taft je proteínový textil citlivý na zásadité prostredie, dlhé slnko a nevhodné trenie. Polyester a nylon môžu lepšie držať rozmer, ale teplom sa lesknú, vlnia alebo tavia. Acetát má osobitnú citlivosť na niektoré rozpúšťadlá a vysoké teplo. Viskózový variant môže po navlhčení stratiť pevnosť a meniť rozmer.",
                f"Pri viskózovej zložke pomôže samostatný článok <a href=\"{ARTICLE_VISCOSE}\">čo je viskóza a ako sa správa za mokra</a>. Zmes nepreberá automaticky lepšie vlastnosti každého vlákna; bezpečnú teplotu určuje najcitlivejšia zložka a hotové spracovanie. Pri metráži žiadajte technický list, pri šatách čítajte etiketu aj pre podšívku a ozdoby.",
            ],
        },
        {
            "heading": "Vodná mapa na tafte: čo sa presúva k okraju kvapky",
            "paragraphs": [
                "Keď lokálne miesto navlhne, voda rozpustí alebo prenesie jemnú nečistotu, zvyšok dokončovacej látky či uvoľnené farbivo. Pri schnutí sa kvapalina pohybuje k okraju a zanechá kruh. Na lesklom hladkom povrchu je rozdiel nápadný. Ďalšia menšia kvapka môže vytvoriť ďalší okraj namiesto opravy prvého.",
                "Čerstvú vodu odsajte z oboch strán bielym savým materiálom bez tlaku. Skúšku rovnomerného navlhčenia robte iba pri povolení etikety a na skrytom leme. Ak sa farba prenáša alebo tuhosť mení, nepokračujte. Hodnotný hodvábny alebo štruktúrovaný odev odovzdajte čistiarni a ukážte presné miesto aj použitý domáci produkt.",
            ],
        },
        {
            "heading": "Pot, deodorant a make-up na spoločenských šatách",
            "paragraphs": [
                f"Pot obsahuje vodu, soli a organické zložky, kým deodorant môže pridať voskovitý alebo minerálny film. Make-up kombinuje pigment a mastnotu. Všeobecné rozdelenie nájdete v <a href=\"{ARTICLE_STAIN}\">návode na rôzne škvrny</a>. Podpazušie kontrolujte z rubu a podšívku oddeľte od vrchného taftu iba v rozsahu, ktorý dovoľuje konštrukcia.",
                "Miesto neprelievajte parfumom, alkoholom ani univerzálnym odmasťovačom. Alkohol môže ovplyvniť farbivo a acetát, voda vytvoriť kruh a trenie zmeniť lesk. Po jednorazovom nosení šaty vyvetrajte a pot riešte včas podľa etikety; stará oxidovaná škvrna sa odstraňuje ťažšie a môže oslabiť hodvábnu priadzu.",
            ],
        },
        {
            "heading": "Môže sa taft prať v práčke alebo ručne",
            "paragraphs": [
                f"Iba pri výslovnom symbole pre celý výrobok. <a href=\"{ARTICLE_FORMAL_DRESS}\">Sprievodca spoločenskými šatami</a> vysvetľuje, prečo samotná vrchná látka nestačí: kostice, výstuž, lepené diely, sieťovina, flitre a podšívka môžu vodu vylúčiť. Pri jednoduchom prateľnom polyesterovom kuse zapnite bezpečné prvky a oddeľte ho od zipsov a drsných textílií.",
                "Ručné pranie nie je automaticky jemnejšie. Dlhé namáčanie, stláčanie a krútenie vytvárajú ostré lomy a nerovnomerné mapy. Ak je ručné pranie povolené, použite dostatok priestoru, rovnomerne rozptýlený kompatibilný prostriedok a odev podopierajte. Vodu nevytláčajte stáčaním sukne a mokré šaty nedvíhajte za ramienka.",
            ],
        },
        {
            "heading": "Ostré lomy, biele čiary a odrenie nie sú vždy opraviteľné",
            "paragraphs": [
                "Taft sa pri zložení ohýba v úzkej línii. Ak sa priadza iba dočasne preusporiadala, mierne zvlhčenie alebo para podľa etikety môže záhyb uvoľniť. Ak sa vlákna zlomili, odrali alebo plasticky zdeformovali, ostane svetlá čiara alebo zmena odrazu. Vyššie teplo môže poškodenie ešte zvýrazniť.",
                "Lom posudzujte lupou bez ťahania. Biela rozstrapkaná línia, praskajúca potlač alebo oslabený šev si vyžadujú opravu alebo konzervovanie, nie žehlenie silou. Pri skladovaní nevytvárajte stále ten istý ostrý prehyb. Veľkú sukňu podoprite v obale a koráliky oddeľte, aby ich hrany netlačili do taftu.",
            ],
        },
        {
            "heading": "Ako sušiť taftové šaty bez vyťahania a vodných okrajov",
            "paragraphs": [
                f"Po povolenom praní odev vyberte okamžite a podoprite celú mokrú hmotnosť. Podšívku, vrecká, záhyby a viacvrstvový živôtik otvorte prúdeniu vzduchu. Všeobecné zásady dopĺňa článok <a href=\"{ARTICLE_DRYING}\">ako sušiť bielizeň bez zatuchnutia</a>. Úzke ramienko môže vytlačiť ramená a ťažká sukňa vyťahať šev v páse.",
                "Sušičku použite iba pri výslovnom symbole. Radiátor a fén vytvoria teplý suchý okraj, kým vnútorná vrstva ostáva vlhká, čím podporia mapu a zmenu tvaru. Odev nechajte schnúť pri stabilnej izbovej teplote, chráňte pred priamym slnkom a polohu meňte s rovnomernou oporou, nie prudkým prehodením.",
            ],
        },
        {
            "heading": "Ako žehliť alebo napariť taft bez lesklej platne",
            "paragraphs": [
                "Najprv prečítajte symbol, odev obráťte naruby a zvoľte najnižšiu účinnú teplotu pre najcitlivejšie vlákno. Čistá ochranná tkanina znižuje priamy kontakt, mäkká rovná podložka bráni pretlačeniu švových rezerv. Žehličku posúvajte bez dlhého státia na jednom mieste a korálikom, flitrom, lepidlu a potlači sa vyhnite.",
                "Para môže uvoľniť záhyb, ale na vodu citlivom hodvábe, acetáte alebo upravenom povrchu vytvoriť škvrnu či zmenu tuhosti. Naparovač držte v odporúčanej vzdialenosti a skúšobné miesto nechajte vychladnúť. Ak sa povrch zvlňuje, mäkne alebo mení odtieň, okamžite zastavte. Horúci taft neohýbajte ani nestláčajte.",
            ],
        },
        {
            "heading": "Švy, riasenie, kostice a ozdoby menia rozhodnutie o čistení",
            "paragraphs": [
                "Husté riasenie vytvára viac vrstiev, ktoré sa horšie oplachujú a schnú. Kostice koncentrujú tlak, kovové ozdoby môžu korodovať a koráliky prerezať priadzu. Lepená výstuž živôtika reaguje na vodu a paru inak než taft. Pred zásahom preto odfoťte tvar a skontrolujte rub, nielen viditeľnú sukňu.",
                "Uvoľnený korálik alebo otvorený šev opravte pred čistením, ak je postup známy. Kovový prvok neprelepujte náhodnou páskou, ktorej lepidlo sa vo vode rozpustí. Pri čistiarni upozornite na náhradné ozdoby, starú opravu a všetky škvrny. Odborník potrebuje vedieť, čo už bolo na mieste použité.",
            ],
        },
        {
            "heading": "Ako taft skladovať medzi sezónami alebo po svadbe",
            "paragraphs": [
                "Odev uložte až po vyčistení a úplnom vysušení. Pot a neviditeľný nápoj časom oxidujú a menia farbu. Šaty podoprite na širokom tvarovanom vešiaku iba vtedy, ak ich hmotnosť nenesú tenké ramienka; ťažkú sukňu možno podoprieť vnútornými pútkami alebo uložiť plocho podľa konštrukcie.",
                "Použite priedušný nekyslý obal vhodný pre hodnotu predmetu a zamedzte tlaku ozdôb na líc. Prehyby robte široké a ich polohu pri dlhom skladovaní kontrolujte. Plastový vak z čistiarne nie je automaticky dlhodobý archívny obal. Pri historickom alebo sentimentálnom odeve sa poraďte s textilným konzervátorom.",
            ],
        },
        {
            "heading": "Ako vybrať taftovú metráž alebo šaty podľa použitia",
            "paragraphs": [
                "Pri metráži sledujte zloženie, hmotnosť, šírku, tuhosť, priečne rebro, zmenu odtieňa podľa smeru a odporúčané žehlenie. Vzorku pokrčte v dlani, nechajte odpočinúť a následne ju otestujte plánovaným švom a parou. Skúšobný kus predperte iba vtedy, keď sa má rovnakým spôsobom ošetrovať aj hotový výrobok.",
                "Pri šatách si pozrite podšívku, okraje kostíc, upevnenie ozdôb, rezervu švov a návod na čistenie. Papierovo tuhý omak môže byť zámerná apretúra, ktorá sa pri vode zmení. Kvalitu nemožno zredukovať na intenzitu šušťania alebo lesku. Dôležitá je stabilita pre daný strih, opraviteľnosť a realistická údržba po nosení.",
            ],
        },
    ],
    "table2_heading": "Taft po nosení alebo čistení: čo znamená konkrétny prejav",
    "table2_intro": "Povrch hodnotíte úplne suchý, vychladnutý a z viacerých uhlov. Lesk a dvojfarebné priadze menia dojem podľa smeru svetla.",
    "table2_headers": ["Prejav", "Pravdepodobná príčina", "Čo overiť", "Bezpečný ďalší krok"],
    "table2_rows": [
        ("Kruhová mapa po kvapke", "Migrácia nečistoty, farbiva alebo apretúry pri schnutí.", "Prenos farby, zloženie a reakciu skrytého lemu.", "Nepridávať ďalšie malé kvapky; pri citlivom kuse zvoliť čistiareň."),
        ("Biela ostrá čiara", "Zlomenie, odretie alebo trvalé preloženie priadze.", "Povrch lupou a celistvosť nite.", "Nežehliť silou; stabilizovať a pri hodnotnom kuse konzultovať opravu."),
        ("Lesklý obdĺžnik", "Príliš vysoký tlak alebo teplota žehličky.", "Rub, švovú rezervu a zmenu po vychladnutí.", "Ďalšie teplo zastaviť; mechanický lesk neodstraňovať drhnutím."),
        ("Živôtik má bubliny", "Uvoľnenie lepidla alebo rozdielna rozmerová zmena vrstiev.", "Podšívku, výstuž a reakciu na jemný dotyk.", "Nenaparovať naslepo; zvoliť krajčírske alebo čistiace posúdenie."),
        ("Odev stratil tuhosť", "Vymytie alebo zmena apretúry, teplo či mechanické poškodenie.", "Technické údaje a rovnakú skrytú zónu.", "Nepridávať domáci škrob bez súhlasu výrobcu."),
    ],
    "steps_heading": "Bezpečný postup pri škvrne alebo povolenom praní taftu",
    "steps": [
        "Určte zloženie taftu a všetkých vrstiev, ozdôb, kostíc a výstuže.",
        "Odfoťte lesk, lomy, vodné mapy, švy a tvar odevu pri rovnakom svetle.",
        "Rozlíšte vodu, pot, mastnotu, pigment a mechanické poškodenie priadze.",
        "Na skrytom leme otestujte farbu, vodu, produkt, tlak aj úplné vyschnutie.",
        "Celý odev perte iba pri výslovnom symbole a s dostatkom priestoru bez drsných prvkov.",
        "Ručne perený kus podopierajte, nestláčajte do ostrých skladov a nekrúťte.",
        "Po navlhčení odev prenášajte s oporou sukne, živôtika aj podšívky.",
        "Sušte pri stabilnej teplote s otvorenými vrstvami a bez radiátora alebo fénu.",
        "Žehlite z rubu cez ochrannú tkaninu pri najnižšej účinnej povolenej teplote.",
        "Čistý suchý odev uložte s oporou a bez tlaku ozdôb a ostrých prehybov.",
    ],
    "remember": [
        "Je taft hodvábny, polyesterový, nylonový, acetátový, viskózový alebo zmesový?",
        "Čo okrem vrchnej látky obsahujú šaty a ktorý diel má najnižší limit?",
        "Je svetlá čiara nečistota, vodný kruh, odrenie alebo zlomená priadza?",
        "Prenáša skryté miesto farbu alebo mení tuhosť po vode a vyschnutí?",
        "Má mokrý odev dostatočnú oporu a prúdenie vzduchu medzi vrstvami?",
        "Je domáce čistenie výslovne povolené pre celý hotový výrobok?",
    ],
    "mistakes": [
        "Považovať každý taft za hodváb alebo každý syntetický taft za prateľný.",
        "Rozotrieť vodnú mapu ďalšími malými kvapkami bez skúšky.",
        "Krútiť ručne pranú sukňu a vytvoriť trvalé ostré lomy.",
        "Žehliť taft vysokou teplotou priamo z líca.",
        "Ignorovať podšívku, kostice, lepidlá a kovové ozdoby.",
        "Uložiť šaty s potom alebo nápojom a s ozdobami pritlačenými do povrchu.",
    ],
    "expert_heading": "Odbornejší pohľad: plátnová väzba, apretúra a mapy po čistení",
    "expert": [
        "LACMA pri historickom hodvábnom odeve aj pri moderných rayonovo-polyesterových šatách označuje taft ako plátnovú väzbu. Museum Conservation Institute rozširuje materiálové možnosti na hodváb, rayon, nylon, acetát, polyester a zmesi a upozorňuje na kruhy po apretúre, krčenie a farebné zmeny. Technický názov teda spája konštrukciu a typický omak, nie jednu chemickú identitu.",
        "AATCC TM61 hodnotí stálofarebnosť pri definovanom praní a TM135 rozmerovú zmenu po konkrétnych domácich postupoch. Pri tafte treba výsledok doplniť o zmenu vzhľadu, tuhosti a švov. Plochá metráž bez podšívky nepredpovedá správanie šiat s kosticami a lepenou výstužou.",
        "ASTM D1683 skúma zlyhanie šitého šva, kým D4966 sleduje oder plochej tkaniny. Vysoká pevnosť pri jednom teste nie je zárukou proti ostrému lomu, vodnému kruhu alebo tepelnej deformácii. Spotrebiteľ potrebuje podmienky metódy, celé zloženie a symbol hotového odevu, nie izolované tvrdenie odolný taft.",
    ],
    "source_intro": "Zdroje podporujú plátnovú, tuhú a lesklú identitu taftu, jeho rôzne vláknové verzie a riziko kruhov, krčenia a farebných zmien. Nepodporujú univerzálne domáce pranie ani jednu teplotu.",
    "sources": [
        ("LACMA: historický hodvábny taft v plátnovej väzbe", LACMA_SILK_TAFFETA),
        ("LACMA: moderný rayonovo-polyesterový taft", LACMA_BLEND_TAFFETA),
        ("Smithsonian Museum Conservation Institute: problémy taftu", MCI_TAFFETA),
        ("CottonWorks: základné tkané väzby", COTTONWORKS_WEAVES),
        ("ASTM D1683: zlyhanie šitých švov", ASTM_SEAM),
        ("ASTM D4966: oder metódou Martindale", ASTM_ABRASION),
        ("AATCC TM61: stálofarebnosť pri praní", AATCC_COLOR),
        ("AATCC TM135: rozmerové zmeny po praní", AATCC_DIMENSION),
        ("EÚ 1007/2011: označovanie textilných vlákien", EU_FIBRE_LABEL),
        ("GINETEX: význam symbolov ošetrovania", GINETEX),
    ],
    "related": [
        ("Čo je satén a prečo nie je vždy hodvábny", ARTICLE_SATIN),
        ("Ako prať spoločenské šaty a kedy zvoliť čistiareň", ARTICLE_FORMAL_DRESS),
        ("Čo je viskóza", ARTICLE_VISCOSE),
        ("Ako čítať štítok na oblečení", ARTICLE_LABEL),
        ("Ako postupovať pri rôznych škvrnách", ARTICLE_STAIN),
        ("Prečo sa oblečenie po praní zráža", ARTICLE_SHRINKAGE),
    ],
    "faq_title": "taft, vodné mapy a spoločenské šaty",
    "faq": [
        ("Čo je taft?", "Husto plátnovo tkaná látka s tuhším omakom, leskom, jemným priečnym rebrom a typickým šušťaním."),
        ("Je taft vždy hodváb?", "Nie. Môže byť polyesterový, nylonový, acetátový, viskózový alebo zmesový."),
        ("Je taft to isté ako satén?", "Nie. Taft má typicky plátnovú väzbu a tuhší omak, satén dlhšie väzné úseky a súvisle hladký lesk."),
        ("Prečo taft šuští?", "Pevnejšia hustá konštrukcia a apretúra pri ohýbaní vytvárajú suchý zvuk a ostré línie."),
        ("Môže sa taft prať v práčke?", "Iba pri výslovnom symbole pre celý výrobok vrátane podšívky, výstuže a ozdôb."),
        ("Na koľko stupňov prať taft?", "Jedna teplota neexistuje. Rozhoduje vlákno, farba, apretúra a konštrukcia hotového kusu."),
        ("Ako odstrániť vodnú mapu?", "Ďalšie malé kvapky nepridávajte. Urobte skrytú skúšku a pri citlivom hodvábe alebo šatách zvoľte čistiareň."),
        ("Ako odstrániť pot z taftových šiat?", "Riešte ho včas podľa zloženia a etikety, bez tvrdého trenia a náhodného alkoholu či parfumovania."),
        ("Ako žehliť taft?", "Z rubu cez ochrannú tkaninu pri najnižšej účinnej povolenej teplote, s malým tlakom."),
        ("Môže sa taft naparovať?", "Iba pri povolení výrobcu a po skrytej skúške; para môže zmeniť farbu, tuhosť alebo výstuž."),
        ("Dá sa opraviť biela čiara po zalomení?", "Ak je priadza zlomená alebo odretá, žehlenie ju neobnoví. Miesto ďalej nezaťažujte."),
        ("Ako sušiť taftové šaty?", "S rovnomernou oporou, otvorenými vrstvami, pri izbovej teplote a mimo radiátora, fénu a priameho slnka."),
        ("Ako skladovať taftové alebo svadobné šaty?", "Čisté a úplne suché, v priedušnom vhodnom obale, s oporou hmotnosti a bez tlaku ozdôb."),
    ],
}

add_cards(
    TAFFETA,
    product_heading="Bežný prací gél je možnosť iba pre výslovne prateľný taft",
    product_intro="Pri jednoduchom polyesterovom alebo inom kompatibilnom kuse s povoleným domácim praním možno tekutý gél presne odmerať a rovnomerne opláchnuť.",
    product_text="Gél najprv overte na skrytom mieste a použite podľa tvrdosti vody a náplne. Koncentrát nelejte priamo na suchý lesklý povrch ani vodnú mapu.",
    product_limit="Nie je univerzálnym prostriedkom na hodváb, acetát, svadobné šaty, kostice, lepenú výstuž alebo profesionálne čistenie. Neopraví trvalý lom, odretú priadzu ani bubliny.",
    category_heading="Pracie gély porovnávajte až po potvrdení domáceho prania",
    category_intro="Pri tafte je prvým rozhodnutím, či sa celý odev smie namočiť. Až potom má význam vyberať kompatibilný prací prostriedok.",
    category_text="V kategórii nájdete gély pre bežnú prateľnú bielizeň. Hodvábny, acetátový, zdobený alebo štruktúrovaný taft môže potrebovať špecializovaný produkt alebo čistiareň.",
)


LODEN: dict[str, object] = {
    "title": "Čo je loden a varená vlna: valchovanie, plsť a bezpečné čistenie",
    "link": "co-je-loden-a-varena-vlna-valchovanie-plst-a-bezpecne-cistenie",
    "meta": "Čo je loden, varená vlna a plsť, ako vznikajú valchovaním a ako čistiť kabát, bundu, klobúk či deku bez zrazenia, stvrdnutia a straty tvaru.",
    "short": "Loden je tradične hustá tkaná vlnená látka, ktorej povrch sa valchovaním a ďalším dokončením zhutní. Varená vlna býva často pletenina zrazená kontrolovaným mokrým procesom a plsť je netkaná vláknová plocha; rovnaký hutný vzhľad preto neznamená rovnakú konštrukciu ani údržbu.",
    "name": "loden a varená vlna",
    "locative": "lodene a varenej vlne",
    "identity_heading": "Loden, varená vlna a plsť nie sú tri názvy jednej konštrukcie",
    "identity_detail": "Klasický loden začína ako tkanina, ktorá sa valchovaním zhutní a následne sa môže česať, strihať a lisovať; varená vlna sa v súčasnom predaji často vyrába z pleteniny kontrolovane zrazenej vlhkosťou, teplom a pohybom, kým pravá plsť vzniká spojením vlákien bez osnovy a útku.",
    "identity_boundary": "Podobne kompaktný kabát môže byť z čistého lodenu, pletenej varenej vlny, vpichovanej plsti, meltonu, zmesi s polyamidom alebo z textílie nalepenej na výstuž. Vzhľad preto nestačí na určenie prania.",
    "label_focus": "percento vlny a ďalších vlákien, tkanú alebo pletenú konštrukciu, podšívku, výstuž, kožené diely, lepené spoje, ozdoby, symbol prania vlny, sušenie a profesionálne čistenie",
    "missing_label": "Pri kabáte bez etikety, staršom klobúku alebo neoznačenej dekorácii neodhadujte prateľnosť podľa hustoty; suché kefovanie a odborné posúdenie sú bezpečnejšie než skúška celého kusu vo vode.",
    "dry_check": "zmenu rozmeru, tvrdé splstnatené miesta, nerovnomerný vlas, lesklé lakte, žmolky, dierky po škodcoch, uvoľnenú podšívku, bubliny výstuže, vodné kruhy, mastný golier a oslabené švy",
    "damage_boundary": "Prach, povrchový vlas alebo čerstvú lokálnu škvrnu možno šetrne riešiť, no zrazený tvrdý diel, zodratý vlas, pretrhnutá priadza a rozlepená výstuž nie sú nečistota, ktorú ďalšie pranie odstráni.",
    "test_focus": "Na skrytom leme sledujte po úplnom vysušení rozmer, tvrdosť, zmenu vlasu a farby; krátka skúška kvapkou nepredpovie správanie celého podšitého kabáta v bubne.",
    "combined_risk": "napučania vlnených vlákien, vzájomného zachytenia šupín, mechanického pohybu, rozdielneho zrazenia vrstiev a straty tvaru pod mokrou hmotnosťou",
    "chemistry_boundary": "Pot, kožný maz, blato, soľ a nápoj potrebujú odlišný prvý krok; zásaditý alebo enzýmový prostriedok určený na bežnú bielizeň nemusí byť vhodný na proteínové vlákno ani na farbivo kabáta.",
    "drying_detail": "Prateľný vlnený kus podoprite v celej ploche, vráťte ho iba jemne do prirodzených rozmerov a otvorte záhyby, vrecká a podšívku; ťažký kabát nevešajte mokrý za úzke ramená bez výslovného pokynu.",
    "heat_boundary": "Teplo spolu s vlhkosťou a pohybom môže podporiť ďalšie plstnatenie, zraziť podšívku, uvoľniť lepidlo, sploštiť vlas alebo vytvoriť lesklú stopu po tlaku.",
    "stop_signs": "rýchle tvrdnutie povrchu, rastúce zrazenie, silný prenos farby, otvorenie šva, bubliny výstuže, lepkavosť, zápach z podšívky, rozširujúca sa vodná mapa alebo oddeľovanie kože a ozdôb",
    "professional_boundary": "Jednoduchý výrobcom označený prateľný doplnok možno ošetriť doma podľa symbolov, no tvarovaný lodenový kabát, klobúk, koženými dielmi kombinovaná bunda a hodnotný starší predmet spravidla potrebujú profesionálne posúdenie.",
    "answer": "Loden je tradične tkaná vlnená látka zhutnená valchovaním a dokončená tak, aby mala kompaktný povrch. Varená vlna je obchodné označenie, ktoré sa často používa pre pleteninu kontrolovane zrazenú do hustejšej štruktúry. Plsť na rozdiel od nich nemusí byť tkaná ani pletená: vlákna držia vzájomným zapletením. Kabát alebo bundu preto neperte iba podľa názvu materiálu. Najprv prečítajte štítok celého výrobku, skontrolujte podšívku, výstuž, kožené diely a tvar. Ak je domáce pranie výslovne povolené, použite iba vhodný postup na vlnu s minimálnym pohybom a bez teplotných šokov, mokrý kus podopierajte a sušte podľa etikety. Tvarovaný lodenový kabát určený na profesionálne čistenie do práčky nepatrí.",
    "intro": "Hutný matný povrch zvádza k predstave, že textília už je zrazená a ďalšie pranie jej nemôže ublížiť. To je nebezpečná skratka. Výrobné valchovanie prebieha kontrolovane a tvorca počíta s pôvodnou konštrukciou, rezervou rozmeru a požadovaným omakom. Domáce pranie hotového kabáta pridáva podšívku, švy, výstuž, gombíky, neznáme farbivo a nerovnomerný pohyb. Tento sprievodca preto najprv oddeľuje loden, varenú vlnu, plsť, melton a tvíd a až potom rieši prach, škvrny, pranie, sušenie, žmolky, skladovanie a opravy.",
    "quick": [
        "<strong>Loden je typicky tkaný:</strong> hustotu získava valchovaním a ďalším mechanickým dokončením.",
        "<strong>Varená vlna býva často pletená:</strong> kontrolované zrazenie zmenší očká a vytvorí kompaktnejší pružnejší povrch.",
        "<strong>Plsť nemá klasickú väzbu:</strong> pri pravom netkanom materiáli nenájdete pravidelnú osnovu, útok ani pletené očká.",
        "<strong>Výrobný proces nie je návod na pranie:</strong> hotový kabát sa môže ďalej zraziť, stvrdnúť alebo stratiť tvar.",
        "<strong>Najprv vetrajte a kefujte:</strong> veľa prachu a mierneho pachu odstránite bez premočenia celej konštrukcie.",
        "<strong>Bežný gél nie je prostriedok na vlnu:</strong> použite iba produkt a postup potvrdený etiketou konkrétneho výrobku.",
    ],
    "overview_heading": "Ako valchovanie mení tkanú vlnu a prečo nejde o náhodné zrazenie",
    "overview": [
        "Vlnené vlákno má zvlnenie a povrchovú štruktúru, vďaka ktorým sa pri vhodnej kombinácii vlhkosti, mechanického pôsobenia a podmienok procesu vlákna približujú a zachytávajú. Pri valchovaní sa tkanina riadene zhutňuje: medzery sa zmenšujú, povrch sa stáva súvislejší a materiál získava plnší omak. Následné česanie, strihanie a lisovanie určia, či bude povrch hladký, vlasový alebo výrazne hutný.",
        "Výrobca sleduje čas, napätie, smer a požadovanú konečnú šírku. Náhodné domáce plstnatenie nemá rovnakú kontrolu. Hotový diel môže zmenišiť iba jednu časť, zvlniť podšívku, stiahnuť šev a zmeniť otvor rukáva. Skutočnosť, že materiál prešiel valchovaním už vo výrobe, preto neznamená rozmerovú stabilitu pri ľubovoľnom ďalšom cykle.",
        "Varená vlna sa v maloobchode často spája s pleteninou, ktorej slučky sa po kontrolovanom mokrom spracovaní zmenšia a čiastočne prekryjú. Zachováva inú pružnosť a správanie na okraji než tkaný loden. Pravá plsť vzniká priamo z vláknovej vrstvy bez tkania či pletenia. Rozpoznanie konštrukcie pomáha odhadnúť mechanické riziko, ale povolenie vody stále dáva výrobca hotového kusu.",
    ],
    "table1_heading": "Loden, varená vlna, plsť, melton a tvíd: praktické rozdiely",
    "table1_intro": "Obchodné názvy sa môžu prekrývať. Tabuľka opisuje typickú konštrukciu, nie náhradný ošetrovací štítok.",
    "table1_headers": ["Materiál", "Typický základ", "Ako vzniká hutnosť", "Dôležitá hranica pri starostlivosti"],
    "table1_rows": [
        ("Loden", "Tkaná vlnená tkanina, často keprového charakteru.", "Valchovanie, česanie, strihanie a lisovanie podľa požadovaného povrchu.", "Kabátová konštrukcia, podšívka a výstuž môžu vyžadovať čistiareň."),
        ("Varená vlna", "Často pletenina z vlny alebo zmesi.", "Kontrolovaná vlhkosť, teplo a pohyb zmenšia očká a zhutnia povrch.", "Môže sa ďalej zraziť; pružný vzhľad nie je povolením práčky."),
        ("Plsť", "Netkaná vrstva vlákien, prípadne vpichovaná alebo inak spevnená.", "Vzájomné zapletenie a spevnenie vlákien bez osnovy a útku.", "Hrany, tvarovanie, lepidlo a farbivo môžu byť citlivé na bodové namočenie."),
        ("Melton", "Hustá tkaná vlnená alebo zmesová kabátovina.", "Výrazné valchovanie a strihanie zvyčajne prekryjú väzbu.", "Patrí do príbuznej zhutnenej tkanej rodiny; názov neurčuje domáce pranie."),
        ("Tvíd", "Tkaná, často vlnená textília s viditeľnejšou priadzou a väzbou.", "Objem tvorí priadza, väzba a dokončenie, nie vždy silné prekrytie povrchu.", "Môže mať voľnejšiu štruktúru, žmolky a podšitú odevnú konštrukciu."),
    ],
    "sections": [
        {
            "heading": "Ako doma rozoznať tkaný loden, pletenú varenú vlnu a plsť",
            "paragraphs": [
                "Pozrite sa na rub, švovú rezervu a voľný okraj pod lupou. Pri lodene možno na menej dokončenom rube alebo v šve nájsť pravidelné kríženie osnovy a útku. Varená vlna môže ukázať zmenšené pletené očká a viac pružnosti v jednom alebo oboch smeroch. Pravá plsť nemá pravidelnú sieť väzby ani slučiek a rez sa správa ako súvislá vláknová plocha.",
                "Silné valchovanie môže tkanú väzbu takmer zakryť a pletené očká môžu byť veľmi malé. Neskúšajte preto okraj násilne párať ani naťahovať. Zloženie a technický opis majú prednosť pred domácim odhadom. Pri hotovom kabáte je navyše dôležitejšia prateľnosť celej zostavy než presný názov jednej vrchnej vrstvy.",
            ],
        },
        {
            "heading": "Loden a melton: prečo patria k sebe, ale nie sú úplne totožné",
            "paragraphs": [
                "Oba názvy sa používajú pri hustých tkaných kabátovinách s vlneným podielom a výrazným valchovaním. Melton má často krátko strihaný, veľmi súvislý povrch, ktorý môže prekryť väzbu. Loden sa spája s tradičným alpským odevom a môže mať charakteristický vlas, smerové dokončenie a konkrétnu hmotnosť. V súčasnom obchode však názvy nie sú zárukou jedného receptu ani čistého zloženia.",
                "Samostatný všeobecný návod na melton by opakoval tú istú rozhodovaciu hranicu: overiť tkaný základ, podiel vlny, úpravu povrchu a kabátovú konštrukciu. Prakticky je užitočnejšie riešiť ho v tejto rodine a porovnať s <a href=\"%s\">tvídom, ktorého väzba a priadza bývajú viditeľnejšie</a>." % ARTICLE_TWEED,
            ],
        },
        {
            "heading": "Prečo sa hotový výrobok môže splstiť ešte viac",
            "paragraphs": [
                "Valchovanie vo výrobe nevyčerpá všetku možnú rozmerovú zmenu. V ďalšom mokrom cykle sa môže zmeniť napätie priadzí, vlákna sa môžu ďalej vzájomne zachytiť a pletené slučky sa stiahnuť. Riziko rastie pri nevhodnom pohybe, teplote, prudkej zmene vody a dlhom trení. Výsledkom nemusí byť rovnomerne menší odev, ale kratší rukáv, užší predný diel alebo zvlnený lem.",
                "Mechanizmy rozmerovej zmeny rozoberá článok <a href=\"%s\">prečo sa oblečenie po praní zráža</a>. Pri vlne hodnotíte aj omak: tvrdší, hrubší a menej pružný panel naznačuje plstnatenie, nie iba dočasnú mokrú kontrakciu. Mokré miesto nenaťahujte nasilu; pretrhnutie šva alebo deformácia dielu by pridali ďalší problém." % ARTICLE_SHRINKAGE,
            ],
            "callout": {
                "title": "Výrobné valchovanie a domáce poškodenie",
                "items": [
                    "Vo výrobe sa zhutnenie plánuje pred strihaním a kontroluje sa konečný rozmer.",
                    "Doma je odev už zošitý s niťami, podšívkou, výstužou a detailmi.",
                    "Nerovnomerné plstnatenie nemožno bezpečne napraviť silným naťahovaním.",
                    "Údaj machine washable alebo symbol vlny musí patriť konkrétnemu hotovému výrobku.",
                ],
                "background": "#f7fbf8",
                "border": "#dbe5de",
            },
        },
        {
            "heading": "Ako odstrániť prach a mierny pach bez prania celého kabáta",
            "paragraphs": [
                "Kabát po nosení vyprázdnite, rozopnite a nechajte vyvetrať na širokej opore mimo dažďa, radiátora a prudkého slnka. Po úplnom vysušení povrch jemne kefujte vhodnou čistou kefou v smere dokončenia. Pracujte po menších plochách a pri bočnom svetle kontrolujte, či nemeníte vlas. Záhyby, vrecká a vnútro goliera ošetrite bez prudkého ohýbania.",
                "Pach po jednom nosení môže pochádzať z povrchovej vlhkosti, dymu alebo podšívky. Vetranie je prvý krok, nie náhrada čistenia mastného goliera či biologickej škvrny. Súvislosti vysvetľuje článok <a href=\"%s\">prečo oblečenie zapácha aj po praní</a>. Parfum neaplikujte na neznámu škvrnu, plsť ani citlivé farbivo iba preto, aby prekryl príčinu." % ARTICLE_ODOR,
            ],
        },
        {
            "heading": "Ako vyčistiť blato, soľ a mastný golier na lodene",
            "paragraphs": [
                "Čisté blato nechajte zaschnúť a voľnú zeminu odstráňte bez vtierania do vlasu. Zvyšok riešte až po kontrole etikety a skrytej skúške. Posypová soľ môže vytvárať svetlý okraj a pri opakovanom bodovaní vodou sa mapa rozširuje. Ošetrenú zónu treba zvlhčiť a odsávať rovnomerne spôsobom kompatibilným s farbou a vnútornými vrstvami.",
                "Golier spája kožný maz, pot, kozmetiku a prach. Tvrdá kefa môže síce časť nečistoty uvoľniť, ale zároveň vyhladiť alebo odtrhnúť vlákna. Rozdelenie škvŕn podľa povahy nájdete v návode <a href=\"%s\">ako odstrániť rôzne škvrny z oblečenia</a>. Pri podšitom kabáte zabráňte presiaknutiu roztoku do výstuže a miesto posúďte až úplne suché." % ARTICLE_STAIN,
            ],
        },
        {
            "heading": "Môže sa lodenový kabát prať v práčke",
            "paragraphs": [
                "Iba vtedy, keď symbol hotového kabáta výslovne povoľuje strojové pranie a výrobca uvádza vhodný program. Samotný údaj sto percent vlny, slovné spojenie varená vlna ani jednoduchý nestrapkaný rez nie sú povolením. Tvarovaný kabát môže mať lepenú prednicu, ramenné vložky, rozdielnu podšívku a gombíky, ktoré mokrý cyklus zmení.",
                "Praktický rozhodovací rámec ponúka článok <a href=\"%s\">ako prať jesenný a ľahký vlnený kabát</a>. Ak etiketa určuje profesionálne čistenie, neobchádzajte ho programom vlna. Domáci bubon nekontroluje tvar prednice a goliera ako výrobné valchovanie. Pri cennom, tmavom alebo výrazne štruktúrovanom kabáte je skúška na leme iba varovaním, nie povolením celého cyklu." % ARTICLE_WOOL_COAT,
            ],
        },
        {
            "heading": "Ako ručne prať varenú vlnu, ak to štítok povoľuje",
            "paragraphs": [
                "Ručné pranie nie je automaticky bezpečnejšie. Nádoba musí mať dostatok priestoru a voda podmienky uvedené výrobcom. Použite iba vhodný prostriedok určený na daný vlnený výrobok, najprv ho rovnomerne rozptýľte a kus jemne podopierajte. Nedrhnite dve plochy o seba, nekrúťte rukávy a nemeňte prudko teplotu medzi praním a oplachom.",
                "Vodu odstráňte spôsobom povoleným etiketou bez stáčania do povrazu. Malý prateľný doplnok položte na čistú savú podložku, zrolujte bez trenia a opäť rozložte. Pri odeve zachovajte symetriu, ale nenapínajte ho na pôvodné miery silou. Ak povrch počas manipulácie tvrdne alebo sa očká strácajú nerovnomerne, zásah zastavte.",
            ],
        },
        {
            "heading": "Ako sušiť loden a varenú vlnu bez vyťahania a zatuchnutia",
            "paragraphs": [
                "Spôsob sušenia určuje štítok. Prateľný sveter alebo doplnok sa často suší naplocho na priedušnej opore, aby mokrá hmotnosť nevyťahala ramená. Kabát môže potrebovať tvarovanú oporu alebo profesionálny postup. Woolmark pri vlnených svetroch zdôrazňuje ploché sušenie a jemné vrátenie do tvaru, no tento všeobecný princíp neprepisuje symbol konkrétneho podšitého odevu.",
                "Vzduch musí dosiahnuť rub, vrecká, golier aj podšívku. Radiátor vysuší povrch rýchlejšie než hrubý šev a môže vytvoriť teplotný rozdiel. Súvisiace praktické zásady nájdete v článku <a href=\"%s\">ako sušiť bielizeň bez zatuchnutia</a>. Odev ukladajte až vtedy, keď sa po krátkom uzavretí nevracia chlad ani vlhký pach." % ARTICLE_DRYING,
            ],
        },
        {
            "heading": "Žmolky, vyhladený vlas a lesklé lakte nie sú ten istý problém",
            "paragraphs": [
                "Žmolok je zhluk uvoľnených a zachytených vlákien na povrchu. Vyhladený vlas vzniká smerovým tlakom a trením, kým lesklý lakeť môže byť kombináciou sploštenia, mazu a tepla. Najprv povrch očistite a pozorujte z rôznych uhlov. Odstraňovač žmolkov nesmie zarezať do hustého lodenu ani zachytiť nosnú priadzu pod vlasom.",
                "Podrobné príčiny rozoberá článok <a href=\"%s\">prečo sa oblečenie žmolkuje</a>. Prístroj skúste na skrytom mieste, pracujte na pevnej rovnej opore a bez tlaku. Mechanicky zodratý alebo horúčavou zlisovaný vlas sa praním neobnoví. Pri drahom kabáte môže odborná para a kefovanie upraviť smer, ale najprv treba vylúčiť oslabenie vlákna." % ARTICLE_PILLING,
            ],
        },
        {
            "heading": "Dierka po moli, prerezaný šev a tenké miesto",
            "paragraphs": [
                "Malý kruhový otvor s oslabenými vláknami môže súvisieť so škodcom, no trhlina pri šve môže byť výsledkom napätia alebo oderu. Pred opravou skontrolujte celý odev, vrecká, prehyby a skladovací priestor. Aktívny problém nemožno vyriešiť iba zašitím jednej dierky. Textil najprv izolujte od ostatných kusov a postupujte podľa overeného režimu pre škodcu a materiál.",
                "Hustý povrch môže umožniť neviditeľné zaplstenie alebo tkanú záplatu, ale oprava musí rozložiť zaťaženie do zdravého okolia. Tvrdé lepidlo vytvára inú ohybnosť a môže byť viditeľné po daždi alebo čistení. Pri hodnotnom lodene odložte odstrihnuté vlákna a náhradnú látku; odborník ich môže použiť na farebne a štruktúrne primeranú opravu.",
            ],
        },
        {
            "heading": "Ako skladovať lodenový kabát, klobúk a vlnenú deku",
            "paragraphs": [
                "Kabát ukladajte čistý, suchý a podopretý širokým vešiakom, ktorý nepretlačí ramená. Vrecká vyprázdnite a zapínanie nechajte bez trvalého napätia. Klobúk podoprite podľa tvaru koruny a nenechajte okraj stáť pod ťažkým predmetom. Deka potrebuje voľné prehyby a suchý priedušný obal; všeobecnú starostlivosť dopĺňa článok <a href=\"%s\">ako prať vlnený pléd a deku</a>." % ARTICLE_WOOL_BLANKET,
                "Pred sezónnym uložením odstráňte pot, omrvinky a biologické škvrny, ktoré priťahujú škodcov. Priestor pravidelne kontrolujte a neuzatvárajte vlhkosť do nepriedušného vaku. Repelent používajte iba podľa jeho návodu a bez priameho neovereného kontaktu s textilom. Po vybratí kabát najprv prezrite a vyvetrajte, nie automaticky preperte.",
            ],
        },
        {
            "heading": "Ako vybrať lodenový kabát alebo metráž podľa skutočnej konštrukcie",
            "paragraphs": [
                "Pri kabáte sledujte percento vlny, hmotnosť, podšívku, výstuž, spôsob zapínania, švové rezervy a jasný ošetrovací návod. Povrch jemne stlačte, pozorujte návrat a smer vlasu a skontrolujte, či sa pri šve neotvára väzba. Označenie tradičný loden nevysvetľuje pôvod vlny, prateľnosť ani odolnosť konkrétneho hotového výrobku.",
                "Pri metráži si vyžiadajte technický list, šírku, plošnú hmotnosť, zloženie, rozmerovú zmenu a odporúčanie na predprípravu. Odrezok otestujte rovnakým spôsobom, akým sa má ošetrovať budúci výrobok, a merajte osnovný aj útkový smer. Výsledok pre metráž však stále nezahŕňa podlep, podšívku a tvarovanie, ktoré pridá krajčír.",
            ],
        },
    ],
    "table2_heading": "Loden alebo varená vlna po čistení: diagnostika konkrétnej zmeny",
    "table2_intro": "Odev hodnotíte úplne suchý, pri rovnakom svetle a bez napínania. Mokrá vlna je tmavšia, ťažšia a dočasne inak pružná.",
    "table2_headers": ["Prejav", "Pravdepodobná príčina", "Čo overiť", "Bezpečný ďalší krok"],
    "table2_rows": [
        ("Diel je menší, tvrdý a hrubší", "Ďalšie plstnatenie po vode, pohybe alebo teple.", "Rozmery, pružnosť, podšívku a ostatné diely.", "Nenapínať silou ani neopakovať cyklus; odborné posúdenie."),
        ("Ostrý kruh po lokálnom čistení", "Migrácia nečistoty, farbiva alebo zvyšku produktu pri schnutí.", "Prenos farby, rub a vnútornú výstuž.", "Nepridávať ďalšie malé kvapky; zvoliť rovnomerný odborný postup."),
        ("Povrch má guľôčky a chumáčiky", "Žmolkovanie z trenia a uvoľnených vlákien.", "Či pod zhlukom ostáva pevná tkanina a či nejde o vytiahnutú niť.", "Na skrytom mieste skúsiť šetrné odstránenie bez zásahu do základu."),
        ("Prednica alebo chlopňa má bubliny", "Rozdielna zmena vrstiev alebo uvoľnenie lepidla.", "Podšívku, lepkavosť a stav po vychladnutí.", "Nepridávať paru ani tlak; krajčírske alebo čistiace posúdenie."),
        ("Objavila sa dierka alebo tenká línia", "Škodca, oder, pretrhnutie priadze alebo namáhaný šev.", "Celý odev, sklad a pevnosť okolitého materiálu.", "Izolovať, určiť príčinu a opraviť pred ďalším nosením či čistením."),
    ],
    "steps_heading": "Bezpečný postup pri lodene a varenej vlne krok za krokom",
    "steps": [
        "Určte, či ide o tkaný loden, pletenú varenú vlnu, plsť, melton alebo zmes a prečítajte celý štítok.",
        "Skontrolujte podšívku, výstuž, kožené diely, ozdoby, švy, škodcov, staré mapy a zmenu rozmeru.",
        "Suchý prach odstráňte jemným vetraním a kefovaním v smere povrchu bez tvrdého tlaku.",
        "Škvrnu rozlíšte podľa typu a kompatibilný postup vyskúšajte na skrytom leme až do úplného vyschnutia.",
        "Celý kus perte iba pri výslovnom symbole; použite určený prostriedok, stabilné podmienky a minimum trenia.",
        "Mokrý výrobok podopierajte, nekrúťte, nenaťahujte a nevytvárajte prudký tepelný rozdiel pri oplachu.",
        "Sušte presne podľa etikety s prúdením vzduchu cez švy, vrecká, podšívku a hrubé zóny.",
        "Po vysušení porovnajte rozmery, omak, vlas, farbu a tvar; tvrdnutie alebo bubliny ďalej nezaťažujte.",
        "Pred uložením odev vyčistite, úplne vysušte, skontrolujte škodcov a použite primeranú oporu.",
    ],
    "remember": [
        "Je základ tkaný, pletený alebo netkaný a kde to možno overiť bez poškodenia?",
        "Aký podiel vlny, syntetiky, podšívky, výstuže, kože a lepidla určuje najnižší limit?",
        "Je zmena škvrna, zvyšok produktu, žmolok, vyhladený vlas, plstnatenie alebo rozlepenie?",
        "Povoľuje etiketa vodu, práčku, ručné pranie, sušičku, žehlenie a profesionálne čistenie?",
        "Má mokrý kus pri prenášaní a sušení rovnomernú oporu bez naťahovania?",
        "Je odev pred uložením čistý, suchý, bez škodcov a bez dlhodobého tlaku na tvarované diely?",
    ],
    "mistakes": [
        "Považovať výrobný názov varená vlna za povolenie ďalšieho horúceho prania.",
        "Zameniť tkaný loden, pletenú varenú vlnu a netkanú plsť iba podľa fotografie líca.",
        "Použiť bežný prací gél na vlnu bez potvrdenia kompatibility výrobcom odevu a produktu.",
        "Drhnúť mastný golier tvrdou kefou, kým sa vlas vyhladí alebo tkanina zoslabne.",
        "Krútiť mokrý kus, vešať ho za úzke ramená alebo ho sušiť na horúcom radiátore.",
        "Napínať splstnatený diel silou a poškodiť šev, podšívku alebo tvar kabáta.",
        "Zakryť vlhkosť a biologickú škvrnu vôňou a odev uložiť bez kontroly škodcov.",
    ],
    "expert_heading": "Odbornejší pohľad: plstnatenie, rozmerová zmena a skúšky odolnosti",
    "expert": [
        "IWTO opisuje spracovanie vlny ako reťazec krokov od prípravy vlákna cez pradenie, tvorbu textílie až po dokončenie. Valchovanie patrí k riadenému dokončeniu, pri ktorom sa vlastnosti plochy menia pred strihaním hotového výrobku. Domáci cyklus už pôsobí na švy, podšívku a výstuž, preto jeho výsledok nemožno zamieňať s opakovaním výrobného procesu.",
        "Woolmark vysvetľuje, že označenie machine washable súvisí s konkrétnou úpravou vlny a že symboly ošetrovania treba dodržať na hotovom výrobku. Ploché sušenie vlneného svetra chráni tvar pred mokrou hmotnosťou, ale tvarovaný kabát môže mať iný pokyn. Všeobecná vlastnosť vlákna preto nikdy neprepisuje etiketu konkrétnej zostavy.",
        "AATCC TM135 meria rozmerovú zmenu po definovaných domácich postupoch a ASTM D4966 odolnosť povrchu pri Martindaleovom odere. Výsledky odpovedajú na rozdielne otázky. Nízka rozmerová zmena vzorky nepreukazuje odolnosť vlasu, šva, lepidla ani podšívky a vysoký počet cyklov oderu nepovoľuje pranie. Porovnateľný údaj vždy potrebuje metódu, podmienky a identitu skúšanej vrstvy.",
    ],
    "source_intro": "Zdroje podporujú rozdiel medzi riadeným spracovaním vlny, prateľnosťou konkrétne upraveného výrobku, rozmerovou zmenou a oderom. Nepodporujú univerzálne domáce pranie všetkých lodenových kabátov ani bežný gél ako prostriedok na vlnu.",
    "sources": [
        ("IWTO: spracovanie vlny a chemické postupy", IWTO_PROCESSING),
        ("IWTO: odborný prehľad odolnosti vlny", IWTO_DURABILITY),
        ("Woolmark: čo znamená prateľná vlna", WOOLMARK_WASHABLE),
        ("Woolmark: vysvetlenie symbolov ošetrovania", WOOLMARK_SYMBOLS),
        ("Woolmark: ako sušiť vlnený sveter", WOOLMARK_DRY),
        ("AATCC TM135: rozmerové zmeny po praní", AATCC_DIMENSION),
        ("ASTM D4966: oder textílií metódou Martindale", ASTM_ABRASION),
        ("EÚ 1007/2011: označovanie textilných vlákien", EU_FIBRE_LABEL),
        ("GINETEX: význam symbolov ošetrovania", GINETEX),
    ],
    "related": [
        ("Čo je tvíd", ARTICLE_TWEED),
        ("Ako prať jesenný a ľahký vlnený kabát", ARTICLE_WOOL_COAT),
        ("Ako prať vlnený pléd a deku", ARTICLE_WOOL_BLANKET),
        ("Ako prať vlnený sveter, keď zapácha", ARTICLE_WOOL_SWEATER),
        ("Prečo sa oblečenie žmolkuje", ARTICLE_PILLING),
        ("Ako čítať štítok na oblečení", ARTICLE_LABEL),
        ("Ako odstrániť rôzne škvrny", ARTICLE_STAIN),
        ("Ako sušiť bielizeň bez zatuchnutia", ARTICLE_DRYING),
    ],
    "faq_title": "loden, varená vlna, plsť a vlnené kabáty",
    "faq": [
        ("Čo je loden?", "Tradične tkaná vlnená kabátovina zhutnená valchovaním a dokončená česaním, strihaním alebo lisovaním podľa požadovaného povrchu."),
        ("Čo je varená vlna?", "Obchodné označenie často používané pre vlnenú pleteninu kontrolovane zrazenú a zhutnenú mokrým procesom."),
        ("Je loden to isté ako varená vlna?", "Nie vždy. Loden je typicky tkaný, kým varená vlna býva často pletená. Konkrétny výrobca však môže názvy používať voľnejšie."),
        ("Je loden plsť?", "Nie v presnom konštrukčnom zmysle. Loden má tkaný základ; pravá plsť je netkaná vláknová plocha."),
        ("Čo je melton?", "Hustá silno valchovaná tkaná kabátovina s krátko strihaným súvislým povrchom, príbuzná lodenu, ale nie automaticky totožná."),
        ("Môže sa lodenový kabát prať v práčke?", "Iba ak to výslovne povoľuje štítok celého kabáta. Mnohé tvarované a podšité kusy patria do čistiarne."),
        ("Na koľko stupňov prať varenú vlnu?", "Jedna univerzálna teplota neexistuje. Dodržte symbol konkrétneho výrobku a vyhnite sa teplotným zmenám a silnému pohybu."),
        ("Aký prací prostriedok použiť na loden?", "Len produkt výslovne kompatibilný s vlnou a s návodom konkrétneho výrobku. Bežný gél nie je automaticky vhodný."),
        ("Ako odstrániť blato z lodenového kabáta?", "Nechajte ho zaschnúť, voľnú zeminu jemne odstráňte a zvyšok riešte podľa etikety po skrytej skúške bez tvrdého drhnutia."),
        ("Ako odstrániť žmolky z varenej vlny?", "Až na úplne suchom kuse, veľmi opatrne a po skúške. Nezachyťte nosnú priadzu ani nezrežte súvislý povrch."),
        ("Dá sa zrazený loden natiahnuť späť?", "Silné splstnatenie býva trvalé. Násilné naťahovanie môže deformovať diel alebo pretrhnúť švy; zvoľte odborné posúdenie."),
        ("Ako sušiť varenú vlnu?", "Presne podľa etikety, často naplocho s rovnomernou oporou a mimo radiátora. Kabát môže mať odlišný profesionálny postup."),
        ("Ako skladovať lodenový kabát?", "Čistý a úplne suchý na širokom vešiaku v priedušnom priestore, po kontrole škvŕn a škodcov."),
        ("Prečo má loden po čistení vodný kruh?", "Nečistota, farbivo alebo zvyšok produktu mohli migrovať k okraju vlhkej zóny. Ďalšie malé kvapky môžu kruh zväčšiť."),
    ],
}

add_cards(
    LODEN,
    product_heading="Bežný prací gél nie je prostriedok na loden ani varenú vlnu",
    product_intro="Konkrétny gél možno zvažovať iba pri výrobku, ktorého etiketa povoľuje domáce pranie a ktorého zloženie je s produktom výslovne kompatibilné.",
    product_text="Prací gél Vevo je určený na kompatibilnú bežnú prateľnú bielizeň. Pred použitím overte návod produktu, symboly odevu a skryté miesto; koncentrát nikdy nelejte priamo na vlnený povrch.",
    product_limit="Táto karta nie je odporúčaním použiť gél na vlnu. Pri lodene, varenej vlne, plsti, podšitom kabáte alebo odeve určenom na profesionálne čistenie zvoľte prostriedok a postup určený výrobcom, prípadne čistiareň.",
    category_heading="Pracie gély patria iba ku kompatibilnej prateľnej bielizni",
    category_intro="Kategóriu má význam prezerať až po potvrdení, že konkrétny výrobok nie je vlnený, citlivo zmesový ani určený na profesionálne čistenie.",
    category_text="V kategórii nájdete gély pre rôzne druhy bežnej prateľnej bielizne. Na loden a varenú vlnu vyberajte výlučne špecializovanú starostlivosť potvrdenú štítkom a výrobcom produktu.",
)


ARTICLES: list[dict[str, object]] = [HERRINGBONE, VOILE, TAFFETA, LODEN]


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
        raise SystemExit("Batch 53 link preflight failed")
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
