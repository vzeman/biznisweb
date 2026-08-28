#!/usr/bin/env python3
"""Build and validate VEVO batch 55 textile-pattern and finish articles."""

from __future__ import annotations

import json
import re
from pathlib import Path

import build_batch_51_woven_surfaces_and_yarns as batch51
from build_batch_53_weaves_curtains_formal_fulled_wool import (
    FIXED_PRICE_RE,
    FORBIDDEN_PUBLIC_RE,
    WORD_RE,
    jaccard,
    preflight_links,
    seven_word_shingles,
    visible_text,
)
from build_batch_49_household_material_systems import render_article


PUBLISH_DATE = "2026-08-28"
CANDIDATES = Path("content/VEVO_CONTENT/batches/batch-55-candidates-2026-08-28.txt")
OUT_JSON = Path("content/VEVO_CONTENT/imports/batch-55-2026-08-28-articles.json")
OUT_PREFLIGHT = Path("content/VEVO_CONTENT/exports/batch-55-2026-08-28-link-preflight.json")

EU_FIBRE_LABEL = "https://eur-lex.europa.eu/eli/reg/2011/1007/oj"
GINETEX = "https://www.ginetex.net/share/article/4201/care-symbols"
AATCC_STANDARDS = "https://www.aatcc.org/testing/standards"
FIT_GLOSSARY = "https://sites.fitnyc.edu/depts/museum/TailorsArt/MenswearFabricsGlossary.htm"
TUL_PATTERNS = "https://dspace.tul.cz/items/86f44e36-1197-4999-aa66-a45a0e8754e8"
TUL_BARCHET = "https://dspace.tul.cz/bitstream/handle/15240/3524/bc_13448.pdf?sequence=1"
GETTY_FUSTIAN = "https://www.getty.edu/vow/AATFullDisplay?find=fustian&logic=AND&note=&subjectid=300132887"
NCSU_DOBBY = "https://repository.lib.ncsu.edu/bitstreams/b9488f0c-e979-4c91-bd60-7ed92fdbc855/download"
PRATT_DOBBY = "https://textileresearchlab.pratt.edu/classroom-kits/woven-exploration"
GETTY_DOBBY = "https://www.getty.edu/vow/AATFullDisplay?find=&logic=&note=&subjectid=300417868"
GETTY_MOIRE = "https://www.getty.edu/vow/AATFullDisplay?find=300214627%5C&logic=null&note=%5C&subjectid=300400593"
MCI_MOIRE = "https://mci.si.edu/node/1317445"
NPS_TEXTILES = "https://www.nps.gov/subjects/museums/upload/MHI_AppK_TextilesObjects.pdf"

ARTICLE_LABEL = "/n/ako-citat-stitok-na-obleceni-material-symboly-prania-a-spravny-program"
ARTICLE_STAIN = "/n/ako-odstranit-zuvacku-krv-vosk-a-ine-skvrny-z-oblecenia"
ARTICLE_COLOR = "/n/stalofarebnost-textilu-preco-farby-blednu-pri-prani-svetle-a-treni"
ARTICLE_SHRINKAGE = "/n/preco-sa-oblecenie-zrazi-po-prani-teplota-vlakna-susicka-a-prevencia"
ARTICLE_DRYING = "/n/ako-susit-bielizen-v-malom-byte-bez-zatuchnutia"
ARTICLE_IRONING = "/n/ako-vyzehlit-koselu-kompletny-sprievodca-pre-dokonaly-vysledok"
ARTICLE_PILLING = "/n/preco-sa-oblecenie-zmolkuje-vlakna-trenie-pranie-a-susenie"
ARTICLE_SNAGGING = "/n/zatrhavanie-textilu-preco-vznikaju-vytiahnute-ocka-a-ako-im-predchadzat"
ARTICLE_FLANNEL = "/n/ako-prat-flanelove-obliecky-aby-zostali-maekke"
ARTICLE_MOLESKIN = "/n/co-je-moleskin-husta-brusena-bavlna-lesk-a-spravne-pranie"
ARTICLE_RIPS = "/n/co-je-rips-priecne-rebrovana-tkanina-oder-a-spravna-starostlivost"
ARTICLE_DAMASK = "/n/co-je-damask-obojstranny-tkany-vzor-a-starostlivost-o-obrusy-a-obliecky"
ARTICLE_PIQUE = "/n/co-je-pike-reliefna-pletenina-porovitost-a-spravne-pranie"
ARTICLE_SATIN = "/n/co-je-saten-nie-je-to-vzdy-hodvab-a-ako-ho-spravne-prat"
ARTICLE_TAFFETA = "/n/co-je-taft-sustava-tkanina-vodne-mapy-a-bezpecne-cistenie"
ARTICLE_FORMAL = "/n/ako-prat-spolocenske-saty-doma-a-kedy-zvolit-cistiaren"
ARTICLE_TWEED = "/n/co-je-tvid-hrubsia-vlnena-tkanina-zmolky-a-cistenie"
ARTICLE_HERRINGBONE = "/n/co-je-rybia-kost-lamany-keper-smer-vzoru-a-spravne-pranie"
ARTICLE_MADRAS = "/n/co-je-madras-karovana-koselovina-pustanie-farby-a-spravne-pranie"
ARTICLE_POPLIN = "/n/co-je-popelin-hladka-koselova-tkanina-vlastnosti-a-starostlivost"
ARTICLE_OXFORD = "/n/co-je-oxfordska-tkanina-koselova-vazba-golier-a-spravne-pranie"

PRODUCT_NAME = "Prací gél hypoalergénny Vevo Ylang Absolute 1L"
PRODUCT_URL = "/p-1627/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l"
CATEGORY_NAME = "Pracie gély"
CATEGORY_URL = "/c/vevo-home-care/pranie/praci-gel"


def add_cards(article: dict[str, object], *, noun: str, limit: str) -> None:
    article.update(
        {
            "product_heading": f"Prací gél použite iba na prateľný {noun}",
            "product_intro": (
                "Ak etiketa celého výrobku povoľuje domáce pranie a všetky vlákna, farbivá "
                "a úpravy sú kompatibilné, tekutý gél možno presne odmerať podľa vody a náplne."
            ),
            "product_name": PRODUCT_NAME,
            "product_url": PRODUCT_URL,
            "product_text": (
                "Tekutý prostriedok sa dá dávkovať bez sypkého zvyšku. Koncentrát nelejte "
                "priamo na suchý vzor, počesaný povrch, lesklú plochu ani poškodený šev."
            ),
            "product_limit": limit,
            "category_heading": "Prací prostriedok vyberajte podľa vlákna a hotového výrobku",
            "category_intro": (
                "Názov vzoru, väzby alebo povrchovej úpravy nie je prací program. Najprv "
                "skontrolujte zloženie, farbu, podšívku, výstuž, ozdoby a všetky symboly."
            ),
            "category_name": CATEGORY_NAME,
            "category_url": CATEGORY_URL,
            "category_text": (
                "V kategórii nájdete gély pre rôzne potreby bežnej prateľnej bielizne. "
                "Vyberte iba kompatibilný výrobok, dodržte dávku a nechajte priestor na oplach."
            ),
        }
    )


PEPITO: dict[str, object] = {
    "title": "Čo je pepito, kohútia stopa a Glen check: rozdiely vo vzore, farbe a starostlivosti",
    "link": "co-je-pepito-kohutia-stopa-a-glen-check-rozdiely-vo-vzore-farbe-a-starostlivosti",
    "meta": "Ako rozoznať pepito, kohútiu stopu a Glen check, odlíšiť tkaný vzor od potlače a správne prať, sušiť a žehliť károvaný odev bez blednutia a deformácie.",
    "short": "Pepito, kohútia stopa a Glen check sú príbuzné kárované motívy, nie názvy vlákien. O praní nerozhoduje veľkosť či tvar kára, ale zloženie, spôsob vytvorenia vzoru a konštrukcia celého odevu.",
    "name": "pepito, kohútia stopa a Glen check",
    "locative": "odeve s károvaným vzorom",
    "identity_heading": "Vzor, väzba a vlákno sú tri odlišné informácie",
    "identity_detail": "Pepito môže vzniknúť farebným striedaním nití v plátnovej väzbe, kohútia stopa spojením kontrastných skupín nití s lomeným zúbkovaným obrysom a Glen check skladaním viacerých drobných kár a prúžkov.",
    "identity_boundary": "Rovnaký motív môže byť utkaný z vlny, bavlny alebo syntetickej zmesi, vytlačený na plátne, vytvorený pletením alebo žakárovou konštrukciou, a každá verzia má iné riziká.",
    "label_focus": "percentá vlákien, podšívku, lepenú výstuž, elastan, farbenie priadze alebo potlač, kovové detaily, kožu, gombíky, povolené odstreďovanie a žehlenie",
    "missing_label": "Pri metráži žiadajte technický list a odmerajte skúšobný odstrižok; pri saku bez etikety nepredpokladajte prateľnosť iba preto, že podobné káro poznáte z bavlnenej košele.",
    "dry_check": "napojenie kára v švoch, vytiahnuté nite, lesklé lakte, odretý sed, skrútenú nohavicu, zvlnenú podšívku, svetlé lomy, farebný prenos a staré lokálne mapy",
    "damage_boundary": "Prach, pot alebo škvrnu možno čistiť, no nesprávne zostrihnutý smer, trvalo skosený diel, odretá priadza či teplom vyleštené miesto sa ďalším praním neopraví.",
    "test_focus": "Skúšku vykonajte na každej kontrastnej farbe a sledujte aj posun línií, zmenu uhla kára a rozdiel odrazu po úplnom vysušení.",
    "combined_risk": "uvoľnenia farbiva, napučania rozdielnych priadzí, posunu väzby, skosenia dielu a odlišného zrazenia vrchnej látky, podšívky a výstuže",
    "chemistry_boundary": "Pot na golieri, mastnota, blato a prenesené farbivo potrebujú odlišný prvý krok; silné bodové bielenie môže prerušiť pravidelnosť motívu viditeľnejšie než na jednofarebnom odeve.",
    "drying_detail": "Nohavice urovnajte podľa bočných švov a smeru osnovy, košeľu otvorte pri golieri a manžetách a podšité sako nevystavujte mokrej hmotnosti bez tvarovej opory.",
    "heat_boundary": "Vysoká teplota môže zraziť jednu vláknovú zložku, zvlniť lepenú chlopňu, zvýrazniť skosenie, poškodiť elastan alebo vytvoriť lesklý obdĺžnik cez tmavé časti vzoru.",
    "stop_signs": "silný prenos jednej farby do druhej, rozostúpenie väzby, zväčšovanie skosenia, zvlnenie chlopne, lepkavá úprava, otvorenie šva alebo rastúci lesk",
    "professional_boundary": "Bežnú prateľnú košeľu možno ošetrovať doma podľa etikety, kým vlnený oblek, podšité sako, plisovaný alebo historický odev a výrobok so symbolom profesionálneho čistenia potrebuje odborný postup.",
    "answer": "Pepito je zvyčajne drobné pravidelné blokové káro, kohútia stopa má špicaté zúbkované výbežky a Glen check skladá viac jemných kár a prúžkov do väčšieho celku. Názvy opisujú vzhľad alebo spôsob farebného vzorovania, nie konkrétne vlákno. Najprv preto zistite, či je vzor utkaný, pletený alebo vytlačený, prečítajte zloženie a ošetrovacie symboly a skontrolujte podšívku aj výstuž. Prateľný kus perte s podobnými farbami, bez preplnenia a bez nalievania koncentrátu na kontrastný motív. Urovnajte švy ešte pred vysušením a žehlite z rubu s malým tlakom. Vlnené sako alebo oblek neperte podľa rady pre bavlnenú pepito košeľu.",
    "intro": "Tieto názvy sa v móde často zamieňajú a obchod môže rovnaký motív označiť inak podľa jazyka alebo mierky. Pre starostlivosť je však ešte dôležitejšia druhá zámena: vzhľad kára nie je zloženie tkaniny. Čiernobiela košeľa s potlačou, vlnené nohavice s farebne tkanou kohúťou stopou a podšité sako s Glen checkom môžu na fotografii pôsobiť príbuzne, no voda, trenie a teplo na ne pôsobia odlišne. Správny postup preto začína identifikáciou vrstiev a až potom rieši škvrnu, cyklus, sušenie a žehlenie.",
    "quick": [
        "<strong>Pepito:</strong> drobný pravidelný kontrastný blokový efekt, často založený na plátnovej väzbe a farebnom poradí nití.",
        "<strong>Kohútia stopa:</strong> lomené štvorce so špicatými výbežkami, v angličtine houndstooth alebo pri menšej mierke dogtooth.",
        "<strong>Glen check:</strong> zložené káro vytvorené kombináciou jemných pruhov a blokov; variácia sa spája aj s názvom Prince of Wales check.",
        "<strong>Rub veľa prezradí:</strong> tkaný motív tvorí štruktúra nití, kým potlač môže mať rub slabší alebo bez kresby.",
        "<strong>Vzor neurčuje pranie:</strong> rozhoduje vlákno, farbivo, výstuž, podšívka, šev a symboly celého výrobku.",
        "<strong>Káro odhalí deformáciu:</strong> skosenie, posun nití a nerovné zrazenie sú na pravidelnej geometrii viditeľné skôr než na jednofarebnej ploche.",
    ],
    "overview_heading": "Ako farebné nite vytvárajú pepito, kohútiu stopu a Glen check",
    "overview": [
        "V tkanej látke vedie osnova pozdĺžne a útok priečne. Keď sa v oboch sústavách pravidelne striedajú svetlé a tmavé skupiny nití, ich križovanie vytvára čisté, zmiešané a kontrastné plochy. Technická univerzita v Liberci pri klasických vzoroch ukazuje, že výsledok neurčuje iba farba, ale aj poradie nití a väzba. Pepito sa preto nedá spoľahlivo opísať iba ako malé štvorčeky bez pohľadu na konštrukciu.",
        "Kohútia stopa využíva členenie a väzobný posun tak, aby hranice blokov získali predĺžené zuby. Pri zmenšení sa v obchode používa aj názov dogtooth, no mierka nie je prísny ošetrovací parameter. Glen check skladá drobnejšie kárované jednotky do väčšieho opakovania a môže pridať kontrastný overcheck. Museum at FIT ho opisuje ako malú tkanú kocku a houndstooth ako špicatý efekt z kontrastných skupín priadzí.",
        "Moderná výroba môže tieto vzhľady napodobniť potlačou, pletením alebo žakárovým riadením. Spotrebiteľ preto nemá z názvu automaticky vyvodzovať, že ide o vlnu ani že motív prechádza celou hrúbkou. Rozpoznanie spôsobu vzorovania pomáha predvídať prenos farby, zachytávanie a skosenie, no povolený program stále určuje štítok hotového výrobku.",
    ],
    "table1_heading": "Pepito, kohútia stopa, shepherd's check a Glen check",
    "table1_intro": "Názvy nemajú vo všetkých obchodoch úplne jednotnú hranicu. Tabuľka uvádza praktické rozlišovacie znaky, nie povolenie na konkrétny cyklus.",
    "table1_headers": ["Vzor", "Typický vzhľad", "Ako vzniká", "Čo sledovať pri starostlivosti"],
    "table1_rows": [
        ("Pepito", "Drobné pravidelné bloky s mäkším alebo bodkovaným prechodom.", "Farebné poradie osnovy a útku, často s jednoduchšou väzbou.", "Prenos kontrastných farieb, rozmer a rovnosť mriežky."),
        ("Kohútia stopa / houndstooth", "Lomené štvorce so štyrmi špicatými výbežkami.", "Kontrastné skupiny priadzí a väzobný posun.", "Zachytenie, lesk, skosenie a napojenie dielov."),
        ("Shepherd's check", "Malé pravidelné dvojfarebné káro, často čierno-biele.", "Jednoduchšie striedanie farebných skupín.", "Stálofarebnosť a presnosť švov; obchodný názov sa môže prekrývať s pepitom."),
        ("Glen check / Glen plaid", "Viac mierok prúžkov a kár v jednom zloženom opakovaní.", "Kombinácia jemných farebných pásov a väzobných efektov.", "Väčšie opakovanie zvýrazní zlé strihanie a nerovné zrazenie."),
        ("Potlačená imitácia", "Podobný motív na ľubovoľnom podklade.", "Pigment alebo farbivo nanesené po vytvorení plochy.", "Odolnosť tlače, rub, trenie a teplota žehlenia."),
    ],
    "sections": [
        {
            "heading": "Ako doma rozlíšiť tkaný vzor od potlače a úpletu",
            "paragraphs": [
                "Prezrite líc aj rub pod lupou. Pri farebne tkanom vzore uvidíte jednotlivé svetlé a tmavé nite pokračovať pozdĺž a naprieč, hoci rub nemusí byť identický. Potlač často sedí na jednej strane priadzí, na ohybe môže odhaliť svetlejší základ a rub býva nevýrazný. Pletenina ukáže slučky a väčšiu pružnosť, nie pravidelné pravouhlé kríženie osnovy a útku.",
                f"Tento test neurčuje vlákno. Polyesterová a vlnená tkanina môžu mať rovnaký tkaný motív, zatiaľ čo bavlnený úplet môže niesť vernú tlač kohútej stopy. Výsledok spojte s <a href=\"{ARTICLE_LABEL}\">údajmi na materiálovom a ošetrovacom štítku</a>. Na hotovom odeve nepárajte šev a nepoužívajte skúšku horením; technický list alebo údaje výrobcu sú bezpečnejšie.",
            ],
        },
        {
            "heading": "Ako rozoznať pepito a kohútiu stopu bez hádky o obchodný názov",
            "paragraphs": [
                "Začnite obrysom jednej opakujúcej sa jednotky. Ak pôsobí ako kompaktný drobný blok bez výrazných dlhých zubov, označenie pepito alebo shepherd's check býva praktické. Ak zo štvorca vystupujú špicaté predĺženia a motív pôsobí dynamickejšie, ide skôr o kohútiu stopu. Pri veľmi malej mierke sa môže objaviť názov dogtooth.",
                "Hranica nie je medzinárodne rovnaká a samotné pomenovanie nezmení údržbu. Dôležité je zdokumentovať mierku, farby, rub a spôsob vytvorenia. Pri reklamácii alebo nákupe metráže je fotografia celej plochy aj detailu užitočnejšia než spoliehanie sa na jeden preložený názov. Nesprávny módny termín nie je dôvodom použiť odlišný prací prostriedok.",
            ],
        },
        {
            "heading": "Glen check, Glen plaid a Prince of Wales check",
            "paragraphs": [
                "Glen check skladá jemné svetlé a tmavé pásy do väčších polí, takže z diaľky vidíte veľké káro a zblízka drobné členenie. Názov Prince of Wales check sa často používa pre variáciu s ďalším kontrastným károm, no predajné označenia nie sú vždy dôsledné. Pre čistenie si namiesto názvu všimnite počet farieb, veľkosť opakovania a to, či je odev podšitý.",
                "Veľké opakovanie funguje ako meracia mriežka. Po praní môže odhaliť skrútený bočný šev, inú zmenu dĺžky podšívky alebo skosenie nohavice. Pred prvým cyklom odmerajte vzdialenosť medzi rovnakými bodmi motívu, nie medzi pohyblivými špičkami. Výsledok posudzujte až úplne suchý a bez násilného naťahovania.",
            ],
        },
        {
            "heading": "Pepito košeľa: golier, manžety, pot a kontrastné farby",
            "paragraphs": [
                "Na košeli sa nečistota sústreďuje na golieri, manžetách a v podpazuší, zatiaľ čo zvyšok môže byť iba zaprášený. Mastné miesto najprv ošetrite kompatibilným produktom v malej dávke a bez tvrdej kefy. Na čiernobielom vzore bodové zosvetlenie prerušuje pravidelnosť, preto skúšku urobte na skrytom šve a sledujte obe farby.",
                f"Ak etiketa povoľuje práčku, košeľu perte s podobne farebnými ľahkými kusmi a nechajte priestor na pohyb. Koncentrát nelejte na suchý golier. Súvisiaci postup pre hladšiu košeľovinu nájdete pri <a href=\"{ARTICLE_POPLIN}\">popelíne</a> a zrnitejšiu konštrukciu pri <a href=\"{ARTICLE_OXFORD}\">Oxforde</a>; vzor však neprenáša ich vlastnosti na váš konkrétny kus.",
            ],
        },
        {
            "heading": "Kohútia stopa na nohaviciach a sukni: sed, kolená a skosenie",
            "paragraphs": [
                "Sed, kolená a vnútorné stehná nesú tlak a trenie, ktoré môže vrch priadze vyhladiť. Svetlejšia plocha bez hmatateľného zvyšku nemusí byť škvrna, ale zmena odrazu. Najprv odstráňte povrchovú nečistotu a porovnajte miesto pod bočným svetlom. Opakované drhnutie alebo horúca para mechanický lesk spravidla zhorší.",
                f"Pred praním zapnite bezpečné zapínanie, vyprázdnite vrecká a odfoťte bočný šev voči mriežke. Po cykle nohavice prenášajte s oporou, urovnajte podľa konštrukcie a sušte podľa symbolu. <a href=\"{ARTICLE_SHRINKAGE}\">Rozmerová zmena textilu</a> a skosenie nie sú totožné: kus môže zachovať dĺžku, ale šev sa otočí pre uvoľnenie napätia.",
            ],
        },
        {
            "heading": "Vlnené sako a oblek s károm: prečo metráž nerozhoduje sama",
            "paragraphs": [
                f"Vlnená vrchná látka môže mať <a href=\"{ARTICLE_TWEED}\">tvídový charakter</a> alebo hladšie oblekové dokončenie. Hotové sako však pridáva podšívku, výstuž chlopní, ramenné diely, plátno alebo lepidlo, tvarovanie parou a množstvo švov. Voda, ktorú znesie samostatná vzorka, môže zvlniť prednicu alebo zmeniť pomer vrstiev.",
                "Medzi noseniami sako vyvetrajte, prach odstráňte čistou mäkkou kefou v konzistentnom smere a škvrnu presne opíšte čistiarni. Ak je povolené iba profesionálne čistenie, domáce ručné pranie nie je jemnejšia verzia toho istého procesu. Pri cennom alebo novom kuse neprežehľujte škvrnu pred posúdením, aby sa teplom nezafixovala.",
            ],
        },
        {
            "heading": "Stálofarebnosť: keď tmavá niť zafarbí svetlé políčko",
            "paragraphs": [
                f"Kontrastný tkaný vzor prináša farby do tesného kontaktu. Uvoľnené farbivo sa môže počas mokrého cyklu preniesť na susednú svetlú priadzu a rozmazať ostré hranice. Pred prvým praním preto urobte skúšku a riaďte sa <a href=\"{ARTICLE_COLOR}\">zásadami stálofarebnosti pri praní a trení</a>. Zachytávač farieb nie je povolenie prekročiť štítok ani záruka proti migrácii.",
                "Ak sa farba prenáša už pri skúške, nepokračujte vyššou teplotou ani dlhším namáčaním. Pri novom výrobku zdokumentujte stav. Na historickom alebo vlnenom kuse môže byť mokré čistenie nevhodné aj bez viditeľného prenosu, pretože rizikom je rozmer a povrch. Bielenie svetlých polí samostatne môže zasiahnuť tmavé nite na ich hranici.",
            ],
        },
        {
            "heading": "Ako odstrániť škvrnu bez rozbitia pravidelnej geometrie",
            "paragraphs": [
                f"Kvapalinu odsajte bielou savou tkaninou a pevnú nečistotu zdvihnite tupou hranou. Postup vyberte podľa pôvodu; orientáciu poskytuje <a href=\"{ARTICLE_STAIN}\">sprievodca rôznymi škvrnami</a>. Pracujte od okraja ku stredu, nepremočte bez potreby veľký štvorec a dodržte kontaktný čas. Na zloženej tkanine otestujte každú farbu.",
                "Po úplnom vysušení porovnajte odtieň, ostrosť hraníc, omak a uhol línií. Svetlejší bod môže byť odstránené farbivo, zvyšok produktu alebo vyhladená priadza. Prvý problém sa neopraví oplachom, druhý môže, tretí zhoršuje trenie. Preto nepokračujte automaticky silnejším prípravkom iba preto, že miesto stále vyzerá inak.",
            ],
        },
        {
            "heading": "Sušenie a žehlenie tak, aby vzor ostal rovný",
            "paragraphs": [
                f"Prateľný odev vyberte ihneď, urovnajte bočné švy, légu, golier a lem bez ťahania za jedinú diagonálu. Sušte podľa symbolu s prúdením vzduchu; praktické hranice opisuje článok <a href=\"{ARTICLE_DRYING}\">ako sušiť bielizeň bez zatuchnutia</a>. Ťažké nohavice nezaveste za jeden úzky bod a podšívku nechajte voľne dýchať.",
                f"Žehlite z rubu cez čistú ochrannú tkaninu pri najnižšej účinnej teplote. <a href=\"{ARTICLE_IRONING}\">Postup žehlenia košele</a> pomôže s poradím dielov, no káro navyše kontrolujte proti pravítku vzoru. Žehličku neposúvajte tak, aby látku naťahovala. Na vlne použite iba povolenú paru a malý tlak, inak tmavé priadze začnú meniť lesk.",
            ],
        },
        {
            "heading": "Napojenie vzoru, oprava šva a reklamácia deformácie",
            "paragraphs": [
                "Napojenie motívu pri vrecku alebo bočnom šve je otázka strihu a šitia, nie prania. Ak je jeden diel od začiatku otočený alebo posunutý, voda ho nepresunie do správnej polohy. Naopak, medzera pri šve po používaní môže znamenať posun priadzí. Miesto ďalej nezaťažujte a nestrihajte vytiahnutú niť bez posúdenia.",
                "Pri novom odeve odfoťte mriežku pred prvým praním, dodržte etiketu a po úplnom vysušení zmerajte rovnaké body. Výrazné skrútenie, prenos farby alebo zvlnenie výstuže riešte s predajcom skôr než domácou úpravou. Krajčír môže opraviť šev či rozložiť napätie, no chemické odfarbenie a zrazenú podšívku nemusí vedieť vrátiť.",
            ],
        },
        {
            "heading": "Ako vyberať károvaný odev podľa použitia, nie iba podľa motívu",
            "paragraphs": [
                f"Pri košeli skontrolujte zloženie, stálofarebnosť, golier a reálnu prateľnosť. Pri nohaviciach sledujte elastan, podšívku, rezervu šva a orientáciu motívu. Pri saku sa pýtajte na profesionálne čistenie a spôsob konštrukcie prednice. Porovnanie s <a href=\"{ARTICLE_MADRAS}\">farebne tkaným madrasom</a> pomôže pochopiť, že káro nie je jedna materiálová kategória.",
                f"Motív rybej kosti je na rozdiel od týchto kár viazaný na lomený keprový smer; vysvetľuje ho článok o <a href=\"{ARTICLE_HERRINGBONE}\">herringbone</a>. Pri nákupe preto nehodnoťte kvalitu iba podľa módneho názvu. Jasná etiketa, rovné švy, presná geometria, opraviteľné spracovanie a vhodný spôsob údržby sú užitočnejšie než tvrdenie, že konkrétny vzor je automaticky odolný.",
            ],
        },
    ],
    "table2_heading": "Károvaný odev po praní: príčina podľa viditeľného prejavu",
    "table2_intro": "Pravidelný vzor zviditeľní chyby, ktoré by na jednofarebnej ploche ostali nenápadné. Výsledok hodnotíte až po úplnom vysušení pri rovnakom svetle.",
    "table2_headers": ["Prejav", "Možná príčina", "Čo overiť", "Bezpečný ďalší krok"],
    "table2_rows": [
        ("Tmavá farba prešla do svetlého poľa", "Uvoľnenie farbiva, dlhé namáčanie alebo nevhodné podmienky.", "Skrytú skúšku, etiketu a rozsah prenosu.", "Zastaviť teplo a chémiu; nový kus zdokumentovať."),
        ("Bočný šev sa otáča cez káro", "Skosenie tkaniny alebo rozdielne uvoľnenie napätia.", "Smer osnovy, suchý rozmer a polohu pred praním.", "Nenapínať mokré; po ustálení riešiť s predajcom alebo krajčírom."),
        ("Koleno alebo lakeť sa leskne", "Mechanické vyhladenie, tlak alebo teplo.", "Či je povrch čistý a jav sa mení podľa uhla.", "Nepridávať trenie ani horúcu paru."),
        ("Pri šve sú svetlé medzery", "Posun priadzí alebo otvorenie stehu.", "Celistvosť nití a miesto najväčšieho ťahu.", "Prestať nosiť a opraviť konštrukciu."),
        ("Podšívka ťahá a káro sa vlní", "Rozdielne zrazenie vrstiev alebo zmena lepidla.", "Symbol, suché rozmery a stav výstuže.", "Nežehliť silou; odborné posúdenie."),
    ],
    "steps_heading": "Ako ošetriť prateľný pepito alebo károvaný odev krok za krokom",
    "steps": [
        "Určte, či je motív tkaný, pletený alebo tlačený, a prečítajte zloženie aj symboly celého výrobku.",
        "Odfoťte smer kára, švy, podšívku, lesklé miesta a existujúce farebné zmeny.",
        "Na skrytom mieste otestujte každú kontrastnú farbu, prostriedok a rozmer po úplnom vysušení.",
        "Škvrnu riešte podľa pôvodu s minimom trenia; koncentrát nelejte na suchý kontrastný motív.",
        "Ak je pranie povolené, triede kus podľa farby a zloženia a ponechajte priestor na pohyb a oplach.",
        "Po cykle odev podoprite, bez ťahania urovnajte švy voči vzoru a sušte podľa symbolu.",
        "Žehlite z rubu cez ochrannú tkaninu s malým tlakom a teplotou najcitlivejšej zložky.",
        "Po ustálení porovnajte farbu, smer mriežky, švy a podšívku; rastúcu deformáciu ďalej nezaťažujte.",
    ],
    "remember": [
        "Je vzor utkaný z farebných nití, pletený, žakárový alebo vytlačený?",
        "Ide o pepito, kohútiu stopu či zložený Glen check, alebo iba voľné obchodné pomenovanie?",
        "Aké vlákna, farbivá, podšívka a výstuž určujú najnižší limit?",
        "Prenáša niektorá kontrastná farba na bielu vlhkú handričku?",
        "Sú bočné švy, mriežka a podšívka rovné ešte pred praním?",
        "Povoľuje etiketa vodu, sušičku, paru a domáce žehlenie?",
    ],
    "mistakes": [
        "Považovať módny názov vzoru za údaj o vlákne alebo automatické povolenie práčky.",
        "Bieliť svetlé políčko bez kontroly tmavých nití na jeho hranici.",
        "Napínať mokré nohavice podľa diagonály motívu a zväčšiť skosenie.",
        "Prať podšité vlnené sako podľa návodu pre bavlnenú pepito košeľu.",
        "Drhnúť lesklé koleno, hoci ide o mechanicky vyhladenú priadzu.",
        "Žehliť z líca vysokým tlakom a vytvoriť lesk cez tmavé časti kára.",
    ],
    "expert_heading": "Odbornejší pohľad: farebné poradie, väzobný efekt a hodnotenie po praní",
    "expert": [
        "Technická univerzita v Liberci opisuje klasické vzory ako výsledok farebného snovania, farebného útku a väzby. Pri rovnakom poradí farieb zmení väzobný bod to, ktorá niť je na líci viditeľná, a preto aj obrys pepita či kohútej stopy. To vysvetľuje, prečo fotografia motívu neodhaľuje všetky konštrukčné informácie a prečo tlačená imitácia nemusí reagovať rovnako.",
        "Museum at FIT oddeľuje Glen plaid ako malé tkané kocky a houndstooth ako špicaté káro z kontrastných skupín priadzí. Tieto definície pomáhajú pomenovať vzhľad, ale neuvádzajú jeden prací proces. AATCC združuje samostatné skúšky pre zmenu rozmeru, stálofarebnosť, oder, švy a vzhľad; jeden úspešný výsledok preto nepreukazuje všetky ostatné vlastnosti hotového saka.",
        "GINETEX symboly určujú maximálne povolené ošetrenie pre celý výrobok a európske pravidlá štandardizujú názvy deklarovaných textilných vlákien. Vzorový názov nie je súčasťou tejto vláknovej informácie. Praktické hodnotenie má preto zaznamenať zloženie, spôsob vzorovania, rozmery, smer línií, farbu a stav komponentov pred aj po definovanom postupe.",
    ],
    "source_intro": "Zdroje podporujú rozlíšenie farebného poradia, väzby, houndstooth a Glen checku aj samostatné hodnotenie farby a rozmeru. Nepodporujú jeden univerzálny program pre každý károvaný odev.",
    "sources": [
        ("Museum at FIT: slovník pánskych tkanín a vzorov", FIT_GLOSSARY),
        ("Technická univerzita v Liberci: klasické vzory spracované tkaním", TUL_PATTERNS),
        ("AATCC: prehľad skúšobných štandardov pre textil", AATCC_STANDARDS),
        ("GINETEX: význam symbolov ošetrovania", GINETEX),
        ("EÚ 1007/2011: názvy a označovanie textilných vlákien", EU_FIBRE_LABEL),
    ],
    "related": [
        ("Ako čítať štítok na oblečení", ARTICLE_LABEL),
        ("Ako farby blednú a prenášajú sa", ARTICLE_COLOR),
        ("Čo je madras a ako sa stará o farebné káro", ARTICLE_MADRAS),
        ("Čo je rybia kosť alebo herringbone", ARTICLE_HERRINGBONE),
        ("Čo je tvíd a ako ho čistiť", ARTICLE_TWEED),
        ("Ako prať sako a kedy zvoliť čistiareň", "/n/ako-prat-sako-doma-a-kedy-ho-radsej-dat-do-cistiarne"),
    ],
    "faq_title": "pepito, kohútia stopa a Glen check",
    "faq": [
        ("Čo je pepito?", "Drobný kontrastný károvaný efekt, často vytvorený farebným poradím osnovy a útku. Názov neurčuje vláknové zloženie."),
        ("Je pepito to isté ako kohútia stopa?", "Nie úplne. Pepito býva blokovejšie a kohútia stopa má výrazné špicaté výbežky, hoci obchodné názvy sa môžu prekrývať."),
        ("Čo znamená houndstooth?", "Anglické označenie kohútej stopy; pri menšej mierke sa používa aj názov dogtooth."),
        ("Čo je Glen check?", "Zložené káro z jemných pásov a blokov, ktoré vytvárajú menšie aj väčšie opakovanie."),
        ("Je Prince of Wales check to isté ako Glen check?", "Názov sa často používa pre variant Glen checku s ďalším kontrastným károm, no predajné označenia nie sú úplne jednotné."),
        ("Ako zistím, či je vzor tkaný?", "Na rube a pod lupou sledujte farebné nite v osnove a útku. Potlač môže byť iba na líci a pletenina ukáže slučky."),
        ("Môže ísť pepito do práčky?", "Iba keď to povoľuje etiketa celého výrobku. Vzor sám o prateľnosti nič nehovorí."),
        ("Na koľko stupňov prať kohútiu stopu?", "Jedna teplota neexistuje. Rozhoduje vlákno, farbivo, podšívka, výstuž a symbol prania."),
        ("Prečo sa tmavá farba vpila do svetlého kára?", "Pravdepodobne sa uvoľnilo farbivo pri nevhodných podmienkach alebo dlhom mokrom kontakte. Ďalšie teplo môže stav zhoršiť."),
        ("Prečo sa káro po praní kriví?", "Môže ísť o skosenie tkaniny, uvoľnenie výrobného napätia alebo rozdielne zrazenie vrstiev."),
        ("Ako žehliť pepito nohavice?", "Podľa etikety, z rubu cez ochrannú tkaninu, s malým tlakom a bez naťahovania smeru vzoru."),
        ("Dá sa vyprať vlnené sako s Glen checkom doma?", "Nie podľa všeobecného návodu. Podšívka a výstuž často vyžadujú profesionálne čistenie; rozhoduje etiketa saka."),
        ("Je svetlé lesklé koleno škvrna?", "Nemusí byť. Ak po odmastení ostáva a mení sa podľa uhla svetla, môže ísť o mechanicky vyhladený povrch."),
    ],
}

add_cards(
    PEPITO,
    noun="bavlnený alebo kompatibilný zmesový károvaný odev",
    limit="Vlnený oblek, podšité sako, lepená chlopňa, kožený detail a symbol profesionálneho čistenia patria mimo tohto odporúčania.",
)


BARCHET: dict[str, object] = {
    "title": "Čo je barchet a fustian: počesané tkaniny, žmolky a správne pranie",
    "link": "co-je-barchet-a-fustian-pocesane-tkaniny-zmolky-a-spravne-pranie",
    "meta": "Čo je barchet, barchent a fustian, ako sa líšia od flanelu a moleskinu a ako prať počesanú bavlnenú tkaninu bez žmolkov, zrazenia a sploštenia vlasu.",
    "short": "Barchet je tradične bavlnená alebo zmesová tkanina s hladším lícom a počesaným rubom. Fustian je historicky širší a premenlivý názov, preto ani jeden termín nenahrádza vláknové zloženie a ošetrovací štítok.",
    "name": "barchet a fustian",
    "locative": "barchete alebo fustiane",
    "identity_heading": "Barchet a fustian nie sú jedna nemenná moderná receptúra",
    "identity_detail": "Barchet sa v stredoeurópskej terminológii opisuje ako stredne ťažká bavlnená tkanina v plátnovej alebo keprovej väzbe, často s hladším lícom a počesaným rubom, zatiaľ čo fustian historicky zahŕňal viac pevných bavlnených, ľanovo-bavlnených a vlasových variantov.",
    "identity_boundary": "Moderná metráž môže obsahovať viskózu, polyester alebo inú zmes a historický predmet môže mať odlišnú osnovu, útok, farbivo aj dokončenie, takže názov nepredpisuje jednu teplotu ani chémiu.",
    "label_focus": "presné percentá vlákien, to, ktorá strana je počesaná, farbenie, potlač, smer vlasu, podšívku, elastan, výstuž, prešívanie a povolené sušenie a žehlenie",
    "missing_label": "Pri metráži žiadajte technický list a odmerajte odstrižok pred skúšobným praním; pri starom odeve alebo prikrývke bez etikety neurčujte zloženie iba podľa mäkkého rubu.",
    "dry_check": "smer a rovnomernosť počesania, žmolky, lysé plochy, sploštené sedy, odretý golier, voľné vlákna, skrútené švy, staré škvrny, prenos farby a zatuchnutie",
    "damage_boundary": "Voľné chĺpky a nečistotu možno odstrániť, no zodratý vlas, pretrhnuté priadze, trvalo zrazený kus alebo tvrdý lesk po tlaku nie sú škvrny, ktoré napraví ďalšie pranie.",
    "test_focus": "Po skúške porovnajte líc a rub osobitne: prenos farby, zmenu výšky vlasu, tvrdnutie, žmolkovanie a rozmer hodnotíte až po úplnom vysušení a jemnom urovnaní.",
    "combined_risk": "napučania základných priadzí, trenia voľných koncov vlasu, zachytávania uvoľnených vlákien, zrazenia a sploštenia počesanej strany tlakom alebo teplom",
    "chemistry_boundary": "Kožný maz na golieri, blato, pot, voľný prach a biologická škvrna nie sú rovnaké znečistenie; silná zásada alebo bielidlo môže oslabiť farbu a zmeniť mäkký povrch skôr než odstráni problém.",
    "drying_detail": "Košeľu alebo pyžamo urovnajte po švoch, prikrývku rozložte s oporou po celej šírke a počesanú stranu nenechajte mokrú pritlačenú na hladkom nepriedušnom podklade.",
    "heat_boundary": "Horúci bubon, radiátor alebo dlhý tlak žehličky môže zraziť celulózové vlákno, sploštiť počesanie, vytvoriť lesk, poškodiť zmesovú zložku alebo ustáliť záhyb.",
    "stop_signs": "silný prenos farby, rastúca lysá plocha, rozpad povrchu na chumáče, otváranie šva, prudké zrazenie, lepkavosť, deformácia podšívky alebo vlhký pach z vnútra",
    "professional_boundary": "Nový výslovne prateľný bavlnený alebo kompatibilný zmesový barchet možno ošetrovať doma, kým vlnený flanel, podšitý odev, historický fustian, čalúnenie a neoznačený hodnotný predmet potrebujú individuálne posúdenie.",
    "answer": "Barchet, nazývaný aj barchent alebo v niektorých obchodoch flanelet, je najčastejšie bavlnená či zmesová tkanina s hladším lícom a počesaným rubom. Fustian je širší historický názov a nemožno ho automaticky preložiť ako jeden dnešný materiál. Pred praním preto zistite zloženie, väzbu, počesanú stranu a všetky súčasti výrobku. Prateľný kus otočte podľa pokynu výrobcu, oddeľte od zipsov a hrubých uterákov, použite presnú dávku a primeranú mechaniku. Mokré počesanie nedrhnite ani nežehlite z líca vysokým tlakom. Žmolky odstraňujte až na suchom pevnom povrchu a len vtedy, keď ide o uzlíky, nie o rozpad nosnej priadze.",
    "intro": "Mäkký povrch zvádza zaradiť každý barchet medzi flanel a použiť rovnaký program. Terminológia je však historicky aj regionálne premenlivá. Barchet môže mať bavlnenú osnovu, hrubší útok a počesaný rub, kým označenie fustian sa v zbierkach viaže na viac rôznych kombinácií priadzí a povrchov. Rozhodujúce je preto zistiť, čo je na vašom kuse skutočne utkané a dokončené. Praktická starostlivosť musí chrániť základnú tkaninu, voľné konce vlasu, farbu, rozmer, švy aj prípadnú podšívku.",
    "quick": [
        "<strong>Barchet býva jednostranne počesaný:</strong> hladšie líce a mäkký rub majú odlišnú reakciu na trenie a tlak.",
        "<strong>Fustian je širší historický pojem:</strong> nemožno z neho bez etikety určiť bavlnu, väzbu ani prací program.",
        "<strong>Počesanie nie je žmolok:</strong> rovnomerné jemné konce sú zámerný povrch; uzlíky a chumáče vznikajú pohybom voľných vlákien.",
        "<strong>Flanel nie je automaticky barchet:</strong> môže byť vlnený, obojstranne počesaný alebo inak dokončený.",
        "<strong>Perte s hladkými kusmi:</strong> zipsy, suchý zips a hrubé slučky zvyšujú zachytávanie a pilling.",
        "<strong>Žehlite z hladšej strany:</strong> iba ak to povoľuje etiketa a s tlakom, ktorý nesploští mäkký rub.",
    ],
    "overview_heading": "Ako vzniká hladké líce a počesaný rub barchetu",
    "overview": [
        "Technická univerzita v Liberci opisuje barchet ako stredne ťažkú bavlnenú tkaninu v plátnovej alebo keprovej väzbe s hladkým lícom a počesaným rubom. Tvrdšie a hustejšie osnovné priadze stabilizujú plochu, kým voľnejší hrubší útok poskytne viac materiálu na počesanie. Hroty pri dokončení zachytia povrchové vlákna a vytiahnu ich do jemnej vrstvy, ktorá zadržiava vzduch a mení dotyk.",
        "Počesanie nestrihá tkaninu na nový druh vlákna. Je to mechanická úprava povrchu, ktorá môže byť jednostranná, rôzne hlboká a následne strihaná či uhladená. Rovnaké bavlnené zloženie preto nemusí znamenať rovnaké žmolkovanie ani tepelný komfort. Hustota, dĺžka staplu, zákrut priadze, väzba a intenzita dokončenia spoločne určujú, koľko voľných koncov sa pri používaní pohybuje.",
        "Slovo fustian má dlhú históriu a Getty ho vedie ako skupinu pevných tkanín s premenlivým zložením a konštrukciou. To je dôležité praktické varovanie: historický názov nemožno použiť ako moderný ošetrovací symbol ani z neho bez etikety odvodiť druh vlákna.",
    ],
    "table1_heading": "Barchet, flanelet, flanel, moleskin a menčester",
    "table1_intro": "Mäkkosť môže vzniknúť odlišnou konštrukciou. Porovnanie pomáha pomenovať povrch, no o vode a teple rozhoduje konkrétne zloženie a výrobok.",
    "table1_headers": ["Materiál alebo názov", "Typický povrch", "Konštrukčná hranica", "Hlavné riziko"],
    "table1_rows": [
        ("Barchet / barchent", "Hladšie líce a počesaný rub.", "Tkaný základ, často bavlnený alebo zmesový.", "Žmolky, sploštenie rubu, zrazenie a prenos farby."),
        ("Flanelet", "Obchodné označenie ľahšej bavlnenej počesanej látky.", "Používanie názvu sa regionálne prekrýva s barchetom.", "Nespoliehať sa na názov bez etikety."),
        ("Flanel", "Mäkký počesaný povrch, niekedy z oboch strán.", "Môže byť vlnený, bavlnený alebo zmesový.", "Vlna potrebuje inú chémiu a mechaniku než bavlna."),
        ("Moleskin", "Hustý krátko brúsený líc s hladkým plným omakom.", "Pevná hustá tkanina, nie voľný rubový vlas.", "Tlakový lesk, oder a zrazenie."),
        ("Menčester", "Pozdĺžne vlasové rebrá a priehlbiny.", "Vlas je organizovaný do kordov.", "Prach medzi rebrami, sploštenie a zachytenie."),
    ],
    "sections": [
        {
            "heading": "Ako rozpoznať počesaný rub bez poškodenia vzorky",
            "paragraphs": [
                "Položte textil na rovný svetlý podklad a porovnajte obe strany pri bočnom osvetlení. Na počesanom rube uvidíte jemné nepravidelné konce prekrývajúce väzné body, zatiaľ čo líc bude čitateľnejší a kompaktnejší. Prejdite čistou dlaňou v dvoch smeroch bez tlaku; zmena odrazu ukáže orientáciu vlasu. Nite nevyťahujte a povrch neškriabte nechtom.",
                "Rovnomerné jemné chĺpky sú súčasť dokončenia. Jednotlivé tvrdé guľôčky, chumáče a holé plochy už môžu znamenať pilling alebo opotrebenie. Pri potlačenom kuse sledujte, či je farba iba na hladšom líci. Dotyk neurčí, či ide o bavlnu, viskózu alebo zmes; to potvrdí etiketa alebo technický list.",
            ],
        },
        {
            "heading": "Barchet verzus flanel: rovnaká mäkkosť neznamená rovnaké pranie",
            "paragraphs": [
                f"<a href=\"{ARTICLE_FLANNEL}\">Flanel</a> je širší názov pre mäkké počesané tkaniny a môže byť vlnený aj bavlnený. Barchet sa v miestnej technickej tradícii spája najmä s bavlneným tkaným základom a počesaným rubom. Obchod však môže termíny používať voľne. Preto najprv oddeľte vláknovú informáciu od názvu povrchu a až potom vyberajte chémiu.",
                "Vlna je citlivá na kombináciu vlhkosti, tepla, zásaditosti a mechaniky, ktorá môže spustiť plstenie. Bavlnený barchet skôr rieši zrazenie, farbu a uvoľňovanie krátkych vlákien. Zmes môže zdediť limity oboch zložiek. Ak štítok uvádza vlnu alebo profesionálne čistenie, bežný gél na prateľnú bavlnu nie je vhodným zjednodušením.",
            ],
        },
        {
            "heading": "Barchet verzus moleskin a menčester",
            "paragraphs": [
                f"<a href=\"{ARTICLE_MOLESKIN}\">Moleskin</a> má hustý kompaktný základ a jemne brúsený líc, ktorý sa tlakom môže vyleštiť. Barchet typicky využíva mäkší počesaný rub, takže trenie sa sústreďuje na voľnejšie konce. Menčester zas vytvára výrazné pozdĺžne vlasové rebrá, v ktorých sa drží prach. Jedna mäkká kategória preto nestačí na výber kefy ani programu.",
                "Pri identifikácii porovnajte smer vlasu, viditeľnosť väzby a reliéf. Krátky rovný povrch moleskinu pôsobí inak než chumáčikový rub barchetu a kordy menčestru sú hmatateľné v pásoch. Ak výrobok kombinuje tieto materiály, napríklad podšívku alebo lem, údržbu určuje najcitlivejšia časť a spôsob ich spojenia.",
            ],
        },
        {
            "heading": "Ako prať barchetové pyžamo, košeľu alebo posteľnú textíliu",
            "paragraphs": [
                "Najprv skontrolujte golier, manžety, podpazušie, gombíky a švy. Zapnite prvky, ktoré by sa mohli zachytiť, a kus otočte podľa pokynu výrobcu; počesaný rub môže byť lepšie chránený vo vnútri, no pri silnom potnom znečistení potrebuje zároveň dobrý kontakt s kúpeľom. Perte s podobne ľahkými hladkými textíliami a nepreplňujte bubon.",
                "Dávku prostriedku prispôsobte vode, náplni a znečisteniu. Nadbytok neznižuje mechanické žmolkovanie a môže zostať v hustom mäkkom povrchu. Program, teplotu a odstreďovanie prevezmite zo štítka, nie z predpokladu, že všetka bavlna znesie intenzívny cyklus. Po skončení kus hneď vyberte a bez krútenia urovnajte švy.",
            ],
        },
        {
            "heading": "Žmolky na počesanom povrchu: vznik, diagnostika a šetrné odstránenie",
            "paragraphs": [
                f"Žmolok vzniká, keď sa uvoľnené konce vlákien trením zapletú a kotviace vlákna ich držia pri povrchu. <a href=\"{ARTICLE_PILLING}\">Podrobný sprievodca žmolkovaním</a> vysvetľuje vplyv priadze, vlákna a používania. Na barchete treba odlíšiť uzlík od rovnomerného počesania; plošné zrezanie vlasu by odstránilo zámernú mäkkosť a oslabilo tenké miesto.",
                "Žmolky odstraňujte iba na úplne suchom kuse rozloženom bez napätia. Najprv skúste skrytú plochu a nastavte nástroj tak, aby nezachytil nosnú priadzu ani šev. Okolie dierky, riedku látku a starý historický kus nehoľte. Po odstránení nehľadajte hladkosť za každú cenu; opakované agresívne znižovanie povrchu skracuje životnosť.",
            ],
        },
        {
            "heading": "Uvoľňovanie chĺpkov po prvom praní a cudzie vlákna na povrchu",
            "paragraphs": [
                "Nový počesaný textil môže uvoľniť časť voľných výrobných vlákien. Rozlíšte mierny jednorazový úbytok od pokračujúceho rozpadu, pri ktorom vznikajú holé miesta, chumáče a prach pri každom dotyku. Kus pred praním nevyklepávajte v interiéri nad citlivou bielizňou a nekombinujte ho s materiálmi, ktoré chĺpky silno zachytia.",
                "Ak na povrchu po praní vidíte inú farbu vlákien, môžu pochádzať z uteráka alebo deky v rovnakej dávke. Najprv ich odstráňte šetrným valčekom alebo kefou vhodnou pre daný smer, nie ďalším horúcim cyklom. Pri rastúcom rozpade skontrolujte etiketu, mechaniku, vek a stav priadze; nový výrobok zdokumentujte pre reklamáciu.",
            ],
        },
        {
            "heading": "Škvrny na hladkom líci a mäkkom rube potrebujú iný tlak",
            "paragraphs": [
                f"Kvapalinu odsajte bez vtlačenia do rubového vlasu. Pevnú nečistotu zdvihnite tupou hranou a postup zvoľte podľa pôvodu podľa <a href=\"{ARTICLE_STAIN}\">návodu na rôzne škvrny</a>. Na líci môžete kontrolovať okraj mapy, na počesanom rube však silné trenie zmení smer a výšku chĺpkov, takže aj čisté miesto zostane opticky odlišné.",
                "Prostriedok skúšajte na oboch stranách skrytého lemu a sledujte farbu aj omak po vyschnutí. Koncentrovanú mláku nenalievajte na suchú tkaninu. Ak škvrna prenikla cez viac vrstiev pyžama, lokálne premočenie musí mať plán na oplach a úplné vysušenie. Zvyšok produktu môže mäkký povrch stvrdnúť.",
            ],
        },
        {
            "heading": "Sušenie bez zrazenia, stvrdnutia a zatuchnutia rubu",
            "paragraphs": [
                f"Mokrý kus podoprite, jemne vyrovnajte švy a sušte podľa etikety s prúdením vzduchu z oboch strán. <a href=\"{ARTICLE_DRYING}\">Vetranie pri sušení v byte</a> je dôležité najmä pri hustejšej prikrývke alebo viacvrstvovom pyžame. Počesaný rub nenechajte dlho pritlačený k nepriedušnej podložke a predmet neukladajte, kým sú švy chladné či vlhké.",
                f"Sušičku použite iba pri výslovnom symbole. Prevaľovanie môže povrch zjemniť, ale aj zvýšiť uvoľňovanie vlákien a zrazenie. Súvislosti rozoberá článok <a href=\"{ARTICLE_SHRINKAGE}\">prečo sa oblečenie po praní zráža</a>. Radiátor a horúci fén vytvárajú lokálny rozdiel; mäkký rub sa môže pri tlaku a teple sploštiť do tvrdšej plochy.",
            ],
        },
        {
            "heading": "Ako žehliť barchet a nezlisovať počesanú stranu",
            "paragraphs": [
                "Žehlenie povoľuje symbol. Pracujte z hladšej strany alebo cez čistú ochrannú tkaninu a použite mäkkú podložku, aby sa rub nestlačil o tvrdú dosku. Teplotu nastavte podľa najcitlivejšej zložky. Žehličku nenechávajte stáť a nevytvárajte vysoký tlak v snahe dosiahnuť úplne hladký vzhľad, ktorý materiálu nemusí byť vlastný.",
                f"Golier a manžety vyrovnávajte po častiach podľa <a href=\"{ARTICLE_IRONING}\">bezpečného poradia žehlenia košele</a>. Ak sa na líci objaví lesk alebo sa rub po vychladnutí nevracia, ďalšie teplo zastavte. Para môže pomôcť iba pri povolenej vláknovej zmesi; na neznámom fustiane alebo lepenom detaile nie je skúšobným postupom bez rizika.",
            ],
        },
        {
            "heading": "Historický fustian, stará prikrývka a čalúnenie",
            "paragraphs": [
                "Historický názov fustian môže pokrývať bavlnu, ľan, hodváb, vlnu a rôzne vlasové dokončenia. Staré farbivo, opravy, podšívka a oslabené sklady majú väčší význam než moderný preklad termínu. Predmet nevkladajte do vody kvôli všeobecnej rade pre barchet. Stav odfoťte, podoprite a voľný prach odstraňujte iba kontrolovaným spôsobom.",
                "Čalúnenie pridáva výplň, rám, lepidlo a neodnímateľné napätie. Povrchový fľak nemožno premočiť bez rizika migrácie nečistôt z hĺbky a pomalého schnutia. Zips na poťahu nie je povolenie práčky. Pri hodnotnom, historickom alebo konštrukčne zložitom kuse konzultujte textilného konzervátora alebo odborné čistenie.",
            ],
        },
        {
            "heading": "Ako vybrať barchetovú metráž a pripraviť ju pred šitím",
            "paragraphs": [
                "Pýtajte sa na zloženie osnovy a útku, väzbu, stranu určenú na líc, smer počesania, plošnú hmotnosť, zrážanie a odporúčanú údržbu. Rovnaký názov na dvoch rolkách nemusí znamenať rovnaký vlas ani stabilitu. Prezrite okraje, rovnomernosť farby a miesta, kde sa počesanie mení. Diely strihajte v konzistentnom smere, aby sa neodlišoval odraz.",
                "Odstrižok označte, odmerajte a ošetrite presne tak, ako plánujete hotový výrobok. Po úplnom vysušení porovnajte dĺžku, šírku, krivosť, farbu, pilling a omak oboch strán. Predpieranie má zmysel iba postupom, ktorý bude povolený aj hotovému kusu. Technický list a zvyšok metráže si odložte pre neskoršiu opravu.",
            ],
        },
    ],
    "table2_heading": "Barchet po praní: čo znamená konkrétna zmena povrchu",
    "table2_intro": "Počesaný povrch vyzerá za mokra tmavší a plochejší. Diagnózu robte až po úplnom vysušení bez agresívneho kefovania.",
    "table2_headers": ["Prejav", "Možná príčina", "Čo overiť", "Bezpečný ďalší krok"],
    "table2_rows": [
        ("Jemné chĺpky ostali na inej bielizni", "Voľné výrobné vlákna alebo silné trenie.", "Či úbytok klesá a nevznikajú lysé plochy.", "Prať s kompatibilnými hladkými kusmi; rastúci rozpad reklamovať."),
        ("Tvrdé uzlíky na rubovej strane", "Pilling z voľných koncov a trenia.", "Pevnosť podkladu a rozdiel od zámerného vlasu.", "Odstrániť až nasucho a veľmi kontrolovane."),
        ("Rub je miestami lesklý a plochý", "Tlak, žehlenie, sedenie alebo horúce sušenie.", "Či je plocha čistá a zmenil sa odraz.", "Nepridávať tlak ani teplo; opotrebenie sa nevyperie."),
        ("Odev je kratší a švy sa krútia", "Zrazenie a uvoľnenie výrobného napätia.", "Rozmery pred cyklom a úplne suchý stav.", "Nenapínať mokré; nový kus zdokumentovať."),
        ("Po uložení sa vracia vlhký pach", "Nedosušené švy alebo hustý počesaný rub.", "Chladné miesta, prúdenie vzduchu a čas sušenia.", "Znovu bezpečne vetrať; pri kontaminácii odborné čistenie."),
    ],
    "steps_heading": "Ako vyprať prateľný barchet krok za krokom",
    "steps": [
        "Overte zloženie, počesanú stranu, podšívku, ozdoby a všetky symboly hotového výrobku.",
        "Odfoťte smer vlasu, žmolky, lysé miesta, švy a rozmery ešte pred navlhčením.",
        "Na skrytom leme otestujte farbu, produkt, zmenu omaku a rozmer po vyschnutí.",
        "Škvrnu odsajte podľa pôvodu bez tvrdej kefy a bez vtlačenia koncentrátu do rubu.",
        "Ak je pranie povolené, oddeľte kus od zipsov, suchých zipsov a hrubých slučkových textílií.",
        "Použite povolený cyklus, presnú dávku, primeranú náplň a nepredlžujte namáčanie bez dôvodu.",
        "Po cykle kus podoprite, urovnajte švy a sušte podľa etikety s prístupom vzduchu k obom stranám.",
        "Povrch a rozmer posúďte až suché; žmolky odstraňujte kontrolovane a rastúce poškodenie zastavte.",
    ],
    "remember": [
        "Je mäkká strana zámerne počesaná a ktorá strana je určená na líc?",
        "Uvádza etiketa bavlnu, viskózu, polyester, vlnu alebo inú zmes?",
        "Ide o moderný barchet, voľne označený flanelet alebo historický fustian?",
        "Sú na povrchu žmolky, cudzie vlákna, lysé plochy alebo iba rovnomerný vlas?",
        "Má náplň hladké kusy bez zipsov, háčikov a suchého zipsu?",
        "Dokáže hustejší rub po cykle úplne vyschnúť aj v švoch a preloženiach?",
    ],
    "mistakes": [
        "Použiť jeden návod na každý výrobok označený barchet, fustian, flanelet alebo flanel.",
        "Oholiť celý počesaný rub, pretože jeho zámerný vlas vyzerá ako jemné žmolky.",
        "Prať mäkký povrch so suchým zipsom, otvoreným zipsom alebo hrubým uterákom.",
        "Naliať koncentrát na suché chĺpky a zanechať tvrdú lokálnu mapu.",
        "Žehliť počesanú stranu vysokým tlakom na tvrdej podložke.",
        "Zabaliť prikrývku podľa suchého líca, hoci rub a švy ostali vlhké.",
    ],
    "expert_heading": "Odbornejší pohľad: väzba, útok vhodný na počesanie a historická terminológia",
    "expert": [
        "Technická univerzita v Liberci opisuje barchet cez vzťah pevnejšej osnovy, voľnejšieho hrubšieho útku, plátnovej alebo keprovej väzby a počesaného rubu. Povrch teda nie je samostatná vrstva nalepená na podklad, ale vychádza z vlákien tkaniny. Opakované trenie môže meniť voľné konce bez toho, aby sa okamžite pretrhla nosná sústava.",
        "Getty Art & Architecture Thesaurus ukazuje, že fustian má širšie historické použitie a premenlivé materiálové aj konštrukčné podoby. Z terminologického záznamu nemožno odvodiť moderné percentá ani prateľnosť. Je však silným dôvodom, prečo historický názov nepovažovať za presný materiálový štítok.",
        "AATCC oddeľuje skúšky žmolkovania, oderu, farby, rozmeru a vzhľadu po praní. Mäkkosť po jednom cykle preto nie je úplný dôkaz životnosti a počet žmolkov nehovorí sám o pevnosti šva. GINETEX symboly určujú maximálny postup pre celý výrobok; skúsenosť s jedným bavlneným barchetom sa neprenáša na vlnený alebo historický variant.",
    ],
    "source_intro": "Zdroje podporujú jednostranne počesanú barchetovú konštrukciu a historicky široké používanie názvu fustian. Nepodporujú jeden univerzálny program pre všetky mäkké tkaniny s týmito názvami.",
    "sources": [
        ("Technická univerzita v Liberci: konštrukcia barchetu", TUL_BARCHET),
        ("Getty AAT: historické a odborné používanie pojmu fustian", GETTY_FUSTIAN),
        ("AATCC: prehľad skúšobných štandardov pre textil", AATCC_STANDARDS),
        ("GINETEX: význam symbolov ošetrovania", GINETEX),
        ("EÚ 1007/2011: názvy a označovanie textilných vlákien", EU_FIBRE_LABEL),
    ],
    "related": [
        ("Ako prať flanelové obliečky, aby zostali mäkké", ARTICLE_FLANNEL),
        ("Čo je moleskin a ako chrániť brúsený povrch", ARTICLE_MOLESKIN),
        ("Čo je rips a ako chrániť rebrovanú tkaninu", ARTICLE_RIPS),
        ("Prečo sa oblečenie žmolkuje", ARTICLE_PILLING),
        ("Prečo sa oblečenie po praní zráža", ARTICLE_SHRINKAGE),
        ("Ako čítať štítok na oblečení", ARTICLE_LABEL),
    ],
    "faq_title": "barchet, fustian a počesané tkaniny",
    "faq": [
        ("Čo je barchet?", "Najčastejšie bavlnená alebo zmesová tkanina s hladším lícom a počesaným rubom, tradične v plátnovej alebo keprovej väzbe."),
        ("Čo znamená barchent?", "Historická alebo regionálna podoba názvu barchet. Presné zloženie aj dnes treba overiť na etikete."),
        ("Čo je fustian?", "Historicky širšia rodina pevných tkanín s premenlivou osnovou, útkom a niekedy vlasovým povrchom; nejde o jedno moderné vlákno."),
        ("Je barchet to isté ako flanel?", "Nie automaticky. Termíny sa prekrývajú, ale flanel môže byť vlnený, obojstranne počesaný a konštrukčne odlišný."),
        ("Je flanelet barchet?", "Obchodné používanie sa môže prekrývať, najmä pri ľahších bavlnených počesaných látkach. Rozhodujú údaje výrobcu."),
        ("Ako zistím, ktorá strana je počesaná?", "Pri bočnom svetle uvidíte na jednej strane viac jemných voľných koncov a menej čitateľné väzné body."),
        ("Môže ísť barchet do práčky?", "Iba ak to povoľuje etiketa konkrétneho hotového výrobku a všetky jeho súčasti."),
        ("Na koľko stupňov prať barchet?", "Jedna teplota neexistuje. Zvoľte maximum zo symbolu podľa zloženia, farby a konštrukcie."),
        ("Prečo barchet po praní žmolkuje?", "Trenie uvoľnených koncov, krátke vlákna, priadza a zaťaženie môžu vytvoriť uzlíky. Vyššia dávka gélu to nevyrieši."),
        ("Môžem počesaný povrch oholiť?", "Iba opatrne odstrániť samostatné žmolky na pevnom suchom kuse. Plošné holenie by odstránilo aj zámerný vlas."),
        ("Ako barchet sušiť?", "Podľa etikety, s oporou a prúdením vzduchu z oboch strán, bez radiátora a bez uloženia vlhkých švov."),
        ("Ako barchet žehliť?", "Pri povolení z hladšej strany alebo cez ochrannú tkaninu, na mäkkej podložke a s minimálnym tlakom."),
        ("Ako čistiť historický fustian?", "Bez domáceho namáčania podľa moderného názvu. Zloženie, farbivo a stav má posúdiť textilný konzervátor."),
    ],
}

add_cards(
    BARCHET,
    noun="bavlnený alebo kompatibilný zmesový barchet",
    limit="Vlnený flanel, historický fustian, čalúnenie, podšitý odev a symbol profesionálneho čistenia potrebujú inú chémiu alebo odborný postup.",
)


DOBBY: dict[str, object] = {
    "title": "Čo je dobby tkanina: malé tkané vzory, zatrhávanie a pranie",
    "link": "co-je-dobby-tkanina-male-tkane-vzory-zatrhavanie-a-pranie",
    "meta": "Čo je dobby tkanina, ako sa líši od potlače, piké, damasku a žakáru a ako prať košeľu, uterák či obliečky bez zatrhnutia, posunu nití a zrazenia.",
    "short": "Dobby tkanina má malé často geometrické motívy vytvorené riadením skupín osnovných nití na tkáčskom stave. Dobby nie je druh vlákna, preto program prania vždy určuje zloženie a celý výrobok.",
    "name": "dobby tkanina",
    "locative": "dobby tkanine",
    "identity_heading": "Dobby opisuje spôsob riadenia väzby, nie bavlnu ani jednu gramáž",
    "identity_detail": "Dobby mechanizmus ovláda sústavy listov a skupiny osnovných nití tak, aby sa v krátkom opakovaní striedali väzby a vznikali malé bodky, kosoštvorce, prúžky alebo drobné kvetinové figúry.",
    "identity_boundary": "Rovnaký drobný tkaný motív môže byť z bavlny, ľanu, viskózy, polyesteru, hodvábu alebo zmesi a môže obsahovať dlhšie flotáže, farbené priadze či konečnú úpravu s iným limitom.",
    "label_focus": "percentá vlákien, veľkosť a typ motívu, dlhšie flotáže, farbenie priadze, elastan, kovovú niť, výšivku, podšívku, povlak, hotelovú úpravu a povolené sušenie a žehlenie",
    "missing_label": "Pri metráži žiadajte technický list a skúšajte odstrižok; pri hotovom kuse bez štítka neurčujte pranie iba podľa drobných kosoštvorcov alebo hladkého bavlneného omaku.",
    "dry_check": "vytiahnuté flotáže, slučky, medzery pri šve, zodraté vrcholy motívu, skrútenie, svetlé lomy, zaschnuté škvrny, zvyšok aviváže, prenos farby a rozdielny lesk",
    "damage_boundary": "Nečistotu a rozpustný zvyšok možno čistiť, no pretrhnutá flotáž, odstrihnutá slučka, posunuté nite alebo teplom sploštený motív sa ďalším praním neobnoví.",
    "test_focus": "Na skrytej časti sledujte farbu, rozmer, výšku a ostrosť motívu, zachytenie nechtom bez ťahania a rozdiel medzi lícom a rubom po úplnom vysušení.",
    "combined_risk": "napučania priadzí, pohybu lokálnych flotáží, zachytenia o tvrdý detail, posunu nití pri šve a rozdielneho zrazenia väzobných oblastí alebo vrstiev výrobku",
    "chemistry_boundary": "Mastný golier, škrob, kozmetika, pigment a zatuchnutie potrebujú odlišné riešenie; silné lokálne drhnutie môže prerušiť jemnú figúru alebo odfarbiť iba jej vystupujúcu časť.",
    "drying_detail": "Košeľu otvorte pri golieri a manžetách, uterák rozložte bez prevesenia za jednu flotáž a obliečku vytraste v rohoch, aby voda nezostala v zložených švoch.",
    "heat_boundary": "Vysoké teplo môže zraziť celulózové vlákno, poškodiť syntetickú zložku, zvýrazniť rozdiel väzieb, zvlniť šev alebo sploštiť reliéf drobného motívu.",
    "stop_signs": "rastúce vytiahnutie, prenos farby, otváranie šva, zväčšovanie deformácie motívu, lepkavý povrch, kovová niť meniaca farbu alebo poškodenie podšívky",
    "professional_boundary": "Bežnú výslovne prateľnú bavlnenú dobby košeľu, uterák alebo obliečku možno ošetrovať doma, zatiaľ čo hodváb, kovové priadze, historický textil, čalúnenie, povlak a profesionálny symbol potrebujú individuálny postup.",
    "answer": "Dobby je tkanina s malými opakovanými figúrami, ktoré vznikajú pri tkaní ovládaním skupín osnovných nití. Motív teda nie je iba vytlačený na hotovom povrchu, no názov dobby nehovorí, z akého vlákna je látka ani na akú teplotu sa perie. Pod lupou skontrolujte líc aj rub, hľadajte dlhšie flotáže a vytiahnuté nite a prečítajte celý štítok. Prateľný kus oddeľte od otvorených zipsov, háčikov a suchého zipsu, neprepĺňajte bubon a koncentrát nelejte na suchý reliéf. Po cykle ho bez ťahania urovnajte a žehlite z rubu s malým tlakom. Kovová, hodvábna, podšitá alebo profesionálne čistiteľná dobby tkanina nepatrí do bežného programu iba preto, že motív je drobný.",
    "intro": "Dobby sa v popise košieľ, obrusov, uterákov a obliečok často objaví bez vysvetlenia a ľudia ho zamieňajú s potlačou alebo žakárom. Technicky ide o spôsob vytvárania menších väzobných figúr, nie o samostatnú surovinu. To prináša dve praktické otázky: kde motív vytvára dlhší nechránený úsek priadze a aké vlákno či úprava ho nesie. Správna starostlivosť musí zachovať farbu, opakovanie, šev aj lokálne flotáže a zároveň rešpektovať golier, podšívku, kovovú niť či funkčnú úpravu hotového výrobku.",
    "quick": [
        "<strong>Motív je utkaný:</strong> dobby mechanizmus mení zdvih skupín osnovných nití a vytvára malé často geometrické opakovania.",
        "<strong>Dobby nie je vlákno:</strong> bavlna, ľan, viskóza, syntetika aj hodváb môžu niesť podobný vzor.",
        "<strong>Rub pomáha pri identifikácii:</strong> väzobná figúra pokračuje konštrukciou, kým tlač môže byť na rube slabá.",
        "<strong>Flotáž sa môže zachytiť:</strong> čím dlhšie ide niť bez previazaného bodu, tým viac ju treba chrániť pred háčikom a zipsom.",
        "<strong>Malý vzor neznamená jednoduchú údržbu:</strong> kovová priadza, farba, povlak alebo podšívka môžu byť najcitlivejšie.",
        "<strong>Žehlite s nízkym tlakom:</strong> reliéf a rozdielny odraz väzieb možno sploštiť aj bez viditeľného spálenia.",
    ],
    "overview_heading": "Ako dobby mechanizmus vytvára malé opakované figúry",
    "overview": [
        "Pri tkaní sa osnovné nite zdvíhajú v skupinách, aby medzi nimi prešiel útok. Dobby zariadenie vyberá kombinácie listov podľa opakovania a umožňuje striedať viac väzobných stavov než jednoduchý pravidelný pohon. North Carolina State University opisuje dobby ako malé figurované vzory, často geometrické alebo drobne kvetinové, vytvorené jednou osnovou a jedným útkom. V jednofarebnom variante ich zviditeľní rozdiel odrazu medzi väzbami.",
        "Pratt Textile Research Lab ho umiestňuje medzi jednoduché základné väzby a zložitejší jacquard: dobby manipuluje osnovnými skupinami tak, aby vytvoril malú textúru, kým žakárový mechanizmus dokáže riadiť jednotlivé osnovné nite pre rozsiahlejšie obrazce. Gettyho terminologický záznam rovnako zdôrazňuje malé často opakované geometrické figúry a jednoduchší rozsah mechanizmu.",
        "Toto rozdelenie je užitočné pri identifikácii, nie pri predpise prania. V jednom opakovaní môže byť plátnová plocha, keprový bod, drobná flotáž alebo farebná priadza. Výslednú pevnosť a zachytávanie ovplyvňuje dĺžka väzného úseku, hustota, zákrut a kvalita nite. Hotový výrobok navyše pridáva švy, okraje a úpravy, ktoré laboratórna vzorka väzby nemá.",
    ],
    "table1_heading": "Dobby, potlač, piké, damask a žakárová tkanina",
    "table1_intro": "Pojmy opisujú rôzne vrstvy textilnej konštrukcie. Tabuľka pomáha pri identifikácii; povolenie práčky musí zostať na etikete celého kusu.",
    "table1_headers": ["Označenie", "Ako vzniká motív", "Typická mierka", "Praktické riziko"],
    "table1_rows": [
        ("Dobby", "Riadenie skupín osnovných nití a kombinácia väzieb.", "Malé často geometrické opakovania.", "Lokálne flotáže, zatrhnutie, posun pri šve a sploštenie."),
        ("Potlač", "Farbivo alebo pigment sa nanesie na hotový podklad.", "Od drobnej bodky po veľký obraz.", "Stálofarebnosť tlače a poškodenie povrchu trením či teplom."),
        ("Piké", "Reliéfna tkaná alebo pletená konštrukcia s vystupujúcimi a zapustenými miestami.", "Bodky, vafle, rebrá a bunky.", "Sploštenie, zachytenie a zvyšok produktu v priehlbinách."),
        ("Damask", "Zmena väzobného efektu vytvára väčší obojstranný motív.", "Často rozsiahle ornamentálne opakovanie.", "Dlhšie väzné úseky, tlak a rozdiel odrazu."),
        ("Žakárová tkanina", "Individuálnejšie riadenie osnovy umožní zložité obrazce.", "Komplexné a väčšie motívy.", "Voľné priadze, viac farieb a konštrukčne špecifická údržba."),
    ],
    "sections": [
        {
            "heading": "Ako rozoznať dobby vzor na košeli, obruse alebo obliečke",
            "paragraphs": [
                "Prezrite plochu pod lupou z líca a rubu. Hľadajte pravidelnú zmenu kríženia nití, ktorá vytvára bod, kosoštvorec, prúžok alebo malú figúru. Jednofarebný vzor sa môže objaviť až pri bočnom svetle, preto látku jemne otočte bez naťahovania. Na rube býva väzobný účinok stále čitateľný, hoci kontrast môže byť opačný alebo menej plastický.",
                "Potlač môže prekryť základnú väzbu farbou a na ohybe odhaliť svetlejší podklad. Výšivka pridáva samostatnú niť a často má na rube začiatky či podklad. Dobby nie je každá drobná bodka. Ak obchod neposkytuje technický opis, používajte výraz drobný tkaný vzor opatrne a starostlivosť odvodzujte zo štítka, nie zo svojho pomenovania.",
            ],
        },
        {
            "heading": "Dobby verzus žakár a damask: rozdiel v riadení a výsledku",
            "paragraphs": [
                f"Dobby pracuje so skupinami osnovných nití cez listy a typicky vytvára menšie opakovania. Žakárové tkanie umožňuje rozsiahlejšiu kontrolu jednotlivých nití a zložitejší obraz. <a href=\"{ARTICLE_DAMASK}\">Damask</a> pomenúva konkrétny obojstranný väzobný efekt a môže byť vyrábaný žakárovou technikou. Tieto pojmy preto nie sú jednoduché stupne kvality.",
                "Malý dobby kosoštvorec môže mať dlhšiu flotáž a zachytiť sa skôr než kompaktná časť väčšieho žakáru; opačne komplexný brokát môže obsahovať oveľa viac citlivých doplnkových nití. Pri údržbe zmerajte konkrétnu dĺžku nechráneného úseku, zloženie a komponenty. Výrobná technika sama neurčuje oder, pevnosť šva ani cenu.",
            ],
        },
        {
            "heading": "Dobby verzus piké a reliéfna pletenina",
            "paragraphs": [
                f"<a href=\"{ARTICLE_PIQUE}\">Piké</a> môže byť tkané aj pletené a vytvára výraznejší bunkový alebo rebrový reliéf. Dobby označuje mechanizmus a drobné väzobné figúry, ktoré nemusia byť hlboko plastické. Na úplete uvidíte slučky a väčšiu pružnosť, na tkanej dobby ploche osnovu a útok. V predaji sa názvy môžu stretnúť, preto kontrolujte skutočnú konštrukciu.",
                "Priehlbiny piké zadržia zvyšok prostriedku, kým drobná dobby flotáž je citlivejšia na háčik. Obe štruktúry môže sploštiť vysoký tlak žehličky, ale príčina a oprava sa líšia. Prateľnosť košele či uteráka vychádza z vlákna, farby a celej zostavy, nie z toho, či reliéf obchod nazval dobby alebo piké.",
            ],
        },
        {
            "heading": "Flotáž, väzný bod a prečo sa drobný motív zatrhne",
            "paragraphs": [
                "Flotáž je úsek nite, ktorý prechádza nad viacerými kolmými niťami bez medziľahlého previazaného bodu. Dlhší úsek zvýrazní lesk alebo figúru, ale je voľnejší voči bodovému háčiku. Neznamená to, že každá dobby tkanina je krehká; riziko závisí od dĺžky, hustoty, priadze, napätia a použitia.",
                f"Vytiahnutú slučku nestrihajte naslepo. <a href=\"{ARTICLE_SNAGGING}\">Sprievodca zatrhávaním textilu</a> vysvetľuje, ako odlíšiť posun od pretrhnutia a odstrániť zdroj zachytenia. Látku položte bez napätia, sledujte pokračovanie nite na rube a pri viditeľnom alebo hodnotnom kuse zvoľte odbornú opravu. Lepidlo z líca vytvorí tvrdý bod.",
            ],
        },
        {
            "heading": "Ako prať dobby košeľu bez poškodenia goliera a motívu",
            "paragraphs": [
                "Skontrolujte golier, manžety, légu, výšivku a podlep. Mastnotu ošetrite lokálne kompatibilným produktom bez kefovania cez vystupujúce figúry. Košeľu oddeľte od zipsov, háčikov a hrubých kusov; bezpečné gombíky zapnite podľa konštrukcie a bubon neprepĺňajte. Sieťka môže obmedziť kontakt, no nenahrádza vhodný cyklus ani stálofarebnosť.",
                "Použite teplotu a mechaniku zo štítka. Príliš malá voľnosť zhorší oplach a vytvorí pevné lomy, príliš prudký osamelý cyklus zas zvyšuje nárazy. Po skončení košeľu ihneď vyberte, urovnajte švy a golier bez ťahania figúr a sušte s prístupom vzduchu pod viacvrstvové časti.",
            ],
        },
        {
            "heading": "Dobby uteráky, utierky a savosť po praní",
            "paragraphs": [
                "Dobby bordúra na uteráku môže byť plochejšia než slučkové froté a reagovať na zrazenie inak. Pred praním skontrolujte, či sa pri prechode medzi štruktúrami neotvára šev alebo nite. Uteráky triede podľa farby a hmotnosti, no jemnú dobby košeľu s nimi nekombinujte; slučky a ťažký mokrý kus zvyšujú trenie.",
                "Ak uterák stráca savosť, príčinou môže byť povrchová úprava, nadbytok aviváže, mastnota alebo príliš veľa produktu, nie samotný dobby motív. Riaďte sa etiketou a presnou dávkou. Bordúru po cykle neťahajte na pôvodnú šírku silou. Rozdielnu zmenu rozmeru medzi froté a bordúrou posúďte úplne suchú.",
            ],
        },
        {
            "heading": "Dobby obliečky a obrusy: škvrny, rohy a veľká plocha",
            "paragraphs": [
                "Na obliečke otvorte rohy, zapnite bezpečné uzávery a označte vytiahnuté nite pred praním. Veľká plocha sa v preplnenom bubne zvinie, horšie opláchne a môže napínať malý motív v rovnakých záhyboch. Obrus pred cyklom zbavte omrviniek a škvrny rozdeľte podľa pôvodu; vosk, mastnota a nápoj nepotrebujú rovnaký prvý krok.",
                f"Postup pri škvrnách dopĺňa <a href=\"{ARTICLE_STAIN}\">sprievodca čistením textilu</a>. Lokálne miesto nedrhnite kefou cez figúru a nepretláčajte vosk horúcou žehličkou. Po praní veľký kus podoprite po šírke, sušte bez ostrého trvalého lomu a žehlenie začnite na skrytom rohu, kde uvidíte reakciu odrazu.",
            ],
        },
        {
            "heading": "Stálofarebnosť jednofarebného a priadzou farbeného dobby vzoru",
            "paragraphs": [
                f"Jednofarebný dobby vzor môže pôsobiť svetlejšie iba pre rozdiel odrazu medzi väzbami. Priadzou farbený variant pridáva reálny farebný kontrast a tlačený dobby vzhľad zasa povrchovú vrstvu. <a href=\"{ARTICLE_COLOR}\">Stálofarebnosť</a> preto skúšajte na všetkých farbách a výsledok pozorujte z viacerých uhlov, aby ste neplietli vyblednutie so sploštením.",
                "Bielu handričku priložte bez agresívneho trenia za podmienok podobných plánovanému ošetreniu. Prenos je varovanie. Dlhé namáčanie a vyššia teplota nie sú bezpečný spôsob jeho overenia. Pri jednofarebnej košeli môže lokálny odstraňovač zmeniť lesk figúry bez viditeľnej straty pigmentu, preto vždy hodnotíte aj štruktúru a omak.",
            ],
        },
        {
            "heading": "Sušenie, napätie pri šve a posun drobných figúr",
            "paragraphs": [
                f"Mokrý textil má vyššiu hmotnosť a pri jemnej tkanine môže vytiahnuť ramená alebo lem. Kus prenášajte s oporou a sušte podľa etikety. <a href=\"{ARTICLE_DRYING}\">Pri sušení v byte</a> nechajte vzduch cirkulovať aj pod preložením. Na šnúre nevešajte veľký obrus či obliečku za bod, kde je už flotáž poškodená.",
                "Svetlé medzery pri šve môžu znamenať posun priadzí, nie rozpustenú škvrnu. Miesto nenapínajte a porovnajte protiľahlý šev. Ak niť nie je pretrhnutá, krajčír môže upraviť šev alebo rozloženie napätia. Ďalší horúci cyklus neposunie figúru späť a môže zafixovať deformáciu.",
            ],
        },
        {
            "heading": "Ako žehliť dobby bez sploštenia a lesklého obdĺžnika",
            "paragraphs": [
                f"Žehlite iba pri povolení, z rubu cez čistú ochrannú tkaninu a s minimálnym tlakom. Mäkká podložka pomáha zachovať reliéf. Pri košeli postupujte po dieloch podľa <a href=\"{ARTICLE_IRONING}\">návodu na žehlenie</a>, ale na každom novom motíve skúste skrytú časť a nechajte ju vychladnúť. Horúce vlákno neťahajte.",
                "Para môže zmeniť odraz dočasne aj trvalo podľa vlákna a dokončenia. Ak sa jednofarebná figúra po žehlení javí ako tmavý blok, porovnajte ju po vychladnutí z viacerých uhlov. Ďalší tlak nepridávajte. Kovová niť, potlač, elastan alebo živicová úprava môžu mať nižší limit než základná bavlna.",
            ],
        },
        {
            "heading": "Ako vybrať dobby košeľu alebo metráž a pýtať sa na užitočné údaje",
            "paragraphs": [
                "Pri košeli sledujte zloženie, hustotu, veľkosť figúry, golier, rovnosť švov a povolené žehlenie. Prstom bez nechtu prejdite po motíve a hľadajte nezvyčajne dlhé voľné úseky. Pri metráži si vyžiadajte technický list, šírku, zrážanie, stálofarebnosť a smer vzoru. Slovo prémiový ani hotelový nie je skúšobná metóda.",
                "Odstrižok odmerajte, operte plánovaným povoleným spôsobom a po úplnom vysušení porovnajte figúru, farbu, šírku a okraje. Ak budete šiť odev, založte aj skúšobný šev; stabilita voľnej metráže nepredpovedá posun pri tesnom stehu. Zvyšok látky a údaje o šarži uchovajte pre budúcu opravu.",
            ],
        },
    ],
    "table2_heading": "Dobby tkanina po praní alebo používaní: diagnostika povrchu",
    "table2_intro": "Motív posudzujte po úplnom vysušení pri rovnakom svetle. Rozdiel odrazu môže vyzerať ako farebná mapa, preto kontrolujte aj omak a stav nite.",
    "table2_headers": ["Prejav", "Možná príčina", "Čo overiť", "Bezpečný ďalší krok"],
    "table2_rows": [
        ("Jedna slučka vystupuje nad motív", "Zachytená flotáž alebo posunutá priadza.", "Pokračovanie nite na rube a zdroj zachytenia.", "Nestrihať; rozložiť napätie alebo odborne opraviť."),
        ("Svetlá medzera pri šve", "Posun priadzí alebo otvorený steh.", "Celistvosť nite a smer zaťaženia.", "Prestať napínať a opraviť konštrukciu."),
        ("Motív je plochý a lesklý", "Vysoký tlak, teplo alebo povrchový oder.", "Porovnať skrytý lem a uhol odrazu.", "Ďalej nežehlite ani nedrhnite."),
        ("Biela tvrdá stopa v priehlbine", "Zvyšok produktu alebo lokálny koncentrát.", "Dávku, náplň, oplach a farbu po navlhčení.", "Pri povolení šetrne opláchnuť bez ďalšej chémie."),
        ("Bordúra uteráka sa zvlnila", "Rozdielna rozmerová zmena dobby a froté časti.", "Suché rozmery, švy a povolený cyklus.", "Nenapínať mokré; nový kus zdokumentovať."),
    ],
    "steps_heading": "Ako ošetriť prateľný dobby výrobok krok za krokom",
    "steps": [
        "Pod lupou rozlíšte tkanú figúru od potlače, výšivky a pleteného reliéfu.",
        "Prečítajte zloženie, farby, úpravy, podšívku a všetky symboly celého výrobku.",
        "Odfoťte flotáže, švy, figúru, rozmery a existujúce lesklé alebo vytiahnuté miesta.",
        "Na skrytej časti otestujte farbu, produkt, rozmer a odraz po úplnom vysušení.",
        "Ak je pranie povolené, oddeľte kus od otvorených zipsov, háčikov a suchého zipsu.",
        "Použite povolený cyklus, presnú dávku a náplň s priestorom na pohyb a oplach.",
        "Po cykle kus podoprite, bez ťahania urovnajte švy a sušte podľa etikety.",
        "Žehlite z rubu s malým tlakom a po vyschnutí skontrolujte figúru, farbu a všetky flotáže.",
    ],
    "remember": [
        "Je figúra skutočne utkaná dobby mechanizmom, vytlačená, vyšívaná alebo pletená?",
        "Kde sú najdlhšie flotáže a sú už niektoré vytiahnuté alebo pretrhnuté?",
        "Aké vlákno, farbivo, kovová niť, povlak či podšívka určujú najnižší limit?",
        "Je jednofarebný rozdiel skutočné vyblednutie alebo iba zmena odrazu väzby?",
        "Obsahuje náplň zips, háčik, suchý zips alebo ťažký uterák, ktorý môže figúru zachytiť?",
        "Povoľuje etiketa sušičku, paru a tlak potrebný pri žehlení?",
    ],
    "mistakes": [
        "Považovať dobby za druh bavlny a nastaviť program bez kontroly zloženia.",
        "Zameniť drobnú potlač za tkanú figúru iba podľa fotografie z e-shopu.",
        "Odstrihnúť vytiahnutú flotáž a vytvoriť pokračujúcu dierku v opakovaní.",
        "Prať jemnú dobby košeľu s otvorenými zipsami alebo ťažkými froté uterákmi.",
        "Drhnúť tvrdý zvyšok v priehlbine kefou a poškodiť vrchol motívu.",
        "Žehliť reliéf z líca vysokým tlakom, kým sa nevytvorí lesklý obdĺžnik.",
    ],
    "expert_heading": "Odbornejší pohľad: listy, krátke opakovanie a rozdiel oproti žakáru",
    "expert": [
        "North Carolina State University opisuje dobby vzory ako malé figurované návrhy s obmedzeným počtom osnovných usporiadaní v jednom opakovaní. Geometrický alebo drobný kvetinový motív môže byť jednofarebný a viditeľný zmenou odrazu, alebo zvýraznený priadzami rôznych farieb. Moderné riadenie mení vzor elektronicky, no základom zostáva koordinovaný pohyb skupín osnovných nití.",
        "Pratt Textile Research Lab a Getty AAT zdôrazňujú malé pravidelné figúry a jednoduchší mechanizmus než jacquard. To neznamená automaticky jednoduchšiu údržbu: lokálna flotáž môže byť dlhá, priadza jemná a hotový výrobok môže obsahovať kov, hodváb alebo povlak. Konštrukčný názov pomáha nájsť rizikové miesta, nie stanoviť bezpečnú teplotu.",
        "AATCC vedie oddelené metódy pre zachytávanie, oder, rozmer, farbu, švy a vzhľad. Pri dobby textílii má zmysel zaznamenať orientáciu vzorky a miesto figúry, pretože plátnová základňa a flotáž nemusia zlyhať rovnako. GINETEX symbol zostáva maximálnym ošetrením celého výrobku a európsky vláknový údaj musí byť čítaný samostatne.",
    ],
    "source_intro": "Zdroje podporujú dobby ako malé často geometrické tkané figúry riadené skupinami osnovných nití a jeho odlíšenie od zložitejšieho žakáru. Nepodporujú jeden program pre každú dobby tkaninu.",
    "sources": [
        ("North Carolina State University: dobby woven fabric", NCSU_DOBBY),
        ("Pratt Textile Research Lab: porovnanie tkaných konštrukcií", PRATT_DOBBY),
        ("Getty AAT: odborná definícia dobby weave", GETTY_DOBBY),
        ("AATCC: prehľad skúšobných štandardov pre textil", AATCC_STANDARDS),
        ("GINETEX: význam symbolov ošetrovania", GINETEX),
        ("EÚ 1007/2011: názvy a označovanie textilných vlákien", EU_FIBRE_LABEL),
    ],
    "related": [
        ("Čo je damask a ako chrániť tkaný vzor", ARTICLE_DAMASK),
        ("Čo je piké a ako ošetrovať reliéf", ARTICLE_PIQUE),
        ("Ako predchádzať zatrhávaniu textilu", ARTICLE_SNAGGING),
        ("Ako čítať štítok na oblečení", ARTICLE_LABEL),
        ("Ako farby blednú pri praní, svetle a trení", ARTICLE_COLOR),
        ("Ako správne vyžehliť košeľu", ARTICLE_IRONING),
    ],
    "faq_title": "dobby tkanina a malé tkané vzory",
    "faq": [
        ("Čo je dobby tkanina?", "Tkanina s malými opakovanými figúrami vytvorenými riadením skupín osnovných nití na dobby stave."),
        ("Je dobby druh bavlny?", "Nie. Dobby opisuje spôsob väzobného vzorovania; tkanina môže mať rozličné vlákna."),
        ("Je dobby vzor vytlačený?", "Pri pravom dobby je figúra utkaná. Potlač môže vzhľad napodobniť na inom podklade."),
        ("Ako sa dobby líši od žakáru?", "Dobby typicky riadi skupiny osnovných nití pre menšie figúry, kým jacquard umožňuje zložitejšie individuálne riadenie a väčšie obrazce."),
        ("Je dobby to isté ako piké?", "Nie. Piké je reliéfna konštrukcia a môže byť tkané alebo pletené; názvy sa však v predaji môžu kombinovať."),
        ("Môže ísť dobby košeľa do práčky?", "Iba ak to povoľuje etiketa celého výrobku a jeho vlákna, farby, golier aj ozdoby."),
        ("Na koľko stupňov prať dobby?", "Jedna teplota neexistuje. Určuje ju zloženie a symbol, nie veľkosť motívu."),
        ("Prečo sa dobby tkanina zatrháva?", "Niektoré figúry obsahujú dlhšie flotáže, ktoré sa môžu zachytiť o háčik, zips alebo drsný povrch."),
        ("Môžem vytiahnutú niť odstrihnúť?", "Nie naslepo. Môžete prerušiť nosnú alebo vzorovú niť a vytvoriť dieru."),
        ("Ako dobby sušiť?", "Podľa etikety, s rovnomernou oporou a bez ťahania za poškodenú figúru alebo úzky bod."),
        ("Ako dobby žehliť?", "Z rubu cez ochrannú tkaninu, s malým tlakom a pri najnižšej účinnej povolenej teplote."),
        ("Prečo jednofarebný vzor vyzerá po žehlení tmavšie?", "Tlak mohol zmeniť odraz väzby alebo sploštiť reliéf, aj keď farbivo zostalo rovnaké."),
        ("Je dobby uterák menej savý?", "Nie automaticky. Savosť závisí od vlákna, slučiek, hustoty, úpravy a zvyškov produktu, nie iba od dobby bordúry."),
    ],
}

add_cards(
    DOBBY,
    noun="bavlnený alebo kompatibilný zmesový dobby výrobok",
    limit="Hodváb, kovové vlákno, povlak, čalúnenie, lepený detail a symbol profesionálneho čistenia patria mimo bežného odporúčania na prateľnú bielizeň.",
)


MOIRE: dict[str, object] = {
    "title": "Čo je moaré: zvlnený lesklý efekt, voda, tlak a bezpečné čistenie",
    "link": "co-je-moare-zvlneny-leskly-efekt-voda-tlak-a-bezpecne-cistenie",
    "meta": "Čo je moaré, ako vzniká zvlnený vodovaný lesk a ako bezpečne čistiť moaré šaty, stuhu či dekoráciu bez vodných máp, sploštenia a straty úpravy.",
    "short": "Moaré je zvlnený vodovaný efekt vytvorený na povrchu tkaniny tlakom, teplom, vlhkosťou, valcami, chémiou alebo tlačou. Nie je to samostatné vlákno a pri čistení môže byť samotná úprava citlivejšia než podklad.",
    "name": "moaré",
    "locative": "moaré povrchu",
    "identity_heading": "Moaré je zámerný povrchový efekt, nie synonymum hodvábu ani vodná škvrna",
    "identity_detail": "Vlnité svetlé a tmavé pásy môžu vzniknúť kalandrovaním rebrovanej tkaniny pod tlakom, teplom a kontrolovanou vlhkosťou, rytým valcom, posunom priadzí, chemickou úpravou alebo potlačou.",
    "identity_boundary": "Efekt sa nachádza na hodvábe, bavlne, viskóze, acetáte, nylonových a ďalších syntetických tkaninách a každý spôsob výroby má inú odolnosť voči vode, rozpúšťadlu, treniu a opätovnému tlaku.",
    "label_focus": "presné vlákna, či je efekt mechanický, reliéfny alebo tlačený, ripsovú či faille základňu, podšívku, výstuž, lepidlo, kovové ozdoby, farbivo a povolené profesionálne čistenie a žehlenie",
    "missing_label": "Neoznačenú stuhu, kabelku, tienidlo alebo spoločenský odev neposudzujte podľa lesku ako bežný polyester; pri metráži žiadajte technický list a pri historickom kuse konzervátora.",
    "dry_check": "rovnomernosť vlnitého efektu, vodné kruhy, lesklé obdĺžniky, zlomy, odreté rebrá, prasknuté švy, lepkavosť, migráciu farby, zvlnenú podšívku a miesta po predchádzajúcom bodovaní",
    "damage_boundary": "Prach alebo čerstvú škvrnu možno opatrne riešiť, no lokálne sploštený kalandrovaný efekt, presunuté rebrá, odlúpnutý povlak či teplom zmenený acetát nie sú nečistoty.",
    "test_focus": "Skúšku pozorujte z viacerých uhlov po úplnom vysušení a porovnajte nielen farbu, ale aj smer vĺn, výšku rebra, lesk, tvrdosť a vznik nového ostrého okraja.",
    "combined_risk": "opätovného napučania a presunu priadzí, redistribúcie úpravy, zmeny kalandrovaného odrazu, vzniku vodnej mapy a deformácie podšívky alebo vystuženého tvaru",
    "chemistry_boundary": "Voda, tenzid, rozpúšťadlo a para môžu ovplyvniť nielen škvrnu, ale aj živicu, farbivo, lepidlo a mechanicky vytvorený povrch; miešanie produktov bez kompatibility je nebezpečné.",
    "drying_detail": "Navlhčenú výslovne prateľnú vzorku sušte rovnomerne bez pritlačenia vlnitého líca, spoločenský odev podoprite po konštrukcii a stuhu nerozkladajte cez ostrú hranu.",
    "heat_boundary": "Horúci fén, radiátor, sušička alebo žehlička môže zmeniť termoplastické vlákno, sploštiť či pretlačiť vlny, vytvoriť nový lesklý blok, uvoľniť lepidlo alebo zraziť podklad.",
    "stop_signs": "miznutie alebo posun vlnitého efektu, rastúca mapa, silný prenos farby, lepkavosť, oddelenie vrstiev, deformácia rebra, tmavnutie kovu, zvlnenie podšívky alebo praskanie priadze",
    "professional_boundary": "Jednoduchý moderný výslovne prateľný bavlnený alebo kompatibilný syntetický kus možno ošetriť podľa výrobcu, no hodvábne moaré, formálne šaty, kabelka, stuha, tienidlo, historický textil a profesionálny symbol potrebujú odborníka.",
    "answer": "Moaré je zámerný zvlnený lesklý alebo vodovaný efekt na tkanine. Môže vzniknúť tlakom, teplom a vlhkosťou pri kalandrovaní rebrovanej plochy, rytým valcom, chemickou úpravou alebo tlačou. Nie je to názov jedného vlákna a nepravidelné vlny nie sú automaticky vodná škvrna. Pred čistením zistite podklad, spôsob úpravy a všetky vrstvy výrobku. Prach odstráňte bez tlaku a lokálnu skúšku vyhodnoťte po úplnom vysušení z viacerých uhlov. Celý kus perte iba pri výslovnom povolení. Žehličku neprikladajte priamo na líc, pretože nový tlak a teplo môžu efekt zmeniť. Hodvábne spoločenské šaty, kabelku, stuhu, tienidlo alebo historické moaré zverte čistiarni či konzervátorovi.",
    "intro": "Slovo moaré sa používa pre textilný vodovaný povrch aj pre optické interferenčné pruhy na fotografii alebo obrazovke. V článku ide o zámerne vyrobenú textíliu. Jej vzhľad môže byť výsledkom presne tej kombinácie, ktorú pri domácom čistení znovu prinášame: vlhkosti, tlaku a tepla. Preto nestačí vybrať jemný program. Treba vedieť, či je efekt mechanicky vtlačený, fixovaný živicou, vytlačený alebo vytvorený presunom rebrovanej tkaniny a či je pod ním hodváb, acetát, bavlna, syntetika, lepidlo alebo podšívka.",
    "quick": [
        "<strong>Moaré je úprava alebo efekt:</strong> zvlnený lesk nevypovedá sám o vláknovom zložení.",
        "<strong>Tlak je súčasť výroby aj riziko:</strong> žehlička môže pôvodné vlny sploštiť alebo vytvoriť nový neželaný blok.",
        "<strong>Voda môže vytvoriť ostrú mapu:</strong> zvlášť pri lokálnom bodovaní, nerovnomernom odsávaní a citlivej úprave.",
        "<strong>Nie každé moaré je hodváb:</strong> efekt sa môže objaviť na bavlne, viskóze, acetáte, nylone a ďalších syntetikách.",
        "<strong>Optické moiré nie je textilná chyba:</strong> pruhy viditeľné iba cez fotoaparát môžu vzniknúť interferenciou mriežok.",
        "<strong>Formálny predmet posudzujte ako celok:</strong> podšívka, výstuž, kov, lepidlo a tvar majú často nižší limit než samotná metráž.",
    ],
    "overview_heading": "Ako vzniká vodovaný vzhľad moaré a prečo sa mení s uhlom svetla",
    "overview": [
        "Smithsonian Museum Conservation Institute zhŕňa viac výrobných metód. Pri bar moiré sa rady zvlnených vzorov vytvárajú mechanicky vlhkosťou, teplom a tlakom; scratch moiré posúva priadze do jednoduchých obrazcov a ďalšie procesy kombinujú mechanické, chemické alebo tlačové kroky. Spoločným výsledkom je plocha, na ktorej sa smer, poloha alebo sploštenie priadzí mení v pravidelných vlnách.",
        "Getty Art & Architecture Thesaurus opisuje moaré ako hodvábnu alebo inú tkaninu s vlnitým vodovaným vzhľadom vytvoreným kalandrovaním alebo rytými valcami. Rebrovaný podklad, napríklad ripsového či faille charakteru, poskytuje línie, ktoré sa pod kontrolovaným tlakom lokálne vychýlia alebo sploštia. Svetlo sa potom odráža v rozdielnych smeroch a vytvára pohyblivý kontrast.",
        "Nie všetky moderné výrobky používajú rovnaký proces. Tlačený motív môže byť farebnou kresbou bez zmeny priadzí, termoplastická syntetika môže držať reliéf teplom a historický hodváb môže mať citlivé farbivo aj oslabené rebrá. Preto sa odolnosť nedá zovšeobecniť zo slova moaré. Výrobný údaj a štítok majú prednosť pred domácim pokusom obnoviť efekt parou.",
    ],
    "table1_heading": "Moaré, saténový lesk, taft, rips a vodná mapa",
    "table1_intro": "Všetky môžu pod svetlom vytvárať lesklé či tmavé pásy, ale vznikajú inak. Rozlíšenie zabráni tomu, aby sa zámerný efekt čistil ako škvrna.",
    "table1_headers": ["Jav alebo materiál", "Ako vzniká vzhľad", "Čo vidno pri pohybe", "Riziko nesprávneho zásahu"],
    "table1_rows": [
        ("Moaré", "Riadený tlak, teplo, vlhkosť, valec, chémia alebo tlač vytvoria vlny.", "Vlnité pásy pravidelne menia odraz a patria k dizajnu.", "Sploštenie, presun efektu a nové mapy po lokálnom čistení."),
        ("Satén", "Dlhšie väzné úseky vytvárajú súvisle hladší lesk.", "Lesk sleduje väzbu, nie nutne vodované vlny.", "Zatrhnutie flotáží a lesklé tlakové stopy."),
        ("Taft alebo faille", "Plátnová či rebrová konštrukcia a filamenty vytvárajú ostrý odraz.", "Pruhy alebo rebrá sú pravidelné bez typického vodovania.", "Vodné mapy, zalomenie a teplom zmenený povrch."),
        ("Rips", "Rozdiel priadzí a väzba vytvoria priečne rebrá.", "Súvislé rovné rebrovanie.", "Oder vrcholov a sploštenie tlakom."),
        ("Vodná mapa", "Nerovnomerný presun nečistoty, produktu, farbiva alebo úpravy.", "Ostrý kruh či nepravidelný okraj, ktorý nepatrí k opakovaniu.", "Ďalšie bodovanie môže okraj zväčšiť."),
    ],
    "sections": [
        {
            "heading": "Ako odlíšiť zámerné moaré od vodnej škvrny",
            "paragraphs": [
                "Zámerný efekt má opakovanie alebo plynulú súvislosť cez väčšiu plochu a pri nakláňaní sa jeho svetlé a tmavé časti menia bez ostrého okraja znečistenia. Vodná mapa sa často viaže na miesto kontaktu, má krúžok, rozdiel omaku alebo nadväzuje na šev. Porovnajte symetrický diel, rub a fotografiu výrobku ešte pred zásahom.",
                "Niektoré mapy však sledujú rebrá a zámerné moaré môže byť nepravidelné. Preto nepoužite vodu iba ako diagnostický test. Miesto pozorujte pri rozptýlenom aj bočnom svetle a čistou suchou handričkou bez tlaku overte, či je na povrchu prach. Ak chýba dokumentácia a zásah by bol na viditeľnom formálnom kuse, zvoľte odborné posúdenie.",
            ],
        },
        {
            "heading": "Textilné moaré verzus optický moiré efekt na fotografii",
            "paragraphs": [
                "Optické moiré vzniká interferenciou dvoch pravidelných mriežok, napríklad jemnej väzby a obrazového snímača alebo pixelov obrazovky. Pruhy sa môžu objaviť iba na fotografii, meniť pri priblížení a na samotnej látke voľným okom chýbať. Nejde o škvrnu ani o dôvod textil čistiť.",
                "Pri dokumentovaní urobte záber z viacerých vzdialeností a uhlov, použite rozptýlené svetlo a porovnajte detail voľným okom. Skutočný vyrábaný moaré povrch ostáva fyzickou vlastnosťou látky, hoci sa tiež mení s uhlom. Ak problém existuje iba v digitálnom zobrazení, rieši sa fotografickou technikou, nie vodou alebo žehlením predmetu.",
            ],
        },
        {
            "heading": "Moaré, satén a taft: lesk bez rovnakého ošetrovacieho pravidla",
            "paragraphs": [
                f"<a href=\"{ARTICLE_SATIN}\">Satén</a> opisuje väzbu s dlhšími flotážami a môže byť hodvábny, viskózový alebo syntetický. <a href=\"{ARTICLE_TAFFETA}\">Taft</a> je pevnejšia šušťavá tkanina citlivá na zalomenie a vodné mapy. Moaré môže byť vytvorené práve na rebrovanom taftovom či faille podklade, no jeho vodované vlny sú ďalšia úroveň dokončenia.",
                "Názvy preto nemožno zameniť za stupne lesku. Saténová stuha bez moaré rieši najmä flotáže, moaré stuha aj stabilitu efektu a taftové šaty navyše konštrukciu. Pri každom kuse skontrolujte vlákno a spôsob výroby. Rada na žehlenie hladkého bavlneného saténu sa nesmie preniesť na acetátové moaré.",
            ],
        },
        {
            "heading": "Voda, para a lokálny tlak: prečo môže efekt zmeniť aj jemný pokus",
            "paragraphs": [
                "Ak výroba používala vlhkosť, teplo a tlak na presun alebo sploštenie priadzí, opakovaná nerovnomerná kombinácia môže zmeniť odraz. Kvapka navlhčí iba časť rebra, prst alebo handrička pridá bodový tlak a fén urýchli okraj. Po vyschnutí sa objaví kruh, rovný otlačok alebo plocha s odlišným smerom vĺn.",
                "To neznamená, že každé moaré nesmie prísť do kontaktu s vodou. Smithsonian uvádza aj bavlnené alebo termoplastické varianty s úpravami, ktoré môžu zniesť určené čistenie. Rozhoduje konkrétny výrobný návod. Domáca skúška má byť minimálna, na skrytom mieste a vyhodnotená úplne suchá; nie je povolením pre celý podšitý predmet.",
            ],
        },
        {
            "heading": "Ako odstrániť prach bez vytvorenia hladkého pruhu",
            "paragraphs": [
                "Pred mokrým zásahom odstráňte iba voľný prach. Na pevnom modernom kuse možno použiť čistý mäkký štetec alebo veľmi nízke kontrolované sanie cez ochrannú sieťku, ak to konštrukcia dovoľuje. Pracujte v smere rebra a bez pritlačenia hubice. Poškodené alebo uvoľnené priadze nevysávajte bez konzervátorského postupu.",
                "Mokrá mikrovláknová handrička nie je univerzálne jemná. Môže zvýšiť trenie, zachytiť niť a vytvoriť pás s odlišným tlakom. Predmet si podoprite, aby ste ho pri čistení neohýbali cez hranu. Pri tienidle alebo kabelke odstráňte prach bez namočenia výstuže a lepidla, ktoré nie sú viditeľné na líci.",
            ],
        },
        {
            "heading": "Lokálna škvrna na moaré: odsatie, hranica a rozhodnutie zastaviť",
            "paragraphs": [
                f"Čerstvú kvapalinu odsajte bielym savým materiálom bez trenia a bez rozširovania mokrej zóny. Pevnú nečistotu zdvihnite tupou hranou. Pôvod škvrny rozlíšte podľa <a href=\"{ARTICLE_STAIN}\">sprievodcu škvrnami</a>, no konkrétny produkt použite iba pri potvrdenej kompatibilite podkladu, farbiva a úpravy. Na neznámom hodvábe nepokračujte domácim bodovaním.",
                "Po každom minimálnom kroku sledujte okraj, prenos farby a posun vĺn. Ak mapa rastie, povrch mäkne, lepkavie alebo sa vlny vyrovnávajú, okamžite prestaňte. Ďalší kruh vody nevyrovná automaticky prvý. Predmet podoprite, nechajte stabilne vyschnúť bez tepla a odborníkovi oznámte presne, čo a ako dlho bolo použité.",
            ],
        },
        {
            "heading": "Ako prať jednoduchý moderný moaré výrobok, ak to výrobca povoľuje",
            "paragraphs": [
                "Domáce pranie prichádza do úvahy iba pri výslovne označenom jednoduchom kuse bez citlivej výstuže a s kompatibilným vláknom. Pred cyklom odfoťte efekt, odmerajte rozmer, skontrolujte farbu a urobte skrytú skúšku. Kus oddeľte od zipsov, háčikov a ťažkých textílií a nepoužívajte predĺžené namáčanie bez pokynu.",
                "Zvoľte presný povolený program, dávku a odstreďovanie. Koncentrát nelejte na suché rebro a bubon neprepĺňajte, aby sa látka nezalomila pod stálym tlakom. Po cykle ju podoprite, urovnajte bez vyhladzovania vĺn dlaňou a sušte podľa etikety bez kolíka na viditeľnej ploche. Výsledok porovnajte až úplne suchý.",
            ],
        },
        {
            "heading": "Spoločenské šaty, sako a kabelka s moaré povrchom",
            "paragraphs": [
                f"<a href=\"{ARTICLE_FORMAL}\">Spoločenské šaty</a> môžu mať podšívku, kostice, výstuž, lepené aplikácie, flitre, výšivku a tvarované sklady. Kabelka pridáva kartón, penu, kovanie, kožu a lepidlo. Aj keď vrchné polyesterové moaré znesie vodu, celý predmet môže stratiť tvar, zadržať vlhkosť alebo preniesť farbu z vnútra.",
                "Taký kus neponárajte podľa rady pre metráž. Odstráňte prach, škvrnu neprežehľujte a čistiarni oznámte, že povrch je moaré a kde vznikol problém. Pri novej kabelke kontaktujte výrobcu. Profesionálne čistenie tiež nie je automaticky bezrizikové, ale umožňuje vybrať rozpúšťadlo, podporu a dokončenie podľa celej konštrukcie.",
            ],
        },
        {
            "heading": "Sušenie bez nového reliéfu, vodnej hranice a deformácie",
            "paragraphs": [
                f"Povolený prateľný kus sušte rovnomerne s prúdením vzduchu a bez ostrého preloženia. <a href=\"{ARTICLE_DRYING}\">Zásady sušenia v interiéri</a> pomôžu obmedziť zatuchnutie, no pri moaré navyše chráňte líc pred tlakom podložky. Ak sa suší naplocho, použite čistú hladkú oporu, ktorá nepretlačí štruktúru, a polohu meňte iba bez šúchania.",
                "Radiátor, fén a intenzívne slnko vytvárajú lokálne tepelné rozdiely. Termoplastická priadza môže zmeniť tvar a živica mäknúť; hodváb sa môže oslabiť alebo farebne meniť. Predmet neukladajte podľa suchého líca, ak podšívka či výstuž ostávajú chladné. Nezaťažujte ho ďalším predmetom, kým nie je stabilný.",
            ],
        },
        {
            "heading": "Žehlenie moaré: kedy nepoužiť priamy kontakt",
            "paragraphs": [
                "Pri moaré je tlak rovnako dôležitý ako teplota. Priamy kontakt z líca môže pôvodné vlny sploštiť alebo vytlačiť tvar otvorov žehliacej dosky. Žehlite iba pri výslovnom symbole, z rubu, cez čistú ochrannú tkaninu a na podložke bez hrubej textúry. Začnite na skrytom okraji s minimálnym účinným tlakom.",
                "Para nie je nástroj na domáce obnovenie vodovaného efektu. Môže uvoľniť priadze, preniesť farbivo alebo aktivovať lepidlo. Po skúške nechajte miesto úplne vychladnúť a pozorujte z viacerých uhlov. Ak sa objaví nový pravidelný blok, lesk alebo posun vĺn, ďalšie žehlenie zastavte a na viditeľnú plochu nepokračujte.",
            ],
        },
        {
            "heading": "Skladovanie stúh, šiat a dekoračného moaré",
            "paragraphs": [
                "Čistý a úplne suchý textil skladujte bez dlhodobého ostrého lomu a bez tlaku ťažkých predmetov. Stuhu naviňte na dostatočne veľké inertné jadro bez napínania a preložte ochrannou vrstvou vhodnou pre daný predmet. Šaty zaveste na širokú oporu iba vtedy, keď ramená unesú hmotnosť; inak ich podoprite vo vodorovnej polohe.",
                "Plastový obal v nestabilnej vlhkosti môže zadržať kondenzáciu a kontakt s farbeným papierom preniesť farbu. Tienidlo či dekoráciu chráňte pred prachom, svetlom a stlačením, ale nezabaľujte vlhkú. Miesto starého prehybu pravidelne kontrolujte; oslabenú líniu nevyrovnávajte parou a silným ťahom.",
            ],
        },
        {
            "heading": "Ako kupovať moaré metráž a overiť budúcu údržbu",
            "paragraphs": [
                "Pýtajte sa na vláknové zloženie, základnú väzbu, spôsob vytvorenia moaré, smer opakovania, teplotný limit a povolené čistenie. Overte, či je efekt trvalý pri zamýšľanom použití a či výrobca poskytuje údaje po praní alebo profesionálnom čistení. Vzorka z fotografie nemusí ukázať rozdiel medzi mechanickým a tlačeným vodovaním.",
                "Odstrižok odfoťte pod rovnakým svetlom, odmerajte a ošetrite iba odporúčaným spôsobom. Po úplnom vysušení porovnajte vlny, rebro, farbu, lesk, rozmer a okraje. Ak plánujete lepenie, výstuž alebo tvarovanie, vytvorte skúšobnú zostavu; samotná metráž nepredpovedá reakciu hotovej kabelky alebo šiat.",
            ],
        },
    ],
    "table2_heading": "Zmena na moaré po čistení: škvrna, posun efektu alebo poškodenie",
    "table2_intro": "Povrch hodnotíte úplne suchý, bez tlaku a z viacerých uhlov. Dočasné stmavnutie vody nie je výsledok, ale rastúca lepkavosť či prenos farby sú dôvod zastaviť.",
    "table2_headers": ["Prejav", "Možná príčina", "Čo overiť", "Bezpečný ďalší krok"],
    "table2_rows": [
        ("Ostrý svetlý alebo tmavý kruh", "Migrácia produktu, nečistoty, farbiva alebo úpravy pri lokálnom navlhčení.", "Okraj po vysušení a prenos na bielu tkaninu.", "Nepridávať ďalší kruh; odborné posúdenie."),
        ("Vlny sa v jednom obdĺžniku stratili", "Tlak žehličky, podložky alebo skladovania.", "Zmenu rebra a odrazu po vychladnutí.", "Ďalej netlačiť ani nepariť."),
        ("Povrch je lepkavý", "Zmena živice, lepidla, povlaku alebo zvyšok produktu.", "Súvis s teplom a aplikovaným miestom.", "Zastaviť chémiu; oddeliť od priľahlých plôch a konzultovať."),
        ("Farba sa prenáša z tmavej vlny", "Nestálofarebné farbivo alebo narušená úprava.", "Každú farbu a pokyny výrobcu.", "Nepredlžovať mokrý kontakt; dokumentovať."),
        ("Šaty sa vlnia pri podšívke", "Rozdielne zrazenie alebo deformácia výstuže.", "Suché rozmery, švy a vnútorné vrstvy.", "Nenapínať ani neprežehliť silou; čistiareň alebo krajčír."),
    ],
    "steps_heading": "Ako bezpečne posúdiť a ošetriť moaré krok za krokom",
    "steps": [
        "Potvrďte, že ide o fyzický moaré efekt, nie iba interferenčné pruhy na fotografii.",
        "Zistite vlákno, základnú väzbu, spôsob úpravy, podšívku, výstuž, ozdoby a symboly.",
        "Odfoťte vlny z viacerých uhlov a označte mapy, zlomy, lesk a staré tlakové stopy.",
        "Prach odstráňte nasucho a bez tlaku; škvrnu odsajte bez rozširovania mokrej zóny.",
        "Skrytú skúšku vyhodnoťte úplne suchú podľa farby, lesku, rebra, vĺn a tvrdosti.",
        "Celý kus perte iba pri výslovnom povolení a bez zipsov, preplnenia či lokálneho koncentrátu.",
        "Sušte s rovnomernou oporou bez tepla a žehlenie vykonajte len z rubu pri minimálnom tlaku.",
        "Pri posune efektu, mape, lepkavosti, prenose farby alebo deformácii okamžite zastavte zásah.",
    ],
    "remember": [
        "Je efekt mechanicky kalandrovaný, rytý, chemický, tlačený alebo neznámy?",
        "Aké vlákno, rebrovaný podklad, živica, podšívka a lepidlo tvoria celý predmet?",
        "Je nepravidelnosť zámerná vlna, vodná mapa, tlakový lesk alebo iba efekt fotoaparátu?",
        "Mení sa farba, rebro a smer vĺn na skrytej skúške po úplnom vysušení?",
        "Máte možnosť rovnomerne vysušiť celý predmet bez pritlačenia líca?",
        "Povoľuje etiketa vodu, profesionálne čistenie, paru a akýkoľvek tlak žehličky?",
    ],
    "mistakes": [
        "Čistiť zámerné vodované vlny ako škvrnu iba preto, že pôsobia nepravidelne.",
        "Použiť kvapku vody ako identifikačný test na viditeľnom hodvábnom povrchu.",
        "Vyhladzovať moaré dlaňou, žehličkou alebo lisom, kým vlny nezmiznú.",
        "Prať spoločenské šaty podľa odolnosti samotnej vrchnej syntetickej metráže.",
        "Sušiť bodovaný povrch fénom a vytvoriť ostrú migračnú hranicu.",
        "Pokúšať sa obnoviť efekt domácou parou bez znalosti výrobného procesu.",
    ],
    "expert_heading": "Odbornejší pohľad: kalandrovanie, presun priadzí a viac výrobných ciest",
    "expert": [
        "Smithsonian Museum Conservation Institute opisuje bar moiré vytvorené mechanicky vlhkosťou, teplom a tlakom, scratch moiré založené na vychýlení priadzí a ďalšie chemické či tlačové varianty. Uvádza tiež viac vláknových rodín. Ošetrenie preto musí byť viazané na konkrétny spôsob výroby a podklad, nie na všeobecný vizuálny termín.",
        "Getty AAT definuje zvlnený vodovaný vzhľad vytvorený kalandrovaním alebo rytými valcami. Kalandrovanie mení geometriu a odraz povrchu tlakom medzi valcami; nový lokálny tlak nemusí kopírovať pôvodné podmienky a môže vytvoriť inú stopu. Rovnaký lesk na acetáte, hodvábe a bavlne preto neznamená rovnakú tepelnú toleranciu.",
        "National Park Service pri textilných predmetoch zdôrazňuje dokumentáciu, oporu, kontrolované odstránenie prachu a opatrnosť pri kombinovaných materiáloch. AATCC zase oddeľuje hodnotenie farby, vzhľadu, rozmeru a ďalších vlastností. Zachovanie vĺn po skúške teda nie je dôkaz, že podšívka, farbivo a lepidlo bezpečne zvládnu celý proces.",
    ],
    "source_intro": "Zdroje podporujú viac mechanických, chemických a tlačových spôsobov výroby moaré aj citlivosť rozhodovania na podklad a celý predmet. Nepodporujú univerzálne domáce prežehlenie alebo pranie každého vodovaného povrchu.",
    "sources": [
        ("Smithsonian Museum Conservation Institute: moiré a možnosti čistenia", MCI_MOIRE),
        ("Getty AAT: definícia moiré a kalandrovaného vodovaného povrchu", GETTY_MOIRE),
        ("National Park Service: starostlivosť o textilné predmety", NPS_TEXTILES),
        ("AATCC: prehľad skúšobných štandardov pre textil", AATCC_STANDARDS),
        ("GINETEX: význam symbolov ošetrovania", GINETEX),
        ("EÚ 1007/2011: názvy a označovanie textilných vlákien", EU_FIBRE_LABEL),
    ],
    "related": [
        ("Čo je satén a ako ho správne prať", ARTICLE_SATIN),
        ("Čo je taft a ako predísť vodným mapám", ARTICLE_TAFFETA),
        ("Čo je rips a ako chrániť priečne rebrá", ARTICLE_RIPS),
        ("Ako prať spoločenské šaty a kedy zvoliť čistiareň", ARTICLE_FORMAL),
        ("Ako odstrániť rôzne škvrny z oblečenia", ARTICLE_STAIN),
        ("Ako čítať štítok na oblečení", ARTICLE_LABEL),
    ],
    "faq_title": "moaré, vodovaný lesk a bezpečné čistenie",
    "faq": [
        ("Čo je moaré?", "Zvlnený vodovaný efekt vytvorený na tkanine tlakom, teplom, vlhkosťou, valcom, chémiou alebo tlačou."),
        ("Je moaré vždy hodváb?", "Nie. Môže sa objaviť aj na bavlne, viskóze, acetáte, nylone a ďalších syntetických vláknach."),
        ("Je moaré druh tkaniny?", "Presnejšie je to povrchový alebo vzorový efekt na podkladovej tkanine, často rebrovanej."),
        ("Ako sa moaré líši od saténu?", "Saténový lesk vychádza z dlhších väzobných úsekov; moaré pridáva typické zvlnené vodované pásy."),
        ("Ako zistím, či je tmavá vlna škvrna?", "Porovnajte opakovanie, rub, symetrický diel a zmenu pri nakláňaní. Neznámy citlivý kus netestujte kvapkou vody."),
        ("Čo je optický moiré efekt?", "Interferenčné pruhy vzniknuté prekrytím mriežok, napríklad väzby a snímača fotoaparátu. Nemusia byť fyzicky na látke."),
        ("Môže ísť moaré do práčky?", "Iba jednoduchý výrobok s výslovným symbolom a potvrdenou kompatibilitou podkladu, farby a úpravy."),
        ("Na koľko stupňov prať moaré?", "Jedna teplota neexistuje. Rozhoduje vlákno, proces výroby, komponenty a štítok."),
        ("Ako odstrániť vodnú mapu z moaré?", "Bez znalosti podkladu nepridávajte ďalšiu vodu. Mapa môže byť presun úpravy alebo farbiva a potrebuje odbornú diagnózu."),
        ("Môžem moaré naparovať?", "Iba pri výslovnom povolení. Para môže zmeniť priadze, povrch, farbivo, živicu alebo lepidlo."),
        ("Ako moaré žehliť?", "Ak to štítok povoľuje, z rubu cez ochrannú tkaninu, na hladkej podložke a s minimálnym tlakom."),
        ("Ako čistiť moaré spoločenské šaty?", "Podľa etikety celých šiat; podšívka, výstuž a ozdoby často znamenajú profesionálne čistenie."),
        ("Dá sa stratený moaré efekt obnoviť doma?", "Nie spoľahlivo. Výrobný tlak, teplota a vlhkosť sú kontrolované; domáci pokus môže poškodenie zväčšiť."),
    ],
}

add_cards(
    MOIRE,
    noun="jednoduchý výslovne prateľný moaré výrobok",
    limit="Hodvábne moaré, spoločenské šaty, stuha, kabelka, tienidlo, historický predmet, lepená výstuž a profesionálny symbol patria mimo tohto odporúčania.",
)


ARTICLES: list[dict[str, object]] = [PEPITO, BARCHET, DOBBY, MOIRE]


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
        if metric["words"] < 3000:
            raise SystemExit(f"Article is too short: {article['title']} ({metric['words']} words)")
        if metric["h2"] < 24 or metric["tables"] < 2 or metric["responsive_tables"] != metric["tables"]:
            raise SystemExit(f"Article structure is incomplete: {article['title']} ({metric})")
        if metric["styled_blocks"] < 10 or metric["action_buttons"] < 2 or metric["faq_questions"] < 12 or metric["one_character_paragraphs"]:
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
        raise SystemExit("Batch 55 link preflight failed")
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
