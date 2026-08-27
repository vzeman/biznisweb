#!/usr/bin/env python3
"""Build and validate VEVO batch 50 textile-structure articles."""

from __future__ import annotations

import json
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

from build_batch_49_household_material_systems import (
    BASE,
    FIXED_PRICE_RE,
    FORBIDDEN_PUBLIC_RE,
    WORD_RE,
    article_hrefs,
    fetch_status,
    render_article,
    visible_text,
)


PUBLISH_DATE = "2026-08-27"
CANDIDATES = Path("content/VEVO_CONTENT/batches/batch-50-candidates-2026-08-27.txt")
OUT_JSON = Path("content/VEVO_CONTENT/imports/batch-50-2026-08-27-articles.json")
OUT_PREFLIGHT = Path("content/VEVO_CONTENT/exports/batch-50-2026-08-27-link-preflight.json")

EU_FIBRE_LABEL = "https://eur-lex.europa.eu/eli/reg/2011/1007/oj"
GINETEX = "https://www.ginetex.net/share/article/4201/care-symbols"
AATCC_DIMENSION = "https://members.aatcc.org/store/tm135/543/"
AATCC_COLOR = "https://members.aatcc.org/store/tm61/495/"
COTTONWORKS_WEAVING = "https://cottonworks.com/wp-content/uploads/2018/01/Weaving_booklet-for_web.pdf"
COTTONWORKS_BASIC_WEAVES = "https://cottonworks.com/learning-hub/weaving/basic-woven-fabric-designs/"
ASTM_COUNT = "https://store.astm.org/d3775-17r23.html"
ASTM_TEAR = "https://store.astm.org/d1424-25.html"
ASTM_ABRASION = "https://store.astm.org/d4966-22r26.html"
ASTM_SNAG = "https://store.astm.org/d3939_d3939m-26.html"
ASTM_THICKNESS = "https://store.astm.org/d1777-26.html"
ASTM_HIGHLOFT = "https://store.astm.org/d6571-22.html"
ISO_NONWOVEN = "https://www.iso.org/standard/90537.html?browse=tc"
VLIESELINE_BROCHURE = "https://www.vlieseline.com/Vlieseline/Website/Downloads/Brochures/Vlieseline_Digitale_Mustermappe_2025.pdf"
ISO_AIR = "https://www.iso.org/standard/16869.html"
ASTM_AIR = "https://store.astm.org/standards/d737"
SPACER_REVIEW = "https://pmc.ncbi.nlm.nih.gov/articles/PMC10222490/"
SPACER_STUDY = "https://pmc.ncbi.nlm.nih.gov/articles/PMC8838024/"

ARTICLE_LABEL = "/n/ako-citat-stitok-na-obleceni-material-symboly-prania-a-spravny-program"
ARTICLE_STAIN = "/n/ako-odstranit-zuvacku-krv-vosk-a-ine-skvrny-z-oblecenia"
ARTICLE_COLOR = "/n/stalofarebnost-textilu-preco-farby-blednu-pri-prani-svetle-a-treni"
ARTICLE_SHRINKAGE = "/n/preco-sa-oblecenie-zrazi-po-prani-teplota-vlakna-susicka-a-prevencia"
ARTICLE_ODOR = "/n/preco-moje-oblecenie-zapacha-po-prani-priciny-a-riesenia"
ARTICLE_DRYING = "/n/ako-susit-bielizen-v-malom-byte-bez-zatuchnutia"
ARTICLE_IRONING = "/n/ako-vyzehlit-koselu-kompletny-sprievodca-pre-dokonaly-vysledok"
ARTICLE_PILLING = "/n/preco-sa-oblecenie-zmolkuje-vlakna-trenie-pranie-a-susenie"
ARTICLE_COTTON = "/n/co-je-bavlna-vlastnosti-vyhody-nevyhody-a-starostlivost"
ARTICLE_POLYESTER = "/n/co-je-polyester-a-ako-ho-prat-aby-nezapachal"
ARTICLE_MUSLIN = "/n/co-je-muselin-vzdusna-gazovina-zrazanie-a-spravna-starostlivost"

LAUNDRY_PRODUCT_NAME = "Prací gél hypoalergénny Vevo Ylang Absolute 1L"
LAUNDRY_PRODUCT_URL = "/p-1627/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l"
LAUNDRY_CATEGORY_NAME = "Pracie gély"
LAUNDRY_CATEGORY_URL = "/c/vevo-home-care/pranie/praci-gel"


RIPS: dict[str, object] = {
    "title": "Čo je rips: priečne rebrovaná tkanina, oder a správna starostlivosť",
    "link": "co-je-rips-priecne-rebrovana-tkanina-oder-a-spravna-starostlivost",
    "meta": "Čo je rips, ako vznikajú priečne rebrá, čím sa líši od úpletu a menčestru a ako ho prať, sušiť, žehliť a chrániť pred oderom.",
    "short": "Rips je tkanina s výraznými priečnymi rebrami vytvorenými väzbou a rozdielom medzi osnovou a útkom. Starostlivosť neurčuje názov rips, ale vlákno, farbenie, povrchová úprava a konštrukcia hotového výrobku.",
    "name": "rips",
    "locative": "ripse",
    "identity_heading": "Rips opisuje tkanú konštrukciu, nie jedno vlákno",
    "identity_detail": "V pravidelnej rebrovej väzbe jedna sústava nití na povrchu prekrýva druhú tak, že vznikajú súvislé priečne línie; ich výšku mení hrúbka a hustota útku aj jemnosť osnovy.",
    "identity_boundary": "Rips môže byť bavlnený, hodvábny, viskózový, polyesterový alebo zmesový a dve podobne rebrované tkaniny preto nemusia zniesť rovnakú vodu, trenie či teplo.",
    "label_focus": "vláknové zloženie, podšívku, výstuž, elastan, acetát, kovové ozdoby, povrch proti škvrnám a povolené žehlenie",
    "missing_label": "Pri odstrihnutej metráži si vyžiadajte technický list; pri hotovom odeve bez etikety neodvodzujte program iba z priečneho rebra.",
    "dry_check": "odreté vrcholy rebier, svetlejšie hrany, posunuté nite pri šve, zalomené sklady, vytiahnuté slučky, fľaky v priehlbinách a lesk po žehlení",
    "damage_boundary": "Nečistotu medzi rebrami možno uvoľniť, no zbrúsený vrchol, presunutý útok alebo sploštenie horúcou žehličkou nie sú škvrny.",
    "test_focus": "Rips pozorujte spredu aj zboku, pretože zmena smeru vlasu, tlaku alebo odrazu môže vyzerať ako rozdiel farby.",
    "combined_risk": "napučania priadze, ohybu vystupujúceho rebra, trenia o susedný kus a tlaku, ktorý reliéf splošťuje",
    "chemistry_boundary": "Mastnotu z goliera, farbu z iného odevu a prach v rebrách nemožno riešiť jedným silným odstraňovačom bez skúšky stálofarebnosti.",
    "drying_detail": "Sukňu alebo sako podoprite podľa švov, záves rozložte po celej šírke a čalúnnický diel nepremočte do výplne len kvôli povrchovému prachu.",
    "heat_boundary": "Tlak a teplo môžu vytvoriť lesklú stopu, zraziť citlivejšie vlákno, uvoľniť výstuž alebo natrvalo znížiť výšku rebier.",
    "stop_signs": "presun útku, otváranie šva, púšťanie farby, mäknutie povrchovej úpravy, lepkavosť, deformácia podšívky alebo rastúca lesklá plocha",
    "professional_boundary": "Bežný prateľný bavlnený rips možno často ošetriť doma podľa etikety, zatiaľ čo hodvábny, čalúnnický, vystužený alebo historický kus potrebuje presnejšie posúdenie.",
    "answer": "Rips je pevnejšie pôsobiaca tkanina s viditeľnými priečnymi rebrami. Nie je to pletený patent ani menčester s vlasom. Pred praním zistite zloženie a konštrukciu celého výrobku, chráňte vystupujúce rebrá pred drsným trením a žehlite iba pri povolení, s malým tlakom z rubu.",
    "intro": "Pri názvoch rips, repp alebo ryps sa ľudia často sústredia na pevný omak a predpokladajú, že látka znesie intenzívny program. Rebro však opisuje geometriu väzby, nie nezničiteľnosť. Jemný hodvábny rips na večernej kabelke, bavlnená stuha, polyesterová sukňa a poťahová tkanina môžu vyzerať príbuzne, ale ich limity sú úplne odlišné.",
    "quick": [
        "Priečne rebrá vznikajú pri tkaní; pletený patent tvorí slučková konštrukcia a menčester má rezaný alebo slučkový vlas.",
        "Odolný vzhľad nehovorí, či je výrobok prateľný. Rozhoduje zloženie, farba, výstuž, podšívka a povrchová úprava.",
        "Najskôr odstráňte prach po smere rebier a škvrnu ošetrujte lokálne bez tvrdého kefovania cez ich vrcholy.",
        "Pri povolenom praní oddeľte rips od zipsov, suchých zipsov a ťažkých uterákov, ktoré zvyšujú bodový oder.",
        "Lesklé alebo sploštené miesto po žehlení sa ďalším praním spravidla neopraví; teplotu a tlak vždy skúšajte z rubu.",
        "Hotový predmet hodnotíte ako systém. Čalúnenie, kabelku či vystužené sako neponárajte iba preto, že samotná lícna tkanina by vodu zniesla.",
    ],
    "overview_heading": "Ako rips vyzerá a prečo sa jeho rebrá správajú inak než hladké plátno",
    "overview": [
        "Pri tkaní sa osnova vedie pozdĺžne a útok sa vkladá priečne. V ripsovej konštrukcii sa rozdielom hrúbky, hustoty alebo väzby vytvorí prevládajúce rebro, najčastejšie naprieč šírkou. Vrcholy zachytávajú svetlo a prvý kontakt s okolím, kým priehlbiny môžu zadržať jemný prach a zvyšky produktu. Preto sa povrch opotrebúva nerovnomerne.",
        "Rebro môže tkaninu opticky aj hmatovo spevniť, ale pevnosť sa musí posudzovať v oboch smeroch a pri šve. Hrubý útok môže vytvoriť výraznú líniu, zatiaľ čo jemná hustá osnova drží jej polohu. Keď sa nite pri tesnom šve posunú, otvorená línia nie je dôkazom nízkej hustoty celého materiálu; často ide o kombináciu strihu, zaťaženia a konštrukcie šva.",
        "V bežnej domácnosti sa rips objavuje na šatách, sukniach, sakách, stuhách, taškách, dekoračných vankúšoch a čalúnení. Každý z týchto výrobkov má inú vnútornú stavbu. Pred akoukoľvek vodou preto oddeľte otázku, ako čistiť líc, od otázky, či možno bezpečne namočiť zips, výstuž, lepidlo, výplň a rub.",
    ],
    "table1_heading": "Rips, patent, menčester a grosgrain: rýchle rozlíšenie",
    "table1_intro": "Názvy sa v obchodoch niekedy používajú voľne. Štruktúru posudzujte lupou z líca aj rubu a potom overte etiketu; vizuálne rozlíšenie neurčuje povolené pranie.",
    "table1_headers": ["Materiál alebo konštrukcia", "Ako vzniká povrch", "Typický znak", "Dôležité pri údržbe"],
    "table1_rows": [
        ("Rips/repp", "Tkaná rebrová väzba alebo výrazný rozdiel medzi sústavami nití.", "Súvislé priečne rebrá bez rezaného vlasu.", "Chrániť vrcholy rebier, zloženie a vnútorné vrstvy."),
        ("Pletený patent", "Striedanie lícnych a rubových stĺpikov očiek.", "Výrazná pružnosť najmä do šírky a zvislé stĺpiky.", "Nepliesť s tkaninou; riešiť vyťahanie a slučky."),
        ("Menčester", "Doplnkový útok tvorí vlas, ktorý sa rozreže alebo upraví.", "Mäkké pozdĺžne rebrá s vlasovým smerom.", "Chrániť vlas pred sploštením a kontrolovať smer."),
        ("Grosgrainová stuha", "Hustá pásková tkanina s priečnym rebrom.", "Úzky pevný pás s hotovými okrajmi.", "Lepidlá, potlač a farba môžu mať nižší limit než priadza."),
        ("Hladké plátno", "Pravidelné previazanie jedna nad, jedna pod.", "Bez dominantného reliéfneho rebra.", "Aj stabilná väzba môže byť jemná podľa priadze a hustoty."),
    ],
    "sections": [
        {
            "heading": "Prečo priečne rebro neznamená automaticky vyššiu životnosť",
            "paragraphs": [
                "Výrazný reliéf môže pôsobiť robustne, no prvý sa dotýka sedadla, popruhu, hrany stola aj bubna práčky. Oder sa preto sústreďuje na vyvýšené línie. Pri tmavom kuse sa najprv ukáže svetlejší vrchol, pretože sa zmení povrch priadze a odraz svetla; nemusí ísť o vypranú škvrnu ani o náhle vyblednutie celého materiálu.",
                "Životnosť závisí od vlákna, zákrutu priadze, hustoty, väzby, dokončenia a zaťaženia v smere rebier. Jemný hodvábny rips môže byť tvarovo presný, ale citlivý na zachytenie. Polyesterový rips môže lepšie držať rozmer, no poškodiť sa teplom. Bavlnený variant môže zniesť bežné pranie, ale zmeniť rozmer alebo farbu. Jedno slovo preto nenahrádza technické údaje.",
            ],
        },
        {
            "heading": "Ako vyčistiť prach a omrvinky medzi rebrami",
            "paragraphs": [
                "Voľný prach odstráňte pred navlhčením, aby sa z neho nevytvorila sivá mapa v priehlbinách. Odev jemne vytraste, mäkkou kefou pracujte v smere rebier a pri povolenom vysávaní použite nízky výkon a ochrannú sieťku. Tvrdú kefu neveďte naprieč reliéfom; môže zdrsniť priadzu a presunúť útok pri šve.",
                "Na čalúnení si vyznačte smer a postupujte po malých zónach. Medzera medzi sedákom a operadlom môže obsahovať piesok, ktorý pri mokrom drhnutí pôsobí ako abrazívum. Najprv ho odsajte. Ak je poťah neodnímateľný, štítok čalúnnickej zostavy a pokyny výrobcu majú prednosť pred všeobecným návodom na textil.",
            ],
        },
        {
            "heading": "Ako ošetriť škvrnu bez svetlého krúžku a sploštenia",
            "paragraphs": [
                f"Čerstvú kvapalinu odsajte bielou savou handričkou bez trenia. Pevnú nečistotu zdvihnite tupou hranou a postup zvoľte podľa jej typu; všeobecné rozdelenie nájdete v návode <a href=\"{ARTICLE_STAIN}\">ako odstraňovať rôzne škvrny z oblečenia</a>. Prostriedok nanášajte na handričku alebo podľa jeho návodu, nie koncentrovanou mlákou priamo na rebro.",
                "Pracujte od okraja ku stredu a navlhčite len takú plochu, ktorú viete rovnomerne opláchnuť alebo odsať. Po vyschnutí skontrolujte povrch z viacerých uhlov. Svetlejší kruh môže byť zvyšok produktu, presunuté farbivo alebo mechanicky sploštené rebrá. Každý z týchto problémov potrebuje iný ďalší krok, preto mapu okamžite nedrhnite silnejšie.",
            ],
        },
        {
            "heading": "Ako prať ripsové šaty, sukňu alebo nohavice",
            "paragraphs": [
                f"Najprv prečítajte <a href=\"{ARTICLE_LABEL}\">materiálový a ošetrovací štítok</a>. Vyprázdnite vrecká, zapnite bezpečné kovanie, uvoľnite odnímateľný opasok a skontrolujte podšívku. Ak je strojové pranie povolené, obrátenie naruby zníži priamy kontakt líca s bubnom, no nezruší zaťaženie švov ani potrebu triedenia.",
                "Rips perte s podobne ľahkými hladkými kusmi. Zipsy, háčiky, suchý zips a hrubé uteráky vytvárajú bodové zachytenie a oder. Nepreplňujte bubon: stlačené záhyby sa trú na rovnakom mieste a zhorší sa oplach medzi rebrami. Program, teplota, otáčky a sušenie musia zostať v hraniciach etikety celého odevu.",
            ],
        },
        {
            "heading": "Rips na saku, kabelke a čalúnení nemožno posudzovať ako tričko",
            "paragraphs": [
                "Sako môže obsahovať lepenú výstuž, ramenné vypchávky, tvarovanú chlopňu a podšívku. Kabelka má lepenky, výstuže, farbenú kožu, kovanie a lepidlá. Čalúnenie je napnuté na výplni a ráme. Voda, ktorá je prijateľná pre vzorku samotného ripsu, môže zmeniť tieto časti alebo vytiahnuť farbu z rubu na líc.",
                "Pri takýchto výrobkoch začnite suchým odstránením prachu a lokálnou skúškou podľa návodu výrobcu. Odnímateľný poťah perte iba vtedy, keď to povoľuje jeho vlastný štítok; zips sám osebe nie je symbol prateľnosti. Neodnímateľný poťah nenamáčajte do hĺbky bez plánu na odsatie a úplné vysušenie výplne.",
            ],
        },
        {
            "heading": "Sušenie bez ostrých lomov a vyťahania švov",
            "paragraphs": [
                f"Mokrý odev vyberte bez krútenia a podoprite jeho hmotnosť. Švy jemne urovnajte, ale rebrá nenaťahujte do šírky v snahe vyrovnať zdanlivé zrazenie. Zmenu rozmeru hodnotíte až po úplnom vyschnutí a ustálení; mechanizmy vysvetľuje článok <a href=\"{ARTICLE_SHRINKAGE}\">prečo sa oblečenie po praní zráža</a>.",
                "Na šnúre nepoužívajte kolík na viditeľnom vrchole a ťažkú sukňu nevešajte za jeden úzky bod. Sušte v tieni a s prúdením vzduchu podľa etikety. Pri podšívke otvorte vrecká a vrstvy tak, aby nezostali vlhké. Prudké teplo môže povrch ustáliť v deformovanom tvare skôr, než sa švy prirodzene vyrovnajú.",
            ],
        },
        {
            "heading": "Ako žehliť rips a zachovať jeho reliéf",
            "paragraphs": [
                f"Žehlenie povoľuje symbol, nie potreba rýchlo odstrániť záhyb. Odev obráťte naruby, použite čistú ochrannú tkaninu a čo najmenší tlak. Praktickú prácu s golierom a švami približuje návod <a href=\"{ARTICLE_IRONING}\">ako vyžehliť košeľu</a>, no rips navyše potrebuje mäkkú podložku, aby sa priečne rebrá nezatlačili do hladka.",
                "Paru aplikujte iba pri kompatibilnom vlákne a úprave. Nadmerná vlhkosť môže ovplyvniť viskózu, lepidlo alebo farbu a dlhé pritlačenie vytvoriť lesklý obdĺžnik. Najprv skúste skrytý lem, nechajte ho vychladnúť a skontrolujte z boku. Horúce miesto neohýbajte ani neukladajte pod ťažký predmet.",
            ],
        },
        {
            "heading": "Posun nití pri šve nie je to isté ako pretrhnutie",
            "paragraphs": [
                "Pri tesnej sukni alebo kabelke môže zaťaženie odtlačiť osnovné a útkové nite od línie stehu, hoci žiadna ešte nepraskla. Objaví sa úzka medzera alebo svetlejšie prúžky. Trhlina naopak znamená porušenie priadze. Rozlíšenie je dôležité, pretože opakované pranie ani zatavenie povrchu neposilní nevhodne namáhaný šev.",
                "Miesto odfoťte bez napínania a porovnajte symetrický šev. Odev ďalej nenoste v stave, ktorý medzeru zväčšuje. Krajčír môže upraviť rezervu šva, výstuž alebo rozloženie napätia, ak je okolitá tkanina zdravá. Lepidlo nanesené z líca vytvára tvrdú hranu a môže komplikovať neskoršiu opravu.",
            ],
        },
        {
            "heading": "Ako skladovať ripsové odevy, stuhy a dekoračné textílie",
            "paragraphs": [
                "Čistý suchý odev zaveste na primerane široký vešiak alebo zložte bez ostrého dlhodobého lomu na viditeľnom mieste. Medzi tmavý rips a svetlú citlivú textíliu vložte čistú priedušnú vrstvu, ak existuje riziko prenosu farby. Kabelku nepreťažujte a neskladujte ju pritlačenú k reliéfnemu kovaniu.",
                "Stuhu nenavíjajte okolo príliš malého jadra, ktoré zalomí rebrá, a čalúnnické vzorky označte zložením a šaržou. Pred sezónnym uložením odstráňte pot a mastnotu, pretože časom oxidujú a viažu prach. Vôňou neprekrývajte vlhkosť; najskôr zaistite čistotu a stabilné suché prostredie.",
            ],
        },
        {
            "heading": "Ako vybrať rips podľa použitia a požadovať užitočné údaje",
            "paragraphs": [
                "Pri odeve sledujte zloženie, plošnú hmotnosť, splývavosť, podšívku a povolenú údržbu. Pri poťahu sa pýtajte na určené použitie, odolnosť proti oderu, žmolkovaniu, stálofarebnosť pri trení a čistenie konkrétnej povrchovej úpravy. Číslo bez názvu skúšobnej metódy a podmienok nemožno bezpečne porovnať s iným číslom.",
                "Vzorku ohnite v oboch smeroch, skúste posun nití pri okraji bez násilia a pozorujte farbu pri bočnom svetle. Malá vzorka však nepredpovie správanie lepidla, šva alebo veľkej čalúnenej plochy. Kvalitný výber spája vhodnú konštrukciu s opraviteľnosťou a zrozumiteľným návodom, nie iba s pevným prvým dojmom.",
            ],
        },
    ],
    "table2_heading": "Zmena na ripse po používaní alebo čistení: čo môže znamenať",
    "table2_intro": "Povrch posudzujte po úplnom vysušení pri rovnakom osvetlení. Reliéf mení odraz, preto jeden svetlý pás nemusí mať iba jednu príčinu.",
    "table2_headers": ["Prejav", "Pravdepodobné vysvetlenie", "Čo skontrolovať", "Bezpečný ďalší krok"],
    "table2_rows": [
        ("Svetlé vrcholy rebier", "Povrchový oder, zmena odrazu alebo úbytok farby.", "Či je priadza zdrsnená a či sa jav mení podľa uhla svetla.", "Nepridávať trenie; pri novom kuse zdokumentovať rozsah."),
        ("Úzka medzera pri šve", "Posun nití pre napätie, strih alebo nevhodný šev.", "Neporušenosť priadzí a symetrický šev.", "Prestať zaťažovať a konzultovať krajčírsku opravu."),
        ("Lesklý hladký obdĺžnik", "Príliš horúca žehlička alebo vysoký tlak.", "Zmenu reliéfu, omaku a zloženie vlákna.", "Nezohrievať znova; odborné posúdenie po vychladnutí."),
        ("Tmavá mapa v priehlbinách", "Zvyšok produktu, mastnota alebo nerovnomerné navlhčenie.", "Prenos na bielu handričku a stav po úplnom vysušení.", "Šetrný kompatibilný oplach alebo lokálne odborné čistenie."),
        ("Skrútený lem alebo podšívka ťahá", "Rozdielna rozmerová zmena vrstiev.", "Suché rozmery, švy a povolený cyklus.", "Nenapínať mokré; riešiť po ustálení alebo reklamovať."),
    ],
    "steps_heading": "Ako ošetriť ripsový odev krok za krokom",
    "steps": [
        "Zistite vláknové zloženie, podšívku, výstuž, ozdoby a všetky symboly na etikete celého odevu.",
        "Pri bočnom svetle skontrolujte vrcholy rebier, priehlbiny, švy, lemy a existujúce lesklé miesta.",
        "Prach odstráňte nasucho po smere rebier; škvrnu odsajte a kompatibilný postup skúste na skrytom mieste.",
        "Ak je pranie povolené, obráťte kus naruby a oddeľte ho od zipsov, háčikov, suchých zipsov a ťažkých textílií.",
        "Použite predpísaný program, primeranú náplň a dávku; koncentrovaný produkt nelejte priamo na reliéf.",
        "Po cykle odev ihneď podoprite, bez krútenia urovnajte švy a sušte podľa etikety s prúdením vzduchu.",
        "Žehlite len pri povolení z rubu, cez ochrannú tkaninu a na mäkkej podložke s minimálnym tlakom.",
        "Po úplnom vysušení porovnajte rozmery, farbu a reliéf; rastúci posun nití alebo delamináciu ďalej nezaťažujte.",
    ],
    "remember": [
        "Je povrch skutočne tkaný rips, alebo pletený patent, menčester či stuha?",
        "Aké vlákno, podšívka, výstuž a povrchová úprava určujú najnižší limit?",
        "Je svetlá línia škvrna, oder vrcholu, posun nití alebo zmena odrazu?",
        "Povoľuje etiketa vodu, práčku, sušičku, paru a konkrétnu teplotu žehlenia?",
        "Je náplň bez tvrdého kovania a má dostatok priestoru na pohyb a oplach?",
        "Je výrobok úplne suchý aj vo švoch, podšívke a vystužených miestach?",
    ],
    "mistakes": [
        "Zameniť tkaný rips za pletený patent a očakávať rovnakú pružnosť aj pranie.",
        "Drhnúť priečne cez rebrá tvrdou kefou alebo s pieskom ponechaným v priehlbinách.",
        "Prať vystuženú kabelku či sako podľa odolnosti samotnej lícnej tkaniny.",
        "Tlačiť žehličkou na reliéf, kým sa plocha neleskne a nesploští.",
        "Napínať mokrý šev, keď sa nite už posunuli od stehu.",
        "Zakryť mastnotu alebo vlhkosť vôňou a uložiť predmet bez úplného vysušenia.",
    ],
    "expert_heading": "Odbornejší pohľad: väzbový efekt, smer skúšky a porovnateľnosť výsledkov",
    "expert": [
        "Základné tkaniny vznikajú krížením osnovy a útku podľa väzbového opakovania. Rebrový efekt možno vytvoriť zoskupením väzbových bodov a rozdielom medzi sústavami nití, takže vzhľad závisí od hrúbky, počtu a prekrytia. CottonWorks vysvetľuje, že väzba mení stabilitu, splývavosť a riziko posunu; spotrebiteľský názov rips však neurčuje konkrétnu hustotu ani vlákno.",
        "ASTM D3775 meria počet osnovných a útkových nití na jednotku dĺžky, ASTM D1424 pokračovanie trhliny a ASTM D4966 oder Martindale. Ide o rozdielne veličiny. Vyšší počet nití automaticky nepredpovedá odolnosť vrcholu rebra ani šva a výsledok v jednom smere nemožno preniesť do druhého bez uvedenia orientácie vzorky.",
        "AATCC TM135 sleduje rozmerovú zmenu po definovaných domácich postupoch. Hotová sukňa však zahŕňa švy, smer strihu, podšívku a tvarové zaťaženie, preto laboratórna hodnota tkaniny nie je povolením na ľubovoľný cyklus. Užitočný údaj vždy obsahuje metódu, podmienky a konkrétny výrobok, na ktorý sa vzťahuje.",
    ],
    "source_intro": "Zdroje vysvetľujú väzbu, počet nití, pokračovanie trhliny, oder a rozmerové zmeny. Nepodporujú jednu univerzálnu teplotu pre všetky ripsové výrobky.",
    "sources": [
        ("CottonWorks: odborný prehľad tkania a základných väzieb", COTTONWORKS_WEAVING),
        ("CottonWorks: základné konštrukcie tkanín", COTTONWORKS_BASIC_WEAVES),
        ("ASTM D3775: počet osnovných a útkových nití", ASTM_COUNT),
        ("ASTM D1424: pokračovanie trhliny v tkanine", ASTM_TEAR),
        ("ASTM D4966: oder textílií metódou Martindale", ASTM_ABRASION),
        ("AATCC TM135: rozmerové zmeny po domácom praní", AATCC_DIMENSION),
        ("EÚ 1007/2011: označovanie textilných vlákien", EU_FIBRE_LABEL),
        ("GINETEX: význam symbolov ošetrovania", GINETEX),
    ],
    "product_heading": "Prací gél použite iba na rips, ktorý má povolené domáce pranie",
    "product_intro": "Pri bežnom prateľnom bavlnenom, polyesterovom alebo kompatibilnom zmesovom odeve môže presne nadávkovaný gél pomôcť odstrániť bežné znečistenie bez nalievania koncentrátu na reliéf.",
    "product_name": LAUNDRY_PRODUCT_NAME,
    "product_url": LAUNDRY_PRODUCT_URL,
    "product_text": "Tekutý gél sa dá dávkovať podľa tvrdosti vody, náplne a znečistenia. Rips perte s priestorom na oplach a produkt nelejte priamo na suchý vrchol rebra bez pokynu výrobcu.",
    "product_limit": "Gél neobnoví odreté alebo žehličkou sploštené rebrá a nie je automaticky vhodný na hodváb, vlnu, citlivú viskózu, čalúnenie, lepenú kabelku ani odev určený na profesionálne čistenie.",
    "category_heading": "Prací prostriedok vyberte podľa vlákna a celého výrobku",
    "category_intro": "Dve ripsové tkaniny môžu mať rovnaký reliéf a rozdielne zloženie. Kategóriu preto prezerajte až po kontrole etikety, farby, výstuže a povoleného cyklu.",
    "category_name": LAUNDRY_CATEGORY_NAME,
    "category_url": LAUNDRY_CATEGORY_URL,
    "category_text": "V kategórii nájdete gély pre rôzne potreby bežnej prateľnej bielizne. Vyberte kompatibilný produkt, dodržte dávku a pri špeciálnom vlákne použite starostlivosť určenú výrobcom.",
    "related": [
        ("Ako čítať štítok na oblečení", ARTICLE_LABEL),
        ("Prečo sa oblečenie po praní zráža", ARTICLE_SHRINKAGE),
        ("Ako farby blednú pri praní, svetle a trení", ARTICLE_COLOR),
        ("Ako odstrániť rôzne škvrny z oblečenia", ARTICLE_STAIN),
        ("Ako správne vyžehliť košeľu", ARTICLE_IRONING),
        ("Prečo sa oblečenie žmolkuje", ARTICLE_PILLING),
    ],
    "faq_title": "rips a priečne rebrované tkaniny",
    "faq": [
        ("Čo je rips?", "Rips je tkanina s výrazným rebrovým efektom, zvyčajne priečnym. Názov opisuje konštrukciu povrchu, nie jedno konkrétne vlákno."),
        ("Je rips to isté ako pletený patent?", "Nie. Rips je tkaný z osnovy a útku, kým patent je slučkový úplet s typickou pružnosťou do šírky."),
        ("Je rips to isté ako menčester?", "Nie. Menčester má vlasové rebrá, rips vytvára reliéf väzbou a rozdielom nití bez typického rezaného vlasu."),
        ("Môže ísť rips do práčky?", "Iba ak to povoľuje etiketa hotového výrobku. Zloženie, podšívka, výstuž a povrchová úprava môžu strojové pranie vylúčiť."),
        ("Na koľko stupňov prať rips?", "Jedna teplota neexistuje. Riaďte sa symbolom a najcitlivejšou súčasťou konkrétneho kusu."),
        ("Ako odstrániť prach medzi rebrami?", "Nasucho, jemne a po smere rebier. Pri vysávači použite nízky výkon a ochranu povrchu, ak to výrobok umožňuje."),
        ("Prečo sú vrcholy rebier svetlejšie?", "Môže ísť o povrchový oder alebo zmenu odrazu svetla. Ak je priadza zdrsnená, ďalšie drhnutie jav zhorší."),
        ("Ako žehliť rips?", "Len pri povolení etikety, z rubu, cez ochrannú tkaninu, na mäkkej podložke a s malým tlakom."),
        ("Dá sa opraviť posun nití pri šve?", "Niekedy krajčírskou úpravou šva alebo rozloženia napätia. Miesto ďalej nenapínajte a nelepite z líca."),
        ("Môže ísť rips do sušičky?", "Iba s výslovným symbolom. Teplo a prevaľovanie môžu meniť rozmer, reliéf, podšívku aj výstuž."),
        ("Ako skladovať ripsovú sukňu alebo sako?", "Čisté a úplne suché, na primeranej opore alebo bez ostrého lomu. Reliéf nestláčajte ťažkými predmetmi."),
    ],
}


VATELIN: dict[str, object] = {
    "title": "Čo je vatelín: objemová výplň, zliehanie a bezpečné pranie",
    "link": "co-je-vatelin-objemova-vypln-zliehanie-a-bezpecne-pranie",
    "meta": "Čo je vatelín, ako sa líši od peny a vlizelínu, prečo zlieha a ako prať, sušiť, skladovať a opraviť výrobok s objemovou výplňou.",
    "short": "Vatelín je plošná objemová výplň z vrstvy vlákien, často spevnená mechanicky alebo tepelne. O praní nerozhoduje výplň sama, ale poťah, prešívanie, spojivo, rozmery a návod celého paplóna, bundy, podložky či dekoračného výrobku.",
    "name": "vatelín",
    "locative": "vatelíne",
    "identity_heading": "Vatelín je funkčná výplň, nie presný názov jedného polyméru",
    "identity_detail": "Vlákna sú rozložené do objemnej plošnej vrstvy a spevnené vpichovaním, tepelnou väzbou, spojivom alebo ďalšou nosnou vrstvou; obchodné výrobky sa líšia zložením, hustotou, hrúbkou a odolnosťou pri praní.",
    "identity_boundary": "Polyesterový, bavlnený, vlnený alebo zmesový vatelín môže mať inú zotaviteľnosť po stlačení a iný tepelný limit, pričom hotový výrobok pridáva poťah, švy, lepidlo a ozdoby.",
    "label_focus": "zloženie výplne aj poťahu, prešívanie, lepenie, odnímateľnosť, maximálne rozmery pre práčku, povolené odstreďovanie a sušenie",
    "missing_label": "Neoznačenú výplň v staršom výrobku nemožno spoľahlivo identifikovať dotykom; pri veľkom paplóne, čalúnení alebo citlivej pamiatke neexperimentujte ponorením.",
    "dry_check": "tenké miesta, hrče, presun výplne, prasknuté prešívanie, vystupujúce vlákna, staré škvrny, zápach vo vnútri a oddelenie poťahu",
    "damage_boundary": "Povrchový prach alebo škvrnu možno čistiť, no roztrhnuté kotviace stehy a nenávratne zlisovanú vrstvu voda znovu rovnomerne nerozloží.",
    "test_focus": "Po vysušení porovnajte hrúbku, pružné zotavenie, hrany a miesto spojenia s poťahom; dočasne mokrá výplň je vždy ťažšia a sploštenejšia.",
    "combined_risk": "nasýtenia celej hrúbky, pohybu voľnejších vlákien, bodového zaťaženia mokrou hmotnosťou a tepla na termicky viazané miesta",
    "chemistry_boundary": "Odstraňovač vhodný na bavlnený poťah môže zanechať koncentrovaný zvyšok vo výplni alebo ovplyvniť spojivo, preto nemožno ošetriť hlbokú škvrnu bez plánu na oplach.",
    "drying_detail": "Paplón, bunda alebo podsedák musí mať prúdenie vzduchu z oboch strán a pravidelne meniť polohu bez ťahania za mokré rohy či úzke prešívanie.",
    "heat_boundary": "Horúci bubon, radiátor alebo fén môže zraziť poťah, zmeniť termicky viazané vlákna, vytvoriť tvrdé body alebo ustáliť výplň v hrči.",
    "stop_signs": "rastúca hrča, rozpad vrstvy, prenos farby, lepkavé spojivo, vystupovanie výplne, trhanie mokrého šva alebo vlhký pach po dlhom sušení",
    "professional_boundary": "Prateľný prešívaný výrobok primeranej veľkosti možno ošetriť doma podľa etikety, zatiaľ čo matrac, čalúnenie, lepená dekorácia, veľký paplón bez kapacity sušenia alebo starý neoznačený kus vyžaduje iný postup.",
    "answer": "Vatelín je objemová vrstva vlákien používaná ako výplň a tepelná alebo tvarová medzivrstva. Nie je to molitan ani nažehľovací vlizelín. Perte ho iba ako súčasť výrobku, ktorého etiketa povoľuje vodu a ktorý má dostatočné prešívanie, priestor v bubne a reálnu možnosť vyschnúť v celej hrúbke.",
    "intro": "Najväčšie riziko pri vatelíne nevzniká iba z teploty, ale z mokrej hmotnosti, presunu vlákien a pomalého vysušenia. Tenká bunda môže byť bežne prateľná, zatiaľ čo rovnako zložená hrubá podložka sa do domácej práčky nezmestí a po namočení ostane vlhká vo vnútri. Bez údajov o celom výrobku preto neexistuje jeden bezpečný program.",
    "quick": [
        "Vatelín je plošná vláknová výplň; pena je súvislý bunkový materiál a vlizelín je najmä výstužná medzivrstva s inou funkciou.",
        "Hrúbka bez uvedeného tlaku nie je úplne porovnateľná. Mäkký high-loft materiál sa pri meraní a používaní stláča.",
        "Prešívanie alebo kotviace body bránia presunu výplne. Ich rozostup musí zodpovedať pokynom konkrétneho vatelínu.",
        "Pred praním overte kapacitu práčky aj sušenia; objemný kus potrebuje priestor na pohyb, oplach a prúdenie vzduchu.",
        "Mokrý výrobok nezdvíhajte za jeden roh. Rozložte hmotnosť a nekrúťte ho, aby sa nepretrhli švy a výplň neposunula.",
        "Hrču po cykle najprv nechajte úplne vyschnúť a jemne rozložte rukami. Ďalšie horúce pranie bez diagnózy môže poškodenie ustáliť.",
    ],
    "overview_heading": "Ako vatelín vytvára objem a prečo jeho vzduchová štruktúra rozhoduje o funkcii",
    "overview": [
        "Objemová výplň pracuje najmä s priestorom medzi vláknami. Zachytený vzduch prispieva k tepelnému komfortu a pružná sieť vlákien pomáha vrstve obnoviť časť hrúbky po stlačení. Samotná vyššia hrúbka však neznamená automaticky lepšiu izoláciu alebo životnosť; záleží na plošnej hmotnosti, orientácii vlákien, spojení a na tom, ako sa vrstva správa pri opakovanom zaťažení.",
        "Vatelín môže byť vpichovaný, tepelne viazaný, chemicky spájaný alebo kombinovaný s nosičom. Niektoré typy sa pri šití vkladajú voľne a kotvia prešívaním, iné sa nažehľujú na vrchnú látku podľa presných parametrov. Označenie z e-shopu preto musí sprevádzať technický list, najmä údaj o zložení, hrúbke, plošnej hmotnosti, maximálnom rozstupe prešívania a údržbe.",
        "V domácnosti ho nájdete v bundách, dekách, patchworkových prikrývkach, chňapkách, podsedákoch, ochranných obaloch a dekoráciách. Každý výrobok má inú hrúbku a mieru kotvenia. Pri starostlivosti je rozhodujúce, či voda a oplach prejdú celým prierezom a či sa mokrá vrstva môže vysušiť skôr, než vznikne zatuchnutie alebo presun výplne.",
    ],
    "table1_heading": "Vatelín, rúno, pena a vlizelín: čo je vo vnútri výrobku",
    "table1_intro": "Názvy sa v bežnej reči prekrývajú. Funkciu a údržbu určte podľa technického listu a rozobrateľnej vzorky, nie podľa jediného dotyku cez poťah.",
    "table1_headers": ["Vrstva", "Základná stavba", "Typická úloha", "Riziko pri čistení"],
    "table1_rows": [
        ("Vatelín/batting", "Objemná plošná sieť vlákien.", "Výplň, tepelná vrstva, zmäkčenie.", "Zliehanie, presun, dlhé schnutie a strata kotvenia."),
        ("Voľná vláknová výplň", "Samostatné chumáče alebo vločky bez súvislej plošnej vrstvy.", "Vankúše, hračky a tvarované dutiny.", "Migrácia do rohov, hrče a nerovnomerné vysušenie."),
        ("Pena/molitan", "Súvislá polymérna bunková štruktúra.", "Tvarová opora a odpruženie.", "Zadržaná voda, degradácia, nevhodné rozpúšťadlá a teplo."),
        ("Vlizelín/interlining", "Tenkšia výstužná vrstva, často s lepidlom.", "Stabilizácia goliera, pásu alebo dielu.", "Delaminácia a rozdielne zrazenie vrstiev."),
        ("Prešívaná zostava", "Poťah, výplň a rub spojené stehmi.", "Udržať vrstvu v ploche a obmedziť presun.", "Prasknutý steh dovolí výplni migrovať pri mokrom pohybe."),
    ],
    "sections": [
        {
            "heading": "Čo znamená loft, plošná hmotnosť a hrúbka v praxi",
            "paragraphs": [
                "Loft opisuje objemný charakter a schopnosť vrstvy držať hrúbku, nie jednu univerzálnu jednotku kvality. Dve výplne rovnakej hrúbky môžu mať rozdielnu plošnú hmotnosť a zotavenie. Ľahká vrstva môže byť vzdušnejšia, ťažšia zasa kompaktnejšia. Pri výrobku treba posudzovať tepelný cieľ, ohyb, pranie a prešívanie ako jeden systém.",
                "ASTM D1777 upozorňuje, že zdanlivá hrúbka textílie závisí od tlaku meracej pätky. Číslo bez uvedenia metódy a tlaku preto neporovnávajte priamo. Pre spotrebiteľa je praktické zmerať rovnaké označené miesto na suchom výrobku pred a po používaní, vždy bez stlačenia rukou a po dostatočnom odpočinku vrstvy.",
            ],
        },
        {
            "heading": "Prečo vatelín zlieha, tvorí hrče alebo sa presúva do rohov",
            "paragraphs": [
                "Vlákna sa pri opakovanom tlaku ohýbajú, kĺžu a strácajú priestor medzi sebou. Teplo môže ovplyvniť termicky spojené body a dlhé mokré prevaľovanie zvyšuje pohyb vrstvy. Ak je prešívanie riedke alebo praskne, väčšia plocha sa môže posunúť ako celok. Hrča preto nie je automaticky dôsledok priveľkého množstva pracieho gélu.",
                "Najprv porovnajte rozloženie po úplnom vysušení. Mäkkú posunutú výplň možno niekedy jemne rozpracovať medzi dlaňami bez trhania švov. Tvrdý zlisovaný ostrov, rozpadávajúce sa vlákna alebo lepkavé miesto naznačujú poškodenie spojiva či teplom. Ďalšie horúce sušenie potom nie je opravou.",
            ],
        },
        {
            "heading": "Prešívanie je technická súčasť, nie iba dekorácia",
            "paragraphs": [
                "Stehy rozdeľujú veľkú plochu na menšie polia a držia výplň pri poťahu. Každý typ vatelínu má odporúčaný maximálny rozstup prešívania; jemný voľnejší materiál môže potrebovať hustejšie kotvenie než pevne viazaná vrstva. Príliš hustý steh však môže vytvoriť chladné línie, stuhnúť omak a pri slabej látke perforovať poťah.",
                "Pred praním skontrolujte začiatky, konce a križovanie stehov. Uvoľnené miesto opravte skôr, než mokrá hmotnosť zväčší otvor. Pri paplóne neťahajte za jednu prešívaciu líniu a pri domácom šití vyrobte skúšobný sendvič z rovnakého poťahu, nite a výplne. Ošetrite ho budúcim cyklom ešte pred dokončením celého výrobku.",
            ],
        },
        {
            "heading": "Ako prať bundu s vatelínom bez presunu výplne",
            "paragraphs": [
                f"Riaďte sa etiketou bundy a skontrolujte vrchný materiál, membránu, podšívku, manžety, zipsy a kapucňu. Návod <a href=\"{ARTICLE_LABEL}\">ako čítať štítok</a> pomôže oddeliť zloženie od symbolov. Ak je pranie povolené, zatvorte bezpečné zapínanie, vyprázdnite vrecká a oddeľte bundu od ťažkých či drsných kusov.",
                "Použite primeranú náplň a cyklus, aby sa bunda mohla pohybovať a dôkladne opláchnuť, ale nenarážala sama prudko do bubna. Po skončení ju vyberte ihneď a podoprite oboma rukami. Výplň nerozťahujte za mokra. Sušenie a prípadné jemné rozrušovanie hrudiek vykonávajte iba spôsobom, ktorý povoľuje výrobca celej bundy.",
            ],
        },
        {
            "heading": "Ako vyprať prešívanú deku alebo paplón a nepreťažiť práčku",
            "paragraphs": [
                "Suchý objem nehovorí, aký ťažký bude kus po nasiaknutí. Pred vložením overte hmotnostnú aj objemovú kapacitu práčky, voľný pohyb bubna a odporúčanie výrobcu. Natlačený paplón sa nemusí rovnomerne navlhčiť ani opláchnuť a pri odstreďovaní vytvorí nevyváženú hmotu. Väčšia profesionálna práčka môže byť bezpečnejšia než domáci kompromis.",
                f"Po cykle kus prenášajte v nádobe alebo s rovnomernou oporou a sušte podľa etikety z oboch strán. V malom byte je kritické prúdenie vzduchu; súvisiaci postup nájdete v článku <a href=\"{ARTICLE_DRYING}\">ako sušiť bielizeň bez zatuchnutia</a>. Ak neviete zaistiť úplné vyschnutie hrubej vrstvy, nezačínajte celoplošné mokré čistenie doma.",
            ],
        },
        {
            "heading": "Lokálna škvrna na výplňovom výrobku: povrch verzus hĺbka",
            "paragraphs": [
                "Najprv zistite, či škvrna zostala na odnímateľnom poťahu alebo prenikla do výplne. Povrchovú kvapalinu odsajte bez tlačenia do hĺbky. Koncentrovaný prostriedok môže vo vatelíne zostať aj po tom, čo je líc na dotyk čistý. Preto používajte minimálne množstvo kompatibilného roztoku a majte pripravené odsatie a sušenie.",
                f"Bielkovina, mastnota, farbivo a vosk potrebujú rozdielne postupy; orientáciu poskytuje článok <a href=\"{ARTICLE_STAIN}\">o odstraňovaní rôznych škvŕn</a>. Ak kontaminácia prenikla hlboko, je hygienicky významná alebo sa nedá opláchnuť bez premočenia veľkého predmetu, odborné extrakčné čistenie či výmena výplne môže byť rozumnejšia než opakované domáce bodovanie.",
            ],
        },
        {
            "heading": "Sušenie celej hrúbky a kontrola skrytej vlhkosti",
            "paragraphs": [
                "Vonkajšia látka môže byť suchá, kým jadro pri šve alebo prešívaní zostáva chladné a vlhké. Výrobok pravidelne obracajte a podopierajte, aby vzduch dosiahol obe strany. Hrubé miesta porovnajte rukou bez silného stlačenia a po krátkom uzavretí skontrolujte, či sa nevracia vlhký pach. Pred uložením si nechajte časovú rezervu.",
                "Sušičku používajte iba pri výslovnom symbole a v režime určenom výrobcom. Pomôcky pridávané do bubna nesmú poškodiť poťah ani stroj a nenahrádzajú povolenú teplotu. Radiátor a horúci fén vysušujú povrch nerovnomerne, kým vnútro môže ostať mokré a termicky viazané body sa lokálne zmenia.",
            ],
        },
        {
            "heading": "Ako skladovať vatelín bez trvalého stlačenia a vlhkosti",
            "paragraphs": [
                "Čistý úplne suchý výrobok skladujte voľne alebo mierne zložený podľa priestoru, nie dlhodobo vákuovo stlačený, ak výrobca taký spôsob nepotvrdzuje. Ťažké predmety vytvárajú ostré zóny tlaku a opakovaný rovnaký lom oslabuje poťah aj výplň. Sezónnu bundu zaveste na širokú oporu, ak sa ramená nevyťahujú.",
                "Priedušný obal chráni pred prachom, ale nevyrieši vlhkú skriňu. Pred uložením skontrolujte pach, švy a hrúbku v rovnakých bodoch. Vôňu pridávajte iba na čistý kompatibilný textil podľa návodu; parfumovanie nie je metóda sušenia ani odstránenia mikrobiálneho zdroja pachu vo vnútri výplne.",
            ],
        },
        {
            "heading": "Oprava prešívania, tenkého miesta a poškodenej výplne",
            "paragraphs": [
                "Uvoľnený steh opravte pred praním tak, aby nová niť neťahala slabý poťah. Pri lokálne posunutej mäkkej výplni možno otvoriť konštrukčný šev a vrstvu vyrovnať, ak je výrobok opraviteľný. Tenké miesto spôsobené trvalým rozpadom vlákien sa však nedá doplniť cez líc bez otvorenia a rovnomerného napojenia novej vrstvy.",
                "Pri odeve sledujte hrúbku aj symetriu a zachovajte voľnosť pohybu. Pri chňapke alebo ochrannej pomôcke poškodená výplň mení funkciu a domáca kozmetická oprava nemusí byť bezpečná. Výrobok určený na ochranu pred teplom, nárazom alebo iným rizikom vymeňte alebo opravte podľa výrobcu, nie iba podľa vzhľadu.",
            ],
        },
        {
            "heading": "Ako vybrať vatelín pri šití a overiť budúcu údržbu",
            "paragraphs": [
                "Vyberajte podľa zloženia, plošnej hmotnosti, hrúbky pri definovanom tlaku, omaku, maximálneho rozstupu prešívania a povoleného prania. Overte, či je materiál nažehľovací alebo voľný a ktorou stranou sa používa. Tepelnú výplň do chňapky nemožno zameniť za bežný polyesterový vatelín bez údajov o určenej funkcii.",
                "Pred strihaním pripravte poťah spôsobom zodpovedajúcim jeho budúcej údržbe a vytvorte skúšobný sendvič. Zmerajte ho, operte podľa plánovanej etikety, úplne vysušte a znova posúďte rozmery, hrúbku, hrče a stehy. Uchovajte názov, šaržu a technický list, aby neskoršia oprava nebola založená na odhade.",
            ],
        },
    ],
    "table2_heading": "Vatelín po praní alebo používaní: diagnostika bez ďalšieho poškodenia",
    "table2_intro": "Zmenu hodnotíte až po úplnom vysušení a odpočinku. Mokrý materiál je prirodzene ťažší a stlačenejší, takže predčasný záver môže viesť k zbytočnému teplu.",
    "table2_headers": ["Prejav", "Možná príčina", "Čo overiť", "Bezpečný ďalší krok"],
    "table2_rows": [
        ("Mäkká hrča v jednom poli", "Presun výplne po prasknutí stehu alebo mokrom pohybe.", "Kotviace švy a prázdne miesto vedľa hrče.", "Po vysušení jemne rozložiť a opraviť kotvenie."),
        ("Celá vrstva je tenšia", "Trvalé stlačenie, teplo, zliehanie alebo rozdiel merania.", "Rovnaké body, rovnaký tlak a čas odpočinku.", "Nezohrievať; porovnať funkciu a zvážiť výmenu."),
        ("Tvrdé ostrovčeky", "Zmena spojiva, lokálne teplo alebo zaschnutý produkt.", "Lepkavosť, zápach a súvis s aplikovaným miestom.", "Nepridávať chémiu; odborné posúdenie alebo výmena vrstvy."),
        ("Vlhký pach sa vracia", "Jadro nie je suché alebo ostala kontaminácia vo výplni.", "Chladné hrubé miesta, švy a čas sušenia.", "Pokračovať v bezpečnom vetraní; pri kontaminácii odborné čistenie."),
        ("Výplň vystupuje cez poťah", "Poškodená tkanina, nevhodná ihla, trenie alebo migrácia vlákien.", "Rozsah otvoru a pevnosť okolitého materiálu.", "Zastaviť používanie, opraviť poťah bez ťahania výplne."),
    ],
    "steps_heading": "Ako bezpečne vyprať výrobok s vatelínom krok za krokom",
    "steps": [
        "Prečítajte etiketu celého výrobku a určte poťah, výplň, prešívanie, lepenie, ozdoby a najnižší limit.",
        "Odfoťte hrúbku, hrče, tenké miesta a poškodené stehy; uvoľnené kotvenie opravte pred vodou.",
        "Overte, že výrobok má v práčke dostatok priestoru a po praní ho viete úplne vysušiť v celej hrúbke.",
        "Škvrnu odsajte a kompatibilný prostriedok otestujte bez zatláčania nečistoty do výplne.",
        "Použite iba povolený cyklus, presnú dávku a primerané odstreďovanie; mokrú vrstvu nekrúťte.",
        "Výrobok vyberte s oporou celej hmotnosti a bez ťahania za roh alebo jednu prešívaciu líniu.",
        "Sušte podľa symbolu z oboch strán, pravidelne meňte polohu a hrče upravujte až šetrne a bez trhania.",
        "Pred uložením overte suchosť švov a jadra, návrat objemu, pach a neporušenosť kotvenia.",
    ],
    "remember": [
        "Je vrstva skutočne vatelín, voľná výplň, pena alebo nažehľovacia výstuž?",
        "Aké zloženie, spojivo, hrúbka a rozstup prešívania uvádza technický list?",
        "Má práčka dostatočný objem a unesie mokrú hmotnosť bez nevyváženého cyklu?",
        "Viete po praní zaistiť prúdenie vzduchu a úplné vysušenie celej hrúbky?",
        "Sú kotviace stehy a poťah neporušené ešte pred namočením?",
        "Je výrobok ochrannou pomôckou, pri ktorej zmena výplne ovplyvňuje bezpečnosť?",
    ],
    "mistakes": [
        "Považovať každý biely objemový materiál za rovnaký polyesterový vatelín.",
        "Natlačiť veľký paplón do malej práčky a očakávať rovnomerné pranie aj oplach.",
        "Zdvíhať nasiaknutý výrobok za jeden roh a roztrhnúť kotviaci steh.",
        "Sušiť hrubú výplň iba z jednej strany alebo horúcim fénom na jednom mieste.",
        "Pridávať ďalší cyklus na mokrú hrču bez čakania na úplné vysušenie.",
        "Použiť bežný vatelín v ochrannej pomôcke bez potvrdenia určenej funkcie a tepelnej odolnosti.",
    ],
    "expert_heading": "Odbornejší pohľad: high-loft netkaná vrstva, tlak pri meraní a zotavenie",
    "expert": [
        "ISO 9092 definuje slovnú zásobu netkaných textílií, no spotrebiteľské slovo vatelín môže zahŕňať viac výrobných systémov. Dôležité je, ako sú vlákna spevnené a či je vrstva samonosná, lepená alebo iba zachytená medzi poťahmi. Funkciu hotového výrobku nemožno odvodiť iba zo všeobecného názvu.",
        "ASTM D1777 meria hrúbku pri určenom tlaku a upozorňuje, že zdanlivá hodnota klesá so zvýšením tlaku. ASTM D6571 sa zameriava na odpor proti stlačeniu a zotavenie high-loft netkanej textílie pri statickom zaťažení. Tieto vlastnosti súvisia, ale nie sú totožné s tepelnou izoláciou, životnosťou šva ani povolením na pranie.",
        "Technická dokumentácia Vlieseline ukazuje, že objemové rúna majú produktovo špecifické zloženie, hmotnosť, spôsob aplikácie a symboly. Rovnaká značka ponúka varianty s rozdielnymi limitmi, čo je praktický dôkaz, prečo nemožno preniesť jeden návod na neoznačený vatelín. Pri šití aj údržbe je technický list nadradený vzhľadu vzorky.",
    ],
    "source_intro": "Zdroje oddeľujú netkanú konštrukciu, meranie hrúbky, zotavenie po stlačení a produktovo špecifické limity. Žiadny z nich nepovoľuje pranie neoznačeného hotového výrobku iba podľa vzhľadu výplne.",
    "sources": [
        ("ISO 9092:2026: slovná zásoba netkaných textílií", ISO_NONWOVEN),
        ("ASTM D1777-26: hrúbka textilných materiálov", ASTM_THICKNESS),
        ("ASTM D6571-22: stlačenie a zotavenie high-loft netkaných textílií", ASTM_HIGHLOFT),
        ("Vlieseline 2025: technické údaje objemových rún", VLIESELINE_BROCHURE),
        ("AATCC TM135: rozmerové zmeny po domácom praní", AATCC_DIMENSION),
        ("EÚ 1007/2011: označovanie textilných vlákien", EU_FIBRE_LABEL),
        ("GINETEX: význam symbolov ošetrovania", GINETEX),
    ],
    "product_heading": "Prací gél patrí iba k výrobku s výslovne povoleným praním",
    "product_intro": "Pri prateľnej bunde, prešívanej deke alebo inom kompatibilnom výrobku možno zvoliť gél podľa poťahu a výplne. Najprv však overte kapacitu, dávku, oplach a sušenie celej hrúbky.",
    "product_name": LAUNDRY_PRODUCT_NAME,
    "product_url": LAUNDRY_PRODUCT_URL,
    "product_text": "Tekutý gél umožňuje presné dávkovanie a rovnomerné rozpustenie v povolenom cykle. Objemný kus potrebuje priestor a dôkladný oplach, aby produkt neostal v jadre.",
    "product_limit": "Gél nie je určením prateľnosti a neobnoví rozpadnutú, zlisovanú alebo presunutú výplň. Nepoužívajte ho automaticky na matrac, penu, lepenú dekoráciu, ochrannú pomôcku ani kus s profesionálnym symbolom.",
    "category_heading": "Prostriedok porovnajte až po posúdení výplňového systému",
    "category_intro": "Poťah, výplň a spojenie vrstiev môžu mať rozdielne limity. Pri povolenom domácom praní vyberajte produkt podľa najcitlivejšej časti a dôsledne rešpektujte jeho dávku.",
    "category_name": LAUNDRY_CATEGORY_NAME,
    "category_url": LAUNDRY_CATEGORY_URL,
    "category_text": "Kategória ponúka pracie gély pre bežnú prateľnú bielizeň. Pri objemnom výrobku osobitne skontrolujte kompatibilitu s vláknom, oplach a možnosť úplného vysušenia.",
    "related": [
        ("Ako čítať štítok na oblečení", ARTICLE_LABEL),
        ("Ako sušiť bielizeň v malom byte", ARTICLE_DRYING),
        ("Prečo oblečenie po praní zapácha", ARTICLE_ODOR),
        ("Prečo sa oblečenie po praní zráža", ARTICLE_SHRINKAGE),
        ("Čo je polyester a ako ho prať", ARTICLE_POLYESTER),
        ("Ako odstrániť rôzne škvrny z oblečenia", ARTICLE_STAIN),
    ],
    "faq_title": "vatelín a objemové výplne",
    "faq": [
        ("Čo je vatelín?", "Je to plošná objemová vrstva vlákien používaná ako výplň, zmäkčenie alebo tepelná medzivrstva."),
        ("Je vatelín vždy polyesterový?", "Nie. Môže byť polyesterový, bavlnený, vlnený alebo zmesový a líšiť sa spôsobom spevnenia."),
        ("Aký je rozdiel medzi vatelínom a molitanom?", "Vatelín tvorí sieť vlákien, kým molitan je súvislá bunková pena. Ich absorpcia, sušenie a starnutie sa líšia."),
        ("Je vatelín to isté ako vlizelín?", "Nie. Vatelín vytvára najmä objem, vlizelín zvyčajne stabilizuje tvar a často môže mať lepiacu vrstvu."),
        ("Môže ísť vatelín do práčky?", "Iba ako súčasť výrobku, ktorého etiketa pranie povoľuje a ktorý má primeranú veľkosť, kotvenie a spôsob sušenia."),
        ("Prečo sa vatelín po praní zhrčil?", "Mohol sa presunúť po prasknutí stehu, zlisovať teplom alebo nerovnomerne vyschnúť. Posudzujte až po úplnom vysušení."),
        ("Ako napraviť hrče vo výplni?", "Suchú mäkkú výplň možno niekedy jemne rozložiť rukami a opraviť kotvenie. Tvrdé alebo lepkavé hrče vyžadujú posúdenie či výmenu."),
        ("Môže ísť výrobok s vatelínom do sušičky?", "Len pri výslovnom symbole a podľa pokynov výrobcu. Teplo môže meniť poťah aj spojenie vlákien."),
        ("Ako dlho schne vatelín?", "Závisí od hrúbky, veľkosti, zloženia, odstreďovania a prúdenia vzduchu. Rozhodujúca je suchosť jadra, nie počet hodín."),
        ("Ako skladovať paplón s vatelínom?", "Čistý, úplne suchý a bez dlhodobého ostrého stlačenia. Spôsob vákuovania si overte u výrobcu."),
        ("Dá sa starý vatelín vymeniť?", "Pri opraviteľnom výrobku áno po otvorení konštrukčného šva. Nová vrstva musí zodpovedať funkcii, hrúbke a budúcej údržbe."),
    ],
}


SPACER_MESH: dict[str, object] = {
    "title": "Čo je 3D sieťovina: dištančný úplet, prúdenie vzduchu a pranie",
    "link": "co-je-3d-sietovina-distancny-uplet-prudenie-vzduchu-a-pranie",
    "meta": "Čo je 3D sieťovina alebo spacer fabric, ako fungujú dve plochy a spojovacie nite a ako čistiť, prať, sušiť a chrániť jej objem.",
    "short": "3D sieťovina je priestorový úplet s dvoma povrchmi spojenými niťami, ktoré udržiavajú odstup. Priedušnosť, pružnosť a prateľnosť nie sú automatické: mení ich hustota, hrúbka, laminácia a konštrukcia celého batohu, topánky, chrániča či podložky.",
    "name": "3D sieťovina",
    "locative": "3D sieťovine",
    "identity_heading": "3D sieťovina je priestorová textilná konštrukcia, nie každá dierkovaná vrstva",
    "identity_detail": "Dva pletené povrchy sú oddelené a prepojené sústavou spacerových nití alebo monofilamentov, ktoré vytvárajú hrúbku, priechodné kanály a odpor pri stlačení.",
    "identity_boundary": "Obchodný názov sa používa aj pri penou laminovaných sieťkach a jednoduchých dierkovaných úpletoch, preto treba overiť rez, rub, vrstvy a technický list.",
    "label_focus": "polyester alebo polyamid, elastan, monofilament, penovú či membránovú lamináciu, lepidlo, výstuž, vyberateľnosť a povolené odstreďovanie",
    "missing_label": "Pri batohu, topánke, sedáku alebo chrániči bez návodu nemožno z viditeľnej sieťky usúdiť, že celý predmet smie do práčky.",
    "dry_check": "piesok v otvoroch, zalomené spojovacie nite, sploštené zóny, odlepené okraje, prasknutú penu, vytiahnuté očká, poškodené švy a pach v hrúbke",
    "damage_boundary": "Prach a soľ možno odstrániť, no trvalo zlomený spacerový monofilament, rozpadnutá pena alebo delaminácia sa ďalším mokrým čistením neobnoví.",
    "test_focus": "Skúšajte aj návrat hrúbky po krátkom rovnomernom stlačení a sledujte, či sa vrstvy neposúvajú alebo neoddeľujú po úplnom vysušení.",
    "combined_risk": "zachyteného piesku, ohybu spojovacích nití, mokrého bodového tlaku a pomalého odparovania z miest prekrytých lemom alebo penou",
    "chemistry_boundary": "Silný odmasťovač alebo rozpúšťadlo môže byť znesiteľné pre jedno polyesterové vlákno, ale poškodiť lepidlo, penu, farbivo či povrchovú úpravu celej zostavy.",
    "drying_detail": "Batoh otvorte, vyberateľnú vložku oddeľte podľa návodu a topánku alebo chránič postavte tak, aby vzduch prúdil cez obe plochy a hrany bez deformovania.",
    "heat_boundary": "Horúci fén, radiátor alebo sušička bez povolenia môže skrútiť monofilamenty, zraziť inú vrstvu, zmeniť penu alebo uvoľniť lamináciu.",
    "stop_signs": "rastúca delaminácia, lámavé spojovacie nite, prenos farby, lepkavá pena, tvrdý chemický pach, rozpad okraja alebo pretrvávajúca vlhkosť v hrúbke",
    "professional_boundary": "Odnímateľnú prateľnú vložku možno často ošetriť doma, zatiaľ čo obuv, ortopedická pomôcka, ochranný prvok, sedadlo alebo lepený batoh sa riadi výrobcom celého produktu.",
    "answer": "3D sieťovina, nazývaná aj spacer fabric alebo dištančný úplet, má dva povrchy spojené niťami, ktoré medzi nimi držia priestor. Čistite ju najprv nasucho od piesku a vlasov, pri povolenom praní obmedzte tvrdé trenie a vysoké otáčky a vysušte ju cez celú hrúbku. Celý výrobok neperte len podľa vzhľadu sieťky.",
    "intro": "Priedušný vzhľad ľahko zvádza k predstave, že voda aj vzduch prejdú všade rovnako. V skutočnom výrobku môže 3D úplet ležať na pene, membráne, plastovej výstuži alebo lepidle a jeho okraje môžu byť uzavreté lemom. Vlhkosť potom odchádza inak než zo samostatnej vzorky a agresívne sušenie môže poškodiť práve skrytú vrstvu.",
    "quick": [
        "Skutočný dištančný úplet má dva textilné povrchy a spojovaciu vrstvu; obyčajná sieťka má iba jednu dierkovanú plochu.",
        "Vyššia hrúbka ani väčšie otvory automaticky neznamenajú vyššiu priedušnosť celého výrobku. Rozhodujú všetky vrstvy a smer prúdenia.",
        "Piesok a kryštáliky soli odstráňte pred vodou, pretože pri mokrom stláčaní režú a odierajú slučky aj spojovacie nite.",
        "Pri strojovom praní sa riaďte etiketou hotového kusu, chráňte sieťku pred zipsami a suchým zipsom a nepoužívajte neprimerané otáčky.",
        "Vysušte obe plochy, hrany a lemy. Suchý líc ešte neznamená suchú spojovaciu vrstvu alebo penu pod ním.",
        "Trvalo sploštené, lámavé alebo odlepené miesto nepokúšajte obnoviť teplom; pri ochrannej pomôcke môže zmena objemu znamenať stratu funkcie.",
    ],
    "overview_heading": "Ako fungujú dve plochy a spojovacie nite v dištančnom úplete",
    "overview": [
        "Priestorový úplet sa vyrába tak, aby predná a zadná plocha zostali oddelené a medzi nimi pracovali spojovacie priadze. Tie sa pri stlačení ohnú alebo vybočia a po odľahčení sa snažia obnoviť geometriu. Výsledkom môže byť pružná hrúbka, rozloženie tlaku a otvorená cesta pre vzduch, no konkrétne vlastnosti závisia od priadze, hustoty, uhla a vzoru spojenia.",
        "Výskum spacerových textílií ukazuje, že zmenou povrchovej štruktúry a hustoty spojovacích nití možno výrazne meniť priedušnosť aj kompresné správanie. Preto sa dva podobne vyzerajúce materiály môžu pri sedení, chôdzi alebo praní správať rozdielne. Marketingové slovo 3D mesh nie je technická špecifikácia a nehovorí nič presné o zotavení po dlhodobom tlaku.",
        "V praxi sa materiál používa na chrbtoch batohov, popruhoch, v obuvi, chráničoch, podložkách, matracových poťahoch, kancelárskych sedadlách a funkčnom odeve. Často je zošitý alebo laminovaný s ďalšou vrstvou. Domáca starostlivosť preto začína identifikáciou celého sendviča a otázkou, či je sieťovina odnímateľná.",
    ],
    "table1_heading": "3D sieťovina, obyčajný mesh, pena a laminát: ako ich rozlíšiť",
    "table1_intro": "Pozrite sa na rez pri leme alebo odnímateľnú vzorku. Ak konštrukciu nevidíte bez poškodenia, riaďte sa návodom výrobcu a predmet nerozoberajte iba kvôli domácemu testu.",
    "table1_headers": ["Konštrukcia", "Ako vyzerá v reze", "Čo vytvára objem", "Hlavná hranica údržby"],
    "table1_rows": [
        ("3D spacer úplet", "Dve pletené plochy spojené priadzami.", "Ohnuté alebo šikmé spojovacie nite.", "Chrániť spojovaciu vrstvu, hrany a návrat hrúbky."),
        ("Jednovrstvová sieťka", "Jedna otvorená plocha bez samostatného jadra.", "Objem je malý a vychádza z priadze či väzby.", "Riziko zachytenia očiek a deformácie otvorov."),
        ("Sieťka laminovaná na penu", "Textilná plocha prilepená k bunkovej vrstve.", "Pena, nie spojovacie textilné nite.", "Lepidlo, degradácia peny a zadržaná voda."),
        ("Sendvič s membránou", "Viac vrstiev, niektoré sú súvislé a veľmi tenké.", "Kombinácia textilu, membrány a prípadnej výstuže.", "Priedušnosť aj pranie určuje celý laminát."),
        ("Plastová mriežka", "Tvarované alebo vytláčané rebrá bez pletených očiek.", "Tuhosť polymérnej geometrie.", "Čistič a teplo posudzovať ako pri plaste, nie pri textilnom úplete."),
    ],
    "sections": [
        {
            "heading": "Priedušnosť nie je iba počet dierok, ktoré vidíte",
            "paragraphs": [
                "Vzduch musí prejsť povrchom, spojovacou vrstvou, druhým povrchom a všetkými susednými vrstvami. Hustý poťah, pena, membrána alebo telo človeka môže cestu výrazne obmedziť. Aj smer prúdenia a stlačenie menia otvorenú plochu. Preto sa priedušnosť samostatnej vzorky nedá automaticky preniesť na batoh pritlačený k chrbtu.",
                "ISO 9237 a ASTM D737 merajú prietok vzduchu cez definovanú plochu pri určenom tlakovom rozdiele. Výsledok má zmysel iba spolu s podmienkami a orientáciou vzorky. Domáci test prefúknutím ústami môže odhaliť úplne uzavretú vrstvu, ale neposkytuje porovnateľné číslo a nepredpovedá odvod vodnej pary pri používaní.",
            ],
        },
        {
            "heading": "Kompresia, pružné zotavenie a trvalo sploštená zóna",
            "paragraphs": [
                "Pri prvom stlačení sa spojovacie nite ohýbajú a materiál môže pôsobiť pružne. Pri vyššom tlaku sa štruktúra postupne zhusťuje a odpor rastie. Opakované alebo dlhodobé zaťaženie môže vytvoriť trvalú deformáciu, najmä ak sa monofilamenty zalomia, teplo zmení polymér alebo sa oddelí povrchová vrstva.",
                "Na suchom výrobku označte porovnávané miesta a zaťažte ich iba rovnakým miernym spôsobom bez poškodzovania. Sledujte návrat po odľahčení a porovnajte ľavú a pravú stranu. Tento domáci test odhalí asymetriu, nie bezpečnostnú spôsobilosť. Pri prilbe, ortéze, chrániči alebo zdravotníckej pomôcke rozhoduje výrobca a určená kontrola.",
            ],
        },
        {
            "heading": "Prach, piesok, vlasy a zvieracie chlpy odstráňte pred navlhčením",
            "paragraphs": [
                "Otvorená štruktúra zachytí častice medzi slučkami a v spojovacej vrstve. Pred čistením predmet obráťte, jemne poklepte a použite mäkkú kefu alebo nízky regulovaný výkon vysávača s ochranou. Vlasy nevytrhávajte ostrým háčikom, ktorý zachytí očko. Lepiaci valček používajte iba na pevnej ploche a bez násilného oddeľovania vrstiev.",
                "Piesok odstráňte zvlášť dôsledne. Po navlhčení sa pri stlačení správa ako abrazívum a môže narezať jemný filament. Kryštáliky soli z potu alebo zimného posypu najprv jemne uvoľnite podľa pokynov výrobcu. Tvrdé kefovanie do hĺbky zatlačí častice bližšie k rubu a nepomôže budúcemu oplachu.",
            ],
        },
        {
            "heading": "Ako vyčistiť 3D sieťovinu na batohu a popruhoch",
            "paragraphs": [
                "Batoh vyprázdnite, povysávajte švy a overte odnímateľné výstuže, elektronické prvky, kožené detaily a náter vnútornej látky. Celý batoh neponárajte len preto, že chrbát je zo sieťoviny. Ak návod povoľuje lokálne čistenie, pracujte po malých plochách mäkkou handričkou a odsávajte roztok bez krútenia popruhu.",
                "Pot sa sústreďuje pri ramenách a krížoch, kde je materiál zároveň najviac stlačený. Primeraný oplach musí odstrániť aj zvyšok produktu, inak sa pach po zahriatí môže vrátiť. Batoh sušte otvorený, v prirodzenom tvare a bez priameho zdroja tepla. Vystužený chrbát pravidelne otáčajte, aby nezostala vlhká hrana pri leme.",
            ],
        },
        {
            "heading": "Ako prať odnímateľnú vložku, poťah alebo športový odev",
            "paragraphs": [
                f"Odnímateľnosť neznamená prateľnosť. Vyhľadajte vlastný štítok vložky a overte zloženie, lamináciu a symboly podľa článku <a href=\"{ARTICLE_LABEL}\">ako čítať ošetrovací štítok</a>. Pri povolenom cykle chráňte otvorenú štruktúru pred zipsami, háčikmi a suchým zipsom a použite len takú ochranu, ktorá dovolí vode preniknúť cez obe plochy.",
                "Nepreplňujte bubon a nevoľte vysoké otáčky nad rámec etikety. Stlačená vložka sa horšie oplachuje a pri prudkom odstreďovaní sa môže zalomiť. Po skončení ju vyberte bez skrúcania, upravte hrany a sušte v tvare. Ak sa po vysušení vrstvy oddeľujú alebo miesto ostáva sploštené, ďalší cyklus problém nevyrieši.",
            ],
        },
        {
            "heading": "3D sieťovina v topánke potrebuje hranicu medzi textilom a celou obuvou",
            "paragraphs": [
                "Zvršok môže kombinovať mesh, termoplastické výstuhy, lepidlá, podšívku, stielku a podrážku. Prostriedok vhodný na polyesterovú sieťku môže zmeniť farbu výstuže alebo pevnosť lepeného spoja. Pred čistením vyberte šnúrky a stielku iba vtedy, ak sú určené na vyberanie, a riaďte sa návodom značky obuvi.",
                "Blato nechajte primerane zaschnúť a voľnú časť odstráňte bez tlačenia do otvorov. Pri lokálnom čistení používajte mäkkú kefu a nízky tlak. Topánku nesušte na radiátore ani horúcim fénom. Vzduch musí dosiahnuť špičku, jazyk aj pätu, ale výplň nevypchávajte materiálom, ktorý prenesie farbu alebo zablokuje prúdenie.",
            ],
        },
        {
            "heading": "Pot, mastnota a pach v otvorenej štruktúre",
            "paragraphs": [
                f"Pach nevzniká preto, že materiál má diery, ale preto, že v ňom zostáva pot, kožný maz, mikrobiálne zvyšky alebo nedosušená vlhkosť. Všeobecné príčiny rozoberá článok <a href=\"{ARTICLE_ODOR}\">prečo oblečenie zapácha po praní</a>. Pri hrubej sieťovine treba vyriešiť zdroj v celom priereze, nie prekryť povrch silnou vôňou.",
                "Mastné znečistenie najprv odsajte a kompatibilný prostriedok skúste na skrytom mieste. Nadbytok gélu sa môže zachytiť v slučkách a zhoršiť oplach. Nemiešajte čističe a nepoužívajte chlór či rozpúšťadlo bez potvrdenia výrobcu. Pri kontakte s pokožkou je dôkladné odstránenie zvyškov dôležitejšie než intenzita parfumácie.",
            ],
        },
        {
            "heading": "Ako vysušiť dištančný úplet cez celú hrúbku",
            "paragraphs": [
                f"Po oplachu odstráňte prebytočnú vodu spôsobom povoleným výrobcom, nie skrúcaním alebo státím na vložke. Materiál postavte či položte tak, aby vzduch prúdil cez obe plochy a okraje. V malom priestore pomôže všeobecný postup <a href=\"{ARTICLE_DRYING}\">sušenia bielizne bez zatuchnutia</a>, no pri sendviči kontrolujte aj lemy a prekrývajúce vrstvy.",
                "Prirodzené prúdenie možno podporiť ventilátorom z primeranej vzdialenosti, ak výrobok zostáva stabilný a nejde o horúci vzduch. Pred použitím stlačte rôzne zóny čistou suchou handričkou a sledujte chladné miesta či návrat pachu. Výrobok nezaťažujte, kým sa spojovacia vrstva po mokrom stave neustáli.",
            ],
        },
        {
            "heading": "Zachytené očko, prerezaná niť a delaminácia vyžadujú rozdielnu opravu",
            "paragraphs": [
                "Vytiahnuté očko neodstrihujte bez znalosti konštrukcie, pretože môže spustiť ďalšie páranie. Voľnú slučku dočasne chráňte pred zachytením a nechajte ju zatiahnuť alebo stabilizovať odborníkom z rubu. Prerezaná spojovacia niť znižuje lokálnu hrúbku a nedá sa nahradiť kvapkou univerzálneho lepidla.",
                "Pri delaminácii sa plocha oddelí od peny, membrány alebo podkladu a môže vytvárať bublinu. Teplo z domácej žehličky nie je kontrolovaný fixačný proces a môže poškodiť okolitý polymér. Pri novom výrobku stav zdokumentujte pred ďalším čistením a riešte s predajcom. Pri bezpečnostnom vybavení výrobok prestaňte používať.",
            ],
        },
        {
            "heading": "Ako vybrať 3D sieťovinu podľa použitia a pýtať sa na merania",
            "paragraphs": [
                "Pri batohu sledujte komfort pod zaťažením, odvod vlhkosti, švy a čistiteľnosť. Pri sedadle potrebujete zotavenie po dlhom tlaku a stabilitu hrán. Pri obuvi sa pridáva ohyb a oder. Pýtajte sa na hrúbku pri definovanom tlaku, priedušnosť s metódou, plošnú hmotnosť a správanie po opakovanom stlačení.",
                "Samostatná vzorka môže pôsobiť vzdušne, kým finálny výrobok ju prekryje nepriepustnou vrstvou. Skontrolujte preto celý rez a spôsob upevnenia. Zrozumiteľný návod na údržbu, dostupná náhradná vložka a opraviteľný šev sú pre dlhodobé používanie rovnako dôležité ako vysoká počiatočná hodnota prietoku vzduchu.",
            ],
        },
    ],
    "table2_heading": "3D sieťovina po čistení alebo tlaku: čo sledovať",
    "table2_intro": "Materiál nechajte úplne vyschnúť a odpočinúť bez zaťaženia. Až potom porovnajte hrúbku, tvar a súdržnosť s nepoškodenou zónou.",
    "table2_headers": ["Prejav", "Možná príčina", "Čo overiť", "Bezpečný ďalší krok"],
    "table2_rows": [
        ("Jedna zóna sa nevráti do výšky", "Zalomené spojovacie nite, dlhý tlak alebo teplo.", "Symetriu, zvuk pri ohybe a stav oboch povrchov.", "Nezohrievať; pri funkčnom prvku posúdiť výmenu."),
        ("Povrch tvorí bublinu", "Delaminácia od peny, membrány alebo výstuže.", "Pohyb vrstiev pri okraji a lepkavosť.", "Nelepiť z líca; zdokumentovať a konzultovať opravu."),
        ("Otvory sú deformované", "Zachytené očko, silné kefovanie alebo nevhodné odstreďovanie.", "Pretrhnutie priadze a šírenie deformácie.", "Chrániť pred záťažou a opraviť z rubu."),
        ("Pach sa vracia po stlačení", "Vlhkosť alebo zvyšok znečistenia ostal v hrúbke.", "Lemy, penu, chladné miesta a oplach.", "Pokračovať v bezpečnom sušení alebo zvoliť odborné čistenie."),
        ("Tvrdý piesok ostáva vo vnútri", "Častice sa zatlačili medzi spojovacie nite.", "Či ich možno uvoľniť bez poškodenia otvorov.", "Najprv suché odsatie; nebrúsiť mokrou kefou."),
    ],
    "steps_heading": "Ako vyčistiť 3D sieťovinu krok za krokom",
    "steps": [
        "Určite, či ide o skutočný dištančný úplet, jednoduchú sieťku alebo laminát s penou či membránou.",
        "Prečítajte návod celého výrobku a zistite zloženie, lepidlá, výstuže, odnímateľné časti a povolený cyklus.",
        "Nasucho odstráňte piesok, vlasy a prach z otvorov, švov a hrán bez zachytávania očiek.",
        "Škvrnu odsajte a kompatibilný roztok otestujte na skrytom mieste vrátane reakcie po úplnom vysušení.",
        "Ak je pranie povolené, chráňte povrch pred zipsami a háčikmi a použite primeranú náplň, dávku a otáčky.",
        "Mokrý materiál nekrúťte ani prudko nestláčajte; upravte hrany a oba povrchy do prirodzeného tvaru.",
        "Sušte s prúdením cez obe strany a kontrolujte lemy, penu a miesta, ktoré zostávajú chladné.",
        "Po odpočinku porovnajte hrúbku, návrat po tlaku a spojenie vrstiev; poškodený funkčný prvok ďalej nezaťažujte.",
    ],
    "remember": [
        "Má materiál dve textilné plochy spojené niťami, alebo ide o sieťku nalepenú na penu?",
        "Ktorá skrytá vrstva, výstuž alebo lepidlo určuje najnižší limit čistenia?",
        "Boli piesok, soľ, vlasy a prach odstránené ešte pred vodou a tlakom?",
        "Povoľuje etiketa ponorenie, práčku, odstreďovanie, sušičku a konkrétnu chémiu?",
        "Môže vzduch pri sušení prechádzať oboma plochami, hranami a lemami?",
        "Je sieťovina súčasťou ochrannej alebo zdravotníckej pomôcky, ktorej funkciu nemožno overiť doma?",
    ],
    "mistakes": [
        "Považovať každú dierkovanú látku za rovnakú 3D sieťovinu.",
        "Vyprať celý batoh alebo topánku podľa odolnosti jedného viditeľného polyesterového povrchu.",
        "Navlhčiť piesok v otvoroch a potom ho vtierať tvrdou kefou do spojovacej vrstvy.",
        "Použiť silný odmasťovač bez kontroly peny, membrány, lepidla a farbiva.",
        "Sušiť monofilamenty horúcim fénom alebo na radiátore, aby sa hrúbka rýchlejšie vrátila.",
        "Ďalej používať sploštený ochranný diel iba preto, že povrch nie je roztrhnutý.",
    ],
    "expert_heading": "Odbornejší pohľad: konštrukcia spacer fabric, prietok vzduchu a kompresná krivka",
    "expert": [
        "Odborný prehľad 3D osnovných úpletov opisuje spacer fabric ako dve plochy spojené priadzami a ukazuje, že strojová konštrukcia môže cielene meniť tlakovú stabilitu aj priedušnosť. Materiál preto nie je iba hrubšia sieťka. Uhol, hustota a typ spojovacích nití vytvárajú rozdielnu odpoveď pri stlačení aj bočnom ohybe.",
        "Štúdia spacerových vložiek rozdeľuje kompresiu na počiatočnú elastickú oblasť, rozsiahlejšie vybočenie spojovacích prvkov a konečné zhutnenie. Zistila tiež významný vplyv hustoty konštrukcie na priedušnosť. Konkrétne laboratórne výsledky však platia pre skúšané vzorky a nemožno ich preniesť na neoznačený batoh či sedadlo.",
        "ISO 9237 a ASTM D737 definujú meranie priepustnosti vzduchu, zatiaľ čo ASTM D1777 rieši hrúbku pri tlaku. Tieto metódy oddeľujú vlastnosti, ktoré sa v reklame často spájajú jedným slovom priedušný. Pri nákupe má zmysel žiadať metódu, tlakový rozdiel, orientáciu a stav celej zostavy, nie iba maximálne číslo zo samostatnej textílie.",
    ],
    "source_intro": "Zdroje vysvetľujú priestorovú pletenú stavbu, nastaviteľnú priedušnosť, kompresné správanie a normované meranie hrúbky a prietoku vzduchu. Nepotvrdzujú prateľnosť celého výrobku podľa samotného názvu 3D mesh.",
    "sources": [
        ("Odborný prehľad: 3D textílie z osnovných úpletov", SPACER_REVIEW),
        ("Výskum spacerovej konštrukcie pre vložky do obuvi", SPACER_STUDY),
        ("ISO 9237: priepustnosť textílií pre vzduch", ISO_AIR),
        ("ASTM D737: priepustnosť textílií pre vzduch", ASTM_AIR),
        ("ASTM D1777-26: hrúbka textilných materiálov", ASTM_THICKNESS),
        ("AATCC TM135: rozmerové zmeny po domácom praní", AATCC_DIMENSION),
        ("EÚ 1007/2011: označovanie textilných vlákien", EU_FIBRE_LABEL),
        ("GINETEX: význam symbolov ošetrovania", GINETEX),
    ],
    "product_heading": "Prací gél použite iba na samostatne prateľnú textilnú časť",
    "product_intro": "Pri odnímateľnej vložke, poťahu alebo odeve s výslovne povoleným domácim praním možno zvoliť gél podľa vlákna a farby. Celý batoh, topánku či chránič neposudzujte podľa sieťky.",
    "product_name": LAUNDRY_PRODUCT_NAME,
    "product_url": LAUNDRY_PRODUCT_URL,
    "product_text": "Tekutý gél umožňuje presné dávkovanie pri povolenom cykle. Otvorená hrubšia štruktúra potrebuje dostatok vody, priestoru a oplachu, aby produkt nezostal medzi plochami.",
    "product_limit": "Produkt nie je určený na penu, lepidlo, membránu, plastovú výstuž ani neodnímateľný funkčný výrobok bez povolenia výrobcu. Neopraví delamináciu alebo zlomené spojovacie nite.",
    "category_heading": "Pracie gély patria k prateľným textíliám, nie automaticky ku každému meshu",
    "category_intro": "Pred výberom produktu určte celý materiálový sendvič a symboly. Pri športovej alebo kontaktnej textílii je primeraná dávka a dôkladný oplach dôležitejší než silná vôňa.",
    "category_name": LAUNDRY_CATEGORY_NAME,
    "category_url": LAUNDRY_CATEGORY_URL,
    "category_text": "V kategórii môžete porovnať gély pre bežnú prateľnú bielizeň. Pri 3D sieťovine použite iba produkt kompatibilný s presným vláknom, farbou a všetkými spojenými vrstvami.",
    "related": [
        ("Ako čítať štítok na oblečení", ARTICLE_LABEL),
        ("Čo je polyester a ako ho prať", ARTICLE_POLYESTER),
        ("Prečo oblečenie po praní zapácha", ARTICLE_ODOR),
        ("Ako sušiť bielizeň v malom byte", ARTICLE_DRYING),
        ("Ako odstrániť rôzne škvrny z oblečenia", ARTICLE_STAIN),
        ("Prečo sa oblečenie žmolkuje", ARTICLE_PILLING),
    ],
    "faq_title": "3D sieťovina a dištančný úplet",
    "faq": [
        ("Čo je 3D sieťovina?", "Je to priestorový úplet s dvoma povrchmi spojenými priadzami, ktoré medzi nimi vytvárajú a podopierajú odstup."),
        ("Je 3D mesh vždy polyester?", "Nie vždy, hoci polyester je častý. Môže obsahovať polyamid, elastan, monofilamenty a ďalšie laminované materiály."),
        ("Aký je rozdiel medzi spacer fabric a obyčajnou sieťkou?", "Spacer fabric má dve plochy a spojovaciu vrstvu, obyčajná sieťka je spravidla jedna otvorená plocha."),
        ("Je 3D sieťovina priedušná?", "Môže byť, ale výsledok mení hustota, stlačenie a všetky vrstvy za ňou. Vzhľad otvorov sám nestačí."),
        ("Môže ísť 3D sieťovina do práčky?", "Iba ak to povoľuje etiketa konkrétnej odnímateľnej časti alebo celého výrobku."),
        ("Ako vyčistiť 3D sieťovinu na batohu?", "Najprv nasucho odstráňte častice a potom použite lokálny postup výrobcu bez premočenia výstuží a lepidiel."),
        ("Ako vyčistiť 3D mesh na topánkach?", "Odstráňte suché blato, použite mäkkú kefu a kompatibilný roztok podľa značky obuvi a nesušte pri zdroji tepla."),
        ("Prečo sa sieťovina po praní sploštila?", "Mohli sa zalomiť spojovacie nite, zmeniť teplom alebo zostať mokré. Hodnoťte po úplnom vysušení bez zaťaženia."),
        ("Ako dlho schne 3D sieťovina?", "Podľa hrúbky, lemov, peny a prúdenia vzduchu. Suchý povrch nie je dôkazom suchého jadra."),
        ("Dá sa odlepená 3D sieťovina znovu prilepiť?", "Niekedy odborným postupom podľa laminátu. Univerzálne lepidlo z líca môže stvrdnúť, zablokovať otvory a rozšíriť poruchu."),
        ("Ako skladovať výrobok s 3D mesh?", "Čistý, suchý a bez dlhodobého bodového stlačenia alebo ostrého zlomu spojovacej vrstvy."),
    ],
}


BATISTE: dict[str, object] = {
    "title": "Čo je batist: jemná plátnová tkanina, priesvitnosť a pranie",
    "link": "co-je-batist-jemna-platnova-tkanina-priesvitnost-a-pranie",
    "meta": "Čo je batist, ako sa líši od voálu, lawnu, mušelínu a organzy a ako ho prať, odstraňovať škvrny, sušiť, žehliť a chrániť švy.",
    "short": "Batist je jemná, ľahká a často polopriesvitná tkanina v plátnovej väzbe. Môže byť bavlnený, ľanový, viskózový, hodvábny aj syntetický, preto názov neurčuje teplotu; rozhoduje presné zloženie, hustota, farbenie, výšivka a konštrukcia hotového výrobku.",
    "name": "batist",
    "locative": "batiste",
    "identity_heading": "Batist opisuje jemný typ tkaniny, nie výlučne bavlnu",
    "identity_detail": "Typický batist používa jemné priadze v jednoduchej plátnovej väzbe a pôsobí ľahko, hladko a mierne priesvitne, no obchodný názov sa používa pri rôznych vláknach a úpravách.",
    "identity_boundary": "Bavlnený detský batist, ľanová vreckovka, polyesterová záclonovina a hodvábna historická textília môžu mať podobnú jemnosť, ale rozdielnu pevnosť za mokra, farbu a teplotný limit.",
    "label_focus": "bavlnu, ľan, hodváb, viskózu alebo syntetiku, podšívku, čipku, výšivku, gumičku, potlač, bielenie a povolené žehlenie",
    "missing_label": "Pri metráži si ponechajte údaj o zložení a skúšobnú vzorku; jemný neoznačený odev bez istoty neperte podľa domnienky, že každý batist je bavlna.",
    "dry_check": "presvitanie oslabených miest, posunuté nite pri šve, drobné dierky, zachytené vlákna, žlté mapy, zvyšky škrobu, rozdielny lesk a uvoľnenú výšivku",
    "damage_boundary": "Škvrnu alebo zvyšok úpravy možno odstrániť, no pretrhnutá jemná priadza, rozostúpený šev a svetlý lom po mechanickom poškodení nie sú nečistoty.",
    "test_focus": "Skúšobné miesto položte na biely aj tmavý podklad, aby ste odlíšili prenos farby, zmenu priesvitnosti a poškodenie hustoty po úplnom vysušení.",
    "combined_risk": "napučania jemnej priadze, posunu nití pri šve, zachytenia v bubne a tlaku, ktorý vytvorí ostrý lom alebo lesk",
    "chemistry_boundary": "Chlórové bielidlo, enzýmový odstraňovač či rozpúšťadlo nemožno zvoliť iba podľa bieleho vzhľadu, pretože nepoznáte vlákno, výšivku ani optické zjasnenie.",
    "drying_detail": "Blúzku, záves alebo detský textil podoprite bez ťahania za mokrú čipku a jemnú metráž rozložte tak, aby kolíky nevytvorili trvalú stopu v oslabenej zóne.",
    "heat_boundary": "Vysoká teplota môže zraziť celulózové vlákno, poškodiť hodváb alebo syntetiku, ustáliť škvrnu a vytvoriť lesklé či zažltnuté miesto.",
    "stop_signs": "púšťanie farby, zväčšovanie dierky, posun nití, praskanie výšivky, lepkavá úprava, deformácia podšívky alebo krehký žltý sklad",
    "professional_boundary": "Bežný moderný prateľný bavlnený batist možno často ošetriť doma, zatiaľ čo hodvábny, historický, maľovaný, silno zdobený alebo krehký kus potrebuje odborný postup.",
    "answer": "Batist je veľmi jemná ľahká tkanina, zvyčajne v plátnovej väzbe. Nie je automaticky zo stopercentnej bavlny. Perte ho podľa štítku, oddeľte od zipsov a drsných kusov, škvrny nedrhnite, používajte miernu mechaniku a pri sušení i žehlení chráňte jemné nite, výšivku a švy.",
    "intro": "Pri batiste býva problém opačný než pri robustne pôsobiacich materiáloch: jeho ľahkosť sa zamieňa za jednotnú citlivosť. Husto utkaný kvalitný bavlnený batist môže byť pri správnom šve stabilný, zatiaľ čo voľnejší viskózový alebo hodvábny kus reaguje na mokrú manipuláciu podstatne citlivejšie. Priesvitnosť sama nepovie zloženie ani pevnosť.",
    "quick": [
        "Batist je typ jemnej tkaniny; vláknom môže byť bavlna, ľan, viskóza, hodváb, polyester alebo zmes.",
        "Plátnová väzba má veľa väzbových bodov, no výslednú pevnosť stále mení jemnosť priadze, hustota, dokončenie a šev.",
        "Voál, lawn, mušelín a organza sa môžu prekrývať vzhľadom, ale líšia sa omakom, priadzou, hustotou a obchodnou tradíciou.",
        "Pred praním zatvorte háčiky, odstráňte predmety so suchým zipsom a jemný kus chráňte bez toho, aby ochranné vrecko blokovalo oplach.",
        "Škvrnu odsávajte, netrite. Na priesvitnej tkanine je mechanicky zmenená plocha viditeľná z oboch strán a pri rôznom podklade.",
        "Žehlite iba podľa symbolu, z rubu, s nízkym tlakom a čistou ochrannou tkaninou; výšivka a syntetická niť môžu mať nižší limit než hlavná plocha.",
    ],
    "overview_heading": "Prečo je batist ľahký, hladký a priesvitný bez toho, aby bol jedným materiálom",
    "overview": [
        "Plátnová väzba strieda osnovnú niť nad a pod jednotlivými útkovými niťami, takže vytvára veľa bodov previazania. Pri veľmi jemných priadzach a nízkej plošnej hmotnosti vznikne ľahká tkanina, cez ktorú čiastočne prechádza svetlo. Hladkosť závisí od priadze, česania, mercerizácie či iného dokončenia, nie iba od väzby.",
        "Batist sa používa na letné blúzky, košele, detské oblečenie, vreckovky, jemnú bielizeň, záclonovinu, podšívkové a vyšívacie projekty. Hotový výrobok môže mať nariasenie, čipku, gumičku, potlač alebo viac vrstiev. Najcitlivejšia z nich určuje pranie, pretože jemná hlavná plocha môže prežiť cyklus, ktorý deformuje výšivku alebo elastický lem.",
        "Svetlo je pri kontrole užitočné: odhalí nerovnomernú hustotu, drobné dierky a posun nití. Zároveň klame pri farbe. Tkanina položená na koži, bielom stole a tmavom podklade vyzerá odlišne bez chemickej zmeny. Stav preto dokumentujte na rovnakom neutrálnom podklade a pri rovnakom rozptýlenom svetle.",
    ],
    "table1_heading": "Batist, voál, lawn, mušelín a organza: praktické rozdiely",
    "table1_intro": "Hranice názvov nie sú vo všetkých obchodoch rovnaké. Nasledujúce znaky pomáhajú pri orientácii, ale etiketu a technický list nenahrádzajú.",
    "table1_headers": ["Názov", "Typický povrch a omak", "Priesvitnosť a tvar", "Čo overiť pred praním"],
    "table1_rows": [
        ("Batist", "Jemný, ľahký, hladký až mäkký v plátnovej väzbe.", "Často polopriesvitný a splývavý.", "Presné vlákno, hustotu, výšivku a švy."),
        ("Voál", "Jemný, často suchší alebo vzdušnejší omak.", "Priesvitný, používaný na odevy a záclony.", "Zákrut priadze, farbu a jemné lemy."),
        ("Lawn", "Veľmi hladký a svieži povrch z jemných priadzí.", "Ľahký, spravidla o niečo plnší vzhľad.", "Dokončenie, bavlnu alebo zmes a rozmer."),
        ("Mušelín", "Rozsah od veľmi jemnej gázoviny po voľnejšie úžitkové plátno.", "Často vzdušný a mäknúci používaním.", "Konkrétnu hustotu a spracovanie, nie iba názov."),
        ("Organza", "Chrumkavý, tuhší a lesklejší priehľadný povrch.", "Drží objem viac než mäkký batist.", "Hodváb alebo syntetiku a veľmi nízky tepelný limit."),
    ],
    "sections": [
        {
            "heading": "Jemná priadza a hustota rozhodujú spolu, nie proti sebe",
            "paragraphs": [
                "Veľký počet jemných nití môže vytvoriť hladkú uzavretú plochu s nízkou hmotnosťou. Menší počet alebo nerovnomerná priadza zasa zvýši priesvitnosť a riziko posunu. Samotný počet nití bez ich jemnosti, väzby a dokončenia nepovie, či bude batist pevný, mäkký alebo vhodný na konkrétny odev.",
                "Pri kúpe pozorujte pravidelnosť proti svetlu a jemne posuňte tkaninu diagonálne bez deformovania. Ak sa nite pri okraji ľahko rozostupujú, hotový výrobok potrebuje primeranú rezervu šva a strih. Domáci ťahový pokus na hotovej blúzke nerobte; oslabenie môže vzniknúť práve skúšaním.",
            ],
        },
        {
            "heading": "Priesvitnosť nie je chyba a nehovorí automaticky o nízkej kvalite",
            "paragraphs": [
                "Priesvitný efekt môže byť zámerný výsledok jemnej priadze, nízkej hmotnosti a rozostupov medzi niťami. Kvalita sa posudzuje podľa rovnomernosti, vhodnosti na použitie, švov, stálofarebnosti a údržby. Letná vrstva môže byť kvalitná aj vtedy, keď potrebuje podšívku; nepriehľadnosť nie je univerzálny rebríček textílií.",
                "Pri fotografovaní škvrny používajte rovnaký podklad. Tmavý stôl zvýrazní otvorenosť a biely zasa môže zakryť vyblednutie. Ak sa po praní zdanlivo zmenila priesvitnosť, skontrolujte rozmery, rozostup nití, zvyšok prostriedku a rovnomernosť žehlenia. Až kombinácia údajov odlíši zrazenie od mechanického posunu.",
            ],
        },
        {
            "heading": "Ako pripraviť batist na pranie a zabrániť zachyteniu",
            "paragraphs": [
                f"Prečítajte <a href=\"{ARTICLE_LABEL}\">štítok s materiálom a symbolmi</a>, potom skontrolujte háčiky, čipku, gumičky, výšivku a voľné nite. Kus oddeľte od zipsov, suchých zipsov, flitrov a hrubých uterákov. Ochranné vrecko môže znížiť bodové zachytenie, no musí byť správnej veľkosti a dovoliť vode a oplachu prejsť látkou.",
                "Farebný alebo potlačený batist prvýkrát perte oddelene podľa etikety a vykonajte skrytú skúšku lokálneho produktu. Nevkladajte ho do preplneného bubna, kde sa jemná plocha stlačí medzi ťažké kusy. Prázdny prudký cyklus s jednou ľahkou blúzkou tiež nemusí byť šetrný; zvoľte program určený výrobcom a primeranú náplň podobných vecí.",
            ],
        },
        {
            "heading": "Ručné pranie batistu neznamená krútenie a dlhé namáčanie",
            "paragraphs": [
                "Ak etiketa povoľuje ručné pranie, použite stabilnú nádobu, predpísanú teplotu a presne nadávkovaný kompatibilný prostriedok. Textil ponárajte a pohybujte jemne, netrite dve vrstvy proti sebe a nenechávajte ho vo vode dlhšie len preto, že je jemný. Dlhý kontakt môže zvýšiť uvoľňovanie farby alebo oslabiť citlivé vlákno.",
                "Oplachujte bez prudkého prúdu na jedno miesto a vodu vytlačte medzi dlaňami alebo cez čistú savú textíliu podľa konštrukcie. Nezdvíhajte mokrú blúzku za čipku či ramienko. Viskózová alebo hodvábna zložka môže mať za mokra iné mechanické správanie než bavlna, preto sa znova riaďte štítkom, nie názvom batist.",
            ],
        },
        {
            "heading": "Ako odstrániť škvrnu z jemného bieleho alebo farebného batistu",
            "paragraphs": [
                f"Kvapalinu odsajte a pevný zvyšok zdvihnite bez rozmazania. Typy škvŕn a bezpečné poradie krokov približuje článok <a href=\"{ARTICLE_STAIN}\">ako odstrániť žuvačku, krv, vosk a iné škvrny</a>. Produkt naneste podľa návodu a skúšku vykonajte na skrytom mieste vrátane výšivky alebo potlače, ak sa jej môže dotknúť.",
                "Biely vzhľad nie je povolenie na chlór. Batist môže obsahovať vlákno, niť alebo povrchovú úpravu, ktoré sa oslabia či zažltnú. Pri škvrne pracujte od okraja, látku podoprite a nedrhnite, pretože mechanicky zdrsnené miesto bude na svetle viditeľné aj po odstránení nečistoty. Neznáme staré žltnutie môže byť degradácia, nie rozpustná škvrna.",
            ],
        },
        {
            "heading": "Výšivka, čipka, riasenie a detský batist majú vlastné limity",
            "paragraphs": [
                "Vyšívacia niť môže púšťať farbu alebo sa zraziť inak než podklad. Čipka sa zachytáva a riasenie sústreďuje nečistotu aj mechanické napätie. Pri detskom odeve skontrolujte patentky, gumičky a viac vrstiev pri leme. Najnižší limit môže patriť drobnému prvku, ktorý na prednej etikete nie je zdôraznený.",
                "Ozdobený kus otočte naruby iba vtedy, ak tým neohnete tuhú aplikáciu. Voľné prvky stabilizujte pred praním a poškodený steh opravte. Pri farbenom vyšívanom textile nerobte skúšku iba na hladkom bielom podklade; otestujte aj niť. Ak sa farba prenáša, zastavte sa namiesto pridania soli, octu alebo iného domáceho receptu.",
            ],
        },
        {
            "heading": "Sušenie jemnej tkaniny bez vyťahania a stopy po kolíku",
            "paragraphs": [
                f"Mokrý batist podoprite a urovnajte švy bez napínania. Ľahký kus môže schnúť rýchlo, no riasenie, dvojitý lem a výšivka zadržia viac vody. Všeobecné zásady prúdenia vzduchu nájdete v článku <a href=\"{ARTICLE_DRYING}\">ako sušiť bielizeň v malom byte</a>. Priamy prudký vietor môže jemnú mokrú plochu bičovať o hranu sušiaka.",
                "Kolíky umiestnite do pevného skrytého šva alebo použite spôsob zo štítku. Tmavý kus chráňte pred priamym slnkom, ktoré môže zvýrazniť nerovnomerné blednutie. Hodvábny či viskózový batist nevešajte za úzky bod bez potvrdenia. Pred uložením skontrolujte suchosť vrstvených lemov, aby nevznikla žltá alebo zatuchnutá línia.",
            ],
        },
        {
            "heading": "Ako žehliť batist bez lesku, zvlnenia a odtlačku výšivky",
            "paragraphs": [
                f"Žehlite iba pri povolení a podľa najcitlivejšej zložky. Použite čistú plochu, ochrannú tkaninu a nízky tlak; praktické základy rozoberá návod <a href=\"{ARTICLE_IRONING}\">ako vyžehliť košeľu</a>. Výšivku podložte mäkko z rubu, aby sa reliéf nepretlačil ako lesklý obrys na okolité miesto.",
                "Para nie je univerzálne bezpečná. Na hodvábe či nestálofarebnej úprave môže vytvoriť mapu a syntetická niť má nižší teplotný limit než bavlnený podklad. Žehličku najprv skúste na skrytom leme a nechajte miesto vychladnúť. Jemnú tkaninu neťahajte za horúca, pretože môžete ustáliť posun šva alebo zvlnený okraj.",
            ],
        },
        {
            "heading": "Posun nití pri šve, drobná dierka a zatrhnutie nie sú rovnaká porucha",
            "paragraphs": [
                "Pri posune sa neporušené nite odtlačia od stehu a vznikne priesvitnejšia línia. Dierka má prerušenú priadzu a zatrhnutie vytiahnutú slučku alebo niť. Každá chyba reaguje na ťah inak. Ďalším praním sa sama neopraví a koncentrovaný avivážny či škrobiaci produkt ju mechanicky nespevní.",
                "Odev prestaňte napínať, miesto položte na kontrastný podklad a odfoťte. Krajčír môže spevniť šev, vložiť jemnú podložku alebo opravovať z rubu podľa zaťaženia. Uzol a kvapka lepidla na líci vytvoria tvrdý bod, ktorý pri ďalšom pohybe reže okolité jemné nite. Pri historickom kuse patrí oprava konzervátorovi.",
            ],
        },
        {
            "heading": "Ako vybrať batist na blúzku, záclonu alebo detský odev",
            "paragraphs": [
                "Na blúzku porovnajte priesvitnosť, splývavosť, švy a potrebu podšívky. Na záclonu pridajte svetlostálosť a tvar po zavesení. Pri detskom odeve sledujte mäkkosť švov, farbivá, opakovanú údržbu a pevnosť patentiek. Rovnaká metráž nemusí byť vhodná na všetky tri účely iba preto, že je príjemná na dotyk.",
                "Vyžiadajte si zloženie, šírku, plošnú hmotnosť, rozmerovú zmenu a odporúčanú údržbu. Kúpte malú rezervu na skúšku, okraje začistite a vzorku operte spôsobom plánovaným pre hotový výrobok. Po úplnom vysušení porovnajte rozmer, farbu, priesvitnosť, omak a posun nití predtým, než nastriháte všetky diely.",
            ],
        },
    ],
    "table2_heading": "Batist po praní alebo žehlení: diagnostická tabuľka",
    "table2_intro": "Jemnú plochu pozorujte na rovnakom bielom a tmavom podklade. Zmena podkladu môže vytvoriť zdanlivý rozdiel, ktorý nie je poškodením.",
    "table2_headers": ["Prejav", "Možná príčina", "Čo overiť", "Bezpečný ďalší krok"],
    "table2_rows": [
        ("Priesvitná línia pri šve", "Posun nití, napätie strihu alebo malá rezerva.", "Či sú priadze celé a či sa línia zväčšuje pri nosení.", "Prestať zaťažovať a riešiť krajčírsku úpravu."),
        ("Žltý ostrý sklad", "Teplo, oxidovaná nečistota alebo dlhé skladovanie vo vlhku.", "Krehkosť, pach a reakciu skrytého miesta.", "Nechlórovať naslepo; pri krehkosti odborné posúdenie."),
        ("Lesklý zvlnený pás", "Vysoký tlak, teplota alebo napínanie pri žehlení.", "Zloženie, odtlačok podkladu a stav priadzí.", "Neopakovať teplo; nechať ustáliť a posúdiť z rubu."),
        ("Drobné dierky po cykle", "Háčik, zips, slabá priadza alebo poškodený bubon.", "Okraje dierky a ďalšie kusy z rovnakej náplne.", "Zastaviť nosenie v napätí a opraviť skôr, než sa otvor zväčší."),
        ("Sivá alebo tvrdšia mapa", "Zvyšok produktu, mastnota alebo nerovnomerné opláchnutie.", "Prenos na bielu handričku a omak po vysušení.", "Pri povolení šetrne opláchnuť bez drhnutia."),
    ],
    "steps_heading": "Ako vyprať batist krok za krokom",
    "steps": [
        "Určite presné vlákno, farbu, podšívku, výšivku, čipku, gumičky a všetky symboly starostlivosti.",
        "Na svetlom aj tmavom podklade skontrolujte dierky, posun nití, staré mapy a poškodené stehy.",
        "Škvrnu odsajte a kompatibilný produkt otestujte na skrytom mieste vrátane výšivky alebo potlače.",
        "Oddeľte batist od zipsov, háčikov, suchého zipsu a ťažkých textílií; vhodne chráňte jemný povrch.",
        "Použite iba povolený ručný alebo strojový cyklus, presnú dávku, primeranú náplň a miernu mechaniku.",
        "Mokrý kus nekrúťte a nezdvíhajte za čipku; podoprite ho a urovnajte švy bez naťahovania.",
        "Sušte podľa etikety v tieni a s prúdením vzduchu, pričom kolíky nedávajte na oslabenú viditeľnú plochu.",
        "Žehlite iba pri povolení z rubu cez ochrannú tkaninu a po vychladnutí skontrolujte reliéf, farbu a rozmery.",
    ],
    "remember": [
        "Je batist bavlnený, ľanový, hodvábny, viskózový, syntetický alebo zmesový?",
        "Ktorá výšivka, čipka, gumička, podšívka alebo potlač má najnižší limit?",
        "Je priesvitná línia vlastnosť väzby, posun nití, dierka alebo rozdiel podkladu?",
        "Sú zipsy, háčiky a drsné textílie oddelené a ochranné vrecko umožňuje oplach?",
        "Povoľuje symbol ručné či strojové pranie, odstreďovanie, sušičku, paru a žehlenie?",
        "Je kus po sušení úplne suchý aj v riasení, výšivke a dvojitom leme?",
    ],
    "mistakes": [
        "Predpokladať, že každý batist je bavlnený a znesie rovnakú teplotu.",
        "Hodnotiť kvalitu iba podľa nepriehľadnosti alebo jedného čísla počtu nití.",
        "Prať jemnú blúzku so zipsami, suchým zipsom a ťažkými uterákmi.",
        "Drhnúť škvrnu a vytvoriť svetlú zdrsnenú plochu viditeľnú proti svetlu.",
        "Použiť chlór na biely batist bez kontroly vlákna, výšivky a povrchovej úpravy.",
        "Napínať mokrý šev alebo tlačiť horúcou žehličkou na reliéfnu výšivku.",
    ],
    "expert_heading": "Odbornejší pohľad: plátnová väzba, počet nití, trhanie a zachytenie",
    "expert": [
        "CottonWorks opisuje plátnovú väzbu ako striedanie jednej nite nad a pod susednou niťou, vďaka čomu má viac bodov previazania než základný keper alebo satén. To podporuje štrukturálnu stabilitu, ale výsledný batist zostáva ovplyvnený veľmi jemnou priadzou, hustotou a dokončením. Väzba sama preto nie je záruka odolnosti hotového šva.",
        "ASTM D3775 meria počet osnovných a útkových nití, ASTM D1424 pokračovanie trhliny a ASTM D3939 zachytenie. Vysoký počet jemných nití môže zlepšiť rovnomernosť, no nehovorí priamo, ako sa tkanina zachytí o háčik alebo ako šev rozloží zaťaženie. Porovnávanie vyžaduje rovnakú metódu, smer a kondicionovanie.",
        "Európske nariadenie upravuje názvy a označovanie textilných vlákien, no batist nie je samostatný názov vlákna v zmysle etikety. GINETEX zároveň vysvetľuje symboly pre hotový výrobok. Spotrebiteľ preto potrebuje dve informácie: deklarované zloženie a povolenú starostlivosť; obchodný názov jemnej tkaniny nenahrádza ani jednu z nich.",
    ],
    "source_intro": "Zdroje podporujú opis plátnovej väzby, rozdielne meranie hustoty, trhania a zachytenia aj potrebu oddeliť obchodný názov od deklarovaného vlákna. Nepodporujú jednu teplotu pre všetky batistové textílie.",
    "sources": [
        ("CottonWorks: odborný prehľad tkania", COTTONWORKS_WEAVING),
        ("CottonWorks: základné konštrukcie tkanín", COTTONWORKS_BASIC_WEAVES),
        ("ASTM D3775: počet osnovných a útkových nití", ASTM_COUNT),
        ("ASTM D1424: pokračovanie trhliny v tkanine", ASTM_TEAR),
        ("ASTM D3939: odolnosť textílie proti zachyteniu", ASTM_SNAG),
        ("AATCC TM61: stálofarebnosť pri praní", AATCC_COLOR),
        ("AATCC TM135: rozmerové zmeny po domácom praní", AATCC_DIMENSION),
        ("EÚ 1007/2011: označovanie textilných vlákien", EU_FIBRE_LABEL),
        ("GINETEX: význam symbolov ošetrovania", GINETEX),
    ],
    "product_heading": "Prací gél použite iba pri kompatibilnom prateľnom batiste",
    "product_intro": "Pri bežnom bavlnenom, polyesterovom alebo inom kompatibilnom batiste s povoleným domácim praním možno zvoliť tekutý gél a dávku prispôsobiť náplni, vode a znečisteniu.",
    "product_name": LAUNDRY_PRODUCT_NAME,
    "product_url": LAUNDRY_PRODUCT_URL,
    "product_text": "Tekutý gél sa rovnomerne dávkuje bez sypkých častíc. Jemný kus potrebuje primeranú náplň a dôkladný oplach, nie koncentrát naliaty priamo na suchú tkaninu.",
    "product_limit": "Produkt nie je automaticky vhodný na hodváb, vlnu, citlivú viskózu, nestálofarebnú výšivku, historický textil ani kus určený na profesionálne čistenie. Neopraví posun nití alebo dierku.",
    "category_heading": "Prací prostriedok vyberajte podľa vlákna, farby a ozdôb",
    "category_intro": "Názov batist nestačí. Pred výberom gélu skontrolujte zloženie hlavnej plochy aj výšivky, povolené pranie a to, či jemná konštrukcia dostane dostatok oplachu.",
    "category_name": LAUNDRY_CATEGORY_NAME,
    "category_url": LAUNDRY_CATEGORY_URL,
    "category_text": "V kategórii nájdete gély pre rôzne potreby bežnej prateľnej bielizne. Pri jemnom batiste použite kompatibilný produkt v presnej dávke a vyhnite sa zbytočne agresívnemu cyklu.",
    "related": [
        ("Čo je bavlna a ako sa o ňu starať", ARTICLE_COTTON),
        ("Čo je mušelín a ako ho prať", ARTICLE_MUSLIN),
        ("Ako čítať štítok na oblečení", ARTICLE_LABEL),
        ("Ako farby blednú pri praní a trení", ARTICLE_COLOR),
        ("Ako odstrániť rôzne škvrny z oblečenia", ARTICLE_STAIN),
        ("Ako správne vyžehliť košeľu", ARTICLE_IRONING),
    ],
    "faq_title": "batist a jemné plátnové tkaniny",
    "faq": [
        ("Čo je batist?", "Je to jemná, ľahká a často polopriesvitná tkanina, typicky v plátnovej väzbe."),
        ("Je batist vždy bavlnený?", "Nie. Môže byť z bavlny, ľanu, hodvábu, viskózy, syntetiky alebo zmesi."),
        ("Aký je rozdiel medzi batistom a voálom?", "Oba sú ľahké, no voál býva často vzdušnejší alebo suchší na dotyk. Presné hranice obchodných názvov sa prekrývajú."),
        ("Aký je rozdiel medzi batistom a mušelínom?", "Mušelín označuje širší rozsah od jemnej gázoviny po voľnejšie plátno; batist býva jemnejší, hladší a pravidelnejší."),
        ("Môže ísť batist do práčky?", "Iba ak to povoľuje etiketa hotového výrobku a všetky ozdoby, podšívky a gumičky sú s cyklom kompatibilné."),
        ("Na koľko stupňov prať batist?", "Jedna teplota neexistuje. Rozhoduje presné vlákno, farba, dokončenie a symbol na konkrétnom kuse."),
        ("Treba batist prať v ochrannom vrecku?", "Pri jemnom odeve môže pomôcť proti zachyteniu, ak má správnu veľkosť a nebráni vode ani oplachu."),
        ("Ako odstrániť škvrnu z bieleho batistu?", "Odsajte ju, určte typ a použite kompatibilný postup po skrytej skúške. Biely vzhľad nie je povolenie na chlór."),
        ("Ako žehliť batist?", "Podľa symbolu, z rubu, cez čistú ochrannú tkaninu a s nízkym tlakom. Výšivku podložte mäkko."),
        ("Prečo sa pri šve objavila priesvitná línia?", "Pravdepodobne sa nite posunuli od stehu. Odev ďalej nenapínajte a nechajte šev posúdiť krajčírom."),
        ("Môže ísť batist do sušičky?", "Len pri výslovnom symbole. Teplo a prevaľovanie môžu poškodiť jemné nite, rozmer, výšivku alebo syntetickú zložku."),
    ],
}


ARTICLES: list[dict[str, object]] = [RIPS, VATELIN, SPACER_MESH, BATISTE]


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
        "batch": "batch-50",
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


def seven_word_shingles(value: str) -> set[tuple[str, ...]]:
    words = [word.casefold() for word in WORD_RE.findall(value)]
    return {tuple(words[index : index + 7]) for index in range(max(0, len(words) - 6))}


def jaccard(left: set[tuple[str, ...]], right: set[tuple[str, ...]]) -> float:
    if not left and not right:
        return 1.0
    return len(left & right) / len(left | right)


def main() -> None:
    candidate_titles = [
        line.strip()
        for line in CANDIDATES.read_text(encoding="utf-8-sig").splitlines()
        if line.strip()
    ]
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
            "responsive_tables": len(
                re.findall(r'<div\b[^>]*style="[^"]*overflow-x:\s*auto', body, re.IGNORECASE)
            ),
            "styled_blocks": len(re.findall(r"<div\b[^>]*style=", body, re.IGNORECASE)),
            "action_buttons": len(
                re.findall(r'<a\b[^>]*style="[^"]*display:\s*inline-block', body, re.IGNORECASE)
            ),
            "one_character_paragraphs": len(one_character_paragraphs),
        }
        if metric["words"] < 2800:
            raise SystemExit(f"Article is too short: {article['title']} ({metric['words']} words)")
        if metric["h2"] < 24 or metric["tables"] < 2 or metric["responsive_tables"] != metric["tables"]:
            raise SystemExit(f"Article structure is incomplete: {article['title']} ({metric})")
        if metric["styled_blocks"] < 10 or metric["action_buttons"] < 2 or metric["one_character_paragraphs"]:
            raise SystemExit(f"Article visual blocks are incomplete: {article['title']} ({metric})")
        metrics.append(metric)
        rendered.append(
            {
                "title": article["title"],
                "short": article["short"],
                "long": body,
                "link": article["link"],
                "date_posted": PUBLISH_DATE,
                "time_posted": "17:00:00",
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
                raise SystemExit(f"Article bodies overlap too much: {left['title']} / {right['title']} ({score:.4f})")

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(rendered, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    report = preflight_links(rendered)
    if report["failure_count"]:
        failed = [check for check in report["checks"] if not check["ok"]]
        print(json.dumps({"failed_links": failed}, ensure_ascii=False, indent=2))
        raise SystemExit("Batch 50 link preflight failed")
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
