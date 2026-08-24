#!/usr/bin/env python3
"""Build and validate VEVO batch 46 special-material articles."""

from __future__ import annotations

import json
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

from build_batch_45_fabric_structures import (
    BASE,
    FIXED_PRICE_RE,
    FORBIDDEN_PUBLIC_RE,
    WORD_RE,
    article_hrefs,
    callout,
    commercial_blocks,
    esc,
    faq,
    fetch_status,
    related_links,
    source_box,
    table,
    visible_text,
)


PUBLISH_DATE = "2026-08-24"
CANDIDATES = Path("content/VEVO_CONTENT/batches/batch-46-candidates-2026-08-24.txt")
OUT_JSON = Path("content/VEVO_CONTENT/imports/batch-46-2026-08-24-articles.json")
OUT_PREFLIGHT = Path("content/VEVO_CONTENT/exports/batch-46-2026-08-24-link-preflight.json")

COTTONWORKS_YARNS = "https://cottonworks.com/wp-content/uploads/2017/11/Textile_Yarns.pdf"
COTTONWORKS_BOUCLE = "https://cottonworks.com/encyclopedia-item/boucle/"
COTTONWORKS_WOVEN = "https://cottonworks.com/learning-hub/weaving/basic-woven-fabric-designs/"
FIT_FABRIC = "https://www.fitnyc.edu/museum/exhibitions/fabric-in-fashion.php"
TRELLEBORG_CR = "https://www.trelleborg.com/en/engineered-coated-fabrics/solutions/coatings/cr"
SCUBAPRO_WETSUIT = "https://ww2.scubapro.com/media/806065/11900-j-o-wetsuit-eng.pdf"
ASTM_SNAG = "https://store.astm.org/d3939_d3939m-26.html"
ASTM_PILLING = "https://store.astm.org/d3512_d3512m-22.html"
ASTM_TEAR = "https://store.astm.org/d1424-25.html"
AATCC_DIMENSION = "https://members.aatcc.org/store/tm135/543/"
EU_FIBRE_LABEL = "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02011R1007-20180215"
GINETEX = "https://www.ginetex.net/share/article/4201/care-symbols"

ARTICLE_POLYESTER = "/n/co-je-polyester-a-ako-ho-prat-aby-nezapachal"
ARTICLE_VISCOSE = "/n/co-je-viskoza-vlastnosti-krcivost-zrazanie-a-starostlivost"
ARTICLE_COTTON = "/n/co-je-bavlna-vlastnosti-vyhody-nevyhody-a-starostlivost"
ARTICLE_BLEND = "/n/polyester-vs-bavlna-rozdiely-pri-noseni-prani-a-voni"
ARTICLE_SHRINKAGE = "/n/preco-sa-oblecenie-zrazi-po-prani-teplota-vlakna-susicka-a-prevencia"
ARTICLE_SNAGGING = "/n/zatrhavanie-textilu-preco-vznikaju-vytiahnute-ocka-a-ako-im-predchadzat"
ARTICLE_PILLING = "/n/preco-sa-oblecenie-zmolkuje-vlakna-trenie-pranie-a-susenie"
ARTICLE_COLOR = "/n/stalofarebnost-textilu-preco-farby-blednu-pri-prani-svetle-a-treni"
ARTICLE_ODOR = "/n/preco-moje-oblecenie-zapacha-po-prani-priciny-a-riesenia"
ARTICLE_DRYING = "/n/ako-susit-bielizen-v-malom-byte-bez-zatuchnutia"
ARTICLE_LABEL = "/n/ako-citat-stitok-na-obleceni-material-symboly-prania-a-spravny-program"
ARTICLE_GEL = "/n/ako-vybrat-praci-gel-podla-typu-bielizne"
ARTICLE_STAIN = "/n/ako-odstranit-zuvacku-krv-vosk-a-ine-skvrny-z-oblecenia"
ARTICLE_IRONING = "/n/ako-vyzehlit-koselu-kompletny-sprievodca-pre-dokonaly-vysledok"


def common_sections(article: dict[str, object]) -> list[dict[str, object]]:
    name = str(article["name"])
    return [
        {
            "heading": f"Názov {name} ešte neurčuje celé zloženie",
            "paragraphs": [
                f"Obchodné označenie {name} opisuje najmä {article['construction_summary']}. Samo osebe neurčuje všetky vrstvy, farbivá, lepidlá, podšívku ani povrchové dokončenie. Dva výrobky s rovnakým názvom preto môžu mať odlišnú hmotnosť, pružnosť, prijímanie vody a povolený spôsob čistenia. Bez presného údaja o zložení nemožno bezpečne preniesť teplotu alebo prostriedok z jedného kusu na druhý.",
                f"Európske označovanie vláknového zloženia a symboly starostlivosti odpovedajú na odlišné otázky. Percentá opisujú deklarované textilné vlákna, kým symboly stanovujú najprísnejší povolený postup pre celý hotový výrobok. Pri {article['genitive']} navyše skontrolujte {article['label_details']}. Najcitlivejší prvok môže znížiť hranicu, ktorú by samotná hlavná plocha ešte zniesla.",
            ],
        },
        {
            "heading": "Najprv odlíšte voľnú nečistotu, škvrnu, pach a poškodenie",
            "paragraphs": [
                f"Prach alebo piesok možno často odstrániť nasucho, škvrna vyžaduje lokálne ošetrenie, pach potrebuje odstrániť zdroj a {article['failure_sign']} je mechanická chyba. Ak všetky prejavy riešite dlhším praním, zvyšujete trenie bez istoty, že zasahujete správnu príčinu. Pred vodou si preto kus prezrite pri dennom svetle, skontrolujte švy a zistite, kde presne problém vznikol.",
                f"Pri {name} je dôležité nehodnotiť iba farbu povrchu. Maz, soľ, kozmetika alebo zvyšok prostriedku sa môžu držať v {article['residue_place']}, zatiaľ čo poškodené miesto môže pôsobiť ako svetlejšia škvrna. Jemné odsatie bielou handričkou a kontrola z rubu poskytnú viac informácií než okamžité drhnutie. Pri neznámej chémii alebo hodnotnom kuse zásah zastavte.",
            ],
        },
        {
            "heading": "Triedenie náplne chráni povrch pred cudzím trením",
            "paragraphs": [
                f"Rovnaká farba neznamená rovnakú mechanickú záťaž. Zips, háčik, suchý zips, hrubý uterák alebo tvrdá aplikácia môžu zachytiť {article['friction_risk']}. Citlivý kus oddeľte od ťažkých a drsných vecí, zatvorte bezpečné kovanie a voľné prvky chráňte iba spôsobom, ktorý stále dovolí vode prejsť celou plochou.",
                "Preplnený bubon nevytvorí šetrnejšie prostredie. Textílie sa stláčajú, záhyby sa trú na jednom mieste a oplach nedosiahne medzi vrstvy. Naopak, jediný ľahký kus v nevhodne prudkom programe môže opakovane narážať do bubna. Program, hmotnosť náplne, otáčky a ochranné vrecko musia byť zvolené ako jeden celok podľa etikety.",
            ],
        },
        {
            "heading": "Dávkovanie a oplach sú dôležitejšie než silná vôňa",
            "paragraphs": [
                f"Nadbytok prostriedku môže pri {name} zostať v {article['residue_place']}, zmeniť omak a po opätovnom navlhčení zvýrazniť pach. Dávku počítajte podľa návodu produktu, veľkosti náplne, tvrdosti vody a znečistenia. Nepridávajte ďalší gél iba preto, že mokrý materiál vonia inak než suchý; najprv skontrolujte oplach a úplné vysušenie.",
                "Dodatočný oplach môže pomôcť po jednorazovom predávkovaní, nie je však univerzálnou opravou nesprávneho programu. Pri ručnom praní vodu vymieňajte bez krútenia a stláčania citlivej konštrukcie. Ak výrobca vyžaduje špeciálny prípravok alebo profesionálne čistenie, bežný prací gél ani domáci doplnok tento pokyn nenahrádza.",
            ],
        },
        {
            "heading": f"Sušenie {article['genitive']} je samostatná fáza starostlivosti",
            "paragraphs": [
                f"Po čistení kus podoprite a nenechajte ho dlho stlačený v nádobe alebo bubne. {article['drying_advice']} Prúdenie vzduchu musí dosiahnuť rub, švy, preloženia a všetky vrstvy. Chladnejšie miesto na dotyk môže ešte obsahovať vlhkosť, hoci vonkajší povrch už pôsobí suchý.",
                f"Sušičku, radiátor, fén a priame slnko nepoužívajte ako univerzálne urýchlenie. Nadmerné teplo môže ovplyvniť {article['heat_risk']}. Pred uložením nechajte kus ustáliť pri izbových podmienkach a znova skontrolujte pach aj tvar. Úplné vysušenie nie je kozmetický detail, ale prevencia zatuchnutia a dlhého pôsobenia vlhkosti.",
            ],
        },
        {
            "heading": "Domáci test je orientačný, nie laboratórny dôkaz",
            "paragraphs": [
                f"Skúška kvapkou vody, jemným ohybom alebo bielou handričkou môže upozorniť na uvoľňovanie farby či povrchovú úpravu, ale neurčí presné vlákno ani životnosť {article['genitive']}. Výsledok mení miesto odberu, tlak, vlhkosť a predchádzajúce čistenie. Domáci test preto používajte na odhalenie rizika pred lokálnym zásahom, nie na prepisovanie etikety.",
                "Normované skúšky vždy definujú vzorku, kondicionovanie, prístroj, smer a spôsob hodnotenia. Číslo pevnosti, žmolkovania alebo rozmerovej zmeny bez názvu metódy nie je priamo porovnateľné s iným číslom. Pre spotrebiteľa z toho vyplýva praktické pravidlo: pýtajte sa, čo sa meralo, a neprekladajte jednu vlastnosť na sľub celkovej kvality.",
            ],
        },
        {
            "heading": "Kedy domáce čistenie prerušiť",
            "paragraphs": [
                f"Zásah zastavte, ak sa uvoľňuje farba, povrch sa lepí, vrstva sa oddeľuje, {article['failure_sign']} alebo etiketa vyžaduje odborný postup. Ďalší cyklus môže z malej chyby vytvoriť väčšiu a sťažiť reklamáciu. Stav odfoťte pri rovnakom svetle, zapíšte použitý postup a nový výrobok riešte s predajcom skôr, než skúsite agresívnejšiu chémiu.",
                "Historický, svadobný, scénický alebo inak hodnotný textil nemožno posudzovať ako bežný moderný kus. Konzervačná prax považuje čistenie za nevratný zásah a krehké, zdobené alebo kombinované predmety nemusia byť vhodné na domáce pranie. Pri takomto kuse je dôležitejšie zachovať materiál než odstrániť každú stopu používania.",
            ],
        },
        {
            "heading": f"Ako vybrať {name} podľa použitia, nie podľa jedného prívlastku",
            "paragraphs": [
                f"Pri kúpe porovnajte presné zloženie, konštrukciu, hrúbku alebo hmotnosť, švy, podšívku a symboly starostlivosti. Dotyk v predajni neukáže, ako sa {name} zmení po vode, pote, trení a sušení. Praktickejšia je informácia o určenom použití, rozmerovej stabilite, stálofarebnosti a dostupnosti opravy než všeobecný prívlastok prémiový alebo odolný.",
                f"Dobrý výrobok z {article['genitive']} má vlastnosti primerané úlohe a zrozumiteľný návod. Jemná blúzka, kabát, športová pomôcka a čalúnenie nepotrebujú rovnakú mäkkosť ani rovnaký spôsob údržby. Kvalitu preto posudzujte ako súlad materiálu, konštrukcie a funkcie, nie ako rebríček založený na jedinom čísle alebo obchodnom názve.",
            ],
        },
    ]


def render_article(article: dict[str, object]) -> str:
    parts = [
        f"<p><strong>Rýchla odpoveď:</strong> {article['answer']}</p>",
        f"<p>{article['intro']}</p>",
        callout("Najdôležitejšie zistenia v skratke", article["quick"]),
        f"<h2>{esc(article['overview_heading'])}</h2>",
    ]
    parts.extend(f"<p>{paragraph}</p>" for paragraph in article["overview"])
    parts.append(f"<h2>{esc(article['table1_heading'])}</h2>")
    parts.append(f"<p>{article['table1_intro']}</p>")
    parts.append(table(article["table1_headers"], article["table1_rows"]))
    for section in list(article["sections"]) + common_sections(article):
        parts.append(f"<h2>{esc(section['heading'])}</h2>")
        parts.extend(f"<p>{paragraph}</p>" for paragraph in section["paragraphs"])
        if section.get("callout"):
            note = section["callout"]
            parts.append(callout(note["title"], note["items"], background=note.get("background", "#fffaf5"), border=note.get("border", "#e6ded2")))
    parts.append(f"<h2>{esc(article['table2_heading'])}</h2>")
    parts.append(f"<p>{article['table2_intro']}</p>")
    parts.append(table(article["table2_headers"], article["table2_rows"]))
    parts.append(f"<h2>{esc(article['steps_heading'])}</h2>")
    parts.append("<ol>" + "".join(f"<li>{item}</li>" for item in article["steps"]) + "</ol>")
    parts.append(callout("Čo si skontrolovať pred čistením", article["remember"], background="#f7fbf8", border="#dbe5de"))
    parts.append(callout("Najčastejšie chyby", article["mistakes"], background="#fff7f7", border="#eadada"))
    parts.append(f"<h2>{esc(article['expert_heading'])}</h2>")
    parts.extend(f"<p>{paragraph}</p>" for paragraph in article["expert"])
    parts.append(source_box(article))
    parts.append(commercial_blocks(article))
    parts.append(related_links(article["related"]))
    parts.append(faq(article))
    return "\n".join(parts)


NEOPRENE: dict[str, object] = {
    "title": "Čo je neoprén: penový materiál, zápach, čistenie a sušenie",
    "link": "co-je-neopren-penovy-material-zapach-cistenie-a-susenie",
    "meta": "Čo je neoprén, z čoho sa skladá neoprénový oblek či bandáž, ako ich čistiť od soli, chlóru a potu a ako sušiť neoprén bez zápachu.",
    "short": "Neoprénový výrobok je zvyčajne viacvrstvový celok z penového elastoméru, textilných poťahov, lepidiel a švov. Naučte sa ho opláchnuť, vyčistiť, vysušiť a uložiť bez stlačenia, zápachu a oddeľovania vrstiev.",
    "name": "neoprén",
    "genitive": "neoprénu",
    "construction_summary": "penový polychloroprén alebo príbuznú elastomérnu penu, často laminovanú textilom a spojenú lepidlom, slepými stehmi, páskou alebo zváraným spojom",
    "label_details": "druh peny, hrúbku, textilný poťah na líci a rube, lepené plochy, typ šva, zips, suchý zips a pokyny výrobcu športovej alebo zdravotnej pomôcky",
    "residue_place": "pórovitej pene, textilnej laminácii, švoch, manžetách a miestach pri zipse",
    "friction_risk": "textilný poťah, odkrytú penu, lepený okraj alebo niť v namáhanom šve",
    "drying_advice": "Hrubší oblek otočte podľa pokynov výrobcu, sušte obe strany v tieni a použite širokú oporu, ktorá nevytvorí ostrý tlak na ramenách. Bandáž alebo návlek rozložte tak, aby pod preložením nezostala voda.",
    "heat_risk": "pružnosť peny, bunkovú štruktúru, lepidlo, lamináciu, farbu a tvar tesniacich okrajov",
    "failure_sign": "pena praská, povrch sa lepí, laminácia tvorí bubliny alebo sa šev a lepený spoj rozchádzajú",
    "answer": "Neoprén nie je obyčajná látka. V potápačskom obleku, plaveckom návleku, rukavici alebo bandáži ide zvyčajne o penovú elastomérnu vrstvu spojenú s textilom, lepidlom a švami. Po použití v slanej alebo chlórovanej vode ho čo najskôr opláchnite čistou sladkou vodou, pri povolení výrobcu jemne ručne vyčistite, bez krútenia vytlačte prebytok vody a nechajte úplne vyschnúť z oboch strán mimo priameho slnka a silného tepla. Bežný prací gél, práčka, sušička, bielidlo ani aviváž nie sú automaticky vhodné. Zápach riešte odstránením soli, potu a zvyškovej vlhkosti, nie silnou parfumáciou. Pri lepených švoch, zdravotnej pomôcke alebo bezpečnostnej výstroji má vždy prednosť presný návod výrobcu.",
    "intro": "Otázka ako prať neoprén často vznikne až vtedy, keď oblek po kúpaní zapácha, bandáž ostane vlhká alebo sa na okraji objaví svetlá mapa. Najväčšou chybou je posudzovať ho ako polyesterové tričko. Povrch môže byť nylonový alebo polyesterový, jadro je však pružná pena a celok držia spoje, ktoré reagujú na teplo, rozpúšťadlá, stlačenie a dlhú vlhkosť inak než bežná tkanina. Správny postup preto začína určením výrobku a jeho konštrukcie. Až potom sa rozhoduje, či stačí oplach, či výrobca povoľuje špeciálny čistiaci prípravok a ako dlho treba sušiť vnútro.",
    "quick": [
        "<strong>Neoprén je viacvrstvový systém:</strong> pena, textilný poťah, lepidlo, švy a doplnky nemajú rovnakú citlivosť.",
        "<strong>Po slanej a chlórovanej vode oplachujte:</strong> čerstvá voda odstráni zvyšky skôr, než zaschnú v švoch a textilnom povrchu.",
        "<strong>Práčka nie je východiskový bod:</strong> použite ju iba pri výslovnom pokyne konkrétneho výrobcu.",
        "<strong>Sušte zvnútra aj zvonka:</strong> suchý líc nemusí znamenať suchú penu, manžetu alebo preložený šev.",
        "<strong>Teplo a slnko môžu urýchliť poškodenie:</strong> radiátor, horúca sušička a dlhé UV žiarenie nie sú bezpečnou skratkou.",
        "<strong>Pach sa neprekrýva vôňou:</strong> najprv odstráňte pot, vodné zvyšky a vlhkosť a skontrolujte stav materiálu.",
    ],
    "overview_heading": "Čo je neoprén a čo sa pod týmto názvom predáva",
    "overview": [
        "Neoprén je známy obchodný názov spájaný s polychloroprénovým kaučukom, označovaným aj CR. Trelleborg opisuje polychloroprén ako všeobecný elastomér s odolnosťou voči poveternostným vplyvom, ozónu, ohybu a oderu, ale zároveň uvádza konkrétne chemické hranice. Tieto priemyselné vlastnosti nemožno preložiť na tvrdenie, že každý spotrebiteľský výrobok odolá rovnakému čisteniu. Penová receptúra, hustota, uzavreté bunky, laminácia a spoje vytvoria odlišný celok.",
        "Potápačský oblek obyčajne využíva penové jadro na obmedzenie prúdenia vody pri tele a textilné laminácie na ochranu, pružnosť a pohodlie. Surfovací oblek, triatlonový oblek, topánky a rukavice sa líšia hrúbkou, strihom aj povrchom. Zdravotná bandáž môže mať suchý zips a kontakt s kožou; obal notebooku zas inú penu, výstuž a podšívku. Jedna veta o praní preto nemôže byť bezpečná pre všetky predmety označené ako neoprénové.",
        "Aj slovné spojenie bez neoprénu môže označovať podobný penový materiál z iného polyméru. Pri kúpe a údržbe sa nespoliehajte iba na hovorový názov. Hľadajte materiálové údaje, návod, povolené prostriedky a obmedzenia sušenia. Ak ide o výstroj, ktorej poškodenie ovplyvní tepelný komfort, vztlak alebo bezpečnosť, vizuálne čistý povrch nestačí; rozhoduje aj celistvosť peny a spojov.",
    ],
    "table1_heading": "Neoprénové výrobky a ich odlišné riziká",
    "table1_intro": "Rozdiel v použití mení typ nečistoty, konštrukciu aj to, či možno výrobok ponoriť. Vždy porovnajte tabuľku s návodom konkrétneho výrobcu.",
    "table1_headers": ["Výrobok", "Čo sa v ňom drží", "Konštrukčné riziko", "Bezpečný prvý krok"],
    "table1_rows": [
        ("Potápačský alebo surfovací oblek", "Soľ, chlór, piesok, pot, kožný maz a opaľovací prípravok.", "Hrubá pena, lepené alebo páskované švy, zips a tesniace okraje.", "Po použití dôkladne opláchnuť čistou sladkou vodou podľa návodu."),
        ("Neoprénové rukavice a topánky", "Voda, piesok, pot a biologické zvyšky v uzavretom priestore.", "Pomalé vnútorné schnutie, lepená podrážka, namáhané ohyby a švy.", "Opláchnuť zvnútra aj zvonka a otvoriť pre prúdenie vzduchu."),
        ("Športová alebo zdravotná bandáž", "Pot, kožný maz, krém a vlákna zo suchého zipsu.", "Elastické okraje, suchý zips, lepidlo a pri zdravotnej pomôcke hygienické požiadavky.", "Riaďte sa návodom výrobcu; po použití rozopnúť a vysušiť."),
        ("Obal, podložka alebo domáci doplnok", "Prach, omrvinky, nápoj a mastnota z rúk.", "Vnútorná výstuž, potlač, zips alebo podšívka nemusia zniesť ponorenie.", "Najprv povrchovo očistiť a overiť, či sa smie celý predmet namočiť."),
    ],
    "sections": [
        {
            "heading": "Penové jadro, textilná laminácia a švy pracujú spolu",
            "paragraphs": [
                "Penové jadro obsahuje veľké množstvo buniek a jeho hrúbka ovplyvňuje objem, ohyb a čas schnutia. Na povrch sa môže laminovať úplet, ktorý chráni penu pred odieraním a uľahčuje obliekanie. Ďalšie panely spája steh, lepidlo, páska alebo kombinácia. Čistiaci postup preto nesmie hodnotiť iba viditeľný textil; poškodenie lepidla alebo peny môže byť vážnejšie než malá farebná zmena na líci.",
                "V miestach ohybu, kolien, podpazušia, päty a zipsu sa materiál opakovane stláča. Soľné kryštály a piesok pridávajú abrazívne častice, ktoré pri pohybe trú povrch. Pred sušením ich treba opláchnuť, nie zatlačiť kefou. Keď sa laminácia oddeľuje, nevkladajte kus do ďalšieho mechanického cyklu; najprv posúďte opravu vhodným systémom odporúčaným výrobcom.",
            ],
        },
        {
            "heading": "Hrúbka v milimetroch nie je jediný údaj o teple",
            "paragraphs": [
                "Označenie 2 mm, 3/2 mm alebo 5/4 mm opisuje hrúbku panelov, nie univerzálnu teplotu vody ani mieru ochrany. Strih, tesnosť, presakovanie pri krku a manžetách, stlačenie peny, pohyb a individuálna tolerancia chladu menia výsledok. Starší oblek môže mať rovnaké číslo na štítku, ale stlačené alebo popraskané miesta už neizolujú rovnako.",
                "Pri čistení hrubšieho obleku počítajte s väčšou hmotnosťou vody a dlhším schnutím. Úzky vešiak môže vytvoriť tlak na ramená, zatiaľ čo preloženie cez šnúru stlačí jednu líniu. Použite širokú oporu odporúčanú výrobcom a počas sušenia kontrolujte najhrubšie panely a švy. Oblek neodkladajte len preto, že povrch prestal kvapkať.",
            ],
        },
        {
            "heading": "Ako opláchnuť neoprén po mori, bazéne a jazere",
            "paragraphs": [
                "Po mori opláchnite obe strany čistou sladkou vodou, aby soľ nezostala v textilnom povrchu, zipse a švoch. Po bazéne je dôležité odstrániť chlórovanú vodu bez dlhého odkladu. SCUBAPRO vo svojich pokynoch k neoprénovým oblekom odporúča po každom použití oplach sladkou vodou, sušenie z oboch strán a vyhýbanie sa priamemu slnku. Návod konkrétneho modelu má vždy prednosť.",
                "Jazero alebo rieka neprinášajú soľ, ale môžu obsahovať sediment a organické zvyšky. Oblek najprv jemne opláchnite, aby sa piesok neuvoľňoval trením pod kefou. Zips pohybujte až po odstránení častíc. Použitú oplachovú vodu vymeňte, ak je kalná; opakované máčanie v tej istej vode iba premiestňuje nečistotu späť na materiál.",
            ],
        },
        {
            "heading": "Ako ručne vyčistiť neoprén bez krútenia",
            "paragraphs": [
                "Ak oplach nestačí a výrobca povoľuje čistiaci prípravok, použite dostatočne veľkú nádobu, vlažnú alebo studenú vodu podľa návodu a presnú koncentráciu kompatibilného produktu. Oblek rozložte, jemne pretláčajte vodu cez textilné povrchy a nesnažte sa penu žmýkať skrútením. Dlhé namáčanie nie je automaticky účinnejšie a môže zbytočne zaťažiť spoje.",
                "Čistú vodu na oplach meňte dovtedy, kým v nej nezostáva viditeľná pena alebo nečistota. Kus zdvíhajte s oporou viacerých miest, pretože nasiaknutá výstroj je ťažšia. Prebytok nechajte odtiecť a materiál iba jemne stlačte medzi dlaňami, ak to výrobca povoľuje. Uterák možno použiť ako savú oporu bez rolovania, ktoré by vytvorilo ostrý záhyb.",
            ],
        },
        {
            "heading": "Môže ísť neoprén do práčky alebo sušičky",
            "paragraphs": [
                "Predvolená odpoveď je nie, pokiaľ výrobca konkrétneho výrobku výslovne nepovoľuje strojový cyklus. Práčka pridáva opakované ohýbanie, odstreďovanie, kontakt s bubnom a riziko zachytenia zipsu. Zdravotná bandáž môže mať vlastný jemný postup, zatiaľ čo potápačský oblek alebo lepená topánka ho nemusia zniesť. Rovnaký názov materiálu preto nestačí na spoločné rozhodnutie.",
                "Sušička pridáva teplo a mechanické prevaľovanie. Aj keď textilný poťah vyzerá pevne, pena, lepidlo a tesniace prvky môžu mať nižší limit. Neoprén nesušte na radiátore ani horúcim fénom. Ak potrebujete rýchlejšie schnutie, zvýšte prúdenie okolitého vzduchu, otvorte preložené miesta a pravidelne kontrolujte rub bez priameho prehrievania.",
            ],
            "callout": {
                "title": "Rozhodovanie pred použitím práčky",
                "items": [
                    "Je na etikete výslovne uvedené strojové pranie, alebo iba všeobecný názov materiálu?",
                    "Obsahuje predmet lepené panely, zips, výstuž, suchý zips alebo bezpečnostnú súčasť?",
                    "Poznáte povolenú teplotu, otáčky, prostriedok a spôsob sušenia pre tento model?",
                    "Ak niektorá odpoveď chýba, zvoľte oplach alebo kontaktujte výrobcu namiesto pokusu.",
                ],
                "background": "#f7fbf8",
                "border": "#dbe5de",
            },
        },
        {
            "heading": "Prečo neoprén zapácha a prečo sa pach vracia",
            "paragraphs": [
                f"Pach môže pochádzať z potu, kožného mazu, vody, mikrobiálnej aktivity, zvyškov produktu alebo z materiálu, ktorý zostal vlhký v taške. Hrubá pena a uzavreté topánky schnú pomalšie než tenké tričko. Všeobecné príčiny rozoberá článok <a href=\"{ARTICLE_ODOR}\">prečo oblečenie zapácha po praní</a>, pri neoprénovej výstroji však treba navyše skontrolovať vnútorné vrstvy a spoje.",
                "Ak sa pach objaví hneď po zahriatí alebo navlhčení, neznamená to, že treba pridať parfum. Materiál znovu opláchnite podľa návodu, zabezpečte úplné vysušenie a vyčistite tašku, v ktorej sa výstroj prenáša. Trvalý chemický, gumový alebo zatuchnutý pach spolu s lepkavosťou, praskaním či oddeľovaním vrstiev môže signalizovať degradáciu; vtedy prestaňte výrobok používať a požiadajte výrobcu o posúdenie.",
            ],
        },
        {
            "heading": "Opaľovací krém, olej a lokálne škvrny",
            "paragraphs": [
                "Mastné prípravky sa držia najmä pri krku, zápästiach a okrajoch. Neodmasťujte ich rozpúšťadlom, koncentrovaným alkoholom ani neovereným odmasťovačom. Trelleborg síce uvádza chemické vlastnosti polychloroprénu, ale spotrebiteľský oblek obsahuje aj iné vrstvy a lepidlá. Najprv skúste postup odporúčaný výrobcom a na skrytom mieste skontrolujte farbu a povrch.",
                "Pevný piesok alebo zaschnuté blato najprv uvoľnite vodou bez škrabania. Pri nápoji na obale notebooku overte, či možno predmet ponoriť a či tekutina neprenikla k zariadeniu. Bezpečnostná priorita elektroniky je oddelená od čistenia materiálu. Škvrna, ktorú nemožno odstrániť bez poškodenia laminácie, môže byť prijateľnejšia než agresívny zásah.",
            ],
        },
        {
            "heading": "Skladovanie bez ostrého preloženia a stlačenia",
            "paragraphs": [
                "Úplne suchý oblek skladujte v chladnom, suchom a tmavom priestore podľa návodu, ideálne na širokom vhodnom vešiaku alebo rozložený bez ostrého lomu. Nenechávajte ho mesiace pod ťažkými predmetmi ani zrolovaný v tesnej taške. Trvalý tlak môže stlačiť penu a záhyb sa môže stať miestom ďalšieho praskania.",
                "Zips nechajte v polohe odporúčanej výrobcom, odstráňte piesok a skontrolujte kovové alebo plastové časti. Bandáže skladujte rozopnuté alebo voľne uložené tak, aby suchý zips netrhal textilný povrch. Pred ďalšou sezónou prezrite lepené spoje, manžety a najviac ohýbané miesta. Čistý zápach nie je dôkazom, že výstroj je konštrukčne v poriadku.",
            ],
        },
    ],
    "table2_heading": "Neoprén po použití: príznak, príčina a ďalší krok",
    "table2_intro": "Rovnaký prejav môže mať hygienickú aj konštrukčnú príčinu. Pred ďalším čistením odlíšte zvyšok vody a produktu od poškodenia peny alebo spoja.",
    "table2_headers": ["Príznak", "Možné vysvetlenie", "Čo skontrolovať", "Rozumný ďalší krok"],
    "table2_rows": [
        ("Pach po úplnom vysušení", "Zvyšky potu, soli, nečistá taška, slabý oplach alebo vnútorná vlhkosť.", "Rub, švy, topánky, tašku a povolený čistiaci postup.", "Zopakovať oplach alebo povolené jemné čistenie a vysušiť obe strany."),
        ("Bublina medzi textilom a penou", "Oddeľovanie laminácie, poškodené lepidlo alebo lokálne prehriatie.", "Okraje bubliny, lepkavosť, praskliny a záruku výrobku.", "Nezaťažovať ďalším cyklom; riešiť opravu alebo reklamáciu."),
        ("Pena je plochá v jednom mieste", "Dlhé stlačenie, úzky vešiak, skladací lom alebo opotrebenie.", "Spôsob skladovania a rozdiel hrúbky oproti okoliu.", "Odstrániť tlak; bezpečnostnú výstroj nechať posúdiť."),
        ("Zips ide ťažko", "Soľ, piesok, korózia, deformácia pásky alebo nedostatok povolenej údržby.", "Čistotu zubov, šev a návod na mazanie konkrétneho zipsu.", "Najprv opláchnuť; nepoužiť náhodný olej ani silu."),
        ("Povrch sa lepí alebo práši", "Degradácia polyméru, povlaku alebo lepidla, chemická reakcia či teplo.", "Rozsah, pach, vek, skladovanie a kontakt s chémiou.", "Prestať používať a kontaktovať výrobcu; nepridávať rozpúšťadlo."),
    ],
    "steps_heading": "Bezpečný postup čistenia neoprénu krok za krokom",
    "steps": [
        "Identifikujte presný výrobok, materiálové vrstvy a pokyny výrobcu; odfoťte existujúce poškodenie.",
        "Odstráňte piesok a pevné častice jemným oplachom bez drhnutia a skontrolujte zipsy a suché zipsy.",
        "Po mori alebo bazéne opláchnite všetky strany čistou sladkou vodou čo najskôr po použití.",
        "Ak je potrebné čistenie, použite iba výrobcom povolený prostriedok, koncentráciu, teplotu a spôsob.",
        "Materiál nekrúťte; prebytok vody nechajte odtiecť s rovnomernou oporou mokrého kusu.",
        "Sušte v tieni s prúdením vzduchu, podľa potreby otočte rub a líc a kontrolujte hrubé švy a preloženia.",
        "Úplne suchý kus uložte bez ostrého lomu, dlhého stlačenia, priameho slnka a zdroja tepla.",
    ],
    "remember": [
        "Je výrobok skutočne z polychloroprénu, alebo ide o iný penový elastomér predávaný pod všeobecným názvom?",
        "Povoľuje výrobca ponorenie, čistiaci prípravok, práčku alebo iba oplach čistou vodou?",
        "Sú pena, laminácia, švy, zips a tesniace okraje bez praskania a oddeľovania?",
        "Odstránili ste piesok a soľ skôr, než začnete materiál ohýbať alebo čistiť?",
        "Má vzduch prístup k lícu, rubu, topánke, manžete a najhrubším panelom?",
        "Je výstroj pred uložením suchá a bez ostrého preloženia či tlaku?",
    ],
    "mistakes": [
        "Vložiť neoprénový oblek do práčky iba preto, že textilný povrch vyzerá ako syntetický úplet.",
        "Použiť bežný gél, aviváž, bielidlo, rozpúšťadlo alebo dezinfekciu bez povolenia výrobcu.",
        "Sušiť penu na radiátore, horúcim vzduchom alebo dlhodobo na priamom slnku.",
        "Zavesiť ťažký mokrý oblek na úzky drôtený vešiak a vytvoriť tlak na ramenách.",
        "Odložiť topánky alebo rukavice, keď je suchý iba vonkajší povrch.",
        "Prekryť zatuchnutie silnou vôňou bez odstránenia potu, vody a vnútornej vlhkosti.",
    ],
    "expert_heading": "Odbornejší pohľad: polychloroprén, laminácia a hranice odolnosti",
    "expert": [
        "Trelleborg klasifikuje polychloroprén ako všeobecný elastomér a uvádza dobré starnutie v ozóne a počasí, odolnosť proti ohybovým trhlinám a oderu, ale tiež zoznam chemických látok, ktoré ho napádajú. Ide o vlastnosti konkrétnych priemyselných formulácií a skúšobných podmienok. Spotrebiteľský oblek obsahuje penovú štruktúru, textil, lepidlo a farbu, preto sa jeho povolená chémia musí čítať z návodu hotového výrobku.",
        "Návod SCUBAPRO pre neoprénové obleky zdôrazňuje oplach čistou sladkou vodou po použití, sušenie zvnútra aj zvonka, vyhýbanie sa priamemu slnku a úplné vysušenie pred uložením. Z toho nemožno odvodiť jednotnú koncentráciu detergentu pre všetky značky. Odborný zdroj podporuje princíp odstránenia vodných zvyškov a šetrného sušenia, nie univerzálny domáci recept.",
        "Pri výstroji je dôležité oddeliť vzhľad od funkcie. Pena môže byť lokálne stlačená, lepený šev oslabený a laminácia oddelená, hoci farba ostala. Čistenie neobnoví pôvodnú bunkovú štruktúru ani pevnosť spoja. Ak má výrobok ochrannú alebo zdravotnú funkciu, rozhodnutie o ďalšom používaní musí vychádzať z pokynov výrobcu a technického stavu, nie iba zo zápachu či čistoty.",
    ],
    "source_intro": "Zdroje vysvetľujú vlastnosti polychloroprénu, výrobcom odporúčaný oplach a sušenie a všeobecné hranice symbolov ošetrovania. Nepodporujú tvrdenie, že každý neoprénový výrobok možno prať rovnakým gélom alebo programom.",
    "sources": [
        ("Trelleborg: polychloroprén a jeho technické vlastnosti", TRELLEBORG_CR),
        ("SCUBAPRO: návod na starostlivosť o neoprénový oblek", SCUBAPRO_WETSUIT),
        ("EÚ 1007/2011: označovanie textilných vlákien", EU_FIBRE_LABEL),
        ("GINETEX: význam symbolov ošetrovania", GINETEX),
    ],
    "product_intro": "Neoprénový oblek, bandáž ani lepený doplnok nepovažujte automaticky za bežnú prateľnú bielizeň. Produktová karta je relevantná iba pre predmet, ktorého výrobca výslovne povoľuje bežný prací gél; pri výstroji má často prednosť špeciálny prípravok alebo samotný oplach.",
    "product_text": "Hypoalergénny prací gél z marseillského mydla je určený na bežnú domácu bielizeň. Na neoprén ho použite len vtedy, keď to povoľuje návod konkrétneho výrobku vrátane dávky, teploty a oplachu. Bez takého povolenia zvoľte výrobcom určený postup.",
    "product_limit": "Gél nie je univerzálny čistič neoprénových oblekov, lepených topánok, zdravotných pomôcok ani bezpečnostnej výstroje. Neopraví degradovanú penu, odlepenú lamináciu a nepreukazuje hygienickú vhodnosť pre zdravotnícke použitie.",
    "category_intro": "Kategória pracích gélov slúži na porovnanie produktov pre bežnú prateľnú bielizeň. Neoprénový predmet do nej zaraďte iba po kontrole etikety a návodu výrobcu; samotný textilný poťah nie je dostatočný dôkaz kompatibility.",
    "category_text": "Pri povolenom bežnom praní vyberajte prostriedok podľa celého výrobku, nie iba podľa slova syntetika. Ak návod vyžaduje špeciálny čistič, oplach bez detergentu alebo odbornú údržbu, túto požiadavku dodržte.",
    "related": [
        ("Prečo oblečenie zapácha po praní", ARTICLE_ODOR),
        ("Ako čítať štítok na oblečení", ARTICLE_LABEL),
        ("Ako vybrať prací gél podľa bielizne", ARTICLE_GEL),
        ("Čo je polyester a ako ho prať", ARTICLE_POLYESTER),
        ("Ako sušiť bielizeň bez zatuchnutia", ARTICLE_DRYING),
        ("Ako predchádzať zatrhávaniu textilu", ARTICLE_SNAGGING),
    ],
    "faq_title": "neoprén, čistenie a sušenie",
    "faq": [
        ("Môže sa neoprén prať v práčke?", "Iba ak to výslovne povoľuje výrobca konkrétneho výrobku. Potápačský oblek, bandáž a obal môžu mať úplne odlišné vrstvy, švy a pokyny."),
        ("Na koľko stupňov prať neoprén?", "Jedna teplota neexistuje. Riaďte sa návodom výrobcu; vysoké teplo môže ovplyvniť penu, lepidlo, lamináciu aj pružné okraje."),
        ("Ako odstrániť zápach z neoprénu?", "Najprv odstráňte soľ, chlór, pot a nečistoty povoleným oplachom alebo čistením. Potom vysušte obe strany aj švy. Silná vôňa príčinu nevyrieši."),
        ("Môžem použiť bežný prací gél?", "Len pri výslovnom povolení výrobcu. Mnohé obleky vyžadujú špeciálny prípravok alebo oplach; bežný gél môže zostať v pene alebo ovplyvniť spoje."),
        ("Môže ísť neoprén do sušičky?", "Bez výslovného povolenia nie. Teplo a prevaľovanie môžu poškodiť penu, lepidlo, textilnú lamináciu a tesniace časti."),
        ("Ako dlho schne neoprénový oblek?", "Závisí od hrúbky, teploty, vlhkosti a prúdenia vzduchu. Rozhodujúca nie je hodina, ale úplná suchosť líca, rubu, švov a najhrubších panelov."),
        ("Prečo sa neoprén lepí alebo práši?", "Môže ísť o degradáciu polyméru, povlaku alebo lepidla. Výrobok prestaňte používať a kontaktujte výrobcu; rozpúšťadlo alebo ďalšie pranie môže stav zhoršiť."),
        ("Ako skladovať neoprénový oblek?", "Úplne suchý, mimo slnka a tepla, bez ostrého preloženia a dlhého stlačenia. Použite širokú oporu alebo spôsob odporúčaný výrobcom."),
    ],
}


CHIFFON: dict[str, object] = {
    "title": "Čo je šifón: jemná priesvitná látka a bezpečná starostlivosť",
    "link": "co-je-sifon-jemna-priesvitna-latka-a-bezpecna-starostlivost",
    "meta": "Čo je šifón, aký je rozdiel medzi hodvábnym a polyesterovým šifónom, ako prať šifónové šaty či blúzku a ako ich sušiť bez vytiahnutia.",
    "short": "Šifón je ľahká priesvitná tkanina, nie jedno vlákno. Spoznajte hodvábny, polyesterový a viskózový šifón, riziko zatrhnutia, šikmého vytiahnutia a bezpečné čistenie hotového odevu.",
    "name": "šifón",
    "genitive": "šifónu",
    "construction_summary": "veľmi ľahkú, priesvitnú tkaninu s jemnými priadzami a zrnitým až mierne drsným povrchom, často v plátnovej väzbe a s vysoko zakrútenými filamentmi",
    "label_details": "presné vlákno, podšívku, šikmý strih, riasenie, výšivku, koráliky, lepenú ozdobu a nosné švy na ramenách a v páse",
    "residue_place": "jemnej otvorenejšej väzbe, riasení, úzkych lemoch, švoch a medzi vrstvami podšívky",
    "friction_risk": "jemné priadze, otvorené medzery väzby, okraj riasenia alebo voľný dekoratívny prvok",
    "drying_advice": "Odev položte na čistú savú podložku alebo ho podoprite spôsobom povoleným etiketou. Šikmo strihané šaty nevešajte mokré za úzke ramienka, pretože vlastná hmotnosť môže meniť dĺžku a tvar.",
    "heat_risk": "jemné vlákna, zrnitý povrch, farbu, podšívku, záhyby a tvar šikmo strihaného dielu",
    "failure_sign": "väzba sa rozostupuje, niť je vytiahnutá, okraj strapká, podšívka ťahá vrchnú vrstvu alebo sa ozdoba uvoľňuje",
    "answer": "Šifón je ľahká, priesvitná tkanina s jemným zrnitým omakom; nie je to názov jedného vlákna. Môže byť z hodvábu, polyesteru, polyamidu, viskózy alebo zo zmesi, a preto sa nedá určiť jedna teplota či program pre všetky šifónové šaty a blúzky. Najprv prečítajte etiketu celého odevu, skontrolujte podšívku, koráliky, šikmý strih a uvoľnené nite. Ak je domáce pranie povolené, znížte trenie, použite presnú dávku kompatibilného prostriedku, mokrý kus podoprite a nekrúťte ho. Hodvábny, zdobený alebo tvarovo komplikovaný šifón môže vyžadovať ručné či profesionálne čistenie. Sušte ho mimo vysokého tepla a pri žehlení alebo naparovaní vždy rešpektujte najcitlivejšiu vrstvu.",
    "intro": "Šifónové šaty môžu vyzerať podobne, no jeden model je polyesterový a prateľný, druhý hodvábny s podšívkou a tretí má koráliky, lepené kamienky alebo šikmý strih. Priehľadnosť vedie k predstave, že ide vždy o krehký hodváb, zatiaľ čo syntetický variant zvádza k opačnej chybe: že znesie akýkoľvek program. Bezpečný postup stojí medzi týmito skratkami. Treba rozlíšiť vlákno, väzbu, zákrut priadze, strih a konštrukciu hotového odevu. Tento návod vysvetľuje, ako prať šifónovú blúzku, šaty, sukňu, šatku aj záves bez zbytočného zatrhnutia, mapy a deformácie.",
    "quick": [
        "<strong>Šifón nie je vlákno:</strong> hodvábny, polyesterový a viskózový variant majú rozdielne hranice vody, tepla a chémie.",
        "<strong>Priesvitnosť nehovorí o pevnosti:</strong> rozhoduje jemnosť priadze, hustota, zákrut, smer a švy.",
        "<strong>Šikmý strih mení mokrú stabilitu:</strong> šaty sa môžu na úzkom vešiaku vytiahnuť vlastnou hmotnosťou.",
        "<strong>Zipsy a háčiky patria mimo náplne:</strong> jedna vytiahnutá niť môže zdeformovať väčšiu plochu.",
        "<strong>Ozdoby určujú najnižší limit:</strong> koráliky, potlač, lepidlo a podšívka môžu vylúčiť práčku.",
        "<strong>Etiketa hotového odevu má prednosť:</strong> všeobecná rada pre polyester alebo hodváb nestačí.",
    ],
    "overview_heading": "Ako vzniká šifónový vzhľad a prečo nie je každý šifón rovnaký",
    "overview": [
        "Šifón sa zvyčajne opisuje ako veľmi ľahká, priesvitná tkanina s jemne zrnitým povrchom. V mnohých variantoch sa používajú vysoko zakrútené filamentové priadze v jednoduchom previazání; opačné smery zákrutu pomáhajú vytvoriť živý, mierne drsný omak a splývavosť. Fashion Institute of Technology uvádza hodvábny šifón ako vzdušnú plátnovo tkanú textíliu, čím zároveň ukazuje, že vlákno a konštrukcia sú dve samostatné informácie.",
        "Polyesterový šifón môže byť odolnejší voči vlhkosti a schnúť rýchlejšie než hodvábny, ale jemné nite, potlač a šikmý strih ostávajú citlivé na trenie a teplo. Viskózový šifón môže po namočení zoslabnúť a meniť rozmer; polyamidový variant má vlastné tepelné limity. Označenie syntetický preto nie je povolením na horúci cyklus a označenie hodvábny nie je dôkazom, že každá škvrna potrebuje rovnaké profesionálne rozpúšťadlo.",
        "Hotový odev môže mať dve alebo tri vrstvy: vrchný šifón, nepriesvitnú podšívku a výstuž pri zapínaní. Každá sa pri vode môže zmeniť inak. Ak sa jedna vrstva zrazí alebo predĺži viac, lem sa zvlní a podšívka začne ťahať. Pred čistením preto nečítajte iba zloženie vrchnej látky; prejdite celú etiketu a skontrolujte spôsob spojenia vrstiev.",
    ],
    "table1_heading": "Druhy šifónu podľa vlákna a hotového výrobku",
    "table1_intro": "Tabuľka ukazuje typické tendencie, nie náhradu etikety. Rovnaké vlákno sa môže správať inak pri odlišnej priadzi, farbe, dokončení a podšívke.",
    "table1_headers": ["Variant", "Typický prejav", "Riziko pri čistení", "Čo rozhoduje"],
    "table1_rows": [
        ("Hodvábny šifón", "Veľmi ľahký, živý, jemne zrnitý a citlivý na mokré trenie.", "Strata lesku, mapa, oslabenie farby, deformácia a poškodenie proteínového vlákna.", "Etiketa, farbenie, ozdoby a odporúčanie odbornej čistiarne."),
        ("Polyesterový šifón", "Pravidelný povrch, rýchlejšie schnutie a dobré držanie jemného skladu.", "Zatrhnutie, statický náboj, tepelná deformácia, potlač a zvyšky produktu.", "Teplotný limit, konštrukcia odevu, podšívka a povolené sušenie."),
        ("Viskózový šifón", "Mäkký pád a príjemný omak s vyšším prijímaním vlhkosti.", "Slabšia stabilita za mokra, zrazenie, vytiahnutie a krčenie.", "Presné zloženie, šetrný pohyb a rovnomerná opora pri sušení."),
        ("Zdobený alebo vrstvený šifón", "Koráliky, výšivka, potlač, riasenie a nepriesvitná podšívka.", "Rozdielna reakcia vrstiev, uvoľnenie ozdoby a lokálne ťahanie nite.", "Najcitlivejší komponent, nosné švy a odborný postup."),
    ],
    "sections": [
        {
            "heading": "Šifón, žoržet a organza nie sú to isté",
            "paragraphs": [
                "Šifón aj žoržet môžu využívať vysoko zakrútené priadze a zrnitý povrch, žoržet však býva o niečo plnší, menej priehľadný a pružnejší na dotyk. Organza je zvyčajne hladšia, tuhšia a drží objemnejší tvar. Tieto názvy sa v predaji používajú voľne, preto pohľad a dotyk nestačia na identifikáciu vlákna ani na určenie prania.",
                "Pri online nákupe si vypýtajte detailnú fotografiu, zloženie vrchnej vrstvy aj podšívky a symboly. Ak predajca uvedie iba šifónový vzhľad, považujte to za opis estetiky. Pre starostlivosť potrebujete vedieť, či je tkanina hodvábna, polyesterová alebo viskózová a či má povrchovú úpravu, ktorá sa môže vodou alebo teplom zmeniť.",
            ],
        },
        {
            "heading": "Prečo sa šifón ľahko zatrhne a posunie pri šve",
            "paragraphs": [
                f"Jemná priadza a priesvitná konštrukcia nechávajú málo materiálu na rozloženie lokálneho ťahu. Háčik, prsteň alebo drsný necht môže vytiahnuť niť a posunúť susedné preväzby. Článok <a href=\"{ARTICLE_SNAGGING}\">prečo vznikajú vytiahnuté očká</a> vysvetľuje rozdiel medzi zachytením a pretrhnutím. Pri šifóne niť neodstrihujte pri líci, kým neviete, kam pokračuje.",
                "Rozostúpenie pri šve nemusí znamenať, že steh praskol. Látka sa môže okolo ihlových otvorov posunúť, najmä pri tesnom strihu alebo malom prídavku. Pred ďalším praním uvoľnite napätie a nechajte miesto opraviť v zdravej ploche. Husté prešitie cez oslabenú tkaninu môže vytvoriť perforovanú líniu a problém zhoršiť.",
            ],
        },
        {
            "heading": "Šikmý strih, splývavosť a zmena dĺžky",
            "paragraphs": [
                "Diel vystrihnutý šikmo voči osnove a útku sa ľahšie prispôsobuje telu a elegantne splýva. Zároveň sa môže pod vlastnou hmotnosťou predĺžiť, najmä za mokra. Nerovný lem po čistení preto nemusí byť iba zrazenie; môže ísť o rozdielne vyvesenie dielov, podšívky a švov. Odev merajte až po úplnom vyschnutí a ustálení bez násilného naťahovania.",
                "Mokré šaty nevešajte za tenké ramienka ani za jeden bod. Rozložte hmotnosť podľa etikety, podoprite pás a ramená alebo sušte naplocho. Pri novom drahšom modeli si pred prvým čistením odmerajte dĺžku na viacerých miestach. Ak sa pri správnom postupe výrazne zmení iba jeden diel, zdokumentujte stav a riešte ho s predajcom.",
            ],
        },
        {
            "heading": "Ako prať šifónové šaty a sukňu",
            "paragraphs": [
                "Najprv prezrite podpazušie, spodný lem, zapínanie, riasenie a spojenie s podšívkou. Zapnite hladký zips, ostrý háčik prekryte alebo odev perte oddelene spôsobom povoleným etiketou. Pri ručnom praní použite veľkú nádobu, aby sa šaty nemuseli skladať do tesného balíka. Vodu cez látku jemne pretláčajte bez drhnutia a krútenia.",
                "Ak je povolená práčka, zvoľte jemný cyklus, primerane nízke otáčky a dostatočne veľké ochranné vrecko bez hrubého zipsu. Vrecko nezmení nevhodný program na bezpečný. Šaty perte s ľahkými hladkými kusmi, nie s rifľami, uterákmi alebo podprsenkou s odkrytým háčikom. Po cykle ich vyberte hneď a mokrú hmotnosť podoprite.",
            ],
        },
        {
            "heading": "Ako prať šifónovú blúzku, šatku a záves",
            "paragraphs": [
                "Blúzka sa najviac znečistí pri golieri, manžetách a v podpazuší. Miesto odsajte, prostriedok otestujte z rubu a netrite jemnú plochu proti sebe. Šatka môže mať ručne rolovaný lem, ktorý určuje nižší limit než stred látky. Po čistení ju podoprite po celej dĺžke a nesušte uviazanú v uzle.",
                "Šifónový záves môže byť polyesterový, ale veľký rozmer a dlhodobé svetlo menia jeho pevnosť. Pred praním povysávajte alebo jemne odstráňte prach, skontrolujte háčiky a oslabnuté okraje. Starší svetlom degradovaný záves sa môže vo vode roztrhnúť aj pri jemnom programe. Ak pri manipulácii práši alebo praská, domáce pranie zastavte.",
            ],
        },
        {
            "heading": "Make-up, parfum, mastnota a vodné mapy",
            "paragraphs": [
                "Make-up a mastnota na priehľadnej vrstve vytvoria viditeľný tieň. Najprv odsajte prebytok bielou savou handričkou a ošetrujte od okraja ku stredu. Nekvapkajte koncentrovaný prostriedok priamo na hodvábny alebo neznámy šifón. Rozdiel zmáčania môže po vysušení vytvoriť okraj, preto treba lokálny postup vždy skúsiť na skrytom mieste.",
                "Parfum alebo lak na vlasy obsahuje zmes prchavých látok a môže meniť farbu či povrch, preto ho nestriekajte priamo na oblečené šifónové šaty. Starú škvrnu nefixujte žehličkou. Ak je vrstva hodvábna, zdobená alebo nestálofarebná, experiment s alkoholom či rozpúšťadlom je neprimerané riziko a vhodnejšia je skúsená čistiareň.",
            ],
        },
        {
            "heading": "Sušenie, naparovanie a žehlenie šifónu",
            "paragraphs": [
                "Po oplachu nechajte vodu odtiecť, kus podoprite medzi čistými uterákmi bez krútenia a uložte ho do tvaru. Pri vešaní použite širokú oporu a zabráňte tomu, aby sa mokrý lem dotýkal podlahy. Priamy slnečný svit môže meniť farbu a vysoké teplo poškodiť syntetické vlákno alebo podšívku.",
                f"Naparovanie môže uvoľniť záhyb, no vzdialenosť, teplotu a vlhkosť musí povoľovať etiketa. Kvapky z naparovača môžu na citlivej farbe vytvoriť mapu. Žehličku používajte z rubu cez čistú ochrannú tkaninu a bez silného tlaku, ktorý sploští textúru. Všeobecné princípy nájdete v návode <a href=\"{ARTICLE_IRONING}\">ako bezpečne žehliť oblečenie</a>.",
            ],
            "callout": {
                "title": "Rýchla kontrola pred naparovaním",
                "items": [
                    "Povoľuje etiketa žehlenie alebo naparovanie vrchnej vrstvy aj podšívky?",
                    "Je nádržka čistá a naparovač nevypúšťa kvapky alebo usadeniny?",
                    "Môžete postup najprv vyskúšať na skrytom leme bez napínania látky?",
                    "Drží odev rovnomerne, alebo visí celou mokrou hmotnosťou na úzkom ramienku?",
                ],
                "background": "#f7fbf8",
                "border": "#dbe5de",
            },
        },
        {
            "heading": "Ako uložiť šifón bez vytiahnutých nití a ostrých skladov",
            "paragraphs": [
                "Čisté suché šaty zaveste na polstrovaný alebo dostatočne široký vešiak, ak ich strih a hmotnosť vešanie povoľujú. Ťažko zdobený alebo šikmo strihaný odev môže byť bezpečnejšie uložiť naplocho s mäkkými prekladmi. Zipsy, koráliky a háčiky susedných šiat oddeľte hladkým obalom, ktorý nefarbí a prepúšťa vzduch.",
                "Šatku neskladujte dlhodobo v pevnom uzle. Záhyby občas presuňte, najmä pri hodvábnom alebo staršom kuse. Plastový vak môže uzavrieť vlhkosť a vytvoriť tlak na ozdoby. Pred sezónnym uložením skontrolujte zvyšky parfumu, make-upu a potu, pretože časom môžu meniť farbu a pri ďalšom čistení sa odstraňujú ťažšie.",
            ],
        },
    ],
    "table2_heading": "Šifón po praní alebo nosení: diagnostická tabuľka",
    "table2_intro": "Pred opakovaným praním rozlíšte usadeninu, zmenu farby, vytiahnutú niť a deformáciu strihu. Každý prejav potrebuje iný ďalší krok.",
    "table2_headers": ["Prejav", "Možná príčina", "Čo overiť", "Bezpečný ďalší krok"],
    "table2_rows": [
        ("Vytiahnutá dlhá niť", "Háčik, šperk, zips alebo lokálny ťah v otvorenej väzbe.", "Či je niť pretrhnutá a kam pokračuje na rubovej strane.", "Neodstrihnúť pri líci; uvoľniť napätie a nechať odborne zatiahnuť."),
        ("Lem je po vysušení nerovný", "Šikmý strih, vytiahnutie za mokra alebo rozdielna zmena podšívky.", "Dĺžku po úplnom ustálení, smer dielov a spôsob sušenia.", "Odev nenapínať; pri novom kuse zdokumentovať a posúdiť úpravu."),
        ("Svetlá alebo tmavá mapa", "Nerovnomerné zmáčanie, zvyšok prostriedku, farbivo alebo poškodenie povrchu.", "Skúšku farby, oplach, použitú chémiu a hranicu mapy.", "Nedrhnúť; pri hodvábe alebo nestálej farbe zvoliť odborné čistenie."),
        ("Vrstva sa rozostupuje pri šve", "Posun nití, príliš tesný strih alebo oslabená tkanina.", "Stav stehu, dierky po ihle a zdravú plochu okolo.", "Pred ďalším praním opraviť s rozložením ťahu do širšej plochy."),
        ("Povrch je tuhý alebo elektrizuje", "Zvyšky produktu, presušenie, syntetické vlákno alebo statický náboj.", "Dávku, oplach, vlhkosť prostredia a povolené doplnky.", "Opraviť dávkovanie a sušenie; nepridávať náhodný zmäkčovač na citlivý kus."),
    ],
    "steps_heading": "Ako bezpečne vyprať šifón krok za krokom",
    "steps": [
        "Prečítajte zloženie a symboly celého odevu a skontrolujte podšívku, ozdoby, švy a šikmý strih.",
        "Odstráňte šperky a drsné predmety, zatvorte bezpečné zapínanie a otestujte farbu aj lokálny prostriedok.",
        "Zvoľte iba výrobcom povolené ručné alebo strojové pranie; jemný názov programu sám osebe nestačí.",
        "Použite kompatibilný prostriedok v presnej dávke a šifón netrite, nekrúťte ani nestláčajte v tesnom balíku.",
        "Oplachujte bez prudkých zmien a mokrý odev vyberte s oporou viacerých miest.",
        "Sušte naplocho alebo na širokej opore podľa etikety, mimo radiátora, horúcej sušičky a priameho slnka.",
        "Žehlite alebo naparujte len pri povolení, z rubu, cez ochrannú tkaninu a po skúške na skrytom mieste.",
    ],
    "remember": [
        "Aké vlákno tvorí vrchný šifón a aké podšívku, nite, výšivku a ozdoby?",
        "Je odev strihaný šikmo a môže sa za mokra vytiahnuť vlastnou hmotnosťou?",
        "Povoľuje etiketa vodu, práčku, odstreďovanie, sušičku, žehlenie alebo profesionálne čistenie?",
        "Sú zipsy a háčiky zakryté a náplň bez uterákov, riflí a drsných aplikácií?",
        "Je použitý prostriedok kompatibilný s hodvábom, viskózou alebo syntetikou konkrétneho kusu?",
        "Má mokrý šifón rovnomernú oporu a podšívka ho nikde neťahá?",
    ],
    "mistakes": [
        "Predpokladať, že každý šifón je polyesterový alebo naopak vždy hodvábny.",
        "Prať jemné šaty s otvorenými zipsami, háčikmi, uterákmi alebo ťažkými nohavicami.",
        "Krútiť mokrú vrstvu, vyvesiť šikmo strihané šaty za ramienka a merať ich ešte vlhké.",
        "Nastriekať parfum alebo lak priamo na priehľadnú látku a škvrnu potom fixovať teplom.",
        "Žehliť z líca vysokou teplotou bez kontroly podšívky a povrchu.",
        "Odstrihnúť vytiahnutú niť pri povrchu bez zistenia smeru a rozsahu deformácie.",
    ],
    "expert_heading": "Odbornejší pohľad: jemná väzba, zákrut a merateľné riziká",
    "expert": [
        "Fashion Institute of Technology opisuje hodvábny šifón ako vzdušnú plátnovo tkanú textíliu, ktorá sa od tela odnáša ľahkým objemom. CottonWorks vysvetľuje plátnovú väzbu ako časté striedanie osnovy a útku. Ani jeden opis však neurčuje konkrétnu pevnosť neznámeho šifónu; jemnosť priadze, hustota, vysoký zákrut, farbenie a dokončenie môžu výsledok výrazne meniť.",
        "ASTM D1424 meria silu potrebnú na pokračovanie už založenej trhliny pri definovanom prístroji a upozorňuje na hranice použitia pre niektoré textílie. ASTM D3939 hodnotí zachytávanie v kontrolovaných podmienkach a uvádza, že otvorené konštrukcie môžu potrebovať inú metódu. Z toho vyplýva, že domáci ťah za okraj ani počet vytiahnutých nití na jedných šatách nie sú univerzálnou skúškou kvality.",
        "AATCC TM135 opisuje rozmerovú zmenu textílií po definovanom domácom praní. Šikmo strihaný odev však môže zmeniť zavesenie aj bez rovnakej zmeny osnovy a útku, pričom podšívka pridáva druhý materiál. Spotrebiteľ by preto mal merať suchý ustálený odev na rovnakých bodoch, zaznamenať postup a nerozlišovať výrobnú konštrukciu od zrazenia iba podľa jedného lemu.",
    ],
    "source_intro": "Zdroje podporujú opis ľahkej plátnovej konštrukcie, rozdiel medzi normovaným trhaním a zachytávaním a potrebu čítať symboly hotového výrobku. Nepodporujú jednu teplotu alebo program pre každý šifón.",
    "sources": [
        ("Fashion Institute of Technology: šifón v kontexte vlákna a konštrukcie", FIT_FABRIC),
        ("CottonWorks: základné tkané väzby", COTTONWORKS_WOVEN),
        ("ASTM D1424-25: pokračovanie trhliny v tkanine", ASTM_TEAR),
        ("ASTM D3939/D3939M-26: odolnosť proti zachyteniu", ASTM_SNAG),
        ("AATCC TM135-2025: rozmerové zmeny po domácom praní", AATCC_DIMENSION),
        ("EÚ 1007/2011: označovanie textilných vlákien", EU_FIBRE_LABEL),
        ("GINETEX: význam symbolov ošetrovania", GINETEX),
    ],
    "product_intro": "Pri bežnom prateľnom polyesterovom alebo inom vhodnom šifóne rozhoduje etiketa, jemná mechanika, presná dávka a čistý oplach. Hodvábny, vlnený, viskózový, zdobený alebo profesionálne čistený kus môže potrebovať odlišný produkt.",
    "product_text": "Hypoalergénny prací gél z marseillského mydla je určený na bežnú domácu bielizeň. Na šifón ho použite iba vtedy, keď je kompatibilný s presným vláknom a etiketa hotového odevu povoľuje bežný prací gél.",
    "product_limit": "Produkt nie je automatickým prostriedkom na hodváb, vlnu, nestálofarebný textil, koráliky, lepené ozdoby ani odev určený na profesionálne čistenie. Neopraví vytiahnutú niť, posun pri šve ani deformovaný šikmý strih.",
    "category_intro": "Prací gél pre šifón vyberajte až po určení vlákna a konštrukcie odevu. Pri jemnej tkanine nie je cieľom najsilnejší zásah, ale odstránenie nečistoty bez zvyškov a nadbytočného trenia.",
    "category_text": "V kategórii môžete porovnať gély pre bežnú domácu bielizeň. Každý variant overte podľa zloženia, farby, podšívky a symbolov konkrétneho šifónového výrobku; pri hodvábe alebo odbornom čistení zvoľte určené riešenie.",
    "related": [
        ("Ako predchádzať zatrhávaniu textilu", ARTICLE_SNAGGING),
        ("Čo je polyester a ako ho prať", ARTICLE_POLYESTER),
        ("Čo je viskóza a ako sa o ňu starať", ARTICLE_VISCOSE),
        ("Prečo sa oblečenie zráža", ARTICLE_SHRINKAGE),
        ("Ako čítať štítok na oblečení", ARTICLE_LABEL),
        ("Stálofarebnosť textilu", ARTICLE_COLOR),
    ],
    "faq_title": "šifón a jeho starostlivosť",
    "faq": [
        ("Je šifón vždy z hodvábu?", "Nie. Môže byť z hodvábu, polyesteru, polyamidu, viskózy alebo zo zmesi. Presné vlákno určuje etiketa, nie vzhľad."),
        ("Môže sa šifón prať v práčke?", "Iba ak to povoľuje etiketa celého odevu. Podšívka, koráliky, šikmý strih alebo citlivé farbenie môžu vyžadovať ručné či profesionálne čistenie."),
        ("Na koľko stupňov prať šifón?", "Univerzálna teplota neexistuje. Riaďte sa symbolmi a najcitlivejšou vrstvou; syntetický a hodvábny šifón nemajú rovnaké hranice."),
        ("Ako vyprať šifónové šaty?", "Skontrolujte podšívku a ozdoby, znížte trenie, použite povolený jemný postup a mokré šaty podoprite. Šikmý strih nevešajte za úzke ramienka."),
        ("Ako vyžehliť šifón?", "Len pri povolení etikety, z rubu, cez ochrannú tkaninu a s nízkou vhodnou teplotou. Najprv skúste skrytý lem a netlačte na zrnitý povrch."),
        ("Čo robiť s vytiahnutou niťou?", "Niť neodstrihujte pri líci. Uvoľnite napätie, zistite smer a pri hodnotnom kuse nechajte niť odborne zatiahnuť alebo miesto opraviť."),
        ("Prečo sa šifónové šaty po praní predĺžili?", "Príčinou môže byť šikmý strih, mokrá hmotnosť, zmena podšívky alebo rozmerová zmena. Hodnoťte ich až úplne suché a ustálené."),
        ("Môže ísť šifón do sušičky?", "Len pri výslovnom symbole. Teplo a prevaľovanie môžu poškodiť jemnú väzbu, syntetické vlákno, podšívku aj ozdoby."),
    ],
}


BOUCLE: dict[str, object] = {
    "title": "Čo je buklé: slučkový povrch, žmolky a šetrné čistenie",
    "link": "co-je-bukle-sluckovy-povrch-zmolky-a-setrne-cistenie",
    "meta": "Čo je buklé, ako vzniká jeho slučkový povrch, aký je rozdiel medzi buklé priadzou, látkou a úpletom a ako čistiť kabát, sako či poťah.",
    "short": "Buklé označuje efektnú slučkovú priadzu alebo textil s nepravidelným kučeravým povrchom. Zistite, ako odlíšiť prach, žmolky a vytiahnutú slučku a ako čistiť odev bez sploštenia textúry.",
    "name": "buklé",
    "genitive": "buklé",
    "construction_summary": "efektnú priadzu s vystupujúcimi slučkami alebo tkaný či pletený povrch, ktorý z nej vznikol; nejde o jedno vlákno ani jednotnú väzbu",
    "label_details": "vláknové zloženie efektnej aj nosnej priadze, tkanú alebo pletenú konštrukciu, podšívku, výstuž, strapce, gombíky a pri čalúnení odnímateľnosť poťahu",
    "residue_place": "slučkách, medzerách medzi efektnými priadzami, švoch, strapcoch a vrstve podšívky",
    "friction_risk": "vystupujúcu slučku, voľný uzlík, strapec alebo jemnú nosnú priadzu medzi objemnými efektmi",
    "drying_advice": "Odev podoprite podľa hmotnosti a tvaru, slučky nestláčajte pod štipcom ani úzkou hranou a po vysušení ich upravte iba jemnou rukou alebo vhodnou kefou podľa výrobcu. Čalúnenie nesmie zostať vlhké vo výplni.",
    "heat_risk": "pružnosť nosnej priadze, plstenie vlny, tvar slučiek, podšívku, výstuž a lepidlá v čalúnení",
    "failure_sign": "slučka je pretrhnutá alebo vytiahnutá do dlhej nite, väzba redne, šev sa rozostupuje alebo sa podšívka a výstuž deformujú",
    "answer": "Buklé je názov efektnej slučkovej priadze alebo textilu s nepravidelným kučeravým povrchom; nie je to jedno vlákno. Môže byť vlnené, bavlnené, akrylové, polyesterové aj zmesové a môže ísť o tkaninu, úplet či čalúnnický poťah. Pred čistením odlíšte prach a cudzie vlákna od žmolkov a vytiahnutej slučky. Voľné nečistoty najprv odstráňte šetrne nasucho, nič nevyťahujte ani neodstrihujte pri povrchu a pri odeve skontrolujte podšívku a výstuž. Domáce pranie použite iba pri povolení etikety, s nízkym trením a presnou dávkou kompatibilného prostriedku. Kabát, sako, vlnené buklé a pevné čalúnenie často potrebujú odborný alebo výrobcom určený postup. Po čistení textúru nestláčajte a celý predmet dôkladne vysušte.",
    "intro": "Buklé sa často kupuje práve pre povrch, ktorý pri nesprávnom čistení trpí ako prvý. Slučky zachytávajú prach, chlpy a nite z inej bielizne, ostrý predmet z nich môže vytiahnuť dlhý úsek a silný tlak ich sploští. Zároveň sa pod rovnakým názvom predáva ľahký pletený kardigán, štruktúrované sako s výstužou, vlnený kabát aj pevná poťahová látka. Ich údržba sa nemôže zjednotiť na jeden program. Praktický návod preto začína konštrukciou, pokračuje rozpoznaním povrchového problému a až potom vyberá vodu, prostriedok, sušenie alebo odborné čistenie.",
    "quick": [
        "<strong>Buklé je priadza alebo povrch, nie vlákno:</strong> presné percentá na etikete určujú reakciu na vodu a teplo.",
        "<strong>Slučka nie je automaticky žmolok:</strong> vytiahnutú konštrukčnú niť nemožno bezpečne oholiť ako voľný chumáč.",
        "<strong>Prach odstráňte pred vodou:</strong> jemné povrchové čistenie zníži množstvo častíc zachytených medzi slučkami.",
        "<strong>Kabát a sako majú vnútornú stavbu:</strong> podšívka, výstuž a tvar môžu vylúčiť domácu práčku.",
        "<strong>Poťah nie je vždy snímateľný:</strong> mokré čalúnenie musí vyschnúť aj vo vnútri bez máp a plesní.",
        "<strong>Trenie je hlavné riziko:</strong> zipsy, suchý zips, pazúriky a drsné povrchy oddeľte od slučiek.",
    ],
    "overview_heading": "Čo znamená buklé priadza, buklé látka a buklé úplet",
    "overview": [
        "CottonWorks definuje bouclé ako drsnú, kučeravú, uzlíkovú efektnú priadzu vytvorenú kombináciou jemnejších a hrubšej silno zakrútenej priadze podávanej rozdielnou rýchlosťou. V učebnom materiáli o priadzach opisuje jadro, efekt a väznú zložku, pričom na povrchu vystupujú nepravidelné slučky. Tento opis vysvetľuje typický vzhľad, nie presnú receptúru každého moderného výrobku.",
        "Buklé priadza sa môže zatkať do látky alebo zapliesť do úpletu. Tkané sako býva rozmerovo stabilnejšie, no môže mať voľnú väzbu, podšívku a výstuž. Pletený kardigán sa viac prispôsobí, ale môže sa vyťahovať a zachytiť. Čalúnnická látka využíva robustnejšiu konštrukciu, niekedy podklad, záter alebo lepidlo, ktoré určujú úplne iný spôsob čistenia.",
        "Názov sa dnes používa aj pre textílie, ktoré slučkový efekt iba napodobňujú. Niektoré majú kompaktnú syntetickú priadzu, iné zmes vlny a akrylu, ďalšie bavlnený efekt na polyesterovom základe. Dotyk a fotografia nestačia na predpoveď žmolkovania, zrazenia alebo stálofarebnosti. Potrebujete zloženie, konštrukciu, určenie a návod hotového výrobku.",
    ],
    "table1_heading": "Buklé podľa typu výrobku a konštrukcie",
    "table1_intro": "Rovnaký slučkový vzhľad môže skrývať odlišnú nosnú konštrukciu. Tabuľka pomáha určiť, čo treba preveriť pred čistením.",
    "table1_headers": ["Typ", "Ako drží tvar", "Typické nečistoty", "Najväčšie riziko"],
    "table1_rows": [
        ("Tkané buklé sako", "Tkanina, podšívka, výstuž, švy a tvarovanie ramien.", "Prach, make-up pri golieri, pot a lokálna mastnota.", "Zrazenie vrstiev, zvlnenie výstuže a vytiahnutie slučky."),
        ("Vlnený buklé kabát", "Objemná priadza, hustejšia tkanina, podšívka a konštrukcia kabáta.", "Prach, cestná špina, vlhkosť, chlpy a pach zo skladovania.", "Plstenie, zmena rozmeru, sploštenie povrchu a strata tvaru."),
        ("Pletený buklé sveter", "Očká úpletu a efektná priadza, často bez pevnej výstuže.", "Pot, kozmetika, cudzie vlákna a trenie pri nosení.", "Vyťahanie za mokra, zachytenie slučky a žmolkovanie."),
        ("Buklé poťah", "Pevná tkanina alebo úplet na podklade, niekedy záter a výplň nábytku.", "Prach, omrvinky, kožný maz, nápoj a zvieracie chlpy.", "Premočenie výplne, mapa, odlepenie podkladu a dlhé schnutie."),
    ],
    "sections": [
        {
            "heading": "Buklé verzus teddy, sherpa a ženilka",
            "paragraphs": [
                "Teddy a sherpa opisujú mäkký kožušinový alebo rúnový vzhľad, často vytvorený vlasom či pletenou slučkou. Ženilka používa priadzu s krátkymi vystupujúcimi vláknami okolo jadra. Buklé má typicky nepravidelné slučky a uzlíky efektnej priadze. V maloobchode sa názvy môžu miešať, preto vizuálna podobnosť nie je dôkazom rovnakej konštrukcie ani údržby.",
                "Pri rozlišovaní sa pozrite z rubu, jemne oddeľte susedné priadze a zistite, či povrch tvorí slučka, strihaný vlas alebo chlpatá priadza. Nerobte to silou na viditeľnom mieste. Zistenie pomôže rozpoznať vytiahnutú konštrukčnú slučku, ale neurčí vlákno. Presné zloženie a symboly stále treba čítať na etikete.",
            ],
        },
        {
            "heading": "Prach, chlpy a omrvinky odstráňte pred praním",
            "paragraphs": [
                "Slučkový povrch zachytí častice, ktoré na hladkej látke ľahko skĺznu. Odev najprv jemne vytraste tam, kde sa prach nerozptýli do miestnosti, a použite valček s primeranou priľnavosťou alebo mäkkú kefku v smere povrchu. Agresívna lepiaca páska môže vytiahnuť voľnú slučku a rotačná kefa vysávača poškodiť niť.",
                "Pri čalúnení použite nízky výkon a hubicu chránenú hladkou mriežkou, ak to výrobca povoľuje. Hubicu nepritláčajte a nepohybujte ňou proti slučkám. Jemné odstránenie prachu obmedzí abrazívne častice; povrchové čistenie má však zmysel iba na pevnom materiáli bez uvoľnených nití.",
            ],
        },
        {
            "heading": "Žmolok, chumáč a vytiahnutá slučka sú tri rozdielne veci",
            "paragraphs": [
                f"Žmolok vzniká spletením uvoľnených vlákien na povrchu, chumáč môže byť cudzie vlákno a vytiahnutá slučka je časť konštrukčnej priadze. Mechanizmus žmolkovania podrobne vysvetľuje článok <a href=\"{ARTICLE_PILLING}\">prečo sa oblečenie žmolkuje</a>. Pri buklé je dôležité nevziať holiaci strojček na každú nerovnosť, pretože môže prerezať slučku, ktorá drží väčšiu časť povrchu.",
                "Povrch prezrite pod bočným svetlom. Voľný chumáč sa často dá jemne zdvihnúť bez pohybu okolitých priadzí; žmolok je zhluk krátkych vlákien; slučka pokračuje do textilu a pri potiahnutí deformuje okolie. Ak si nie ste istí, nič nestrihajte. Dlhú slučku uvoľnite bez napätia a nechajte ju zatiahnuť z rubu odborníkom.",
            ],
            "callout": {
                "title": "Čo nikdy neodstrihovať naslepo",
                "items": [
                    "Dlhú slučku, ktorá pri jemnom pohybe ťahá susedné priadze.",
                    "Uzol alebo väznú niť, ktorá môže stabilizovať efektnú priadzu.",
                    "Niť vychádzajúcu priamo zo šva, lemu alebo miesta pri gombíku.",
                    "Nerovnosť na novom kuse pred fotografiou a posúdením reklamácie.",
                ],
                "background": "#f7fbf8",
                "border": "#dbe5de",
            },
        },
        {
            "heading": "Ako čistiť buklé sako a kabát",
            "paragraphs": [
                "Sako a kabát prezrite zvonku aj zvnútra. Skontrolujte ramenné výplne, lepenú výstuž, podšívku, gombíky, vrecká a záložky. Aj keď vrchná buklé látka obsahuje syntetiku, vnútorná stavba môže vodu vylučovať. Ak etiketa uvádza profesionálne čistenie, domáci jemný program nie je rovnocenná náhrada.",
                "Medzi čisteniami pomáha vetranie, jemné odstránenie prachu a lokálne odsatie čerstvej škvrny bez rozširovania. Mokré miesto nedrhnite kefou, pretože slučky sa môžu sploštiť alebo vytiahnuť. Po daždi kabát zaveste na širokú oporu, urovnajte bez silného ťahania a nechajte vyschnúť mimo radiátora pred uložením do skrine.",
            ],
        },
        {
            "heading": "Ako prať buklé sveter alebo kardigán",
            "paragraphs": [
                "Ak etiketa povoľuje vodu, rozlíšte vlnu, bavlnu, akryl, polyester a zmes. Vlnený úplet môže plstnatieť pri kombinácii teploty, mechaniky a nevhodnej chémie; viskózová zložka môže za mokra slabnúť; syntetický efekt sa môže teplom deformovať. Gombíky zatvorte len vtedy, ak tým nevznikne napätie, a voľné pásy či šnúrky zabezpečte.",
                "Ručné pranie robte v nádobe, kde kus leží bez tesného skladania. Vodu jemne pretláčajte, nevytvárajte penu prudkým trením a úplet nezdvíhajte za rukáv. Po oplachu ho podoprite, prebytok vody odsajte do uteráka bez krútenia a sušte naplocho v rozmere. Vešiak môže mokrý sveter predĺžiť a vytvoriť výčnelky na ramenách.",
            ],
        },
        {
            "heading": "Ako vyčistiť buklé sedačku a kreslo",
            "paragraphs": [
                "Najprv nájdite pokyny výrobcu nábytku a kód čistenia poťahu. Zistite, či je poťah snímateľný a prateľný; zips na spodnej strane automaticky neznamená, že výrobca povoľuje pranie. Výplň, podklad, lepidlo a protipožiarna úprava môžu mať vlastné limity. Pri záruke nevykonávajte neoverený mokrý zásah.",
                "Tekutinu čo najskôr odsajte bielou savou handričkou bez trenia. Pracujte od okraja a nepremočte plochu tak, aby voda prenikla do peny. Malý skrytý test kontrolujte až po úplnom vysušení, pretože mapa alebo stuhnutie sa môžu objaviť neskôr. Veľkú mastnú škvrnu, neznámy pigment alebo hlboké premočenie zverte odbornému čisteniu čalúnenia.",
            ],
        },
        {
            "heading": "Trenie pri nosení a v domácnosti",
            "paragraphs": [
                "Popruh kabelky, bezpečnostný pás, hrana stola a opierka stoličky opakovane stláčajú rovnakú plochu. Na saku sa preto povrch môže uhladiť alebo žmolkovať skôr na boku, lakti a pod pazuchou. Pranie mechanickú zmenu nevráti. Znížte zdroj trenia a povrch neupravujte agresívnym kartáčom, ktorý by vytvoril inú textúru než okolie.",
                "Na sedačke pôsobia švy riflí, zipsy, pazúriky a detské hračky. Ochranná deka môže pomôcť na často používanom mieste, ale jej drsný rub nesmie slučky ďalej zachytávať. Pravidelne odstráňte piesok a omrvinky, ktoré fungujú ako abrazívne častice. Vytiahnutú slučku riešte skôr, než sa zachytí znova.",
            ],
        },
        {
            "heading": "Sušenie a obnova povrchu bez sploštenia",
            "paragraphs": [
                "Po povolenom praní odev rozložte do tvaru a zaistite prúdenie vzduchu. Slučky nestláčajte štipcami ani pod ťažkou mokrou vrstvou. Kabát podoprite širokým vešiakom, sveter sušte naplocho a snímateľný poťah nasaďte späť len podľa pokynov výrobcu. Predčasné nasadenie môže uzavrieť vlhkosť vo vnútri nábytku.",
                "Povrch upravujte až úplne suchý. Prstami alebo veľmi jemnou vhodnou kefou možno urovnať lokálne pritlačené slučky, ale silné česanie zmení smer a vytiahne priadzu. Para môže ovplyvniť vlnu, syntetiku, výstuž aj lepidlo; nepoužívajte ju bez povolenia. Cieľom nie je vytvoriť dokonale rovný vlas, pretože nepravidelnosť patrí k buklé.",
            ],
        },
    ],
    "table2_heading": "Buklé po čistení alebo nosení: čo príznak znamená",
    "table2_intro": "Slučkový povrch môže zakryť rozdiel medzi cudzím vláknom a konštrukčnou chybou. Pred zásahom si pomôžte bočným svetlom a kontrolou z rubu.",
    "table2_headers": ["Príznak", "Možná príčina", "Čo overiť", "Ďalší krok"],
    "table2_rows": [
        ("Dlhá vytiahnutá slučka", "Zachytenie o šperk, zips, pazúr alebo drsnú hranu.", "Či niť pokračuje do väzby a deformuje okolie.", "Nestrihať; uvoľniť napätie a zatiahnuť z rubu odborným spôsobom."),
        ("Malé pevné chumáče", "Žmolky z povrchových vlákien alebo cudzie vlákna z inej textílie.", "Či sa pri pohybe hýbe aj konštrukčná slučka.", "Cudzie vlákno odstrániť jemne; žmolok riešiť iba vhodným nástrojom."),
        ("Sploštená lesklá plocha", "Tlak, trenie, horúca para alebo žehlenie.", "Miesto kontaktu, zloženie a povolenú tepelnú údržbu.", "Nepridávať ďalšie teplo; po vysušení jemne urovnať podľa návodu."),
        ("Poťah má mapu", "Nerovnomerné premočenie, zvyšok čističa alebo presun nečistoty.", "Hranicu mapy, podklad, skúšku farby a vlhkosť výplne.", "Zastaviť lokálne experimenty a zvoliť odborné čistenie celej zóny."),
        ("Sako stratilo tvar", "Rozdielne zrazenie vrchnej látky, podšívky a výstuže alebo nevhodné sušenie.", "Ramenné výplne, klopy, podšívku a etiketu.", "Nenaprávať ďalším praním; zveriť krajčírovi alebo čistiarni."),
    ],
    "steps_heading": "Bezpečný postup čistenia buklé krok za krokom",
    "steps": [
        "Určite, či ide o tkaný odev, úplet, snímateľný poťah alebo pevné čalúnenie, a prečítajte celý návod.",
        "Pri bočnom svetle skontrolujte slučky, švy, podšívku, výstuž a odlíšte prach, žmolok a vytiahnutú niť.",
        "Voľné častice odstráňte šetrne nasucho bez rotačnej kefy, agresívnej pásky a ťahania slučiek.",
        "Lokálny prostriedok otestujte na skrytom mieste a po teste počkajte na úplné vysušenie.",
        "Domáce pranie použite iba pri povolení etikety, s kompatibilným produktom, nízkym trením a voľnou náplňou.",
        "Mokrý odev podoprite, nekrúťte ho a sušte podľa konštrukcie bez radiátora, tlaku a ostrých štipcov.",
        "Povrch upravujte až suchý a konštrukčnú slučku nikdy neodstrihujte naslepo pri líci.",
    ],
    "remember": [
        "Je buklé tkanina, úplet, efektná priadza v zmesi alebo poťah s podkladom?",
        "Aké vlákna tvoria nosnú a efektnú priadzu a čo povoľuje etiketa celého výrobku?",
        "Ide o voľný prach, cudzie vlákno, žmolok alebo vytiahnutú konštrukčnú slučku?",
        "Obsahuje odev podšívku, výstuž a tvarové diely, ktoré sa nesmú namočiť?",
        "Je poťah skutočne snímateľný a prateľný podľa výrobcu, alebo má iba servisný zips?",
        "Môže celý predmet po čistení rýchlo a rovnomerne vyschnúť aj vo vnútri?",
    ],
    "mistakes": [
        "Oholiť alebo odstrihnúť každú nerovnosť bez rozlíšenia žmolku a konštrukčnej slučky.",
        "Prať buklé s uterákmi, otvorenými zipsami, suchými zipsami a textíliami, ktoré púšťajú vlákna.",
        "Vložiť sako alebo kabát do práčky iba podľa zloženia vrchnej látky a ignorovať výstuž.",
        "Premočiť sedačku a hodnotiť výsledok skôr, než vyschne poťah, podklad aj výplň.",
        "Sušiť vlnené alebo syntetické slučky na radiátore a následne ich rozčesávať silou.",
        "Prekrývať vlhkosť a nečistotu parfumom namiesto odstránenia zdroja a úplného vysušenia.",
    ],
    "expert_heading": "Odbornejší pohľad: efektná priadza, žmolkovanie a zachytávanie",
    "expert": [
        "CottonWorks opisuje buklé ako efektnú priadzu s jadrom, efektom a väznou zložkou, pri ktorej na povrchu vystupujú nepravidelné slučky a špirály. To vysvetľuje, prečo jedna vytiahnutá slučka môže súvisieť s dlhším úsekom priadze. Konkrétny výrobok však môže používať inú modernú konštrukciu, preto definícia priadze nenahrádza mikroskopický rozbor ani etiketu.",
        "ASTM D3512 uvádza, že žmolkovanie ovplyvňuje typ a rozmer vlákna, priadza, konštrukcia, dokončenie aj spôsob údržby a hodnotí sa vizuálnou stupnicou v definovanom prístroji. ASTM D3939 skúša odolnosť proti zachytávaniu a zároveň upozorňuje, že výsledok ovplyvňuje vzhľad, kontrast a typ konštrukcie. Domáce spočítanie slučiek preto nie je priamo porovnateľné laboratórne skóre.",
        "Pri čalúnení sa povrchová textília stáva súčasťou systému s podkladom, švom a výplňou. Voda môže preniesť rozpustenú nečistotu do okraja škvrny a pomalé schnutie zasiahnuť vnútro. Bez znalosti kódu údržby a podkladu nemožno odporučiť univerzálny extrakčný postup. Najbezpečnejšie je začať suchým odstránením častíc a mokrý zásah obmedziť na výrobcom povolenú metódu.",
    ],
    "source_intro": "Zdroje podporujú konštrukčný opis buklé priadze, rozdiel medzi žmolkovaním a zachytávaním a opatrnosť pri čistení citlivého textilu. Nepodporujú jednu metódu pre odev, úplet a čalúnenie.",
    "sources": [
        ("CottonWorks: definícia buklé priadze", COTTONWORKS_BOUCLE),
        ("CottonWorks: odborný materiál o efektných priadzach", COTTONWORKS_YARNS),
        ("ASTM D3512/D3512M-22: hodnotenie žmolkovania", ASTM_PILLING),
        ("ASTM D3939/D3939M-26: odolnosť proti zachyteniu", ASTM_SNAG),
        ("EÚ 1007/2011: označovanie textilných vlákien", EU_FIBRE_LABEL),
        ("GINETEX: význam symbolov ošetrovania", GINETEX),
    ],
    "product_intro": "Prací gél je relevantný iba pre buklé odev alebo úplet, ktorého etiketa povoľuje domáce pranie. Sako s výstužou, vlnený kabát a čalúnenie môžu vyžadovať špeciálny alebo odborný postup bez bežného gélu.",
    "product_text": "Hypoalergénny prací gél z marseillského mydla je určený na bežnú domácu bielizeň. Pri prateľnom bavlnenom, polyesterovom alebo inom kompatibilnom buklé ho použite podľa etikety, hmotnosti náplne a tvrdosti vody.",
    "product_limit": "Produkt nie je automaticky vhodný na vlnu, hodváb, lepenú výstuž, pevné čalúnenie alebo profesionálne čistený kabát. Neodstráni žmolky, nevráti prerezanú slučku a neopraví stratu tvaru.",
    "category_intro": "Pri výbere gélu oddeľte prateľný sveter od saka, kabáta a poťahu. Kategória pomáha porovnať produkty pre bežnú bielizeň, ale o vhodnosti rozhoduje zloženie a symboly hotového výrobku.",
    "category_text": "Zvoľte iba prostriedok kompatibilný s vláknom a povoleným programom. Pri vlne, hodvábe, podšívke alebo špeciálnej úprave použite výrobcom určený produkt alebo odbornú službu.",
    "related": [
        ("Prečo sa oblečenie žmolkuje", ARTICLE_PILLING),
        ("Ako predchádzať zatrhávaniu textilu", ARTICLE_SNAGGING),
        ("Ako čítať štítok na oblečení", ARTICLE_LABEL),
        ("Prečo sa oblečenie zráža", ARTICLE_SHRINKAGE),
        ("Stálofarebnosť textilu", ARTICLE_COLOR),
        ("Ako vybrať prací gél", ARTICLE_GEL),
    ],
    "faq_title": "buklé a slučkový povrch",
    "faq": [
        ("Je buklé vždy vlnené?", "Nie. Môže obsahovať vlnu, bavlnu, akryl, polyester, viskózu alebo zmesi. Buklé opisuje efektnú priadzu či povrch, nie jedno vlákno."),
        ("Môže sa buklé prať v práčke?", "Len ak to povoľuje etiketa celého výrobku. Pletený sveter môže byť prateľný, kým sako s výstužou alebo kabát potrebuje odborné čistenie."),
        ("Ako odstrániť prach z buklé?", "Jemne nasucho valčekom, mäkkou kefou alebo nízkym vysávaním cez ochrannú mriežku podľa výrobcu. Slučky neťahajte a nepoužívajte rotačnú kefu."),
        ("Môžem buklé oholiť od žmolkov?", "Najprv odlíšte žmolok od konštrukčnej slučky. Holiaci nástroj môže slučku prerezať, preto pri neistote povrch nestrihajte."),
        ("Čo robiť s vytiahnutou slučkou?", "Neodstrihujte ju pri líci. Uvoľnite napätie a nechajte ju vhodným nástrojom zatiahnuť na rub alebo odborne opraviť."),
        ("Ako vyčistiť buklé sedačku?", "Nájdite kód a pokyny výrobcu, najprv odstráňte voľný prach a mokrý zásah testujte na skrytom mieste. Výplň nepremočte."),
        ("Ako sušiť buklé sveter?", "Po povolenom praní ho podoprite, odsajte vodu bez krútenia a sušte naplocho v tvare mimo radiátora a priameho slnka."),
        ("Prečo buklé po čistení splošťuje?", "Príčinou môže byť tlak, mokré trenie, teplo, nevhodná para alebo zvyšok produktu. Povrch upravujte jemne až po úplnom vysušení."),
    ],
}


CREPE: dict[str, object] = {
    "title": "Čo je krep: zrnitý povrch, krčivosť a správna starostlivosť",
    "link": "co-je-krep-zrnity-povrch-krcivost-a-spravna-starostlivost",
    "meta": "Čo je krep, ako vzniká jeho zrnitý povrch, aký je rozdiel medzi krepdešínom a žoržetom a ako prať, sušiť a žehliť krepový odev.",
    "short": "Krep je rodina textílií so zrnitým alebo zvlneným povrchom vytvoreným zákrutom priadze, väzbou či úpravou. Spoznajte druhy krepu a starostlivosť bez sploštenia a deformácie.",
    "name": "krep",
    "genitive": "krepu",
    "construction_summary": "rodinu textílií so zrnitým, zvlneným alebo krepovým povrchom vytvoreným vysoko zakrútenou priadzou, väzbou, chemickou alebo mechanickou úpravou",
    "label_details": "presné vlákno, smer a intenzitu textúry, šikmý strih, podšívku, plisovanie, riasenie, potlač, výšivku a tvarové výstuže",
    "residue_place": "zrnitom povrchu, jemných záhyboch, riasení, švoch a medzi vrchnou vrstvou a podšívkou",
    "friction_risk": "vystupujúce zrná, jemnú vysoko zakrútenú priadzu, plisovaný okraj alebo voľný šev",
    "drying_advice": "Odev uložte do prirodzeného tvaru bez napínania textúry. Ľahký šikmo strihaný krep podoprite, pletený alebo viskózový kus sušte naplocho podľa etikety a plisovanie nechajte bez silného tlaku.",
    "heat_risk": "zákrut priadze, zrnitú textúru, plisovanie, rozmer, lesk, farbu a rozdielnu reakciu podšívky",
    "failure_sign": "povrch je lokálne hladký alebo stuhnutý, priadze sa rozostupujú, plisovanie mizne, šev sa krúti alebo podšívka ťahá vrchnú vrstvu",
    "answer": "Krep nie je jedno vlákno ani jedna presná väzba. Je to rodina tkanín a úpletov so zrnitým, zvlneným alebo jemne pokrčeným povrchom, ktorý môže vzniknúť vysokým zákrutom priadze, konštrukciou alebo následnou úpravou. Krep môže byť hodvábny, vlnený, viskózový, polyesterový aj zmesový, preto univerzálna teplota prania neexistuje. Pred čistením skontrolujte zloženie, symboly, podšívku, plisovanie a šikmý strih. Ak je voda povolená, znížte trenie, použite kompatibilný prostriedok, kus nekrúťte a sušte ho v prirodzenom tvare. Silné žehlenie môže sploštiť zámerný reliéf, kým para môže meniť viskózu, hodváb, vlnu aj trvalosť plisu. Hodnotný, hodvábny, vlnený alebo konštrukčne zložitý krep zverte postupu uvedenému výrobcom alebo odbornej čistiarni.",
    "intro": "Slovo krep sa objavuje pri šatách, blúzkach, oblekoch, závesoch aj posteľnom textile, no pod jedným názvom sa skrývajú veľmi odlišné konštrukcie. Krepdešín je hladší a jemnejší, žoržet zrnitý a priesvitnejší, vlnený krep pružne tvarovateľný a polyesterový krep môže držať plisovanie. Povrch navyše nemusí vzniknúť tým istým spôsobom. Niekedy ho vytvára vysoký zákrut priadze, inokedy väzba alebo dokončenie. Preto sa krep neperie podľa názvu, ale podľa konkrétneho vlákna a hotového odevu. Nasledujúci postup vysvetľuje, ako zachovať textúru, rozmer, švy a sklad bez zbytočného tepla a tlaku.",
    "quick": [
        "<strong>Krep je rodina povrchov:</strong> názov neprezrádza, či ide o hodváb, vlnu, viskózu alebo polyester.",
        "<strong>Textúra môže vznikať rôzne:</strong> vysokým zákrutom, väzbou, chemickou úpravou alebo kombináciou.",
        "<strong>Krepdešín a žoržet nie sú synonymá:</strong> líšia sa priehľadnosťou, dotykom, priadzou aj použitím.",
        "<strong>Silný tlak môže povrch sploštiť:</strong> žehlenie a para sa riadia etiketou a skúškou z rubu.",
        "<strong>Viskóza a šikmý strih potrebujú oporu:</strong> mokrý odev sa môže vytiahnuť alebo zmeniť dĺžku.",
        "<strong>Plisovanie je samostatný limit:</strong> nie každý sklad sa po vode a teple vráti do pôvodného tvaru.",
    ],
    "overview_heading": "Ako vzniká krepový povrch a prečo existuje toľko druhov",
    "overview": [
        "CottonWorks opisuje krepové priadze ako priadze s veľmi vysokou nevyváženou úrovňou zákrutu, ktorá im dáva sklon ku krúteniu a vytváraniu drsnej kamienkovej textúry v látke. Pri tkaní sa môžu striedať priadze s opačným smerom zákrutu, aby sa napätia rozložili a po dokončení vytvorili živý povrch. Krepový vzhľad však možno dosiahnuť aj nepravidelnou väzbou alebo úpravou, takže samotný reliéf nepreukazuje konkrétnu priadzu.",
        "Krepdešín tradične využíva hladšiu osnovu a vysoko zakrútený útok, preto má jemný zrnitý rub a splývavý vzhľad. Žoržet býva zrnitý výraznejšie a často je priesvitnejší, pretože vysoko zakrútené priadze pracujú v oboch smeroch. Krep marokén alebo iné obchodné typy môžu byť ťažšie a kompaktnejšie. Názvy sa naprieč trhmi používajú rôzne a nevytvárajú jednotnú normu údržby.",
        "Vlákno mení odozvu povrchu. Hodvábny krep môže byť citlivý na vodné mapy a mokré trenie, vlnený na plstenie a teplo, viskózový na mokrú pevnosť a rozmer, polyesterový na vysokú teplotu a statický náboj. Zmes pridáva kompromis, nie automaticky najlepšie vlastnosti všetkých zložiek. Etiketa a najcitlivejší detail preto rozhodujú viac než názov druhu.",
    ],
    "table1_heading": "Najčastejšie druhy krepu a ich starostlivosť",
    "table1_intro": "Ide o orientačné rozdiely. Presné správanie určuje vlákno, hustota, farbenie, dokončenie a konštrukcia hotového výrobku.",
    "table1_headers": ["Druh alebo použitie", "Typický povrch", "Časté riziko", "Čo skontrolovať"],
    "table1_rows": [
        ("Krepdešín", "Jemný, splývavý, mierne zrnitý povrch s mäkším leskom.", "Vodná mapa, zachytenie, zmena farby a deformácia šikmého strihu.", "Vlákno, podšívku, farbenie a povolené profesionálne alebo ručné čistenie."),
        ("Žoržet", "Výraznejšie zrnitý, ľahký až priesvitný povrch s pružným pádom.", "Zatrhnutie, posun nití, vyťahanie a zvyšky prostriedku v textúre.", "Jemnosť priadze, hustotu, švy, ozdoby a teplotný limit."),
        ("Vlnený krep", "Matný, pružný a tvarovateľný povrch na šaty, sukne a obleky.", "Plstenie, zrazenie, lesk po tlaku a strata tvaru výstuže.", "Podiel vlny, podšívku, výstuž a symbol profesionálneho čistenia."),
        ("Polyesterový alebo viskózový krep", "Od ľahkého šatového po pevnejší odevný povrch a plisovanie.", "Tepelná deformácia polyesteru alebo mokrá nestabilita viskózy.", "Presné percentá, plisovanie, sušičku, paru a spôsob sušenia."),
    ],
    "sections": [
        {
            "heading": "Krepdešín, žoržet a krepový satén",
            "paragraphs": [
                "Krepdešín má jemnú zrnitú textúru a mäkký pád, žoržet býva drsnejší a priesvitnejší. Krepový satén kombinuje hladšiu lesklejšiu stranu s krepovým charakterom druhej strany. Obchodný názov môže opisovať líc, rub alebo zamýšľané použitie. Pred čistením si preto prezrite obe strany a zistite, ktorá je vystavená noseniu a škvrnám.",
                "Rozdiel v názve neznamená automaticky rozdiel vo vlákne. Všetky tri varianty môžu byť hodvábne aj syntetické. Naopak, dve polyesterové látky môžu mať odlišný zákrut, podšívku a tepelnú úpravu. Pri kúpe žiadajte presné zloženie a symboly; fráza hodvábny vzhľad opisuje estetiku a nesmie sa zamieňať s hodvábnym vláknom.",
            ],
        },
        {
            "heading": "Vysoký zákrut, pružnosť a návrat textúry",
            "paragraphs": [
                "Priadza s vysokým nevyváženým zákrutom má vnútornú tendenciu krútiť sa. Keď sa vhodne zatká a uvoľní dokončením, vytvorí mikrovrásnenie a pružný živý omak. Voda a teplo môžu časť napätia zmeniť, ale výsledok závisí od vlákna a stabilizácie. Nemožno preto sľúbiť, že pokrčený krep sa po pare vždy vráti do pôvodného stavu.",
                "Silné plošné žehlenie môže zrnitý reliéf dočasne alebo trvalo sploštiť. Pri vysoko zakrútenej viskóze môže mokrý ťah meniť rozmer, pri syntetike zas vysoká teplota zafixovať nežiaduci lesk. Cieľom starostlivosti nie je urobiť krep hladký ako popelín, ale odstrániť nečistotu a lokálne záhyby bez potlačenia zamýšľanej textúry.",
            ],
        },
        {
            "heading": "Ako prať krepové šaty, sukňu a nohavice",
            "paragraphs": [
                "Skontrolujte pás, zips, podšívku, spodný lem, záševky a šikmo strihané diely. Odev obráťte naruby iba vtedy, ak ozdoby a konštrukcia umožňujú bezpečné otočenie. Hladký zips zatvorte, ostrý háčik chráňte a kus oddeľte od uterákov, riflí a suchých zipsov. Domáce pranie použite len pri povolenom symbole.",
                "Pri ručnom praní ponechajte odev voľne ponorený a jemne pretláčajte vodu. Netrite zrnitý povrch proti sebe a nekrúťte šikmo strihaný lem. Pri povolenej práčke použite šetrnú náplň a primerane nízke otáčky. Po cykle odev podoprite, urovnajte švy bez naťahovania a porovnávajte rozmer až úplne suchý.",
            ],
        },
        {
            "heading": "Ako prať krepovú blúzku a šatku",
            "paragraphs": [
                "Golier, manžety a podpazušie najprv prezrite proti svetlu. Čerstvý maz odsajte, lokálny prostriedok otestujte z rubu a nepoužívajte horúcu vodu ako prvý krok. Jemný krep môže po trení zmeniť lesk a farbu, aj keď škvrna zoslabne. Hodvábny alebo nestálofarebný kus zverte postupu uvedenému výrobcom.",
                "Šatku podoprite po celej dĺžke a nesušte ju v uzle. Ručne rolovaný lem, strapec alebo potlač môže určovať nižšiu hranicu než stred plochy. Po vysušení ju ukladajte bez tvrdého preloženia na rovnakom mieste. Pri žehlení pracujte z rubu cez ochrannú tkaninu a nesplošťujte zrnitú štruktúru silným tlakom.",
            ],
        },
        {
            "heading": "Plisovaný krep a sklady, ktoré sa nemusia obnoviť",
            "paragraphs": [
                "Plisovanie môže byť vytvorené teplom na syntetike, mechanicky alebo ďalšou úpravou. Voda, para a tlak môžu sklad uvoľniť, zmeniť jeho ostrosť alebo vytvoriť nerovnaký výsledok medzi panelmi. Etiketa musí povoľovať nielen vlákno, ale aj údržbu konkrétneho plisovaného výrobku. Nežehlite sklad naplocho len preto, že sa po praní rozostúpil.",
                "Plisovanú sukňu sušte v prirodzenej dĺžke bez štipcov cez hrany. Sklady urovnajte jemne prstami bez ťahania mokrej látky. Ak sa jeden panel výrazne zmenil, ďalšia para môže rozdiel zväčšiť. Pri hodnotnom alebo zložito skladanom kuse je vhodnejšia čistiareň, ktorá vie kontrolovať tvarovanie a teplotu.",
            ],
            "callout": {
                "title": "Kedy plisovanie radšej nenaparovať doma",
                "items": [
                    "Etiketa povoľuje iba profesionálne čistenie alebo zakazuje žehlenie.",
                    "Neznáma podšívka, lepidlo alebo dekorácia leží priamo pod skladom.",
                    "Farba pri skúške púšťa alebo povrch po kvapke tvorí mapu.",
                    "Sklad je lokálne deformovaný a nie je jasné, či ide o tepelne fixovanú úpravu.",
                ],
                "background": "#f7fbf8",
                "border": "#dbe5de",
            },
        },
        {
            "heading": "Škvrny, vodné mapy a zmena lesku",
            "paragraphs": [
                "Na jemnom krepe môže lokálne zmáčanie vytvoriť viditeľný okraj. Najprv odsajte tekutinu, škvrnu nerozširujte a testujte na skrytom leme. Make-up a mastnota potrebujú iný postup než nápoj alebo pigment. Univerzálne drhnutie pracím gélom môže zmeniť smer priadze, lesk a farbu skôr, než sa škvrna rozpustí.",
                f"Zafarbené miesto nevystavujte žehličke ani sušičke, kým nie je výsledok jasný. Vysoké teplo môže zafixovať zvyšok a súčasne sploštiť povrch. Všeobecné rozdelenie škvŕn nájdete v návode <a href=\"{ARTICLE_STAIN}\">ako odstraňovať rôzne škvrny z oblečenia</a>, no pri hodvábe, vlne a nestálej farbe vždy použite užšiu hranicu výrobcu.",
            ],
        },
        {
            "heading": "Sušenie a žehlenie bez straty zrnitého povrchu",
            "paragraphs": [
                "Po oplachu nechajte vodu odtiecť a odev podoprite. Viskózový alebo pletený krep sušte naplocho, ľahké šaty na širokej opore iba pri povolení etikety. Nevyťahujte lem do rozmeru silou. Priamy radiátor, horúci fén a dlhé slnko môžu meniť farbu, syntetické vlákno, vlnu aj napätie priadze.",
                f"Pri žehlení začnite najnižším povoleným stupňom, pracujte z rubu cez čistú tkaninu a používajte minimum tlaku. Para nie je automaticky jemná; pridáva teplo aj vodu. Podrobnejšie rozhodovanie vysvetľuje návod <a href=\"{ARTICLE_IRONING}\">ako žehliť oblečenie podľa materiálu</a>. Prirodzenú zrnitú textúru nežehlite do úplnej hladkosti.",
            ],
        },
        {
            "heading": "Skladovanie krepových odevov a dlhých šiat",
            "paragraphs": [
                "Dlhé šaty zaveste na široký polstrovaný vešiak, ak to hmotnosť a šikmý strih dovoľujú, a podoprite ich závesnými pútkami. Ťažký, zdobený alebo viskózový kus môže byť bezpečnejšie uložiť naplocho s mäkkými prekladmi. Nedovoľte, aby zipsy a koráliky susedného oblečenia trieli priamo o zrnitý povrch.",
                "Pred uložením odstráňte pot a kozmetiku povoleným postupom a odev úplne vysušte. Plastový vak môže uzavrieť vlhkosť; tesná skriňa zas stlačiť plisovanie. Pri dlhodobom skladovaní občas presuňte miesto preloženia a skontrolujte švy. Zatuchnutie riešte vetraním priestoru a odstránením zdroja vlhkosti, nie parfumáciou textilu.",
            ],
        },
    ],
    "table2_heading": "Krep po praní: ako čítať zmenu povrchu a tvaru",
    "table2_intro": "Zrnitý povrch môže zvýrazniť mapu aj záhyb. Najprv určite, či ide o zvyšok produktu, zmenu priadze, rozmer alebo konštrukčný problém.",
    "table2_headers": ["Prejav", "Možná príčina", "Čo overiť", "Bezpečný ďalší krok"],
    "table2_rows": [
        ("Povrch je lokálne hladký a lesklý", "Silný tlak, horúca žehlička, trenie alebo zmena dokončenia.", "Teplotu, smer žehlenia a či sú priadze neporušené.", "Nepridávať teplo; po ustálení posúdiť odborné naparenie."),
        ("Šaty sa predĺžili", "Šikmý strih, mokrá hmotnosť, viskóza alebo rozdielna zmena podšívky.", "Rozmer po úplnom vysušení a spôsob opory.", "Nenapínať; nechať ustáliť a pri novom kuse zdokumentovať odchýlku."),
        ("Plisovanie sa rozostúpilo", "Voda, teplo, tlak alebo nedostatočne stabilná úprava.", "Symboly, vlákno a spôsob fixácie skladu.", "Nežehliť naplocho; zvoliť odborné tvarovanie."),
        ("Na látke ostala mapa", "Nerovnomerné zmáčanie, farbivo, zvyšok produktu alebo mastnota.", "Skúšku farby, oplach a hranicu mapy po vysušení.", "Nedrhnúť; pri citlivom vlákne použiť odborné čistenie."),
        ("Šev sa krúti alebo rozostupuje", "Rozdielne napätie, posun nití, zrazenie alebo tesný strih.", "Smer dielov, stav stehu a zdravú plochu okolo.", "Pred ďalším praním opraviť a nevyrovnávať násilným ťahom."),
    ],
    "steps_heading": "Ako bezpečne vyprať krep krok za krokom",
    "steps": [
        "Určite presné vlákno, druh krepu, podšívku, plisovanie, šikmý strih a všetky symboly ošetrovania.",
        "Skontrolujte farbu, švy, zipsy a ozdoby a lokálny prostriedok otestujte na skrytom suchom mieste.",
        "Zvoľte iba povolené ručné, strojové alebo profesionálne čistenie a oddeľte krep od drsných kusov.",
        "Použite kompatibilný prostriedok v presnej dávke; zrnitý povrch netrite a mokrý odev nekrúťte.",
        "Oplachujte šetrne, kus vyberte s rovnomernou oporou a švy urovnajte bez naťahovania.",
        "Sušte naplocho alebo na širokej opore podľa etikety a chráňte plisovanie pred tlakom a teplom.",
        "Žehlite či naparujte len pri povolení, z rubu, cez ochrannú tkaninu a bez sploštenia textúry.",
    ],
    "remember": [
        "Vzniká krepový povrch zákrutom priadze, väzbou, plisovaním alebo následnou úpravou?",
        "Je vrchná vrstva hodvábna, vlnená, viskózová, polyesterová alebo zmesová?",
        "Má odev podšívku, výstuž, šikmý strih alebo plisovanie s nižším limitom?",
        "Povoľuje etiketa vodu, práčku, sušičku, paru, žehlenie alebo iba profesionálne čistenie?",
        "Sú farba a povrch po skrytej skúške stabilné až po úplnom vysušení?",
        "Má mokrý odev rovnomernú oporu a nie je zavesený za úzky bod?",
    ],
    "mistakes": [
        "Považovať krep za jedno vlákno a preniesť rovnakú teplotu na hodváb, vlnu, viskózu aj polyester.",
        "Vyžehliť zrnitý povrch dohladka a tým odstrániť charakter materiálu.",
        "Použiť paru na plisovanie bez poznania fixácie, podšívky a tepelného limitu.",
        "Krútiť mokré šaty, zavesiť šikmý strih za ramienka a merať ho ešte vlhký.",
        "Drhnúť vodnú mapu alebo mastnotu koncentrovaným prostriedkom bez skúšky farby.",
        "Uložiť krep vlhký alebo stlačený medzi ostrými zipsami a ťažkým oblečením.",
    ],
    "expert_heading": "Odbornejší pohľad: krepový zákrut, rozmer a skúšky povrchu",
    "expert": [
        "CottonWorks uvádza, že krepové priadze majú veľmi vysoký nevyvážený zákrut a sklon ku krúteniu, čím v tkanine vytvárajú drsnú kamienkovú textúru. Tento mechanizmus vysvetľuje iba jednu vetvu krepových textílií. Moderný výrobok môže používať nepravidelnú väzbu alebo povrchovú úpravu, preto názov krep nemožno spätne preložiť na presný zákrut či pevnosť.",
        "AATCC TM135 meria rozmerové zmeny textílií po definovanom domácom praní. Pri krepových šatách však výsledný tvar ovplyvňuje aj šikmý strih, podšívka, šev a gravitácia pri mokrom sušení. Meranie má zmysel iba medzi rovnakými označenými bodmi na suchom ustálenom kuse. Jedna zmena lemu bez záznamu podmienok nevysvetľuje mechanizmus.",
        "ASTM D1424 sa týka pokračovania trhliny a ASTM D3939 zachytávania, teda dvoch rozdielnych vlastností. Jemný zrnitý krep môže byť citlivý na háčik, ale z toho nemožno bez skúšky odvodiť jeho silu pri trhaní alebo životnosť šva. Odborné hodnotenie musí pomenovať metódu, smer a kondicionovanie; domáca starostlivosť má minimalizovať rizikové trenie bez predstierania laboratórneho výsledku.",
    ],
    "source_intro": "Zdroje podporujú opis vysoko zakrútenej krepovej priadze, normované meranie rozmerovej zmeny, trhania a zachytávania a význam symbolov. Nepodporujú jednu starostlivosť pre všetky krepové textílie.",
    "sources": [
        ("CottonWorks: odborný materiál o krepových priadzach", COTTONWORKS_YARNS),
        ("AATCC TM135-2025: rozmerové zmeny po domácom praní", AATCC_DIMENSION),
        ("ASTM D1424-25: pokračovanie trhliny v tkanine", ASTM_TEAR),
        ("ASTM D3939/D3939M-26: odolnosť proti zachyteniu", ASTM_SNAG),
        ("EÚ 1007/2011: označovanie textilných vlákien", EU_FIBRE_LABEL),
        ("GINETEX: význam symbolov ošetrovania", GINETEX),
    ],
    "product_intro": "Pri prateľnom polyesterovom, bavlnenom alebo inom kompatibilnom krepe môže byť bežný gél vhodný, ale až po kontrole etikety. Hodvábny, vlnený, viskózový, plisovaný alebo profesionálne čistený kus môže potrebovať iný produkt.",
    "product_text": "Hypoalergénny prací gél z marseillského mydla je určený na bežnú domácu bielizeň. Pri krepe ho použite iba vtedy, keď je kompatibilný s presným vláknom, farbou a povoleným spôsobom prania.",
    "product_limit": "Produkt nie je automatickým riešením pre hodváb, vlnu, nestálofarebný viskózový krep, plisovanie, výstuž ani odev určený na profesionálne čistenie. Neobnoví sploštenú textúru alebo deformovaný šikmý strih.",
    "category_intro": "Pri krepe vyberajte gél podľa vlákna a konštrukcie, nie podľa zrnitého vzhľadu. Správna dávka a oplach sú dôležité, pretože zvyšky sa môžu držať v mikrotextúre.",
    "category_text": "V kategórii nájdete gély pre rôzne potreby bežnej bielizne. Pred použitím porovnajte zloženie produktu so symbolmi krepu, podšívkou a prípadným plisovaním; špeciálne vlákna ošetrite určeným spôsobom.",
    "related": [
        ("Čo je viskóza a ako sa o ňu starať", ARTICLE_VISCOSE),
        ("Čo je polyester a ako ho prať", ARTICLE_POLYESTER),
        ("Prečo sa oblečenie zráža", ARTICLE_SHRINKAGE),
        ("Ako predchádzať zatrhávaniu", ARTICLE_SNAGGING),
        ("Stálofarebnosť textilu", ARTICLE_COLOR),
        ("Ako čítať štítok na oblečení", ARTICLE_LABEL),
    ],
    "faq_title": "krep a krepové odevy",
    "faq": [
        ("Je krep vždy z hodvábu?", "Nie. Krep môže byť hodvábny, vlnený, viskózový, polyesterový, bavlnený aj zmesový. Názov opisuje charakter povrchu alebo konštrukcie."),
        ("Aký je rozdiel medzi krepdešínom a žoržetom?", "Krepdešín býva jemnejší a splývavejší, žoržet výraznejšie zrnitý a často priesvitnejší. Presné vlákno sa pri oboch môže líšiť."),
        ("Môže sa krep prať v práčke?", "Len ak to povoľuje etiketa celého odevu. Hodváb, vlna, podšívka, výstuž, šikmý strih alebo plisovanie môžu vyžadovať iný postup."),
        ("Na koľko stupňov prať krep?", "Jedna teplota neexistuje. Rozhoduje presné vlákno, farba, dokončenie a najcitlivejšia časť hotového výrobku."),
        ("Ako žehliť krep?", "Pri povolení etikety z rubu, cez ochrannú tkaninu, s nízkou vhodnou teplotou a minimom tlaku. Zrnitý povrch nežehlite úplne dohladka."),
        ("Prečo sa krep po praní natiahol?", "Príčinou môže byť mokrá hmotnosť, šikmý strih, viskózové vlákno alebo rozdielna reakcia podšívky. Hodnoťte až suchý ustálený odev."),
        ("Ako zachrániť plisovaný krep?", "Nepridávajte ďalšie teplo naslepo. Overte symboly a spôsob fixácie skladu; hodnotný kus zverte odbornému tvarovaniu."),
        ("Môže ísť krep do sušičky?", "Iba pri výslovnom symbole. Teplo a prevaľovanie môžu meniť textúru, rozmer, plisovanie, syntetické vlákna aj podšívku."),
    ],
}


ARTICLES: list[dict[str, object]] = [NEOPRENE, CHIFFON, BOUCLE, CREPE]


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
        "batch": "batch-46",
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
        public_text = f"{article['title']} {article['short']} {body}"
        visible = visible_text(body)
        if FORBIDDEN_PUBLIC_RE.search(public_text):
            raise SystemExit(f"Forbidden public wording in {article['title']}")
        if FIXED_PRICE_RE.search(visible_text(public_text)):
            raise SystemExit(f"Fixed price found in {article['title']}")
        metric = {
            "title": article["title"],
            "slug": article["link"],
            "words": len(WORD_RE.findall(visible)),
            "h2": len(re.findall(r"<h2\b", body, re.IGNORECASE)),
            "tables": len(re.findall(r"<table\b", body, re.IGNORECASE)),
            "responsive_tables": len(re.findall(r'<div\b[^>]*style="[^"]*overflow-x:\s*auto', body, re.IGNORECASE)),
            "styled_blocks": len(re.findall(r"<div\b[^>]*style=", body, re.IGNORECASE)),
            "action_buttons": len(re.findall(r'<a\b[^>]*style="[^"]*display:\s*inline-block', body, re.IGNORECASE)),
        }
        if metric["words"] < 2800:
            raise SystemExit(f"Article is too short: {article['title']} ({metric['words']} words)")
        if metric["h2"] < 24 or metric["tables"] < 2 or metric["responsive_tables"] != metric["tables"]:
            raise SystemExit(f"Article structure is incomplete: {article['title']} ({metric})")
        if metric["styled_blocks"] < 10 or metric["action_buttons"] < 2:
            raise SystemExit(f"Article visual blocks are incomplete: {article['title']} ({metric})")
        metrics.append(metric)
        rendered.append({
            "title": article["title"],
            "short": article["short"],
            "long": body,
            "link": article["link"],
            "date_posted": PUBLISH_DATE,
            "time_posted": "13:00:00",
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
        raise SystemExit("Batch 46 link preflight failed")
    print(json.dumps({"article_count": len(rendered), "metrics": metrics, "link_preflight": True}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
