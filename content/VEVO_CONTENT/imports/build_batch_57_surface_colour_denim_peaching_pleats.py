#!/usr/bin/env python3
"""Build and validate VEVO batch 57 surface-finish articles."""

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


PUBLISH_DATE = "2026-10-26"
CANDIDATES = Path("content/VEVO_CONTENT/batches/batch-57-candidates-2026-10-26.txt")
OUT_JSON = Path("content/VEVO_CONTENT/imports/batch-57-2026-10-26-articles.json")
OUT_PREFLIGHT = Path("content/VEVO_CONTENT/exports/batch-57-2026-10-26-link-preflight.json")

EU_FIBRE_LABEL = "https://eur-lex.europa.eu/eli/reg/2011/1007/oj"
GINETEX = "https://www.ginetex.net/share/article/4201/care-symbols"
AATCC_STANDARDS = "https://members.aatcc.org/store/items/"
AATCC_LAUNDERING = "https://members.aatcc.org/store/tm61/495/"
COTTON_PIGMENT = "https://cottonworks.com/encyclopedia-item/pigment/"
COTTON_DYEING = "https://cottonworks.com/learning-hub/dyeing/dyeing-basics/"
COTTON_PRINTING = "https://cottonworks.com/learning-hub/printing/printing-process/"
COTTON_STONE = "https://cottonworks.com/encyclopedia-item/stone-washed/"
COTTON_DENIM = "https://cottonworks.com/encyclopedia-item/denim/"
COTTON_GARMENT_FINISHING = "https://cottonworks.com/learning-hub/finishing/garment-finishing/"
DENIM_CELLULASE = "https://pmc.ncbi.nlm.nih.gov/articles/PMC124001/"
DENIM_STONE_ENZYME = "https://jtatm.textiles.ncsu.edu/index.php/JTATM/article/download/9352/4550/37103"
COTTON_SUEDING = "https://cottonworks.com/encyclopedia-item/sueding/"
COTTON_SANDING = "https://cottonworks.com/encyclopedia-item/sanding/"
COTTON_MECHANICAL = "https://cottonworks.com/learning-hub/finishing/mechanical-finishing/"
MICROFIBRE_STUDY = "https://link.springer.com/article/10.1007/s11356-023-25246-8"
COTTON_HEATSETTING = "https://cottonworks.com/encyclopedia-item/heatsetting/"
PLEAT_RETENTION = "https://www.jstage.jst.go.jp/article/senshoshi1960/21/6/21_6_250/_article/-char/en"
VAM_PLEATS = "https://media.vam.ac.uk/media/documents/legacy_documents/file_upload/17744_file.pdf"
NPS_TEXTILE_STORAGE = "https://www.nps.gov/subjects/museums/upload/MHI_AppK_TextilesObjects.pdf"

ARTICLE_LABEL = "/n/ako-citat-stitok-na-obleceni-material-symboly-prania-a-spravny-program"
ARTICLE_COLOR = "/n/stalofarebnost-textilu-preco-farby-blednu-pri-prani-svetle-a-treni"
ARTICLE_BLEEDING = "/n/ako-zabranit-pustaniu-farby-pri-prani-noveho-oblecenia"
ARTICLE_NEW = "/n/ako-prat-nove-oblecenie-prvykrat-farby-chemicky-pach-zrazanie-a-stitok"
ARTICLE_STAINS = "/n/ako-odstranit-zuvacku-krv-vosk-a-ine-skvrny-z-oblecenia"
ARTICLE_DRYING = "/n/ako-susit-bielizen-v-malom-byte-bez-zatuchnutia"
ARTICLE_DENIM = "/n/ako-prat-riflovu-bundu-a-tmave-dzinsy-aby-nepustali-farbu"
ARTICLE_PILLING = "/n/preco-sa-oblecenie-zmolkuje-vlakna-trenie-pranie-a-susenie"
ARTICLE_GARMENT_DYED = "/n/co-je-garment-dyed-oblecenie-farbenie-hotoveho-odevu-blednutie-a-pranie"
ARTICLE_BIOPOLISH = "/n/co-je-biolestena-bavlna-enzymova-uprava-zmolky-a-pranie"
ARTICLE_MOLESKIN = "/n/co-je-moleskin-husta-brusena-bavlna-lesk-a-spravne-pranie"
ARTICLE_POLYESTER = "/n/polyester-vs-bavlna-rozdiely-pri-noseni-prani-a-voni"
ARTICLE_SEERSUCKER = "/n/co-je-seersucker-zvlnena-tkanina-priedusnost-a-pranie-bez-zehlenia"
ARTICLE_CREPE = "/n/co-je-krep-zrnity-povrch-krcivost-a-spravna-starostlivost"

PRODUCT_NAME = "Prací gél hypoalergénny Vevo Ylang Absolute 1L"
PRODUCT_URL = "/p-1627/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l"
CATEGORY_NAME = "Pracie gély"
CATEGORY_URL = "/c/vevo-home-care/pranie/praci-gel"


def add_cards(article: dict[str, object], *, product_heading: str, product_limit: str) -> None:
    article.update(
        {
            "product_heading": product_heading,
            "product_intro": (
                "Ak ošetrovací štítok povoľuje domáce pranie a všetky farbivá, vlákna, "
                "povrchové úpravy aj ozdoby sú kompatibilné, tekutý prací prostriedok možno "
                "odmerať podľa tvrdosti vody, veľkosti náplne a miery znečistenia."
            ),
            "product_name": PRODUCT_NAME,
            "product_url": PRODUCT_URL,
            "product_text": (
                "Tekutý gél sa dávkuje bez sypkého zvyšku. Koncentrát nelejte priamo na "
                "suchý povrch, výrazný sklad ani farebný šev a neprekračujte dávku v nádeji, "
                "že obnovíte pôvodnú výrobnú úpravu."
            ),
            "product_limit": product_limit,
            "category_heading": "Prací prostriedok prispôsobte celému odevu",
            "category_intro": (
                "Obchodný názov povrchu nie je prací program. Pri výbere zohľadnite vláknové "
                "zloženie, farbu, elastan, podšívku, potlač, ozdoby, intenzitu znečistenia "
                "a všetky symboly na etikete."
            ),
            "category_name": CATEGORY_NAME,
            "category_url": CATEGORY_URL,
            "category_text": (
                "V kategórii nájdete pracie gély pre rôzne potreby bežnej prateľnej bielizne. "
                "Vyberte iba kompatibilný výrobok, dodržte dávkovanie a ponechajte v bubne "
                "dostatok priestoru na pohyb aj oplach."
            ),
        }
    )


PIGMENT_DYED: dict[str, object] = {
    "title": "Čo je pigmentovo farbené oblečenie: patina, oter farby a starostlivosť",
    "link": "co-je-pigmentovo-farbene-oblecenie-patina-oter-farby-a-starostlivost",
    "meta": "Čo znamená pigmentovo farbené oblečenie, prečo vytvára patinu, ako odlíšiť bežný oter od poškodenia spojiva a ako ho bezpečne prať.",
    "short": "Pigmentovo farbené oblečenie nesie nerozpustné farebné častice najmä na povrchu vlákien, kde ich drží spojivo. Patina môže byť zámerná, no silný prenos, lepkavosť a praskanie už treba posúdiť osobitne.",
    "name": "pigmentovo farbené oblečenie",
    "locative": "pigmentovo farbenom odeve",
    "identity_heading": "Pigment opisuje farebný systém, nie vláknové zloženie",
    "identity_detail": "Pigment je nerozpustná farebná častica bez prirodzenej afinity k vláknu, preto sa na textilnom povrchu upevňuje polymérnym spojivom a vytvára vrstvu, ktorá sa pri používaní môže mechanicky meniť.",
    "identity_boundary": "Pigmentovo farbený kus môže byť zároveň garment-dyed, ak sa farba nanášala po ušití, ale tieto výrazy nie sú synonymá: jeden opisuje farebný systém a druhý poradie výroby.",
    "label_focus": "vláknové zloženie, informáciu pigment dyed alebo wash separately, kontrastné švy, potlač, koženú nášivku, elastan, povolené bielenie, teplotu, sušenie a žehlenie",
    "missing_label": "Ak pôvod a spôsob farbenia nie sú potvrdené, vintage vzhľad sám nestačí; podobnú patinu môže vytvoriť indigo, sírne farbivo, potlač, brúsna úprava alebo obyčajné opotrebenie.",
    "dry_check": "suchý farebný prenos, svetlejšie hrany, ostré mapy, lepkavé alebo gumové miesta, praskliny, odlupovanie, rozdiel medzi švom a plochou, potlač a poškodenie elastanu",
    "damage_boundary": "Mäkké postupné zosvetlenie hrán môže byť navrhnutá patina; farebný prach na pokožke, lepiaci povrch alebo kúsky spojiva nie sú automaticky prijateľná estetika.",
    "test_focus": "Porovnajte suchý a mierne vlhký prenos na bielu handričku bez agresívneho drhnutia a sledujte, či sa uvoľňuje iba farba, alebo aj lepkavá či prášivá povrchová vrstva.",
    "combined_risk": "mechanického oteru spojiva, pohybu mokrého pigmentu, lokálneho kontaktu s koncentrovaným produktom, trenia na vystúpených švoch a tepelného starnutia povrchu",
    "chemistry_boundary": "Ocot, soľ, bielidlo ani nadbytok gélu nie sú univerzálny fixačný kúpeľ; bez znalosti spojiva môžu zmeniť odtieň, vytvoriť mapu alebo urýchliť popraskanie povrchu.",
    "drying_detail": "Odev nenechávajte mokrý zložený na svetlom textile, urovnajte ho bez trenia a sušte bez kontaktu s citlivým čalúnením; odtieň aj omak hodnotíte až po úplnom vysušení.",
    "heat_boundary": "Horúca sušička a žehlička môžu poškodiť elastan, potlač alebo termoplastické spojivo a zrýchliť odieranie na hranách, aj keď základné vlákno samotné vyššiu teplotu znáša.",
    "stop_signs": "farba na pokožke alebo nábytku pri bežnom suchom kontakte, lepkavý film, odpadávajúce farebné šupinky, prudké mapovité zosvetlenie, zápach z povrchu alebo poškodenie po povolenom prvom cykle",
    "professional_boundary": "Jednoduché označené tričko možno prať doma podľa etikety, kým hodnotný kus, viacfarebná potlač, kožené prvky, neznáme spojivo alebo silný prenos vyžadujú údaje výrobcu, reklamáciu či odborné čistenie.",
    "answer": "Pigmentovo farbené oblečenie má farbu vytvorenú nerozpustnými časticami, ktoré na povrchu drží spojivo. Preto sa môže na švoch a hranách postupne vytvárať mäkká patina. Neznamená to však, že každý prenos farby je v poriadku. Nový kus prezrite, odfoťte a podľa pokynov overte suchý aj vlhký oter. Perte ho naruby, samostatne alebo s veľmi podobnými tmavými farbami, pri teplote zo štítku, s presnou dávkou a bez dlhého namáčania. Mokré oblečenie hneď vyberte a neodkladajte na svetlý textil. Pri lepkavosti, praskaní alebo silnom suchom prenose ďalšie pokusy zastavte.",
    "intro": "Pigment-dyed mikina alebo nohavice sa často predávajú s prísľubom prirodzenej patiny. Vizuálny výsledok môže byť veľmi príjemný, no praktická starostlivosť je odlišná od predstavy, že farba jednoducho prenikla do vlákna. Pigment leží prevažne na povrchu a jeho správanie závisí od spojiva, vytvrdnutia, trenia, tepla aj konštrukcie odevu. Spotrebiteľ preto potrebuje rozlíšiť tri javy: zámerné rovnomerné starnutie, voľný farebný prenos a skutočné zlyhanie povrchovej vrstvy. Každý z nich vedie k inému rozhodnutiu.",
    "quick": [
        "<strong>Pigment nie je rozpustené farbivo:</strong> na vláknach ho drží spojivo.",
        "<strong>Patina vzniká trením:</strong> hrany, švy, kolená a lakte sa menia skôr než chránená plocha.",
        "<strong>Garment-dyed nie je synonymum:</strong> opisuje fázu farbenia, nie nevyhnutne použitý farebný systém.",
        "<strong>Prvé prania oddeľte:</strong> nový sýty kus môže uvoľniť farbu do vody alebo pri mokrom kontakte.",
        "<strong>Viac produktu farbu nezafixuje:</strong> koncentrát môže na povrchu vytvoriť mapu.",
        "<strong>Lepkavosť a šupinky sú varovanie:</strong> nejde iba o pekné blednutie hrán.",
    ],
    "overview_heading": "Ako pigment zostáva na textilnom povrchu a prečo sa časom mení",
    "overview": [
        "Bežné farbivá sa počas farbenia adsorbujú, difundujú alebo inak viažu k vhodnému vláknu. Pigment je odlišný: ide o nerozpustnú časticu bez vlastnej afinity k bavlne, polyesteru či ich zmesi. Výrobca preto pripraví disperziu pigmentu a spojiva, nanesie ju na textil a následne vytvorí podmienky, pri ktorých spojivo vytvrdne a udrží častice na povrchu. Rovnováha je dôležitá: málo spojiva znižuje odolnosť, priveľa môže zmeniť mäkkosť a priedušný pocit.",
        "Pri nosení sa najviac namáhajú vystúpené miesta. Koleno sa ohýba, lakeť sa opiera, okraj vrecka sa trie a šev vytvára vyšší profil. Povrchová vrstva sa preto mení nerovnomerne a odhaľuje svetlejšiu podkladovú farbu. Keď je tento vývoj pomalý, rozložený podľa mechaniky odevu a zodpovedá popisu výrobcu, môže ísť o zámernú patinu. Ostré chemické mapy alebo lepiace plochy majú inú príčinu.",
        "Pranie pridáva vodu, detergent, pohyb a teplotu. Každý faktor môže ovplyvniť spojivo, uvoľniť povrchovú nečistotu alebo zvýrazniť mechanický oter. Výsledok preto nemožno predpovedať iba podľa bavlny na etikete. Rozhoduje celý systém vrátane pigmentu, živice, predchádzajúcej úpravy, šitia, potlače a presného návodu na ošetrovanie.",
    ],
    "table1_heading": "Pigment, farbivo, potlač a garment dyeing nie sú to isté",
    "table1_intro": "Názvy opisujú rozdielnu časť výroby. Jeden odev môže spĺňať viac riadkov naraz, preto sa pýtajte na farebný systém aj poradie procesu.",
    "table1_headers": ["Pojem", "Kde je farba", "Typická zmena", "Čo rozhoduje pri praní"],
    "table1_rows": [
        ("Pigmentovo farbené", "Častice so spojivom prevažne na povrchu.", "Oder hrán, patina, prípadne praskanie spojiva.", "Etiketa, kvalita vytvrdnutia, trenie a teplo."),
        ("Reaktívne farbivo", "Farbivo je chemicky viazané na celulózu.", "Celkové blednutie alebo prenos nefixovaného zvyšku.", "Fixácia, afterwashing, teplota a chémia."),
        ("Pigmentová potlač", "Pigment so spojivom iba v motíve.", "Praskanie a oder potlačených plôch.", "Pranie naruby, trenie a žehlenie motívu."),
        ("Garment dyeing", "Odev sa farbí po ušití; môže ísť o viac systémov.", "Tonálne rozdiely medzi dielmi a švami.", "Použité farbivo, komponenty a pokyny výrobcu."),
        ("Indigo denim", "Indigo tvorí prstencový efekt na priadzi.", "Charakteristické zosvetľovanie vystúpených miest.", "Triedenie, mechanika, prenos pri trení a sušenie."),
    ],
    "sections": [
        {
            "heading": "Ako spoznať pigmentovo farbenú mikinu, tričko alebo nohavice",
            "paragraphs": [
                "Najspoľahlivejšia je informácia výrobcu: pigment dyed, pigment garment dye, wash separately alebo upozornenie, že odtieň sa nosením mení. Samotný matný či vintage vzhľad nestačí. Podobný efekt môže vzniknúť praním denimu, brúsením, nepravidelným garment dyeing alebo potlačou. Prezrite rub, vnútro vrecka, krytý šev a miesto pod nášivkou; povrchové farbenie môže ukázať kontrast medzi chránenou a exponovanou časťou.",
                "Dotyk tiež nie je definitívny test. Niektoré kvalitné pigmentové úpravy sú mäkké, iné majú plnší omak a lacná potlač môže pôsobiť podobne. Ak údaj chýba, neexperimentujte s domácou fixáciou. Správnejšie je postupovať podľa najcitlivejšej možnosti, použiť skrytú skúšku a pri novom kuse si odložiť opis produktu, aby sa očakávaná patina dala odlíšiť od chyby.",
            ],
        },
        {
            "heading": "Patina, crocking a púšťanie farby označujú rozdielne javy",
            "paragraphs": [
                "Patina je zmena vzhľadu rozložená podľa používania: mäkšie svetlé hrany na vrecku, lakti alebo šve. Crocking označuje prenos farby pri trení a skúša sa samostatne za sucha a za mokra. Púšťanie do pracieho kúpeľa je ďalší jav. Kus môže mať peknú patinu a zároveň neprijateľný prenos na sedačku, alebo môže pri prvom praní mierne zafarbiť vodu bez toho, aby prášil za sucha.",
                f"Pre širší kontext si prečítajte, <a href=\"{ARTICLE_COLOR}\">prečo farby blednú pri praní, svetle a trení</a>. Pri hodnotení si zapisujte podmienky: bol povrch suchý, spotený alebo mokrý, aký tlak vznikol a na aký materiál sa farba preniesla. Bez týchto údajov sa ľahko zmieša navrhnuté starnutie so slabou stálofarebnosťou.",
            ],
        },
        {
            "heading": "Prvé pranie nového pigmentovo farbeného oblečenia",
            "paragraphs": [
                f"Pred prvým cyklom odfoťte farbu pri dennom rozptýlenom svetle, skontrolujte štítok a postupujte podľa návodu <a href=\"{ARTICLE_NEW}\">ako prať nové oblečenie prvýkrát</a>. Odev obráťte naruby, zapnite prvky, ale nevytvorte tesnú guľu. Sýty kus perte samostatne alebo s veľmi podobnými tmavými farbami a nenechávajte ho namočený bez pokynu výrobcu.",
                "Použite presnú dávku, nie poistnú dvojnásobnú. Príliš veľa gélu môže zhoršiť oplach a lokálny kontakt koncentrátu môže zmeniť povrch. Po skončení kus ihneď vyberte, urovnajte bez drhnutia a sušte podľa symbolu. Vodu v bubne nemožno hodnotiť ako laboratórny test, ale zreteľný prenos na ďalšie kusy je signál na oddelenie a preverenie.",
            ],
            "callout": {
                "title": "Pred prvým cyklom si vytvorte porovnateľný záznam",
                "items": [
                    "Odfoťte líc, rub, švy, lakte, kolená a okraje vreciek.",
                    "Uložte produktový opis o patine a odporúčanom praní.",
                    "Zapíšte program, teplotu, sušenie a použitú dávku.",
                    "Výsledok porovnajte až úplne suchý pri rovnakom svetle.",
                ],
                "background": "#f7fbf8",
                "border": "#dbe5de",
            },
        },
        {
            "heading": "Ako prať pigmentovo farbenú mikinu bez zbytočného oderu",
            "paragraphs": [
                "Mikina má hrubšie švy, rebrované lemy, kapucňu a často kovový zips, takže v bubne vzniká viac trenia než pri ľahkom tričku. Perte ju naruby, zips zaistite, šnúrky uvoľnite podľa konštrukcie a nekombinujte s uterákmi či odevmi s hrubými zipsami. Voľný bubon umožní pohyb aj oplach bez neustáleho tlaku na rovnaké hrany.",
                "Ak má mikina kontrastnú potlač alebo výšivku, riadi vás najcitlivejšia časť. Vyššia teplota nemusí odstrániť pot lepšie, ak zároveň oslabí spojivo alebo elastan. Pach riešte vhodným programom, primeranou dávkou a úplným vysušením. Vôňa ani zmäkčovadlo nezlepia späť poškodenú pigmentovú vrstvu.",
            ],
        },
        {
            "heading": "Ako odstrániť škvrnu bez svetlej mapy",
            "paragraphs": [
                f"Najprv určte typ škvrny a odstráňte prebytok bez rozotierania. Všeobecné poradie nájdete v návode <a href=\"{ARTICLE_STAINS}\">ako odstraňovať škvrny z oblečenia</a>, no pri pigmentovom povrchu navyše chráňte spojivo. Prípravok skúste na skrytom mieste, používajte krátky kontrolovaný kontakt a miesto nečistite tvrdou kefou ani melamínovou hubkou.",
                "Mokrá plocha vždy stmavne, preto výsledok neposudzujte počas ošetrenia. Ostrý svetlý okraj po vyschnutí môže byť odstránený pigment, nie zvyšok škvrny. Vtedy ďalšie drhnutie situáciu zhorší. Bielidlo použite iba pri výslovnom povolení symbolom a potvrdenej kompatibilite; názov bavlna sám osebe nestačí.",
            ],
        },
        {
            "heading": "Sušenie, žehlenie a kontakt so svetlými povrchmi",
            "paragraphs": [
                f"Odev po praní nepoložte na biely uterák, svetlú sedačku ani nelakované drevo. Sušte ho podľa symbolu s prúdením vzduchu a bez preloženia mokrej pigmentovej plochy na seba. Praktické zásady rozoberá článok <a href=\"{ARTICLE_DRYING}\">ako sušiť bielizeň bez zatuchnutia</a>. Pri sušičke rešpektujte povolenie aj nízky režim a nepresušujte.",
                "Žehličku neveďte priamo po pigmentovom motíve alebo neznámej povrchovej vrstve. Použite rub, ochrannú textíliu a teplotu určenú najcitlivejšou zložkou. Teplo môže dočasne zmeniť lesk a pri termoplastickom spojive aj trvalo zmeniť omak. Odtieň posudzujte až po vychladnutí.",
            ],
        },
        {
            "heading": "Čo robiť, keď oblečenie farbí pokožku alebo nábytok",
            "paragraphs": [
                "Najprv prestaňte kus používať na citlivom svetlom povrchu a zdokumentujte miesto, podmienky aj rozsah prenosu. Odlíšte suchý kontakt od spoteného alebo mokrého. Jedno opakované pranie navyše nie je automatické riešenie; môže odstrániť voľný pigment, ale aj zväčšiť oder a sťažiť posúdenie pôvodného stavu.",
                f"Ak výrobca povoľuje domáce pranie, riaďte sa postupom pre <a href=\"{ARTICLE_BLEEDING}\">nové oblečenie, ktoré púšťa farbu</a>. Pri silnom suchom prenose, zafarbení pokožky alebo škode na nábytku kontaktujte predajcu. Nepoužívajte soľ ani ocot bez konkrétneho návodu, pretože pigment so spojivom nereaguje ako jedna univerzálna trieda rozpustených farbív.",
            ],
        },
        {
            "heading": "Ako pigmentovo farbené oblečenie skladovať a kombinovať",
            "paragraphs": [
                "Ukladajte ho úplne suché, čisté a bez dlhého tlaku na ostrý sklad. Nový tmavý kus prvé týždne nedávajte tesne k bielej kabelke, svetlej bunde alebo lakovanému povrchu, kým nepoznáte jeho správanie pri trení. Vešiak musí podopierať ramená bez vytvorenia vystúpených bodov, ktoré sa časom obrúsia.",
                "Pri skladaní nevytvárajte stále rovnaký lom cez hrubú potlač. Ak má výrobok navrhnutú patinu, občasná zmena miesta preloženia pomôže predísť jednej ostrej línii od tlaku. Obal má byť čistý a priedušný; uzavretie mierne vlhkého odevu podporí pach a môže preniesť farbu medzi dotýkajúcimi sa plochami.",
            ],
        },
        {
            "heading": "Kedy je zmena vzhľadu bežná a kedy ju reklamovať",
            "paragraphs": [
                "Bežná patina je spravidla postupná, mäkko ohraničená a objavuje sa tam, kde dáva mechanický zmysel. Reklamačný dôvod môže naznačovať prudké mapovité odfarbenie po povolenom cykle, lepkavosť, odlupovanie, silný suchý prenos alebo poškodenie mimo popísaného charakteru výrobku. Porovnajte stav s fotografiami pred praním a s produktovým opisom.",
                "Pred kontaktovaním predajcu nepridávajte ďalšie experimentálne kúpele, nežehlite poškodené miesto a nesnažte sa ho farebne prekryť. Uchovajte etiketu, účet, záznam programu a použitý výrobok. Presný opis, či ide o prenos, oder, praskanie alebo chemickú mapu, je užitočnejší než všeobecné tvrdenie, že farba zišla.",
            ],
        },
    ],
    "table2_heading": "Pigmentovo farbený odev po praní: diagnóza pred ďalším zásahom",
    "table2_intro": "Jeden svetlejší vzhľad môže mať viac príčin. Rozhoduje umiestnenie, tvar okraja, omak, prenos a história ošetrenia.",
    "table2_headers": ["Prejav", "Možné vysvetlenie", "Ako ho odlíšiť", "Bezpečný ďalší krok"],
    "table2_rows": [
        ("Mäkké svetlé hrany", "Navrhnutá patina alebo bežný mechanický oder.", "Zmena sleduje švy, kolená a lakte a vzniká postupne.", "Prať naruby, znížiť trenie a sledovať vývoj."),
        ("Ostrá svetlá kvapka", "Koncentrát, bielidlo alebo iná lokálna chémia.", "Má ohraničený tvar a nesleduje konštrukciu.", "Nepridávať ďalšiu chémiu; zdokumentovať."),
        ("Farba na bielej handričke", "Crocking pri suchom alebo vlhkom trení.", "Zopakovať iba jemnú porovnateľnú skúšku na skrytom mieste.", "Oddeliť od svetlých povrchov; pri silnom prenose kontaktovať predajcu."),
        ("Povrch je lepkavý", "Poškodené alebo nedostatočne vytvrdené spojivo.", "Lepí aj po úplnom vysušení a zachytáva prach.", "Zastaviť teplo a trenie; nereparovať domácim náterom."),
        ("Farebné šupinky", "Praskanie pigmentovej vrstvy alebo potlače.", "Uvoľňuje sa vrstva, nie iba voľné vlákno.", "Ďalej neprať naslepo; posúdiť reklamáciu."),
    ],
    "steps_heading": "Ako prať pigmentovo farbené oblečenie krok za krokom",
    "steps": [
        "Prečítajte zloženie, upozornenie na pigment, triedenie, teplotu, sušenie a žehlenie.",
        "Odfoťte pôvodný odtieň, švy, hrany, potlač a už existujúcu patinu.",
        "Na skrytom mieste podľa pokynov overte suchý a vlhký prenos bez silného tlaku.",
        "Odev obráťte naruby, zaistite zipsy a oddeľte ho od svetlej a drsnej bielizne.",
        "Zvoľte povolený program a presnú dávku; koncentrát nelejte priamo na suchý povrch.",
        "Po cykle kus ihneď vyberte a nenechajte ho mokrý na inom textile.",
        "Sušte iba povoleným spôsobom bez prudkého tepla a zbytočného trenia.",
        "Výsledok porovnajte úplne suchý; pri lepkavosti, šupinkách alebo silnom prenose zásah zastavte.",
    ],
    "remember": [
        "Je pigmentový systém potvrdený výrobcom, alebo ho odhadujete podľa vzhľadu?",
        "Ide o celoplošné farbenie, pigmentovú potlač alebo kombináciu viacerých vrstiev?",
        "Prenáša sa farba za sucha, za mokra alebo iba v pracom kúpeli?",
        "Má odev elastan, nášivku, podšívku alebo potlač s prísnejším limitom?",
        "Vzniká zmena postupne na hranách, alebo má ostrý chemický okraj?",
        "Je kus pred uložením úplne suchý a oddelený od svetlého povrchu?",
    ],
    "mistakes": [
        "Považovať každý vintage vzhľad za dôkaz pigmentového farbenia.",
        "Ospravedlniť silný suchý prenos tvrdením, že patina je zámerná.",
        "Nalievať koncentrovaný gél priamo na suchú tmavú plochu.",
        "Drhnúť škvrnu kefou, kým nevznikne svetlý kruh.",
        "Nechať nový mokrý kus na bielom uteráku alebo čalúnení.",
        "Pokúšať sa fixovať neznáme spojivo octom, soľou alebo vysokým teplom.",
    ],
    "expert_heading": "Odbornejší pohľad: častica, spojivo, vytvrdnutie a skúšky prenosu",
    "expert": [
        "CottonWorks definuje pigment ako nerozpustnú časticu bez afinity k substrátu, preto je potrebné spojivo. Pri vytvrdnutí vzniká polymérna sieť, ktorá zachytáva pigment na povrchu. Receptúra ovplyvňuje odtieň, mäkkosť, priedušný pocit aj odolnosť. Príliš hrubý film môže pôsobiť tuhšie, kým nedostatočné vytvrdnutie zvyšuje prenos a mechanické zlyhanie.",
        "Farebnú stálosť nemožno zhrnúť jedným slovom. AATCC používa samostatné skúšky prania, vody, potu a crockingu. Suchý a mokrý oter majú rozdielne podmienky a výsledok jedného testu nepreukazuje ostatné. Domáca biela handrička môže upozorniť na problém, ale bez normalizovaného tlaku, času a hodnotiacej stupnice nie je náhradou laboratórneho výsledku.",
        "Pigmentová patina je predovšetkým povrchovo-mechanický vývoj. Preto má význam znížiť zbytočné trenie, no nie sľubovať nemennosť. GINETEX symboly určujú maximálny postup hotového výrobku a európske vláknové označenie opisuje zloženie; ani jeden údaj sám nepovie kvalitu spojiva. Tú musí výrobca overiť na konkrétnom výrobku.",
    ],
    "source_intro": "Zdroje vysvetľujú rozdiel medzi pigmentom a farbivom, potrebu spojiva, skúšky prania a trvalý význam ošetrovacieho štítku. Nepodporujú domáce univerzálne fixovanie farby.",
    "sources": [
        ("CottonWorks: pigment", COTTON_PIGMENT),
        ("CottonWorks: základy farbenia", COTTON_DYEING),
        ("CottonWorks: pigmentová tlač a spojivo", COTTON_PRINTING),
        ("AATCC: prehľad textilných skúšok", AATCC_STANDARDS),
        ("GINETEX: symboly ošetrovania", GINETEX),
        ("EÚ 1007/2011: označovanie textilných vlákien", EU_FIBRE_LABEL),
    ],
    "related": [
        ("Garment-dyed oblečenie", ARTICLE_GARMENT_DYED),
        ("Prečo farby blednú", ARTICLE_COLOR),
        ("Ako zabrániť púšťaniu farby", ARTICLE_BLEEDING),
        ("Ako prať nové oblečenie prvýkrát", ARTICLE_NEW),
        ("Ako čítať ošetrovací štítok", ARTICLE_LABEL),
        ("Ako odstraňovať škvrny", ARTICLE_STAINS),
    ],
    "faq_title": "pigmentovo farbené oblečenie",
    "faq": [
        ("Čo znamená pigmentovo farbené oblečenie?", "Farbu tvoria nerozpustné pigmentové častice upevnené spojivom prevažne na povrchu textilu."),
        ("Je pigment-dyed to isté ako garment-dyed?", "Nie. Pigment opisuje farebný systém, garment-dyed fázu farbenia po ušití. Jeden kus môže byť oboje."),
        ("Prečo pigmentový odev bledne na hranách?", "Vystúpené švy, lakte a vrecká sa viac trú, preto sa povrchová vrstva mení rýchlejšie."),
        ("Je každé blednutie zámerná patina?", "Nie. Ostrá mapa, silný prenos, lepkavosť alebo šupinky môžu znamenať poškodenie."),
        ("Ako ho prať prvýkrát?", "Podľa etikety, naruby, samostatne alebo s veľmi podobnými tmavými farbami a bez dlhého mokrého státia."),
        ("Môžem pigmentovú farbu zafixovať octom?", "Nie univerzálne. Ocot nemusí ovplyvniť spojivo žiaducim spôsobom a môže zmeniť inú časť odevu."),
        ("Pomôže soľ proti oteru?", "Nie ako všeobecný postup. Mechanický oter spojiva sa soľou neopraví."),
        ("Môže ísť odev do sušičky?", "Iba pri povolenom symbole. Teplo a prevaľovanie môžu urýchliť oder alebo poškodiť spojivo a elastan."),
        ("Prečo farbí sedačku?", "Môže mať slabú stálofarebnosť pri suchom alebo vlhkom trení. Kontakt obmedzte a riešte s predajcom."),
        ("Ako odstrániť škvrnu?", "Prípravok skúste na skrytom mieste, krátko pôsobte, nedrhnite a výsledok kontrolujte až suchý."),
        ("Čo znamená lepkavý povrch?", "Môže ísť o zmenené alebo nedostatočne vytvrdené spojivo. Zastavte teplo a ďalšie experimenty."),
        ("Môžem ho žehliť?", "Iba podľa symbolu, ideálne z rubu a bez priameho tlaku na pigmentový motív."),
        ("Kedy odev reklamovať?", "Pri silnom prenose, prudkom mapovitom zosvetlení, lepkavosti alebo odlupovaní po dodržanom postupe."),
    ],
}

add_cards(
    PIGMENT_DYED,
    product_heading="Prací gél pre označený prateľný pigmentovo farbený odev",
    product_limit="Produkt nezafixuje voľný pigment, neopraví poškodené spojivo, nezastaví navrhnutú patinu a neodstráni svetlú mapu po chemickom poškodení.",
)


STONEWASHED: dict[str, object] = {
    "title": "Čo je stonewashed a enzyme-washed denim: čo úpravy znamenajú a ako prať",
    "link": "co-je-stonewashed-a-enzyme-washed-denim-co-upravy-znamenaju-a-ako-prat",
    "meta": "Ako vzniká stonewashed a enzyme-washed denim, čo robí pemza a celuláza, ako odlíšiť patinu od oslabenia a ako bezpečne prať džínsy.",
    "short": "Stonewashed a enzyme-washed denim získava obnosený vzhľad priemyselným oderom, celulázou alebo ich kombináciou. Domáce pranie má výsledok udržiavať, nie výrobnú úpravu opakovať.",
    "name": "stonewashed a enzyme-washed denim",
    "locative": "priemyselne opranom denime",
    "identity_heading": "Stonewash a enzyme wash sú výrobné úpravy hotového denimu",
    "identity_detail": "Stonewashing používa mechanický oder pemzy alebo iného média, kým enzyme washing využíva celulázu na kontrolované narušenie povrchových bavlnených mikrovlákien a uvoľnenie časti indiga.",
    "identity_boundary": "Biostoning denimu môže kombinovať enzým a mechaniku, ale nie je to domáce pridanie enzýmového detergentu ani úplné synonymum všeobecného bioleštenia každého bavlneného textilu.",
    "label_focus": "podiel bavlny a elastanu, tmavý alebo svetlý wash, poškodené efekty, nášivku, nity, zips, povolenú teplotu, pranie naruby, sušičku a upozornenie na prenos farby",
    "missing_label": "Bez údajov výrobcu nemožno podľa vzhľadu určiť, či patinu vytvorila pemza, enzým, laser, chemický discharge alebo kombinácia procesov; voľte najmiernejší postup pre celý odev.",
    "dry_check": "stenčené kolená a sed, zámerné diery, pretrhnuté osnovné nite, ostré kovanie, uvoľnené nity, suchý prenos indiga, skrútené švy, poškodený elastan a tvrdé zvyšky v lemoch",
    "damage_boundary": "Svetlé švy a mäkké prechody môžu patriť k wash efektu, ale rastúca diera, praskajúci elastan, prášivé vlákna a strata pevnosti sú konštrukčné problémy.",
    "test_focus": "Na skrytom mieste overte prenos indiga a porovnajte pevnosť vystavenej plochy s chráneným vnútrom; skúška farby však nesmie zahŕňať drhnutie, ktoré samo vytvorí ďalší stonewash efekt.",
    "combined_risk": "ďalšieho mechanického oderu oslabených miest, uvoľnenia indiga, trenia kovových prvkov, zrážania bavlny, tepelného poškodenia elastanu a zachytenia zámerných dier",
    "chemistry_boundary": "Pemza, abrazívne predmety, chlórové pokusy ani priemyselné enzýmové kúpele nepatria do domácej práčky; môžu poškodiť odev, spotrebič aj ďalšiu bielizeň.",
    "drying_detail": "Džínsy po cykle hneď vyberte, urovnajte švy a pás bez násilného naťahovania a sušte podľa symbolu tak, aby zámerné diery a oslabené kolená neniesli celú mokrú hmotnosť.",
    "heat_boundary": "Horúca sušička môže podporiť zrazenie bavlny, poškodiť elastan, zvýšiť trenie na vystúpených hranách a urýchliť zmenu už priemyselne zoslabenej plochy.",
    "stop_signs": "rýchlo rastúca diera, uvoľnený nit, ostré kovanie, trhajúce sa nite pri jemnom pohybe, silný suchý prenos, gumovité vlákna elastanu alebo prudká nerovnomerná zmena po povolenom cykle",
    "professional_boundary": "Bežné označené džínsy možno prať doma, kým zberateľský denim, ručne zdobený kus, kožené prvky, rozsiahle oslabenie alebo sporná strata pevnosti potrebujú výrobné údaje, opravu či odborné posúdenie.",
    "answer": "Stonewashed denim získava obnosený vzhľad mechanickým oderom, tradične pemzou; enzyme-washed denim využíva celulázu, ktorá kontrolovane pôsobí na povrch bavlny a pomáha uvoľniť farbu. Obe úpravy sa robia vo výrobe za riadených podmienok. Doma ich neopakujte kameňmi, bielidlom ani náhodným enzýmovým kúpeľom. Džínsy pred praním skontrolujte, obráťte naruby, zaistite kovové prvky, perte s podobnými tmavými farbami podľa etikety a hneď vyberte z bubna. Sušičku použite iba pri povolení. Ak sa zámerná diera zväčšuje, elastan praská alebo denim stráca pevnosť, riešte opravu skôr než ďalší cyklus.",
    "intro": "Vypraný vzhľad denimu môže vyzerať ako obyčajné staré džínsy, no často ide o presne nastavenú priemyselnú úpravu. Pemza, celuláza, čas, pH, teplota a pohyb menia farbu aj povrch a pri príliš silnom zásahu aj hmotnosť či pevnosť látky. Spotrebiteľ už dostáva hotový výsledok. Jeho úlohou nie je proces znovu vyrábať, ale obmedziť zbytočné ďalšie namáhanie, chrániť oslabené oblasti a rozlíšiť očakávanú patinu od poškodenia, ktoré sa môže pri praní rýchlo šíriť.",
    "quick": [
        "<strong>Stonewash je riadený oder:</strong> tradične vzniká pohybom pemzy s mokrými džínsami.",
        "<strong>Enzyme wash používa celulázu:</strong> enzým pôsobí na povrch bavlnených vlákien.",
        "<strong>Indigo zvýrazňuje kontrast:</strong> odretie povrchu odhaľuje svetlejšie jadro priadze.",
        "<strong>Domáca práčka nie je finisážna linka:</strong> kamene a silná chémia poškodzujú odev aj spotrebič.",
        "<strong>Patina a pevnosť sú dve otázky:</strong> pekná farba nevylučuje stenčené koleno.",
        "<strong>Elastan mení limity:</strong> strečový denim je citlivejší na teplo a agresívne cykly.",
    ],
    "overview_heading": "Ako vzniká stonewashed a enzyme-washed vzhľad denimu",
    "overview": [
        "Klasický modrý denim má indigo najmä na povrchu osnovných priadzí a svetlý útok. Pri mechanickom odieraní sa z vystúpených miest odstráni časť zafarbeného povrchu a ukáže sa svetlejšia vrstva. Pemza sa v priemyselnom rotačnom zariadení pohybuje spolu s odevmi a vytvára nerovnomerný, no kontrolovaný kontrast na švoch, lemoch a záhyboch. Intenzita závisí od média, času, množstva, vlhkosti a mechaniky.",
        "Celluláza štiepi väzby v celulóze na povrchu bavlnených vlákien. V správne nastavenom procese uvoľní mikrovlákna a časť indiga s menším množstvom kameňov alebo bez nich. Výsledok ovplyvňuje typ enzýmu, pH, teplota, čas a deaktivácia. Výskum ukazuje, že zmena vzhľadu ide spolu so zmenami hmotnosti, tuhosti a podľa závažnosti aj pevnosti, preto sa priemyselný recept nedá bezpečne improvizovať doma.",
        "Po úprave sa odev oplachuje, odstraňuje sa uvoľnený materiál a rieši sa backstaining, teda opätovné usadenie odretého indiga na svetlejšie miesta. Hotové džínsy môžu dostať ďalšie efekty laserom, lokálnym brúsením, zosvetľujúcou chémiou alebo opravou dier. Obchodný názov wash preto nie vždy prezradí presnú sekvenciu; pre domácu starostlivosť je rozhodujúci štítok a fyzický stav konkrétneho kusu.",
    ],
    "table1_heading": "Stonewash, enzyme wash, rinse wash a prirodzené nosenie",
    "table1_intro": "Vzhľad môže byť podobný, mechanizmus a počiatočné oslabenie však nie. Názov je orientačný, kým štítok a stav odevu rozhodujú prakticky.",
    "table1_headers": ["Úprava", "Hlavný mechanizmus", "Typický výsledok", "Domáca hranica"],
    "table1_rows": [
        ("Stonewash", "Mechanický oder pemzou alebo iným médiom.", "Svetlejšie švy, hrany a obnosený povrch.", "Proces doma neopakovať kameňmi."),
        ("Enzyme wash / biostoning", "Kontrolované pôsobenie celulázy na bavlnu.", "Zmäkčenie a uvoľnenie časti indiga.", "Detergent nie je priemyselný finisážny kúpeľ."),
        ("Stone-enzyme wash", "Kombinácia mechaniky a enzýmu.", "Výraznejší efekt pri optimalizovaných podmienkach.", "Sledovať pevnosť oslabených miest."),
        ("Rinse wash", "Odstránenie veľkosti a zmäkčenie s malým oderom.", "Tmavší a rovnomernejší vzhľad.", "Stále môže prenášať indigo."),
        ("Prirodzené nosenie", "Opakované ohýbanie, trenie, pranie a svetlo.", "Osobná patina podľa pohybu tela.", "Opraviť stenčenie skôr, než sa zmení na dieru."),
    ],
    "sections": [
        {
            "heading": "Čím sa enzyme-washed denim líši od bioleštenej bavlny",
            "paragraphs": [
                f"Oba procesy môžu používať celulázu, no cieľ a substrát nie sú rovnaké. <a href=\"{ARTICLE_BIOPOLISH}\">Bioleštenie bavlny</a> sa sústreďuje na odstránenie povrchového chlpku a zlepšenie vzhľadu či žmolkovania. Enzyme wash denimu cielene využíva štruktúru indigo priadze na wash efekt a môže meniť odtieň, mäkkosť aj kontrast švov.",
                "Pre spotrebiteľa z toho nevyplýva, že každý enzýmový prací prostriedok bude pokračovať v kontrolovanom efekte. Priemyselná operácia nastavuje typ enzýmu, aktivitu, pH, teplotu, pohyb a čas a potom reakciu zastaví. Domáce opakované namáhanie nemá rovnakú kontrolu a na už zoslabenej ploche môže skôr urýchliť opotrebenie.",
            ],
        },
        {
            "heading": "Prečo sa švy, vrecká a lemy zosvetlia viac",
            "paragraphs": [
                "Šev má viac vrstiev a vystúpený profil, preto sa o kameň alebo bubon opiera intenzívnejšie než rovná plocha. Lem, okraj vrecka a pútko na opasok sa počas výroby aj nosenia opakovane ohýbajú. Indigo sa na povrchu priadze odiera a kontrast zvýrazní biely útok alebo svetlejšie jadro. Rovnomerné svetlé hrany preto môžu byť súčasťou navrhnutého vzhľadu.",
                "Ostrá diera pri nite, pretrhnutie na jednom nite alebo kovový nit, ktorý sa vytrháva, už nie sú iba farebný efekt. Pred praním skontrolujte rukou bez silného ťahu, či denim pri šve drží. Ak je plocha papierovo tenká, opravte ju skôr, než pohyb vo vode zväčší otvor.",
            ],
        },
        {
            "heading": "Ako skontrolovať pevnosť starších stonewashed džínsov",
            "paragraphs": [
                "Džínsy rozložte a prezrite proti rozptýlenému svetlu. Tenké miesta na kolenách, medzi stehnami, pri zadných vreckách a pod pásom môžu prepúšťať viac svetla. Sledujte tiež biele nitky elastanu, vrásnenie okolo šva a malé priečne praskliny. Nevykonávajte silnú skúšku ťahom; na oslabenej ploche by ste sami vytvorili trhlinu.",
                "Porovnajte exponované miesto s vnútornou plochou rovnakého dielu. Ak sa niť pri jemnom pohybe drobí alebo kovanie nemá pevný podklad, pranie odložte a zvoľte opravu. Záplata alebo podloženie pred otvorením diery zachová viac pôvodnej tkaniny než zásah po roztrhnutí.",
            ],
            "callout": {
                "title": "Miesta, ktoré skontrolujte pred každým praním",
                "items": [
                    "Vnútorné stehná a rozkrok, kde sa kombinuje pot, napätie a trenie.",
                    "Kolená a sed, ktoré už mohli byť zosvetlené priemyselne.",
                    "Rohy zadných vreciek, pútka, zips a okolie nitov.",
                    "Zámerné diery a strapce, ktoré sa môžu zachytiť o inú bielizeň.",
                ],
                "background": "#fffaf5",
                "border": "#e6ded2",
            },
        },
        {
            "heading": "Ako prať stonewashed džínsy bez zrýchlenia poškodenia",
            "paragraphs": [
                f"Riaďte sa etiketou a praktickým návodom <a href=\"{ARTICLE_DENIM}\">ako prať tmavé džínsy a rifľovú bundu</a>. Vyprázdnite vrecká, zaistite zips a gombík, obráťte kus naruby a oddeľte ho od svetlej bielizne. Drsné uteráky, háčiky a kovové ozdoby zvyšujú trenie; nový sýty denim môže navyše preniesť farbu.",
                "Bubon nepreplňte, ale neperte ani jeden ťažký kus v podmienkach, pri ktorých prudko udiera o stenu. Zvoľte program, teplotu a otáčky zo štítku. Presná dávka a dobrý oplach sú dôležitejšie než veľa produktu. Pri oslabených dierach použite dostatočne veľký vak len vtedy, ak nebude odev stlačený a voda ním bude prechádzať.",
            ],
        },
        {
            "heading": "Prečo do domácej práčky nepatrí pemza ani abrazívne predmety",
            "paragraphs": [
                "Priemyselné stonewashing zariadenie je navrhnuté na médium, prach, kal a následné odstránenie kameňov. Domáca práčka nemá rovnakú konštrukciu ani procesnú kontrolu. Pemza sa môže rozpadnúť, upchať cesty, poškriabať bubon, poškodiť tesnenie a uviaznuť vo vreckách. Kovová guľa, brúsna špongia či iná improvizácia pridáva nekontrolovaný náraz.",
                "Domáci výsledok navyše nebude reprodukovateľný. Odev sa môže zodrať pri jednom šve a zostať tmavý inde. Silná chémia môže vytvoriť oranžovú alebo žltú mapu, oslabiť celulózu a poškodiť elastan. Ak chcete svetlejší odtieň, bezpečnejší je nákup hotového wash efektu než pokus premeniť existujúci kus v spotrebiči.",
            ],
        },
        {
            "heading": "Backstaining: prečo svetlé miesta po praní znovu zosivejú alebo zmodrajú",
            "paragraphs": [
                "Pri odieraní denimu sa uvoľní indigo a vláknitý materiál. Ak sa neudrží rozptýlený a neodstráni oplachom, môže sa znovu usadiť na svetlejších útkových nitiach, vreckových vakoch alebo už vyblednutých plochách. Tento jav sa nazýva backstaining. V priemysle sa kontroluje receptúrou, pohybom, oplachom a načasovaním procesu.",
                "Doma znížite riziko vhodným triedením, nepreplneným bubnom, primeranou dávkou a úplným oplachom. Zachytávač farby nie je záruka a nevyrieši mechanické poškodenie. Ak sa svetlý detail po jednom povolenom praní výrazne zafarbí, stav zdokumentujte a ďalší experiment odložte.",
            ],
        },
        {
            "heading": "Strečový denim potrebuje prísnejšiu kontrolu tepla",
            "paragraphs": [
                "Elastan dodáva džínsom pružnosť, no je citlivejší na vysoké teploty, oxidáciu a dlhodobé mechanické namáhanie než bavlnený základ. Stonewash efekt môže už zvýrazniť ohýbané miesta a tesný strih ďalej zaťažuje vlákna na kolenách, sedacej časti a páse. Praskajúce biele vlákna alebo vlnitý povrch signalizujú, že pružná zložka stráca funkciu.",
                "Horúca voda ani sušička nie sú bezpečná metóda na zmenšenie voľných strečových džínsov. Výsledok môže byť nerovnomerný a krátkodobý, zatiaľ čo elastan sa poškodí. Dodržte symboly, kus nepresušujte a pri ukladaní ho nestláčajte do ostrého skladu cez oslabené koleno.",
            ],
        },
        {
            "heading": "Odstraňovanie škvŕn zo zosvetleného denimu",
            "paragraphs": [
                f"Na svetlom wash efekte sa ľahko prehliadne lokálne odfarbenie. Najprv odstráňte pevný alebo mastný prebytok a postupujte podľa typu škvrny; všeobecný rámec je v článku <a href=\"{ARTICLE_STAINS}\">ako odstrániť rôzne škvrny z oblečenia</a>. Prípravok skúste na vnútornej strane a nedrhnite kefou, ktorá by vytvorila nový svetlý ostrov.",
                "Mokrá bavlna stmavne, preto miesto hodnotíte až suché. Bielidlo môže reagovať s indigom prudko a zanechať ostrý farebný posun. Ak etiketa jeho použitie nepovoľuje, neaplikujte ho. Pri oleji alebo živici radšej opakujte mierny kompatibilný postup než jeden agresívny zásah.",
            ],
        },
        {
            "heading": "Sušenie a skladovanie bez deformácie a zatuchnutia",
            "paragraphs": [
                f"Po cykle džínsy ihneď vyberte, urovnajte švy a zabezpečte vzduch z oboch strán. Hrubý pás, vrecká a viacvrstvový lem schnú pomalšie než stehno; praktické zásady nájdete v návode <a href=\"{ARTICLE_DRYING}\">ako sušiť bielizeň bez zatuchnutia</a>. Mokré džínsy nevešajte za jednu oslabenú slučku a neskladajte na seba.",
                "Úplne suchý kus skladujte zložený bez tlaku na kovanie alebo na pevnom vešiaku podľa hmotnosti. Zámerné diery neprekladajte cez ostrý okraj police. Ak sa objaví zatuchnutie, riešte úplné pranie a sušenie, nie prevoňanie uzavretého vlhkého denimu.",
            ],
        },
    ],
    "table2_heading": "Stonewashed džínsy sa menia: patina, prenos alebo oslabenie",
    "table2_intro": "Farbu a pevnosť posudzujte oddelene. Svetlejší povrch môže byť v poriadku, aj keď konštrukcia potrebuje opravu.",
    "table2_headers": ["Prejav", "Možná príčina", "Čo overiť", "Bezpečný ďalší krok"],
    "table2_rows": [
        ("Švy sú rovnomerne svetlejšie", "Pôvodný stonewash alebo bežný oder.", "Fotografie nového kusu a symetriu efektu.", "Pokračovať šetrne podľa etikety."),
        ("Svetlé nite znovu modrajú", "Backstaining uvoľneným indigom.", "Triedenie, dávku, náplň a oplach.", "Oddeliť tmavé kusy a nepreplniť bubon."),
        ("Koleno prepúšťa svetlo", "Stenčenie po výrobe a nosení.", "Pevnosť susedných nití bez silného ťahu.", "Podložiť a opraviť pred ďalším praním."),
        ("Biele elastické vlákna vyčnievajú", "Poškodenie elastanu teplom alebo namáhaním.", "Zloženie a históriu sušičky.", "Zastaviť teplo; zvážiť opravu alebo výmenu."),
        ("Vznikla ostrá oranžová mapa", "Chemické odfarbenie alebo oxidácia indiga.", "Tvar mapy a použitý prípravok.", "Nepridávať ďalšiu chémiu; zdokumentovať."),
    ],
    "steps_heading": "Ako prať stonewashed alebo enzyme-washed džínsy krok za krokom",
    "steps": [
        "Prečítajte zloženie, symboly, upozornenie na farbu a pokyny k wash efektu.",
        "Skontrolujte kolená, rozkrok, rohy vreciek, pútka, nity a zámerné diery.",
        "Vyprázdnite vrecká, zaistite zips a kovové prvky a odev obráťte naruby.",
        "Oddeľte ho od svetlej bielizne, drsných uterákov a predmetov s háčikmi.",
        "Zvoľte povolený program, teplotu a otáčky a použite presnú dávku.",
        "Po cykle džínsy ihneď vyberte, bez krútenia urovnajte švy a pás.",
        "Sušte podľa symbolu s oporou oslabených miest a bez zbytočného tepla.",
        "Po vyschnutí znovu skontrolujte pevnosť; rastúce poškodenie opravte pred nosením.",
    ],
    "remember": [
        "Je názov stonewash alebo enzyme wash potvrdený, alebo ho odhadujete podľa vzhľadu?",
        "Obsahuje denim elastan, potlač, koženú nášivku alebo oslabené ozdobné diery?",
        "Držia pútka, nity, zips a rohy vreciek pevne v tkanine?",
        "Je zmena iba farebná, alebo je plocha aj tenšia a mechanicky slabšia?",
        "Má nový sýty kus priestor na pranie oddelene od svetlej bielizne?",
        "Vyschli pás, vrecká, lemy a rozkrok úplne pred uložením?",
    ],
    "mistakes": [
        "Vkladať pemzu alebo iné abrazívne predmety do domácej práčky.",
        "Pokúšať sa vytvoriť wash efekt chlórom alebo náhodným enzýmovým kúpeľom.",
        "Hodnotiť iba farbu a ignorovať stenčenie kolena či rozkroku.",
        "Prať zámerne roztrhané džínsy s háčikmi, zipsami a drsnými uterákmi.",
        "Sušiť strečový denim vysokým teplom v snahe upraviť veľkosť.",
        "Nechať mokré tmavé džínsy dlho na svetlom textile.",
    ],
    "expert_heading": "Odbornejší pohľad: celuláza, indigo, backstaining a pevnosť",
    "expert": [
        "Celulázy nie sú jedna identická látka. Rôzne enzýmové zložky pôsobia na celulózový povrch odlišne a výskum na denime porovnáva ich schopnosť vytvoriť stonewash efekt pri rôznom stupni hydrolýzy. Proces musí vyvážiť požadované zosvetlenie s úbytkom hmotnosti, pevnosti a nežiaducim poškodením. Z toho nemožno odvodiť bezpečný domáci recept.",
        "Indigo sa pri tradičnom denime sústreďuje na povrchu osnovnej priadze. Mechanika alebo enzým odstráni farebný povrch a odhalí svetlejšie vrstvy. Uvoľnený pigment a vláknité častice sa môžu znovu usadiť, preto je backstaining dôležitý výrobný parameter. AATCC skúšky prania a crockingu hodnotia iné vlastnosti a výsledky nemožno zlučovať do jedného dojmu.",
        "Štúdie stone-enzyme procesov zaznamenávajú súčasné zmeny farby, tuhosti, rozmerov, hmotnosti a mechanických vlastností. Hotový výrobok teda môže začínať na inom bode životnosti než tmavý rinse-wash denim. GINETEX etiketa zostáva maximálnym povoleným domácim postupom a stav konkrétneho kusu môže vyžadovať ešte miernejšiu starostlivosť.",
    ],
    "source_intro": "Zdroje dokumentujú priemyselné stonewashing a enzyme-washing procesy, účinok celulázy a skúšky farby. Nepodporujú domáce používanie pemzy ani improvizované chemické starnutie.",
    "sources": [
        ("CottonWorks: stone washed", COTTON_STONE),
        ("CottonWorks: denim", COTTON_DENIM),
        ("CottonWorks: garment finishing", COTTON_GARMENT_FINISHING),
        ("PubMed Central: celulázy a stonewash efekt", DENIM_CELLULASE),
        ("NC State JTATM: stone-enzyme úprava denimu", DENIM_STONE_ENZYME),
        ("AATCC TM61: stálofarebnosť pri praní", AATCC_LAUNDERING),
        ("GINETEX: symboly ošetrovania", GINETEX),
    ],
    "related": [
        ("Ako prať tmavé džínsy", ARTICLE_DENIM),
        ("Čo je bioleštená bavlna", ARTICLE_BIOPOLISH),
        ("Prečo sa oblečenie žmolkuje", ARTICLE_PILLING),
        ("Prečo farby blednú", ARTICLE_COLOR),
        ("Ako čítať ošetrovací štítok", ARTICLE_LABEL),
        ("Ako sušiť bielizeň bez zatuchnutia", ARTICLE_DRYING),
    ],
    "faq_title": "stonewashed a enzyme-washed denim",
    "faq": [
        ("Čo znamená stonewashed denim?", "Denim bol pri výrobe mokro mechanicky odieraný, tradične pemzou, aby získal mäkší a obnosený vzhľad."),
        ("Čo znamená enzyme-washed denim?", "Pri výrobe sa použil enzým, najčastejšie celuláza, na kontrolované pôsobenie na bavlnený povrch a farbu."),
        ("Je biostoning to isté ako bioleštenie?", "Nie úplne. Môžu používať podobnú enzýmovú rodinu, no denimový proces cieli aj na wash efekt indiga."),
        ("Môžem dať pemzu do práčky?", "Nie. Môže poškodiť odev, bubon, tesnenie a odtok a vytvorí nekontrolovaný výsledok."),
        ("Môžem enzyme wash zopakovať pracím gélom?", "Bežný prací cyklus nie je riadená priemyselná finisáž a nemá slúžiť na výrobu ďalšieho efektu."),
        ("Prečo sú švy svetlejšie?", "Vystupujú a pri priemyselnom aj bežnom trení strácajú povrchové indigo rýchlejšie."),
        ("Ako džínsy prať prvýkrát?", "Podľa etikety, naruby, oddelene alebo s podobnými tmavými farbami a s okamžitým vybratím po cykle."),
        ("Čo je backstaining?", "Opätovné usadenie uvoľneného indiga a vlákien na svetlejšie miesta denimu."),
        ("Môžu ísť do sušičky?", "Iba pri povolenom symbole; teplo a trenie môžu podporiť zrazenie, poškodenie elastanu a ďalší oder."),
        ("Ako zistím, že denim je príliš tenký?", "Proti svetlu vidno oslabenie, nite sa pri jemnom pohybe oddeľujú alebo sa pri šve objavujú malé trhliny."),
        ("Ako prať roztrhané džínsy?", "Skontrolujte a prípadne podložte diery, chráňte ich pred zachytením a použite iba povolený mierny program."),
        ("Prečo strečové džínsy strácajú tvar?", "Elastan môže starnúť teplom, trením a napätím; wash úprava základnej bavlny ho pred tým nechráni."),
        ("Kedy džínsy opraviť?", "Keď je plocha stenčená, dierka rastie, pútko sa vytrháva alebo kovanie už nemá pevný podklad."),
    ],
}

add_cards(
    STONEWASHED,
    product_heading="Prací gél pre označený prateľný stonewashed denim",
    product_limit="Produkt nevytvára bezpečný stonewash efekt, nenahrádza priemyselný enzýmový proces, neopraví stenčené vlákna a nezastaví všetok prenos indiga.",
)


PEACH_FINISH: dict[str, object] = {
    "title": "Čo je peach finish alebo brúsený povrch: jemnosť, lesk a starostlivosť",
    "link": "co-je-peach-finish-alebo-bruseny-povrch-jemnost-lesk-a-starostlivost",
    "meta": "Čo je peach finish a peached fabric, ako vzniká jemný brúsený povrch, čím sa líši od flanelu a ako ho prať bez máp, žmolkov a sploštenia.",
    "short": "Peach finish je jemná mechanická úprava, pri ktorej abrazívne valce vytvoria na povrchu textilu krátky mäkký chlp. Neurčuje vláknové zloženie ani automatický prací program.",
    "name": "peach finish alebo jemne brúsený textil",
    "locative": "jemne brúsenom peached povrchu",
    "identity_heading": "Peach finish je povrchová úprava, nie nové vlákno",
    "identity_detail": "Peaching alebo jemné sueding vedie tkaninu či úplet cez abrazívne valce, ktoré kontrolovane narušia povrch priadzí a vytvoria veľmi krátky mäkký chlp pripomínajúci pokožku broskyne.",
    "identity_boundary": "Peached polyester, bavlna a zmes môžu na dotyk pôsobiť podobne, no ich savosť, teplotná odolnosť, farbenie a sušenie sa riadia vláknami a konštrukciou, nie slovom peach.",
    "label_focus": "vláknové percentá, jednostranné alebo obojstranné brúsenie, membránu či záter, elastan, potlač, podšívku, povolenú teplotu, odstreďovanie, sušičku a žehlenie",
    "missing_label": "Jemný dotyk nerozlíši polyester od bavlny ani brúsenie od jemného nappingu; bez etikety nepoužívajte teplo, bielidlo či tvrdú kefu iba podľa pocitu z povrchu.",
    "dry_check": "rozdielny smer chĺpku, hladké lesklé miesta, žmolky, vyčesané či lysé plochy, olejové mapy, zachytené vlákna, poškodený záter, uvoľnený šev a lokálnu zmenu od predchádzajúceho čistenia",
    "damage_boundary": "Zmena odrazu pri prečesaní rukou môže byť normálna, kým trvalo hladká lesklá plocha, stenčenie, hrudky žmolkov alebo odlupujúci sa podklad predstavujú skutočnú zmenu povrchu.",
    "test_focus": "Skúšobné miesto posudzujte v oboch smeroch chĺpku a pri rovnakom svetle; inak môže nepoškodená plocha vyzerať tmavšia iba preto, že krátke vlákna ležia opačne.",
    "combined_risk": "zaplavenia krátkeho chĺpku produktom, lokálneho trenia, zachytenia o drsné zipsy, tvorby žmolkov, tepelného sploštenia a pri syntetike aj uvoľnenia vláknitých fragmentov",
    "chemistry_boundary": "Zmäkčovadlo, odmasťovač ani vyššia dávka gélu neobnovia odbrúsené vlákna; zvyškový film môže naopak zmeniť smerovanie chĺpku, omak a zachytávanie nečistôt.",
    "drying_detail": "Textil sušte bez pevného preloženia mokrých plôch na seba, zabezpečte vzduch pri švoch a po vyschnutí krátky chlp iba jemne urovnajte čistou rukou alebo spôsobom potvrdeným výrobcom.",
    "heat_boundary": "Silná sušička, radiátor alebo horúca žehlička môže povrch sploštiť, vytvoriť lesklý odtlačok, zraziť bavlnený základ alebo poškodiť polyester, elastan či skrytý záter.",
    "stop_signs": "rastúca lysá plocha, prášivé alebo odpadávajúce vlákna, odlupovanie podkladu, ostrá farebná mapa, lepkavosť, silné žmolkovanie po jednom cykle alebo poškodenie funkčnej vrstvy",
    "professional_boundary": "Jednoduchú označenú košeľu či posteľnú textíliu možno prať doma, kým čalúnenie, membrána, lepený laminát, hodnotný odev alebo neznáme viacvrstvové vyhotovenie potrebuje výrobné údaje či odborné čistenie.",
    "answer": "Peach finish je jemné brúsenie povrchu tkaniny alebo úpletu. Rýchle abrazívne valce vytvoria krátky mäkký chlp, ktorý mení dotyk aj odraz svetla. Neznamená to, že materiál je bavlna, semiš alebo flanel, a neurčuje jednu teplotu prania. Najprv čítajte zloženie a štítok, potom povrch skontrolujte v oboch smeroch chĺpku. Pri povolenom praní kus oddeľte od zipsov a drsných uterákov, použite presnú dávku, nepreplňte bubon a po cykle ho hneď vyberte. Škvrny netrite do hladkého kruhu a povrch nežehliť silným tlakom z líca. Trvalá lysá mapa alebo odpadávajúce vlákna sa ďalším praním neopravujú.",
    "intro": "Peached košeľa, obliečka alebo ľahká bunda pôsobí mäkko a teplo, hoci chlp môže byť sotva viditeľný. Práve táto jemnosť vedie k častému omylu: používateľ predpokladá, že ide o konkrétnu mäkkú surovinu, a zvolí program podľa dotyku. V skutočnosti môže byť základ bavlnený, polyesterový, viskózový alebo zmesový a brúsenie je iba posledná mechanická úprava. Starostlivosť musí chrániť krátke vystupujúce vlákna, farbu aj prípadnú skrytú membránu či záter. Najväčším rizikom býva lokálne trenie, nesprávne svetelné hodnotenie a film po nadmernom dávkovaní.",
    "quick": [
        "<strong>Peach finish je mechanická úprava:</strong> povrch vytvára jemný kontakt s abrazívnymi valcami.",
        "<strong>Zloženie môže byť rôzne:</strong> bavlna, polyester aj zmesi môžu byť peached.",
        "<strong>Smer chĺpku mení farbu:</strong> tmavšia mapa nemusí byť škvrna ani vyblednutie.",
        "<strong>Drsné kusy oddeľte:</strong> zipsy, suchý zips a uteráky zvyšujú oder aj žmolkovanie.",
        "<strong>Škvrnu nekrúžte kefou:</strong> lokálne sploštenie môže zostať viditeľnejšie než pôvodná nečistota.",
        "<strong>Viac gélu nezmäkčuje povrch:</strong> zvyškový film mení omak a zachytáva prach.",
    ],
    "overview_heading": "Ako brúsenie vytvorí krátky broskyňový chlp",
    "overview": [
        "Pri mechanickom sueding alebo sanding sa otvorená tkanina či úplet vedie cez valce pokryté jemným abrazívnym povrchom. Valce sa pohybujú inou rýchlosťou než textil a kontrolovane zachytia alebo prerežú časť povrchových mikrovlákien. Výrobca nastavuje zrno, tlak, počet kontaktov, napätie a stranu úpravy. Cieľom pri peach finish nie je dlhý huňatý vlas, ale rovnomerný veľmi krátky chlp a mäkší omak.",
        "Účinok závisí od priadze a konštrukcie. Staplová bavlnená priadza už obsahuje konce vlákien, zatiaľ čo filamentový polyester sa pri brúsení musí kontrolovane narušiť. Príliš silná operácia môže znižovať hmotnosť, meniť pevnosť alebo vytvoriť nerovnosť; príliš slabá neposkytne rovnomerný dotyk. Hotový povrch sa preto neposudzuje iba rukou, ale aj vzhľadom, odolnosťou proti žmolkovaniu, oderu a ďalšiemu praniu.",
        "Krátke vlákna odrážajú svetlo podľa smeru. Keď po povrchu prejdete rukou, jedna plocha môže vyzerať svetlejšia a druhá tmavšia bez akejkoľvek zmeny farbiva. Tento optický jav komplikuje hodnotenie škvŕn a lokálneho čistenia. Vždy porovnávajte pri rovnakom rozptýlenom svetle a povrch jemne urovnajte rovnakým smerom až po úplnom vysušení.",
    ],
    "table1_heading": "Peach finish, napping, flanel, moleskin a pravý semiš",
    "table1_intro": "Mäkký dotyk môže vzniknúť rôznymi konštrukciami. Názov porovnávajte so zložením, dĺžkou vlasu a rubom výrobku.",
    "table1_headers": ["Povrch", "Ako vzniká", "Typický znak", "Hranica starostlivosti"],
    "table1_rows": [
        ("Peach finish / sueding", "Jemné abrazívne brúsenie povrchu.", "Veľmi krátky rovnomerný mäkký chlp.", "Chrániť pred lokálnym trením a tlakom."),
        ("Napping / počesanie", "Drôtené valce vyťahujú dlhší vlas.", "Plnší, teplejší, viditeľnejšie chlpatý povrch.", "Riziko splstenia a žmolkov podľa materiálu."),
        ("Flanel", "Tkanina alebo úplet môže byť jednostranne či obojstranne počesaný.", "Hrejivý mäkký povrch; názov nie je jedno vlákno.", "Riaďte sa zložením a hustotou."),
        ("Moleskin", "Hustá tkanina s brúseným lícom.", "Plnší pevný základ a semišový vzhľad.", "Ťažší kus schne pomalšie."),
        ("Pravý semiš", "Kožený materiál, nie textilné brúsenie.", "Štruktúra kože a osobitný návod.", "Neperie sa podľa pravidiel peached textilu."),
    ],
    "sections": [
        {
            "heading": "Ako identifikovať peached fabric bez spoliehania sa na marketing",
            "paragraphs": [
                "Hľadajte údaje peach finish, peached, peach skin, sueded alebo sanded spolu s vláknovým zložením. Povrch býva jemne matný a po prečesaní mení odraz, no nie každý mäkký textil je brúsený. Jemný flanel môže mať dlhší vytiahnutý vlas, moleskin hustejší základ a mikrovlákno môže pôsobiť zamatovo už konštrukciou priadze.",
                "Prezrite rub a rez v skrytom šve. Jednostranné brúsenie bude výrazné iba na líci, kým obojstranná úprava mení obe plochy. Z dotyku však nezistíte presné vlákno ani skrytý záter. Ak je výrobok bunda, poťah alebo viacvrstvová textília, ošetrovací návod celku má väčšiu váhu než názov povrchu.",
            ],
        },
        {
            "heading": "Peach finish verzus flanel a moleskin v každodennej praxi",
            "paragraphs": [
                f"Pri flaneli sa mäkkosť často spája s počesaním, ktoré vytiahne viditeľnejší vlas a zvyšuje vzduch zachytený pri povrchu. Peach finish býva jemnejší a kratší. <a href=\"{ARTICLE_MOLESKIN}\">Moleskin</a> je hustá tkanina so silnejším základom a brúseným lícom. Tieto rozdiely ovplyvňujú hmotnosť, schnutie, žmolkovanie aj odolnosť proti deformácii.",
                "Obchodné názvy sa používajú voľne, preto sa nesnažte odvodiť teplotu z porovnania s iným kusom v skrini. Ľahká peached polyesterová blúzka môže potrebovať nízke teplo, kým bavlnená košeľovina povoľuje iný cyklus. Rozhoduje etiketa konkrétneho výrobku a najcitlivejšia vrstva.",
            ],
        },
        {
            "heading": "Ako povrch prezrieť pred praním a odlíšiť mapu od smeru chĺpku",
            "paragraphs": [
                "Položte suchý kus na rovnú plochu pri rozptýlenom dennom svetle. Čistou rukou prejdite malú oblasť jedným a potom opačným smerom. Ak sa tmavšia plocha presúva spolu so smerom chĺpku, ide pravdepodobne o optický efekt. Skutočná škvrna zostáva na rovnakom mieste a môže mať iný omak alebo okraj.",
                "Zaznamenajte lysé lesklé miesta na lakťoch, golieri a bokoch, kde vzniká trenie. Skontrolujte aj žmolky, vytiahnuté nite a rub, či nie je poškodený záter. Pred namočením odfoťte problematické miesto z viacerých uhlov; mokrý chlp sa položí a na chvíľu zmení celý vzhľad.",
            ],
            "callout": {
                "title": "Rýchle rozlíšenie troch podobných prejavov",
                "items": [
                    "Mapa sa mení po jemnom prečesaní: pravdepodobne smer krátkeho chĺpku.",
                    "Miesto je mastné alebo lepivé: najprv riešte konkrétnu škvrnu.",
                    "Plocha je trvalo hladká a lesklá: povrch mohol byť sploštený oderom alebo teplom.",
                    "Z povrchu sa oddeľujú hrudky: skontrolujte žmolkovanie a pevnosť priadze.",
                ],
                "background": "#f7fbf8",
                "border": "#dbe5de",
            },
        },
        {
            "heading": "Ako prať peached košeľu, šaty alebo obliečky",
            "paragraphs": [
                "Najprv roztrieďte podľa farby a vláknového zloženia. Zapnite zipsy, odstráňte predmety z vreciek a kus obráťte naruby, ak to výrobca odporúča. Nekombinujte ho s otvoreným suchým zipsom, drsným uterákom alebo pracovným odevom s kovovými hranami. Pri posteľnej bielizni zapnite uzáver, aby sa vo vnútri nehromadili ďalšie kusy.",
                "Použite program, teplotu a otáčky zo štítku. Bubon nechajte dostatočne voľný, aby sa povrch opláchol, no nespoliehajte sa na extrémne malú náplň ako ochranu; vyšší pomer vody a mechanika môžu tiež ovplyvniť uvoľnenie vlákien. Dávku odmerajte podľa vody a náplne a nepridávajte zmäkčovadlo, ak ho výrobca neodporúča.",
            ],
        },
        {
            "heading": "Ako odstrániť mastnú alebo farebnú škvrnu bez lysého kruhu",
            "paragraphs": [
                f"Odoberte prebytok a zvoľte postup podľa pôvodu škvrny; všeobecný rámec ponúka článok <a href=\"{ARTICLE_STAINS}\">ako odstraňovať škvrny z oblečenia</a>. Pod miesto vložte savú čistú vrstvu a pracujte od okraja. Namiesto krúživého drhnutia jemne prikladajte alebo prepracujte roztok spôsobom povoleným výrobcom.",
                "Prípravok skúste na skrytom šve a sledujte nielen farbu, ale aj smer chĺpku, lesk a tuhosť po vyschnutí. Silná kefa môže odstrániť krátke vlákna a vytvoriť svetlý lesklý kruh. Ak zostane olejový tieň, opakovaný mierny krok je bezpečnejší než prudké zvýšenie koncentrácie.",
            ],
        },
        {
            "heading": "Žmolky, sploštenie a lesklé miesta majú rozdielne príčiny",
            "paragraphs": [
                f"Žmolok vzniká, keď sa uvoľnené vlákna zachytia a zvinú do uzlíka; podrobnejší mechanizmus opisuje článok <a href=\"{ARTICLE_PILLING}\">prečo sa oblečenie žmolkuje</a>. Sploštenie je uloženie krátkeho chĺpku do jedného smeru a môže sa čiastočne upraviť po šetrnom praní či urovnaní. Lesklá lysá plocha znamená, že vystupujúce vlákna sa zodrali alebo zatlačili.",
                "Textilný strojček používajte iba na úplne suchom pevne podloženom kuse a bez tlaku. Pri jemnom peached povrchu môže ľahko odstrániť aj pôvodný chlp, preto najprv testujte skryté miesto. Tvrdá kefa alebo holiaca čepeľ nie sú vhodné na plošné oživenie; stratený povrch mechanicky nevrátia.",
            ],
        },
        {
            "heading": "Sušenie a žehlenie bez odtlačku a sploštenia",
            "paragraphs": [
                f"Po praní kus hneď vyberte a rozložte tak, aby neležal mokrý v ostrom sklade. Dodržte symbol sušenia a zabezpečte prúdenie vzduchu pri švoch; viac rád je v článku <a href=\"{ARTICLE_DRYING}\">ako sušiť bielizeň bez zatuchnutia</a>. Na radiátor nedávajte lícnu plochu priamo, pretože lokálny tlak a teplo môžu vytvoriť odtlačok.",
                "Ak je žehlenie povolené, pracujte z rubu pri najnižšej účinnej teplote a použite čistú ochrannú textíliu. Silný tlak z líca splošťuje krátky chlp a mení lesk. Para nemusí byť vhodná pre každý záter alebo zmes; symbol a skúška na skrytom mieste majú prednosť pred všeobecným odporúčaním.",
            ],
        },
        {
            "heading": "Peached syntetika, mikrovlákna a poctivé hranice tvrdení",
            "paragraphs": [
                "Brúsenie syntetického filamentového povrchu môže vytvoriť jemné vystupujúce segmenty, preto sa pri praní prirodzene otvára otázka uvoľňovania vlákien. Peer-reviewed štúdia domácich podmienok však ukazuje, že výsledok závisí od mnohých vlastností tkaniny a prania a žiadny jediný parameter nemá univerzálnu prevahu. V skúmanom páre dokonca peached zmes uvoľnila menej než porovnateľná nebrúsená verzia.",
                "Zodpovedná rada preto nie je sľub, že každý brúsený textil púšťa viac alebo menej. Dodržte plnú, nie preťaženú náplň podľa spotrebiča, vhodný cyklus, opravujte poškodenia a kupujte kvalitný výrobok s jasným zložením. Filtračné pomôcky môžu zachytiť časť fragmentov, ale nemenia potrebu správneho prania a dlhej životnosti odevu.",
            ],
        },
        {
            "heading": "Ako peached textil skladovať a kedy ho reklamovať",
            "paragraphs": [
                "Uložte ho čistý a úplne suchý, bez dlhého tlaku pod ťažkým balíkom a bez priameho kontaktu so suchým zipsom. Vešiak musí zodpovedať hmotnosti odevu. Pri obliečkach striedajte miesto preloženia a nevkladajte medzi ne drsné dekorácie, ktoré by pri vyťahovaní povrch poškrabali.",
                "Reklamáciu zvážte pri výraznej nerovnosti, lysých mapách, prášivom uvoľňovaní, zmene farby alebo odlupovaní po dodržanom prvom cykle. Pred ďalším praním urobte fotografie v rovnakom svetle a zapíšte program. Bežné postupné opotrebenie na lakti sa posudzuje inak než náhla plošná porucha nového výrobku.",
            ],
        },
    ],
    "table2_heading": "Brúsený povrch po praní: čo pravdepodobne pozorujete",
    "table2_intro": "Pred ďalším zásahom urovnajte suchý chlp jedným smerom a porovnajte omak, farbu, lesk aj pevnosť.",
    "table2_headers": ["Prejav", "Možná príčina", "Ako ho odlíšiť", "Bezpečný ďalší krok"],
    "table2_rows": [
        ("Tmavšia plocha bez iného omaku", "Opačný smer krátkeho chĺpku.", "Mení sa po jemnom prečesaní a s uhlom svetla.", "Urovnať po úplnom vysušení."),
        ("Lesklý hladký lakeť", "Sploštenie alebo odretie povrchu.", "Plocha zostáva hladká v oboch smeroch.", "Znížiť trenie; nebrúsiť domácou kefou."),
        ("Hrudky na povrchu", "Žmolkovanie uvoľnených vlákien.", "Uzlíky držia na vláknach, nejde iba o prach.", "Na suchom kuse opatrne testovať textilný strojček."),
        ("Ostrý svetlý kruh", "Lokálne drhnutie alebo chémia.", "Má pevný okraj a zmenený omak.", "Zastaviť zásah a zdokumentovať."),
        ("Veľa voľných vlákien", "Počiatočný zvyšok výroby, opotrebenie alebo porucha.", "Porovnať po ďalšom povolenom cykle a skontrolovať pevnosť.", "Pri trvalom výraznom uvoľňovaní kontaktovať predajcu."),
    ],
    "steps_heading": "Ako prať textil s peach finish krok za krokom",
    "steps": [
        "Prečítajte zloženie, všetky symboly a informáciu o zátare, membráne či podšívke.",
        "Suchý povrch prezrite z viacerých uhlov a jemne urovnajte chlp oboma smermi.",
        "Označte škvrny a overte prípravok na skrytom mieste bez krúživého drhnutia.",
        "Oddeľte kus od zipsov, suchého zipsu, háčikov a drsných uterákov.",
        "Zvoľte povolený program, teplotu a otáčky a použite presnú dávku.",
        "Po cykle kus ihneď vyberte, urovnajte bez silného tlaku a nekrúťte.",
        "Sušte podľa symbolu s prúdením vzduchu a bez priameho radiátora.",
        "Výsledok hodnotte úplne suchý; pri lysých mapách alebo odlupovaní ďalší zásah zastavte.",
    ],
    "remember": [
        "Poznáte základné vlákno a všetky skryté vrstvy, nielen názov povrchu?",
        "Je zmena farby stabilná, alebo sa mení so smerom chĺpku a svetlom?",
        "Má odev zips, suchý zips alebo kovanie, ktoré treba zaistiť?",
        "Je škvrna mastná, pigmentová, vodná alebo iba optická mapa?",
        "Povoľuje etiketa zmäkčovadlo, sušičku, paru a žehlenie?",
        "Je povrch pred uložením úplne suchý a bez tlaku od ťažkých vecí?",
    ],
    "mistakes": [
        "Považovať peach finish za označenie bavlny alebo konkrétny prací program.",
        "Posudzovať fľak bez urovnania smeru krátkeho chĺpku.",
        "Čistiť mastnú škvrnu tvrdou kefou do lysého kruhu.",
        "Prať s otvoreným suchým zipsom, kovovými háčikmi alebo drsnými uterákmi.",
        "Použiť veľa gélu alebo zmäkčovadla na obnovenie mäkkosti.",
        "Žehliť líc silným tlakom a vysokou teplotou.",
    ],
    "expert_heading": "Odbornejší pohľad: abrazívny valec, krátky vlas a uvoľnenie vlákien",
    "expert": [
        "CottonWorks opisuje sueding ako mechanickú úpravu, pri ktorej sa textil opakovane dotýka rýchlo rotujúcich abrazívnych valcov. Viacvalcový systém pracuje s uhlom obopnutia a napätím, kým gap sander tlačí textil do presne nastavenej medzery. Zrno, rýchlosť a tlak rozhodujú, či vznikne jemný rovnomerný peach efekt alebo príliš agresívne poškodenie.",
        "Brúsenie mení optiku aj mechaniku povrchu, no základná reakcia na vodu a teplo ostáva závislá od vlákna. Polyester je termoplastický, bavlna môže relaxovať a zrážať sa a elastan má vlastné tepelné limity. Preto sa názov finishu musí vždy čítať spolu s európskym zložením a so symbolmi hotového výrobku.",
        "Výskum uvoľňovania mikrovlákien pri domácom praní upozorňuje na vplyv konštrukcie, hmotnosti, hustoty, priadze, mechanickej úpravy, pomeru kúpeľa a agitácie. Jedna peached vzorka nemôže reprezentovať všetky brúsené textílie. Poctivý záver je chrániť životnosť, nepreháňať mechaniku a nepoužívať univerzálne tvrdenie bez údajov ku konkrétnemu výrobku.",
    ],
    "source_intro": "Zdroje vysvetľujú mechanické brúsenie, jeho zariadenia, povrchové efekty a mnohofaktorové uvoľňovanie vlákien. Neurčujú jeden prací program pre všetky peached materiály.",
    "sources": [
        ("CottonWorks: sueding", COTTON_SUEDING),
        ("CottonWorks: sanding", COTTON_SANDING),
        ("CottonWorks: mechanical finishing", COTTON_MECHANICAL),
        ("Environmental Science and Pollution Research: mikrovlákna pri praní", MICROFIBRE_STUDY),
        ("AATCC: prehľad textilných skúšok", AATCC_STANDARDS),
        ("GINETEX: symboly ošetrovania", GINETEX),
        ("EÚ 1007/2011: označovanie textilných vlákien", EU_FIBRE_LABEL),
    ],
    "related": [
        ("Čo je moleskin", ARTICLE_MOLESKIN),
        ("Prečo farby blednú", ARTICLE_COLOR),
        ("Prečo sa oblečenie žmolkuje", ARTICLE_PILLING),
        ("Ako čítať ošetrovací štítok", ARTICLE_LABEL),
        ("Ako odstraňovať škvrny", ARTICLE_STAINS),
        ("Ako sušiť bielizeň bez zatuchnutia", ARTICLE_DRYING),
    ],
    "faq_title": "peach finish a brúsený textil",
    "faq": [
        ("Čo je peach finish?", "Jemná mechanická úprava, ktorá abrazívnym kontaktom vytvorí na povrchu veľmi krátky mäkký chlp."),
        ("Je peached fabric vždy bavlna?", "Nie. Brúsiť možno bavlnu, polyester, viskózu aj zmesi; zloženie treba čítať osobitne."),
        ("Je peach finish to isté ako flanel?", "Nie. Flanel býva výraznejšie počesaný a môže mať inú konštrukciu, hmotnosť aj dĺžku vlasu."),
        ("Je to pravý semiš?", "Nie. Ide o textilný povrch pripomínajúci semiš, nie o kožu."),
        ("Prečo sa farba mení po prejdení rukou?", "Krátke vlákna odrážajú svetlo podľa smeru, preto plocha opticky zosvetlie alebo stmavne."),
        ("Ako ho prať?", "Podľa vláknového zloženia a etikety, oddelene od drsných prvkov, s presnou dávkou a voľným oplachom."),
        ("Môžem použiť aviváž?", "Iba ak ju povoľuje výrobca. Film môže zmeniť omak a pri funkčnom textile aj požadované vlastnosti."),
        ("Ako odstrániť mastnú škvrnu?", "Odoberte prebytok, testujte skryté miesto a pracujte bez tvrdej kefy a kruhového drhnutia."),
        ("Prečo vznikol lesklý lakeť?", "Krátky chlp sa mohol sploštiť alebo zodrieť opakovaným tlakom a trením."),
        ("Dá sa lysý povrch obnoviť?", "Stratené vlákna sa domácim praním nevrátia. Agresívne kefovanie môže mapu zväčšiť."),
        ("Môže ísť do sušičky?", "Iba pri povolenom symbole a režime; teplo a prevaľovanie môžu povrch sploštiť alebo poškodiť zmes."),
        ("Môžem ho žehliť?", "Ak je to povolené, žehlite z rubu pri nízkej teplote a bez silného tlaku na líc."),
        ("Kedy výrobok reklamovať?", "Pri náhlej nerovnosti, výraznom uvoľňovaní vlákien, odlupovaní alebo lysých mapách po dodržanom prvom praní."),
    ],
}

add_cards(
    PEACH_FINISH,
    product_heading="Prací gél pre označený prateľný textil s peach finish",
    product_limit="Produkt neobnoví zodratý krátky chlp, nevyrovná trvalo lesklú mapu, neopraví laminát a nemení pokyny pre membránu, elastan ani profesionálne čistenie.",
)


PLISSE: dict[str, object] = {
    "title": "Čo je plisé a krčená úprava: trvalé sklady, teplo a pranie",
    "link": "co-je-plise-a-krcena-uprava-trvale-sklady-teplo-a-pranie",
    "meta": "Čo je plisé, ako vznikajú trvalé sklady a krčené povrchy, ako prať plisovanú sukňu alebo šaty a prečo ich nežehliť naslepo.",
    "short": "Plisé môže byť tepelne stabilizované v termoplastickom vlákne, vytvorené chemickou úpravou, konštrukciou alebo šitím. Spôsob výroby určuje, či voda a teplo sklad zachovajú.",
    "name": "plisé alebo zámerne krčený textil",
    "locative": "plisovanom alebo zámerne krčenom odeve",
    "identity_heading": "Plisé opisuje tvar povrchu, nie jediný materiál ani postup",
    "identity_detail": "Pravidelné sklady môžu byť vytvorené lisovaním a teplotou v termoplastickom polyesteri, chemickou alebo živicovou úpravou celulózy, tkanou či pletenou konštrukciou, šitím alebo kombináciou týchto metód.",
    "identity_boundary": "Trvalý heat-set sklad, obyčajný zažehlený záhyb, seersuckerové zvlnenie, krepový zrnitý povrch a mechanicky stlačená krčená úprava sa môžu podobať, ale na vodu, paru a tlak reagujú rozdielne.",
    "label_focus": "vláknové zloženie, podšívku, elastický pás, spôsob plisovania, glued alebo fused diely, korálky, výšivku, povolené pranie, odstreďovanie, sušenie, žehlenie a profesionálne čistenie",
    "missing_label": "Bez údajov výrobcu nie je bezpečné rozhodnúť, či para sklad obnoví alebo vyrovná; pri drahom, vintage, viacvrstvovom alebo ručne plisovanom kuse voľte odborné posúdenie.",
    "dry_check": "rozbiehajúce sa sklady, ostré zlomy, deformovaný pás, vytiahnuté nite, prenos farby, povolenú niť, zvlnenú podšívku, odlepené hrany, stopy po žehličke a nerovnomerné sploštenie",
    "damage_boundary": "Mäkké otvorenie skladov pri pohybe tela je funkčné, kým ploché mapy, skrútené záhyby, vytiahnutý šev alebo rozdielna dĺžka podšívky signalizujú zmenu konštrukcie.",
    "test_focus": "Na skrytom okraji sledujte, či sa sklad po navlhčení a úplnom vysušení sám vráti; nikdy netestujte žehličkou priamo v strede, pretože nevratná zmena by ostala viditeľná.",
    "combined_risk": "uvoľnenia tepelne alebo chemicky nastaveného tvaru, stlačenia skladov v preplnenom bubne, zachytenia hrán, rozdielneho zrážania vrstiev a nevratného vyžehlenia zámerného reliéfu",
    "chemistry_boundary": "Škrob, lak, silná živica ani domáci fixačný kúpeľ bezpečne neobnovia neznámy výrobný proces; môžu zanechať film, zmeniť farbu alebo zlepiť susedné sklady.",
    "drying_detail": "Odev podoprite po celej šírke, sklady nechajte prirodzene klesať v pôvodnom smere a zabezpečte vzduch pri páse, podšívke a vnútorných záhyboch bez sponiek, ktoré vytvoria nové lomy.",
    "heat_boundary": "Teplo môže termoplastický sklad stabilizovať aj zničiť podľa teploty, času a tlaku; pri celulóze môže zmeniť živicovú úpravu a pri zmesi poškodiť najcitlivejšiu ozdobu či elastan.",
    "stop_signs": "sklady sa po skrytej skúške nevracajú, farba sa prenáša, podšívka ťahá, lepený pás sa vlní, ozdoby sa uvoľňujú, povrch sa leskne alebo sa jednotlivé záhyby začínajú trhať",
    "professional_boundary": "Jednoduchú označenú polyesterovú sukňu možno často prať doma, kým couture plisé, hodváb, kovové vlákna, lepená viacvrstvová konštrukcia, vintage odev alebo pokyn dry clean only vyžadujú odborné čistenie.",
    "answer": "Plisé je textil s pravidelnými skladmi, no ich trvácnosť môže vzniknúť viacerými spôsobmi. Polyesterové záhyby bývajú často tepelne nastavené, kým bavlna či viskóza môžu potrebovať inú úpravu alebo konštrukciu. Preto sa plisovaná sukňa neperie podľa vzhľadu, ale podľa zloženia a štítku. Pred praním odfoťte smer a hĺbku skladov, skontrolujte pás, podšívku a ozdoby. Pri povolenom praní použite voľný jemný cyklus bez stlačenia, hneď vyberte, nekrúťte a sušte v prirodzenom smere skladov. Nežehlite ich naplocho ani sa ich nepokúšajte doma znovu zafixovať vysokým teplom. Ak sa tvar rozpadá alebo podšívka ťahá, zastavte a overte výrobcu či čistiareň.",
    "intro": "Plisovaná sukňa pôsobí ako jednoduchý rad záhybov, ale z hľadiska starostlivosti môže byť jedným z najcitlivejších kusov šatníka. Sklad môže držať vďaka termoplastickej pamäti polyesteru, živicovej úprave, tkanému reliéfu, šitiu alebo vrstvenej konštrukcii. Voda, para a tlak preto nemajú univerzálny účinok. Rovnaká žehlička, ktorá na jednej látke sklad vytvorí, môže na inej trvalo vyrovnať dizajn alebo poškodiť lesk. Bezpečný postup musí chrániť tvar, farbu, pás, podšívku aj všetky ozdoby a skončiť úplným vysušením vnútorných záhybov.",
    "quick": [
        "<strong>Plisé nie je jedno vlákno:</strong> rovnaký vzhľad môže mať polyester, hodváb, bavlna aj zmes.",
        "<strong>Heat-set funguje pri termoplastoch:</strong> teplo môže tvar stabilizovať, ale nesprávne teplo ho aj zmení.",
        "<strong>Štítok celku má prednosť:</strong> podšívka, pás a ozdoby môžu mať prísnejší limit.",
        "<strong>Sklady nestláčajte:</strong> preplnený bubon a krútenie vytvárajú nové nepravidelné lomy.",
        "<strong>Nežehlite naplocho:</strong> zámerný reliéf sa nemusí dať doma obnoviť.",
        "<strong>Sušte v pôvodnom smere:</strong> voda musí odísť aj z pásu a vnútorných záhybov.",
    ],
    "overview_heading": "Ako vznikajú trvalé sklady a prečo neexistuje jeden návod",
    "overview": [
        "Polyester a ďalšie termoplastické vlákna možno zahriať v kontrolovanom tvare a následne ochladiť, aby získali rozmerovú stabilitu alebo záhyb. Účinok závisí od polyméru, teploty, času, vlhkosti, tlaku a predchádzajúceho tepelného spracovania. Priemyselný heatsetting sa robí s kontrolou, ktorú bežná žehlička nemá. Príliš nízka teplota tvar neudrží, príliš vysoká môže vytvoriť lesk, deformáciu alebo poškodenie.",
        "Celulózové vlákna nie sú termoplasty rovnakého typu. Trvalejší sklad na bavlne môže využívať zosieťujúcu chemickú úpravu, šitie alebo konštrukciu. Viskóza v mokrom stave stráca časť pevnosti a môže sa deformovať. Hodváb reaguje na vodu, tlak a farbivo osobitne. Preto názov plisé bez zloženia a výrobného návodu neposkytuje bezpečnú teplotu ani rozhodnutie, či sa smie ponoriť.",
        "Krčený povrch môže byť zámerne nepravidelný, napríklad mechanicky stlačený, krepový alebo tkaný so zmenou napätia. Seersucker vytvára reliéf rozdielnym napätím priadzí a nemal by sa vyžehliť do roviny. Pri nákupe sa pýtajte, či je tvar permanentný, aký spôsob čistenia výrobca testoval a či sa sklad po navlhčení prirodzene obnoví.",
    ],
    "table1_heading": "Plisé, zažehlený puk, seersucker a krep",
    "table1_intro": "Povrchy sa môžu opticky podobať, no tvar držia iným mechanizmom. Pri praní nerozhoduje iba pravidelnosť záhybu.",
    "table1_headers": ["Povrch", "Ako môže držať tvar", "Čo ho ohrozuje", "Čo overiť"],
    "table1_rows": [
        ("Heat-set polyesterové plisé", "Tepelné nastavenie termoplastického vlákna.", "Nevhodné teplo, tlak, dlhé stlačenie.", "Etiketu, podšívku a povolené sušenie."),
        ("Chemicky stabilizovaný sklad", "Živicová alebo iná úprava celulózového základu.", "Nevhodná chémia, bielidlo a vysoké teplo.", "Údaje výrobcu a najcitlivejšiu vrstvu."),
        ("Zažehlený puk", "Dočasné alebo opakovane obnovované lisovanie.", "Voda a bežné nosenie ho môžu uvoľniť.", "Či výrobca povoľuje žehlenie a ako puk viesť."),
        ("Seersucker", "Tkaný rozdiel napätia vytvára zvlnené pruhy.", "Silné žehlenie sploští želaný reliéf.", "Zloženie a pokyn nežehliť či žehliť mierne."),
        ("Krep / krčená úprava", "Priadza, väzba alebo dokončovanie vytvára zrnitý povrch.", "Ťah, para a tlak môžu zmeniť textúru.", "Konkrétny materiál a výrobný návod."),
    ],
    "sections": [
        {
            "heading": "Ako zistiť, z čoho je plisovaná sukňa alebo šaty",
            "paragraphs": [
                "Prečítajte vláknové percentá vrchnej látky aj podšívky a hľadajte osobitný pokyn k plisovaniu. Polyester môže naznačovať heat-set, no nie je to dôkaz, že každá ozdoba a pás znášajú rovnaké teplo. Zips, lepená výstuž, guma, kovová niť alebo potlač pridávajú vlastné limity. Pri vintage kuse môže byť etiketa neúplná alebo môže opisovať iba hlavnú látku.",
                "Prezrite rub a švy. Sklad môže byť miestami zaistený niťou, pri páse zošitý a smerom nadol otvorený. Podšívka sa nemusí plisovať vôbec. Ak sa vrchná vrstva a podšívka správajú rozdielne, ponorenie môže vytvoriť ťahanie alebo nerovnakú dĺžku. Bez jasného návodu zvoľte odborné čistenie namiesto pokusu s vysokou parou.",
            ],
        },
        {
            "heading": "Heat-set polyesterové plisé a hranice tepelnej pamäte",
            "paragraphs": [
                f"Polyester je termoplastický, takže kontrolované teplo môže nastaviť rozmer a sklad. Súvisiace vlastnosti syntetického vlákna rozoberá porovnanie <a href=\"{ARTICLE_POLYESTER}\">polyester verzus bavlna</a>. Hotový záhyb však neznamená neobmedzenú tepelnú odolnosť. Ďalšie teplo, tlak a para môžu preusporiadať povrch, vytvoriť lesk alebo sploštiť vrchol skladu.",
                "Výskum retencie plisov na polyesterových tkaninách ukazuje, že výsledok súvisí s metódou lisovania a teplotou a že silnejšie nastavenie môže meniť omak. Domáca žehlička nemá dáta o pôvodnom procese. Preto neodhadujte obnovovaciu teplotu podľa internetu a nepoužívajte najvyšší stupeň v nádeji, že sklad znovu zafixujete.",
            ],
        },
        {
            "heading": "Bavlna, viskóza, hodváb a zmesi potrebujú inú logiku",
            "paragraphs": [
                "Bavlnené plisé môže držať vďaka úprave alebo šitiu a voda môže uvoľniť dočasne zažehlený tvar. Viskóza je v mokrom stave citlivejšia na ťah, takže ťažká sukňa sa nemá zdvíhať za jeden bod. Hodváb môže vytvoriť vodné mapy a farebný prenos. Zmes sa riadi najcitlivejšou zložkou aj spôsobom výroby.",
                "Ak výrobca povoľuje domáce pranie, nevytvárajte si vlastnú teplotu podľa dominantného percenta. Symbol zohľadňuje hotový kus. Pri chýbajúcom štítku vykonajte iba nenápadnú kontrolu navlhčenia bez trenia a nechajte miesto úplne vyschnúť; ak sa sklad nevráti, nepokračujte na celej ploche.",
            ],
        },
        {
            "heading": "Plisé, seersucker a krep sa nemajú žehliť rovnakým spôsobom",
            "paragraphs": [
                f"<a href=\"{ARTICLE_SEERSUCKER}\">Seersucker</a> drží zvlnenie v konštrukcii tkaniny a jeho výhodou je reliéf bez potreby vyžehlenia do roviny. <a href=\"{ARTICLE_CREPE}\">Krep</a> môže mať zrnitý povrch vďaka zákrutu priadze, väzbe alebo dokončeniu. Plisé používa opakujúce sa sklady. Silný tlak, ktorý pomôže obyčajnej košeli, môže všetky tri efekty sploštiť.",
                "Pri žehlení sa riaďte symbolom a konkrétnym návodom. Ak je povolené iba naparovanie z odstupu, nemeňte ho na priame pritlačenie žehličky. Pri ručne zaistenom sklade môže čistiareň použiť tvarovaciu pomôcku alebo presnú techniku, ktorú doma nemožno bezpečne napodobniť.",
            ],
        },
        {
            "heading": "Ako pripraviť plisovanú sukňu pred praním",
            "paragraphs": [
                "Odfoťte celú siluetu a detail pravidelnosti skladov, najmä pri páse a leme. Skontrolujte zips, háčik, gumu, podšívku, uvoľnené stehy a ozdoby. Vyprázdnite vrecká a odstráňte odnímateľný opasok. Ak je jeden sklad už skrútený, pranie ho nemusí napraviť a môže sťažiť neskoršie posúdenie.",
                "Škvrny označte a predčistite bez roztvorenia či násilného stlačenia záhybu. Prípravok testujte na skrytej časti. Ak povrch púšťa farbu, pás sa odliepa alebo podšívka ťahá ešte za sucha, domáci cyklus odložte. Prací vak má zmysel iba dostatočne veľký, aby sukňu nezroloval do pevnej gule.",
            ],
            "callout": {
                "title": "Fotografie, ktoré pomôžu zachovať pôvodný tvar",
                "items": [
                    "Celá sukňa zavesená spredu a zboku bez napínania.",
                    "Napojenie skladov na pás a spôsob ich zaistenia niťou.",
                    "Lem, podšívka, zips, háčik, ozdoby a už existujúce nerovnosti.",
                    "Detail jedného typického skladu s meradlom alebo rovným okrajom.",
                ],
                "background": "#fffaf5",
                "border": "#e6ded2",
            },
        },
        {
            "heading": "Ako prať plisovanú polyesterovú sukňu v práčke",
            "paragraphs": [
                "Iba ak to povoľuje etiketa. Sukňu voľne obráťte naruby, zaistite zips a háčik a vložte do veľkého jemného vaku bez tesného rolovania. Perte s ľahkými hladkými kusmi podobnej farby, nie s džínsami, uterákmi alebo oblečením so suchým zipsom. Bubon nepreplňte, aby sa sklady nestlačili do nepravidelných lomov.",
                "Použite povolený jemný program, presnú dávku a nízke mechanické namáhanie podľa symbolu. Po skončení sukňu okamžite vyberte, podoprite oboma rukami a nekrúťte. Jednotlivé sklady urovnajte iba jemným vedením v pôvodnom smere; nesnažte sa ich naťahovaním predĺžiť ani za mokra znovu lisovať.",
            ],
        },
        {
            "heading": "Ručné pranie nie je automaticky šetrnejšie",
            "paragraphs": [
                "Pri ručnom praní používateľ často sukňu dlho namáča, stláča, krúti a vyberá za pás, čo môže byť horšie než testovaný jemný strojový cyklus. Ak výrobca povoľuje ručné pranie, pripravte veľkú čistú nádobu, rozpustenú dávku a krátky kontrolovaný kontakt. Odev podopierajte v celej dĺžke a vodu odstráňte bez žmýkania.",
                "Ťažký mokrý viskózový alebo viacvrstvový kus sa môže vlastnou hmotnosťou vytiahnuť. Farebný hodváb môže vytvoriť mapu. Pokyn hand wash preto neznamená ľubovoľnú techniku. Ak etiketa určuje profesionálne čistenie, domáce ponorenie nie je miernejšia alternatíva.",
            ],
        },
        {
            "heading": "Sušenie plisovanej sukne bez nových lomov",
            "paragraphs": [
                f"Po odstránení prebytočnej vody podľa návodu rozložte sukňu alebo ju zaveste na širokú oporu tak, aby pás niesol hmotnosť rovnomerne. Sklady nechajte klesať zvislo v pôvodnom smere a zabezpečte vzduch medzi vrstvami. Praktické vetranie rozoberá článok <a href=\"{ARTICLE_DRYING}\">ako sušiť bielizeň bez zatuchnutia</a>. Nepoužívajte štipce uprostred viditeľného skladu.",
                "Sušička je možná iba pri výslovnom symbole. Prevaľovanie a teplo môžu tvar zmeniť, najmä pri neznámej kombinácii materiálov. Odev neukladajte ani nežehliť, kým pás a vnútorné záhyby nie sú úplne suché. Vlhkosť uzavretá medzi vrstvami spôsobuje pach a môže preniesť farbu.",
            ],
        },
        {
            "heading": "Ako skladovať plisé bez dlhodobého sploštenia",
            "paragraphs": [
                "Postup závisí od hmotnosti a konštrukcie. Ľahká stabilná sukňa môže visieť na širokom čalúnenom alebo klipovom vešiaku, ktorý nezanechá stopu a neroztiahne pás. Ťažký či krehký kus je bezpečnejšie uložiť naplocho s voľnými podloženými skladmi. Nevkladajte ho pod ťažký balík a nepoužívajte príliš malý box.",
                "Múzejné odporúčania pri citlivých textíliách zdôrazňujú minimum ostrých skladov, primeranú oporu a obmedzenie tlaku na rovnaké miesto. Moderné plisé nie je automaticky zbierkový predmet, princíp však pomáha: čistý suchý kus, dostatok priestoru a žiadne vlhké uzavretie. Pri dlhom skladovaní stav pravidelne kontrolujte.",
            ],
        },
        {
            "heading": "Kedy sa sklad dá upraviť a kedy patrí do čistiarne",
            "paragraphs": [
                "Mierne otvorený moderný polyesterový sklad sa môže po povolenom praní a zvislom vysušení sám usadiť. Ak výrobca uvádza presný spôsob naparovania, postupujte na skrytej časti a bez priameho tlaku. Neznámy sklad, ostrá plochá mapa, zmenený lesk alebo rozdielna dĺžka vrstiev však vyžadujú odborné posúdenie.",
                "Čistiareň vopred informujte o spôsobe plisovania, ak ho poznáte, a ukážte fotografie pôvodného tvaru. Opýtajte sa, či vie zabezpečiť retenciu skladov a ako pracuje s podšívkou a ozdobami. Pri novom výrobku, ktorý po dodržanom postupe stratí pravidelnosť, najprv zvážte reklamáciu, nie domáce prelisovanie.",
            ],
        },
    ],
    "table2_heading": "Plisé po praní alebo skladovaní: diagnóza pred žehličkou",
    "table2_intro": "Najprv určte, či sa zmenil tvar, rozmer, vrstva alebo iba smer uloženia. Pri neznámom procese vysoké teplo problém zafixuje.",
    "table2_headers": ["Prejav", "Možná príčina", "Čo overiť", "Bezpečný ďalší krok"],
    "table2_rows": [
        ("Sklady sú mäkšie, ale pravidelné", "Dočasná vlhkosť alebo otvorenie pri pohybe.", "Stav po úplnom zvislom vysušení.", "Nechať doschnúť a urovnať bez tlaku."),
        ("Jedna plocha je úplne rovná", "Lokálne teplo, tlak alebo chybná retencia.", "Lesk, okraj mapy a históriu žehlenia.", "Nezohrievať znovu; kontaktovať odborníka."),
        ("Podšívka ťahá lem", "Rozdielna rozmerová zmena vrstiev.", "Presah a napätie po celom obvode.", "Nevisieť za jednu vrstvu; odborne posúdiť."),
        ("Pás sa vlní", "Zmena výstuže, gumy alebo lepeného dielu.", "Či je pás úplne suchý a či sa oddeľuje.", "Nenaparovať naslepo; overiť výrobcu."),
        ("Vznikli nové nepravidelné lomy", "Stlačenie v bubne, vaku alebo pri skladovaní.", "Či pôvodné sklady stále držia pod nimi.", "Podľa návodu jemne zavesiť; neprelisovať naplocho."),
    ],
    "steps_heading": "Ako ošetriť plisovaný alebo zámerne krčený odev krok za krokom",
    "steps": [
        "Zistite zloženie, spôsob plisovania, podšívku, ozdoby a všetky symboly.",
        "Odfoťte pôvodný smer, hĺbku a pravidelnosť skladov pri páse aj leme.",
        "Skontrolujte zips, háčik, gumu, zaistené nite, podšívku a farebný prenos.",
        "Škvrnu riešte lokálne po skrytej skúške bez trenia a rozťahovania skladu.",
        "Ak je pranie povolené, použite voľný jemný cyklus alebo presný ručný postup výrobcu.",
        "Odev podoprite, nekrúťte a hneď po cykle urovnajte sklady v pôvodnom smere.",
        "Sušte podľa symbolu s prúdením vzduchu medzi vrstvami a bez prudkého tepla.",
        "Nežehlite naplocho; pri strate tvaru alebo ťahaní vrstiev vyhľadajte odborníka.",
    ],
    "remember": [
        "Je sklad heat-set, chemicky stabilizovaný, šitý, tkaný alebo iba zažehlený?",
        "Má vrchná látka, pás, podšívka a ozdoba rovnaké ošetrovacie limity?",
        "Je domáce pranie výslovne povolené, alebo etiketa vyžaduje profesionálne čistenie?",
        "Vojde sa odev do bubna alebo vaku bez stlačenia do pevnej gule?",
        "Môže pri sušení visieť bez vytiahnutia mokrej látky a pásu?",
        "Poznáte bezpečnú teplotu skôr, než použijete paru alebo žehličku?",
    ],
    "mistakes": [
        "Predpokladať, že každé plisé je polyesterové a trvalo tepelne nastavené.",
        "Prať plisovanú sukňu v preplnenom bubne alebo príliš malom vaku.",
        "Krútiť mokrú viskózu a zdvíhať ťažký kus iba za pás.",
        "Žehliť celý povrch naplocho v snahe odstrániť nežiaduci lom.",
        "Použiť škrob, lak alebo vysoké teplo ako domácu náhradu priemyselného procesu.",
        "Uložiť plisé pod ťažké veci alebo ešte vlhké do uzavretého obalu.",
    ],
    "expert_heading": "Odbornejší pohľad: termoplastická stabilizácia, retencia skladu a omak",
    "expert": [
        "Heatsetting používa zvýšenú teplotu na stabilizáciu termoplastického substrátu. Pri plisovaní je textil počas tepelného cyklu držaný v konkrétnom tvare a po ochladení si ho lepšie zachováva. Výsledok závisí od polyméru aj histórie spracovania, preto nemožno jednu teplotu preniesť medzi rôzne polyesterové zmesi, podšívky a ozdoby.",
        "Peer-reviewed štúdia retencie plisov na polyesterových oblekových tkaninách porovnávala suché a parné lisovanie a zistila rozdielne účinné podmienky aj zmenu omaku. Tieto laboratórne hodnoty nie sú domácim návodom, ale dokazujú, že tlak, vlhkosť a teplota pôsobia spolu. Viac tepla nemusí znamenať lepší spotrebiteľský výsledok.",
        "Victoria and Albert Museum skúmalo heat-set plisované polyesterové vzorky a pri konkrétnych podmienkach nezistilo oslabenie pozdĺž plisov; ide však o obmedzený experiment na konkrétnych vzorkách, nie záruku pre každý odev. GINETEX symboly a údaje výrobcu zostávajú rozhodujúce pre celý výrobok a konzervačné odporúčania majú prísnejšie hranice než bežná moderná sukňa.",
    ],
    "source_intro": "Zdroje vysvetľujú heatsetting, meranie retencie skladu, správanie konkrétnych polyesterových vzoriek a bezpečné skladovanie citlivých textílií. Neposkytujú univerzálnu domácu teplotu na obnovu plisovania.",
    "sources": [
        ("CottonWorks: heatsetting", COTTON_HEATSETTING),
        ("J-STAGE: retencia plisov a omak polyesterovej tkaniny", PLEAT_RETENTION),
        ("Victoria and Albert Museum: heat-set polyesterové plisé", VAM_PLEATS),
        ("National Park Service: starostlivosť a skladovanie textilných predmetov", NPS_TEXTILE_STORAGE),
        ("GINETEX: symboly ošetrovania", GINETEX),
        ("EÚ 1007/2011: označovanie textilných vlákien", EU_FIBRE_LABEL),
    ],
    "related": [
        ("Polyester verzus bavlna", ARTICLE_POLYESTER),
        ("Čo je seersucker", ARTICLE_SEERSUCKER),
        ("Čo je krep", ARTICLE_CREPE),
        ("Ako čítať ošetrovací štítok", ARTICLE_LABEL),
        ("Ako odstraňovať škvrny", ARTICLE_STAINS),
        ("Ako sušiť bielizeň bez zatuchnutia", ARTICLE_DRYING),
    ],
    "faq_title": "plisé a krčená úprava",
    "faq": [
        ("Čo je plisé?", "Textil s pravidelnými skladmi vytvorenými teplom, chemickou úpravou, konštrukciou, šitím alebo kombináciou metód."),
        ("Je plisé vždy polyester?", "Nie. Polyester je častý pri heat-set plisovaní, ale existujú bavlnené, viskózové, hodvábne aj zmesové varianty."),
        ("Čo znamená heat-set sklad?", "Tvar termoplastického vlákna bol stabilizovaný kontrolovaným teplom a ochladením."),
        ("Môže sa plisovaná sukňa prať v práčke?", "Iba ak to povoľuje etiketa a celý kus vrátane podšívky, pásu a ozdôb je kompatibilný."),
        ("Je ručné pranie bezpečnejšie?", "Nie automaticky. Dlhé namáčanie, krútenie a zdvíhanie mokrého kusa môžu spôsobiť väčšiu deformáciu."),
        ("Mám použiť prací vak?", "Len dostatočne veľký a priepustný vak, ktorý odev chráni bez stlačenia skladov."),
        ("Ako plisé sušiť?", "Podopreté alebo zavesené podľa etikety, v pôvodnom smere skladov, s prúdením vzduchu medzi vrstvami."),
        ("Môže ísť do sušičky?", "Iba pri výslovnom symbole. Teplo a prevaľovanie môžu tvar zmeniť."),
        ("Môžem sklady prežehliť?", "Iba podľa konkrétneho návodu. Pri neznámom procese môžete reliéf nevratne sploštiť alebo vytvoriť lesk."),
        ("Pomôže para?", "Niekedy môže uvoľniť vedľajší lom, ale na inom materiáli otvorí pôvodný sklad. Bez návodu ju nepoužívajte naslepo."),
        ("Ako odstrániť škvrnu?", "Po skrytej skúške lokálne, bez roztvorenia a trenia záhybu, s rešpektovaním farby a podšívky."),
        ("Ako plisé skladovať?", "Čisté a suché, voľne zavesené alebo naplocho podľa hmotnosti, bez tlaku a ostrých nových lomov."),
        ("Kedy ísť do čistiarne?", "Pri pokyne dry clean only, hodvábe, couture či vintage kuse, lepených vrstvách alebo strate tvaru a ťahaní podšívky."),
    ],
}

add_cards(
    PLISSE,
    product_heading="Prací gél iba pre plisé s výslovne povoleným domácim praním",
    product_limit="Produkt nenastaví ani neobnoví trvalé sklady, nenahrádza odborné plisovanie, neopraví zmenenú podšívku a nemení pokyn na profesionálne čistenie.",
)


ARTICLES: list[dict[str, object]] = [PIGMENT_DYED, STONEWASHED, PEACH_FINISH, PLISSE]


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
                "time_posted": f"{14 + index:02d}:00:00",
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
        raise SystemExit("Batch 57 link preflight failed")
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
