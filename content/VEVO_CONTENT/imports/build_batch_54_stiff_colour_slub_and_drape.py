#!/usr/bin/env python3
"""Build and validate VEVO batch 54 material-care articles."""

from __future__ import annotations

import json
import re
from pathlib import Path

import build_batch_51_woven_surfaces_and_yarns as batch51
from build_batch_53_weaves_curtains_formal_fulled_wool import (
    FIXED_PRICE_RE,
    FORBIDDEN_PUBLIC_RE,
    WORD_RE,
    add_cards,
    jaccard,
    preflight_links,
    render_article,
    seven_word_shingles,
    visible_text,
)


PUBLISH_DATE = "2026-08-28"
CANDIDATES = Path("content/VEVO_CONTENT/batches/batch-54-candidates-2026-08-28.txt")
OUT_JSON = Path("content/VEVO_CONTENT/imports/batch-54-2026-08-28-articles.json")
OUT_PREFLIGHT = Path("content/VEVO_CONTENT/exports/batch-54-2026-08-28-link-preflight.json")

EU_FIBRE_LABEL = "https://eur-lex.europa.eu/eli/reg/2011/1007/oj"
GINETEX = "https://www.ginetex.net/share/article/4201/care-symbols"
ISO_WASH = "https://www.iso.org/standard/75934.html"
ISO_APPEARANCE = "https://www.iso.org/standard/67602.html"
ISO_COLOR = "https://www.iso.org/standard/51276.html"
AATCC_STANDARDS = "https://www.aatcc.org/testing/standards"
UVA_BUCKRAM = "https://explore.lib.virginia.edu/exhibits/show/preservation-services/fabrics"
NPS_TEXTILE_OBJECTS = "https://www.nps.gov/subjects/museums/upload/MHI_AppK_TextilesObjects.pdf"
NLM_WET_TEXTILES = "https://www.nlm.nih.gov/hmd/preservation/textiles.html"
COOPTEX_MADRAS = "https://cooptex.gov.in/image/data/PPT/Co-optexStory.pdf"
LACMA_SHANTUNG = "https://collections.lacma.org/object/71338"
MONTANA_SHANTUNG = "https://arc.lib.montana.edu/msu-extension/objects/ext1-000316.pdf"
LACMA_TAFFETA = "https://collections.lacma.org/object/130818"
UFS_CHALLIS = "https://scholar.ufs.ac.za/server/api/core/bitstreams/72a78a86-ca25-49f9-80da-b2c912699326/content"
UTT_FASHION_HANDBOOK = "https://utt.edu.tt/fashion/cafd_student_handbook.pdf"
WOOLMARK_CARE = "https://www.woolmark.com/care/care-for-wool/"

ARTICLE_LABEL = "/n/ako-citat-stitok-na-obleceni-material-symboly-prania-a-spravny-program"
ARTICLE_STAIN = "/n/ako-odstranit-zuvacku-krv-vosk-a-ine-skvrny-z-oblecenia"
ARTICLE_COLOR = "/n/stalofarebnost-textilu-preco-farby-blednu-pri-prani-svetle-a-treni"
ARTICLE_SHRINKAGE = "/n/preco-sa-oblecenie-zrazi-po-prani-teplota-vlakna-susicka-a-prevencia"
ARTICLE_DRYING = "/n/ako-susit-bielizen-v-malom-byte-bez-zatuchnutia"
ARTICLE_IRONING = "/n/ako-vyzehlit-koselu-kompletny-sprievodca-pre-dokonaly-vysledok"
ARTICLE_SNAGGING = "/n/zatrhavanie-textilu-preco-vznikaju-vytiahnute-ocka-a-ako-im-predchadzat"
ARTICLE_COTTON = "/n/co-je-bavlna-vlastnosti-vyhody-nevyhody-a-starostlivost"
ARTICLE_VISCOSE = "/n/co-je-viskoza-vlastnosti-krcivost-zrazanie-a-starostlivost"
ARTICLE_SATIN = "/n/co-je-saten-nie-je-to-vzdy-hodvab-a-ako-ho-spravne-prat"
ARTICLE_TAFFETA = "/n/co-je-taft-sustava-tkanina-vodne-mapy-a-bezpecne-cistenie"
ARTICLE_FORMAL = "/n/ako-prat-spolocenske-saty-doma-a-kedy-zvolit-cistiaren"
ARTICLE_BATISTE = "/n/co-je-batist-jemna-platnova-tkanina-priesvitnost-a-pranie"
ARTICLE_CREPE = "/n/co-je-krep-zrnity-povrch-krcivost-a-spravna-starostlivost"
ARTICLE_WOOL_BLEND = "/n/vlna-a-polyamid-preco-sa-miesaju-vlakna-a-ako-to-ovplyvnuje-pranie"
ARTICLE_CANVAS = "/n/co-je-canvas-pevne-platno-skvrny-a-spravne-pranie"


BUCKRAM: dict[str, object] = {
    "title": "Čo je buckram: vystužená tkanina, tvarovanie, prach a bezpečné čistenie",
    "link": "co-je-buckram-vystuzena-tkanina-tvarovanie-prach-a-bezpecne-cistenie",
    "meta": "Čo je buckram, prečo drží tvar, ako ho odlíšiť od plátna a výstuže a ako bezpečne čistiť klobúk, knižnú väzbu či odev bez vymytia apretúry.",
    "short": "Buckram je hrubšia riedko až stredne husto tkaná oporná látka, ktorej tuhosť vytvára škrob, glej, živica alebo iná úprava. Voda preto nemusí iba odstrániť škvrnu: môže presunúť apretúru, zmeniť tvar a vytvoriť mapu.",
    "name": "buckram",
    "locative": "buckrame",
    "identity_heading": "Buckram spája tkaný základ s tužiacou úpravou",
    "identity_detail": "Tradičný buckram má plátnovo previazanú bavlnenú alebo ľanovú základnú tkaninu a výraznú vodou alebo teplom aktivovanú apretúru, ktorá obmedzí pohyb nití a dovolí materiál tvarovať.",
    "identity_boundary": "Moderný variant môže obsahovať syntetické vlákna, akrylátovú živicu, lamináciu alebo dve zlepené vrstvy, zatiaľ čo podobne vyzerajúce plátno môže držať tvar iba hustotou a hrúbkou priadze.",
    "label_focus": "odnímateľnosť výstuže, zloženie tkaniny, druh apretúry, lepené spoje, farbivo, podšívku, kov, papier, kožu, perie, ozdoby a pokyn na profesionálne čistenie celého predmetu",
    "missing_label": "Klobúk, staršiu knihu alebo dekoráciu bez dokumentácie nevkladajte do vody podľa rady pre bavlnenú metráž; najprv zistite, či výstuž tvorí nosnú kostru a či ju možno vôbec oddeliť.",
    "dry_check": "uvoľnený prach, mastný dotyk, svetlé vodné kruhy, praskajúcu apretúru, mäkké prepadnuté miesto, hrdzu, pleseň, oddelené vrstvy, rozstrapkaný okraj, zlomený tvar a stopy po hmyze",
    "damage_boundary": "Voľný prach možno zachytiť, no vymytá tuhosť, zlomený oblúk klobúka, odlúpnutá povrchová vrstva alebo zvlnený papier nie sú škvrny, ktoré opraví ďalší mokrý cyklus.",
    "test_focus": "Na skúšobnom okraji sledujte nielen farbu, ale aj mäknutie, lepkavosť, migráciu bieleho povlaku, zmenu priehľadnosti a návrat tuhosti po úplnom vysušení.",
    "combined_risk": "rozpustenia alebo napučania apretúry, posunu otvorenejšej väzby, rozdielneho zmrštenia podšívky a poťahu a deformácie mäkkej nosnej kostry",
    "chemistry_boundary": "Prach, kožný maz, škrobová škvrna, pleseň a hrdza nie sú jeden problém; náhodné rozpúšťadlo môže ovplyvniť živicu, farbu, lepidlo, papier aj ozdobu skôr než samotnú nečistotu.",
    "drying_detail": "Navlhčenú oddeliteľnú vzorku podoprite v plánovanom tvare a sušte rovnomerne z oboch strán; klobúk, väzbu alebo vystužený odev nevystavujte gravitácii bez pevnej opory.",
    "heat_boundary": "Horúci vzduch môže príliš rýchlo spevniť okraj, zvlniť podklad, zmeniť termoplastickú živicu, povoliť lepidlo alebo zafixovať deformáciu skôr, než ju stihnete bezpečne vyrovnať.",
    "stop_signs": "mäknutie nosnej plochy, lepkavý povrch, biely migrujúci povlak, prenos farby, oddelenie vrstiev, rastúca mapa, hrdzavý výtok, praskanie alebo zmena tvaru počas skúšky",
    "professional_boundary": "Samostatný nový prateľný diel možno ošetriť podľa technického listu, kým klobúk, kniha, historický kostým, divadelná dekorácia alebo vstavaná výstuž si vyžadujú klobučníka, kníhviazača, čistiareň alebo konzervátora.",
    "answer": "Buckram je tkaná oporná látka spevnená apretúrou, napríklad škrobom, glejom alebo syntetickou živicou. Používa sa vo výstužiach odevov, klobúkoch, knižných väzbách, krabiciach a dekoráciách. Nie je automaticky prateľný len preto, že jeho základom môže byť bavlna alebo ľan. Voda môže rozpustiť alebo presunúť tužiacu zložku, zmeniť tvar a vytvoriť mapu. Najprv odstráňte iba voľný prach, skontrolujte všetky vrstvy a urobte skúšku na skrytom okraji. Celý predmet namočte len pri výslovnom povolení výrobcu. Klobúk, knihu, historický kus alebo vstavanú výstuž radšej zverte odborníkovi, ak by strata tuhosti poškodila funkciu.",
    "intro": "Pri buckrame je najdôležitejšie pochopiť, že jeho úlohou nie je príjemný dotyk, ale kontrola tvaru. To, čo vyzerá ako pevná látka, môže svoju stabilitu získavať z vrstvy citlivej na vodu, paru, rozpúšťadlo alebo tlak. Rovnaké slovo sa navyše používa pri klobúkoch, výstužiach, knihách aj kulisách, hoci každý hotový predmet kombinuje iné lepidlá, poťahy a ozdoby. Bezpečný postup preto nezačína otázkou na teplotu prania, ale tým, či možno nosnú vrstvu vôbec navlhčiť bez straty funkcie.",
    "quick": [
        "<strong>Tuhosť nemusí byť vlastnosť vlákna:</strong> často ju vytvára škrob, glej alebo syntetická živica.",
        "<strong>Voda môže meniť tvar:</strong> apretúra sa môže rozpustiť, presunúť alebo po vyschnutí stvrdnúť nerovnomerne.",
        "<strong>Predmet posudzujte ako celok:</strong> poťah, podšívka, papier, kov, koža a lepidlo môžu mať nižší limit než buckram.",
        "<strong>Prach riešte nasucho:</strong> nízky kontrolovaný ťah a ochranná sieťka sú bezpečnejšie než mokrá handra na neznámom kuse.",
        "<strong>Strata tuhosti nie je škvrna:</strong> opakované pranie ju neobnoví a domáci škrob nemusí nahradiť pôvodnú úpravu.",
        "<strong>Odborník chráni konštrukciu:</strong> klobučník, kníhviazač alebo konzervátor vie určiť, čo možno rozobrať a znovu vytvarovať.",
    ],
    "overview_heading": "Ako vzniká tuhosť buckramu a prečo sa pri vode môže stratiť",
    "overview": [
        "Buckram nie je jedna nemenná surovina. University of Virginia pri knižných poťahoch uvádza súčasné bavlnené a polyesterové varianty s akrylovou úpravou, zatiaľ čo odevné a klobučnícke materiály môžu používať inú tkanú základňu aj iný spôsob vystuženia. Spoločná je funkcia: spevnená vrstva má držať tvar, odolávať namáhaniu alebo podopierať ďalší materiál.",
        "Táto výhoda je zároveň rizikom čistenia. Kvapka môže lokálne rozpustiť tužiacu zložku, ktorá sa s vodou presunie k okraju a tam zaschne ako tvrdší alebo svetlejší kruh. Pri celkovom namočení sa osnova a útok uvoľnia, poddajná plocha klesne a poťah či podšívka ju môže stiahnuť iným smerom. Niektoré moderné živice naopak vodu znesú, ale reagujú na teplo alebo rozpúšťadlo.",
        "Názov buckram preto nie je hotový ošetrovací návod. Rozhoduje výrobca konkrétneho materiálu a konštrukcia predmetu. Samostatný nový diel určený na kostým môže mať postup tvarovania, zatiaľ čo historická knižná väzba potrebuje minimum zásahov. Pri oboch treba zachovať rozmer, polohu vlákien, rovnomernosť apretúry a väzbu k susedným materiálom.",
    ],
    "table1_heading": "Buckram, plátno, výstuž a kníhviazačská tkanina",
    "table1_intro": "Podobný pevný vzhľad môže vzniknúť odlišným spôsobom. Rozlíšenie určuje, či voda odstraňuje nečistotu alebo zároveň rozpúšťa nosnú vlastnosť.",
    "table1_headers": ["Materiál", "Čo drží tvar", "Typické použitie", "Hlavné riziko čistenia"],
    "table1_rows": [
        ("Buckram", "Tkaný základ plus výrazná apretúra alebo povlak.", "Klobúky, odevná výstuž, knihy, krabice a dekorácie.", "Strata alebo presun tuhosti, mapa a deformácia."),
        ("Husté plátno", "Hustota, hrúbka a väzba priadzí, prípadne ľahká úprava.", "Tašky, pracovné textílie, poťahy a maľované plátna.", "Zrážanie, farba, povlak a lokálne odrenie."),
        ("Netkaná výstuž", "Zlisované vlákna a často tavné lepidlo.", "Golier, pás, manžeta a spevnenie dielu.", "Odlepenie, bubliny, zrazenie a teplota žehlenia."),
        ("Vlasová výstuž", "Tkanina z hrubších vlasových alebo zmesových priadzí.", "Tradičné saká a chlopne.", "Tvarovanie, rozdielne zrážanie vrstiev a profesionálna údržba."),
        ("Kníhviazačské plátno", "Tkanina s výplňou a povrchom upraveným pre väzbu.", "Dosky a chrbty kníh.", "Voda, oder tlače, zvlnenie dosky a lepidlo."),
    ],
    "sections": [
        {
            "heading": "Ako zistiť, či je buckram samostatný, odnímateľný alebo vstavaný",
            "paragraphs": [
                f"Na odeve najprv použite <a href=\"{ARTICLE_LABEL}\">návod na čítanie štítka</a> a prezrite švy z rubu bez párania. Odnímateľná výstuž môže byť vložená v samostatnom vrecku alebo prichytená stehom, zatiaľ čo tavná výstuž je prilepená na vrchnú látku. Klobúk môže mať buckram úplne zakrytý podšívkou a poťahom, takže dotyk odhalí iba pevnosť, nie zloženie.",
                "Pri knihe sledujte, či je povrchová tkanina súčasťou dosky, chrbta alebo dekorácie a či sa pod ňou nenachádza papier citlivý na vodu. Nič neodliepajte kvôli identifikácii. Ak by skúmanie vyžadovalo rezanie stehu, nadvihnutie väzby alebo demontáž ozdoby, ide už o odborný zásah. Fotografia konštrukcie a výrobné údaje sú bezpečnejšie než domáca skúška rozpúšťadlom.",
            ],
        },
        {
            "heading": "Suché odstránenie prachu z klobúka, väzby a dekorácie",
            "paragraphs": [
                "Predmet položte na čistú pevnú oporu a prach oddeľte od mastnej škvrny či plesne. Jemný mäkký štetec môže viesť uvoľnené častice k dýze nastavenej na nízky ťah bez priameho kontaktu. Pri krehkom povrchu sa medzi textil a dýzu používa čistá ochranná sieťka, ktorá bráni vtiahnutiu nite alebo ozdoby. Vysávanie však nie je vhodné na uvoľnené koráliky, výšivku či odlupujúci sa povlak.",
                "Smithsonian odporúča pri starších textíliách pracovať naplocho, s nízkym ťahom a cez ochrannú mriežku, ale zároveň upozorňuje na predmety, ktoré sa vysávať nemajú. Domáci silný vysávač neprikladajte k okraju klobúka ani chrbtu knihy. Ak sa povrch zdvíha, praská alebo prášok vychádza priamo z apretúry, čistenie zastavte a zachytenú časticu nevyhadzujte pred posúdením.",
            ],
        },
        {
            "heading": "Čo urobí kvapka vody so škrobom, glejom a živicou",
            "paragraphs": [
                "Vodou rozpustná apretúra môže lokálne zmäknúť už pri vlhkej handričke. Počas schnutia sa rozpustená zložka a jemná nečistota pohybujú k okraju mokrej zóny, kde vytvoria svetlejší, tmavší alebo tvrdší lem. Ak sa miesto mechanicky pritlačí, väzba sa vyrovná inak než okolie a zmena ostane viditeľná aj po návrate tuhosti.",
                "Syntetická živica nemusí zmäknúť ihneď, no môže zbelieť, napučať alebo reagovať na alkohol a teplo. Bez technického listu nemožno bezpečne odhadnúť rozpustnosť podľa veku či farby. Skúšobné miesto musí byť malé, podopreté a hodnotené suché. Pridanie väčšieho množstva vody s cieľom zjednotiť mapu môže zmeniť celý diel a preniesť problém do poťahu alebo lepidla.",
            ],
        },
        {
            "heading": "Ako čistiť klobúk s buckramovou kostrou",
            "paragraphs": [
                "Najprv určte poťah, podšívku, pásku, lepidlo, kovové prvky, perie a ozdoby. Povrchová škvrna na poťahu nemusí byť prístupná bez navlhčenia kostry pod ním. Lokálny roztok preto nanášajte iba po skúške a po kvapkách kontrolovaných savým materiálom, nie striekaním celej koruny. Okraj pri práci podoprite v pôvodnom oblúku.",
                "Ak sa koruna prepadla, nepolievajte ju a nesušte na miske náhodného rozmeru. Klobučník používa tvar, paru alebo aktiváciu primeranú konkrétnemu materiálu a vie znovu napnúť poťah. Domáci fén môže vytvoriť tvrdý okraj, uvoľniť lepidlo a zraziť textil. Pri hodnotnom, starom alebo bohato zdobenom klobúku je rozobratie a opätovné blokovanie bezpečnejšie v dielni.",
            ],
        },
        {
            "heading": "Knižná väzba a krabica: textil nemožno oddeliť od papiera",
            "paragraphs": [
                "Na knihe môže mokrá handra preniesť vlhkosť cez tkaninu do lepenky, papiera, farby a lepidla. Doska sa zvlni, chrbát stuhne v inom uhle a pigment alebo razba sa zotrú skôr, než sa odstráni škvrna. Prach z väzby sa preto odstraňuje kontrolovane nasucho a kniha sa počas práce podopiera. Tekutinu po nehode odsajte bez roztvárania mokrého chrbta silou.",
                "Plesnivý alebo zapáchajúci kus neuzatvárajte s ostatnými knihami. Najprv riešte prostredie, zdroj vlhkosti a možnú kontamináciu. Biely povlak nemusí byť vždy pleseň; môže ísť o migrujúcu úpravu alebo produkt degradácie. Domáci alkohol, ocot ani bielidlo nepoužívajte ako diagnostický test. Kníhviazač alebo konzervátor dokáže rozlíšiť povrch, stabilitu farby a možnosti rozobratia.",
            ],
        },
        {
            "heading": "Buckram v golieri, páse a kostýmovej výstuži",
            "paragraphs": [
                "V odeve buckram nesie golier, pás, klobúkový tvar alebo kostýmový objem. Pri praní sa vrchná látka, výstuž, podšívka a nite môžu zmeniť rozdielne. Výsledkom je zvlnený okraj, bublina, pokrútený diel alebo tvrdý kruh. Symbol profesionálneho čistenia rešpektujte aj vtedy, keď vrchný poťah vyzerá ako bežná bavlna.",
                f"Jednoduché porovnanie s <a href=\"{ARTICLE_CANVAS}\">hustým canvasom</a> nestačí, pretože plátno môže byť samo nosné, kým buckram sa spolieha na apretúru. Ak je výstuž odnímateľná a výrobca ju označí ako prateľnú, perte ju samostatne podľa technického listu a sušte v rozmere. Vstavaný diel neprešívajte a neškrobte naslepo; zmena tuhosti ovplyvní celý strih.",
            ],
        },
        {
            "heading": "Vodná mapa, mastný dotyk a neznáma škvrna",
            "paragraphs": [
                f"Pri čerstvej kvapaline najprv odsajte prebytok bez rozširovania zóny. Podľa <a href=\"{ARTICLE_STAIN}\">sprievodcu rôznymi škvrnami</a> odlíšte vodný zvyšok, mastnotu, pigment a bielkovinu, no postup prispôsobte citlivej apretúre. Mastný dotyk na okraji klobúka môže byť v poťahu, podšívke aj nosnej vrstve; silný odmasťovač môže vytiahnuť farbu a rozpustiť lepidlo.",
                "Starú mapu neobkresľujte ďalšími mokrými kruhmi. Na malej zóne skúšajte savosť a prenos, pričom okolie podoprite. Ak sa po vysušení vytvorí tvrdší lem alebo sa kostra zmäkčí, nepokračujte. Pri hodnote predmetu je lepšie ponechať miernu škvrnu než dosiahnuť rovnomernú čistotu za cenu straty tvaru.",
            ],
        },
        {
            "heading": "Ako podoprieť a vytvarovať navlhčený samostatný diel",
            "paragraphs": [
                "Tvarovanie je prípustné iba pri type určenom výrobcom na aktiváciu vodou alebo parou. Pred navlhčením odmerajte šírku, dĺžku a krivku, pripravte čistú nekorodujúcu formu a ochrannú medzivrstvu. Materiál navlhčite rovnomerne v predpísanom rozsahu, neťahajte otvorenú väzbu za jeden roh a vyhladzujte bez ostrých záhybov.",
                "Diel nechajte na opore až do úplného vyschnutia v celej hrúbke. Predčasné zloženie vytvorí lom, kým uzatvorenie vlhkého kusu podporí pach a mikrobiálny rast. Ak sa rozmer mení nerovnomerne alebo povrch lepkavie, proces zastavte. Nie každú stratu tuhosti možno obnoviť domácim škrobom; iná receptúra zmení farbu, pružnosť aj budúcu opraviteľnosť.",
            ],
        },
        {
            "heading": "Pleseň, hmyz a dlhodobé skladovanie",
            "paragraphs": [
                "Buckram skladujte čistý, suchý, chránený pred svetlom, prachom a tlakom. Klobúk podoprite tak, aby okraj neniesol hmotnosť koruny, a knihu nestláčajte medzi deformované zväzky. Vlhká pivnica a horúca povala prinášajú veľké zmeny teploty a relatívnej vlhkosti, ktoré podporujú zvlnenie, krehkosť, lepkavosť aj pleseň.",
                "Pri podozrení na hmyz alebo pleseň predmet izolujte bez vzduchotesného uzavretia mokrého materiálu a zdokumentujte nález. Neaplikujte parfum ani insekticíd priamo na textil. Prach z aktívnej plesne neroznášajte vysávačom bez vhodnej filtrácie a ochrany. Najprv odstráňte príčinu vlhkosti a pri historickom predmete konzultujte bezpečný spôsob dekontaminácie.",
            ],
        },
        {
            "heading": "Kedy sa oplatí oprava namiesto čistenia",
            "paragraphs": [
                "Zlomený okraj klobúka, uvoľnená väzba knihy, prepadnutý kostýmový diel alebo oddelená výstuž potrebujú konštrukčnú opravu. Čistenie môže odstrániť prach, ale nevráti nosnosť ani správne napätie. Pred zásahom si určte prioritu: vzhľad, funkciu, historický materiál alebo možnosť predmet používať. Každý cieľ môže viesť k inému kompromisu.",
                "Pri novom výrobku odfoťte stav a kontaktujte predajcu skôr, než pridáte škrob, lepidlo alebo steh. Pri staršom kuse uchovajte uvoľnené časti a informáciu o predchádzajúcej vode či produkte. Odborník potom môže zvoliť podloženie, lokálne spevnenie, nové tvarovanie alebo výmenu skrytej výstuže bez zbytočného odstránenia pôvodného materiálu.",
            ],
        },
    ],
    "table2_heading": "Prejav na buckrame: čo pravdepodobne znamená",
    "table2_intro": "Stav posudzujte pri rovnakom svetle až po úplnom vyschnutí. Tuhosť, farba a tvar sú rovnako dôležité ako viditeľná škvrna.",
    "table2_headers": ["Prejav", "Možná príčina", "Čo overiť", "Bezpečný ďalší krok"],
    "table2_rows": [
        ("Mäkký kruh po vode", "Lokálne rozpustenie alebo presun apretúry.", "Návrat tuhosti, okraj mapy a podklad.", "Ďalšiu vodu nepridávať; podoprieť a odborne posúdiť."),
        ("Biely prášok", "Prach, migrujúca úprava, degradácia alebo pleseň.", "Či je povrch aktívny, lepkavý a spojený s vlhkosťou.", "Neutierať mokro; izolovať a identifikovať príčinu."),
        ("Prepadnutá koruna", "Strata nosnosti, zlomenie alebo navlhnutie výstuže.", "Poťah, švy, tvar a reakciu na ľahkú oporu.", "Nenaparovať naslepo; zvoliť klobučníka."),
        ("Vlnitá knižná doska", "Rozdielne napučanie textilu, papiera a lepidla.", "Vlhkosť, chrbát, farbu a pevnosť spoja.", "Sušiť s odbornou oporou, knihu neotvárať silou."),
        ("Lepkavý povrch", "Zmena živice, lepidla alebo nevhodný čistič.", "Teplotu, použité produkty a rozsah lepkavosti.", "Zastaviť čistenie a oddeliť od priľahlých plôch."),
    ],
    "steps_heading": "Bezpečné rozhodovanie pri čistení buckramu",
    "steps": [
        "Určte funkciu buckramu a či je samostatný, odnímateľný alebo vstavaný.",
        "Zaznamenajte poťah, podšívku, papier, lepidlo, kov, kožu a ozdoby.",
        "Odfoťte tvar, rozmer, tuhosť, mapy, praskliny a uvoľnené časti.",
        "Voľný prach odstráňte primeraným suchým spôsobom s pevnou oporou.",
        "Na skrytom okraji otestujte farbu, tuhosť, lepkavosť a úplné vyschnutie.",
        "Vodu použite iba v rozsahu výslovne povolenom pre celý predmet.",
        "Navlhčený diel podoprite v správnom tvare a neurychľujte schnutie teplom.",
        "Pri zmene apretúry, vrstiev alebo tvaru proces okamžite zastavte.",
        "Pred uložením overte suchosť v celej hrúbke a stabilitu povrchu.",
        "Konštrukčné poškodenie riešte opravou, nie opakovaným čistením.",
    ],
    "remember": [
        "Drží tvar samotná väzba alebo vodou či teplom citlivá apretúra?",
        "Môže sa nosná vrstva oddeliť bez párania alebo poškodenia predmetu?",
        "Aké ďalšie materiály by voda, para alebo rozpúšťadlo zasiahli?",
        "Je biely povlak prach, úprava, degradácia alebo možná pleseň?",
        "Má navlhčený diel pripravenú čistú oporu presného tvaru?",
        "Je cieľom odstrániť škvrnu, obnoviť tvar alebo zachovať originál?",
    ],
    "mistakes": [
        "Prať celý predmet iba preto, že základná tkanina vyzerá ako bavlna.",
        "Utierať knižnú väzbu alebo klobúk veľkou mokrou handrou.",
        "Zvyšovať dávku, keď skúšobné miesto mäkne alebo lepkavie.",
        "Sušiť prepadnutý tvar horúcim fénom na náhodnej forme.",
        "Nahradiť neznámu pôvodnú apretúru domácim škrobom bez skúšky.",
        "Považovať prasknutú výstuž alebo zvlnenú dosku za nečistotu.",
    ],
    "expert_heading": "Odbornejší pohľad: apretúra je súčasť konštrukcie",
    "expert": [
        "University of Virginia opisuje knižný buckram ako odolnú poťahovú tkaninu, pri ktorej sa dnes používa bavlna a polyester s akrylovou úpravou. Tento príklad ukazuje, prečo samotné tradičné pomenovanie nestačí na určenie rozpustnosti alebo teploty: výsledné správanie vytvára základná tkanina spolu s konkrétnym povlakom.",
        "Buckram je iba jedna z možností, ako výrobok vystužiť. Rovnaký golier môže používať vlasové súkno, plátno, netkanú výstuž alebo lepený systém. Čistiaci postup preto musí rešpektovať všetky vrstvy a spôsob ich spojenia, nie iba názov viditeľnej vrchnej látky.",
        "Príručka National Park Service pre textilné predmety oddeľuje povrchové vysávanie, mokré čistenie, chemické čistenie a lokálne ošetrenie a pri nestabilných či významných kusoch určuje hranicu pre konzervátora. National Library of Medicine pri mokrých textíliách navyše zdôrazňuje úplnú oporu, šetrné sušenie a zákaz manipulácie s krehkými zlepenými vrstvami. Tieto zásady podporujú opatrnosť, nie univerzálne povolenie buckram namočiť.",
    ],
    "source_intro": "Zdroje podporujú rozdiel medzi tkaným základom, tužiacou úpravou, vstavanou výstužou a celým predmetom. Nepodporujú univerzálne mokré čistenie klobúkov, kníh ani historických výstuží.",
    "sources": [
        ("University of Virginia Library: buckram ako knižná poťahová tkanina", UVA_BUCKRAM),
        ("National Park Service: čistenie a starostlivosť o textilné predmety", NPS_TEXTILE_OBJECTS),
        ("National Library of Medicine: opora a sušenie mokrých textílií", NLM_WET_TEXTILES),
        ("GINETEX: symboly ošetrovania", GINETEX),
        ("EÚ 1007/2011: označovanie textilných vlákien", EU_FIBRE_LABEL),
    ],
    "related": [
        ("Ako čítať štítok na oblečení", ARTICLE_LABEL),
        ("Čo je canvas a ako sa oň starať", ARTICLE_CANVAS),
        ("Ako postupovať pri rôznych škvrnách", ARTICLE_STAIN),
        ("Prečo sa textil zráža", ARTICLE_SHRINKAGE),
        ("Ako sušiť textil bez zatuchnutia", ARTICLE_DRYING),
        ("Ako riešiť vytiahnutú niť", ARTICLE_SNAGGING),
    ],
    "faq_title": "buckram, výstuž a bezpečné čistenie",
    "faq": [
        ("Čo je buckram?", "Tkaná oporná látka spevnená škrobom, glejom, živicou alebo inou úpravou, používaná na tvar a výstuž."),
        ("Je buckram vždy bavlnený?", "Nie. Tradičný býva bavlnený alebo ľanový, moderné varianty môžu obsahovať syntetiku, povlak alebo lamináciu."),
        ("Dá sa buckram prať?", "Iba ak to výslovne povoľuje výrobca konkrétneho materiálu a všetky vrstvy hotového predmetu."),
        ("Prečo buckram po vode zmäkne?", "Voda môže rozpustiť alebo presunúť tužiacu zložku, ktorá obmedzuje pohyb nití."),
        ("Ako vyčistiť buckramový klobúk?", "Najprv odstráňte voľný prach, overte poťah a ozdoby a mokrý zásah robte iba po skrytej skúške; hodnotný kus patrí klobučníkovi."),
        ("Môžem vysávať starý buckram?", "Len pri stabilnom povrchu, nízkom ťahu, pevnej opore a ochrannej sieťke; nie pri uvoľnených ozdobách alebo prášiacej úprave."),
        ("Ako odstrániť vodnú mapu?", "Ďalšiu vodu nepridávajte naslepo. Skontrolujte tuhosť a farbu a pri nosnej alebo historickej vrstve zvoľte odborníka."),
        ("Dá sa strata tuhosti opraviť škrobom?", "Nie spoľahlivo. Iná apretúra môže zmeniť farbu, pružnosť, tvar a budúcu opraviteľnosť."),
        ("Ako sušiť navlhčený buckram?", "Na čistej opore správneho tvaru, rovnomerne, bez radiátora a fénu, až do sucha v celej hrúbke."),
        ("Je buckram to isté ako plátno?", "Nie. Plátno môže držať tvar najmä hustotou a hrúbkou; buckram sa typicky spolieha aj na výraznú tužiacu úpravu."),
        ("Ako čistiť buckram v knižnej väzbe?", "Prednostne kontrolovane nasucho. Voda môže zvlniť lepenku, ovplyvniť farbu a uvoľniť lepidlo."),
        ("Kedy zavolať odborníka?", "Pri zmene tvaru, lepkavosti, plesni, oddelených vrstvách, historickom predmete alebo potrebe rozobrať konštrukciu."),
        ("Ako buckram skladovať?", "Čistý, suchý, podopretý, mimo svetla, prudkých zmien vlhkosti a tlaku iných predmetov."),
    ],
}

add_cards(
    BUCKRAM,
    product_heading="Prací gél patrí iba k oddeliteľnému prateľnému dielu",
    product_intro="Ak je nový samostatný buckramový diel alebo jeho textilný poťah výrobcom označený ako prateľný, možno zvoliť kompatibilný tekutý prostriedok a presné dávkovanie.",
    product_text="Prací gél Vevo použite iba podľa štítka, po skrytej skúške a s úplným opláchnutím. Koncentrát nelejte priamo na apretúru ani na viditeľnú mapu.",
    product_limit="Produkt nie je určený na knižné väzby, klobúkové kostry, papier, lepidlá, historické predmety ani vstavané výstuže bez povoleného mokrého čistenia.",
    category_heading="Pracie gély vyberajte až po potvrdení prateľnosti",
    category_intro="Pri buckrame je prvým rozhodnutím ochrana tvaru a apretúry. Kategória pracích prostriedkov dáva zmysel len pri kompatibilnom oddeliteľnom textilnom diele.",
    category_text="Porovnajte pracie gély pre bežnú prateľnú bielizeň a riaďte sa najcitlivejšou vrstvou. Pri klobúku, knihe alebo štruktúrovanom odeve zvoľte špecializovanú starostlivosť.",
)


MADRAS: dict[str, object] = {
    "title": "Čo je madras: károvaná košeľovina, púšťanie farby a správne pranie",
    "link": "co-je-madras-karovana-koselovina-pustanie-farby-a-spravne-pranie",
    "meta": "Čo je madras, ako sa líši od ginghamu a potlače, čo znamená bleeding Madras a ako prať károvanú košeľu bez prenosu farby, zrazenia a skrútenia.",
    "short": "Madras je ľahká tkaná košeľovina spájaná s farebnými pruhmi a károm, tradične z bavlny a farbených priadzí. Historický bleeding Madras zámerne uvoľňoval časť farby, no nie každý moderný madras má púšťať.",
    "name": "madras",
    "locative": "madrase",
    "identity_heading": "Madras opisuje ľahkú tkaninu a farebnú tradíciu, nie každé káro",
    "identity_detail": "Typický madras používa ľahkú plátnovú bavlnenú konštrukciu s pruhmi alebo károm vytvoreným farbenými osnovnými a útkovými priadzami, takže motív je čitateľný na oboch stranách.",
    "identity_boundary": "Moderný výrobok môže byť farebne stály, potlačený, zmesový alebo zošitý z viacerých kúskov, zatiaľ čo historický bleeding Madras bol osobitný variant, pri ktorom sa uvoľňovanie farby stalo súčasťou meniaceho sa vzhľadu.",
    "label_focus": "bavlnu alebo zmes, tkaný či tlačený motív, kontrast svetlých a tmavých priadzí, patchworkové švy, podšívku, výšivku, stálofarebnosť, zrážanie a povolené sušenie",
    "missing_label": "Neoznačený nový farebný kus perte prvýkrát oddelene až po skúške prenosu; ak predajca sľubuje stálofarebnosť, trvalé farbenie pokožky alebo okolitých polí dokumentujte pre reklamáciu.",
    "dry_check": "prenos farby na svetlý golier, biele zalomenia, rozdiel medzi lícom a rubom, potlač bez zmeny väzby, uvoľnené spojovacie nite, skrútený šev, zrazené diely a pigment na pokožke",
    "damage_boundary": "Voľné farbivo a škvrnu možno riešiť vhodným praním, no zosvetlený pruh, už zafarbené svetlé pole, roztrhnutá priadza alebo trvalo skrútený šev sa ďalším silnejším cyklom nemusia napraviť.",
    "test_focus": "Bielou vlhkou handričkou skúšajte každú sýtu farbu aj miesto, kde tmavá priadza križuje svetlú; výsledok porovnajte po vysušení bez silného trenia.",
    "combined_risk": "uvoľnenia farbiva, trenia svetlých a tmavých priadzí, napučania bavlny, zrazenia a skosenia ľahkej konštrukcie pri dlhom mokrom stave",
    "chemistry_boundary": "Ocot alebo soľ nedokážu univerzálne zmeniť nevhodne zvolené či slabo naviazané farbivo na stálofarebné a oxidujúce bielidlo môže zmeniť pôvodné tóny aj svetlé polia.",
    "drying_detail": "Košeľu vyberte hneď, rozložte golier, manžety a švy, skontrolujte prenos farby a sušte bez kontaktu s bielou stenou, uterákom alebo iným kusom, kým je ešte mokrá.",
    "heat_boundary": "Vysoká teplota môže podporiť zrážanie bavlny, zvýrazniť skrútenie švov, zataviť syntetickú zložku alebo zafixovať škvrnu, ktorú ste pred žehlením neodstránili.",
    "stop_signs": "silný prenos farby po opakovanom oplachu, zafarbenie svetlých polí, rastúce zrazenie, skrútenie šva, oslabenie priadze, praskanie potlače alebo dráždivý zvyšok",
    "professional_boundary": "Bežnú prateľnú bavlnenú košeľu možno ošetrovať doma podľa etikety, kým starší ručne tkaný kus, hodnotný patchwork, zložitá podšívka alebo nejasne deklarovaný bleeding Madras potrebuje individuálne posúdenie.",
    "answer": "Madras je ľahká tkanina, tradične bavlnená, s pruhmi alebo károm vytvoreným farbenými priadzami. Historický bleeding Madras bol osobitný variant, pri ktorom sa farba pri praní zámerne uvoľňovala a odtiene sa postupne tlmili. To však neznamená, že každý moderný madras má farbiť pokožku, biele časti odevu alebo ostatnú bielizeň. Pred prvým praním prečítajte štítok, urobte skúšku suchého a mokrého prenosu a nový sýty kus perte oddelene. Použite najnižšiu povolenú teplotu, primeraný program a presnú dávku. Po cykle ho ihneď vyberte, urovnajte švy a nechajte schnúť bez kontaktu so svetlým textilom.",
    "intro": "Madras spája remeselnú históriu, letnú košeľovinu a výrazné farebné káro. Práve preto vzniká nedorozumenie: rada, že pravý madras má púšťať, sa prenesie aj na modernú farebne stálu košeľu, alebo sa naopak historický bleeding variant reklamuje za každú zmenu odtieňa. Praktická starostlivosť potrebuje zistiť, či je motív tkaný alebo tlačený, aké vlákna a farbivá výrobca deklaruje a či uvoľnená farba ostáva iba vo vode, alebo zafarbí svetlé priadze, pokožku a iné povrchy.",
    "quick": [
        "<strong>Nie každé káro je madras:</strong> názov sa viaže na ľahkú tkaninu a historickú textilnú tradíciu.",
        "<strong>Motív býva tkaný:</strong> farebné osnovné a útkové priadze vytvoria pruhy a štvorce viditeľné na oboch stranách.",
        "<strong>Bleeding Madras je osobitný variant:</strong> zámerná zmena farieb nie je licencia pre každý moderný kus trvalo zafarbovať okolie.",
        "<strong>Prvý cyklus robte oddelene:</strong> sýty nový kus nesmie ohroziť bielu bielizeň ani vlastné svetlé polia.",
        "<strong>Ocot nie je univerzálny fixátor:</strong> domáci kúpeľ nenahradí správne farbivo a technologické ustálenie.",
        "<strong>Mokrý kus nenechávajte v koši:</strong> dlhý kontakt podporuje prenos farby medzi preloženými plochami.",
    ],
    "overview_heading": "Ako vzniká madrasové káro a čo presne znamená bleeding Madras",
    "overview": [
        "Pri tkanom káre sa farebné pásy osnovy križujú s farebnými pásmi útku. V priesečníkoch sa opticky miešajú a vytvoria ďalšie odtiene bez potlače na hotovom povrchu. Ľahká bavlnená priadza a plátnová väzba dávajú materiálu vzdušnosť, no zároveň môžu zvýrazniť nerovnosť ručného tkania, posun nite a zmenu rozmeru.",
        "Indická spoločnosť Co-optex vo svojej histórii opisuje bleeding Madras ako ručne tkané káro z farbených priadzí, pri ktorom sa odtiene po praní menili a prelínali. Ide o konkrétnu regionálnu a výrobnú tradíciu, nie o povinnú vlastnosť každej dnešnej košele s károm ani o ospravedlnenie neželaného zafarbenia ostatnej bielizne.",
        "Moderná stálofarebnosť sa hodnotí definovanými skúškami, nie vetou farba nepustí. ISO 105-C06 skúma odolnosť farby voči domácemu a komerčnému praniu, kým AATCC uvádza samostatné metódy pre pranie, suché a mokré trenie či pot. Výsledok závisí od farbiva, vlákna, koncentrácie, teploty, pohybu a susednej textílie.",
    ],
    "table1_heading": "Madras, gingham, tartan, patchwork a potlačené káro",
    "table1_intro": "Vzhľad môže byť podobný, ale konštrukcia mení spôsob, akým sa farba prenáša a ako sa po praní posúvajú švy a diely.",
    "table1_headers": ["Označenie", "Ako vzniká vzor", "Typický dojem", "Čo kontrolovať pri praní"],
    "table1_rows": [
        ("Madras", "Farebné pruhy osnovy a útku, často ľahká bavlna.", "Nepravidelnejšie viacfarebné káro alebo pruhy.", "Prenos každej farby, zrážanie, skosenie a švy."),
        ("Bleeding Madras", "Variant s farbivami zámerne meniacimi vzhľad pri praní.", "Postupne tlmenejšie a prelínajúce sa odtiene.", "Či zmena zodpovedá deklarácii a nepoškodzuje okolie."),
        ("Gingham", "Pravidelné farebné a svetlé priadze v rovnomernom opakovaní.", "Čisté malé alebo stredné štvorce.", "Prenos tmavej priadze do svetlého poľa."),
        ("Tartan", "Definované usporiadanie pruhov, často v keprovej väzbe.", "Opakujúci sa viacfarebný set s diagonálnou kresbou.", "Vlákno, keprové skosenie a profesionálne hranice."),
        ("Potlačené káro", "Farba je nanesená na hotovú základnú tkaninu.", "Rub môže byť svetlejší alebo bez plného motívu.", "Oder a praskanie tlače, prenos pigmentu a podklad."),
    ],
    "sections": [
        {
            "heading": "Ako rozoznať tkané káro od potlače",
            "paragraphs": [
                "Pozrite sa lupou z líca aj rubu. Pri tkanom káre prechádzajú farebné priadze celou dĺžkou alebo šírkou a motív je čitateľný na oboch stranách, hoci rub môže mať iný lesk. Pri potlači vidíte základnú väzbu a farbu sústredenú najmä na líci. Tenké miesto nenapínajte silou a nevyťahujte niť z hotovej košele.",
                "Patchwork je ďalší systém: vzor nevzniká iba v jednej tkanine, ale zošitím kúskov s rôznym smerom, zložením a farbou. Každý diel môže zrážať alebo púšťať inak. Pred prvým praním skontrolujte všetky švy, svetlé susedné polia a prípadné lepidlo aplikácie. Jediná skúška na modrom štvorci nepotvrdí správanie červenej priadze ani spojovacej nite.",
            ],
        },
        {
            "heading": "Suchá a mokrá skúška prenosu farby",
            "paragraphs": [
                f"Najprv prečítajte <a href=\"{ARTICLE_COLOR}\">vysvetlenie stálofarebnosti</a>. Na skrytom mieste pritlačte bielu suchú bavlnenú handričku bez agresívneho trenia a potom samostatnou navlhčenou handričkou zopakujte krátky kontrolovaný kontakt. Skúšajte každú sýtu farbu, šev aj miesto, kde sa tmavé a svetlé priadze dotýkajú.",
                "Slabý odtieň na handričke je varovanie pre samostatné pranie, nie presná laboratórna známka. Silný prenos za mokra znamená riziko pre golier, manžetu, vlastné svetlé štvorce aj ďalšiu bielizeň. Skúšku nechajte vyschnúť a posúďte aj pôvodné miesto. Ak sa objaví svetlý fľak alebo farba pokračuje po opakovanom jemnom oplachu, stav zdokumentujte.",
            ],
        },
        {
            "heading": "Ako prvýkrát vyprať madrasovú košeľu",
            "paragraphs": [
                f"Použite <a href=\"{ARTICLE_LABEL}\">symboly na štítku</a>, zapnite alebo zabezpečte gombíky podľa konštrukcie, vyprázdnite vrecká a košeľu obráťte naruby, ak to výrobca nevylučuje. Nový sýty kus perte samostatne alebo iba s overenými podobnými farbami. Bubon nepreplňte a koncentrát nelejte priamo na suché káro.",
                "Vyberte najnižšiu povolenú teplotu a mechaniku, ktorá zodpovedá znečisteniu, nie automaticky horúci bavlnený program. Po cykle skontrolujte vodu, vlastné svetlé polia a prenos na čistú vlhkú handričku. Košeľu nenechávajte zloženú v bubne ani v koši. Ihneď urovnajte légu, golier, manžety a bočné švy.",
            ],
        },
        {
            "heading": "Červeno-biele a modro-biele káro bez zafarbenia svetlých polí",
            "paragraphs": [
                "Kontrastný kus je citlivý aj vtedy, keď sa perie sám, pretože tmavá priadza leží priamo vedľa svetlej. Dlhé namáčanie a ponechanie mokrej košele preloženej cez seba predlžujú kontakt uvoľneného farbiva s bielym poľom. Pracujte rýchlo podľa etikety, použite dostatok vody a po cykle odev rozložte bez odkladu.",
                "Ak sa biele pole zafarbí, nevysušujte ho vysokou teplotou a nepridávajte naslepo chlór. Odfarbovač môže vytiahnuť aj pôvodné farby a poškodiť zmes či výšivku. Odev ponechajte vlhký iba tak dlho, ako vyžaduje bezpečný postup, oddeľte ho od ostatných povrchov a kontaktujte výrobcu alebo odbornú čistiareň s fotografiou a presným opisom cyklu.",
            ],
        },
        {
            "heading": "Pomôže ocot, soľ alebo obrúsok na zachytenie farby",
            "paragraphs": [
                "Domáce rady zamieňajú podmienky farbenia s neskorším praním hotového odevu. Ocot môže meniť pH kúpeľa, ale neprevedie každé slabo naviazané farbivo na stálofarebné. Soľ sa používa v určitých farbiarskych procesoch, no neurčená dávka v domácej práčke nie je univerzálny fixátor. Obe látky navyše môžu ovplyvniť kov, gumu, zvyšky produktu a oplach.",
                "Obrúsok na zachytenie farby môže časť uvoľneného farbiva naviazať, ale nechráni spoľahlivo vlastné biele pole a neopraví chybnú stálofarebnosť. Vnímajte ho ako doplnok podľa návodu, nie povolenie miešať rizikové farby. Najdôležitejšie zostáva triedenie, skúška, správna teplota, krátky mokrý kontakt a okamžité vybratie.",
            ],
            "callout": {
                "title": "Čo má pri púšťaní farby najväčší význam",
                "items": [
                    "Overiť štítok a deklaráciu výrobcu ešte pred prvým praním.",
                    "Skúšať každú sýtu priadzu nasucho aj za mokra.",
                    "Nový kontrastný kus prať oddelene a nenechať ho mokrý zložený.",
                    "Silný alebo pretrvávajúci prenos zdokumentovať namiesto opakovaných domácich kúpeľov.",
                ],
                "background": "#f7fbf8",
                "border": "#dbe5de",
            },
        },
        {
            "heading": "Zrážanie, skosenie a skrútenie švov",
            "paragraphs": [
                f"Ľahká bavlna môže pri prvom praní zmeniť rozmer a tkanina môže uvoľniť napätie z výroby. Mechanizmus vysvetľuje článok <a href=\"{ARTICLE_SHRINKAGE}\">prečo sa oblečenie zráža</a>. Pred prvým cyklom odmerajte dĺžku medzi pevnými bodmi, šírku hrudníka a polohu bočných švov. Fotografia kára pomôže odlíšiť zrazenie od optického posunu vzoru.",
                "Mokrý odev neťahajte za diagonálu a nesnažte sa násilne vrátiť každý štvorec do pravého uhla. Urovnajte osnovu, útok a švy v prirodzenom rozmere. Ak sa jeden patchworkový diel zrazí viac, ťah sa sústredí v šve a môže otvoriť dierku. Výrazná zmena nového výrobku pri dodržanom štítku patrí do dokumentácie pre predajcu.",
            ],
        },
        {
            "heading": "Škvrna na madrasovom káre bez svetlej mapy",
            "paragraphs": [
                f"Prebytok odsajte a pôvod škvrny určte podľa <a href=\"{ARTICLE_STAIN}\">praktického sprievodcu škvrnami</a>. Produkt skúšajte na všetkých farbách, ktorých sa lokálne ošetrenie dotkne. Silné trenie môže odstrániť povrchové farbivo z tmavej priadze a súčasne zatlačiť nečistotu do svetlej. Výsledkom je svetlý kruh nápadnejší než pôvodná stopa.",
                "Neaplikujte bielidlo iba preto, že časť kára je biela. Roztok sa kapilárne dostane k farebnej priadzi a môže meniť odtieň aj pevnosť. Pri oleji pracujte s kompatibilným tenzidom a dôkladným oplachom, pri bielkovine nezačínajte vysokou teplotou. Pred sušením a žehlením musí zmiznúť škvrna aj zvyšok produktu.",
            ],
        },
        {
            "heading": "Patchworkový madras: každý diel môže reagovať inak",
            "paragraphs": [
                "Patchwork spája viac tkanín, smerov väzby, nití a niekedy aj výstuž. Jedna časť môže byť predpraná, druhá nie; jedna farebne stála, druhá zámerne meniaca odtieň. Pred celkovým praním skúšajte každý výrazný diel a spojovaciu niť. Uvoľnený šev opravte skôr, než sa pri mokrom pohybe otvor zväčší.",
                "Pri sušení rozložte hmotnosť rovnomerne, aby ťažší nasýtený diel neťahal ľahší. Žehlenie prispôsobte najcitlivejšej časti a používajte ochrannú tkaninu. Hrubšie prekrížené švy môžu schnúť dlhšie a pri vysokom tlaku sa pretlačiť na líc. Ak odev obsahuje lepidlo alebo výstuž, symbol celého výrobku má prednosť pred radou pre bavlnu.",
            ],
        },
        {
            "heading": "Ako madras sušiť a žehliť bez ďalšieho prenosu",
            "paragraphs": [
                f"Po cykle košeľu vyberte okamžite a sušte tak, aby sa mokré tmavé polia dlho neopierali o svetlú vrstvu. V interiéri pomáha prúdenie vzduchu podľa návodu <a href=\"{ARTICLE_DRYING}\">ako sušiť bez zatuchnutia</a>. Odev nedávajte na biely radiátor, uterák ani čalúnenie, kým skúška nepotvrdí, že za mokra neprenáša farbu.",
                f"Žehlite až po kontrole škvrny a prenosu. Podľa <a href=\"{ARTICLE_IRONING}\">postupu pre košeľu</a> začnite golierom a menšími dielmi, no pri madrase navyše skúšajte ochrannú tkaninu na každej sýtej farbe. Teplotu určuje najcitlivejšie vlákno a potlač. Silný tlak môže vytvoriť lesklé štvorce a zafixovať zvyšnú nečistotu.",
            ],
        },
        {
            "heading": "Kedy je zmena farby vlastnosť a kedy reklamácia",
            "paragraphs": [
                "Pri deklarovanom bleeding Madras môže byť postupné tlmenie a prelínanie odtieňov súčasťou zamýšľaného vzhľadu. Aj vtedy však výrobca potrebuje uviesť ošetrenie a používateľ má vedieť, že kus môže farbiť. Zmena by nemala byť prekvapením spôsobeným chýbajúcim návodom ani viesť k nebezpečnému prenosu na pokožku a okolie.",
                "Pri modernom výrobku deklarovanom ako stálofarebný je silný opakovaný prenos, zafarbenie vlastných bielych polí alebo výrazná zmena rozmeru dôvodom na dokumentáciu. Odfoťte etiketu, odev pred a po, použitý program, teplotu a produkt. Pred reklamáciou ho neodfarbujte, neprešívajte ani opakovane neperte agresívnejšie, lebo zmeníte dôkaz aj možnosť odbornej analýzy.",
            ],
        },
    ],
    "table2_heading": "Madras po praní: príčina a ďalší krok",
    "table2_intro": "Odev hodnotíte úplne suchý, no prenos farby kontrolujete aj za mokra. Rozlíšte zámernú patinu, voľné farbivo a mechanické poškodenie.",
    "table2_headers": ["Prejav", "Pravdepodobná príčina", "Čo overiť", "Bezpečný ďalší krok"],
    "table2_rows": [
        ("Voda je mierne sfarbená", "Prebytočné alebo zámerne fugítne farbivo.", "Deklaráciu, vlastné svetlé polia a ďalší mokrý prenos.", "Prať oddelene a sledovať vývoj podľa etikety."),
        ("Biele pole je ružové alebo modré", "Prenos farbiva počas prania alebo mokrého kontaktu.", "Ktorá priadza púšťa a či odev už vyschol teplom.", "Nepridávať chlór; riešiť kompatibilný postup alebo reklamáciu."),
        ("Bočný šev sa skrútil", "Uvoľnenie napätia, skosenie alebo rozdielne zrazenie.", "Rozmer pred a po a polohu kára.", "Urovnať za vlhka bez násilia; výraznú zmenu dokumentovať."),
        ("Tmavé pole má svetlý kruh", "Lokálny odstraňovač, trenie alebo strata farbiva.", "Prenos, povrch pri bočnom svetle a použitý produkt.", "Ďalšie drhnutie zastaviť; mechanická zmena sa nemusí vyprať."),
        ("Farba zostáva na pokožke", "Nízka stálosť voči treniu alebo potu.", "Suchý a mokrý prenos a deklaráciu výrobcu.", "Odev nenosiť na citlivej pokožke; kontaktovať predajcu."),
    ],
    "steps_heading": "Bezpečný prvý cyklus madrasovej košele",
    "steps": [
        "Prečítajte zloženie, symboly a informáciu o možnej zmene farby.",
        "Odfoťte káro, rozmery, bočné švy a vlastné svetlé polia.",
        "Urobte suchú a mokrú skúšku každej sýtej priadze na skrytom mieste.",
        "Nový kontrastný kus perte oddelene pri najnižšej povolenej teplote.",
        "Použite presnú dávku kompatibilného prostriedku a dostatok priestoru na oplach.",
        "Po cykle skontrolujte prenos a odev okamžite rozložte.",
        "Urovnajte légu, golier, manžety a švy bez ťahania diagonálou.",
        "Sušte mimo svetlých povrchov a bez prudkého lokálneho tepla.",
        "Žehlite až po odstránení škvŕn a overení farby, z rubu pri povolenej teplote.",
        "Pretrvávajúci prenos alebo výraznú zmenu zdokumentujte pred ďalším zásahom.",
    ],
    "remember": [
        "Je motív tkaný, potlačený alebo zošitý z viacerých dielov?",
        "Deklaruje výrobca stálofarebný alebo zámerne meniaci sa bleeding variant?",
        "Prenáša farbu každá sýta priadza nasucho aj za mokra?",
        "Sú biele polia súčasťou toho istého kusu a v priamom kontakte s tmavou farbou?",
        "Má mokrá košeľa po cykle priestor na okamžité rozloženie?",
        "Je zmena iba odtieňom, alebo ide aj o zrazenie, skrútenie a oslabenie?",
    ],
    "mistakes": [
        "Považovať každú modernú károvanú košeľu za bleeding Madras.",
        "Prvýkrát prať sýty kontrastný kus s bielou bielizňou.",
        "Nechať mokrú košeľu zloženú v bubne alebo koši.",
        "Spoliehať sa na ocot, soľ alebo obrúsok namiesto skúšky a triedenia.",
        "Bieliť vlastné svetlé polia bez ochrany farebných priadzí.",
        "Opakovane prať problémový kus agresívnejšie pred reklamáciou.",
    ],
    "expert_heading": "Odbornejší pohľad: pranie, trenie a vzhľad sú odlišné skúšky",
    "expert": [
        "ISO 105-C06 špecifikuje skúšky odolnosti farby voči domácemu a komerčnému praniu s referenčným prostriedkom. Jedna skúška sleduje zmenu odtieňa aj zafarbenie susednej textílie za definovaných podmienok. Výsledok nemožno preniesť na každú teplotu, každý detergent alebo ľubovoľne dlhé namáčanie.",
        "AATCC vedie samostatné metódy pre pranie, suché a mokré trenie, pot a vzhľad po domácom praní. Košeľa môže mať prijateľnú zmenu farby v jednom cykle, ale stále prenášať pigment pri mokrom trení alebo sa skrútiť v šve. Spotrebiteľská skúška handričkou je orientačná a laboratórne hodnotenie nenahrádza.",
        "Historické záznamy Co-optex opisujú bleeding Madras ako konkrétny ručne tkaný výrobok, ktorého prírodne farbené priadze sa pri praní prelínali do tlmenejších tónov. Moderný názov madras však môže označovať aj farebne stály priemyselný výrobok. Údržba preto musí vychádzať z deklarácie konkrétneho kusu, nie z romantizovanej všeobecnej rady.",
    ],
    "source_intro": "Zdroje podporujú rozdiel medzi historickým bleeding variantom, modernou tkanou košeľovinou a samostatným meraním stálofarebnosti pri praní a trení. Nepodporujú ocot ani soľ ako univerzálnu domácu opravu farbiva.",
    "sources": [
        ("Co-optex: história ručne tkaného bleeding Madras", COOPTEX_MADRAS),
        ("ISO 105-C06: stálofarebnosť pri domácom praní", ISO_COLOR),
        ("AATCC: prehľad skúšobných metód", AATCC_STANDARDS),
        ("ISO 6330: domáce pranie a sušenie pri textilnom skúšaní", ISO_WASH),
        ("GINETEX: symboly ošetrovania", GINETEX),
        ("EÚ 1007/2011: označovanie textilných vlákien", EU_FIBRE_LABEL),
    ],
    "related": [
        ("Ako funguje stálofarebnosť textilu", ARTICLE_COLOR),
        ("Ako čítať štítok na oblečení", ARTICLE_LABEL),
        ("Čo je bavlna", ARTICLE_COTTON),
        ("Prečo sa oblečenie zráža", ARTICLE_SHRINKAGE),
        ("Ako postupovať pri rôznych škvrnách", ARTICLE_STAIN),
        ("Ako vyžehliť košeľu", ARTICLE_IRONING),
    ],
    "faq_title": "madras, káro a púšťanie farby",
    "faq": [
        ("Čo je madras?", "Ľahká tkanina spájaná s farebnými pruhmi a károm, tradične bavlnená a tkaná z farbených priadzí."),
        ("Je každé káro madras?", "Nie. Gingham, tartan, potlač a patchwork môžu vyzerať podobne, ale majú inú konštrukciu a históriu."),
        ("Čo je bleeding Madras?", "Historický variant, pri ktorom sa časť farbiva pri praní zámerne uvoľňovala a vzhľad sa postupne tlmil."),
        ("Má každý madras púšťať farbu?", "Nie. Moderný výrobok môže a často má byť farebne stály podľa svojej deklarácie."),
        ("Ako prvýkrát prať madrasovú košeľu?", "Po skúške farby ju perte oddelene pri najnižšej povolenej teplote a po cykle ju ihneď rozložte."),
        ("Pomôže ocot zafixovať farbu?", "Nie univerzálne. Zmena pH nenahradí správne farbivo a priemyselné ustálenie pre konkrétne vlákno."),
        ("Pomôže soľ?", "Nie ako všeobecná oprava hotového odevu. Používa sa iba v určitých farbiarskych procesoch za riadených podmienok."),
        ("Môžem madras prať s bielou bielizňou?", "Nový alebo rizikový kontrastný kus nie. Najprv potvrďte suchú aj mokrú stálofarebnosť."),
        ("Čo robiť, keď sa biele pole zafarbilo?", "Nepoužívajte naslepo chlór ani vysoké teplo. Zdokumentujte stav a zvoľte kompatibilný postup alebo reklamáciu."),
        ("Prečo sa košeľa skrútila?", "Môže ísť o uvoľnenie výrobného napätia, skosenie väzby alebo rozdielne zrazenie dielov."),
        ("Ako madras sušiť?", "Ihneď rozložený, s urovnanými švami, mimo priameho slnka a svetlých povrchov, kým je mokrý."),
        ("Ako ho žehliť?", "Z rubu pri teplote najcitlivejšej zložky, až po odstránení škvŕn a overení farby."),
        ("Kedy reklamovať púšťanie farby?", "Keď moderný výrobok napriek dodržanému návodu silno farbí pokožku, vlastné svetlé polia alebo okolie a vlastnosť nebola deklarovaná."),
    ],
}

add_cards(
    MADRAS,
    product_heading="Prací gél pre farebne stály prateľný madras",
    product_intro="Ak štítok povoľuje domáce pranie a skúška nepotvrdila nebezpečný prenos, možno na bavlnenú alebo kompatibilnú zmesovú košeľu použiť presne odmeraný tekutý gél.",
    product_text="Prací gél Vevo dávkujte podľa tvrdosti vody, náplne a znečistenia. Koncentrát nelejte na suché káro a nový kontrastný kus perte oddelene.",
    product_limit="Prací gél nezafixuje fugítne farbivo, neobnoví zosvetlený pruh a neochráni vlastné biele polia pri silnom prenose. Problém najprv diagnostikujte.",
    category_heading="Pracie gély porovnávajte spolu s dávkovaním a oplachom",
    category_intro="Pri madrasovej košeli rozhoduje farba, vlákno a prvý samostatný cyklus. Vyššia dávka neznamená väčšiu ochranu odtieňov.",
    category_text="V kategórii nájdete gély pre prateľnú bielizeň. Vyberte kompatibilný produkt, dodržte etiketu a ponechajte v bubne priestor na dôkladný oplach.",
)


SHANTUNG: dict[str, object] = {
    "title": "Čo je šantán: nepravidelný povrch, hodvábne a zmesové varianty",
    "link": "co-je-santan-nepravidelny-povrch-hodvabne-a-zmesove-varianty",
    "meta": "Čo je šantán, prečo má nopky, ako sa líši od taftu a saténu a ako čistiť, sušiť a žehliť hodvábny či syntetický šantán bez máp a lesku.",
    "short": "Šantán je plátnovo tkaná látka s nepravidelnejšími, hrubšími miestami v útkových priadzach, ktoré vytvárajú zámerný nopkovaný povrch. Pôvodne sa spájal s hodvábom, no dnes môže byť aj syntetický alebo zmesový.",
    "name": "šantán",
    "locative": "šantáne",
    "identity_heading": "Nopok šantánu je súčasť priadze, nie automaticky chyba",
    "identity_detail": "Šantán má typicky plátnovú väzbu a v útku nepravidelné hrubšie úseky alebo nopky, ktoré vytvárajú priečne hrbolčeky, meniaci sa odraz a charakteristický menej uniformný povrch.",
    "identity_boundary": "Tradičný hodvábny variant sa správa inak než polyesterová imitácia, viskózová zmes alebo štruktúrované šaty s podšívkou, výstužou a ozdobami.",
    "label_focus": "hodváb, polyester, viskózu, acetát alebo zmes, podšívku, výstuž, lepidlo, kostice, ozdoby, farbivo, profesionálne čistenie, povolenú paru a žehlenie",
    "missing_label": "Lesk, šušťanie a nepravidelné nite nepostačujú na určenie vlákna; pri šatách bez etikety neodhadujte prateľnosť podľa syntetického omaku ani podľa rady pre samotnú metráž.",
    "dry_check": "pravidelné nopky, jednu vyčnievajúcu slučku, prerušenú niť, rozostúpenie pri šve, vodné kruhy, pot, mastný lesk, odtlačenú švovú rezervu, bubliny výstuže a poškodenie ozdôb",
    "damage_boundary": "Zámerný nopok patrí do štruktúry, kým voľná slučka, pretrhnutá nosná priadza, odreté miesto alebo rozídený šev sú poškodenie; ani jedno sa nemá vyhladiť tvrdým drhnutím.",
    "test_focus": "Skúšku robte na hladšom aj nopkovanom úseku a sledujte, či sa mení odraz, farba, tuhosť alebo poloha hrubšej útkovej priadze.",
    "combined_risk": "migrácie farby a apretúry, zachytenia vystupujúcich nopkov, rozdielneho zrazenia vrstiev, trvalého tlakového lesku a deformácie štruktúrovaného odevu",
    "chemistry_boundary": "Voda, pot, make-up, olej a alkoholový produkt reagujú s hodvábom, farbivom, acetátom a povrchom odlišne; univerzálny odmasťovač môže vytvoriť mapu alebo matnú plochu.",
    "drying_detail": "Navlhčený prateľný kus podoprite, otvorte podšívku a záhyby a nenechajte ťažkú sukňu visieť za ramienka; hladké a nopkované plochy nesmú schnúť stlačené o seba.",
    "heat_boundary": "Vysoký tlak môže sploštiť nopky a vytvoriť lesklý obdĺžnik, kým teplo môže poškodiť hodváb, acetát, syntetické vlákno, lepidlo, kostice aj ozdobu.",
    "stop_signs": "silný prenos farby, rastúci vodný kruh, mäknutie výstuže, oddeľovanie podšívky, vytiahnutá nosná niť, zmena nopkov na lesklú plochu alebo deformácia šva",
    "professional_boundary": "Jednoduchý výslovne prateľný syntetický kus možno ošetrovať doma, no hodvábny šantán, spoločenské šaty, sako, záves s podšívkou alebo historický textil patria k čistiarni či konzervátorovi.",
    "answer": "Šantán je plátnovo tkaná látka s nepravidelnými hrubšími miestami v útkových priadzach, ktoré vytvárajú zámerné nopky a priečny štruktúrovaný povrch. Pôvodne sa spájal s hodvábom, dnes môže byť polyesterový, viskózový alebo zmesový. Nopok preto nevyrovnávajte ako chybu a prateľnosť neurčujte podľa názvu. Prečítajte zloženie a štítok celého výrobku, odlíšte pravidelnú štruktúru od vytiahnutej či pretrhnutej nite a vodu skúšajte iba na skrytom mieste. Jednoduchý prateľný kus čistite s nízkou mechanikou, sušte podopretý a žehlite z rubu cez ochrannú tkaninu. Hodvábne alebo štruktúrované šaty zverte čistiarni.",
    "intro": "Nepravidelnosť šantánu zvádza k dvom opačným chybám. Niekto považuje každý nopok za výrobnú vadu a snaží sa ho odstrihnúť alebo vyžehliť; iný zas označí za prirodzenú štruktúru aj skutočne vytiahnutú nosnú niť. Starostlivosť musí najprv rozlíšiť opakujúci sa hrubší útok od lokálnej slučky, prerušenia a rozostúpeného šva. Druhým krokom je vláknové zloženie a tretím celá konštrukcia šiat, saka alebo závesu. Až potom možno bezpečne rozhodnúť o vode, produkte, sušení a teple.",
    "quick": [
        "<strong>Šantán má zámerné nopky:</strong> hrubšie miesta v útkovej priadzi vytvárajú nepravidelný priečny povrch.",
        "<strong>Nie je vždy hodvábny:</strong> moderná látka môže byť polyesterová, viskózová, acetátová alebo zmesová.",
        "<strong>Nie je to taft:</strong> šantán zdôrazňuje nepravidelnú priadzu, taft skôr tuhý uniformnejší plátnový povrch a jemné priečne rebro.",
        "<strong>Nopok neodstrihujte:</strong> môže byť súčasťou priadze; zásah vytvorí slabé miesto alebo dierku.",
        "<strong>Voda môže zanechať mapu:</strong> najmä pri hodvábe, farbive a povrchovej úprave skúšajte úplné vyschnutie.",
        "<strong>Žehlite s malým tlakom:</strong> silná platňa sploští štruktúru a vytvorí lesklý odtlačok.",
    ],
    "overview_heading": "Ako vzniká nopkovaný povrch šantánu",
    "overview": [
        "V plátnovej väzbe sa osnovné a útkové nite pravidelne striedajú. Pri šantáne obsahuje útok dlhšie hrubšie úseky, uzlíky alebo nepravidelnosti, ktoré vystupujú nad hladší základ a vytvárajú priečne nopky. Svetlo sa od nich odráža rozdielne, preto plocha mení tón a reliéf pri pohľade z rôznych uhlov.",
        "LACMA označuje historickú blúzku zo šantánu ako hodvábnu plátnovú tkaninu. Archív Montana State University pri skúšanom rayonovom šantáne zasa uvádza nepravidelne rozmiestnené nopkové útkové priadze a zmes acetátového a regenerovaného celulózového rayonu. Dva odlišné príklady potvrdzujú, že názov opisuje charakter tkaniny, ale sám neurčuje vláknovú chémiu ani bezpečný program.",
        "Prirodzený alebo zámerne vytvorený nopok je opakujúcou sa súčasťou priadze a býva previazaný v štruktúre. Zatrhnutie vzniká neskôr mechanickým zachytením a môže vytvoriť voľnú slučku, napätú líniu či chýbajúcu niť v susednej oblasti. Rozlíšenie lupou je dôležitejšie než pokus hrbolček vytiahnuť alebo odstrihnúť.",
    ],
    "table1_heading": "Šantán, taft, dupion, satén a syntetická imitácia",
    "table1_intro": "Názvy sa v predaji prekrývajú. Porovnanie pomáha opísať povrch, ale presnú starostlivosť určí až zloženie a štítok hotového výrobku.",
    "table1_headers": ["Označenie", "Typická konštrukcia", "Povrch", "Hlavné riziko"],
    "table1_rows": [
        ("Šantán", "Plátnová väzba s nepravidelnými hrubšími útkovými miestami.", "Nopkovaný, priečne štruktúrovaný a menej uniformný.", "Zámena nopku s poškodením, mapy a sploštenie."),
        ("Taft", "Hustá plátnová tkanina, často jemná osnova a plnší útok.", "Tuhší, šušťavý, lesklý a uniformnejšie rebrovaný.", "Ostré lomy, vodné kruhy a odtlačenie švov."),
        ("Dupion", "Hodvábna plátnová tkanina s výraznými nepravidelnosťami priadze.", "Často plnší a hrubšie nopkovaný vzhľad.", "Variabilná kvalita, farbivo a profesionálna údržba."),
        ("Satén", "Saténová väzba s dlhšími väznými úsekmi.", "Súvislo hladký lesk bez typického priečneho nopku.", "Zatrhnutie, tlakový lesk a citlivosť vlákna."),
        ("Syntetická imitácia", "Polyesterová alebo iná tkanina napodobňujúca nopky.", "Stabilnejší alebo pravidelnejší reliéf.", "Teplo, statika, potlač, povlak a konštrukcia odevu."),
    ],
    "sections": [
        {
            "heading": "Ako odlíšiť zámerný nopok od zatrhnutia a prasknutej nite",
            "paragraphs": [
                f"Položte látku na rovnú podložku a prezrite lupou z líca aj rubu. Prirodzené nopky sa opakujú v priečnom smere a hrubšia priadza pokračuje vo väzbe. <a href=\"{ARTICLE_SNAGGING}\">Zatrhnutie textilu</a> často vytvorí jednu voľnú slučku, napätú líniu, posunutie okolitej väzby alebo chýbajúci úsek na inom mieste. Nič neťahajte pinzetou.",
                "Pretrhnutá priadza má rozstrapkané konce a pri jemnom uvoľnení napätia sa otvor môže zväčšiť. Nopok neodstrihujte, pretože môže spájať pokračovanie útku. Pri novom odeve si porovnajte symetrické a menej namáhané plochy. Ak je chyba jediná, napína susedné nite alebo vznikla po zachytení, ide skôr o poškodenie než o bežný charakter povrchu.",
            ],
        },
        {
            "heading": "Hodvábny, polyesterový, viskózový a acetátový šantán",
            "paragraphs": [
                "Hodváb je proteínové vlákno citlivé na silnú zásadu, nevhodné enzýmy, dlhé svetlo a mechanické trenie. Polyester môže lepšie držať rozmer, ale vysoké teplo a tlak zmenia lesk alebo zatavia povrch. Acetát má osobitnú citlivosť na niektoré rozpúšťadlá a teplo. Viskóza môže za mokra stratiť časť pevnosti a meniť rozmer.",
                f"Pri viskózovom variante pomôže samostatný článok <a href=\"{ARTICLE_VISCOSE}\">čo je viskóza a ako sa správa za mokra</a>. Zmes nepreberá automaticky najlepšie vlastnosti každej zložky. Bezpečnú chémiu a teplotu určuje najcitlivejšie vlákno spolu s farbivom, apretúrou, niťou, podšívkou a výstužou.",
            ],
        },
        {
            "heading": "Šantán verzus taft a satén v domácej diagnostike",
            "paragraphs": [
                f"Šantán aj <a href=\"{ARTICLE_TAFFETA}\">taft</a> môžu byť plátnovo tkané a tuhšie, ale šantán zdôrazňuje nepravidelnú útkovú priadzu a rozptýlené nopky. Taft má zvyčajne uniformnejší lesk, suchšie šušťanie a jemné priečne rebro. <a href=\"{ARTICLE_SATIN}\">Satén</a> je väzba s dlhšími hladkými úsekmi, ktoré vytvárajú súvislejší odraz.",
                "Omak nemožno použiť na určenie vlákna. Polyester môže napodobniť hodváb a apretúra môže zmeniť mäkkosť. Rozlíšenie názvu pomáha pochopiť riziko povrchu, no práčku povoľuje iba štítok. Pri štruktúrovaných šatách navyše rozhoduje živôtik, podšívka, kostice a ozdoby, nie iba vrchný diel.",
            ],
        },
        {
            "heading": "Môže sa šantán prať ručne alebo v práčke",
            "paragraphs": [
                f"Najprv použite <a href=\"{ARTICLE_LABEL}\">všetky symboly na štítku</a>. Jednoduchý nezdobený syntetický kus môže mať povolené jemné domáce pranie, kým hodvábny alebo štruktúrovaný výrobok často vyžaduje profesionálnu starostlivosť. Ručné pranie nie je automaticky bezpečné: dlhé namáčanie, stláčanie a krútenie vytvoria mapy a ostré lomy.",
                "Pri povolenom praní zabezpečte priestor, rovnomerne rozptýlený kompatibilný prostriedok a nízke trenie. Odev neperte so zipsami, háčikmi a drsnými uterákmi, ktoré zachytia nopky. Mokrú sukňu podopierajte v celej ploche a nezdvíhajte ju za tenké ramienka. Ak sa farba alebo tuhosť mení už pri skúške, nepokračujte celým cyklom.",
            ],
        },
        {
            "heading": "Vodná mapa a nerovnomerný lesk",
            "paragraphs": [
                "Kvapka môže rozpustiť jemnú nečistotu, uvoľniť farbivo alebo zmeniť apretúru. Pri schnutí sa materiál presunie k okraju a vytvorí kruh. Nopkovaný povrch zároveň schne nerovnomerne a mení odraz, takže mokrá plocha vyzerá horšie, než bude po úplnom vysušení. Výsledok preto nehodnoťte okamžite.",
                "Čerstvú tekutinu odsajte bielym savým materiálom z oboch strán bez trenia. Ďalšie malé kvapky nepridávajte s cieľom zjednotiť okraj. Na skrytom leme skúšajte rovnakú vodu, produkt aj odsatie a miesto nechajte vyschnúť. Pri hodvábe, silnom prenose farby alebo spoločenských šatách zvoľte čistiareň skôr, než sa mapa rozšíri.",
            ],
        },
        {
            "heading": "Pot, deodorant, make-up a nápoj na šantáne",
            "paragraphs": [
                f"Pot kombinuje vodu, soli a organické zložky, deodorant môže pridať voskovitý či minerálny film a make-up pigment s mastnotou. <a href=\"{ARTICLE_STAIN}\">Sprievodca škvrnami</a> pomáha zvoliť prvý krok, no pri hodvábnom alebo acetátovom šantáne nepoužívajte náhodný alkohol, univerzálny odmasťovač ani parfum na prekrytie stopy.",
                "Podpazušie a golier kontrolujte z rubu, pričom vrchnú látku a podšívku oddeľte iba v rozsahu, ktorý konštrukcia dovoľuje. Miesto netrite naprieč nopkami a pred žehlením skontrolujte aj mastný zvyšok. Starý pot môže meniť farbu a oslabiť hodváb; včasné odborné čistenie je bezpečnejšie než opakované lokálne pokusy.",
            ],
        },
        {
            "heading": "Ako sušiť šantán bez vyťahania a pritlačenia nopkov",
            "paragraphs": [
                f"Povolený mokrý kus prenášajte s oporou a otvorte záhyby, vrecká aj podšívku. V interiéri zabezpečte prúdenie vzduchu podľa návodu <a href=\"{ARTICLE_DRYING}\">ako sušiť bielizeň bez zatuchnutia</a>. Odev neklaďte nopkovaným lícom na drsný uterák a neskladajte mokré diely na seba, pretože tlak zmení reliéf a spomalí schnutie.",
                "Úzke ramienko môže vytlačiť ramená a hmotnosť sukne vyťahať pás. Sušičku, radiátor a fén použite iba pri výslovnom povolení, ktoré je pri citlivých šatách nepravdepodobné. Odev nechajte schnúť pri stabilnej teplote mimo priameho slnka a tvar upravujte jemne, bez ťahania za jednu hrubšiu niť.",
            ],
        },
        {
            "heading": "Ako žehliť a naparovať bez sploštenia povrchu",
            "paragraphs": [
                "Žehlenie začnite z rubu pri najnižšej účinnej teplote pre najcitlivejšiu zložku. Použite čistú ochrannú tkaninu a mäkšiu podložku, ktorá dovolí nopkom zapadnúť bez tvrdého sploštenia. Švové rezervy podložte samostatne, aby sa ich obrys nepretlačil na líc. Žehličku nenechávajte stáť na jednom mieste.",
                "Para môže uvoľniť záhyb, ale tiež vytvoriť kvapky, vodnú mapu alebo zmenu výstuže. Naparovač skúšajte z odstupu na skrytej zóne a povrch nechajte vychladnúť. Ak nopky mäknú, lepia sa, lesknú alebo sa látka zvlňuje, ďalšie teplo zastavte. Ozdoby, lepidlo, acetát a syntetické nite môžu mať nižší limit než vrchná plocha.",
            ],
        },
        {
            "heading": "Štruktúrované šaty, sako a záves s podšívkou",
            "paragraphs": [
                f"Hotové spoločenské šaty obsahujú viac než šantán. <a href=\"{ARTICLE_FORMAL}\">Návod na spoločenské šaty</a> vysvetľuje vplyv kostíc, výstuže, sieťoviny, ozdôb a podšívky. Aj prateľná polyesterová metráž môže v hotovom živôtiku stratiť tvar, keď sa podlep navlhčí alebo sa vrstvy zrazia rozdielne.",
                "Záves môže mať podšívku, závažie, kovový háčik a veľkú mokrú hmotnosť. Nevkladajte ho do práčky podľa rady pre blúzku. Prach najprv odstráňte primerane konštrukcii a škvrnu skúšajte na leme. Pri veľkom hodvábnom alebo podšitom kuse je profesionálne čistenie praktickejšie než domáce sušenie bez dostatočnej opory.",
            ],
        },
        {
            "heading": "Ako šantán skladovať a chrániť pred zatrhnutím",
            "paragraphs": [
                "Odev uložte čistý a úplne suchý. Nopkovaný povrch oddeľte od flitrov, zipsov, suchých zipsov a ostrých hrán. Ťažké šaty vešajte iba za vnútorné pútka a na širokom tvarovanom vešiaku, ak konštrukcia znesie hmotnosť. Inak ich podoprite plocho s mäkkými širokými prehybmi.",
                "Nevytvárajte stále ten istý ostrý sklad a na líc neklaďte ťažké predmety. Hodváb chráňte pred dlhým svetlom, prachom a stopami potu, ktoré časom oxidujú. Pri objavení slučky ju nezatlačujte lepidlom a neodstrihujte. Odev prestaňte nosiť v namáhanej oblasti a zvoľte krajčírsku opravu, ktorá rozloží napätie.",
            ],
        },
    ],
    "table2_heading": "Nopok, slučka, mapa alebo lesk: ako čítať povrch",
    "table2_intro": "Povrch posudzujte z líca aj rubu, pri bočnom svetle a bez ťahania nite. Rovnaký hrbolček môže mať odlišný význam podľa väzby a okolia.",
    "table2_headers": ["Prejav", "Pravdepodobný význam", "Čo overiť", "Bezpečný ďalší krok"],
    "table2_rows": [
        ("Opakujúce sa hrubšie miesta", "Zámerné nopky útkovej priadze.", "Pokračovanie nite na rube a podobné miesta v okolí.", "Neodstrihovať ani nevyhladzovať silným tlakom."),
        ("Jedna voľná slučka", "Zatrhnutie alebo posun priadze.", "Napätú líniu a chýbajúci úsek inde.", "Neťahať; rozložiť napätie alebo zvoliť opravu."),
        ("Kruhová mapa", "Migrácia nečistoty, farby alebo apretúry pri schnutí.", "Prenos farby a reakciu skrytého lemu.", "Nepridávať ďalšie kvapky; citlivý kus odborne vyčistiť."),
        ("Lesklý obdĺžnik", "Tlak a teplo žehličky sploštili povrch.", "Rub, švy a zmenu po vychladnutí.", "Ďalšie teplo zastaviť; mechanický lesk nedrhnúť."),
        ("Bublina na živôtiku", "Uvoľnenie výstuže alebo rozdielne zrazenie vrstiev.", "Podšívku, lepidlo a tvar šva.", "Nenaparovať naslepo; zvoliť krajčírske posúdenie."),
    ],
    "steps_heading": "Bezpečný postup pri čistení šantánu",
    "steps": [
        "Určte vlákno, plátnovú konštrukciu a všetky vrstvy hotového výrobku.",
        "Odfoťte nopky, slučky, mapy, švy, podšívku a tvar pred zásahom.",
        "Odlíšte opakujúcu sa štruktúru od vytiahnutej alebo pretrhnutej nite.",
        "Na skrytom mieste otestujte farbu, vodu, produkt, tlak a úplné vyschnutie.",
        "Celý kus perte iba pri výslovnom symbole a s minimálnym mechanickým stresom.",
        "Mokrý odev prenášajte s oporou, bez krútenia a ťahania za ramienka.",
        "Sušte s otvorenými vrstvami, bez drsného podkladu a prudkého tepla.",
        "Žehlite z rubu cez ochrannú tkaninu pri najnižšej účinnej teplote.",
        "Nopky, ozdoby a švy chráňte pred zipsami, suchým zipsom a tlakom.",
        "Pri hodvábe, mape alebo štruktúrovanom odeve zvoľte odbornú starostlivosť.",
    ],
    "remember": [
        "Je hrubšie miesto opakujúci sa nopok alebo jedna napätá slučka?",
        "Je šantán hodvábny, polyesterový, viskózový, acetátový alebo zmesový?",
        "Čo obsahuje podšívka, výstuž, kostice, lepidlo a ozdoba?",
        "Prenáša skrytá zóna farbu alebo mení lesk po úplnom vyschnutí?",
        "Má mokrý odev oporu, aby ho nevyťahala vlastná hmotnosť?",
        "Je domáce pranie povolené pre celý výrobok, nie iba pre metráž?",
    ],
    "mistakes": [
        "Odstrihnúť prirodzený nopok a prerušiť útkovú priadzu.",
        "Považovať každý šantán za hodváb alebo každý syntetický variant za prateľný.",
        "Rozotierať vodnú mapu ďalšími kvapkami bez skúšky.",
        "Krútiť ručne prané šaty a dvíhať ich mokré za ramienka.",
        "Žehliť z líca vysokým tlakom a sploštiť nopkovaný povrch.",
        "Ignorovať kostice, podšívku, lepidlo a ozdoby hotového odevu.",
    ],
    "expert_heading": "Odbornejší pohľad: názov tkaniny nestačí na predikciu údržby",
    "expert": [
        "Záznam LACMA dokladá hodvábny šantán v plátnovej väzbe. Historický skúšobný materiál z archívu Montana State University opisuje rayonový šantán cez nepravidelne rozmiestnené nopkové útkové priadze a uvádza aj jeho vláknové zloženie. Rovnaký názov teda spája rozpoznateľnú konštrukciu a povrch, nie jednu vláknovú rodinu.",
        "ISO 6330 definuje viacero postupov domáceho prania a šesť spôsobov sušenia pre textilné skúšanie. Rozdielna kombinácia stroja, prostriedku, mechaniky a sušenia môže zmeniť výsledok. Jedna rada jemný cyklus preto nie je technický dôkaz pre hodvábne šaty ani pre celý podšitý výrobok.",
        "ISO 15487 hodnotí po praní a sušení nielen rozmer, ale aj farbu, žmolkovanie, matovanie, hladkosť plochy a švov či poškodenie komponentov. Pri šantáne je dôležitý aj lokálny tlakový lesk a zachovanie nopkov. Plochá vzorka metráže nevystihne živôtik s kosticami a výstužou.",
    ],
    "source_intro": "Zdroje podporujú plátnovú nopkovanú identitu šantánu, existenciu hodvábnych aj syntetických variantov a oddelené hodnotenie vzhľadu po praní. Nepodporujú jednu domácu metódu pre všetky šantánové výrobky.",
    "sources": [
        ("LACMA: hodvábny šantán v plátnovej väzbe", LACMA_SHANTUNG),
        ("Montana State University: konštrukcia a skúšanie rayonového šantánu", MONTANA_SHANTUNG),
        ("LACMA: hodvábny taft na porovnanie", LACMA_TAFFETA),
        ("ISO 6330: domáce pranie a sušenie pri textilnom skúšaní", ISO_WASH),
        ("ISO 15487: vzhľad po domácom praní a sušení", ISO_APPEARANCE),
        ("GINETEX: symboly ošetrovania", GINETEX),
        ("EÚ 1007/2011: označovanie textilných vlákien", EU_FIBRE_LABEL),
    ],
    "related": [
        ("Čo je taft", ARTICLE_TAFFETA),
        ("Čo je satén", ARTICLE_SATIN),
        ("Ako prať spoločenské šaty", ARTICLE_FORMAL),
        ("Čo je viskóza", ARTICLE_VISCOSE),
        ("Ako riešiť zatrhnutie", ARTICLE_SNAGGING),
        ("Ako postupovať pri rôznych škvrnách", ARTICLE_STAIN),
    ],
    "faq_title": "šantán, nopky a jemné odevy",
    "faq": [
        ("Čo je šantán?", "Plátnovo tkaná látka s nepravidelnými hrubšími útkovými miestami, ktoré vytvárajú nopkovaný povrch."),
        ("Je šantán vždy hodváb?", "Nie. Moderný šantán môže byť polyesterový, viskózový, acetátový alebo zmesový."),
        ("Je nopok výrobná chyba?", "Zvyčajne nie, ak sa podobné hrubšie miesta opakujú a priadza pokračuje vo väzbe."),
        ("Ako spoznám zatrhnutie?", "Jedna voľná slučka, napätá línia, chýbajúci úsek alebo rozstrapkaný koniec skôr signalizujú poškodenie."),
        ("Je šantán to isté ako taft?", "Nie. Taft má typicky uniformnejší tuhý plátnový povrch, šantán zámerne nepravidelnú útkovú priadzu."),
        ("Môže sa šantán prať v práčke?", "Iba pri výslovnom symbole pre celý výrobok vrátane podšívky, výstuže a ozdôb."),
        ("Môže sa prať ručne?", "Len pri povolení. Dlhé namáčanie, stláčanie a krútenie môžu byť škodlivejšie než riadený jemný cyklus."),
        ("Ako odstrániť vodnú mapu?", "Ďalšie kvapky nepridávajte. Urobte skrytú skúšku a pri hodvábe alebo šatách zvoľte čistiareň."),
        ("Ako šantán sušiť?", "S rovnomernou oporou, otvorenými vrstvami, bez drsného podkladu, radiátora a fénu."),
        ("Ako ho žehliť?", "Z rubu cez ochrannú tkaninu pri najnižšej účinnej povolenej teplote a s malým tlakom."),
        ("Môže sa naparovať?", "Len po skrytej skúške a pri povolení; kvapky pary môžu vytvoriť mapu a teplo zmeniť výstuž."),
        ("Dajú sa nopky vyhladiť?", "Nemajú sa odstraňovať. Silné vyhladenie mení charakter povrchu a môže poškodiť priadzu."),
        ("Kedy zvoliť čistiareň?", "Pri hodvábe, spoločenských šatách, podšitom saku, silnom prenose farby, mapách alebo poškodenej niti."),
    ],
}

add_cards(
    SHANTUNG,
    product_heading="Prací gél iba pre výslovne prateľný variant",
    product_intro="Jednoduchý nezdobený syntetický alebo iný kompatibilný šantán možno pri povolenom domácom praní ošetriť presne odmeraným tekutým prostriedkom.",
    product_text="Prací gél Vevo najprv otestujte na skrytom mieste a koncentrát nelejte priamo na nopok, vodnú mapu, hodvábny povrch ani škvrnu.",
    product_limit="Produkt nie je univerzálny prostriedok na hodváb, acetát, spoločenské šaty, kostice, lepenú výstuž alebo profesionálne čistenie.",
    category_heading="Pracie gély až po kontrole vlákna a konštrukcie",
    category_intro="Pri šantáne má prednosť štítok celého výrobku. Názov látky ani syntetický omak samy osebe nepovoľujú práčku.",
    category_text="V kategórii nájdete gély pre kompatibilnú prateľnú bielizeň. Pri hodvábnom, zdobenom alebo štruktúrovanom kuse zvoľte starostlivosť určenú výrobcom.",
)


CHALLIS: dict[str, object] = {
    "title": "Čo je challis: mäkká splývavá tkanina, zrážanie a pranie",
    "link": "co-je-challis-makka-splyvava-tkanina-zrazanie-a-pranie",
    "meta": "Čo je challis, ako sa líši od viskózy, krepu a batistu a ako prať, sušiť a žehliť mäkké šaty či blúzku bez zrazenia, vyťahania a posunu švov.",
    "short": "Challis je mäkká ľahká plátnovo tkaná látka so splývavým omakom. Môže byť vlnená, bavlnená, viskózová alebo z iného staplového vlákna, preto názov sám neurčuje pranie.",
    "name": "challis",
    "locative": "challise",
    "identity_heading": "Challis je charakter ľahkej tkaniny, nie synonymum viskózy",
    "identity_detail": "Challis má typicky jednoduchú plátnovú väzbu, jemnejšie staplové priadze, nízku až strednú hmotnosť a mäkké dokončenie, ktoré podporuje plynulé splývanie namiesto tuhosti.",
    "identity_boundary": "Historický vlnený challis, bavlnená šatovka, viskózová blúzka a moderná zmes môžu vyzerať podobne, no za mokra majú inú pevnosť, zrážanie, pružnú návratnosť a tepelný limit.",
    "label_focus": "presné percentá vlákien, potlač, podšívku, elastan, výstuž, šikmo strihané diely, veľmi jemné švy, ozdoby, povolený program, sušenie naplocho a žehlenie",
    "missing_label": "Pri metráži žiadajte technický list a pred ušitím testujte odstrižok; pri hotových šatách bez etikety nepovažujte mäkký studený omak automaticky za viskózu ani za povolenie ručného prania.",
    "dry_check": "skrútené bočné švy, zvlnený lem, rozostúpenie pri šve, vyťahané ramená, žmolky, lesk, prenos tlače, mapy, vytiahnutú priadzu a rozdielnu dĺžku šikmo strihaných dielov",
    "damage_boundary": "Prach a škvrnu možno čistiť, no otvorený šev, trvalo predĺžený šikmý diel, zodratý povrch alebo zrazená podšívka nie sú zvyšky prostriedku, ktoré odstráni ďalší cyklus.",
    "test_focus": "Odstrižok alebo skrytý lem odmerajte pred navlhčením a po úplnom vysušení, potom sledujte farbu, krčivosť, povrch a návrat do pôvodného smeru osnovy a útku.",
    "combined_risk": "napučania staplových priadzí, dočasne nižšej mokrej stability niektorých vlákien, posunu v šve, zrazenia a predĺženia mäkkej alebo šikmo strihanej plochy pod vlastnou hmotnosťou",
    "chemistry_boundary": "Vlna, viskóza, bavlna a syntetická zmes nemajú rovnakú toleranciu voči zásade, enzýmom, bielidlu ani rozpúšťadlu; názov challis tieto rozdiely nevymaže.",
    "drying_detail": "Mokrý prateľný odev podoprite, urovnajte švy a lem bez násilného naťahovania a podľa zloženia sušte naplocho alebo na vhodnej opore; šikmý diel nechajte stabilizovať pred konečným meraním.",
    "heat_boundary": "Vysoké teplo môže zraziť vlnu alebo bavlnu, oslabiť či vyleštiť viskózový povrch, poškodiť elastan a vytvoriť zvlnené švy pri rozdielnej reakcii nití a tkaniny.",
    "stop_signs": "rastúce zrazenie, silný prenos farby, otváranie šva, predĺženie ramena, trvalý lesk, stvrdnutie, zvlnenie podšívky alebo praskanie potlače",
    "professional_boundary": "Jednoduchú výslovne prateľnú bavlnenú alebo viskózovú blúzku možno ošetrovať doma, kým vlnený, podšitý, šikmo strihaný, historický alebo profesionálne čistiteľný challis potrebuje individuálny postup.",
    "answer": "Challis je mäkká, ľahká a splývavá plátnovo tkaná látka, nie jedno konkrétne vlákno. Môže byť z vlny, bavlny, viskózy alebo zmesi, takže bezpečný program určuje štítok hotového odevu. Pred praním skontrolujte farbu, jemné švy, podšívku a šikmo strihané diely. Prateľný kus perte s nízkou mechanikou a voľným priestorom, nepoužívajte produkt určený pre iné vlákno a mokrý odev nedvíhajte za ramená. Po cykle ho podoprite, urovnajte bez ťahania a sušte podľa symbolu. Žehlite z rubu pri najnižšej účinnej teplote. Vlnené, podšité alebo iba profesionálne čistiteľné šaty zverte čistiarni.",
    "intro": "Challis sa v obchodoch často stotožní s viskózou, pretože moderné šaty a blúzky z tejto látky bývajú mäkké, chladivé a splývavé. Historicky aj technicky však názov opisuje širší typ ľahkej plátnovej tkaniny, ktorá môže použiť viac druhov staplových priadzí. Dve podobné šaty sa preto môžu pri vode správať opačne. Jedny sa zrazia, druhé predĺžia pod mokrou hmotnosťou, tretie sa zvlňujú v švoch a vlnený variant môže pri pohybe plstnatieť. Bez zloženia nie je jedna bezpečná teplota ani spôsob sušenia.",
    "quick": [
        "<strong>Challis nie je vlákno:</strong> môže byť vlnený, bavlnený, viskózový, syntetický alebo zmesový.",
        "<strong>Typicky je plátnovo tkaný:</strong> mäkké dokončenie a jemné priadze vytvárajú splývanie.",
        "<strong>Mokrý kus podoprite:</strong> jemné švy a šikmo strihané diely sa môžu vyťahať vlastnou hmotnosťou.",
        "<strong>Rozmer merajte až suchý:</strong> dočasné napučanie a krčenie počas schnutia nemusia byť konečný výsledok.",
        "<strong>Vlna a viskóza nie sú jeden program:</strong> najcitlivejšie vlákno, podšívka a potlač určujú hranice.",
        "<strong>Pred šitím testujte odstrižok:</strong> predpranie, stabilizácia šikmého lemu a šev sú súčasťou kvality hotového odevu.",
    ],
    "overview_heading": "Prečo challis splýva a zároveň môže meniť rozmer",
    "overview": [
        "Univerzitné textilné materiály dokumentujú challis pri viacerých vláknových systémoch. Výskum University of the Free State pracuje s ľahkým vlneným challisom v plátnovej väzbe, kým módna príručka University of Trinidad and Tobago uvádza rayonový challis medzi ľahkými odevnými tkaninami. Názov preto nemožno zamieňať so slovom viskóza ani z neho odvodiť jediný prací program.",
        "Splývanie vzniká kombináciou nízkej ohybovej tuhosti, jemnej priadze a mäkkého dokončenia. Tá istá poddajnosť však znamená, že mokrá hmotnosť ľahko ťahá za rameno, šev a šikmo strihaný lem. Pri tkanine strihanej pod uhlom voči osnove a útku sa diel prirodzene viac deformuje a pred konečným zarovnaním potrebuje čas na stabilizáciu.",
        "Vláknový systém určuje, čo sa deje vo vode. Bavlna napučiava a môže sa zraziť, viskóza môže mať za mokra nižšiu pevnosť a vlnené vlákno reaguje na kombináciu vlhkosti, tepla a mechaniky. Syntetická zložka môže pridať rozmerovú stabilitu, no znížiť bezpečnú teplotu žehlenia. Obchodný názov neprebíja tieto rozdiely.",
    ],
    "table1_heading": "Challis, krep, batist, voál a viskózová šatovka",
    "table1_intro": "Porovnanie vysvetľuje povrch a konštrukciu. Konkrétny program vždy vychádza z vlákna, farby, dokončenia a hotového odevu.",
    "table1_headers": ["Označenie", "Typický princíp", "Omak a vzhľad", "Praktické riziko"],
    "table1_rows": [
        ("Challis", "Ľahká plátnová tkanina zo staplových priadzí.", "Mäkký, hladký až jemne zrnitý a splývavý.", "Zrážanie, vyťahanie, posun šva a farba podľa vlákna."),
        ("Krep", "Krepový efekt priadzou, väzbou alebo dokončením.", "Výraznejšie zrnitý a pružne pôsobiaci povrch.", "Sploštenie reliéfu, krčivosť a rozmerová zmena."),
        ("Batist", "Veľmi jemná ľahká plátnová tkanina.", "Hladší, vzdušný a často priesvitnejší.", "Trhanie, švy, zrážanie a vysoké trenie."),
        ("Voál", "Jemná priesvitná plátnová tkanina s pevnejším zákrutom.", "Vzdušný a priehľadný, často tuhší než challis.", "Záhyby, posun priadzí a háčiky pri záclone."),
        ("Viskózová šatovka", "Širšie obchodné označenie podľa vlákna a použitia.", "Môže byť challis, krep, keper aj úplet.", "Mokrá pevnosť, zrážanie a sušenie podľa konkrétnej väzby."),
    ],
    "sections": [
        {
            "heading": "Ako challis rozoznať bez zamieňania s viskózou",
            "paragraphs": [
                "Pozrite sa lupou na pravidelné striedanie osnovy a útku, ľahký charakter a mäkké splývanie. Názov viskóza na etikete hovorí o vlákne, nie o väzbe. Viskózová látka môže byť krep, keper, saténová väzba, úplet alebo challis. Rovnako challis nemusí obsahovať ani percento viskózy.",
                f"Pri neistote použite <a href=\"{ARTICLE_VISCOSE}\">samostatné vysvetlenie viskózy</a>, ale nesnažte sa zloženie potvrdiť pálením nite na hotovom odeve. Omak ovplyvňuje aviváž, apretúra, zákrut a zmes. Jediným spoľahlivým základom spotrebiteľského rozhodnutia je etiketa alebo technický list metráže.",
            ],
        },
        {
            "heading": "Rozdiel medzi challisom, krepom a batistom",
            "paragraphs": [
                f"<a href=\"{ARTICLE_CREPE}\">Krep</a> má zrnitý efekt vytvorený priadzou, väzbou alebo dokončením a často pružnejšie zotavenie povrchu. <a href=\"{ARTICLE_BATISTE}\">Batist</a> je veľmi jemná plátnová tkanina, zvyčajne hladšia a priesvitnejšia. Challis sa identifikuje skôr mäkkým splývaním a staplovou priadzou než extrémnou priesvitnosťou alebo výraznou krepovou zrnitosťou.",
                "Obchodné názvy sa používajú voľne a povrchová úprava môže rozdiel zmenšiť. Porovnávajte rub, hranu odstrižku a správanie pri jemnom ohnutí, nie iba fotografiu. Starostlivosť neurčujte podľa najbližšej podobnosti. Dve plátnové tkaniny môžu mať odlišné vlákna, farbivo a zrážanie, kým rovnaká viskóza sa v krepe a challise správa mechanicky inak.",
            ],
        },
        {
            "heading": "Viskózový challis: mokrá hmotnosť a jemné švy",
            "paragraphs": [
                "Pri viskózovom variante rešpektujte symboly a znížte mechanické namáhanie. Mokrý odev môže byť ťažší a jemná priadza či šev zraniteľnejší, preto ho nevyťahujte z bubna za jedno rameno. Podoprite sukňu aj živôtik, nestláčajte do povrazu a nekrúťte. Dlhé namáčanie nie je automaticky šetrná náhrada cyklu.",
                "Po praní urovnajte švy bez násilného naťahovania. Viskózová tkanina môže počas schnutia pôsobiť tuho alebo pokrčene a po úplnom vyschnutí a povolenom žehlení znovu zmäknúť. Rozmer nehodnoťte mokrý. Ak sa šev otvoril alebo rameno predĺžilo, ďalší cyklus chybu nevráti; odev potrebuje stabilizáciu alebo opravu.",
            ],
        },
        {
            "heading": "Vlnený a bavlnený challis majú odlišné hranice",
            "paragraphs": [
                "Vlnený challis môže byť ľahký a hladký, no stále ide o proteínové vlákno citlivé na kombináciu tepla, vlhkosti a mechaniky. Použite iba postup a prostriedok povolený štítkom, nešúchajte škvrnu a pri sušení zachovajte rozmer. Podšitý vlnený odev alebo kus označený na profesionálne čistenie neperte podľa rady pre viskózovú blúzku.",
                f"Bavlnený challis môže lepšie znášať bežné pranie, ale stále sa môže zraziť, pokrčiť a pustiť farbu. <a href=\"{ARTICLE_COTTON}\">Vlastnosti bavlny</a> dopĺňajú vláknový základ, no jemný šev a splývavá konštrukcia vyžadujú nižšie trenie než hrubý uterák. Zmes s polyesterom nemení výrobok na tepelne neobmedzený; žehlenie určuje najcitlivejšia zložka.",
            ],
        },
        {
            "heading": "Prvý cyklus, meranie a predpranie metráže",
            "paragraphs": [
                f"Hotový odev pred prvým praním odmerajte medzi pevnými bodmi a odfoťte polohu bočných švov a lemu. Mechanizmus rozmerovej zmeny vysvetľuje článok <a href=\"{ARTICLE_SHRINKAGE}\">prečo sa textil zráža</a>. Pri metráži odstrižok označte smerom osnovy, zmerajte štvorcovú plochu a otestujte ju presne tak, ako sa má neskôr ošetrovať hotový výrobok.",
                "Predpranie má význam iba pri metráži a postupe, ktorý bude povolený aj po ušití. Ak plánujete profesionálne čistený vlnený odev, domáce namočenie odstrižku nevytvorí relevantnú prípravu. Po skúške látku úplne vysušte, nechajte stabilizovať a znovu zmerajte. Sledujte aj farbu, krčivosť, povrch, skosenie a pevnosť budúceho šva.",
            ],
        },
        {
            "heading": "Posun priadzí, otváranie šva a šikmý strih",
            "paragraphs": [
                "Mäkká tkanina sa môže pri tesnom stehu alebo napätí odsunúť od šva a vytvoriť svetlé medzery bez pretrhnutia priadze. Šikmo strihaný diel má zasa väčšiu poddajnosť mimo smeru osnovy a útku. Pri nosení a mokrom sušení sa môže predĺžiť, preto sa lem pri šití necháva stabilizovať pred konečným zarovnaním.",
                "Otvorený šev nepretierajte lepidlom a neperte znovu s nádejou, že priadza napučí späť. Znížte zaťaženie, odfoťte rub a zvoľte krajčírsku opravu, ktorá rozloží napätie. Pri novom odeve môže výrazný posun znamenať nevhodnú hustotu, steh alebo švovú rezervu. Reklamáciu riešte pred domácim prešitím.",
            ],
        },
        {
            "heading": "Potlač, púšťanie farby a lokálne škvrny",
            "paragraphs": [
                f"Jemné kvetinové a geometrické tlače môžu mať rozdielnu stálosť v jednotlivých pigmentoch. Skrytú skúšku robte na každej sýtej farbe a postup porovnajte s článkom o <a href=\"{ARTICLE_COLOR}\">stálofarebnosti textilu</a>. Koncentrát nelejte na suchú tlač a škvrnu netrite veľkým kruhom, pretože mäkký povrch sa môže lokálne vyhladiť.",
                f"Pôvod škvrny určte podľa <a href=\"{ARTICLE_STAIN}\">sprievodcu rôznymi nečistotami</a>. Mastnotu najprv odsajte, bielkovinu nezačínajte vysokou teplotou a neznámy farebný fľak nebielte naslepo. Pri vlnenom alebo viskózovom variante overte kompatibilitu produktu s vláknom. Výsledok posudzujte úplne suchý a z viacerých uhlov.",
            ],
        },
        {
            "heading": "Ako challis prať bez zbytočného trenia",
            "paragraphs": [
                f"Najprv prečítajte celý štítok podľa <a href=\"{ARTICLE_LABEL}\">návodu na symboly</a>. Zapnite bezpečné uzávery, uvoľnené ozdoby opravte a kus oddeľte od zipsov, háčikov a hrubých uterákov. Zvoľte program, teplotu a prostriedok pre konkrétne vlákno. Bubon nechajte dostatočne voľný, aby sa odev rozvinul a dôkladne opláchol.",
                "Ručné pranie robte iba pri povolení a s dostatkom vody. Odev nechajte voľne plávať, nestláčajte ho proti drsnému umývadlu a nekrúťte. Po oplachu vodu vytlačte podopretím a jemným tlakom podľa návodu, nie skrúcaním rukávov. Mokré šaty prenášajte na uteráku alebo oboma rukami pod celou plochou.",
            ],
        },
        {
            "heading": "Ako challis sušiť bez vyťahaných ramien a zvlneného lemu",
            "paragraphs": [
                f"Všeobecné prúdenie vzduchu vysvetľuje návod <a href=\"{ARTICLE_DRYING}\">ako sušiť bielizeň v interiéri</a>. Pri challise navyše rozložte mokrú hmotnosť. Ľahký stabilný kus možno podľa etikety zavesiť na vhodný vešiak, ale viskózové, vlnené, šikmo strihané alebo ťažšie šaty často potrebujú sušenie naplocho či s výraznou oporou.",
                "Lem neskracujte ani nehodnoťte, kým odev úplne nevyschne a nestabilizuje sa. Šikmý diel môže pod gravitáciou klesnúť a po odpočinku sa čiastočne ustáliť. Sušičku používajte iba pri výslovnom symbole. Radiátor a fén vytvárajú nerovnomerné teplo, ktoré zvýrazní zrazenie, mapu a zvlnenie švu.",
            ],
        },
        {
            "heading": "Ako žehliť alebo naparovať mäkkú tkaninu",
            "paragraphs": [
                "Žehlite z rubu cez čistú ochrannú tkaninu pri teplote najcitlivejšieho vlákna. Látku pred žehličkou nenaťahujte a nepoužívajte silný tlak na šikmý diel, pretože horúci a vlhký textil sa ľahko deformuje. Švové rezervy podložte, aby sa nepretlačili na líc. Tlač a ozdobu skúšajte samostatne.",
                "Para môže uvoľniť krčenie, no viskózu alebo vlnu netvarujte zavesenú pod celou mokrou hmotnosťou. Skúšobné miesto nechajte vychladnúť a až potom hodnotíte lesk a rozmer. Ak sa povrch vyleští, tlač lepí alebo šev zvlňuje, ďalšie teplo zastavte. Odev pred uložením nechajte úplne vychladnúť a vyschnúť.",
            ],
        },
        {
            "heading": "Skladovanie, cestovanie a drobná oprava",
            "paragraphs": [
                "Challis ukladajte čistý a suchý, bez tlaku zipsov, korálikov a ostrých hrán. Ťažšie alebo šikmo strihané šaty nezavesujte dlhodobo na tenké ramienka; použite vnútorné pútka, široký vešiak alebo uloženie naplocho podľa konštrukcie. Pri cestovaní robte mäkké široké sklady a odev po vybalení nechajte odpočinúť.",
                "Vytiahnutú slučku neodstrihujte a otvorený šev nezaťažujte nosením. Oprava má obnoviť rozloženie napätia bez tuhého lepidla a nápadného zhustenia. Pri hodnotnej tlači alebo veľmi jemnej vlne zvoľte krajčíra či textilného odborníka. Dlhé svetlo môže meniť farbu, preto odev neskladujte na otvorenom slnečnom vešiaku.",
            ],
        },
    ],
    "table2_heading": "Challis po praní: čo znamená konkrétny prejav",
    "table2_intro": "Rozmer a omak hodnotíte úplne suché po stabilizácii. Mokrý vzhľad sám osebe nerozhoduje, no otvorený šev alebo prenos farby je dôvod zásah zastaviť.",
    "table2_headers": ["Prejav", "Pravdepodobná príčina", "Čo overiť", "Bezpečný ďalší krok"],
    "table2_rows": [
        ("Odev je mokrý tvrdší", "Dočasný stav vlákna, voda alebo zvyšok prostriedku.", "Oplach, úplné vyschnutie a povolené žehlenie.", "Nehodnotiť predčasne; nekrútiť ani nedrhnúť."),
        ("Rameno sa predĺžilo", "Mokrá hmotnosť a nedostatočná opora.", "Šev, smer strihu a rozmer po stabilizácii.", "Sušiť podopreté; trvalú zmenu riešiť krajčírsky."),
        ("Pri šve sú svetlé medzery", "Posun priadzí alebo otvorenie šva.", "Či je niť celá a kde sa sústreďuje ťah.", "Ďalšie nosenie zastaviť; spevniť vhodnou opravou."),
        ("Lem je zvlnený", "Rozdielne zrazenie, šikmý strih alebo napätie stehu.", "Dĺžku po odpočinku a polohu osnovy a útku.", "Nenapínať silou; stabilizovať a až potom upraviť."),
        ("Lesklá plocha po žehlení", "Príliš vysoký tlak alebo teplota.", "Zmenu po vychladnutí a stav vlákna.", "Ďalšie teplo zastaviť; mechanický lesk nedrhnúť."),
    ],
    "steps_heading": "Bezpečný postup pri praní challisu",
    "steps": [
        "Určte vlákno, plátnovú konštrukciu, podšívku, potlač a smer strihu.",
        "Odmerajte odev a odfoťte švy, lem, ramená a existujúce poškodenie.",
        "Na skrytom mieste otestujte farbu, produkt a rozmer po úplnom vysušení.",
        "Zvoľte program a chémiu podľa najcitlivejšej zložky, nie podľa názvu challis.",
        "Chráňte kus pred zipsami, háčikmi, hrubými uterákmi a preplneným bubnom.",
        "Mokrý odev nekrúťte a prenášajte ho s rovnomernou oporou.",
        "Urovnajte švy a lem bez násilného naťahovania osnovy, útku či šikmého dielu.",
        "Sušte podľa symbolu, často naplocho alebo na širokej vhodnej opore.",
        "Žehlite z rubu pri najnižšej účinnej teplote a bez ťahania látky.",
        "Rozmer a lem posúďte až po úplnom vysušení a stabilizácii.",
    ],
    "remember": [
        "Je challis vlnený, bavlnený, viskózový, syntetický alebo zmesový?",
        "Je diel strihaný po osnove, útku alebo šikmo?",
        "Sú jemné švy, ramená a podšívka pripravené niesť mokrú hmotnosť?",
        "Prenáša potlač alebo sýta farba na bielu vlhkú handričku?",
        "Aký bol rozmer pred praním a po úplnom vysušení?",
        "Povoľuje štítok práčku, ručné pranie, sušenie naplocho alebo čistiareň?",
    ],
    "mistakes": [
        "Považovať challis za synonymum viskózy a zvoliť jeden program pre všetky kusy.",
        "Dvíhať mokré šaty za ramená alebo tenké ramienka.",
        "Krútiť ručne praný odev a otvoriť jemné švy.",
        "Napínať zvlnený lem ešte mokrý bez merania a stabilizácie.",
        "Žehliť mäkkú tkaninu vysokým tlakom z líca.",
        "Skrátiť šikmý lem skôr, než sa látka po šití a praní ustáli.",
    ],
    "expert_heading": "Odbornejší pohľad: vzhľad je výsledkom väzby, priadze aj procesu",
    "expert": [
        "Výskum University of the Free State opisuje skúšaný vlnený challis ako ľahkú plátnovú tkaninu a sleduje jeho vlastnosti po úprave aj praní. Príručka University of Trinidad and Tobago používa označenie rayonový challis pri ľahkých odevných tkaninách. Tieto zdroje potvrdzujú, že challis nemožno zredukovať na jedno vlákno ani ošetrovať iba podľa obchodného názvu.",
        "ISO 6330 oddeľuje množstvo pracích postupov a šesť spôsobov sušenia, pretože kombinácia stroja, mechaniky, prostriedku a sušenia ovplyvňuje výsledok. Pri mäkkej tkanine je spôsob podopretia po praní rovnako dôležitý ako teplota vody.",
        "ISO 15487 hodnotí farbu, žmolkovanie, matovanie, hladkosť plochy a švov, zalisované záhyby aj komponenty. Pri challise treba k vzhľadu pridať rozmer, posun v šve a predĺženie šikmých dielov. Jedno číslo zrážania preto neopisuje celú použiteľnosť hotových šiat.",
    ],
    "source_intro": "Zdroje podporujú challis ako mäkkú ľahkú plátnovú tkaninu z viacerých vláknových rodín a samostatné hodnotenie prania, sušenia, vzhľadu a rozmeru. Nepodporujú jeden univerzálny postup pre každý kus.",
    "sources": [
        ("University of the Free State: skúšanie ľahkého vlneného challisu", UFS_CHALLIS),
        ("University of Trinidad and Tobago: rayonový challis v odevnej praxi", UTT_FASHION_HANDBOOK),
        ("ISO 6330: domáce pranie a sušenie pri textilnom skúšaní", ISO_WASH),
        ("ISO 15487: vzhľad po domácom praní a sušení", ISO_APPEARANCE),
        ("Woolmark: starostlivosť o vlnené varianty", WOOLMARK_CARE),
        ("GINETEX: symboly ošetrovania", GINETEX),
        ("EÚ 1007/2011: označovanie textilných vlákien", EU_FIBRE_LABEL),
    ],
    "related": [
        ("Čo je viskóza", ARTICLE_VISCOSE),
        ("Čo je krep", ARTICLE_CREPE),
        ("Čo je batist", ARTICLE_BATISTE),
        ("Ako čítať štítok na oblečení", ARTICLE_LABEL),
        ("Prečo sa oblečenie zráža", ARTICLE_SHRINKAGE),
        ("Vlna a polyamid v zmesiach", ARTICLE_WOOL_BLEND),
    ],
    "faq_title": "challis, splývanie a zrážanie",
    "faq": [
        ("Čo je challis?", "Mäkká ľahká plátnovo tkaná látka zo staplových priadzí, používaná na šaty, blúzky a šatky."),
        ("Je challis viskóza?", "Nie automaticky. Môže byť viskózový, vlnený, bavlnený, syntetický alebo zmesový."),
        ("Ako sa líši od krepu?", "Challis je typicky plátnový a mäkko splývavý; krep má výraznejší zrnitý efekt vytvorený priadzou, väzbou alebo dokončením."),
        ("Môže sa challis prať v práčke?", "Iba pri výslovnom symbole pre konkrétne vlákno a celý hotový výrobok."),
        ("Je ručné pranie bezpečnejšie?", "Nie vždy. Dlhé namáčanie, stláčanie a krútenie môžu poškodiť jemné švy a mokré vlákno."),
        ("Na koľko stupňov prať challis?", "Jedna teplota neexistuje. Rozhoduje vlákno, farba, potlač, podšívka a štítok."),
        ("Prečo sa challis zrazil?", "Môže ísť o relaxáciu napätia, napučanie a následné zrazenie vlákna alebo príliš vysoké teplo a mechaniku."),
        ("Prečo sa šaty predĺžili?", "Mokrá hmotnosť a šikmý strih môžu vytiahnuť rameno, šev alebo lem bez dostatočnej opory."),
        ("Ako challis sušiť?", "Podopretý podľa vlákna a symbolu, často naplocho, bez radiátora, fénu a prudkého slnka."),
        ("Ako ho žehliť?", "Z rubu cez ochrannú tkaninu pri najnižšej účinnej teplote, bez naťahovania mäkkej plochy."),
        ("Čo znamenajú medzery pri šve?", "Môže ísť o posun priadzí alebo otvorenie šva; ďalšie pranie problém nezacelí."),
        ("Treba metráž pred šitím vyprať?", "Iba spôsobom, ktorý bude povolený aj hotovému výrobku, a po skúške rozmeru, farby a povrchu na odstrižku."),
        ("Kedy zvoliť čistiareň?", "Pri vlnenom, podšitom, šikmo strihanom, historickom alebo výslovne profesionálne čistiteľnom odeve."),
    ],
}

add_cards(
    CHALLIS,
    product_heading="Prací gél podľa vlákna, nie podľa samotného názvu challis",
    product_intro="Pri výslovne prateľnom bavlnenom, viskózovom alebo inom kompatibilnom variante možno použiť presne odmeraný tekutý prostriedok a nízku mechaniku.",
    product_text="Prací gél Vevo najprv overte na skrytom mieste. Dávkujte podľa vody a náplne, koncentrát nelejte na suchú tlač a nechajte priestor na oplach.",
    product_limit="Pri vlnenom challise, citlivej zmesi, podšívke alebo profesionálnom symbole môže byť potrebný iný špecializovaný prostriedok alebo čistiareň.",
    category_heading="Pracie gély porovnávajte po overení vláknovej rodiny",
    category_intro="Challis môže mať viac zložení. Najprv určte najcitlivejšie vlákno, farbu, potlač, švy a povolený spôsob sušenia.",
    category_text="V kategórii nájdete gély pre rôzne druhy prateľnej bielizne. Vyberte iba kompatibilný produkt a vyššou dávkou nenahrádzajte správny program ani oporu pri sušení.",
)


ARTICLES: list[dict[str, object]] = [BUCKRAM, MADRAS, SHANTUNG, CHALLIS]


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
        raise SystemExit("Batch 54 link preflight failed")
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
