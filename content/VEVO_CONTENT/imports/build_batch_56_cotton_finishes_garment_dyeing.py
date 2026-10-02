#!/usr/bin/env python3
"""Build and validate VEVO batch 56 cotton-finish and garment-dyeing articles."""

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


PUBLISH_DATE = "2026-10-02"
CANDIDATES = Path("content/VEVO_CONTENT/batches/batch-56-candidates-2026-10-02.txt")
OUT_JSON = Path("content/VEVO_CONTENT/imports/batch-56-2026-10-02-articles.json")
OUT_PREFLIGHT = Path("content/VEVO_CONTENT/exports/batch-56-2026-10-02-link-preflight.json")

SANFOR_PROCESS = "https://www.sanforized.de/en/was-ist-sanfor"
SANFOR_STANDARD = "https://www.sanforized.de/en/einlaufstandards"
COTTON_SHRINKING = "https://cottonworks.com/learning-hub/quality-assurance/shrinking-and-skewing/"
COTTON_MECHANICAL = "https://cottonworks.com/learning-hub/finishing/mechanical-finishing/"
COTTON_DURABLE_PRESS = "https://cottonworks.com/encyclopedia-item/durable-press/"
COTTON_PUREPRESS = "https://cottonworks.com/product-innovation/product-technologies/purepress-technology/"
COTTON_GARMENT_DYE = "https://cottonworks.com/encyclopedia-item/garment-dyeing/"
COTTON_DYEING = "https://cottonworks.com/learning-hub/dyeing/dyeing-basics/"
AATCC_STANDARDS = "https://members.aatcc.org/store/items/"
GINETEX = "https://www.ginetex.net/share/article/4201/care-symbols"
EU_FIBRE_LABEL = "https://eur-lex.europa.eu/eli/reg/2011/1007/oj"
EU_FORMALDEHYDE = "https://single-market-economy.ec.europa.eu/news/chemicals-eu-restricts-exposure-carcinogenic-substance-formaldehyde-consumer-products-2023-07-14_en"
BIOPOLISH_REVIEW = "https://pmc.ncbi.nlm.nih.gov/articles/PMC3168787/"
BIOPOLISH_STUDY = "https://pmc.ncbi.nlm.nih.gov/articles/PMC10840485/"
BIOPOLISH_RECENT = "https://pmc.ncbi.nlm.nih.gov/articles/PMC10884504/"

ARTICLE_LABEL = "/n/ako-citat-stitok-na-obleceni-material-symboly-prania-a-spravny-program"
ARTICLE_COTTON = "/n/co-je-bavlna-vlastnosti-vyhody-nevyhody-a-starostlivost"
ARTICLE_SHRINKAGE = "/n/preco-sa-oblecenie-zrazi-po-prani-teplota-vlakna-susicka-a-prevencia"
ARTICLE_COLOR = "/n/stalofarebnost-textilu-preco-farby-blednu-pri-prani-svetle-a-treni"
ARTICLE_PILLING = "/n/preco-sa-oblecenie-zmolkuje-vlakna-trenie-pranie-a-susenie"
ARTICLE_NEW = "/n/ako-prat-nove-oblecenie-prvykrat-farby-chemicky-pach-zrazanie-a-stitok"
ARTICLE_BLEEDING = "/n/ako-zabranit-pustaniu-farby-pri-prani-noveho-oblecenia"
ARTICLE_DRYING = "/n/ako-susit-bielizen-v-malom-byte-bez-zatuchnutia"
ARTICLE_IRONING = "/n/ako-vyzehlit-koselu-kompletny-sprievodca-pre-dokonaly-vysledok"
ARTICLE_STAINS = "/n/ako-odstranit-zuvacku-krv-vosk-a-ine-skvrny-z-oblecenia"
ARTICLE_DENIM = "/n/ako-prat-riflovu-bundu-a-tmave-dzinsy-aby-nepustali-farbu"

PRODUCT_NAME = "Prací gél hypoalergénny Vevo Ylang Absolute 1L"
PRODUCT_URL = "/p-1627/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l"
CATEGORY_NAME = "Pracie gély"
CATEGORY_URL = "/c/vevo-home-care/pranie/praci-gel"


def add_cards(article: dict[str, object], *, product_heading: str, product_limit: str) -> None:
    article.update(
        {
            "product_heading": product_heading,
            "product_intro": (
                "Ak ošetrovací štítok povoľuje domáce pranie a všetky vlákna, farbivá "
                "aj úpravy sú kompatibilné, tekutý prací prostriedok možno presne odmerať "
                "podľa tvrdosti vody, veľkosti náplne a miery znečistenia."
            ),
            "product_name": PRODUCT_NAME,
            "product_url": PRODUCT_URL,
            "product_text": (
                "Tekutý gél sa dávkuje bez sypkého zvyšku. Koncentrát nelejte priamo na "
                "suchý odev, farebný šev ani upravený povrch a neprekračujte dávku v nádeji, "
                "že obnovíte výrobnú úpravu."
            ),
            "product_limit": product_limit,
            "category_heading": "Prací prostriedok prispôsobte celému odevu",
            "category_intro": (
                "Názov výrobnej úpravy nie je prací program. Pri výbere zohľadnite zloženie, "
                "farbu, elastan, podšívku, potlač, intenzitu znečistenia aj symboly na etikete."
            ),
            "category_name": CATEGORY_NAME,
            "category_url": CATEGORY_URL,
            "category_text": (
                "V kategórii nájdete pracie gély pre rôzne potreby bežnej prateľnej bielizne. "
                "Vyberte iba kompatibilný výrobok, dodržte dávkovanie a ponechajte priestor na oplach."
            ),
        }
    )


SANFORIZED: dict[str, object] = {
    "title": "Čo je sanforizovaná alebo predzrazená bavlna: zvyškové zrážanie a starostlivosť",
    "link": "co-je-sanforizovana-alebo-predzrazena-bavlna-zvyskove-zrazanie-a-starostlivost",
    "meta": "Čo znamená sanforizovaná a predzrazená bavlna, koľko sa ešte môže zraziť a ako správne merať, prať, sušiť a reklamovať rozmerovú zmenu.",
    "short": "Sanforizácia je kontrolovaná mechanická úprava, ktorá znižuje zvyškové zrážanie bavlnenej tkaniny ešte pred ušitím. Neznamená nulovú zmenu rozmeru ani povolenie ignorovať štítok hotového odevu.",
    "name": "sanforizovaná alebo predzrazená bavlna",
    "locative": "sanforizovanej bavlne",
    "identity_heading": "Sanforizácia upravuje rozmerovú stabilitu, nie chemické zloženie bavlny",
    "identity_detail": "Pri kontrolovanom kompresnom zrážaní sa tkanina zvlhčí, mechanicky stlačí v pozdĺžnom smere a následne vysuší tak, aby sa časť napätia a budúceho zmrštenia uvoľnila už vo výrobe.",
    "identity_boundary": "Nápis predzrazené môže označovať aj iný výrobný postup alebo iba všeobecné obchodné tvrdenie; chránené označenie SANFOR sa viaže na konkrétny proces a kontrolované limity, no ani ono neprepisuje etiketu hotového výrobku.",
    "label_focus": "zloženie vrchnej látky, elastan, podšívku, výstuž, povrchovú úpravu, potlač, výšivku, odporúčanú teplotu, mechaniku, sušičku a spôsob žehlenia",
    "missing_label": "Pri metráži si vyžiadajte technický list s podmienkami skúšky; pri hotovom odeve bez etikety nemožno z jedného slova na visačke bezpečne určiť teplotu ani spôsob sušenia.",
    "dry_check": "pôvodnú dĺžku a šírku medzi pevnými bodmi, smer osnovy, skosenie bočných švov, zvlnenie lemu, napätie pri gumičke, praskanie potlače a odlišnú dĺžku podšívky",
    "damage_boundary": "Zvyškové zrazenie možno zmerať, no elasticky vytiahnuté koleno, skrútený strih, poškodený elastan, zle zvolená veľkosť alebo kratšia podšívka nie sú tým istým javom.",
    "test_focus": "Vzorku alebo odev merajte pred cyklom a po úplnom vysušení a klimatizovaní rovnakým spôsobom; mokrý rozmer, násilné naťahovanie alebo iné miesto merania nedajú porovnateľný výsledok.",
    "combined_risk": "uvoľnenia výrobného napätia, napučania bavlnených vlákien, rozdielnej stability osnovy a útku, vysokej mechaniky a presušenia pri nadmernej teplote",
    "chemistry_boundary": "Prací prostriedok odstráni nečistotu, ale neobnoví nesprávne vykonanú kompresiu ani neopraví tkaninu, ktorú poškodilo teplo; väčšia dávka neznižuje zvyškové zrážanie.",
    "drying_detail": "Košeľu urovnajte podľa švov, nohavice podľa smeru nohavíc a obliečku otvorte v rohoch; rozmer hodnotíte až po rovnomernom vysušení, nie po vytiahnutí horúceho kusu zo sušičky.",
    "heat_boundary": "Vysoká teplota môže urýchliť relaxáciu bavlny, poškodiť elastan, zmeniť potlač, stiahnuť šijaciu niť alebo vytvoriť rozdiel medzi vrchnou látkou a výstužou.",
    "stop_signs": "prudká zmena rozmeru po jednom povolenom cykle, skrútenie šva, zvlnenie légy, popraskanie potlače, stiahnutá podšívka, lepkavá úprava alebo rastúca deformácia pri každom praní",
    "professional_boundary": "Jednoduchý označený bavlnený odev možno prať doma podľa etikety, kým konštrukčne zložité sako, lepená výstuž, neznáma metráž, historický textil alebo sporná reklamácia potrebujú technické údaje či odborné posúdenie.",
    "answer": "Sanforizovaná bavlna prešla vo výrobe riadeným mechanickým predzrazením, ktoré znižuje množstvo zmeny očakávanej pri ďalšom praní. Neznamená to, že sa odev už nikdy nezmenší. Rozhoduje, či ide o overené označenie, aká je konštrukcia tkaniny, ako bol odev ušitý a akú teplotu, pohyb a sušenie povoľuje etiketa. Nový kus odmerajte medzi rovnakými bodmi, perte podľa symbolov, nepresušujte ho a výsledok posudzujte až úplne suchý. Skrútený šev, poškodený elastan alebo zle zvolená veľkosť sa nesmú zamieňať so zvyškovým zrážaním.",
    "intro": "Pri kúpe bavlnenej košele, nohavíc alebo metráže znie predzrazené ako poistka proti zmene veľkosti. V skutočnosti ide o informáciu o dokončovacom procese, nie o absolútnu záruku. Bavlnená tkanina môže v sebe niesť napätie z pradenia, tkania, napínania a sušenia. Kompresná úprava časť tohto napätia uvoľní ešte pred strihaním, no hotový výrobok pridá švy, nite, elastan, výstuž, potlač a spôsob domáceho ošetrovania. Preto má zmysel rozumieť procesu, správne merať a pri nečakanej zmene najprv určiť príčinu.",
    "quick": [
        "<strong>Sanforizácia je mechanická úprava:</strong> pracuje s vlhkosťou, kompresiou a kontrolovaným sušením tkaniny.",
        "<strong>Predzrazené neznamená nezraziteľné:</strong> po povolenom praní môže zostať malá zvyšková rozmerová zmena.",
        "<strong>Označenia nie sú rovnocenné:</strong> všeobecné obchodné tvrdenie nemusí mať rovnaký skúšobný rámec ako licencované označenie SANFOR.",
        "<strong>Merajte vždy rovnako:</strong> pred praním aj po úplnom vysušení medzi pevne označenými bodmi bez naťahovania.",
        "<strong>Sledujte oba smery:</strong> dĺžka, šírka a skosenie šva sú tri odlišné výsledky.",
        "<strong>Štítok má prednosť:</strong> predzrazenie nepovoľuje vyššiu teplotu, sušičku ani bielenie nad limit celého odevu.",
    ],
    "overview_heading": "Ako funguje kontrolované kompresné zrážanie bavlnenej tkaniny",
    "overview": [
        "Pri výrobe tkanina prechádza pod napätím cez viac operácií. Osnovné nite sa pri tkaní zvlňujú okolo útku, plocha sa napína na šírku a povrch sa následne dokončuje. Keď sa neošetrená bavlna neskôr namočí a uvoľní, časť uloženého napätia sa zmení na kratší alebo užší rozmer. CottonWorks tento jav odlišuje od chemického poškodenia: ide najmä o relaxáciu konštrukcie a o to, koľko priestoru si priadze po praní znovu usporiadajú.",
        "Pri sanforizácii sa tkanina primerane zvlhčí a vedie medzi plochami, ktoré ju kontrolovane stlačia v pozdĺžnom smere. Gumový pás sa najprv natiahne, tkanina sa k nemu priloží a pri návrate pásu sa mechanicky skomprimuje. Palcový alebo plstený sušiaci úsek následne stabilizuje dosiahnutý rozmer. Cieľom nie je zmeniť bavlnu na iné vlákno, ale priblížiť konštrukciu rozmeru, ku ktorému by sa inak dopracovala až pri používaní.",
        "Účinok sa hodnotí definovaným praním, sušením a meraním, nie pocitom po jednom náhodnom cykle. Oficiálne podmienky SANFOR rozlišujú tkaniny a pleteniny a uvádzajú rozdielne limity. Domáci výsledok môže ovplyvniť teplota, sušička, preplnenie, program, prirodzená pružnosť, šitie aj meranie. Preto sa technický údaj číta spolu s metódou skúšky a s etiketou hotového výrobku.",
    ],
    "table1_heading": "Sanforizované, predzrazené a bežné bavlnené výrobky",
    "table1_intro": "Tieto označenia hovoria o rozdielnej úrovni informácie. Ani jeden riadok nenahrádza ošetrovacie symboly konkrétneho odevu.",
    "table1_headers": ["Označenie", "Čo možno rozumne vyvodiť", "Čo z neho nezistíte", "Čo si overiť"],
    "table1_rows": [
        ("SANFOR / Sanforized", "Tkanina má byť spracovaná a kontrolovaná podľa pravidiel príslušného označenia.", "Presný limit hotového odevu pri každom domácom programe.", "Licenciu alebo technický list, typ textilu a skúšobnú metódu."),
        ("Predzrazené / pre-shrunk", "Výrobca deklaruje zásah na zníženie budúcej zmeny.", "Aký proces použil a aké zvyškové zrazenie nameral.", "Konkrétnu hodnotu, smer merania a podmienky prania a sušenia."),
        ("Mechanicky kompaktované", "Rozmer bol upravený tlakom a pohybom, spravidla bez potreby zosieťujúcej živice.", "Kompatibilitu farby, elastanu, potlače a šitia.", "Celú konštrukciu výrobku a ošetrovací štítok."),
        ("Bez údaja", "Nie je potvrdený konkrétny proces ani limit.", "Či sa kus zrazí málo alebo výrazne.", "Skúšobný odstrižok pri metráži alebo údaje výrobcu pri odeve."),
        ("Sanforizovaná metráž", "Úprava bola vykonaná pred strihaním.", "Či krajčír rešpektoval smer, či niť a podšívka reagujú rovnako.", "Rezervu, švy, vložené materiály a výsledok hotového kusu."),
    ],
    "sections": [
        {
            "heading": "Sanforizovaná bavlna a predzrazená bavlna nie sú vždy presné synonymá",
            "paragraphs": [
                "Sanforizácia označuje konkrétnu rodinu kontrolovaných kompresných procesov a značka SANFOR má vlastné podmienky používania. Predzrazenie je širší pojem. Výrobca mohol látku vyprať, napariť, mechanicky skompaktovať alebo použiť inú kombináciu. Bez technického listu nepoznáte cieľovú hodnotu, počet skúšobných cyklov ani to, či sa údaj vzťahuje na metráž alebo hotový výrobok.",
                "V bežnej domácnosti preto tieto názvy nečítajte ako prísľub nulovej zmeny. Užitočnejšia otázka znie: aké zvyškové zrážanie výrobca deklaruje v dĺžke a šírke a pri akom postupe? Ak predajca odpovie iba slovom predzrazené, pri tesne sediacom odeve alebo presnom interiérovom rozmere rátajte s rezervou a dodržte najmiernejší povolený spôsob sušenia.",
            ],
        },
        {
            "heading": "Koľko sa môže sanforizovaná bavlna ešte zraziť",
            "paragraphs": [
                "Oficiálne štandardy SANFOR uvádzajú pre označené tkaniny veľmi nízky zvyškový limit pri stanovenej skúške; pri pleteninách sú hranice odlišné. Hodnotu však nemožno automaticky preniesť na každý výrobok s voľným slovom sanforizované. Rozhoduje licencia, textilná konštrukcia, smer, skúšobná norma, cyklus sušenia a presnosť merania. Navyše šev alebo podšívka môžu zmeniť vzhľad aj pri stabilnej vrchnej tkanine.",
                f"Ak potrebujete prakticky posúdiť zmenu, spojte tento návod so všeobecným vysvetlením, <a href=\"{ARTICLE_SHRINKAGE}\">prečo sa oblečenie zráža po praní</a>. Malá zmena po prvom cykle môže byť zvyšková relaxácia. Veľký skok, ktorý prekročí deklaráciu pri dodržanom postupe, treba zdokumentovať. Opakované horúce sušenie však môže vytvoriť podmienky, ktoré výrobca v deklarácii vôbec nepovolil.",
            ],
        },
        {
            "heading": "Ako doma správne zmerať zvyškové zrážanie",
            "paragraphs": [
                "Odev položte bez naťahovania na rovnú plochu a označte si reprodukovateľné body: stred zadného dielu od pevného šva, šírku medzi prieramkami, vnútornú dĺžku nohavice alebo rozmer vzorky medzi značkami. Zapíšte centimetre, spôsob uloženia, teplotu, program a sušenie. Pružný pás nemerajte podľa voľného okraja, ktorý sa pri každom položení usadí inak.",
                "Po praní nechajte kus úplne vyschnúť a ustáliť pri bežnej izbovej klíme. Zmerajte tie isté body rovnakým metrom a bez žehlenia, ktoré by mohlo plochu dočasne vytiahnuť. Percentuálnu zmenu vypočítate ako rozdiel pôvodného a nového rozmeru delený pôvodným rozmerom. Pri reklamácii priložte fotografie značiek a etikety, nie iba tvrdenie, že je kus menší.",
            ],
            "callout": {
                "title": "Aby bolo meranie porovnateľné",
                "items": [
                    "Použite pevné švy alebo vyznačené body, nie pohyblivý okraj.",
                    "Zapíšte dĺžku aj šírku a samostatne sledujte skrútenie šva.",
                    "Porovnávajte až úplne suchý a uvoľnený kus.",
                    "Nenaťahujte odev späť na pôvodný rozmer pred druhým meraním.",
                ],
                "background": "#f7fbf8",
                "border": "#dbe5de",
            },
        },
        {
            "heading": "Ako prať sanforizovanú bavlnenú košeľu",
            "paragraphs": [
                f"Najprv prečítajte <a href=\"{ARTICLE_LABEL}\">materiálový a ošetrovací štítok</a>. Zapnite alebo uvoľnite prvky podľa odporúčania výrobcu, vyprázdnite vrecká a lokálne ošetrite golier bez tvrdej kefy. Košeľu triede podľa farby, nechajte bubon voľný a použite dávku určenú tvrdosťou vody a náplňou. Sanforizácia nechráni farbivo pred bielidlom ani výstuž goliera pred nevhodnou chémiou.",
                f"Po skončení košeľu hneď vyberte, urovnajte légu, golier a švy a sušte spôsobom zo štítku. Pri povolenej sušičke voľte len schválený režim a nepresušujte. <a href=\"{ARTICLE_IRONING}\">Správne poradie žehlenia košele</a> pomôže vyrovnať záhyby, ale vysoká para a ťah nemajú slúžiť na násilné vracanie zrazeného rozmeru.",
            ],
        },
        {
            "heading": "Sanforizované džínsy, nohavice a rozdiel oproti surovému denimu",
            "paragraphs": [
                "Pri džínsoch sa označenie sanforized často používa na odlíšenie od nesanforizovaného surového denimu, ktorý môže mať po prvom praní väčšiu rozmerovú zmenu. Ani sanforizované džínsy však nie sú rozmerovo nehybné. Hustá tkanina, elastan, pás, vreckové vaky a šijacie nite sa počas nosenia a prania správajú odlišne a tesný pás sa môže po nosení znovu mierne povoliť.",
                f"Džínsy perte podľa etikety obrátené naruby a s podobnými tmavými farbami. Súvisiace zásady rozoberá návod <a href=\"{ARTICLE_DENIM}\">ako prať tmavé džínsy a rifľovú bundu</a>. Nezamieňajte úbytok farby s úbytkom rozmeru a nehodnoťte veľkosť tesne po horúcom sušení. Ak výrobca odporúča sušenie na vzduchu, sušička nie je vhodný test kvality sanforizácie.",
            ],
        },
        {
            "heading": "Metráž, šitie a otázka, či treba látku pred strihaním vyprať",
            "paragraphs": [
                "Pri metráži si vyžiadajte informáciu o zvyškovom zrážaní, šírke a odporúčanom predspracovaní. Aj sanforizovanú látku môže byť rozumné pred strihaním ošetriť rovnakým povoleným spôsobom, akým sa bude prať hotový výrobok, no najprv to overte u dodávateľa. Povrch odpudzujúci vodu, škrob, potlač alebo elastická zložka môžu vyžadovať iný postup než obyčajná bavlnená košeľovina.",
                "Skúšobný odstrižok olemujte, vyznačte meraciu plochu a perte ho bez násilného naťahovania. Krajčír musí zohľadniť smer osnovy, rezervu švov a rozdielnu stabilitu podšívky, výstuže a nite. Predpranie hotovej metráže nevyrieši krivo položený strih ani nevhodnú vložku. Pri presnom výrobku je technický údaj hodnotnejší než spoliehanie sa na názov v e-shope.",
            ],
        },
        {
            "heading": "Prečo sa bočný šev skrúti, aj keď sa dĺžka takmer nezmenila",
            "paragraphs": [
                "Skosenie alebo spirality vzniká, keď sa smer tkaniny či pleteniny po uvoľnení odchýli a bočný šev sa začne otáčať dopredu alebo dozadu. Je to iný parameter než percento zrážania. Odev môže zachovať dĺžku aj šírku, ale pôsobiť deformovane. Príčinou môže byť šikmo položený strih, napätie pri pletení, nesprávne vyrovnanie tkaniny alebo rozdielne správanie dielov.",
                "Pred prvým praním odfoťte polohu šva voči stredu nohavice a po cykle ju porovnajte bez krútenia rukou. Sanforizácia môže zahŕňať kontrolu skosenia, no všeobecný nápis predzrazené ho negarantuje. Ak sa nový odev pri dodržaní etikety výrazne skrúti, nenaťahujte ho opakovane v mokrom stave; zdokumentujte rozmer aj uhol a kontaktujte predajcu.",
            ],
        },
        {
            "heading": "Elastan, potlač, výšivka a podšívka určujú vlastné limity",
            "paragraphs": [
                "Bavlnená vrchná tkanina môže byť stabilizovaná, no elastan citlivo reaguje na vysoké teplo a oxidáciu. Veľká potlač sa môže zmrštiť inak než podklad, výšivka môže sťahovať okolie a podšívka môže po praní zostať kratšia. Pri košeli s lepenou výstužou sa môže zvlniť léga alebo golier bez toho, aby sa samotná plocha bavlny výrazne zmenila.",
                "Ošetrovací symbol zohľadňuje hotový výrobok, preto sa riaďte najcitlivejšou súčasťou. Pred lokálnym čistením vyskúšajte farbu nite aj potlače. Pri kombinovanom kuse zaznamenajte nielen celkovú dĺžku, ale aj vzťah vrstiev: presah podšívky, rovnosť lemu a napätie okolo aplikácie. To pomáha odlíšiť zrazenie látky od poruchy spoja.",
            ],
        },
        {
            "heading": "Prvé pranie nového predzrazeného odevu",
            "paragraphs": [
                f"Pred prvým cyklom odstráňte visačky, no odfoťte zloženie, symboly a tvrdenie o predzrazení. Skontrolujte farebný prenos na skrytom mieste, odmerajte kus a perte ho s kompatibilnými farbami. Podrobnejšiu prípravu nájdete v článku <a href=\"{ARTICLE_NEW}\">ako prať nové oblečenie prvýkrát</a>. Nepridávajte vyššiu teplotu len preto, aby ste odev zmenšili na mieru.",
                "Ak chcete predvídateľný výsledok, použite presne ten spôsob, ktorý plánujete dlhodobo a ktorý zároveň povoľuje etiketa. Náhodná kombinácia horúcej vody a sušičky síce môže rozmer zmenšiť, ale môže poškodiť farbu, elastan a švy a výsledok nemusí byť rovnomerný. Prvé pranie je kontrola starostlivosti, nie domáca výrobná operácia.",
            ],
        },
        {
            "heading": "Sušenie, žehlenie a dočasné vytiahnutie rozmeru",
            "paragraphs": [
                f"Po praní bavlna drží vodu a mokrý kus sa vlastnou hmotnosťou môže na vešiaku dočasne predĺžiť. Naopak, horúca sušička môže podporiť relaxáciu a presušenie. Vyberte spôsob zo štítku, urovnajte švy a zabezpečte prúdenie vzduchu podľa zásad pre <a href=\"{ARTICLE_DRYING}\">sušenie bielizne bez zatuchnutia</a>. Rozmer nemerajte počas týchto prechodných stavov.",
                "Žehlenie s parou môže plochu dočasne vyrovnať alebo pri ťahu natiahnuť. Preto sa pri technickom porovnaní meria po definovanom ošetrení a ustálení, nie hneď pod žehličkou. Pri bežnom nosení žehlite podľa najcitlivejšej zložky, z rubu pri potlači a bez silného ťahania okrajov. Násilné blokovanie nie je dôkazom, že odev rozmer skutočne drží.",
            ],
        },
        {
            "heading": "Kedy ide o zvyškové zrážanie a kedy o reklamovateľnú chybu",
            "paragraphs": [
                "Malá zmena v rámci deklarovaného limitu, nameraná po stanovenom cykle, môže byť očakávaným zvyškovým zrážaním. Podozrivá je veľká, nerovnomerná alebo pokračujúca zmena pri postupe, ktorý etiketa povoľuje, najmä ak sa pridá skrútenie šva či rozdiel medzi dielmi. Rozhodujú dôkazy: pôvodné rozmery, účet, etiketa, fotografie a záznam o praní a sušení.",
                "Pred reklamáciou odev nežehlite do rozmeru, nestrihajte lem a neopakujte horúci cyklus. Predajcovi popíšte pevné meracie body a percentuálnu zmenu. Ak výrobca používa konkrétne technické tvrdenie, požiadajte o skúšobnú metódu. Rozdiel medzi nesprávnou veľkosťou a zmenou po praní sa najlepšie preukazuje meraním pred prvým použitím.",
            ],
        },
    ],
    "table2_heading": "Odev je po praní menší alebo deformovaný: čo pravdepodobne sledujete",
    "table2_intro": "Jeden dojem môže mať viac príčin. Najprv porovnajte suché rozmery, švy, vrstvy a pružnosť, až potom vyberte ďalší krok.",
    "table2_headers": ["Prejav", "Možné vysvetlenie", "Ako ho odlíšiť", "Bezpečný ďalší krok"],
    "table2_rows": [
        ("Kus je kratší aj užší", "Zvyšková relaxácia, vyššia teplota alebo presušenie.", "Zmerať oba smery po úplnom vysušení a porovnať s postupom.", "Ďalej nezohrievať; zdokumentovať a overiť deklarovaný limit."),
        ("Bočný šev sa otáča", "Skosenie, spirality alebo šikmo položený strih.", "Porovnať polohu šva, aj keď sa dĺžka takmer nezmenila.", "Nenapínať mokré; pri novom kuse riešiť s predajcom."),
        ("Pás je tesný, kolená voľné", "Zmes zrazenia, elastického zotavenia a vytiahnutia pri nosení.", "Skontrolovať elastan a merať po rovnakom čase od prania.", "Vyhnúť sa teplu a nehodnotiť iba jeden pružný bod."),
        ("Podšívka ťahá vrchnú látku", "Rozdielna stabilita vrstiev alebo výstuže.", "Porovnať presah a zvlnenie po celom obvode.", "Nežehliť silou; zvoliť odborné posúdenie."),
        ("Potlač praská, no rozmer je podobný", "Teplo alebo mechanika poškodili povrch, nie rozmerovú stabilitu.", "Prezrieť okraje motívu a skrytú plochu.", "Zastaviť teplo a trenie; nevydávať jav za zrazenie bavlny."),
    ],
    "steps_heading": "Ako ošetriť sanforizovaný alebo predzrazený bavlnený odev krok za krokom",
    "steps": [
        "Odfoťte etiketu, tvrdenie o úprave a pôvodný stav švov, potlače a podšívky.",
        "Odev položte bez naťahovania a zmerajte dĺžku, šírku a polohu švov medzi pevnými bodmi.",
        "Roztrieďte ho podľa farby a zloženia a škvrnu ošetrite iba kompatibilným prípravkom.",
        "Zvoľte program, teplotu a mechaniku podľa symbolov celého výrobku, nie podľa slova sanforizované.",
        "Použite primeranú dávku prostriedku a nechajte v bubne priestor na pohyb a oplach.",
        "Po cykle kus hneď vyberte, urovnajte švy bez násilného ťahania a sušte povoleným spôsobom.",
        "Nechajte odev úplne vyschnúť a ustáliť, potom zopakujte meranie rovnakou technikou.",
        "Pri nečakanej zmene zastavte ďalšie teplo, uchovajte záznam a požiadajte o technické údaje alebo reklamáciu.",
    ],
    "remember": [
        "Je označenie SANFOR overené, alebo ide iba o všeobecné tvrdenie predzrazené?",
        "Poznáte deklarovanú hodnotu, smer a podmienky skúšky?",
        "Má odev elastan, podšívku, výstuž, potlač alebo inú citlivú zložku?",
        "Merali ste rovnaké pevné body pred praním aj po úplnom vysušení?",
        "Odlišujete zrazenie, skosenie šva, pružné zotavenie a poškodenie teplom?",
        "Dodržali ste symbol prania, sušenia a žehlenia celého výrobku?",
    ],
    "mistakes": [
        "Považovať predzrazené za prísľub nulovej zmeny pri ľubovoľnom programe.",
        "Merať mokrý alebo horúci kus a porovnávať ho s voľne položeným suchým odevom.",
        "Sledovať iba dĺžku a prehliadnuť šírku, skrútený šev alebo kratšiu podšívku.",
        "Použiť horúcu sušičku ako spôsob úmyselného zmenšenia presne na mieru.",
        "Zamieňať poškodený elastan alebo prasknutú potlač so zvyškovým zrážaním.",
        "Opakovane naťahovať nový chybný kus a zničiť dôkazy potrebné na reklamáciu.",
    ],
    "expert_heading": "Odbornejší pohľad: zvyškové zrážanie, smer tkaniny a skúšobné podmienky",
    "expert": [
        "Kontrolovaná kompresia mení geometriu tkaniny bez toho, aby z bavlny vytvorila nové vlákno. Vlhkosť zvyšuje pohyblivosť priadzí, gumový pás prenesie pozdĺžnu kompresiu a sušiaci úsek stabilizuje nový stav. Výsledok závisí od väzby, hustoty, priadze, predchádzajúceho napätia a nastavenia stroja. Preto sa účinok overuje meraním po definovanom ošetrení, nie iba vizuálnou kontrolou role.",
        "SANFOR GmbH uvádza pre označené tkaniny limit zrazenia alebo predĺženia v smere osnovy aj útku pri určenej norme ISO 6330, pričom pleteniny majú odlišný rámec. Toto číslo je vlastnosť overená za stanovených podmienok. Domáca sušička, odlišný program alebo hotový výrobok s ďalšími materiálmi môže vytvoriť iný výsledok, a preto sa technické tvrdenie musí čítať spolu s metodikou.",
        "AATCC združuje samostatné metódy pre rozmerové zmeny tkanín a odevov po domácom praní. Oddeľuje meranie dĺžky a šírky od vzhľadu švov či krútenia. GINETEX zároveň vysvetľuje, že symboly vyjadrujú maximálnu dovolenú záťaž pri ošetrovaní hotového výrobku. Sanforizácia znižuje jedno riziko, ale nezvyšuje automaticky hranice farbiva, elastanu, potlače ani výstuže.",
    ],
    "source_intro": "Zdroje vysvetľujú kompresný proces, limity označenia, rozmerové skúšanie a význam symbolov. Nepodporujú tvrdenie, že predzrazená bavlna sa pri každom domácom postupe zmení presne rovnako alebo vôbec.",
    "sources": [
        ("SANFOR: ako funguje kontrolované kompresné zrážanie", SANFOR_PROCESS),
        ("SANFOR: štandardy zvyškového zrážania", SANFOR_STANDARD),
        ("CottonWorks: shrinking and skewing", COTTON_SHRINKING),
        ("CottonWorks: mechanical finishing", COTTON_MECHANICAL),
        ("AATCC: prehľad textilných skúšobných štandardov", AATCC_STANDARDS),
        ("GINETEX: význam symbolov ošetrovania", GINETEX),
    ],
    "related": [
        ("Prečo sa oblečenie zráža po praní", ARTICLE_SHRINKAGE),
        ("Ako čítať štítok na oblečení", ARTICLE_LABEL),
        ("Čo je bavlna a ako sa o ňu starať", ARTICLE_COTTON),
        ("Ako prať nové oblečenie prvýkrát", ARTICLE_NEW),
        ("Ako prať tmavé džínsy", ARTICLE_DENIM),
        ("Ako správne vyžehliť košeľu", ARTICLE_IRONING),
    ],
    "faq_title": "sanforizovaná a predzrazená bavlna",
    "faq": [
        ("Čo znamená sanforizovaná bavlna?", "Bavlnená tkanina prešla kontrolovaným mechanickým kompresným procesom, ktorý znižuje jej zvyškové zrážanie pri ďalšom ošetrovaní."),
        ("Je sanforizované to isté ako predzrazené?", "Sanforizácia je konkrétnejší kontrolovaný proces. Predzrazené je širšie tvrdenie a bez technického listu nemusí označovať rovnakú metódu ani limit."),
        ("Môže sa sanforizovaná bavlna ešte zraziť?", "Áno, malé zvyškové zrážanie je možné. Výsledok ovplyvňuje konštrukcia, hotový odev, teplota, mechanika a sušenie."),
        ("Koľko percent sa zrazí predzrazená bavlna?", "Jedno číslo pre všetky výrobky neexistuje. Pýtajte sa na deklarovaný limit, smer merania a konkrétnu skúšobnú metódu."),
        ("Môžem sanforizovanú košeľu prať na vysokej teplote?", "Iba ak ju povoľuje ošetrovací štítok celého odevu. Úprava nezvyšuje teplotnú odolnosť farby, elastanu ani výstuže."),
        ("Môže ísť sanforizovaná bavlna do sušičky?", "Len pri príslušnom symbole. Presušenie alebo vyššia teplota môžu zvýšiť zmenu rozmeru a poškodiť ďalšie súčasti."),
        ("Ako zmeriam zrazenie košele?", "Pred praním aj po úplnom vysušení merajte medzi rovnakými pevnými bodmi na rovnej ploche bez naťahovania a zapíšte spôsob ošetrenia."),
        ("Je skrútený bočný šev zrazenie?", "Nie nevyhnutne. Môže ísť o skosenie alebo spirality, ktoré treba hodnotiť samostatne od dĺžky a šírky."),
        ("Treba sanforizovanú metráž pred šitím vyprať?", "Závisí od technického listu a plánovanej údržby. Najbezpečnejší je skúšobný olemovaný odstrižok ošetrený povoleným spôsobom."),
        ("Dá sa zrazená sanforizovaná bavlna natiahnuť späť?", "Mierne dočasné urovnanie môže byť možné, no násilné naťahovanie nie je spoľahlivá oprava a môže deformovať švy alebo tkaninu."),
        ("Prečo podšívka po praní ťahá?", "Vrchná látka, podšívka a výstuž mohli zmeniť rozmer rozdielne. Nejde automaticky o chybu samotnej sanforizácie."),
        ("Ako reklamovať veľké zrazenie?", "Uchovajte etiketu a doklad, zdokumentujte pôvodné aj konečné meranie, popíšte presný program a ďalšie teplo už nepoužívajte."),
        ("Zníži väčšia dávka pracieho gélu zrážanie?", "Nie. Nadbytok môže zhoršiť oplach; rozmerovú stabilitu výrobnej konštrukcie neobnoví."),
    ],
}

add_cards(
    SANFORIZED,
    product_heading="Prací gél pre označenú prateľnú predzrazenú bavlnu",
    product_limit="Predzrazenie nenahrádza štítok a produkt nie je určený na obnovu rozmeru, poškodeného elastanu, potlače ani lepených vrstiev.",
)


EASY_CARE: dict[str, object] = {
    "title": "Čo je nekrčivá bavlna a easy-care úprava: ako funguje a ako ju prať",
    "link": "co-je-nekrciva-bavlna-a-easy-care-uprava-ako-funguje-a-ako-ju-prat",
    "meta": "Ako funguje nekrčivá, easy-care, non-iron a durable-press bavlna, aké má limity a ako ju prať, sušiť a žehliť bez poškodenia úpravy.",
    "short": "Nekrčivá alebo easy-care bavlna je textil s úpravou, ktorá pomáha obmedziť tvorbu záhybov a zachovať hladší vzhľad po praní. Nie je to nové vlákno, nezostáva navždy bez vrások a nesmie sa ošetrovať bez ohľadu na štítok.",
    "name": "nekrčivá bavlna a easy-care úprava",
    "locative": "easy-care bavlne",
    "identity_heading": "Easy-care opisuje funkčnú úpravu, nie samostatný druh bavlny",
    "identity_detail": "Pri durable-press alebo wrinkle-resistant dokončení sa celulózové reťazce stabilizujú zosieťujúcou chémiou a následným vytvrdením, aby sa tkanina po namočení a pohybe ľahšie vracala k nastavenému tvaru.",
    "identity_boundary": "Výrazy easy-care, non-iron, wrinkle-free a nekrčivé nemajú v každom obchode rovnakú úroveň výkonu a rôzni výrobcovia používajú odlišné živice, katalyzátory, receptúry aj procesy; z názvu nemožno určiť presnú chémiu.",
    "label_focus": "percento bavlny a syntetických vlákien, typ úpravy, golier a manžety, elastan, potlač, odporúčaný program, maximálnu teplotu, sušičku, žehlenie a prípadné osobitné upozornenia výrobcu",
    "missing_label": "Ak neviete, či hladkosť vytvára chemická úprava, syntetická zmes alebo konštrukcia tkaniny, voľte šetrný postup podľa najcitlivejšej známej zložky a vyžiadajte si údaje predajcu.",
    "dry_check": "lesk na golieri a lakťoch, zlomené záhyby, žltnutie, krehkosť, praskanie pri šve, odretie manžiet, rozdielny odtieň dielov a stav lepených výstuží",
    "damage_boundary": "Pokles nekrčivosti po používaní môže byť opotrebenie alebo vypranie funkčnej úpravy; škvrna, mastný lesk, poškodené vlákno a trvalý lom majú inú príčinu a ďalšia dávka chémie ich neopraví.",
    "test_focus": "Po skúške sledujte farbu, omak, pevnosť povrchu a vzhľad záhybov po úplnom vysušení; zmäknutie, žltnutie alebo krehkosť sú rovnako dôležité ako odstránenie škvrny.",
    "combined_risk": "napučania bavlny, trenia v záhyboch, hydrolýzy alebo opotrebovania funkčnej úpravy, tepelného namáhania a rozdielnej reakcie goliera, šijacej nite a výstuže",
    "chemistry_boundary": "Viac pracieho prostriedku neobnoví zosieťovanie a agresívne bodové bielenie môže zmeniť farbu či pevnosť; každý odstraňovač škvŕn treba skúsiť na skrytom mieste.",
    "drying_detail": "Košeľu vyberte po skončení cyklu, vyrovnajte golier, manžety, légu a švy a nechajte vzduch prúdiť; dlhé stlačenie v bubne vytvorí záhyby, ktoré samotný nápis non-iron neodstráni.",
    "heat_boundary": "Nadmerné teplo môže zožltnúť povrch, poškodiť elastan, lepidlo a potlač alebo urýchliť krehkosť; vysoká teplota nie je bezpečný spôsob, ako znovu aktivovať opotrebovanú úpravu.",
    "stop_signs": "náhle žltnutie, nezvyčajný zápach po zahriatí, praskanie priadze, tvrdý krehký omak, zvlnenie goliera, farebný prenos, lepkavý povrch alebo dráždenie pokožky pri novom kuse",
    "professional_boundary": "Bežnú označenú easy-care košeľu možno prať doma, ale obleková košeľa s komplikovanou výstužou, neznámy vintage odev, silná reakcia pokožky alebo podozrenie na chybnú úpravu si vyžaduje údaje výrobcu, predajcu alebo odborníka.",
    "answer": "Easy-care, non-iron, wrinkle-resistant a durable-press bavlna sú názvy pre textílie upravené tak, aby sa po praní menej krčili a lepšie držali tvar. Zvyčajne ide o zosieťujúcu úpravu celulózy, nie o nový druh vlákna. Košeľu perte podľa etikety s primeranou dávkou, nepreplňte bubon, po skončení ju hneď vyberte, urovnajte a nepresušujte. Jemné dožehlenie môže byť stále potrebné. Viac tepla ani produktu neobnoví opotrebovanú úpravu a tvrdenie formaldehyde-free treba chápať ako informáciu o konkrétnej technológii, nie ako vlastnosť všetkých výrobkov.",
    "intro": "Nekrčivá košeľa sľubuje menej práce, no výraz nekrčivá často vytvára nesprávne očakávanie dokonale hladkého odevu za každých podmienok. Bavlna pri navlhčení stráca časť väzieb, ktoré držia jej dočasný tvar, a pri sušení si môže zafixovať nové záhyby. Funkčná úprava tento pohyb obmedzí, ale výsledok závisí od receptúry, konštrukcie, spôsobu prania a toho, ako rýchlo odev po cykle vyberiete. Dôležité je rozlíšiť úpravu od polyesterovej zmesi, chrániť namáhané miesta a nevytvárať zdravotné tvrdenia iba podľa obchodného názvu.",
    "quick": [
        "<strong>Easy-care nie je vlákno:</strong> najčastejšie ide o funkčnú úpravu bavlny alebo o kombináciu úpravy a zmesi.",
        "<strong>Non-iron neznamená bez jediného záhybu:</strong> výsledok ovplyvňuje náplň, odstreďovanie, sušenie a okamžité vybratie.",
        "<strong>Úprava má životnosť:</strong> trenie, opakované pranie, nevhodná chémia a teplo môžu jej účinok postupne znižovať.",
        "<strong>Golier a manžety starnú rýchlejšie:</strong> kombinujú pot, maz, oder, výstuž a časté lokálne čistenie.",
        "<strong>Chémia nie je pri každom výrobku rovnaká:</strong> tvrdenie bez formaldehydu sa musí viazať na konkrétnu technológiu alebo certifikáciu.",
        "<strong>Správne sušenie je rozhodujúce:</strong> preplnený bubon a čakanie po cykle zhoršia vzhľad aj kvalitnej úpravy.",
    ],
    "overview_heading": "Prečo sa bavlna krčí a ako easy-care úprava mení jej správanie",
    "overview": [
        "Celulóza v bavlnenom vlákne vytvára medzi reťazcami množstvo vodíkových väzieb. Keď textil navlhne, časť väzieb sa dočasne naruší, vlákna a priadze sa pri pohybe presunú a pri sušení sa nové usporiadanie zafixuje. Záhyb preto nie je iba povrchová čiara; je výsledkom zmeny polohy vlákien a väzieb v celej konštrukcii. Hustá košeľovina, jemná priadza a spôsob tkania ovplyvňujú, ako viditeľný bude.",
        "Durable-press úpravy vytvárajú medzi celulózovými reťazcami stabilnejšie prepojenia. Po vytvarovaní a vytvrdení má textil väčšiu schopnosť vrátiť sa k nastavenému vzhľadu. Rovnaký princíp môže pomáhať zachovať puky alebo tvar švu. Výrobný kompromis spočíva v tom, že silnejšie zosieťovanie môže ovplyvniť omak, savosť, odolnosť proti oderu či pevnosť, preto sa receptúra vyvažuje s požadovaným použitím.",
        "Výkon sa skúša po definovaných pracích a sušiacich cykloch a hodnotí sa vzhľad švov, hladkosť aj zachovanie záhybov. Domáce označenie neuvádza celý protokol. Dve košele s rovnakým slovom easy-care preto nemusia po desiatich praniach vyzerať rovnako. Podstatný je výrobca, deklarovaná úroveň, vláknová zmes, strih, šitie a dodržanie podmienok z etikety.",
    ],
    "table1_heading": "Easy-care, non-iron, wrinkle-resistant a syntetická zmes",
    "table1_intro": "Názvy sa v predaji prekrývajú. Tabuľka ukazuje, akú otázku si pri každom tvrdení položiť, nie univerzálny stupeň výkonu.",
    "table1_headers": ["Označenie", "Typický význam", "Možný omyl", "Čo overiť"],
    "table1_rows": [
        ("Easy-care", "Jednoduchšia údržba a lepší vzhľad po praní.", "Že odev netreba nikdy urovnať ani žehliť.", "Zloženie, pokyny na sušenie a deklarovaný výkon."),
        ("Wrinkle-resistant", "Zvýšená odolnosť proti pokrčeniu počas nosenia alebo prania.", "Že odolnosť je absolútna a trvalá.", "Počet cyklov, namáhané miesta a spôsob skúšky."),
        ("Non-iron", "Výrobca cieli na nositeľný vzhľad bez bežného žehlenia.", "Že horúca sušička vždy zlepší výsledok.", "Presný návod na pranie, odstreďovanie a vybratie."),
        ("Durable press", "Úprava má po praní zachovať hladkosť alebo nastavené puky.", "Že každá receptúra má rovnakú chémiu a vedľajšie vlastnosti.", "Technológiu, certifikáciu a limity hotového odevu."),
        ("Bavlna/polyester", "Nižšiu krčivosť môže priniesť vláknová zmes aj bez rovnakej úpravy.", "Že ide automaticky o chemicky upravenú čistú bavlnu.", "Percentá vlákien, teplotu a citlivosť syntetiky."),
    ],
    "sections": [
        {
            "heading": "Ako rozlíšiť nekrčivú úpravu od polyesterovej zmesi",
            "paragraphs": [
                "Najprv prečítajte percentá vlákien. Košeľa môže byť zo stopercentnej bavlny s funkčným dokončením, zo zmesi bavlny a polyesteru, alebo môže kombinovať oboje. Polyester obmedzuje prijímanie vody a pomáha tvarovej stabilite, no prináša inú citlivosť na vysokú teplotu a iné správanie pri mastnote. Samotný hladký omak ani rýchle schnutie nie sú spoľahlivým dôkazom.",
                f"Údaj o vláknach vysvetľuje základ, kým marketingový názov opisuje výkon. Prepojte ho s návodom <a href=\"{ARTICLE_COTTON}\">čo je bavlna a ako sa o ňu starať</a> a so symbolmi na konkrétnom kuse. Ak výrobca uvádza technológiu alebo certifikáciu, overte ju v jeho dokumentácii. Domáca skúška horením je na hotovom odeve nebezpečná a nerozlíši konkrétnu úpravu.",
            ],
        },
        {
            "heading": "Ako prať easy-care a non-iron košeľu v práčke",
            "paragraphs": [
                "Zapnite alebo uvoľnite zapínanie podľa konštrukcie, vyberte výstuže goliera, ak sú odnímateľné, a škvrny riešte pred cyklom. Zvoľte program zo štítku, primerané odstreďovanie a menšiu náplň než pri uterákoch. Košeľa potrebuje priestor, aby sa nezlisovala do záhybov. Perte ju s podobnými ľahkými farbami, nie so zipsami, džínsami a drsnými kusmi.",
                "Prací prostriedok dávkujte podľa vody a znečistenia. Nadbytok nezvýši nekrčivosť a môže zostať pri golieri či manžetách. Aviváž používajte iba vtedy, ak ju výrobca odevu aj produktu povoľuje; povlak môže meniť savosť a omak. Pri nejasnom štítku neexperimentujte s vysokou teplotou len preto, že ide o bavlnu.",
            ],
        },
        {
            "heading": "Golier, manžety a podpazušie: lokálne čistenie bez oslabenia povrchu",
            "paragraphs": [
                "Kožný maz, pot, kozmetika a trenie sa sústreďujú na malé plochy, ktoré sa zároveň často ohýbajú. Prípravok naneste v množstve a čase podľa návodu, bez tvrdej kefy a bez miešania chemikálií. Skúšku urobte na vnútornej strane lemu. Ak sa mení farba, povrch tvrdne alebo sa priadza strapká, nepokračujte silnejším zásahom.",
                f"Škvrnu určte podľa pôvodu; orientáciu poskytuje článok <a href=\"{ARTICLE_STAINS}\">ako odstrániť rôzne škvrny z oblečenia</a>. Žltý golier nemusí byť iba nečistota. Môže ísť o oxidovaný maz, zmenu farbiva, zvyšok produktu alebo starnutie úpravy. Bielenie bez diagnózy môže zvýrazniť kontrast medzi golierom a telom košele.",
            ],
        },
        {
            "heading": "Prečo sa nekrčivá košeľa po praní predsa pokrčí",
            "paragraphs": [
                "Najčastejšie je bubon preplnený, odstreďovanie príliš intenzívne alebo košeľa zostala po skončení stlačená pod ostatnou bielizňou. Záhyby sa počas chladnutia a schnutia stabilizujú. Úprava zlepšuje zotavenie, ale nemôže vyrovnať odev, ktorý bol celé hodiny stočený. Rovnako dôležité je, či výrobca hodnotil vzhľad po sušičke alebo po sušení na vešiaku.",
                "Po cykle košeľu pretraste jemným pohybom, urovnajte švy, golier, manžety a légu a zaveste ju na vhodne široký vešiak. Neťahajte za mokré rohy. Ak etiketa povoľuje sušičku, vyberte kus pri zodpovedajúcej zvyškovej vlhkosti a nenechajte ho preschnúť. Výsledok posudzujte až suchý a oblečený, nie zmačkaný v koši.",
            ],
            "callout": {
                "title": "Štyri podmienky hladšieho výsledku",
                "items": [
                    "Menšia náplň, v ktorej sa košeľa môže voľne pohybovať.",
                    "Odstreďovanie v hranici určenej etiketou a konštrukciou.",
                    "Okamžité vybratie po skončení cyklu.",
                    "Urovnanie švov a sušenie bez presušenia alebo stlačenia.",
                ],
                "background": "#f7fbf8",
                "border": "#dbe5de",
            },
        },
        {
            "heading": "Sušička a non-iron bavlna: teplo nie je univerzálna aktivácia",
            "paragraphs": [
                "Niektoré easy-care výrobky sú navrhnuté tak, aby po povolenom sušení v bubne dosiahli dobrý vzhľad, iné sušičku obmedzujú. Rozhoduje symbol. Vyššia teplota môže síce dočasne znížiť viditeľné záhyby, ale zároveň namáha farbivo, elastan, šijaciu niť, výstuž a samotnú úpravu. Presušenie zvyšuje statiku aj riziko pevných lomov.",
                f"Pri sušení na vzduchu zabezpečte priestor a cirkuláciu podľa návodu <a href=\"{ARTICLE_DRYING}\">ako sušiť bielizeň bez zatuchnutia</a>. Košeľu nevešajte na úzky drôt, ktorý vytvorí hrany na ramenách. Pri sušičke dodržte náplň a vyberte kus bez odkladu. Ak sa účinok úpravy znižuje, nepridávajte ďalšie teplo nad povolený limit.",
            ],
        },
        {
            "heading": "Treba nekrčivú košeľu žehliť a pri akej teplote",
            "paragraphs": [
                "Názov non-iron môže znamenať, že pri správnom postupe je odev prijateľný bez bežného žehlenia, nie že sa žehlička nesmie nikdy použiť. Riaďte sa symbolom. Jemné prežehlenie z rubu alebo cez ochrannú tkaninu pri najnižšej účinnej teplote môže upraviť golier a légu. Pri zmesi má prednosť limit syntetickej alebo elastickej zložky.",
                f"Podrobný sled dielov ponúka návod <a href=\"{ARTICLE_IRONING}\">ako vyžehliť košeľu</a>. Žehličku nenechávajte stáť, nevyvíjajte silný tlak na lesklé miesto a parou sa nesnažte opraviť chemicky oslabené vlákno. Ak povrch žltne, zapácha, lepí sa alebo mení lesk, okamžite teplo zastavte.",
            ],
        },
        {
            "heading": "Ako dlho vydrží easy-care úprava a podľa čoho spoznať jej úbytok",
            "paragraphs": [
                "Životnosť závisí od technológie, úrovne vytvrdenia, kvality bavlny, konštrukcie a počtu i podmienok cyklov. Výrobca môže výkon hodnotiť po definovanom počte praní, no domáce používanie nie je úplne rovnaké. Namáhané lakte, manžety a golier môžu stratiť hladkosť skôr než chrbát. Postupný úbytok sa prejaví dlhším sušením záhybov alebo potrebou ľahkého žehlenia.",
                "Opotrebovanie nie je dôvod pridávať koncentrovanú chémiu, škrob ani vysoké teplo bez odporúčania výrobcu. Porovnajte rovnaký kus pri rovnakom programe a po rovnakom spôsobe sušenia. Ak sa zmena objavila náhle po jednom cykle, skontrolujte teplotu, bielidlo, kontakt s iným prípravkom a mechanické poškodenie. Pri novom odeve si uchovajte údaje pre reklamáciu.",
            ],
        },
        {
            "heading": "Formaldehyd, tvrdenie formaldehyde-free a rozumné čítanie rizika",
            "paragraphs": [
                "Niektoré tradičné zosieťujúce systémy mohli uvoľňovať zvyškový formaldehyd, preto vývoj smeruje k receptúram s nízkym alebo nulovým uvoľňovaním a k alternatívnym chemickým cestám. Nemožno však tvrdiť, že každá easy-care košeľa používa rovnakú látku. Európska regulácia stanovuje limity emisií formaldehydu zo spotrebiteľských výrobkov a výrobcovia môžu poskytovať ďalšie certifikácie.",
                "Tvrdenie formaldehyde-free hodnotí konkrétny výrobok alebo technológiu podľa stanoveného kritéria; samo osebe nehovorí o všetkých farbivách, apretúrach ani o individuálnej tolerancii pokožky. Nový odev vyperte podľa etikety pred dlhým kontaktom, ak to výrobca odporúča. Pri pretrvávajúcom podráždení odev prestaňte nosiť a riešte zdravotnú otázku s lekárom, nie ďalším domácim chemickým pokusom.",
            ],
        },
        {
            "heading": "Citlivá pokožka a nový easy-care odev",
            "paragraphs": [
                f"Pred prvým nosením skontrolujte etiketu a odporúčanie výrobcu. Ak je pranie povolené, samostatný alebo farebne kompatibilný prvý cyklus odstráni voľný prach a časť zvyškov z výroby a balenia; postup opisuje článok <a href=\"{ARTICLE_NEW}\">ako prať nové oblečenie prvýkrát</a>. Nepoužívajte viac prostriedku a dôkladne opláchnite podľa možností práčky.",
                "Pocit svrbenia môže súvisieť s hrubým švom, potením, farbivom, zvyškom produktu, parfumáciou alebo úpravou a bez vyšetrenia sa príčina nedá určiť. Odev ďalej nenoste, ak reakcia pokračuje, a uchovajte názov, zloženie a údaje výrobcu. Článok nemá nahrádzať medicínsku diagnózu ani odporúčať neutralizovanie neznámej chémie.",
            ],
        },
        {
            "heading": "Ako vyberať nekrčivú košeľu podľa reálneho používania",
            "paragraphs": [
                "Na služobné cestovanie hľadajte jasný návod, skúšobnú deklaráciu, rovné švy a materiál, ktorý znáša plánovaný spôsob sušenia. Pri celodennom nosení zohľadnite priedušnosť, omak a savosť, nielen hladkosť na vešiaku. Hustejšia tkanina môže držať tvar, ale byť teplejšia. Zmes môže schnúť rýchlejšie, no vyžaduje nižšiu teplotu žehlenia.",
                "Pozrite si vnútro goliera, manžety a rezervu švov. Kvalitné spracovanie nemožno nahradiť funkčnou apretúrou. Pri nákupe cez internet hľadajte percentá vlákien, symboly a vysvetlenie tvrdenia non-iron. Ak výrobca neuvádza spôsob sušenia, je ťažké posúdiť, či výsledok dosiahnete vo vlastnej domácnosti.",
            ],
        },
        {
            "heading": "Kedy je zhoršený vzhľad bežné opotrebenie a kedy chyba",
            "paragraphs": [
                "Postupný úbytok hladkosti po mnohých cykloch môže patriť k životnosti úpravy. Náhle žltnutie, krehkosť, rozpad nite, zvlnenie výstuže alebo veľká farebná zmena po prvom povolenom praní je iná situácia. Pred ďalším zásahom odfoťte suchý odev pri rovnakom svetle, etiketu a nastavenie programu a uchovajte doklad.",
                "Neskúšajte chybu prekryť škrobom, bielidlom alebo opakovaným horúcim žehlením. Mohli by ste zmeniť dôkaz aj samotný materiál. Predajcovi popíšte, čo sa zmenilo a kde: hladkosť celej plochy, šev, farba, pevnosť alebo výstuž. Presný opis je užitočnejší než všeobecné tvrdenie, že košeľa už nie je non-iron.",
            ],
        },
    ],
    "table2_heading": "Easy-care košeľa po praní: prejavy a možné príčiny",
    "table2_intro": "Výsledok hodnotíte až po správnom vysušení. Viaceré javy vyzerajú podobne, ale vyžadujú odlišný ďalší krok.",
    "table2_headers": ["Prejav", "Možná príčina", "Čo overiť", "Bezpečný ďalší krok"],
    "table2_rows": [
        ("Celá košeľa je silno pokrčená", "Preplnenie, vysoké odstreďovanie, čakanie v bubne alebo slabší účinok úpravy.", "Náplň, program, čas vybratia a spôsob sušenia.", "Zopakovať iba povolený postup s menšou náplňou; neprekročiť teplo."),
        ("Golier žltne alebo tvrdne", "Maz, zvyšok produktu, teplo, výstuž alebo starnutie úpravy.", "Skrytý lem, zápach, históriu bielidla a teplotu.", "Zastaviť agresívnu chémiu a určiť príčinu."),
        ("Manžeta sa strapká", "Oder a pokles pevnosti pri opakovanom ohýbaní.", "Či sú nite pretrhnuté, nie iba znečistené.", "Ďalej nedrhnúť; opraviť alebo reklamovať."),
        ("Léga alebo golier sa vlní", "Rozdielna zmena vrchnej látky a lepená výstuž.", "Stav po úplnom vyschnutí a povolenie pary.", "Nežehliť silou; pri novom kuse zdokumentovať."),
        ("Odev dráždi pokožku", "Viac možných príčin vrátane zvyšku produktu, farbiva, švu alebo úpravy.", "Čas reakcie, miesto kontaktu a údaje výrobcu.", "Prestať nosiť; pri pretrvávaní konzultovať lekára."),
    ],
    "steps_heading": "Ako prať a sušiť easy-care košeľu krok za krokom",
    "steps": [
        "Prečítajte zloženie, funkčné tvrdenie a všetky symboly vrátane sušičky a žehlenia.",
        "Prezrite golier, manžety, podpazušie, švy, výstuž a zmeny farby ešte za sucha.",
        "Škvrnu ošetrite kompatibilným prípravkom po skrytej skúške a bez tvrdej kefy.",
        "Košeľu perte s podobnými ľahkými farbami v nepreplnenom bubne na povolenom programe.",
        "Dávkujte podľa vody a náplne; väčšie množstvo neobnoví funkčnú úpravu.",
        "Po skončení košeľu ihneď vyberte, urovnajte golier, manžety, légu a švy.",
        "Sušte iba povoleným spôsobom a vyhnite sa presušeniu alebo dlhému stlačeniu.",
        "Ak treba, žehlite pri najnižšej účinnej teplote; nezvyčajnú zmenu zdokumentujte.",
    ],
    "remember": [
        "Je odev z čistej bavlny s úpravou, zo zmesi alebo z kombinácie oboch?",
        "Čo presne výrobca sľubuje pod názvom easy-care alebo non-iron?",
        "Povoľuje etiketa sušičku, paru a akú teplotu žehlenia?",
        "Sú golier, manžety a lakte iba znečistené, alebo už mechanicky poškodené?",
        "Vyberiete košeľu z bubna hneď a máte priestor na správne urovnanie?",
        "Objavila sa zmena postupne, alebo náhle po jednom konkrétnom cykle?",
    ],
    "mistakes": [
        "Predpokladať, že non-iron znamená nulové záhyby bez ohľadu na náplň a sušenie.",
        "Prať košeľu v preplnenom bubne s džínsami, zipsami a uterákmi.",
        "Nechať ju po cykle hodiny stlačenú a očakávať automatické vyrovnanie.",
        "Použiť vyššiu teplotu ako pokus o obnovenie opotrebovanej úpravy.",
        "Drhnúť žltý golier bez rozlíšenia mazu, zvyšku, farbiva a degradácie.",
        "Tvrdiť, že každá easy-care technológia má rovnakú chémiu alebo zdravotný profil.",
    ],
    "expert_heading": "Odbornejší pohľad: zosieťovanie celulózy, výkon a regulačný kontext",
    "expert": [
        "Durable-press úprava vytvára priečne väzby medzi celulózovými reťazcami, čím obmedzuje ich relatívny pohyb po navlhčení a pomáha textilu vrátiť sa k nastavenému tvaru. Účinok závisí od molekuly, katalyzátora, pH, naneseného množstva, sušenia a vytvrdenia. Silnejšia reakcia nemusí byť automaticky lepšia, pretože výrobca vyvažuje zotavenie zo záhybu s pevnosťou, oderom, omakom a savosťou.",
        "CottonWorks opisuje durable press aj novšie technológie s odlišným profilom uvoľňovania formaldehydu. Z toho nemožno odvodiť chémiu neoznačeného odevu. Európska komisia zaviedla emisné limity formaldehydu pre spotrebiteľské výrobky. Regulačný kontext je dôvod požadovať technické údaje a rešpektovať označenie, nie dôvod diagnostikovať reakciu podľa názvu košele.",
        "AATCC používa samostatné metódy na hodnotenie hladkosti po domácom praní, vzhľadu švov a zachovania pukov. GINETEX symboly určujú maximálne dovolené ošetrenie hotového výrobku. Dobrý výsledok preto vzniká spojením overenej úpravy a reprodukovateľného cyklu: primeranej náplne, definovaného odstreďovania, vhodného sušenia a včasného vybratia.",
    ],
    "source_intro": "Zdroje vysvetľujú princíp durable-press úprav, skúšanie vzhľadu, symboly a európsky regulačný kontext. Nepodporujú tvrdenie, že každá nekrčivá košeľa používa rovnakú chémiu alebo nikdy nepotrebuje žehlenie.",
    "sources": [
        ("CottonWorks: durable press", COTTON_DURABLE_PRESS),
        ("CottonWorks: PUREPRESS technology", COTTON_PUREPRESS),
        ("AATCC: prehľad štandardov pre textilné skúšky", AATCC_STANDARDS),
        ("GINETEX: symboly ošetrovania", GINETEX),
        ("Európska komisia: obmedzenie emisií formaldehydu", EU_FORMALDEHYDE),
    ],
    "related": [
        ("Ako správne vyžehliť košeľu", ARTICLE_IRONING),
        ("Ako čítať štítok na oblečení", ARTICLE_LABEL),
        ("Čo je bavlna", ARTICLE_COTTON),
        ("Ako prať nové oblečenie prvýkrát", ARTICLE_NEW),
        ("Ako odstrániť rôzne škvrny", ARTICLE_STAINS),
        ("Ako sušiť bielizeň bez zatuchnutia", ARTICLE_DRYING),
    ],
    "faq_title": "nekrčivá bavlna a easy-care úprava",
    "faq": [
        ("Čo znamená easy-care bavlna?", "Bavlnený textil alebo zmes je navrhnutá či upravená tak, aby sa jednoduchšie udržiavala a po praní si lepšie zachovala hladký vzhľad."),
        ("Je non-iron košeľa zo stopercentnej bavlny?", "Môže byť, ale nemusí. Skontrolujte percentá vlákien; hladkosť môže podporovať úprava, syntetická zmes alebo oboje."),
        ("Musí sa nekrčivá košeľa žehliť?", "Pri správnom praní a sušení často stačí urovnanie, no jemné dožehlenie môže byť potrebné. Rozhoduje výrobok a požadovaný vzhľad."),
        ("Ako prať non-iron košeľu?", "Podľa etikety, v nepreplnenom bubne s podobnými ľahkými farbami, primeranou dávkou a okamžitým vybratím po cykle."),
        ("Môže ísť easy-care košeľa do sušičky?", "Iba pri povolenom symbole. Dodržte teplotu, náplň a vyberte ju bez odkladu, aby sa nepresušila."),
        ("Prečo je non-iron košeľa pokrčená?", "Častou príčinou je preplnenie, silné odstreďovanie, čakanie v bubne, nesprávne sušenie alebo postupný úbytok účinku úpravy."),
        ("Vydrží easy-care úprava navždy?", "Nie nevyhnutne. Jej výkon môže klesať trením, opakovaným praním, nevhodnou chémiou a teplom."),
        ("Dá sa nekrčivá úprava obnoviť väčšou dávkou gélu?", "Nie. Prací prostriedok čistí, ale nevytvorí späť priemyselné zosieťovanie celulózy."),
        ("Obsahuje každá nekrčivá košeľa formaldehyd?", "Také tvrdenie nie je správne. Technológie a receptúry sa líšia; overujte údaje konkrétneho výrobcu alebo certifikácie."),
        ("Čo znamená formaldehyde-free?", "Je to tvrdenie viazané na konkrétny výrobok, technológiu alebo skúšobný limit. Neopisuje automaticky všetky ostatné chemické zložky."),
        ("Prečo žltne golier easy-care košele?", "Môže ísť o maz, zvyšok produktu, teplo, farbivo, výstuž alebo starnutie úpravy. Najprv treba odlíšiť príčinu."),
        ("Je aviváž vhodná na non-iron košeľu?", "Použite ju iba vtedy, ak ju povoľuje výrobca odevu aj produktu. Povlak môže meniť savosť a omak."),
        ("Čo robiť, ak nový odev dráždi pokožku?", "Prestaňte ho nosiť, uchovajte údaje o výrobku a pri pretrvávajúcej reakcii sa poraďte s lekárom; príčinu neurčujte domácim chemickým testom."),
    ],
}

add_cards(
    EASY_CARE,
    product_heading="Prací gél pre bežnú prateľnú easy-care košeľu",
    product_limit="Produkt čistí kompatibilný odev, ale neobnovuje priemyselnú nekrčivú úpravu a nenahrádza obmedzenia pre elastan, farbivo, potlač ani výstuž.",
)


BIOPOLISHED: dict[str, object] = {
    "title": "Čo je bioleštená bavlna: enzýmová úprava, žmolky a pranie",
    "link": "co-je-biolestena-bavlna-enzymova-uprava-zmolky-a-pranie",
    "meta": "Čo znamená bioleštená bavlna, ako celuláza odstraňuje povrchové mikrovlákna, čo dokáže proti žmolkom a ako hotový textil správne prať.",
    "short": "Bioleštenie je priemyselne riadená enzýmová úprava celulózového textilu, ktorá odstraňuje časť vyčnievajúcich mikrovlákien a zjemňuje povrch. Neznamená trvalú odolnosť proti žmolkom ani návod pridávať enzýmy doma.",
    "name": "bioleštená bavlna",
    "locative": "bioleštenej bavlne",
    "identity_heading": "Bioleštenie je dokončovací proces na povrchu celulózového vlákna",
    "identity_detail": "Celulázové enzýmy za riadeného pH, teploty, času a mechaniky hydrolyzujú dostupné celulózové mikrovlákna, ktoré vyčnievajú z priadzí, a uvoľnený materiál sa následne z povrchu odstráni.",
    "identity_boundary": "Hotový bioleštený textil už prešiel priemyselným procesom; spotrebiteľský enzymatický prací prostriedok pracuje v inej koncentrácii a podmienkach a nemožno ho vydávať za domácu obnovu bioleštenia.",
    "label_focus": "presné vláknové zloženie, úplet alebo tkaninu, farbivo, potlač, elastan, výšivku, deklaráciu bio-polished, povolenú teplotu, mechaniku, sušičku a žehlenie",
    "missing_label": "Bez údajov výrobcu nemožno hladký povrch spoľahlivo odlíšiť od mercerizácie, opaľovania chĺpkov, kalandrovania, silikónového zmäkčenia alebo prirodzene jemnej priadze.",
    "dry_check": "rovnomernosť povrchu, jemný chlp, žmolky, tenké miesta, odreté lakte, strapkajúci sa šev, prasknutú potlač, farebný rozdiel a zvyšky zachytené na povrchu",
    "damage_boundary": "Voľný žmolok možno odstrániť, ale stenčená priadza, diera, zodratý reliéf alebo chemicky oslabené vlákno sa ďalším enzymatickým zásahom neopraví.",
    "test_focus": "Na skrytom mieste sledujte nielen farbu a škvrnu, ale aj uvoľnené vlákna, zmenu omaku, matnenie a pokles pevnosti po úplnom vysušení.",
    "combined_risk": "oderu medzi kusmi, ďalšieho uvoľňovania povrchových vlákien, zachytenia o zipsy, vysokej enzymatickej aktivity pri nevhodných podmienkach a tepelného namáhania zmesi",
    "chemistry_boundary": "Bielidlo, odstraňovač škvŕn a enzýmový detergent majú odlišný účel; ich kombinovanie alebo predlžovanie kontaktu nad návod môže oslabiť farbu a povrch namiesto opravy žmolkov.",
    "drying_detail": "Úplet urovnajte bez zavesenia za jeden bod, tričko sušte podľa symbolu a poťah otvorte v rohoch; povrch nekefujte a nežmolkujte, kým je mokrý a zraniteľnejší.",
    "heat_boundary": "Horúci bubon môže zraziť bavlnu, poškodiť elastan a potlač alebo zvýrazniť oder; teplo nevracia odstránené mikrovlákna ani nenahrádza chýbajúcu pevnosť.",
    "stop_signs": "nadmerné púšťanie vlákien, rýchlo rastúce tenké miesto, dierka, silný prenos farby, drsný krehký omak, rozpad potlače alebo opakované žmolkovanie po veľmi krátkom nosení",
    "professional_boundary": "Bežné označené bioleštené tričko možno prať doma, ale hodnotný jemný úplet, nejasná zmes, lokálne stenčenie, priemyselné nastavenie procesu alebo spor o kvalitu patria výrobcovi, laboratóriu či textilnému odborníkovi.",
    "answer": "Bioleštená bavlna prešla kontrolovanou úpravou celulázou, ktorá odstránila časť vyčnievajúcich mikrovlákien. Povrch môže byť hladší, čistejší a menej náchylný na počiatočné žmolkovanie, no odev sa stále môže odierať, zachytiť a časom vytvoriť žmolky. Perte ho podľa etikety s podobne jemnými kusmi, bez preplnenia a bez zipsov či suchých zipsov. Nepokúšajte sa proces zopakovať domácim pridávaním enzýmov. Nadmerné priemyselné bioleštenie môže znížiť hmotnosť alebo pevnosť, preto hladší povrch nie je automaticky dôkazom vyššej životnosti.",
    "intro": "Slovo bioleštená pôsobí ako ekologický názov alebo sľub textilu, ktorý sa nikdy nežmolkuje. Technicky však opisuje enzymatické povrchové dokončenie celulózových materiálov. Celuláza pracuje s dostupnými časťami bavlneného vlákna a pri správnom riadení odstráni jemný chlp bez neprimeraného zásahu do nosnej konštrukcie. Hranica medzi užitočným vyčistením povrchu a stratou hmotnosti či pevnosti závisí od procesu. Spotrebiteľ preto potrebuje vedieť, čo úprava dokáže, čo už nedokáže a ako hotový odev chrániť pred ďalším oderom.",
    "quick": [
        "<strong>Bioleštenie využíva celulázu:</strong> enzým cieli na prístupné celulózové mikrovlákna na povrchu.",
        "<strong>Ide o výrobný proces:</strong> pH, teplota, čas, dávka, pohyb a deaktivácia sa priemyselne kontrolujú.",
        "<strong>Povrch môže byť hladší:</strong> odstránenie chĺpkov zlepšuje čistotu vzhľadu a môže obmedziť počiatočný pilling.",
        "<strong>Nie je to imunita proti žmolkom:</strong> trenie, krátke vlákna, slabá priadza a nevhodné pranie ostávajú dôležité.",
        "<strong>Viac enzýmu nie je lepšie:</strong> príliš silný zásah môže viesť k strate hmotnosti a pevnosti.",
        "<strong>Domáce pranie má udržiavať, nie znovu vyrábať:</strong> spotrebiteľ nemá napodobňovať priemyselnú úpravu.",
    ],
    "overview_heading": "Ako celuláza mení povrch bavlnenej tkaniny alebo úpletu",
    "overview": [
        "Bavlnená priadza nie je dokonale hladký valec. Z jej povrchu vyčnievajú konce vlákien a jemné fibrily, ktoré rozptyľujú svetlo, zachytávajú sa a pri trení sa môžu zapliesť do žmolkov. Pri bioleštení celuláza rozkladá prístupné beta-väzby v celulóze najmä na týchto vyčnievajúcich častiach. Mechanický pohyb potom pomáha oslabené mikrovlákna oddeliť a odplaviť.",
        "Proces musí mať riadenú kyslosť alebo neutralitu podľa typu enzýmu, vhodnú teplotu, čas, pomer kúpeľa a mechaniku. Po dosiahnutí cieľa sa aktivita zastaví zmenou podmienok a materiál sa opláchne. Ak zásah pokračuje príliš dlho alebo je príliš intenzívny, enzým nepozná obchodný sľub a môže zasiahnuť viac celulózy, čo sa prejaví stratou hmotnosti, pevnosti alebo zmenou rozmeru.",
        "Výsledok sa posudzuje kombináciou vzhľadu, omaku, žmolkovania, hmotnosti a mechanických vlastností. Výskum ukazuje, že podmienky výrazne menia kompromis medzi hladkosťou a zachovaním pevnosti. Preto nemožno z jedného lesklého povrchu určiť kvalitu procesu a nemožno odporučiť univerzálnu domácu dávku enzýmu na všetky bavlnené výrobky.",
    ],
    "table1_heading": "Bioleštenie a podobné spôsoby zjemnenia povrchu",
    "table1_intro": "Hladký alebo mäkký bavlnený povrch môže vzniknúť viacerými cestami. Správne pomenovanie pomáha nastaviť realistické očakávanie.",
    "table1_headers": ["Proces alebo vlastnosť", "Ako pôsobí", "Typický výsledok", "Dôležitá hranica"],
    "table1_rows": [
        ("Bioleštenie", "Celuláza kontrolovane odstraňuje vyčnievajúce celulózové mikrovlákna.", "Čistejší, hladší povrch a nižší počiatočný chlp.", "Nadmerný zásah môže znížiť hmotnosť a pevnosť."),
        ("Opaľovanie chĺpkov", "Krátky plameň tepelne odstráni povrchové vlákna.", "Menej chĺpkov pred ďalším spracovaním.", "Je to tepelný, nie enzymatický proces."),
        ("Mercerizácia", "Bavlna sa za kontrolovaných podmienok spracuje zásadou a napätím.", "Zmena lesku, prijímania farbiva a vlastností vlákna.", "Nie je synonymom bioleštenia."),
        ("Kalandrovanie", "Valce pôsobia tlakom a niekedy teplom.", "Hladší alebo lesklejší povrch.", "Efekt môže byť povrchový a citlivý na ďalšie ošetrenie."),
        ("Jemná česaná priadza", "Výber a usporiadanie dlhších vlákien znižujú voľné konce už v priadzi.", "Rovnomernejší základ bez rovnakého dokončovacieho kroku.", "Kvalita priadze a bioleštenie sa môžu kombinovať."),
    ],
    "sections": [
        {
            "heading": "Čo presne robí celuláza a prečo neodstráni iba hotové žmolky",
            "paragraphs": [
                "Celuláza je skupina enzýmov, ktoré štiepia celulózu. Pri správnom procese sú najprístupnejšie jemné povrchové časti, preto sa odstráni chlp a povrch sa opticky vyčistí. Enzým však nerozoznáva, ktorá celulóza je esteticky nežiaduca a ktorá nesie zaťaženie. Selektivitu vytvárajú podmienky, krátky kontrolovaný čas a prístupnosť povrchu, nie absolútna biologická hranica.",
                "Hotový žmolok je spleť vlákien, ktorá môže obsahovať pevne ukotvené konce, nečistotu a syntetickú zložku. Priemyselné bioleštenie sa často používa pred vznikom takéhoto poškodenia. Domáce opakované enzymatické pranie nie je presný depilačný nástroj a na zmesi s polyesterom môže bavlnenú časť oslabiť, kým syntetické jadro žmolku zostane.",
            ],
        },
        {
            "heading": "Bioleštená bavlna a žmolky: čo sa zlepší a čo ostáva",
            "paragraphs": [
                "Zníženie voľného povrchového chĺpku obmedzí materiál, ktorý sa môže v prvých cykloch zapliesť, a farba môže pôsobiť sýtejšie, pretože svetlo sa menej rozptyľuje. Úprava však nezmení krátku nekvalitnú priadzu na dlhé pevné vlákno. Oder pod pazuchou, popruh tašky, bezpečnostný pás a trenie v bubne naďalej uvoľňujú nové konce.",
                f"Mechanizmy všeobecného pillingu podrobne vysvetľuje článok <a href=\"{ARTICLE_PILLING}\">prečo sa oblečenie žmolkuje</a>. Pri hodnotení biolešteného kusa sledujte, či ide o voľné chumáčiky z cudzej bielizne, skutočný uzlík ukotvený v priadzi alebo stenčenie povrchu. Každý jav potrebuje iný postup a žiadny neoprávňuje k neobmedzenému enzymatickému zásahu.",
            ],
        },
        {
            "heading": "Ako prať bioleštené bavlnené tričko",
            "paragraphs": [
                f"Začnite štítkom a zložením podľa návodu <a href=\"{ARTICLE_LABEL}\">ako čítať údaje na oblečení</a>. Tričko otočte naruby, ak to vyhovuje potlači a odporúčaniu výrobcu, a perte ho s podobne farebnými ľahkými kusmi. Zipsy zatvorte, suchý zips izolujte a hrubé uteráky oddeľte. Mechanické trenie je pre povrch často dôležitejšie než samotné slovo bioleštené.",
                "Použite program, teplotu a odstreďovanie zo symbolov. Dávku neprekračujte a koncentrovaný gél nelejte na suchý textil. Ak prací prostriedok obsahuje enzýmy, používajte ho iba podľa jeho určenia a etikety odevu; nie preto, aby ste doma zopakovali výrobný proces. Po cykle tričko vyberte, urovnajte bez krútenia a sušte povoleným spôsobom.",
            ],
        },
        {
            "heading": "Je enzýmový prací prostriedok vhodný na bioleštenú bavlnu",
            "paragraphs": [
                "Enzýmový detergent môže obsahovať proteázy, amylázy, lipázy, celulázy alebo ich kombináciu v spotrebiteľskej koncentrácii. Je navrhnutý na konkrétne typy škvŕn a podmienky prania, nie na priemyselné dokončenie textilu. Vhodnosť určuje výrobca produktu a ošetrovací štítok. Bioleštenie samo osebe nie je príkaz používať ani zákaz používať každý enzýmový gél.",
                "Dôležitá je dávka, teplota a kontakt. Nenechávajte koncentrovaný prípravok na suchom farebnom povrchu a nepredlžujte namáčanie nad návod. Na vlnu a hodváb platia iné hranice než na bavlnu. Ak odev obsahuje citlivú výšivku, potlač, elastan alebo zmes, rozhoduje najcitlivejšia súčasť.",
            ],
        },
        {
            "heading": "Prečo si bioleštenie neskúšať vyrobiť doma",
            "paragraphs": [
                "Priemyselný proces kontroluje aktivitu enzýmu cez pH, teplotu, čas, koncentráciu, mechaniku a následnú deaktiváciu. Domáca práčka neposkytuje rovnaké meranie ani rovnomerný kontakt a spotrebiteľ nepozná rezervu pevnosti konkrétnej priadze. Opakovanie cyklov môže vytvoriť nerovnomernú stratu hmotnosti, poškodiť šev alebo zosvetliť namáhané miesta.",
                "Domáce produkty označené ako odžmolkovače môžu fungovať mechanicky alebo chemicky a treba ich hodnotiť podľa vlastného návodu. Nepridávajte laboratórny či priemyselný enzým do práčky a nekombinujte ho s neznámou chémiou. Ak potrebujete hladší vzhľad, bezpečnejšie je predchádzať treniu a suché žmolky odstrániť primeraným mechanickým nástrojom.",
            ],
            "callout": {
                "title": "Prečo je výrobný proces ťažké zopakovať",
                "items": [
                    "Aktivita celulázy prudko závisí od pH a teploty.",
                    "Čas a mechanika určujú, koľko oslabených vlákien sa odstráni.",
                    "Proces musí byť rovnomerný po celej ploche a včas zastavený.",
                    "Výsledok sa kontroluje aj hmotnosťou a pevnosťou, nielen dotykom.",
                ],
                "background": "#fffaf5",
                "border": "#e6ded2",
            },
        },
        {
            "heading": "Ako odstrániť žmolky bez stenčenia biolešteného povrchu",
            "paragraphs": [
                "Odev musí byť čistý, úplne suchý a položený na pevnej rovnej podložke. Najprv odstráňte voľné vlákna lepiacim valčekom s miernym účinkom. Skutočné žmolky zastrihnite alebo ohoľte textilným strojčekom nastaveným tak, aby nezasiahol nosnú priadzu. Pracujte pri dobrom svetle, na malej ploche a mimo švov, potlače a tenkých miest.",
                "Čepeľ netlačte a nevracajte sa opakovane na rovnaké miesto. Ak sa povrch dvíha v celých slučkách, vidno mriežku úpletu alebo vzniká priehľadná plocha, zastavte. Odstránenie uzlíka zlepší vzhľad, ale nevráti stratený materiál. Častá potreba holenia je signálom oderu, nevhodného prania alebo slabej konštrukcie, nie výzvou k agresívnejšiemu zásahu.",
            ],
        },
        {
            "heading": "Farba a lesk po bioleštení",
            "paragraphs": [
                "Po odstránení jemného chĺpku sa svetlo rozptyľuje menej a povrch môže pôsobiť čistejšie a sýtejšie bez toho, aby obsahoval viac farbiva. Tento optický efekt je jeden z dôvodov použitia bioleštenia. Pri neskoršom odere sa povrch opäť zmatní, pretože vznikajú nové voľné konce. Nejde automaticky o chemické vyblednutie.",
                f"Skutočnú stálofarebnosť treba hodnotiť oddelene podľa zásad článku <a href=\"{ARTICLE_COLOR}\">prečo farby blednú pri praní, svetle a trení</a>. Ak farba prechádza na bielu vlhkú handričku, riešte farbivo a podmienky prania. Ak sa mení iba odraz podľa uhla a povrch je chlpatý, pravdepodobnejší je mechanický jav. Bielidlo hladkosť neobnoví.",
            ],
        },
        {
            "heading": "Úplet, tkanina, viskóza a zmesi: názov procesu nestačí",
            "paragraphs": [
                "Bioleštenie sa používa na bavlnené úplety aj tkaniny a môže sa aplikovať aj na ďalšie celulózové materiály. Voľný úplet má inú mechaniku než hustá košeľovina. Viskóza môže mať odlišnú mokrú pevnosť a fibriláciu. Pri zmesi bavlny s polyesterom enzým pôsobí na celulózovú časť, kým syntetická zložka zostáva a môže držať žmolok pri povrchu.",
                "Ošetrenie preto vyberajte podľa celého zloženia a konštrukcie. Jemný úplet nesušte zavesený za ramená, ak by sa vytiahol, a hrubú tkaninu neperte so zipsami. Povrchová hladkosť nehovorí o pevnosti švu, rozmerovej stabilite ani o kompatibilite potlače. Každú vlastnosť treba posudzovať samostatne.",
            ],
        },
        {
            "heading": "Strata hmotnosti a pevnosti: kedy je proces príliš intenzívny",
            "paragraphs": [
                "Výskumné práce pri bioleštení sledujú percento úbytku hmotnosti, pevnosť, žmolkovanie a vzhľad. Určitý úbytok zodpovedá odstráneným povrchovým vláknam, no pri rastúcej intenzite sa môže znížiť pevnosť. Optimálny bod preto nie je maximálne hladký povrch za každú cenu, ale kompromis, pri ktorom sa dosiahne vzhľad bez neprijateľného oslabenia.",
                "Spotrebiteľ nevie úbytok zmerať iba dotykom. Varovné sú priehľadné miesta, rýchly vznik dierok, rozsiahle púšťanie vlákien a rozpad pri šve. Odev ďalej nehoľte, nenamáčajte v enzýme a neperte s hrubou bielizňou. Pri novom výrobku stav zdokumentujte a riešte s predajcom; pri staršom môže ísť o kombináciu výroby a opotrebenia.",
            ],
        },
        {
            "heading": "Sušenie, žehlenie a skladovanie hladkého bavlneného povrchu",
            "paragraphs": [
                f"Sušte podľa etikety a s voľným prúdením vzduchu, ako vysvetľuje návod <a href=\"{ARTICLE_DRYING}\">ako sušiť bielizeň bez zatuchnutia</a>. Jemný úplet položte alebo podoprite tak, aby sa nevytiahol. Pri povolenej sušičke obmedzte presušenie a kontakt s drsnými kusmi. Mokré chĺpky nekefujte a žmolky neodstraňujte, kým sa štruktúra úplne nestabilizuje.",
                "Žehlite pri teplote najcitlivejšej zložky a z rubu pri potlači. Silný tlak môže vytvoriť lesklú mapu, ktorá sa mylne považuje za lepšie bioleštenie. Čistý suchý odev skladujte bez ostrého lomu a trenia o hrubé povrchy. Pri dlhom nosení striedajte kusy, aby sa namáhané zóny zotavili a neboli stále vystavené rovnakému oderu.",
            ],
        },
        {
            "heading": "Ako posúdiť kvalitu biolešteného trička pri kúpe",
            "paragraphs": [
                "Pozrite povrch proti svetlu, jemne prejdite čistou rukou a skontrolujte vnútro švov. Hladkosť má byť rovnomerná, bez lysých pásov, voľných chumáčov a priehľadných miest. Ohnite materiál bez násilného ťahu a sledujte, či sa priadza nerozostupuje. Jasná etiketa a údaje o zložení sú dôležitejšie než samotné slovo bio.",
                "Kvalitu nemožno určiť iba leskom. Jemná dlhá priadza, hustota, väzba, šitie, farbenie a správna intenzita procesu sa podieľajú na životnosti. Ak predajca tvrdí úplnú odolnosť proti žmolkom alebo opravu vlákna enzýmom, ide o prehnané očakávanie. Realistický prínos je čistejší počiatočný povrch a potenciálne lepšie správanie pri pillingu za vhodnej starostlivosti.",
            ],
        },
    ],
    "table2_heading": "Bioleštená bavlna po praní: povrchové zmeny a ich význam",
    "table2_intro": "Povrch hodnotíte suchý pri bočnom svetle. Rozlíšte cudzie vlákna, žmolok, optické zmatnenie a skutočnú stratu nosného materiálu.",
    "table2_headers": ["Prejav", "Možná príčina", "Ako overiť", "Bezpečný ďalší krok"],
    "table2_rows": [
        ("Jemné voľné chĺpky", "Oder alebo vlákna prenesené z inej bielizne.", "Skúsiť jemný valček a pozrieť ukotvenie.", "Oddeliť od uterákov a znížiť trenie."),
        ("Pevné malé žmolky", "Zapletené povrchové vlákna a opakovaný oder.", "Prezrieť namáhané zóny a zloženie zmesi.", "Odstrániť na suchom kuse šetrným strojčekom."),
        ("Matná plocha bez uzlíkov", "Nový chlp, fibrilácia alebo zmena odrazu.", "Porovnať smer svetla a skryté miesto.", "Nedrhnúť ani nebieliť; upraviť pranie."),
        ("Priehľadné alebo tenké miesto", "Strata nosných vlákien, silný oder alebo oslabenie.", "Skontrolovať mriežku a pevnosť pri šve.", "Zastaviť holenie a ďalšiu enzymatickú záťaž."),
        ("Farba prechádza na handričku", "Nedostatočná stálofarebnosť, nie samotné bioleštenie.", "Urobiť skrytú skúšku za podmienok výrobcu.", "Oddeliť farby a pri novom kuse zdokumentovať."),
    ],
    "steps_heading": "Ako ošetrovať bioleštenú bavlnu krok za krokom",
    "steps": [
        "Overte zloženie, konštrukciu, potlač a všetky ošetrovacie symboly.",
        "Prezrite povrch, švy, žmolky a tenké miesta ešte za sucha pri bočnom svetle.",
        "Škvrnu riešte podľa pôvodu a kompatibility, nie predlžovaním náhodného enzymatického kontaktu.",
        "Odev oddeľte od zipsov, suchých zipsov, hrubých uterákov a kusov púšťajúcich vlákna.",
        "Perte v nepreplnenom bubne na povolenom programe s presne odmeraným prostriedkom.",
        "Po cykle kus podoprite, bez krútenia urovnajte a sušte iba povoleným spôsobom.",
        "Žmolky odstraňujte až na úplne suchom povrchu a zastavte pri stenčení alebo slučkách.",
        "Pri rýchlom rozpade, strate pevnosti alebo farebnom prenose zdokumentujte stav a nepokračujte silnejšie.",
    ],
    "remember": [
        "Je deklarované skutočné bioleštenie alebo iba hladký omak bez technického údaja?",
        "Ide o čistú bavlnu, viskózu, úplet, tkaninu alebo zmes so syntetikou?",
        "Sú na povrchu cudzie vlákna, žmolky, nový chlp alebo stenčená nosná priadza?",
        "Povoľuje etiketa použitý prostriedok, teplotu, odstreďovanie a sušičku?",
        "Je odev oddelený od zipsov, suchého zipsu a hrubých slučkových textílií?",
        "Nezamieňate spotrebiteľské enzymatické pranie s priemyselným bioleštením?",
    ],
    "mistakes": [
        "Považovať slovo bio za dôkaz ekologickej certifikácie alebo nulového vplyvu.",
        "Sľubovať, že bioleštený textil sa nikdy nebude žmolkovať.",
        "Pridávať priemyselnú celulázu do domácej práčky bez kontroly procesu.",
        "Prať hladké tričko so zipsami, suchým zipsom a hrubými uterákmi.",
        "Holiť mokrý alebo stenčený povrch a zasiahnuť nosné slučky.",
        "Zamieňať optické zmatnenie s vyblednutím a reagovať silným bielidlom.",
    ],
    "expert_heading": "Odbornejší pohľad: enzymatická hydrolýza, strata hmotnosti a pevnosť",
    "expert": [
        "Celulázy zahŕňajú enzýmové aktivity, ktoré sprístupňujú, štiepia a dokončujú rozklad celulózových reťazcov. V textilnom bioleštení sa využíva väčšia prístupnosť povrchových fibríl a mechanické odstránenie oslabených častí. Aktivita závisí od typu enzýmu, pH, teploty, času, pomeru kúpeľa a mechaniky. Preto sa výsledok nedá opísať iba dávkou produktu.",
        "Recenzované štúdie sledujú hladkosť a pilling spolu so stratou hmotnosti a pevnosťou. Zlepšenie jedného parametra môže pri rastúcej intenzite zhoršiť iný. Moderné postupy skúmajú účinnejšie alebo cielenejšie systémy, no základná potreba proces ukončiť a overiť mechanické vlastnosti ostáva. Spotrebiteľský článok preto nesmie premeniť priemyselnú metódu na domácu receptúru.",
        "AATCC ponúka samostatné metódy pre pilling, vzhľad a pevnosť a GINETEX stanovuje hranice domáceho ošetrovania hotového výrobku. Označenie bioleštené hovorí o minulom dokončovacom kroku, nie o budúcej nezničiteľnosti. Životnosť naďalej určujú priadza, konštrukcia, farbivo, šitie, používanie a mechanika každého prania.",
    ],
    "source_intro": "Zdroje opisujú mechanizmus celulázy, prínosy aj kompromis medzi hladkosťou, stratou hmotnosti a pevnosťou. Nepodporujú domácu výrobu bioleštenia ani záruku nulového žmolkovania.",
    "sources": [
        ("Peer-reviewed review: cellulases in textile processing", BIOPOLISH_REVIEW),
        ("PubMed: enzymatic biopolishing and cotton properties", BIOPOLISH_STUDY),
        ("Recent open-access study: controlled cotton biopolishing", BIOPOLISH_RECENT),
        ("AATCC: prehľad skúšobných štandardov", AATCC_STANDARDS),
        ("GINETEX: symboly ošetrovania", GINETEX),
        ("EÚ 1007/2011: názvy textilných vlákien", EU_FIBRE_LABEL),
    ],
    "related": [
        ("Prečo sa oblečenie žmolkuje", ARTICLE_PILLING),
        ("Čo je bavlna", ARTICLE_COTTON),
        ("Ako čítať štítok na oblečení", ARTICLE_LABEL),
        ("Prečo farby blednú", ARTICLE_COLOR),
        ("Ako odstrániť rôzne škvrny", ARTICLE_STAINS),
        ("Ako sušiť bielizeň bez zatuchnutia", ARTICLE_DRYING),
    ],
    "faq_title": "bioleštená bavlna a enzýmová úprava",
    "faq": [
        ("Čo je bioleštená bavlna?", "Bavlnený textil, ktorého povrch bol kontrolovane ošetrený celulázou s cieľom odstrániť časť vyčnievajúcich mikrovlákien."),
        ("Je bioleštenie ekologická certifikácia?", "Nie. Názov opisuje enzymatický proces; sám osebe nepotvrdzuje pôvod suroviny, celkový vplyv ani certifikáciu."),
        ("Prečo je bioleštený povrch hladší?", "Odstráni sa časť jemných vyčnievajúcich vlákien, takže povrch menej rozptyľuje svetlo a pôsobí čistejšie."),
        ("Žmolkuje sa bioleštená bavlna?", "Môže. Úprava znižuje počiatočný chlp, ale trenie, priadza, konštrukcia a pranie môžu vytvoriť nové žmolky."),
        ("Dá sa bioleštenie zopakovať doma?", "Bezpečne nie. Priemyselný proces riadi pH, teplotu, čas, mechaniku a deaktiváciu a kontroluje aj stratu hmotnosti a pevnosť."),
        ("Mám používať enzymatický prací gél?", "Iba ak je vhodný pre zloženie a etiketu odevu a použijete ho podľa návodu. Nie je to automatická povinnosť pre bioleštený textil."),
        ("Môže celuláza poškodiť bavlnu?", "Pri nadmernej intenzite môže zvýšiť stratu hmotnosti a znížiť pevnosť, preto sa výrobný proces presne kontroluje."),
        ("Ako prať bioleštené tričko?", "S podobnými jemnými farbami, oddelene od zipsov a hrubých textílií, na programe a teplote zo štítku a bez preplnenia."),
        ("Ako odstrániť žmolky?", "Na úplne suchom a pevne podloženom kuse použite jemný textilný strojček bez tlaku; pri tenkom mieste alebo slučkách zastavte."),
        ("Prečo bioleštené tričko zmatnelo?", "Mohol vzniknúť nový povrchový chlp oderom alebo sa zmenil odraz svetla. Nemusí ísť o chemické vyblednutie."),
        ("Je bioleštenie to isté ako mercerizácia?", "Nie. Mercerizácia používa kontrolované pôsobenie zásady a napätia; bioleštenie využíva celulázu na povrchové mikrovlákna."),
        ("Je bioleštenie to isté ako opaľovanie chĺpkov?", "Nie. Opaľovanie používa krátky tepelný zásah plameňom, kým bioleštenie je enzymatický proces."),
        ("Čo robiť, ak odev rýchlo stráca vlákna?", "Zastavte mechanické odžmolkovanie a ďalšiu enzymatickú záťaž, zdokumentujte stenčenie a pri novom výrobku kontaktujte predajcu."),
    ],
}

add_cards(
    BIOPOLISHED,
    product_heading="Prací gél pre označený prateľný bioleštený textil",
    product_limit="Produkt používajte iba podľa etikety hotového odevu. Nesľubuje priemyselné bioleštenie, opravu vlákien, odstránenie všetkých žmolkov ani obnovu stratenej pevnosti.",
)


GARMENT_DYED: dict[str, object] = {
    "title": "Čo je garment-dyed oblečenie: farbenie hotového odevu, blednutie a pranie",
    "link": "co-je-garment-dyed-oblecenie-farbenie-hotoveho-odevu-blednutie-a-pranie",
    "meta": "Čo znamená garment-dyed oblečenie, ako sa farbí hotový odev, prečo sa diely líšia odtieňom a ako ho prať bez nežiaduceho prenosu farby.",
    "short": "Garment-dyed odev sa farbí až po nastrihaní a ušití. Švy, nite, štítky a vrstvy môžu prijať farbu odlišne, preto je prirodzená variácia možná, no nekontrolované púšťanie farby nie je automaticky vlastnosťou každého kusu.",
    "name": "garment-dyed oblečenie",
    "locative": "odeve farbenom po ušití",
    "identity_heading": "Garment dyeing opisuje okamih farbenia v výrobnom poradí",
    "identity_detail": "Pri garment dyeing sa najprv vytvoria diely a ušije hotový alebo takmer hotový výrobok a až potom sa celý kus vloží do farbiaceho procesu, takže farbu súčasne prijíma vrchná látka, švy, nite a kompatibilné komponenty.",
    "identity_boundary": "Farbenie hotového odevu nie je to isté ako farbenie priadze pred tkaním, farbenie metráže pred strihaním, domáce prefarbovanie ani pigmentové farbenie, pri ktorom nerozpustný pigment potrebuje spojivo.",
    "label_focus": "vláknové percentá, typ farbenia alebo pigmentu, upozornenie na farebnú variáciu, kontrastnú niť, potlač, elastan, kožený detail, pranie oddelene, odporúčanú teplotu a sušenie",
    "missing_label": "Ak výrobca nepotvrdil spôsob farbenia, nerobte záver iba zo svetlejších švov alebo vintage vzhľadu; podobný efekt môže vytvoriť pranie, potlač, pigment, oder alebo kombinácia rôzne zafarbených dielov.",
    "dry_check": "rozdiel odtieňa na švoch, vreckách a lemoch, suchý oter, svetlé lomy, mapy, farebnú niť, potlač, koženú nášivku, elastické časti a predchádzajúce lokálne odfarbenie",
    "damage_boundary": "Zámerná tonálna variácia môže patriť k dizajnu, ale prenos farby na pokožku, nábytok či inú bielizeň, ostrá chemická mapa a poškodené spojivo pigmentu sú samostatné problémy.",
    "test_focus": "Bielou vlhkou a suchou handričkou skúšajte iba nenápadnú oblasť podľa pokynov výrobcu a porovnajte prenos, zmenu odtieňa, okraj mapy a stav povrchu po úplnom vysušení.",
    "combined_risk": "uvoľnenia nefixovaného farbiva, trenia na vystúpených švoch, rozdielneho prijímania farby jednotlivými vláknami, migrácie pri dlhom mokrom kontakte a tepelného poškodenia elastanu či spojiva",
    "chemistry_boundary": "Bielidlo, kyslý domáci kúpeľ ani nadbytok gélu nedokážu univerzálne zafixovať farbu; nesprávna chémia môže vytvoriť svetlú mapu, zmeniť spojivo alebo poškodiť kontrastný komponent.",
    "drying_detail": "Odev po praní nenechávajte mokrý zložený na inom textile, urovnajte švy a sušte bez kontaktu s citlivým svetlým povrchom; odtieň hodnotíte až suchý pri rovnakom osvetlení.",
    "heat_boundary": "Horúca sušička môže urýchliť oder vystúpených hrán, zraziť základ, poškodiť elastan alebo pigmentové spojivo a zvýrazniť kontrast medzi dielmi.",
    "stop_signs": "silný prenos pri suchej alebo vlhkej skúške, farbenie pokožky a nábytku, nové ostré mapy, lepkavý pigment, praskajúci povrch, nerovnomerné zosvetlenie alebo poškodenie nášivky",
    "professional_boundary": "Bežné označené bavlnené garment-dyed tričko možno prať doma, kým viacfarebný odev, kožené detaily, neznáme pigmentové spojivo, hodnotný kus alebo výrazné púšťanie farby potrebuje údaje výrobcu, reklamáciu či odborné čistenie.",
    "answer": "Garment-dyed znamená, že odev bol zafarbený až po ušití. Tento postup môže vytvoriť jemne nerovnomerný, živý alebo vintage odtieň a zvýrazniť švy, pretože niť, výstuž a vrstvy prijímajú farbu odlišne. Nie je to však ospravedlnenie pre neobmedzené púšťanie farby. Nový kus najprv prezrite a otestujte podľa pokynov, perte ho podľa etikety oddelene alebo s veľmi podobnými tmavými farbami, použite presnú dávku, nenechávajte ho mokrý na inej bielizni a chráňte pred zbytočným trením a teplom. Pigment-dyed odev je príbuzná, ale nie totožná podskupina.",
    "intro": "Farbenie hotového odevu dáva dizajnérovi možnosť vytvoriť kolekciu z pripravených nezafarbených kusov a dosiahnuť mäkší, menej uniformný vzhľad. Zároveň kladie technické nároky na kompatibilitu šijacej nite, gombíkov, zipsu, výstuže a všetkých dielov, ktoré už počas farbenia tvoria jeden výrobok. Spotrebiteľ potom vidí svetlejší lem, inú niť alebo patinu a nevie, či ide o zámer, chybu alebo neskorší oder. Správna starostlivosť začína rozlíšením výrobného postupu, typom farbiva a skúškou stálofarebnosti, nie domácim pokusom farbu zafixovať octom či soľou.",
    "quick": [
        "<strong>Odev sa farbí po ušití:</strong> hotový alebo takmer hotový kus vstupuje do farbiaceho zariadenia ako celok.",
        "<strong>Komponenty nemusia mať rovnaký odtieň:</strong> polyesterová niť, bavlnená látka, elastan a výstuž prijímajú farbu odlišne.",
        "<strong>Variácia môže byť zámerná:</strong> švy, hrany a vrstvy vytvárajú mäkší alebo obnosený vzhľad.",
        "<strong>Púšťanie farby nie je automaticky v poriadku:</strong> mokré a suché trenie sa hodnotí samostatne.",
        "<strong>Pigment nie je rozpustené farbivo:</strong> na povrchu potrebuje spojivo a pri odere sa môže meniť inak.",
        "<strong>Prvé cykly vyžadujú opatrnosť:</strong> podobné farby, krátky mokrý kontakt, žiadne odkladanie na svetlý textil.",
    ],
    "overview_heading": "Ako prebieha farbenie hotového odevu a prečo vzniká charakteristická variácia",
    "overview": [
        "Pri tradičnom kusovom farbení sa zafarbí metráž a až potom sa strihá. Pri garment dyeing sa šijú kusy z pripravenej farbiteľnej látky a hotové odevy sa spracujú v zariadení, ktoré umožňuje pohyb farbiaceho kúpeľa cez vrstvy, švy a vnútro. Proces musí zohľadniť absorpciu, teplotu, mechaniku a priestor, aby sa kusy nezamotali a farba prenikla čo najrovnomernejšie.",
        "Šev obsahuje viac vrstiev a vystúpené hrany, šijacia niť môže byť z polyesteru a výstuž môže mať inú chemickú dostupnosť. Tieto komponenty preto nemusia prijať rovnakú farbu ako bavlnená plocha. Výrobca môže rozdiel cielene využiť ako estetiku. Ak však konštrukcia nebola navrhnutá na následné farbenie, môže sa objaviť zrazenie, zvlnenie, znečistenie svetlého komponentu alebo nerovnomerný odtieň.",
        "Po farbení nasleduje oplach, odstránenie voľného farbiva, prípadná fixácia, zmäkčenie a sušenie. Výkon sa hodnotí skúškami stálofarebnosti pri praní, vode, potení a trení. Názov garment-dyed opisuje výrobnú fázu, nie výsledok každej skúšky. Kvalitný kus môže mať zámernú variáciu a zároveň primeranú stálofarebnosť; nekvalitný môže púšťať farbu bez ohľadu na pekný príbeh o patine.",
    ],
    "table1_heading": "Garment dyeing, piece dyeing, yarn dyeing a pigment dyeing",
    "table1_intro": "Rozdiel je najmä v tom, kedy a ako sa farba pridáva. Rovnaký vizuálny efekt môže vzniknúť viacerými cestami, preto rozhodujú údaje výrobcu.",
    "table1_headers": ["Postup", "Kedy sa farbí", "Typický znak", "Hranica pri starostlivosti"],
    "table1_rows": [
        ("Garment dyeing", "Po nastrihaní a ušití odevu.", "Tonálna variácia pri švoch a komponentoch je možná.", "Hotový kus môže mať rozdielne vlákna a vrstvy."),
        ("Piece dyeing", "Na hotovej metráži pred strihaním.", "Plocha môže byť rovnomernejšia pred vstupom do výroby.", "Neskoršia niť a komponenty nemusia byť farbené spolu."),
        ("Yarn dyeing", "Priadza sa farbí pred tkaním alebo pletením.", "Farebné nite vytvárajú káro, pruh alebo melír v konštrukcii.", "Nie je synonymom farbenia hotového odevu."),
        ("Pigment dyeing", "Pigment sa pomocou spojiva ukladá najmä na povrch, často aj na hotový odev.", "Charakteristické obrusovanie a povrchový vzhľad.", "Spojivo a oter sú ďalšie kritické vlastnosti."),
        ("Domáce prefarbovanie", "Spotrebiteľ farbí už používaný výrobok.", "Výsledok závisí od neznámych úprav, škvŕn a zmesí.", "Nie je porovnateľné s riadenou výrobou a môže zafarbiť práčku."),
    ],
    "sections": [
        {
            "heading": "Ako spoznať garment-dyed odev bez unáhleného záveru",
            "paragraphs": [
                "Najspoľahlivejší je údaj výrobcu na etikete alebo produktovej stránke. Vizuálne indície sú iba pomocné: jemná variácia okolo švov, podobná farba vrchnej látky a bavlnenej nite, svetlejší polyesterový steh, tónované vnútorné štítky alebo celkový mäkký vzhľad. Žiadny jednotlivý znak proces nedokazuje, pretože môže vzniknúť aj praním, potlačou alebo opotrebením.",
                "Prezrite rub, vnútro vrecka a priestor pod lemom. Ak je farba iba na povrchu a pri ohybe presvitá svetlý základ, môže ísť o pigmentový alebo tlačený efekt. Ak sú priadze zafarbené v celom priereze, môže ísť o reaktívne či priame farbivo, no ani to neurčí fázu výroby. Pri drahšom kuse žiadajte technický opis namiesto domáceho chemického testovania.",
            ],
        },
        {
            "heading": "Prečo sú švy, nite, gombíky a štítky iného odtieňa",
            "paragraphs": [
                "Bavlnená látka môže prijímať farbivo určené pre celulózu, zatiaľ čo polyesterová šijacia niť za rovnakých podmienok zostane svetlejšia. V hrubom šve prechádza kúpeľ cez viac vrstiev a mechanický kontakt je iný. Gombík, zipsová páska, elastická niť a lepená výstuž majú vlastné zloženie. Výsledkom je tonálny kontrast, ktorý môže byť zámerný.",
                "Rozdielny odtieň nie je automaticky chyba, ale treba sledovať funkciu. Ak sa šev vlní, výstuž sa oddeľuje, gumička stratila pružnosť alebo svetlý komponent zachytil škvrny farbiva, ide o viac než estetickú variáciu. Pri novom odeve odfoťte stav pred prvým praním, aby bolo možné odlíšiť výrobný vzhľad od neskoršej zmeny.",
            ],
        },
        {
            "heading": "Ako prať garment-dyed tričko, mikinu alebo nohavice prvýkrát",
            "paragraphs": [
                f"Prečítajte štítok a postup pre <a href=\"{ARTICLE_NEW}\">prvé pranie nového oblečenia</a>. Urobte skrytú skúšku prenosu, odev otočte podľa odporúčania výrobcu a perte ho samostatne alebo s veľmi podobnými tmavými farbami. Krátky povolený cyklus je bezpečnejší než dlhé namáčanie. Bubon nepreplňte, aby sa produkt opláchol a odev nezostal stlačený v kontakte s inou farbou.",
                "Použite teplotu a prostriedok zo štítku. Farbu sa nepokúšajte fixovať kuchynskou soľou, octom alebo náhodnou zmesou; pri moderných farbivách môže byť taký zásah neúčinný a poškodiť komponenty či práčku. Po cykle kus hneď vyberte, nenechajte ho mokrý na bielom uteráku a sušte bez priameho intenzívneho svetla, ak ho výrobca obmedzuje.",
            ],
        },
        {
            "heading": "Ako zabrániť prenosu farby na inú bielizeň",
            "paragraphs": [
                f"Triedenie podľa približného odtieňa je základ, no dôležité sú aj teplota, čas a mechanika. Veľmi tmavý nový kus oddeľte, aj keď etiketa sľubuje stálofarebnosť. Praktické kroky rozoberá článok <a href=\"{ARTICLE_BLEEDING}\">ako zabrániť púšťaniu farby pri praní nového oblečenia</a>. Zachytávač farby môže doplniť správny postup, ale nie je zárukou ani povolením miešať rizikové kusy.",
                "Farba sa môže prenášať aj mimo práčky: z mokrého lemu na kôš, zo spotených nohavíc na svetlú sedačku alebo pri suchom trení na kabelku. Pred prvým dlhým nosením skontrolujte skrytú oblasť bielou handričkou bez agresívneho drhnutia. Pri silnom prenose výrobok izolujte a kontaktujte predajcu namiesto opakovaného prania v nádeji, že sa všetka farba odplaví.",
            ],
            "callout": {
                "title": "Miesta, kde sa farba môže preniesť",
                "items": [
                    "V práčke na svetlejšiu bielizeň počas mokrého kontaktu.",
                    "Po cykle z mokrého odevu na kôš, uterák alebo sušiacu plochu.",
                    "Pri nosení na pokožku, spodnú vrstvu, kabelku alebo čalúnenie.",
                    "Pri skladovaní vlhkého alebo spoteného kusu vedľa svetlého textilu.",
                ],
                "background": "#fffaf5",
                "border": "#e6ded2",
            },
        },
        {
            "heading": "Zámerné blednutie verzus slabá stálofarebnosť",
            "paragraphs": [
                "Zámerná patina býva rozložená v súlade s konštrukciou a dizajnom: vystúpené švy, hrany a plochy dostanú mäkší tón. Slabá stálofarebnosť sa prejavuje prenosom na kontaktný materiál, prudkou zmenou po povolenom cykle alebo ostrými mapami bez logiky konštrukcie. Výrobca má jasne komunikovať, že každý kus sa môže líšiť, no táto veta nenahrádza primeranú funkčnosť.",
                f"Samostatné mechanizmy prania, vody, potu, svetla a trenia vysvetľuje článok o <a href=\"{ARTICLE_COLOR}\">stálofarebnosti textilu</a>. Pri reklamácii porovnajte fotografie pred a po cykle, etiketu a miesta prenosu. Jednotný mierny vývoj farby po čase je iný jav než modrý odtlačok na sedačke pri prvom nosení.",
            ],
        },
        {
            "heading": "Pigment-dyed odev: príbuzný vzhľad, odlišný mechanizmus",
            "paragraphs": [
                "Pigmenty sú nerozpustné farebné častice bez prirodzenej afinity k vláknu, preto potrebujú spojivo, ktoré ich prichytí na povrch. Pigmentové farbenie možno použiť na hotový odev a často vytvára charakteristické obrusovanie na švoch. Nie každý garment-dyed kus je však pigmentový a nie každá pigmentová tlač sa označuje ako garment dyeing.",
                "Pri starostlivosti chráňte spojivo pred nadmerným oderom, nevhodným rozpúšťadlom a teplom. Praskajúci alebo lepkavý povrch nie je voľné rozpustené farbivo a dlhé namáčanie ho neopraví. Koncentrát nelejte na suchú plochu a žehlenie z líca používajte iba pri výslovnom povolení. Pri nejasnom výrobku sa riaďte najcitlivejšou povrchovou úpravou.",
            ],
        },
        {
            "heading": "Škvrny na garment-dyed odeve bez svetlej mapy",
            "paragraphs": [
                f"Najprv určte pôvod škvrny podľa návodu <a href=\"{ARTICLE_STAINS}\">ako odstraňovať rôzne škvrny</a>. Tekutinu odsajte, pevnú nečistotu nadvihnite a prípravok skúste na skrytom šve. Pracujte od okraja ku stredu a nevytvárajte veľký mokrý kruh. Na mäkkom tonálnom povrchu môže lokálne odfarbenie pôsobiť výraznejšie než pôvodná škvrna.",
                "Bielidlo nepoužívajte bez povolenia symbolom a kompatibility farbiva. Aj kyslíkový produkt môže zmeniť odtieň. Po lokálnom ošetrení dodržte oplach z návodu a miesto neposudzujte mokré. Tmavší kruh môže byť iba voda, no ostrý svetlý okraj po vyschnutí môže znamenať odstránené farbivo alebo presunutú apretúru.",
            ],
        },
        {
            "heading": "Sušenie na slnku, v sušičke a na vešiaku",
            "paragraphs": [
                "Mokrý odev podoprite, urovnajte švy a sušte podľa symbolu. Intenzívne priame slnko môže zmeniť citlivé farbivo a vytvoriť rozdiel medzi osvetlenou a preloženou plochou. Na vešiaku sa môže ťažká mikina vytiahnuť, preto použite vhodnú oporu. Tmavý kus nenechávajte v kontakte so svetlou stenou, drevom alebo textilom, kým nie je úplne suchý.",
                f"Sušička je prípustná iba pri symbole. Teplo a prevaľovanie zvyšujú oder vystúpených hrán a pri pigmentovom povrchu môžu urýchliť patinu. Praktické vetranie rozoberá článok <a href=\"{ARTICLE_DRYING}\">ako sušiť bielizeň bez zatuchnutia</a>. Odtieň porovnávajte pri rovnakom dennom rozptýlenom svetle až po vychladnutí a vysušení.",
            ],
        },
        {
            "heading": "Domáce prefarbovanie nie je pokračovanie výrobného garment dyeing",
            "paragraphs": [
                "Výrobný tím vyberá pripravenú látku, kompatibilné nite a komponenty a nastavuje kúpeľ podľa hmotnosti a vlákna. Používaný odev už obsahuje škvrny, zvyšky apretúr, potlač, elastan, polyesterové nite a miesta opotrebenia. Domáce farbivo preto môže chytiť nerovnomerne a nezafarbí každý materiál rovnakým odtieňom.",
                "Pred domácim farbením treba poznať zloženie, povolenú teplotu, kapacitu zariadenia a pokyny výrobcu farbiva. Proces môže zafarbiť práčku a ďalšie komponenty a nemusí prekryť bielidlovú škvrnu. Ak je cieľom profesionálny výsledok na hodnotnom kuse, vhodnejšia je špecializovaná služba; pôvodné označenie garment-dyed nezaručuje úspech ďalšieho farbenia.",
            ],
        },
        {
            "heading": "Ako vyberať garment-dyed oblečenie pri nákupe",
            "paragraphs": [
                "Skontrolujte, či predajca vysvetľuje prirodzenú variáciu, odporúčanie pre prvé prania a možné blednutie. Prezrite švy, vnútro vrecka, potlač, gombíky a kožené alebo papierové nášivky. Jemná nerovnomernosť môže byť estetika, no škvrnité hrudky, lepkavý povrch, poškodená gumička a farba na rukách sú varovné.",
                "Zvážte, kde budete odev nosiť. Veľmi sýte nohavice, ktoré majú prirodzene meniť patinu, nemusia byť ideálne k bielej sedačke alebo kabelke bez overenia suchého oteru. Pri kúpe si odložte produktový opis, pretože informácia o zámernej variácii a starostlivosti môže neskôr pomôcť rozlíšiť očakávaný vývoj od chyby.",
            ],
        },
        {
            "heading": "Kedy farebnú zmenu reklamovať",
            "paragraphs": [
                "Reklamáciu zvážte pri silnom prenose na pokožku alebo bežný kontaktný povrch, prudkom nerovnomernom vyblednutí po dodržanom prvom cykle, poškodení komponentu alebo zmene mimo popísanej estetiky. Zdokumentujte suchý odev pred praním, etiketu, presný program, použitý produkt a poškodený predmet. Farebnú handričku uchovajte iba čisto a bezpečne, ak môže pomôcť.",
                "Pred posúdením nepridávajte ocot, soľ, bielidlo ani ďalší horúci cyklus. Mohli by zmeniť farbu a sťažiť určenie príčiny. Predajcovi rozlíšte mokrý prenos, suchý oter, celkové blednutie a lokálnu mapu. Ak výrobca výslovne deklaroval variáciu, porovnajte ju s konkrétnym prejavom, nie s domnienkou, že každá zmena je dovolená.",
            ],
        },
    ],
    "table2_heading": "Farebná zmena garment-dyed odevu: zámer, opotrebenie alebo problém",
    "table2_intro": "Pozorovanie rozdeľte podľa toho, či ide o odtieň, prenos, povrch alebo konštrukciu. Až potom zvoľte ďalší krok.",
    "table2_headers": ["Prejav", "Možné vysvetlenie", "Čo overiť", "Bezpečný ďalší krok"],
    "table2_rows": [
        ("Švy sú od začiatku svetlejšie", "Iné vlákno nite, hrúbka vrstiev alebo zámerná variácia.", "Produktový opis a stav pred prvým praním.", "Ak je funkcia v poriadku, dodržať šetrnú starostlivosť."),
        ("Farba prešla na inú bielizeň", "Voľné farbivo, nevhodné triedenie, teplota alebo dlhý mokrý kontakt.", "Etiketu, skúšku a podmienky cyklu.", "Oddeliť kus, ďalej nezohrievať a pri silnom prenose reklamovať."),
        ("Hrany mäkko blednú", "Zámerná patina alebo bežný mechanický oder.", "Či zmena sleduje vystúpené švy a vyvíja sa postupne.", "Znížiť trenie a prať naruby, ak to výrobca odporúča."),
        ("Vznikla ostrá svetlá mapa", "Lokálna chémia, bielidlo, kvapka koncentrátu alebo strata farbiva.", "Tvar, históriu čistenia a skrytú skúšku.", "Nepridávať ďalšiu chémiu; zdokumentovať."),
        ("Povrch lepí alebo praská", "Poškodené pigmentové spojivo alebo potlač.", "Či je efekt povrchový a reaguje na teplo.", "Zastaviť trenie a teplo; overiť výrobcu."),
    ],
    "steps_heading": "Ako prať garment-dyed oblečenie krok za krokom",
    "steps": [
        "Prečítajte zloženie, upozornenie na farbenie, triedenie, sušenie a všetky symboly.",
        "Odfoťte pôvodný odtieň, švy, nite, potlač, nášivku a existujúce tonálne rozdiely.",
        "Na skrytom mieste overte suchý a vlhký prenos bez agresívneho drhnutia.",
        "Nový sýty kus perte samostatne alebo s veľmi podobnými tmavými farbami a bez dlhého namáčania.",
        "Použite presnú dávku kompatibilného prostriedku; nepridávajte domáce fixačné zmesi.",
        "Po cykle odev ihneď vyberte a nenechajte ho mokrý na inom textile alebo citlivom povrchu.",
        "Sušte iba povoleným spôsobom, chráňte pred zbytočným trením a intenzívnym svetlom.",
        "Pri silnom prenose, ostrej mape alebo poškodení povrchu ďalšie zásahy zastavte a stav zdokumentujte.",
    ],
    "remember": [
        "Potvrdil výrobca garment dyeing, alebo ho odhadujete iba podľa vzhľadu švov?",
        "Ide o rozpustné farbivo, pigment so spojivom, potlač alebo kombináciu?",
        "Prenáša sa farba pri suchom alebo vlhkom kontakte na bielu handričku?",
        "Má odev polyesterovú niť, elastan, výstuž, potlač či koženú nášivku?",
        "Periete nový sýty kus oddelene a vyberiete ho z bubna bez odkladu?",
        "Je zmena mäkká a konštrukčne logická, alebo ostrá, náhla a nekontrolovaná?",
    ],
    "mistakes": [
        "Považovať každý vintage vzhľad alebo svetlý šev za dôkaz garment dyeing.",
        "Ospravedlniť silné púšťanie farby tvrdením, že každý taký odev má blednúť.",
        "Miešať nový sýty kus so svetlou bielizňou iba so zachytávačom farby.",
        "Nechať mokrý odev dlho v bubne alebo na svetlom uteráku.",
        "Fixovať moderné farbivo náhodným octom alebo soľou bez pokynu výrobcu.",
        "Zamieňať pigmentové spojivo s rozpusteným farbivom a drhnúť praskajúci povrch.",
    ],
    "expert_heading": "Odbornejší pohľad: afinita farbiva, komponenty a skúšky stálofarebnosti",
    "expert": [
        "Farbivo musí byť kompatibilné s vláknom a procesom. Reaktívne farbivá sa môžu chemicky viazať na celulózu, kým iné triedy používajú odlišné mechanizmy afinity a fixácie. Pri hotovom odeve sú v jednom kúpeli komponenty s rozdielnym zložením a prístupnosťou. Proces preto vyžaduje konštrukciu pripravenú na farbenie, kontrolu pomeru kúpeľa, pohybu, teploty, pH, fixácie a oplachu.",
        "Pigmenty sú nerozpustné častice a bez spojiva nemajú vlastnú afinitu k vláknu. Pigment-dyed garment môže zámerne ukazovať povrchový oder, no výkon spojiva pri praní a trení sa musí posudzovať. Preto je nesprávne používať pigment-dyed a garment-dyed ako úplné synonymá: prvý termín opisuje farebný systém, druhý fázu, v ktorej sa odev farbí.",
        "AATCC rozlišuje stálofarebnosť pri praní, vode, potení a crockingu, teda prenose pri trení. Jeden dobrý výsledok nepreukazuje všetky ostatné. CottonWorks opisuje garment dyeing ako farbenie odevu po zostavení a upozorňuje na potrebu kompatibilných komponentov. GINETEX symboly potom určujú maximálne domáce ošetrenie hotového výrobku, nie výrobný kúpeľ.",
    ],
    "source_intro": "Zdroje vysvetľujú fázu garment dyeing, princípy farbív a pigmentov, skúšanie prenosu aj význam ošetrovacích symbolov. Nepodporujú tvrdenie, že zámerná variácia ospravedlňuje každý farebný prenos.",
    "sources": [
        ("CottonWorks: garment dyeing", COTTON_GARMENT_DYE),
        ("CottonWorks: dyeing basics", COTTON_DYEING),
        ("AATCC: prehľad skúšok stálofarebnosti", AATCC_STANDARDS),
        ("GINETEX: symboly ošetrovania", GINETEX),
        ("EÚ 1007/2011: označovanie textilných vlákien", EU_FIBRE_LABEL),
    ],
    "related": [
        ("Prečo farby pri praní a trení blednú", ARTICLE_COLOR),
        ("Ako zabrániť púšťaniu farby", ARTICLE_BLEEDING),
        ("Ako prať nové oblečenie prvýkrát", ARTICLE_NEW),
        ("Ako prať tmavé džínsy", ARTICLE_DENIM),
        ("Ako čítať štítok na oblečení", ARTICLE_LABEL),
        ("Ako odstrániť rôzne škvrny", ARTICLE_STAINS),
    ],
    "faq_title": "garment-dyed oblečenie a farbenie hotového odevu",
    "faq": [
        ("Čo znamená garment-dyed?", "Odev bol zafarbený až po nastrihaní a ušití, takže do procesu vstupuje ako hotový alebo takmer hotový výrobok."),
        ("Je garment-dyed oblečenie vždy bavlnené?", "Nie. Proces možno použiť na rôzne kompatibilné vlákna a zmesi; farbivo aj podmienky sa musia prispôsobiť zloženiu."),
        ("Prečo sú švy svetlejšie?", "Niť môže byť z iného vlákna, šev má viac vrstiev a farbiaci kúpeľ sa k nemu dostáva inak. Rozdiel môže byť zámerný."),
        ("Musí garment-dyed odev blednúť?", "Odtieň sa môže používaním vyvíjať, no miera závisí od farbiva, fixácie, oderu a starostlivosti. Silný nekontrolovaný prenos nie je automatická norma."),
        ("Ako ho prať prvýkrát?", "Podľa etikety, samostatne alebo s veľmi podobnými tmavými farbami, bez dlhého namáčania a s okamžitým vybratím po cykle."),
        ("Môžem farbu zafixovať octom?", "Nie univerzálne. Moderné farbivá majú rozdielnu chémiu a domáci ocot nemusí pomôcť; riaďte sa iba pokynom výrobcu."),
        ("Pomôže soľ proti púšťaniu farby?", "Nie ako všeobecný domáci postup. Bez znalosti farbiva a procesu môže byť neúčinná a nenahrádza správnu fixáciu vo výrobe."),
        ("Je pigment-dyed to isté ako garment-dyed?", "Nie. Pigment opisuje farebný systém so spojivom; garment-dyed opisuje farbenie po ušití. Môžu sa prekrývať, ale nie sú totožné."),
        ("Prečo farbí pokožku alebo sedačku?", "Môže ísť o slabú stálofarebnosť pri suchom či vlhkom trení. Odev prestaňte používať pri citlivých povrchoch a kontaktujte predajcu."),
        ("Môže ísť do sušičky?", "Iba pri povolenom symbole. Teplo a trenie môžu zrýchliť oder, zrazenie a poškodenie pigmentového povrchu."),
        ("Ako odstrániť škvrnu bez odfarbenia?", "Určte jej pôvod, prípravok skúste na skrytom mieste, nedrhnite a nepoužívajte bielidlo bez výslovného povolenia."),
        ("Dá sa odev doma znovu prefarbiť?", "Technicky niektoré výrobky áno, no výsledok je neistý pre zmesi, nite, škvrny a úpravy. Nie je to pokračovanie pôvodného riadeného procesu."),
        ("Kedy farebnú zmenu reklamovať?", "Pri silnom prenose, prudkom nerovnomernom vyblednutí alebo poškodení po dodržanom postupe. Uchovajte etiketu, fotografie a údaje o cykle."),
    ],
}

add_cards(
    GARMENT_DYED,
    product_heading="Prací gél pre označený prateľný garment-dyed odev",
    product_limit="Produkt nezaručuje stálofarebnosť, nezastaví navrhnutú patinu a neopraví odretý pigment, odstránené farbivo ani poškodené spojivo.",
)


ARTICLES: list[dict[str, object]] = [SANFORIZED, EASY_CARE, BIOPOLISHED, GARMENT_DYED]


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
        raise SystemExit("Batch 56 link preflight failed")
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


