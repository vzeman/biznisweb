#!/usr/bin/env python3
"""Build and validate VEVO batch 44 fabric-construction articles."""

from __future__ import annotations

import html
import json
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests


BASE = "https://www.vevo.sk"
PUBLISH_DATE = "2026-08-20"
CANDIDATES = Path("content/VEVO_CONTENT/batches/batch-44-candidates-2026-08-20.txt")
OUT_JSON = Path("content/VEVO_CONTENT/imports/batch-44-2026-08-20-articles.json")
OUT_PREFLIGHT = Path("content/VEVO_CONTENT/exports/batch-44-2026-08-20-link-preflight.json")

PRODUCT_NAME = "Prací gél hypoalergénny Vevo Ylang Absolute 1L"
PRODUCT_URL = "/p-1627/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l"
CATEGORY_NAME = "Pracie gély"
CATEGORY_URL = "/c/vevo-home-care/pranie/praci-gel"

ISO_KNIT_TYPES = "https://www.iso.org/standard/15553.html"
ISO_KNIT_CONCEPTS = "https://www.iso.org/standard/33711.html"
ASTM_KNIT_COUNT = "https://store.astm.org/d8007-24.html"
AATCC_SKEW = "https://members.aatcc.org/store/tm179/577/"
AATCC_DIMENSION = "https://members.aatcc.org/store/tm135/543/"
COTTONWORKS_JERSEY = "https://cottonworks.com/learning-hub/knitting/single-and-double-knits/"
COTTONWORKS_PLAIN = "https://cottonworks.com/learning-hub/weaving/basic-woven-fabric-designs/"
COTTONWORKS_PERCALE = "https://cottonworks.com/encyclopedia-item/percale/"
ISO_WOVEN_COUNT = "https://www.iso.org/standard/86700.html"
ASTM_WOVEN_COUNT = "https://store.astm.org/d3775-17r23.html"
DLA_RIPSTOP = "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=23768"
ISO_TEAR = "https://www.iso.org/standard/23369.html"
ASTM_TEAR = "https://store.astm.org/d1424-25.html"
EU_FIBRE_LABEL = "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02011R1007-20180215"
GINETEX = "https://www.ginetex.net/share/article/4201/care-symbols"

ARTICLE_COTTON = "/n/co-je-bavlna-vlastnosti-vyhody-nevyhody-a-starostlivost"
ARTICLE_COTTON_ELASTANE = "/n/bavlna-a-elastan-starostlivost-o-tricka-rifle-a-spodnu-bielizen"
ARTICLE_VISCOSE = "/n/co-je-viskoza-vlastnosti-krcivost-zrazanie-a-starostlivost"
ARTICLE_POLYAMIDE = "/n/co-je-polyamid-alebo-nylon-vlastnosti-odolnost-a-pranie"
ARTICLE_POLYESTER_COTTON = "/n/polyester-vs-bavlna-rozdiely-pri-noseni-prani-a-voni"
ARTICLE_SHRINKAGE = "/n/preco-sa-oblecenie-zrazi-po-prani-teplota-vlakna-susicka-a-prevencia"
ARTICLE_SEAMS = "/n/pevnost-sva-a-posun-niti-preco-oblecenie-praska-pri-svoch"
ARTICLE_GSM = "/n/gramaz-latky-co-znamena-gsm-pri-uterakoch-oblieckach-a-trickach"
ARTICLE_COLORFASTNESS = "/n/stalofarebnost-textilu-preco-farby-blednu-pri-prani-svetle-a-treni"
ARTICLE_THREAD_COUNT = "/n/pocet-niti-pri-oblieckach-co-znamena-thread-count-a-co-o-kvalite-nehovori"
ARTICLE_BEDDING_CHOICE = "/n/bavlna-lan-saten-alebo-flanel-ake-obliecky-vybrat-podla-sezony-a-potenia"
ARTICLE_BEDDING_WASH = "/n/ako-spravne-prat-obliecky-kompletny-sprievodca-starostlivostou"
ARTICLE_BEDDING_FREQUENCY = "/n/ako-casto-prat-postelne-pradlo"
ARTICLE_TENSILE = "/n/pevnost-textilu-v-tahu-a-proti-roztrhnutiu-co-skusky-hovoria-o-odolnosti"
ARTICLE_SNAGGING = "/n/zatrhavanie-textilu-preco-vznikaju-vytiahnute-ocka-a-ako-im-predchadzat"
ARTICLE_SOFTSHELL = "/n/co-je-softshell-vrstvy-membrana-impregnacia-a-spravna-starostlivost"
ARTICLE_MEMBRANE = "/n/co-je-membranove-oblecenie-vodny-stlpec-priedusnost-a-pranie-bez-poskodenia"
ARTICLE_BREATHABILITY = "/n/priedusnost-savost-a-rychloschnutie-ako-citat-vlastnosti-textilu"
ARTICLE_UV = "/n/ochrana-textilu-pred-uv-ziarenim-co-znamena-upf-a-co-ju-znizuje"

FORBIDDEN_PUBLIC_RE = re.compile(
    r"longtail|long-tail|long tail|\bkeyword(?:s)?\b|kľúčov\w*\s+slov\w*|"
    r"\bSEO\b|search\s+intent|sub[- ]?quer(?:y|ies)|sub[- ]?query|"
    r"fan[- ]?out|fanout|\bCTA\b",
    re.IGNORECASE,
)
FIXED_PRICE_RE = re.compile(r"\b\d{1,4}(?:[.,]\d{2})?\s*(?:EUR|€)\b", re.IGNORECASE)
TAG_RE = re.compile(r"<[^>]+>")
WORD_RE = re.compile(r"[^\W_]+(?:[-'][^\W_]+)?", re.UNICODE)


def esc(value: object) -> str:
    return html.escape(str(value), quote=True)


def table(headers: list[str], rows: list[tuple[str, ...]]) -> str:
    head = "".join(
        '<th style="border: 1px solid #e5e5e5; padding: 10px; text-align: left; background: #fafafa;">'
        f"{esc(header)}</th>"
        for header in headers
    )
    body = "\n".join(
        "<tr>"
        + "".join(
            '<td style="border: 1px solid #e5e5e5; padding: 10px; vertical-align: top;">'
            f"{cell}</td>"
            for cell in row
        )
        + "</tr>"
        for row in rows
    )
    return (
        '<div style="overflow-x: auto; margin: 20px 0;">'
        '<table style="width: 100%; min-width: 680px; border-collapse: collapse;">'
        f"<thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div>"
    )


def callout(
    title: str,
    items: list[str],
    *,
    background: str = "#fffaf5",
    border: str = "#e6ded2",
) -> str:
    bullets = "".join(f"<li>{item}</li>" for item in items)
    return (
        f'<div style="border: 1px solid {border}; border-radius: 8px; padding: 18px; '
        f'margin: 22px 0; background: {background};">'
        f'<h2 style="margin-top: 0;">{esc(title)}</h2><ul>{bullets}</ul></div>'
    )


def source_box(article: dict[str, object]) -> str:
    links = "".join(
        f'<li><a rel="noopener" href="{url}" target="_blank">{esc(label)}</a></li>'
        for label, url in article["sources"]
    )
    return (
        '<div style="border-left: 4px solid #111; padding: 16px 18px; margin: 24px 0; '
        'background: #fbfbfb;">'
        '<h2 style="margin-top: 0;">Odborné zdroje a hranice porovnávania</h2>'
        f"<p>{article['source_intro']}</p><ul>{links}</ul></div>"
    )


def commercial_blocks(article: dict[str, object]) -> str:
    return f"""
<div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 24px 0; background: #f7fbf8;">
<h2 style="margin-top: 0;">Prací prostriedok prispôsobte zloženiu a úprave</h2>
<p>{article['product_intro']}</p>
<div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff; margin: 14px 0;">
<h3 style="margin-top: 0;">{PRODUCT_NAME}</h3>
<p>{article['product_text']}</p>
<p><strong>Dôležitá hranica:</strong> {article['product_limit']}</p>
<p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="{PRODUCT_URL}">Pozrieť produkt</a></p>
</div>
</div>
<div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 24px 0; background: #fffaf5;">
<h2 style="margin-top: 0;">Porovnajte pracie gély pre bežnú domácu bielizeň</h2>
<p>{article['category_intro']}</p>
<div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff; margin: 14px 0;">
<h3 style="margin-top: 0;">{CATEGORY_NAME}</h3>
<p>{article['category_text']}</p>
<p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="{CATEGORY_URL}">Pozrieť kategóriu</a></p>
</div>
</div>
""".strip()


def related_links(items: list[tuple[str, str]]) -> str:
    links = "".join(f'<li><a href="{href}">{esc(label)}</a></li>' for label, href in items)
    return f"<h2>Súvisiace návody na VEVO</h2><ul>{links}</ul>"


def faq(article: dict[str, object]) -> str:
    parts = [f"<h2>FAQ: {esc(article['faq_title'])}</h2>"]
    for question, answer in article["faq"]:
        parts.append(f"<h3>{esc(question)}</h3><p>{answer}</p>")
    return "\n".join(parts)


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
    for section in article["sections"]:
        parts.append(f"<h2>{esc(section['heading'])}</h2>")
        parts.extend(f"<p>{paragraph}</p>" for paragraph in section["paragraphs"])
        if section.get("callout"):
            note = section["callout"]
            parts.append(
                callout(
                    note["title"],
                    note["items"],
                    background=note.get("background", "#fffaf5"),
                    border=note.get("border", "#e6ded2"),
                )
            )
    parts.append(f"<h2>{esc(article['table2_heading'])}</h2>")
    parts.append(f"<p>{article['table2_intro']}</p>")
    parts.append(table(article["table2_headers"], article["table2_rows"]))
    parts.append(f"<h2>{esc(article['steps_heading'])}</h2>")
    parts.append("<ol>" + "".join(f"<li>{item}</li>" for item in article["steps"]) + "</ol>")
    parts.append(callout("Čo si skontrolovať pred praním", article["remember"], background="#f7fbf8", border="#dbe5de"))
    parts.append(callout("Najčastejšie chyby", article["mistakes"], background="#fff7f7", border="#eadada"))
    parts.append(f"<h2>{esc(article['expert_heading'])}</h2>")
    parts.extend(f"<p>{paragraph}</p>" for paragraph in article["expert"])
    parts.append(source_box(article))
    parts.append(commercial_blocks(article))
    parts.append(related_links(article["related"]))
    parts.append(faq(article))
    return "\n".join(parts)


ARTICLES: list[dict[str, object]] = [
    {
        "title": "Čo je jersey úplet: pružnosť, krútenie švov a správne pranie",
        "link": "co-je-jersey-uplet-pruznost-krutenie-svov-a-spravne-pranie",
        "meta": "Čo je jersey úplet, prečo pruží, stáča okraje alebo krúti bočné švy a ako prať bavlnený, viskózový či syntetický jersey bez zbytočnej deformácie.",
        "short": "Jersey je úpletová konštrukcia, nie názov jedného vlákna. Zistite, prečo pruží, stáča okraje, krúti švy a ako pranie prispôsobiť skutočnému zloženiu odevu.",
        "answer": "Jersey je hladký záťažný úplet vytvorený z očiek, najčastejšie s odlišnou lícnou a rubovou stranou. Prirodzene sa viac prispôsobuje pohybu než bežná tkanina, no jeho správanie závisí od vlákna, hustoty očiek, dĺžky slučky, gramáže, dokončenia a prípadného elastanu. Bavlnený jersey, viskózový jersey a polyesterový jersey preto nemožno prať jedným automatickým pravidlom. Skontrolujte celé zloženie a symboly, odev obráťte naruby, nepreťažujte ho ťažkými kusmi, použite presnú dávku vhodného prostriedku a po praní ho sušte bez bodového vytiahnutia. Skrútený bočný šev nemusí znamenať chybu práčky; môže súvisieť so šikmosťou úpletu, priadzou, strihaním, šitím aj zmenou po praní.",
        "intro": "Tričko môže byť mäkké a pružné aj bez elastanu, pretože samotné očká jersey úpletu sa pri zaťažení menia a preskupujú. Tá istá voľnosť však znamená, že materiál ľahšie reaguje na napnutie pri praní, vysoké otáčky, zavesenie za mokra a nerovnomerné švy. Názov jersey navyše nehovorí, či je výrobok z bavlny, viskózy, polyesteru, vlny alebo zmesi. Praktický návod preto musí najprv oddeliť konštrukciu od vláknového zloženia a až potom riešiť teplotu, program, sušenie či žehlenie.",
        "quick": [
            "<strong>Jersey je úplet, nie vlákno:</strong> rovnaká konštrukcia môže byť bavlnená, viskózová, syntetická aj zmesová.",
            "<strong>Pružnosť nie je to isté ako návratnosť:</strong> látka sa môže ľahko natiahnuť, ale po dlhom zaťažení sa nemusí úplne vrátiť.",
            "<strong>Stočený okraj je vlastnosť konštrukcie:</strong> pri jednolícnom jersey sa voľný rez môže stáčať aj bez poškodenia.",
            "<strong>Krútenie švov má viac príčin:</strong> šikmosť úpletu, smer priadze, strih, šitie a pranie sa môžu navzájom sčítať.",
            "<strong>Štítok má prednosť:</strong> najcitlivejšie vlákno, potlač, lem, výstuž alebo elastická časť určuje bezpečný postup.",
        ],
        "overview_heading": "Čo presne znamená jersey a ako ho spoznať",
        "overview": [
            "V bežnom oblečení sa slovom jersey najčastejšie označuje záťažný úplet, pri ktorom vznikajú vodorovné rady očiek a zvislé stĺpiky očiek. Lícna strana jednolícneho jersey zvyčajne ukazuje jemné zvislé stĺpiky pripomínajúce písmeno V, kým rub pôsobí vodorovnejšie a oblúčikovito. Nejde o dve nalepené vrstvy, ale o dve tváre tej istej slučkovej konštrukcie. Hrúbku a vzhľad možno meniť priadzou, jemnosťou stroja, dĺžkou očka, hustotou a dokončovaním.",
            "Úplet sa od tkaniny líši princípom tvorby plochy. Tkanina prepája osnovné a útkové priadze cez seba, zatiaľ čo záťažný úplet vytvára očká z priadze podávanej naprieč. Očká sa môžu pri pohybe meniť bez toho, aby sa samotné vlákno natiahlo o rovnakú hodnotu. Preto býva jersey pohodlný na tričká, šaty, pyžamá, spodnú bielizeň a obliečky, ale stabilita hotového výrobku závisí od konštrukcie švov a správneho strihu.",
            "Pri nákupe si názov látky spojte s údajmi na etikete. Nariadenie EÚ o označovaní textilu pracuje s vláknovým zložením, takže údaj bavlna, polyester alebo viskóza odpovedá na inú otázku než slovo jersey. Zloženie napovie správanie pri vlhkosti, teple a schnutí; konštrukcia napovie pružnosť, stabilitu okrajov a spôsob deformácie. Ani jedna informácia sama nestačí na presnú starostlivosť.",
        ],
        "table1_heading": "Jersey podľa zloženia: rovnaké očká, rozdielne správanie",
        "table1_intro": "Tabuľka opisuje bežné tendencie, nie náhradu etikety. Výrobca môže použiť zmes, farbivo, potlač, živicu, brúsenie alebo inú úpravu, ktorá bezpečný postup zmení.",
        "table1_headers": ["Typ jersey", "Typický pocit a použitie", "Hlavné riziko", "Čo skontrolovať pred praním"],
        "table1_rows": [
            ("Bavlnený jersey", "Savý, mäkký, častý pri tričkách, pyžamách a detskom oblečení.", "Rozmerová zmena, blednutie, vyťahanie za mokra a žmolky podľa priadze.", "Farbu, potlač, povolenú teplotu, sušičku a predzrazenie."),
            ("Viskózový alebo modalový jersey", "Splývavý a mäkký pri šatách, blúzkach či spodných vrstvách.", "Vyťahanie mokrého kusu, deformácia ramien a citlivosť dokončenia.", "Jemný program, menšiu náplň, otáčky a spôsob sušenia."),
            ("Polyesterový jersey", "Rýchlejšie schne, používa sa na športové a ľahké odevy.", "Zápach v zvyškoch mazu, statika, žmolky a poškodenie funkčnej úpravy.", "Zákaz aviváže, potlač, teplotu a odporúčaný prostriedok."),
            ("Jersey s elastanom", "Lepšie prilieha a môže sa vracať po natiahnutí.", "Teplo, chlór, dlhé napätie a postupná strata návratnosti.", "Percento elastanu, sušičku, bielenie a intenzitu odstreďovania."),
            ("Vlnený jersey", "Jemný, pružný a tepelne pohodlný pri šatách či vrstvách.", "Splstnatenie, zrazenie a deformácia nevhodnou mechanikou.", "Program na vlnu, vhodný prostriedok a vodorovné sušenie."),
        ],
        "sections": [
            {
                "heading": "Prečo jersey pruží aj bez elastanu",
                "paragraphs": [
                    "Pri ťahu sa mení geometria očiek: slučky sa otvárajú, preskupujú a časť priadze sa premiestni zo zakrivených úsekov do smeru zaťaženia. To umožní zmenu rozmeru bez toho, aby bolo každé vlákno samo vysoko elastické. Šírková a dĺžková pružnosť nebýva rovnaká, preto sa tričko správa inak pri obliekaní cez ramená a inak pri dlhom ťahu smerom nadol.",
                    "Elastan môže zvýšiť pružnosť a najmä návratnosť, ale jeho prítomnosť neodstraňuje význam hustoty, gramáže a stehu. Voľný ľahký jersey sa môže naťahovať ľahko a pritom sa kolená či lakte po nosení vydúvajú. Kompaktnejší úplet s vhodne vloženým elastanom sa môže vracať lepšie. Číslo na etikete preto treba posudzovať spolu s reálnou konštrukciou a strihom.",
                ],
            },
            {
                "heading": "Pružnosť, rast a návratnosť nie sú synonymá",
                "paragraphs": [
                    "Pružnosť opisuje, o koľko sa látka pri určenej sile predĺži. Rast látky je zvyšková deformácia, ktorá zostane po pôsobení zaťaženia a po jeho odstránení. Návratnosť vyjadruje, ako dobre sa materiál približuje k pôvodnému rozmeru. Dve látky môžu pri skúšaní dosiahnuť podobné predĺženie, ale jedna sa vráti a druhá ostane zvlnená alebo vydutá.",
                    "V domácnosti sa rast prejaví vyťahanými lakťami, kolenami, lemom alebo ramenami po sušení na úzkom vešiaku. Pranie môže dočasne časť deformácie uvoľniť, ale nie je opravou unavenej priadze, poškodeného elastanu ani nesprávneho strihu. Pri kúpe skúste látku jemne natiahnuť a sledujte, či sa po chvíli vracia bez zvlnenia; nejde o normovanú skúšku, iba o praktické pozorovanie.",
                ],
            },
            {
                "heading": "Prečo sa odstrihnutý jersey stáča na okrajoch",
                "paragraphs": [
                    "Jednolícny jersey má na líci a rube rozdielnu geometriu slučiek a vnútorné sily nie sú na voľnom okraji vyvážené. Preto sa priečny a pozdĺžny rez môže stáčať odlišným smerom. Stáčanie odstrihnutého zvyšku látky samo osebe nie je dôkaz, že je tričko zničené alebo zle vyprané. V hotovom odeve okraj stabilizuje lem, šev, pásik alebo dvojitá vrstva.",
                    "Problém vzniká vtedy, keď sa lem po praní pretáča, nevie ležať naplocho alebo sa okraj začína párať. Vtedy skontrolujte šitie, napätie nite, rozdielne zrazenie vrstiev a či sa kus pri sušení nenaťahoval. Silné žehlenie môže lem na chvíľu sploštiť, ale neodstráni konštrukčnú nerovnováhu ani nesprávne prišitú pásku.",
                ],
            },
            {
                "heading": "Prečo sa bočný šev trička po praní skrúti",
                "paragraphs": [
                    "Skrútený šev môže súvisieť so šikmosťou radov alebo stĺpikov očiek, zvyškovým krútiacim momentom priadze, nerovnomerným uvoľnením úpletu, strihaním dielov mimo správneho smeru alebo šitím pod rozdielnym napätím. Pranie a sušenie tieto vnútorné napätia uvoľní, takže chyba sa ukáže až po prvých cykloch. Nie každý posun je spôsobený vysokou teplotou a nie každý sa dá žehlením trvalo napraviť.",
                    "AATCC TM179 hodnotí zmenu šikmosti látky po štandardizovanom domácom praní a oddeľuje meranie od dojmu pri nosení. Pri hotovom odeve navyše rozhodujú švy a strih. Ak sa nový výrobok po dodržaní etikety výrazne skrúti, zdokumentujte stav pred a po praní a riešte kvalitu s predajcom. Opakované agresívne cykly nie sú spoľahlivý spôsob nápravy.",
                ],
                "callout": {
                    "title": "Rýchla kontrola skrúteného trička",
                    "items": [
                        "Položte suché tričko bez napínania na rovnú plochu a zarovnajte ramená, nie bočné švy nasilu.",
                        "Porovnajte oba bočné švy, spodný lem, stred potlače a smer zvislých stĺpikov očiek.",
                        "Skontrolujte, či sa zmenil celý diel, iba lem alebo len miesto pri jednom šve.",
                        "Pri novom kuse si odložte etiketu a záznam použitého programu; výrazná zmena môže byť výrobný problém.",
                    ],
                    "background": "#f7fbf8",
                    "border": "#dbe5de",
                },
            },
            {
                "heading": "Bavlnený jersey: savosť, zrazenie a farba",
                "paragraphs": [
                    "Bavlna prijíma vlhkosť a bavlnený jersey býva príjemný na bežné tričká, no po namočení je ťažší a voľná konštrukcia sa môže pri manipulácii natiahnuť. Rozmerovú zmenu ovplyvňuje predchádzajúce dokončenie, hustota a spôsob sušenia. Podrobný rozdiel medzi vláknom a starostlivosťou vysvetľuje článok <a href=\"/n/co-je-bavlna-vlastnosti-vyhody-nevyhody-a-starostlivost\">čo je bavlna</a>.",
                    "Tmavé a potlačené tričko perte naruby s podobnými farbami a nepoužívajte vyššiu teplotu len preto, že ide o bavlnu. Potlač, elastický lem alebo šijacia niť môžu mať nižšiu hranicu. Ak chcete znížiť blednutie a mechanické opotrebovanie, vyhnite sa zbytočne dlhému drsnému cyklu a kus po doprati nenechávajte pokrčený vo vlhkom bubne.",
                ],
            },
            {
                "heading": "Viskózový a modalový jersey: hmotnosť vody a vyťahanie",
                "paragraphs": [
                    "Splývavý viskózový jersey môže pôsobiť ľahko za sucha, ale po nasiaknutí sa jeho hmotnosť sústredí do ramien, štipcov a úzkych bodov. Preto mokré šaty nevyťahujte z bubna za jedno ramienko a nevešajte ich na tenký vešiak, ak etiketa odporúča vodorovné alebo tvarované sušenie. Menšia náplň znižuje zamotanie do ťažkých uterákov a riflí.",
                    "Viskóza, modal a lyocell patria do príbuznej skupiny regenerovaných celulózových vlákien, no konkrétne vlastnosti sa líšia výrobou, priadzou a dokončením. Návod <a href=\"/n/co-je-viskoza-vlastnosti-krcivost-zrazanie-a-starostlivost\">čo je viskóza</a> vysvetľuje, prečo sa mokrá manipulácia a sušenie nesmú odvodzovať iba z mäkkého dotyku.",
                ],
            },
            {
                "heading": "Polyesterový jersey: rýchle schnutie, maz a zápach",
                "paragraphs": [
                    "Polyesterový jersey môže schnúť rýchlo, ale telesný maz a zvyšky produktu sa môžu držať v materiáli a opakovaným sušením sa zápach zvýrazní. Riešením nie je automaticky viac gélu alebo silnejšia vôňa. Pomáha prať bez preplnenia, dávkovať podľa vody a náplne, neodkladať spotený kus uzavretý a overiť, či funkčná úprava povoľuje použitý prostriedok.",
                    "Polyester a bavlna majú iné správanie pri vlhkosti, teple a schnutí; ich praktické porovnanie nájdete v článku <a href=\"/n/polyester-vs-bavlna-rozdiely-pri-noseni-prani-a-voni\">polyester verzus bavlna</a>. Aviváž môže pri niektorých funkčných úpletoch zhoršiť zamýšľané vlastnosti, preto sa riaďte výrobcom. Parfumovaný produkt používajte iba vtedy, keď je vhodný pre daný odev a používateľa.",
                ],
            },
            {
                "heading": "Jersey s elastanom: čo najviac poškodzuje návratnosť",
                "paragraphs": [
                    "Elastan pomáha priliehaniu a návratu po natiahnutí, ale neznáša každú kombináciu tepla, chémie a dlhého mechanického napätia. Horúca sušička, nepovolené bielenie, sušenie na radiátore alebo dlhodobé skladovanie v silnom natiahnutí môžu životnosť skrátiť. Presná hranica závisí od výrobku, preto neplatí univerzálne číslo pre všetky legíny a tričká.",
                    "Pri zmesi vychádzajte z najcitlivejšej zložky a konštrukcie. Článok <a href=\"/n/bavlna-a-elastan-starostlivost-o-tricka-rifle-a-spodnu-bielizen\">bavlna a elastan</a> ukazuje, prečo ani malé percento elastanu nemožno pri sušení ignorovať. Odev neodstreďujte agresívnejšie iba preto, aby bol skoro suchý; vyššie mechanické zaťaženie môže zhoršiť zvlnenie švov a lemov.",
                ],
            },
            {
                "heading": "Ako prať jersey tričko krok za krokom",
                "paragraphs": [
                    "Najprv zapnite alebo zakryte tvrdé prvky na ostatných kusoch, tričko obráťte naruby a oddeľte ho od suchých zipsov, podprsenkových háčikov a hrubých uterákov. Vyberte program, teplotu a otáčky podľa etikety. Bubon nechajte dostatočne voľný, aby sa ľahký úplet neuzamkol do ťažkej mokrej gule a prostriedok sa mohol opláchnuť.",
                    "Dávku pracieho gélu prispôsobte tvrdosti vody, znečisteniu a skutočnej náplni. Viac produktu neznamená lepšie zachovanie tvaru; zvyšky môžu meniť dotyk a viesť k opakovanému praniu. Škvrnu ošetrite lokálne kompatibilným postupom bez silného drhnutia očiek. Po skončení cyklu tričko vyberte, jemne pretrepte a zarovnajte bez ťahania za výstrih.",
                ],
            },
            {
                "heading": "Sušenie jersey bez vytiahnutých ramien a zvlneného lemu",
                "paragraphs": [
                    "Mokrý jersey podoprite celou plochou alebo ho rozložte podľa pokynov. Tenký vešiak môže vytvoriť výstupky na ramenách a štipce bodové otlaky. Ak etiketa povoľuje šnúru, preložte kus cez viac bodov tak, aby hmotnosť nevisela iba na výstrihu. Vodorovné sušenie je zvlášť dôležité pri ťažkých, viskózových a vlnených úpletoch.",
                    "Sušičku použite iba pri povolenom symbole a zvoľte odporúčanú teplotu. Nadmerné teplo môže zmeniť elastan, potlač, rozmer aj povrch. Po vysušení nenechávajte tričko dlho zlisované medzi ťažkými kusmi. Ak je mierne pokrčené, najprv ho vyrovnajte rukou; žehlenie nastavte podľa najcitlivejšej časti a potlač nežehlite priamo.",
                ],
            },
            {
                "heading": "Žmolky, zatrhnutia a dierky v jersey",
                "paragraphs": [
                    "Povrchové žmolky vznikajú, keď sa uvoľnené konce vlákien trením zapletú do chumáčikov. Krátke vlákna, mäkká voľná priadza, trenie pri nosení a drsné pranie môžu riziko zvýšiť. Samotný názov jersey nepredpovie výsledok. Perte naruby, obmedzte kontakt s drsnými prvkami a žmolky odstraňujte opatrne bez prerezania očiek.",
                    "Zatrhnuté očko neťahajte a neodstrihujte bez kontroly, pretože slučková štruktúra môže začať utekať. Jemne presuňte uvoľnenú priadzu na rub alebo opravu zverte krajčírovi. Súvisiaci článok <a href=\"/n/zatrhavanie-textilu-preco-vznikaju-vytiahnute-ocka-a-ako-im-predchadzat\">o zatrhávaní textilu</a> rozlišuje povrchové očko, prasknutú priadzu a poškodenie pri šve.",
                ],
            },
            {
                "heading": "Ako vybrať kvalitný jersey podľa použitia",
                "paragraphs": [
                    "Na tričko sledujte rovnomernosť očiek, priehľadnosť pri miernom natiahnutí, návrat po pohybe, stabilitu výstrihu a kvalitu švov. Na priliehavé legíny je dôležitejšia návratnosť a krytie, pri pyžame mäkkosť, priedušnosť a odolnosť proti opakovanému praniu. Gramáž pomáha opisovať hmotnosť plochy, ale nie je samostatným hodnotením kvality; vysvetľuje ju článok <a href=\"/n/gramaz-latky-co-znamena-gsm-pri-uterakoch-oblieckach-a-trickach\">čo znamená GSM</a>.",
                    "Pozrite sa, či sa bočné švy už v predajni stáčajú, či lem zvlňuje a či potlač pri jemnom natiahnutí nepraská. Overte zloženie a symboly, nie iba marketingový názov. Drahší kus nemusí byť automaticky stabilnejší, ale transparentná etiketa, rovnomerná konštrukcia a šitie bez napätia dávajú viac informácií než samotné slovo prémiový.",
                ],
                "callout": {
                    "title": "Čo prezradí skúška v ruke a čo nie",
                    "items": [
                        "Jemné natiahnutie ukáže okamžitý návrat, nie správanie po desiatkach praní.",
                        "Pohľad proti svetlu ukáže nerovnomerné očká, nie presnú pevnosť alebo životnosť.",
                        "Dotyk napovie mäkkosť povrchu, nie vláknové zloženie ani chemickú úpravu.",
                        "Rovný šev pri novom kuse znižuje podozrenie na chybu, ale nezaručuje nulovú zmenu po praní.",
                    ],
                },
            },
        ],
        "table2_heading": "Problémy jersey po praní: príčina a bezpečný ďalší krok",
        "table2_intro": "Rovnaký prejav môže mať viac príčin. Pred ďalším cyklom skontrolujte zloženie, švy, teplotu, náplň aj spôsob sušenia, aby ste problém nezhoršili.",
        "table2_headers": ["Prejav", "Možné príčiny", "Čo overiť", "Ďalší krok"],
        "table2_rows": [
            ("Bočný šev sa stočil dopredu", "Šikmosť úpletu, priadza, nesprávny strih alebo uvoľnenie po praní.", "Oba švy, smer očiek, lem a dodržaný program.", "Sušiť zarovnané bez násilia; pri novom výrazne zmenenom kuse riešiť kvalitu."),
            ("Tričko je širšie a kratšie", "Rozmerová zmena, uvoľnenie očiek alebo teplo pri sušení.", "Zloženie, sušičku, pôvodný rozmer a symboly.", "Ďalší cyklus viesť podľa etikety; nenapínať mokrý kus do opačného extrému."),
            ("Ramená majú výstupky", "Mokré zavesenie na úzkom vešiaku alebo štipcoch.", "Hmotnosť mokrej látky a spôsob sušenia.", "Navlhčiť iba ak to etiketa povoľuje, vytvarovať a sušiť s plnou oporou."),
            ("Povrch je drsný alebo lepkavý", "Zvyšky prostriedku, preplnenie, tvrdá voda alebo poškodená úprava.", "Dávku, oplach, veľkosť náplne a kompatibilitu produktu.", "Odstrániť príčinu; nepridávať ďalší produkt naslepo."),
            ("Očko je vytiahnuté", "Kontakt s háčikom, zipsom, šperkom alebo drsným bubnom.", "Či je priadza iba posunutá alebo prasknutá.", "Neodstrihovať; presunúť na rub alebo odborne opraviť."),
        ],
        "steps_heading": "Bezpečný postup pri prvom praní nového jersey",
        "steps": [
            "Prečítajte zloženie aj všetky symboly a odfoťte si pôvodný tvar, ak chcete sledovať zmenu.",
            "Otočte odev naruby, zatvorte tvrdé prvky ostatných kusov a oddeľte ho od suchých zipsov a hrubých textílií.",
            "Zvoľte najjemnejší povolený program, teplotu a odstreďovanie; bubon nepreplňujte.",
            "Dávkujte prostriedok podľa etikety produktu, tvrdosti vody, znečistenia a skutočnej náplne.",
            "Po doprati podoprite mokrý kus, jemne ho zarovnajte a sušte spôsobom povoleným výrobcom.",
            "Až po úplnom vysušení vyhodnoťte rozmer, švy, lem a povrch; mokrý jersey sa správa inak než suchý.",
        ],
        "remember": [
            "Je jersey bavlnený, viskózový, syntetický, vlnený alebo zmesový?",
            "Obsahuje elastan, potlač, výstuž, lepený prvok alebo citlivú aplikáciu?",
            "Povoľuje etiketa zvolený program, odstreďovanie, sušičku a žehlenie?",
            "Nebude sa ľahký úplet prať spolu s tvrdými zipsami, háčikmi a ťažkými uterákmi?",
            "Má mokrý kus pri sušení dostatočnú oporu bez bodového ťahu?",
        ],
        "mistakes": [
            "Považovať jersey za synonymum bavlny a ignorovať skutočné vláknové zloženie.",
            "Zamieňať ľahké natiahnutie s dobrou návratnosťou po dlhom nosení.",
            "Vešať ťažký mokrý úplet za výstrih alebo na úzky vešiak.",
            "Pridať viac gélu, keď je povrch tuhý, bez kontroly dávky a oplachu.",
            "Odstrihnúť vytiahnuté očko a spustiť páranie slučkovej konštrukcie.",
            "Snažiť sa výrobnú šikmosť napraviť opakovaným horúcim praním alebo násilným napínaním.",
        ],
        "expert_heading": "Odbornejší pohľad: ako sa jersey opisuje a skúša",
        "expert": [
            "ISO 4921 definuje základné pojmy pletenia a ISO 8388 triedi typy pletenín. ASTM D8007-24 pri záťažných pleteninách meria počet zvislých stĺpikov a vodorovných radov očiek oddelene; priamo uvádza jersey ako typický jednolícny úplet. Tieto údaje opisujú konštrukciu a hustotu, nie automaticky mäkkosť, návratnosť, pevnosť šva alebo bezpečný prací program.",
            "AATCC TM179-2025 hodnotí zmenu šikmosti tkanín a pletenín po štandardizovanom domácom praní. Výsledok sa viaže na stanovené cykly a meranie značiek, nie na voľný dojem, že sa tričko trochu posunulo. Rozmerové zmeny samostatne rieši AATCC TM135-2025. Preto je technicky nepresné označiť každé skrátenie, rozšírenie a skrútenie jedným slovom zrazenie.",
            "Pre spotrebiteľa z toho vyplýva praktická hranica: normy umožňujú porovnať presne pripravené vzorky a postupy, ale nevysvetlia každý hotový odev bez údajov o priadzi, farbení, dokončení, strihu a šití. Domáca starostlivosť má rešpektovať symboly a znižovať zbytočné teplo, trenie a ťah; nedokáže odstrániť výrobnú nerovnováhu konštrukcie.",
        ],
        "source_intro": "Zdroje oddeľujú názov úpletu od vláknového zloženia, opisujú očká a štandardizované meranie šikmosti či rozmerovej zmeny. Žiadny z nich neurčuje jeden prací program pre všetky výrobky označené jersey.",
        "sources": [
            ("ISO 8388:1998: typy pletenín a odborná terminológia", ISO_KNIT_TYPES),
            ("ASTM D8007-24: počet stĺpikov a radov očiek záťažných pletenín", ASTM_KNIT_COUNT),
            ("AATCC TM179-2025: zmena šikmosti po domácom praní", AATCC_SKEW),
            ("CottonWorks: jednolícny jersey a jeho slučková konštrukcia", COTTONWORKS_JERSEY),
            ("EÚ 1007/2011: označovanie vláknového zloženia textilu", EU_FIBRE_LABEL),
            ("GINETEX: význam symbolov ošetrovania", GINETEX),
        ],
        "product_intro": "Pri bežnom prateľnom jersey je cieľom odstrániť nečistoty bez nadmerného dávkovania, zvyškov a zbytočného mechanického zaťaženia. Prostriedok vždy porovnajte so zložením, farbou a symbolmi konkrétneho odevu.",
        "product_text": "Prací gél z marseillského mydla je určený na bežnú domácu bielizeň. Pri bavlnenom alebo vhodnom zmesovom jersey ho použite iba podľa etikety produktu a odevu, s dávkou prispôsobenou vode a náplni a s dostatočným priestorom na oplach.",
        "product_limit": "Gél neopraví skrútený šev, unavený elastan ani vytiahnuté očko a nie je automaticky vhodný pre vlnu, hodváb, funkčnú úpravu alebo každý viskózový úplet. Pri osobitnom pokyne výrobcu použite určený prostriedok.",
        "category_intro": "Pri porovnávaní pracích gélov sledujte určenie pre farbu a materiál, odporúčanú dávku a kompatibilitu s citlivou časťou výrobku. Mäkký jersey nepotrebuje najvyššiu dávku, ale správny cyklus a dobrý oplach.",
        "category_text": "V kategórii nájdete pracie gély pre bežnú bielizeň. Pred výberom skontrolujte, či jersey neobsahuje vlnu, membránu, špeciálnu športovú úpravu alebo inú zložku, pre ktorú výrobca požaduje odlišný produkt.",
        "related": [
            ("Čo je bavlna a ako sa o ňu starať", ARTICLE_COTTON),
            ("Bavlna a elastan v tričkách a bielizni", ARTICLE_COTTON_ELASTANE),
            ("Čo je viskóza", ARTICLE_VISCOSE),
            ("Prečo sa oblečenie zrazí po praní", ARTICLE_SHRINKAGE),
            ("Pevnosť šva a posun nití", ARTICLE_SEAMS),
            ("Čo znamená gramáž GSM", ARTICLE_GSM),
        ],
        "faq_title": "jersey úplet a jeho pranie",
        "faq": [
            ("Je jersey vždy bavlna?", "Nie. Jersey označuje najmä úpletovú konštrukciu a môže byť z bavlny, viskózy, polyesteru, vlny alebo zmesi. Rozhodujúce zloženie nájdete na etikete."),
            ("Prečo jersey pruží, keď nemá elastan?", "Pružnosť vytvára aj geometria očiek, ktoré sa pri ťahu otvárajú a preskupujú. Elastan môže zlepšiť rozsah a návratnosť, ale nie je jediným zdrojom pohybu."),
            ("Prečo sa okraje jersey stáčajú?", "Jednolícny úplet má na oboch stranách inú geometriu a voľný rez nemá vyvážené sily. Hotový lem alebo šev má stáčanie stabilizovať."),
            ("Dá sa skrútený bočný šev napraviť?", "Mierny tvar možno po praní jemne zarovnať pri sušení, ale výrobnú šikmosť, nesprávny strih alebo šitie nemožno spoľahlivo odstrániť domácim praním."),
            ("Môže sa jersey sušiť na vešiaku?", "Iba ak to dovoľuje etiketa a mokrý kus sa vlastnou hmotnosťou nevyťahuje. Ťažké, viskózové a vlnené úplety často potrebujú väčšiu oporu."),
            ("Na koľko stupňov prať jersey?", "Neexistuje jedna teplota pre všetok jersey. Riaďte sa zložením, farbou, potlačou, elastanom a symbolom prania na hotovom výrobku."),
        ],
    },
    {
        "title": "Čo je popelín: hladká košeľová tkanina, vlastnosti a starostlivosť",
        "link": "co-je-popelin-hladka-koselova-tkanina-vlastnosti-a-starostlivost",
        "meta": "Čo je popelín, ako sa líši od Oxfordu, kepru či saténu a ako prať bavlnenú alebo zmesovú popelínovú košeľu bez blednutia, zrazenia a poškodenia goliera.",
        "short": "Popelín je jemná, hladká a pevne pôsobiaca tkanina, nie jedno konkrétne vlákno. Sprievodca vysvetľuje väzbu, zloženie, krčivosť, pranie košieľ a bezpečné žehlenie.",
        "answer": "Popelín je spravidla ľahká až stredne ľahká husto tkaná látka s jednoduchou plátnovou väzbou a hladkým povrchom. Často sa používa na košele, blúzky, šaty, detské oblečenie a ľahké bytové textílie. Názov však neurčuje vláknové zloženie: popelín môže byť bavlnený, polyesterový, zmesový alebo obsahovať elastan. Preto nemožno z názvu odvodiť jednu teplotu prania. Skontrolujte etiketu hotového výrobku, golierové výstuže, potlač a najcitlivejšiu zložku; perte s podobnými farbami, nepreplňujte bubon, presne dávkujte prostriedok a košeľu vyberte hneď po cykle. Hladký povrch neznamená nekrčivosť a vysoký počet nití sám nedokazuje lepšiu životnosť.",
        "intro": "Popelín vyzerá na prvý pohľad jednoducho: rovná plocha, jemná štruktúra a čistý košeľový vzhľad. Za výsledkom je však kombinácia osnovných a útkových priadzí, ich jemnosti, hustoty, napätia, farbenia a konečnej úpravy. Dve košele označené rovnakým názvom môžu mať rozdielnu savosť, priehľadnosť, krčivosť aj toleranciu tepla. Praktická starostlivosť preto nezačína tvrdením, že popelín sa perie na určitej teplote, ale čítaním zloženia a symbolov konkrétneho výrobku.",
        "quick": [
            "<strong>Popelín je názov látky:</strong> bežne ide o plátnovú väzbu, nie o samostatné vlákno uvedené v percentách.",
            "<strong>Hladkosť vzniká kombináciou:</strong> jemnosť priadze, hustota, väzba a dokončenie sú rovnako dôležité ako bavlna či polyester.",
            "<strong>Košeľa má viac materiálov:</strong> golier, manžety, výstuž, niť, gombíky a potlač môžu mať vlastné hranice.",
            "<strong>Krčenie nie je iba vlastnosť názvu:</strong> ovplyvňuje ho vlákno, hustota, úprava, odstreďovanie, sušenie aj čas vo vlhkom bubne.",
            "<strong>Počet nití nie je známka sám osebe:</strong> bez jemnosti, kvality priadze a dokončenia nehodnotí celý výrobok.",
        ],
        "overview_heading": "Ako je popelín vytvorený a prečo pôsobí hladko",
        "overview": [
            "Plátnová väzba strieda každú osnovnú priadzu nad a pod susednými útkovými priadzami v jednoduchom rytme 1/1. Vzniká veľa väzných bodov, ktoré pomáhajú stabilite plochy a vytvárajú rovnomerný vzhľad. Pri popelíne sa zvyčajne používajú jemné priadze a kompaktná konštrukcia; historické a obchodné definície sa však môžu líšiť podľa trhu. Preto je bezpečnejšie opisovať konkrétny výrobok cez jeho merateľnú konštrukciu než cez jednu absolútnu definíciu názvu.",
            "Osnova vedie v pozdĺžnom smere látky a počas tkania je pod napätím, útok sa vkladá naprieč. Rozdielna jemnosť alebo hustota týchto sústav môže vytvoriť veľmi jemné priečne rebro, ktoré nie je to isté ako výrazná diagonála kepru. Hladký povrch môže dobre niesť potlač a pôsobiť upravene, ale pri veľmi ľahkom materiáli môže zároveň presvitať a zvýrazniť záhyby.",
            "Názov popelín nemožno zameniť s údajom 100 % bavlna. Nariadenie EÚ požaduje označenie vláknového zloženia, takže spotrebiteľ má hľadať bavlnu, polyester, elastan alebo inú zložku osobitne. Rovnaká väzba z bavlny prijíma vlhkosť a schne inak než polyesterová zmes. Zloženie tiež mení žehlenie, statiku, zápach a spôsob, akým látka reaguje na dlhé nosenie.",
        ],
        "table1_heading": "Popelín a príbuzné košeľové látky: čo sa líši",
        "table1_intro": "Názvy sa v obchode niekedy používajú voľne. Tabuľka ukazuje typické konštrukčné rozdiely, ale konkrétne zloženie, gramáž a úpravu treba vždy overiť na výrobku.",
        "table1_headers": ["Látka alebo väzba", "Typická štruktúra", "Pocit a vzhľad", "Dôležitá hranica"],
        "table1_rows": [
            ("Popelín", "Kompaktná plátnová väzba z jemných priadzí, niekedy s jemným priečnym efektom.", "Hladký, čistý, ľahký až stredne ľahký povrch.", "Môže byť z rôznych vlákien a mať odlišnú nekrčivú či inú úpravu."),
            ("Oxford", "Variácia plátnovej košíkovej väzby, často s hrubším útkom.", "Textúrovanejší a opticky uvoľnenejší košeľový povrch.", "Názov nevypovedá automaticky o hrúbke každej konkrétnej košele."),
            ("Keper alebo twill", "Väzné body vytvárajú viditeľnú diagonálu a dlhšie flotáže.", "Mäkšie splývanie alebo robustnejší vzhľad podľa konštrukcie.", "Diagonála neznamená automaticky vyššiu odolnosť vo všetkých smeroch."),
            ("Saténová väzba", "Dlhšie flotáže vytvárajú hladší a lesklejší povrch.", "Lesk a plynulejší dotyk, ale vyššia citlivosť na zatrhnutie podľa priadze.", "Satén je väzba; nemusí byť z hodvábu."),
            ("Jersey", "Slučkový úplet namiesto dvoch prepletených sústav priadzí.", "Prirodzená pružnosť a odlišná stabilita okrajov.", "Nie je tkanina a pri rovnakom vláknovom zložení sa správa inak."),
        ],
        "sections": [
            {
                "heading": "Popelín nie je samostatné textilné vlákno",
                "paragraphs": [
                    "Na etikete vláknového zloženia sa objavujú názvy ako bavlna, polyester alebo elastan, nie popelín ako percentuálna zložka. Popelín opisuje látku a jej konštrukčný charakter. To je prakticky dôležité, pretože bavlnený popelín môže viac sať a krčiť sa, polyesterová zmes rýchlejšie schnúť a popelín s elastanom vyžaduje nižšie tepelné zaťaženie podľa pokynov výrobcu.",
                    "Ak e-shop uvádza iba slovo popelín bez zloženia, informácia nestačí na rozhodnutie o praní ani komforte. Pri nákupe hľadajte percentá vlákien, gramáž, nepriehľadnosť, typ úpravy a symboly. Pri hotovej košeli sa navyše pýtajte na výstuž goliera a manžiet, pretože práve tie môžu obmedziť sušičku alebo žehlenie viac než samotná plocha látky.",
                ],
            },
            {
                "heading": "Plátnová väzba, hustota a jemnosť priadze",
                "paragraphs": [
                    "Plátnová väzba má časté prekríženia osnovy a útku, čo podporuje stabilitu a rovnomernú plochu. Výsledný dotyk však závisí od hrúbky priadze, počtu nití na jednotku dĺžky, zákrutu, napätia pri tkaní a dokončenia. Veľmi jemná hustá tkanina môže pôsobiť hladko, kým hrubšia priadza pri rovnakom základnom rytme vytvorí výraznejší povrch.",
                    "Aktuálna ISO 7211-2:2024 ponúka tri metódy určenia počtu nití na centimeter podľa charakteru tkaniny. Meria sa osnova a útok, nie neurčitý pocit hustoty medzi prstami. Toto číslo však nehovorí, či je priadza kvalitná, rovnomerná, pevná alebo vhodne dokončená. Pri porovnaní košieľ preto nepriraďujte hustote význam, ktorý samotná metóda nemeria.",
                ],
            },
            {
                "heading": "Hladký povrch, priedušnosť a pocit pri nosení",
                "paragraphs": [
                    "Ľahký bavlnený popelín môže pôsobiť sviežo, pretože nie je objemný a bavlna prijíma časť vlhkosti. To neznamená, že každý popelín má rovnakú priedušnosť. Hustota, hrúbka, apretúra, živica, potlač a zmes syntetických vlákien menia prenos vzduchu aj vodnej pary. Marketingové slovo ľahký preto nenahrádza meranie ani skúsenosť pri reálnom strihu.",
                    "Priliehavá košeľa s hustou väzbou a podšívkou môže byť teplejšia než voľný model z podobnej látky. Manžety, golier a dvojité diely znižujú prúdenie lokálne. Rozdiel medzi savosťou, priedušnosťou a rýchloschnutím podrobne vysvetľuje článok <a href=\"/n/priedusnost-savost-a-rychloschnutie-ako-citat-vlastnosti-textilu\">ako čítať vlastnosti textilu</a>.",
                ],
            },
            {
                "heading": "Prečo sa popelín krčí a čo znamená nekrčivá úprava",
                "paragraphs": [
                    "Záhyb vzniká, keď sa priadze a vlákna po stlačení či ohnutí nevrátia úplne do pôvodného usporiadania. Bavlnená tkanina môže po navlhčení a sušení vytvoriť výrazné pokrčenie, no výsledok ovplyvňuje hustota, jemnosť priadze, predzrazenie a konečná úprava. Košeľa ponechaná hodinu mokrá v bubne bude spravidla pokrčenejšia než kus vybratý a vyrovnaný hneď.",
                    "Označenie easy care alebo nekrčivá úprava môže znamenať osobitné chemické a mechanické dokončenie, nie zmenu plátnovej väzby. Stále treba rešpektovať symbol žehlenia a teplotu. Úprava nemusí odstrániť všetky záhyby a môže sa opotrebovať. Nepridávajte vysokú teplotu naslepo; najprv skúste košeľu správne zavesiť, vyrovnať švy a žehliť mierne vlhkú v povolenom režime.",
                ],
                "callout": {
                    "title": "Ako znížiť pokrčenie bez agresívneho zásahu",
                    "items": [
                        "Neplňte bubon po okraj; košeľa potrebuje priestor na pohyb a oplach.",
                        "Zvoľte primerané odstreďovanie podľa etikety, nie automaticky maximum.",
                        "Vyberte košeľu hneď po skončení programu, pretrepte ju a zarovnajte golier, légu a manžety.",
                        "Sušte na vhodne širokom vešiaku iba vtedy, ak mokrá hmotnosť nedeformuje ramená.",
                        "Žehlite podľa najcitlivejšej zložky a skúšku urobte na nenápadnom mieste.",
                    ],
                    "background": "#f7fbf8",
                    "border": "#dbe5de",
                },
            },
            {
                "heading": "Bavlnený popelín a zmes s polyesterom",
                "paragraphs": [
                    "Bavlnený popelín môže dobre prijímať pot a poskytovať prirodzený dotyk, no zvyčajne schne pomalšie a môže sa viac krčiť než zmes. Polyesterová zložka môže urýchliť schnutie a zvýšiť rozmerovú stabilitu, ale môže meniť statiku, zadržiavanie mazu a tepelný pocit. Výsledok závisí od podielu aj konštrukcie, nie od jednoduchého súboja dobrého a zlého vlákna.",
                    "Praktické rozdiely rozoberá sprievodca <a href=\"/n/polyester-vs-bavlna-rozdiely-pri-noseni-prani-a-voni\">polyester verzus bavlna</a>. Pri zmesi nastavte žehličku a sušičku podľa citlivejšej zložky a úpravy. Bavlnený názov nie je povolenie na bielenie alebo vysokú teplotu; farebná niť, elastan, výstuž a potlač môžu taký postup vylúčiť.",
                ],
            },
            {
                "heading": "Popelín s elastanom a priliehavé košele",
                "paragraphs": [
                    "Malý podiel elastanu pomáha košeli prispôsobiť sa pohybu, no môže zvýšiť citlivosť na teplo, chlór a dlhé napätie. Veľmi tesný strih zároveň zaťažuje švy, gombíkovú légu a oblasti lakťov bez ohľadu na deklarovanú pružnosť. Ak sa látka pri nosení vlní okolo gombíkov, príčinou môže byť veľkosť a strih, nie nedostatok pracieho prostriedku.",
                    "Po praní košeľu nenaťahujte nasilu do pôvodnej šírky a nesušte na horúcom radiátore. Jemne zarovnajte diely v medziach prirodzeného tvaru. Pri žehlení použite teplotu povolenú štítkom a nezostávajte dlho na jednom mieste. Detailné hranice zmesí s elastanom približuje článok <a href=\"/n/bavlna-a-elastan-starostlivost-o-tricka-rifle-a-spodnu-bielizen\">bavlna a elastan</a>.",
                ],
            },
            {
                "heading": "Ako prať popelínovú košeľu bez poškodenia goliera",
                "paragraphs": [
                    "Rozopnite gombíky podľa pokynov výrobcu, vyberte odnímateľné výstuže goliera a skontrolujte vrecká. Košeľu otočte naruby, ak chcete chrániť farbu a potlač, ale silne znečistený golier najprv lokálne ošetrite vhodným prostriedkom. Látku nedrhnite kefou agresívne; trenie môže zosvetliť farbu a poškodiť hrany goliera skôr než plochu košele.",
                    "Perte s podobnými farbami a ľahkými kusmi. Ťažké rifle a uteráky zvyšujú trenie a môžu košeľu v preplnenom bubne silno pokrčiť. Program, teplotu a odstreďovanie vyberte zo štítku. Presná dávka gélu a dostatok vody na oplach sú užitočnejšie než nadbytok produktu, ktorý môže zostať vo výstužiach a zmeniť dotyk.",
                ],
            },
            {
                "heading": "Mastný golier, pot a škvrny na hladkej látke",
                "paragraphs": [
                    "Golier a manžety zachytávajú kožný maz, kozmetiku a pot. Na hladkom svetlom povrchu sú okraje viditeľné skoro, ale prudké drhnutie môže vytvoriť svetlejšiu stopu. Najprv overte stálofarebnosť na skrytom mieste, použite malé množstvo kompatibilného prípravku a nechajte ho pôsobiť iba podľa návodu. Škvrnu nezapekajte žehličkou ani sušičkou pred kontrolou výsledku.",
                    "Pri opakovanom sivom golieri skontrolujte, či košeľa neostáva dlho spotená, či dávka zodpovedá tvrdej vode a či bubon nie je preplnený. Silná vôňa nemá zakrývať zvyšky mazu. Ak je košeľa označená iba na chemické čistenie, domáci vodný postup môže poškodiť výstuž alebo tvar aj vtedy, keď samotný popelín pôsobí prateľne.",
                ],
            },
            {
                "heading": "Farba, potlač a blednutie popelínu",
                "paragraphs": [
                    "Veľká rovná plocha popelínu dobre ukazuje sýtu farbu aj presnú potlač, ale rovnako zviditeľní blednutie na hranách, golieri a miestach trenia. Farbu ovplyvňuje typ farbiva, väzba na vlákno, voda, prací prostriedok, teplota, oxidanty, trenie a slnečné sušenie. Tmavý názov farby sám nehovorí, ako dobre odtieň odolá cyklom.",
                    "Košeľu perte naruby s podobnými farbami a nepoužívajte bielidlo bez povolenia. Pri pruhovaných alebo kontrastných dieloch sa riaďte výrobcom. Článok <a href=\"/n/stalofarebnost-textilu-preco-farby-blednu-pri-prani-svetle-a-treni\">o stálofarebnosti textilu</a> vysvetľuje, prečo skúšky pri praní, svetle a trení odpovedajú na odlišné otázky.",
                ],
            },
            {
                "heading": "Sušenie, žehlenie a skladovanie popelínu",
                "paragraphs": [
                    "Košeľu po cykle vyberte, jemne pretrepte a zarovnajte légu, švy, golier a manžety. Vhodne široký vešiak môže znížiť záhyby, ale mokrý ťažší kus nesmie vytiahnuť ramená. Sušičku použite iba pri povolenom symbole; teplo môže zmeniť zmes, zraziť bavlnu, poškodiť elastan alebo deformovať lepenú výstuž.",
                    "Žehlite z rubu alebo cez ochrannú tkaninu podľa farby a úpravy, najmä pri potlači a lesklých stopách. Začnite nižšou povolenou teplotou a postupujte po dieloch bez dlhého tlaku na jednom mieste. Košele skladujte suché a s dostatočným priestorom; natlačená skriňa obnoví ostré záhyby aj po starostlivom žehlení.",
                ],
            },
            {
                "heading": "Švy, posun nití a opotrebovanie pri lakťoch",
                "paragraphs": [
                    "Kompaktná plátnová väzba býva stabilná, no pevnosť hotovej košele závisí od jemnosti priadze, šitia, prídavku na šev a napätia pri nosení. Pri tesnom rukáve sa látka namáha pri lakti a ramene; pri nevhodnom šve sa nite môžu posúvať alebo plocha praskne vedľa stehu. Vyšší počet nití automaticky nezaručuje silnejší šev.",
                    "Ak sa pri šve otvárajú medzery bez pretrhnutej nite, môže ísť o posun priadzí; ak je plocha prerežaná stehmi, mechanizmus je iný. Sprievodca <a href=\"/n/pevnost-sva-a-posun-niti-preco-oblecenie-praska-pri-svoch\">o pevnosti šva a posune nití</a> pomáha rozlíšiť opravu od problému veľkosti či konštrukcie.",
                ],
            },
            {
                "heading": "Ako vybrať popelínovú košeľu alebo šaty",
                "paragraphs": [
                    "Pozrite látku proti rozptýlenému svetlu, jemne ju pokrčte v dlani a sledujte návrat. Skontrolujte rovnosť osnovy a útku pri leme, nadväznosť vzoru, švy a výstuž. Na leto môže byť dôležitá ľahkosť a voľný strih, na pracovnú košeľu nepriehľadnosť, stabilný golier a jednoduché žehlenie. Jedno univerzálne najlepšie prevedenie neexistuje.",
                    "Porovnajte zloženie a gramáž, ale žiadne číslo nečítajte izolovane. Článok <a href=\"/n/gramaz-latky-co-znamena-gsm-pri-uterakoch-oblieckach-a-trickach\">o gramáži GSM</a> vysvetľuje hmotnosť plochy a jej limity. Pri online nákupe hľadajte detailnú fotografiu väzby, informáciu o pružnosti, úprave a transparentné symboly starostlivosti; neurčité označenie prémiový popelín nestačí.",
                ],
                "callout": {
                    "title": "Popelín vhodný na účel, nie iba podľa názvu",
                    "items": [
                        "Na formálnu košeľu sledujte nepriehľadnosť, golier, presnosť švov a správanie pri žehlení.",
                        "Na letné šaty zohľadnite voľnosť strihu, podšívku, prúdenie a zachovanie tvaru po sedení.",
                        "Na detské oblečenie kontrolujte mäkkosť švov, farbu, časté pranie a reálnu toleranciu sušenia.",
                        "Na tvorenie overte predzrazenie a farbenie ešte pred strihaním drahého projektu.",
                    ],
                },
            },
        ],
        "table2_heading": "Popelín po praní: čo znamenajú najčastejšie prejavy",
        "table2_intro": "Skôr než zmeníte teplotu alebo pridáte ďalší produkt, určte, či ide o zvyšky, rozmerovú zmenu, farbu, výstuž alebo mechanické poškodenie.",
        "table2_headers": ["Prejav", "Možné vysvetlenie", "Čo skontrolovať", "Bezpečný ďalší krok"],
        "table2_rows": [
            ("Košeľa je veľmi pokrčená", "Preplnenie, vysoké odstreďovanie, dlhý čas vo vlhkom bubne alebo prirodzená krčivosť.", "Náplň, program, úpravu a sušenie.", "Vyberať hneď, zarovnať a žehliť iba podľa symbolu."),
            ("Golier sa zvlnil", "Rozdielne zrazenie vrstiev, poškodená výstuž alebo nevhodné teplo.", "Výstuž, sušičku, žehlenie a pôvodný stav.", "Nevyrovnávať extrémnym teplom; pri novom kuse riešiť kvalitu."),
            ("Tmavé hrany zosvetleli", "Trenie, agresívne lokálne čistenie alebo nízka stálofarebnosť.", "Miesto poškodenia, použitý prípravok a kontakt s tvrdými kusmi.", "Prať naruby a znížiť zbytočné trenie; farbu nemožno gélom obnoviť."),
            ("Povrch je tuhý", "Zvyšky prostriedku, tvrdá voda, apretúra alebo tepelné poškodenie.", "Dávku, oplach, náplň a zloženie.", "Opraviť dávkovanie; nepridávať aviváž bez overenia účelu a znášanlivosti."),
            ("Pri šve sa tvoria medzery", "Posun nití, tesný strih alebo nevhodná konštrukcia šva.", "Či sú priadze celé, veľkosť a napätie pri pohybe.", "Nenosiť pod ďalším ťahom a opravu ukotviť v zdravej ploche."),
        ],
        "steps_heading": "Prvé pranie popelínovej košele bez zbytočného rizika",
        "steps": [
            "Prečítajte zloženie a symboly vrátane sušičky a žehlenia; skontrolujte odnímateľné výstuže goliera.",
            "Ošetrite golier a manžety lokálne bez agresívneho drhnutia a najprv overte farbu na skrytom mieste.",
            "Košeľu perte s podobnými farbami a ľahkými kusmi v nepreplnenom bubne na povolenom programe.",
            "Prací gél dávkujte podľa náplne, znečistenia a tvrdosti vody; nepoužívajte bielenie bez povolenia.",
            "Po skončení programu košeľu ihneď vyberte, pretrepte a zarovnajte golier, légu, švy a manžety.",
            "Sušte a žehlite podľa najcitlivejšej zložky a výstuže, nie podľa všeobecného predpokladu o bavlne.",
        ],
        "remember": [
            "Je popelín bavlnený, polyesterový, elastický alebo viacvláknová zmes?",
            "Obsahuje košeľa lepenú výstuž, potlač, kontrastnú niť alebo citlivé gombíky?",
            "Povoľuje etiketa zvolenú teplotu, odstreďovanie, sušičku a paru?",
            "Je bubon dostatočne voľný a bez ťažkých drsných kusov?",
            "Bol golier ošetrený kompatibilne bez silného trenia a neovereného bielidla?",
        ],
        "mistakes": [
            "Považovať popelín za synonymum stopercentnej bavlny.",
            "Hodnotiť kvalitu iba podľa jedného čísla počtu nití.",
            "Prať košeľu s ťažkými rifľami a uterákmi v preplnenom bubne.",
            "Drhnúť golier tvrdou kefou a vytvoriť svetlé mechanické poškodenie.",
            "Nechať košeľu dlho mokrú v bubne a potom riešiť záhyby extrémnym teplom.",
            "Žehliť výstuž, elastan alebo potlač podľa najvyššej teploty vhodnej pre čistú bavlnu.",
        ],
        "expert_heading": "Odbornejší pohľad: väzba, počet nití a limity názvu",
        "expert": [
            "CottonWorks opisuje plátnovú väzbu ako rytmus 1/1 s vysokým počtom prekrížení osnovy a útku. Táto základná schéma pomáha vysvetliť stabilnú hladkú plochu, ale neurčuje priadzu, hustotu, gramáž ani povrchovú úpravu konkrétneho popelínu. Obchodný názov preto nemožno použiť ako úplnú materiálovú špecifikáciu.",
            "ISO 7211-2:2024 určuje tri metódy počítania nití na centimeter v tkaninách a ASTM D3775-17(2023) osobitne pracuje s počtom osnovných a útkových nití. Výsledok je konštrukčný údaj, nie celkové skóre kvality. Bez jemnosti priadze, väzby, hmotnosti, pevnosti, stálofarebnosti a dokončenia môže byť porovnanie dvoch čísel zavádzajúce.",
            "Európske pravidlá označovania vláknového zloženia oddeľujú názvy vlákien od ostatných obchodných informácií. Pre spotrebiteľa to znamená, že popelín treba čítať spolu s percentami vlákien a symbolmi GINETEX. Domáce pranie môže obmedziť blednutie, zvyšky a mechanické poškodenie, ale nedokáže zmeniť nekvalitnú priadzu, chybnú výstuž alebo zle navrhnutý šev.",
        ],
        "source_intro": "Zdroje vysvetľujú plátnovú väzbu, normované meranie počtu nití a oddelené označovanie vláknového zloženia. Neposkytujú jednu univerzálnu definíciu ani prací režim pre všetky výrobky predávané pod názvom popelín.",
        "sources": [
            ("CottonWorks: základné plátnové väzby", COTTONWORKS_PLAIN),
            ("ISO 7211-2:2024: počet nití na jednotku dĺžky tkaniny", ISO_WOVEN_COUNT),
            ("ASTM D3775-17(2023): osnovné a útkové nite v tkanine", ASTM_WOVEN_COUNT),
            ("EÚ 1007/2011: označovanie vláknového zloženia textilu", EU_FIBRE_LABEL),
            ("GINETEX: význam symbolov ošetrovania", GINETEX),
        ],
        "product_intro": "Pri bežnej prateľnej popelínovej košeli je dôležité odstrániť maz z goliera a manžiet bez nadmerného dávkovania a drhnutia. Výber prostriedku musí rešpektovať farbu, zmes, výstuž a symboly.",
        "product_text": "Prací gél z marseillského mydla je určený na bežnú domácu bielizeň. Na vhodný bavlnený alebo zmesový popelín ho dávkujte podľa etikety, tvrdosti vody a náplne; košeľu perte s podobnými farbami a nechajte jej priestor na oplach.",
        "product_limit": "Gél neodstráni výrobnú krčivosť, poškodenú výstuž, vyblednuté hrany ani posun priadzí. Nie je automaticky vhodný pre každý elastický, špeciálne upravený alebo iba chemicky čistiteľný výrobok.",
        "category_intro": "Pri výbere gélu porovnajte určenie pre farbu a materiál, dávkovanie a znášanlivosť s najcitlivejšou časťou košele. Hladká látka nepotrebuje viac produktu, ale presný postup a dôkladný oplach.",
        "category_text": "V kategórii nájdete pracie gély pre bežnú domácu bielizeň. Pred použitím overte zloženie popelínu, golierovú výstuž, elastan, potlač a symboly; osobitný pokyn výrobcu má prednosť.",
        "related": [
            ("Čo je bavlna", ARTICLE_COTTON),
            ("Polyester verzus bavlna", ARTICLE_POLYESTER_COTTON),
            ("Bavlna a elastan", ARTICLE_COTTON_ELASTANE),
            ("Stálofarebnosť textilu", ARTICLE_COLORFASTNESS),
            ("Pevnosť šva a posun nití", ARTICLE_SEAMS),
            ("Čo znamená gramáž GSM", ARTICLE_GSM),
        ],
        "faq_title": "popelín a starostlivosť o košele",
        "faq": [
            ("Je popelín vždy bavlnený?", "Nie. Popelín je názov látky a bežne plátnovej konštrukcie. Môže byť bavlnený, polyesterový, zmesový alebo obsahovať elastan; percentá nájdete na etikete."),
            ("Aký je rozdiel medzi popelínom a Oxfordom?", "Popelín býva jemnejší a hladší v kompaktnej plátnovej väzbe. Oxford je zvyčajne textúrovanejšia variácia košíkovej väzby, často s hrubším útkom."),
            ("Krčí sa popelín?", "Môže sa krčiť, najmä bavlnený. Mieru mení hustota, priadza, úprava, odstreďovanie, sušenie a čas ponechaný vo vlhkom bubne."),
            ("Na koľko stupňov prať popelínovú košeľu?", "Podľa etikety hotovej košele. Zloženie, farba, elastan, potlač a výstuž neumožňujú stanoviť jednu teplotu pre všetok popelín."),
            ("Môže ísť popelín do sušičky?", "Iba pri povolenom symbole. Teplo môže zraziť bavlnu, poškodiť elastan, zmeniť lepenú výstuž alebo zvýšiť pokrčenie pri presušení."),
            ("Je vyšší počet nití vždy lepší?", "Nie. Počet nití opisuje časť konštrukcie, ale nehodnotí jemnosť a kvalitu priadze, pevnosť, farbenie, gramáž, dokončenie ani šitie hotového výrobku."),
        ],
    },
    {
        "title": "Čo je perkál: hustá tkanina na obliečky, vlastnosti a pranie",
        "link": "co-je-perkal-husta-tkanina-na-obliecky-vlastnosti-a-pranie",
        "meta": "Čo je perkál, ako sa líši od saténu a flanelu, čo znamená počet nití a ako prať perkálové obliečky bez zvyškov, zrazenia a poškodenia farby.",
        "short": "Perkál je hladká, husto tkaná látka v plátnovej väzbe, obľúbená najmä na obliečky. Sprievodca vysvetľuje konštrukciu, dotyk, počet nití aj správne pranie.",
        "answer": "Perkál je spravidla jemná a kompaktná tkanina v plátnovej väzbe, pri ktorej sa osnovné a útkové nite pravidelne striedajú nad a pod sebou. Typický je hladký matný povrch a svieži, pevnejší dotyk, no názov perkál neurčuje jedno vlákno ani jednu kvalitatívnu triedu. Môže byť z bavlny, zmesi alebo iného materiálu a jeho správanie menia priadze, hustota, farbenie aj konečná úprava. Obliečky preto perte podľa etikety hotového výrobku, so zapnutými uzávermi, podobnými farbami, primeranou náplňou a presnou dávkou gélu. Vyšší počet nití automaticky neznamená lepšiu priedušnosť, pevnosť ani životnosť a horúce pranie nie je vhodné pre každý farebný či upravený perkál.",
        "intro": "Názov perkál sa často používa ako skratka pre kvalitné posteľné prádlo, ale sám osebe nepovie, z čoho je látka vyrobená, ako jemná je priadza ani ako bola dokončená. Dve obliečky s rovnakým obchodným označením môžu mať rozdielnu hmotnosť, nepriehľadnosť, krčivosť, stálofarebnosť aj pocit na pokožke. Praktický výber preto nestojí na jednom čísle z obalu. Dôležité je rozumieť plátnovej väzbe, čítať zloženie a symboly a pri praní rešpektovať aj zips, gombíky, potlač a šitie hotového výrobku.",
        "quick": [
            "<strong>Perkál nie je vlákno:</strong> názov opisuje typ husto tkanej látky; percentá bavlny alebo iných vlákien treba hľadať osobitne.",
            "<strong>Typický je matný svieži dotyk:</strong> výsledok však mení jemnosť priadze, hustota, úprava, pranie a sušenie.",
            "<strong>Počet nití má hranice:</strong> meria časť konštrukcie, nie mäkkosť, pevnosť, farbu, šitie ani celkovú kvalitu obliečky.",
            "<strong>Pranie určuje etiketa:</strong> univerzálna teplota pre všetok perkál neexistuje, najmä pri farbách, zmesiach a funkčných úpravách.",
            "<strong>Tvrdosť po praní má viac príčin:</strong> skontrolujte dávku, náplň, oplach, tvrdosť vody a presušenie skôr než pridáte ďalší produkt.",
        ],
        "overview_heading": "Ako vzniká perkál a prečo pôsobí sviežo",
        "overview": [
            "Základom perkálu je plátnová väzba 1/1: každá osnovná niť sa pravidelne prekladá nad a pod susednými útkovými niťami. Veľké množstvo väzných bodov vytvára rovnomernú a pomerne stabilnú plochu bez diagonály typickej pre keper a bez dlhých flotáží saténovej väzby. Samotná schéma však neurčuje hrúbku ani kvalitu. Jemné česané priadze v kompaktnej zostave vytvoria iný výsledok než hrubšie, nerovnomerné alebo silno upravené priadze v rovnakej väzbe.",
            "CottonWorks opisuje perkál ako hladkú plátnovú tkaninu často vyrábanú z česaných priadzí a pri posteľných plachtách uvádza bežný rozsah 160 až 300 nití. Ide o užitočný opis typických výrobkov, nie o zákonnú hranicu a nie o dôkaz, že číslo 300 je vždy lepšie než 200. Spôsob počítania, jemnosť priadze, viacnásobné priadze, gramáž, pevnosť a dokončenie môžu význam čísla zásadne zmeniť.",
            "Svieži alebo chladivejší prvý dotyk perkálu súvisí s hladkým povrchom, nízkym objemom a tým, ako látka odvádza teplo a vlhkosť v konkrétnej konštrukcii. Nemožno z toho vyvodiť, že každý perkál je najpriedušnejšia alebo najlepšia letná obliečka. Hustá apretúra, zmes vlákien, viac vrstiev, nepremokavá ochrana matraca aj mikroklíma spálne môžu výsledok zmeniť viac než samotný názov tkaniny.",
        ],
        "table1_heading": "Perkál, satén, flanel a jersey: rozdiel nie je iba v mäkkosti",
        "table1_intro": "Porovnanie opisuje typické konštrukcie. Konkrétna obliečka môže mať inú vláknovú zmes, gramáž, úpravu a odporúčanú starostlivosť.",
        "table1_headers": ["Materiál alebo konštrukcia", "Typická stavba", "Bežný pocit", "Čo názov nezaručuje"],
        "table1_rows": [
            ("Perkál", "Kompaktná plátnová väzba s pravidelným prekladaním 1/1.", "Hladký, matný, svieži a spočiatku pevnejší dotyk.", "Konkrétne vlákno, počet nití, priedušnosť, stálofarebnosť ani životnosť."),
            ("Bavlnený satén", "Saténová väzba s dlhšími flotážami priadze na povrchu.", "Hladší, splývavejší a často lesklejší povrch.", "Že ide o hodváb alebo že bude odolnejší proti zatrhnutiu."),
            ("Flanel", "Tkanina s mechanicky počesaným povrchom, ktorý vytvára vlas.", "Mäkký, teplejší a menej hladký dotyk.", "Jedno vláknové zloženie ani rovnakú tvorbu žmolkov pri všetkých výrobkoch."),
            ("Jersey obliečky", "Slučkový úplet namiesto osnovy a útku v tkanej ploche.", "Pružnejší, mäkký a tričkový pocit.", "Rozmerovú stabilitu, podiel bavlny ani odolnosť švov."),
            ("Mušelín", "Voľnejšia plátnová tkanina, niekedy vo viacerých vrstvách.", "Vzdušnejší, mäkký a výraznejšie textúrovaný povrch.", "Rovnakú hustotu, nepriehľadnosť alebo spôsob prania všetkých výrobkov."),
        ],
        "sections": [
            {
                "heading": "Perkál nie je synonymum stopercentnej bavlny",
                "paragraphs": [
                    "Na etikete sa vláknové zloženie uvádza percentami a názvami uznaných vlákien. Perkál je obchodné a konštrukčné označenie látky, preto môže byť zo 100 % bavlny, zo zmesi bavlny s polyesterom alebo z iného deklarovaného materiálu. Bavlnený perkál bude prijímať a uvoľňovať vlhkosť inak než zmesový, no ani údaj 100 % bavlna nepovie, aká jemná je priadza, aká je hustota alebo akým procesom prešlo farbenie.",
                    "Pri nákupe preto oddeľte tri vrstvy informácií: vláknové zloženie, konštrukciu látky a starostlivosť o hotový výrobok. Zips môže byť polyesterový, niť môže mať inú tepelnú odolnosť a potlač či nekrčivá úprava môže obmedziť bielenie a sušičku. Vysvetlenie vlastností samotnej bavlny nájdete v článku <a href=\"/n/co-je-bavlna-vlastnosti-vyhody-nevyhody-a-starostlivost\">čo je bavlna a ako sa o ňu starať</a>.",
                ],
            },
            {
                "heading": "Plátnová väzba a veľké množstvo väzných bodov",
                "paragraphs": [
                    "V plátnovej väzbe sa každá niť často kríži s niťami druhého systému. To obmedzuje dĺžku voľných úsekov na povrchu a pomáha vytvoriť rovnomernú matnú plochu. Zároveň však časté ohyby jemných priadzí ovplyvňujú dotyk a pevnosť. Ak je konštrukcia príliš kompaktná alebo silno dokončená, látka môže pôsobiť tuhšie; ak je príliš riedka, môže presvitať a pri namáhaní sa môžu nite posúvať.",
                    "Pojem hustý preto nie je absolútna známka. Rovnováha osnovy a útku, zákrut priadze, jej rovnomernosť a napätie pri tkaní rozhodujú spolu. Na hotovej obliečke navyše záleží na strihu, kvalite šva a presnosti rozmerov. Domáce pranie nevylepší riedku konštrukciu ani krivý šev, môže však obmedziť zbytočné trenie a rozmerové zmeny, ak sa dodrží štítok.",
                ],
            },
            {
                "heading": "Počet nití: čo sa skutočne meria",
                "paragraphs": [
                    "ISO 7211-2:2024 stanovuje metódy určenia počtu nití na jednotku dĺžky tkaniny. Osnova a útok sa vyhodnocujú ako konkrétny konštrukčný údaj. ASTM D3775-17(2023) pracuje podobne s počtom osnovných a útkových nití. Výsledok môže pomôcť porovnávať podobné látky merané rovnakým spôsobom, ale neposudzuje mäkkosť, farbu, pevnosť šva, chemickú úpravu ani to, či sa vám pod obliečkou bude príjemne spať.",
                    "Marketingové číslo môže byť ovplyvnené tým, či sa počítajú jednotlivé zložky viacnásobnej priadze alebo skutočné priadze v ploche. Bez transparentnej metodiky sa dve hodnoty nedajú automaticky porovnať. Samostatný návod <a href=\"/n/pocet-niti-pri-oblieckach-co-znamena-thread-count-a-co-o-kvalite-nehovori\">o počte nití pri obliečkach</a> vysvetľuje, prečo treba číslo čítať spolu s jemnosťou priadze, gramážou a spracovaním.",
                ],
            },
            {
                "heading": "Jemnosť, česanie a kvalita priadze",
                "paragraphs": [
                    "Jemnejšia priadza umožňuje umiestniť do rovnakej šírky viac nití bez toho, aby bola tkanina neprimerane ťažká. Pri česanej bavlnenej priadzi sa odstraňuje časť kratších vlákien a vlákna sa lepšie usporiadajú, čo môže podporiť hladší a rovnomernejší povrch. Neznamená to však nulovú chlpatosť, nemožnosť žmolkovania ani automatickú odolnosť proti roztrhnutiu.",
                    "Pevnosť priadze závisí od dĺžky a kvality vlákien, zákrutu, rovnomernosti a spracovania. Veľmi jemná tkanina môže byť príjemná, no pri chybe priadze alebo agresívnom trení sa opotrebuje. Pri výbere si všímajte priesvit proti svetlu, nepravidelné hrubé miesta, vytiahnuté nite a rovnosť väzby. Jedno číslo na obale tieto znaky nenahradí.",
                ],
            },
            {
                "heading": "Svieži dotyk, mäknutie a povrchová úprava",
                "paragraphs": [
                    "Nový perkál môže byť na dotyk pevnejší a po niekoľkých správnych praniach prirodzene zmäknúť. Časť prvého dojmu môže vytvárať výrobná apretúra, kalandrovanie alebo iné dokončenie povrchu. Po ich postupnom odstránení sa dotyk zmení, čo nie je automaticky chyba. Naopak, drsný pocit po praní môže pochádzať zo zvyškov prostriedku, tvrdej vody, presušenia alebo poškodených vlákien.",
                    "Nesnažte sa nové obliečky zmäkčiť nadmerným množstvom gélu či aviváže. Nadbytok musí práčka odplaviť a môže na kompaktnej látke zanechať film. Aviváž môže meniť savosť a dotyk a nie je povinnou súčasťou každého prania. Najprv použite presnú dávku, primeranú náplň a program s účinným oplachom podľa etikety.",
                ],
                "callout": {
                    "title": "Keď je perkál po praní tvrdý",
                    "items": [
                        "Overte dávku prostriedku podľa hmotnosti suchej náplne, znečistenia a tvrdosti vody.",
                        "Naplňte bubon tak, aby sa veľké kusy mohli rozvinúť a voda medzi nimi cirkulovala.",
                        "Skontrolujte, či obliečky nezostali stočené v jednej prikrývke alebo zachytené zipsom.",
                        "Nepresušujte ich pri vysokej teplote a nenechávajte dlho na prudkom slnku bez potreby.",
                        "Ak vidíte alebo cítite zvyšky, riešte oplach a príčinu dávkovania, nie ďalšiu vrstvu produktu.",
                    ],
                    "background": "#f7fbf8",
                    "border": "#dbe5de",
                },
            },
            {
                "heading": "Perkál verzus bavlnený satén",
                "paragraphs": [
                    "Rozdiel medzi perkálom a bavlneným saténom je predovšetkým vo väzbe, nie v tom, že jeden musí byť bavlna a druhý hodváb. Perkál má pravidelné krátke preklady plátnovej väzby, saténová väzba necháva dlhšie úseky priadze na povrchu. Satén preto často pôsobí hladšie, lesklejšie a splývavejšie, zatiaľ čo perkál býva matnejší a sviežo pevný.",
                    "Dlhšie flotáže saténu môžu byť podľa priadze citlivejšie na zatrhnutie; častejšie väzné body perkálu zas nie sú zárukou nezničiteľnosti. Komfort závisí od zloženia, hustoty, hmotnosti, úpravy a klímy. Výber podľa sezóny a potenia rozoberá článok <a href=\"/n/bavlna-lan-saten-alebo-flanel-ake-obliecky-vybrat-podla-sezony-a-potenia\">bavlna, ľan, satén alebo flanel</a>.",
                ],
            },
            {
                "heading": "Priedušnosť, potenie a teplota v spálni",
                "paragraphs": [
                    "Priedušnosť označuje prestup vzduchu, kým schopnosť pracovať s vlhkosťou zahŕňa prijatie, rozvod a odparenie vody. Hladký bavlnený perkál môže mnohým ľuďom pôsobiť príjemne v teple, ale tento pocit nie je univerzálny laboratórny výsledok pre všetky výrobky s rovnakým názvom. Hustota, farbivo, úprava aj chránič matraca menia mikroklímu lôžka.",
                    "Ak sa v noci výrazne potíte, nestačí vymeniť iba obliečku. Sledujte prikrývku, matrac, pyžamo, teplotu, vlhkosť a vetranie. Textil treba po navlhnutí nechať vyschnúť a prať v rozumnom intervale. Praktické odporúčania k hygiene nájdete v článku <a href=\"/n/ako-casto-prat-postelne-pradlo\">ako často prať posteľné prádlo</a>.",
                ],
            },
            {
                "heading": "Prvé pranie perkálových obliečok",
                "paragraphs": [
                    "Pred prvým použitím si prečítajte etiketu, pretože nový výrobok môže obsahovať prebytočné farbivo, apretúru a stopy z výroby alebo balenia. Zapnite zips či gombíky, obliečky otočte podľa odporúčania výrobcu a perte ich oddelene alebo s farebne kompatibilnými ľahkými textíliami. Nepoužívajte preventívne najvyššiu teplotu ani bielidlo.",
                    "Prvé pranie môže uvoľniť napätie z tkania a dokončenia, preto sa rozmery mierne zmenia aj pri správnom postupe. Výrobca má takú zmenu zohľadniť v rozmeroch a pokynoch. Ak sa obliečka výrazne skrúti, zrazí alebo pustí neprimerané množstvo farby pri dodržanom štítku, zdokumentujte stav a zvážte reklamáciu namiesto opakovaného horúceho prania.",
                ],
            },
            {
                "heading": "Ako prať perkálové obliečky pri bežnej údržbe",
                "paragraphs": [
                    "Obliečky roztrieďte podľa farby a povolenej teploty, uzavrite zipsy a skontrolujte, či v rohoch nezostali drobné predmety. Veľké kusy nekombinujte s množstvom uterákov a odevov s háčikmi, ktoré zvyšujú trenie. Bubon nepreplňte; kompaktná plachta musí mať priestor rozvinúť sa, aby sa voda a prostriedok dostali na celý povrch a následne sa vypláchli.",
                    "Teplotu a program vyberte podľa symbolu, farby, znečistenia a hygienickej potreby. Všeobecné tvrdenie, že všetky bavlnené obliečky sa musia prať na 60 °C, ignoruje potlač, zmes, úpravu a pokyn výrobcu. Kompletný postup vrátane triedenia a sušenia ponúka návod <a href=\"/n/ako-spravne-prat-obliecky-kompletny-sprievodca-starostlivostou\">ako správne prať obliečky</a>.",
                ],
            },
            {
                "heading": "Dávkovanie gélu, tvrdá voda a oplach",
                "paragraphs": [
                    "Dávka sa má riadiť návodom produktu, hmotnosťou suchej bielizne, mierou znečistenia a tvrdosťou vody. Priveľa gélu nezvýši mechanicky kapacitu práčky ani priestor medzi obliečkami. V preplnenom bubne môže byť oplach nerovnomerný a zvyšky sa prejavia tuhším, lepkavým alebo silno voňajúcim povrchom. Príliš málo prostriedku pri tvrdej vode zas nemusí zvládnuť maz a minerály.",
                    "Ak sa problém opakuje, zistite miestnu tvrdosť vody, vážte typickú náplň a skontrolujte zásuvku aj údržbu práčky. Dodatočný oplach môže pomôcť pri jednorazovom predávkovaní, ale nenahrádza opravu dávky. Pri citlivej pokožke je obzvlášť dôležitý čistý oplach, vhodný prostriedok a dodržanie pokynov zdravotníka pri známej alergii.",
                ],
            },
            {
                "heading": "Zrazenie, krivé švy a skrútené obliečky",
                "paragraphs": [
                    "Bavlnené vlákna a tkanina môžu po navlhčení, mechanickom pohybe a teple uvoľniť vnútorné napätia. Rozmerová zmena závisí od priadze, konštrukcie, predzrazenia, programu a sušenia. Ak sa osnova a útok nezmršťujú rovnako alebo bol diel vystrihnutý mimo smeru, obliečka sa môže skrútiť. Domáce žehlenie vie tvar mierne zarovnať, nie odstrániť výrobnú chybu.",
                    "Pred praním nového drahšieho setu môžete zmerať dĺžku a šírku na rovnej ploche a po cykle porovnať za rovnakých podmienok. Nespoliehajte sa na odhad mokrého kusu. Článok <a href=\"/n/preco-sa-oblecenie-zrazi-po-prani-teplota-vlakna-susicka-a-prevencia\">prečo sa textil zrazí po praní</a> vysvetľuje rozdiel medzi relaxačnou zmenou, plstením a tepelným poškodením.",
                ],
            },
            {
                "heading": "Sušenie perkálu bez zbytočného presušenia",
                "paragraphs": [
                    "Po skončení programu obliečky ihneď vyberte, rozmotajte a pretrepte. Sušenie na vzduchu znižuje potrebu tepelného zaťaženia, no silné priame slnko môže postupne meniť farbu. Sušičku použite iba pri povolenom symbole a zvoľte primeraný režim. Dlhé presušenie môže zvýrazniť pokrčenie, tvrdosť dotyku a rozmerovú zmenu.",
                    "Veľké kusy sa v sušičke môžu zvinúť do balíka, v ktorom je vonkajšia časť suchá a vnútorná vlhká. Prerušte cyklus iba bezpečným spôsobom podľa návodu spotrebiča, kusy uvoľnite a po skončení overte suchosť rohov a švov. Vlhké obliečky neukladajte do uzavretej skrine, pretože parfum ani aviváž nevyriešia zatuchnutie z nedosušenia.",
                ],
            },
            {
                "heading": "Žehlenie, mangľovanie a výrazné záhyby",
                "paragraphs": [
                    "Perkál možno často žehliť, ale povolenú teplotu určuje etiketa konkrétnej zmesi, farby a úpravy. Najľahšie sa vyrovná ešte mierne vlhký. Žehlite z rubu pri tmavých farbách a potlači, aby ste znížili riziko lesklých stôp. Zips, plastové gombíky a elastické časti nevystavujte priamemu vysokému teplu.",
                    "Domáci alebo profesionálny mangeľ vytvára tlak a teplo na veľkej ploche, preto nie je automaticky vhodný pre každú potlač, výšivku či uzáver. Pred použitím overte symboly a odporúčanie výrobcu. Cieľom nie je dosiahnuť papierovú tvrdosť, ale vyrovnať tkaninu bez spálenia, zmeny farby alebo poškodenia úpravy.",
                ],
            },
            {
                "heading": "Žmolky, odreté miesta a malé trhliny",
                "paragraphs": [
                    "Hladký perkál môže časom vytvoriť žmolky, najmä ak sú na povrchu kratšie vlákna, látka sa trie o drsné pyžamo alebo sa perie s uterákmi a otvorenými zipsami. Žmolkovanie nie je totožné s nečistotou a ďalší prací prostriedok ho neodstráni. Agresívne holenie tenkej tkaniny môže prerezať nite a vytvoriť slabé miesto.",
                    "Trhlina pri šve môže signalizovať nevhodný prídavok, posun nití alebo napätie príliš malej obliečky na objemnej prikrývke. Trhlina uprostred môže súvisieť s opotrebovaním, ostrým predmetom alebo lokálne oslabenou priadzou. Opravu urobte skôr, než sa otvor zväčší, a záplatu ukotvite v zdravej ploche bez príliš hustého stehu.",
                ],
            },
            {
                "heading": "Ako vybrať perkálové obliečky podľa reálnych potrieb",
                "paragraphs": [
                    "Najprv si určte rozmer matraca, prikrývky a vankúša, typ uzáveru a toleranciu zrazenia deklarovanú výrobcom. Potom porovnajte vláknové zloženie, hustotu, gramáž, nepriehľadnosť, dotyk a pokyny starostlivosti. Pre človeka, ktorý nechce žehliť, môže byť dôležitejšia úprava a správanie po sušení než maximálne číslo nití; pri citlivej pokožke zas hladké švy a dôkladný oplach.",
                    "Pri online nákupe hľadajte presné zloženie a fotografie detailu väzby, nie iba slová hotelová kvalita. Overte, či sú údaje o počte nití vysvetlené a či sa dajú porovnávať. Gramáž je užitočný údaj o hmotnosti plochy, nie o celej kvalite; jej význam rozoberá článok <a href=\"/n/gramaz-latky-co-znamena-gsm-pri-uterakoch-oblieckach-a-trickach\">čo znamená GSM</a>.",
                ],
                "callout": {
                    "title": "Čo si pri perkále všímať ešte pred kúpou",
                    "items": [
                        "Presné percentá vlákien, nie iba slovo bavlnený alebo prírodný.",
                        "Rozmer po vypraní, typ uzáveru a spracovanie vnútorných švov.",
                        "Zrozumiteľnú informáciu o počte nití a spôsob, akým ju výrobca interpretuje.",
                        "Symboly prania, sušenia a žehlenia, ktoré zodpovedajú vašej domácej rutine.",
                        "Možnosť reklamovať výrazné skrútenie, pustenie farby alebo chybu väzby pri správnej starostlivosti.",
                    ],
                },
            },
        ],
        "table2_heading": "Perkál po praní: príčina sa neskrýva vždy v látke",
        "table2_intro": "Rovnaký prejav môže mať viac príčin. Najprv skontrolujte etiketu, dávkovanie, náplň a sušenie, až potom meňte celý postup.",
        "table2_headers": ["Prejav", "Možné príčiny", "Čo overiť", "Rozumný ďalší krok"],
        "table2_rows": [
            ("Obliečka je tvrdá alebo lepkavá", "Nadbytok prostriedku, slabý oplach, tvrdá voda, apretúra alebo presušenie.", "Dávku, hmotnosť náplne, program, tvrdosť vody a sušenie.", "Pri zvyškoch zopakovať oplach a pri ďalšom praní opraviť dávku."),
            ("Rozmer sa výrazne zmenšil", "Relaxačné zrazenie, vysoké teplo, nevhodná sušička alebo nedostatočné predzrazenie.", "Pôvodný rozmer, štítok, teplotu a spôsob sušenia.", "Nenaprávať ešte vyšším teplom; zdokumentovať odchýlku a posúdiť reklamáciu."),
            ("Švy sa skrútili", "Nerovnováha konštrukcie, krivý strih alebo rozdielne zrazenie osnovy a útku.", "Smer väzby, rovnosť dielov a stav pred praním.", "Mierne zarovnať za vlhka; výrobnú chybu riešiť s predajcom."),
            ("Farba bledne na záhyboch", "Trenie, nevhodné bielenie, vysoká teplota alebo slabšia stálofarebnosť.", "Spoločnú náplň, prostriedok, slnko a symboly.", "Prať naruby s podobnými farbami a znížiť zbytočné trenie."),
            ("Vznikajú žmolky alebo tenké miesta", "Krátke vlákna, drsné kusy v náplni, dlhé používanie alebo lokálne trenie.", "Povrch pyžama, zipsy, uteráky a miesto poškodenia.", "Oddeliť drsné kusy a malé poškodenie opraviť skôr, než sa rozšíri."),
            ("Po otvorení skrine cítiť zatuchnutie", "Nedosušené rohy, vlhká skriňa alebo príliš tesné uloženie.", "Suchosť švov, vetranie a relatívnu vlhkosť priestoru.", "Textil znovu vysušiť a odstrániť zdroj vlhkosti, nie pach iba prekryť."),
        ],
        "steps_heading": "Bezpečný postup prania perkálových obliečok",
        "steps": [
            "Prečítajte vláknové zloženie a všetky symboly; skontrolujte potlač, zips, gombíky a poškodené švy.",
            "Roztrieďte obliečky podľa farby a povoleného režimu, uzavrite zapínanie a nenechajte malé kusy vo vnútri návliečky.",
            "Naplňte bubon s rezervou na rozvinutie veľkých kusov a nekombinujte ich s drsnými ťažkými textíliami.",
            "Gél dávkujte podľa suchej hmotnosti, znečistenia a tvrdosti vody; bielidlo použite iba pri výslovnom povolení.",
            "Po skončení obliečky ihneď vyberte, rozmotajte, pretrepte a sušte v povolenom režime bez zbytočného presušenia.",
            "Pred uložením overte suchosť rohov, švov a uzáverov a skladujte ich v suchom, vetranom priestore.",
        ],
        "remember": [
            "Uvádza etiketa bavlnu, zmes alebo iné vlákno a akú teplotu povoľuje?",
            "Sú potlač, zips a švy bez poškodenia a zodpovedajú zvolenému programu?",
            "Majú obliečky v bubne priestor rozvinúť sa a dôkladne sa opláchnuť?",
            "Zodpovedá dávka gélu hmotnosti, znečisteniu a miestnej tvrdosti vody?",
            "Povoľuje výrobca sušičku, žehlenie, bielenie alebo profesionálne mangľovanie?",
        ],
        "mistakes": [
            "Považovať perkál za názov vlákna alebo automaticky za stopercentnú bavlnu.",
            "Vybrať obliečky iba podľa najvyššieho čísla počtu nití bez kontextu.",
            "Naplniť bubon veľkými kusmi tak, že sa nemôžu rozvinúť a opláchnuť.",
            "Použiť najvyššiu teplotu iba preto, že výrobok obsahuje bavlnu.",
            "Pridať viac gélu alebo aviváže pri tvrdom dotyku bez kontroly zvyškov a presušenia.",
            "Uložiť navonok suchú obliečku bez kontroly vlhkých rohov a švov.",
        ],
        "expert_heading": "Odbornejší pohľad: perkál, počet nití a merateľná kvalita",
        "expert": [
            "CottonWorks charakterizuje perkál ako hladkú tkaninu v plátnovej väzbe, bežne z česaných priadzí, a pri plachtách uvádza typický rozsah 160 až 300 nití. Tento opis pomáha rozpoznať konštrukciu, no nie je univerzálnou certifikačnou hranicou pre každý výrobok. Ani matný dotyk či konkrétne číslo nepreukazujú samy osebe pevnosť, priedušnosť alebo životnosť.",
            "ISO 7211-2:2024 a ASTM D3775-17(2023) opisujú meranie počtu osnovných a útkových nití. Ide o užšiu veličinu než celková kvalita. Odborné porovnanie by muselo doplniť jemnosť a pevnosť priadze, hmotnosť, rozmerovú stabilitu, stálofarebnosť, pevnosť proti roztrhnutiu, švy a dokončenie. Hodnota získaná inou metodikou nemusí byť priamo porovnateľná.",
            "Európske nariadenie o názvoch textilných vlákien vyžaduje informáciu o vláknovom zložení oddelene od obchodného názvu. Symboly GINETEX potom komunikujú najprísnejší povolený spôsob ošetrovania hotového výrobku. Z týchto zdrojov vyplýva praktická zásada: názov perkál vysvetlí typ látky, etiketa zloženie a symboly hranice domáceho prania; žiadna z týchto informácií nenahrádza ostatné.",
        ],
        "source_intro": "Zdroje opisujú plátnovú konštrukciu perkálu, normované počítanie nití a označovanie vláknového zloženia. Typický rozsah alebo názov látky nemožno používať ako univerzálne skóre kvality či jednotný prací program.",
        "sources": [
            ("CottonWorks: perkál a jeho typická konštrukcia", COTTONWORKS_PERCALE),
            ("CottonWorks: základné plátnové väzby", COTTONWORKS_PLAIN),
            ("ISO 7211-2:2024: počet nití na jednotku dĺžky", ISO_WOVEN_COUNT),
            ("ASTM D3775-17(2023): počet osnovných a útkových nití", ASTM_WOVEN_COUNT),
            ("EÚ 1007/2011: označovanie vláknového zloženia", EU_FIBRE_LABEL),
            ("GINETEX: význam symbolov ošetrovania", GINETEX),
        ],
        "product_intro": "Pri bežných prateľných perkálových obliečkach rozhoduje správna dávka, voľný pohyb veľkých kusov a účinný oplach. Prostriedok vyberte podľa zloženia, farby, potlače a povolenej teploty.",
        "product_text": "Hypoalergénny prací gél z marseillského mydla je určený na bežnú domácu bielizeň. Pri vhodnom bavlnenom alebo zmesovom perkále ho použite podľa etikety produktu a obliečok, hmotnosti náplne a tvrdosti vody.",
        "product_limit": "Gél nedokáže napraviť nekvalitnú priadzu, krivý strih, poškodenú farbu ani nadmerné zrazenie. Označenie hypoalergénny nevylučuje individuálnu reakciu a nenahrádza dôkladný oplach ani odporúčanie lekára.",
        "category_intro": "Prací gél pre obliečky nevyberajte podľa čo najsilnejšej vône alebo najvyššej dávky. Dôležitejšia je kompatibilita s farbou a zložením, zrozumiteľné dávkovanie a kvalitný oplach.",
        "category_text": "V kategórii pracích gélov môžete porovnať riešenia pre bežnú domácu bielizeň. Pri perkále má vždy prednosť etiketa obliečok a osobitné obmedzenia potlače, zmesi alebo povrchovej úpravy.",
        "related": [
            ("Ako správne prať obliečky", ARTICLE_BEDDING_WASH),
            ("Ako často prať posteľné prádlo", ARTICLE_BEDDING_FREQUENCY),
            ("Počet nití pri obliečkach", ARTICLE_THREAD_COUNT),
            ("Ako vybrať obliečky podľa sezóny", ARTICLE_BEDDING_CHOICE),
            ("Čo je bavlna", ARTICLE_COTTON),
            ("Prečo sa textil zrazí po praní", ARTICLE_SHRINKAGE),
        ],
        "faq_title": "perkálové obliečky a ich pranie",
        "faq": [
            ("Je perkál vždy zo 100 % bavlny?", "Nie. Perkál označuje typ tkaniny, nie jedno vlákno. Môže byť bavlnený alebo zmesový; presné percentá treba čítať na etikete."),
            ("Aký je rozdiel medzi perkálom a bavlneným saténom?", "Perkál má plátnovú väzbu s krátkymi pravidelnými prekladmi a typicky matný svieži dotyk. Saténová väzba má dlhšie flotáže a často hladší, lesklejší povrch."),
            ("Je perkál vhodný na leto?", "Mnohým ľuďom vyhovuje jeho hladký a svieži dotyk, no reálnu tepelnú pohodu mení hustota, zloženie, úprava, prikrývka aj klíma spálne."),
            ("Na koľko stupňov prať perkálové obliečky?", "Na teplotu povolenú etiketou konkrétnych obliečok. Farba, zmes, potlač, uzáver a úprava neumožňujú určiť jednu teplotu pre všetok perkál."),
            ("Prečo je perkál po praní tvrdý?", "Príčinou môže byť apretúra, nadbytok prostriedku, slabý oplach, tvrdá voda alebo presušenie. Najprv skontrolujte dávku, náplň, program a sušenie."),
            ("Znamená vyšší počet nití kvalitnejší perkál?", "Nie automaticky. Počet nití opisuje hustotu v dvoch smeroch, ale nehodnotí jemnosť a pevnosť priadze, gramáž, farbenie, dokončenie ani šitie."),
        ],
    },
    {
        "title": "Čo je ripstop: mriežkovaná tkanina, odolnosť a pranie outdoorového oblečenia",
        "link": "co-je-ripstop-mriezkovana-tkanina-odolnost-a-pranie-outdooroveho-oblecenia",
        "meta": "Čo je ripstop, ako funguje zosilnená mriežka, čo nehovorí o nepremokavosti a ako prať ripstop nohavice, bundy, stany či tašky bez poškodenia úpravy.",
        "short": "Ripstop je tkanina so zosilnenými niťami v pravidelnej mriežke, ktorá má obmedziť šírenie trhliny. Sprievodca vysvetľuje pevnosť, nátery, opravy aj bezpečné pranie.",
        "answer": "Ripstop je konštrukcia tkaniny, do ktorej sú v pravidelných rozostupoch vložené hrubšie, združené alebo inak zosilňujúce nite. Na povrchu často vytvárajú viditeľnú štvorcovú či obdĺžnikovú mriežku. Jej úlohou je pomôcť obmedziť šírenie niektorých trhlín, nie urobiť látku nezničiteľnou. Ripstop môže byť z polyamidu, polyesteru, bavlny alebo zmesi; môže byť bez náteru, impregnovaný, laminovaný aj kombinovaný s membránou. Mriežka preto sama nedokazuje nepremokavosť, priedušnosť, odolnosť proti prepichnutiu ani konkrétnu pevnosť. Pri praní najprv identifikujte celý výrobok a jeho úpravu. Bežný nepoťahovaný prateľný kus ošetrite podľa etikety, kým membrána, DWR, polyuretánový či silikónový náter môžu vyžadovať osobitný prostriedok, nižšie teplo alebo iba lokálne čistenie.",
        "intro": "Na outdoorových nohaviciach, vetrovke, batohu alebo stane vyzerá ripstopová mriežka ako jednoduchý vizuálny znak odolnosti. V skutočnosti je iba jednou časťou systému. O výsledku rozhoduje základná väzba, materiál a jemnosť priadze, rozstup výstuží, švy, náter, membrána, spôsob používania aj kvalita výroby. Rovnaká mriežka môže byť na ultralight nylone aj na pevnej pracovnej tkanine, no tieto výrobky nemožno prať ani mechanicky zaťažovať rovnakým spôsobom. Bez etikety a technickej špecifikácie sa z názvu nedá odvodiť bezpečný univerzálny postup.",
        "quick": [
            "<strong>Ripstop je konštrukcia:</strong> pravidelné zosilňujúce nite tvoria mriežku; nejde o jedno textilné vlákno ani automatickú certifikáciu.",
            "<strong>Trhlina nie je to isté ako prepichnutie:</strong> odolnosť proti roztrhnutiu, ťahu, oderu a prepichnutiu sa hodnotí odlišnými skúškami.",
            "<strong>Mriežka nie je membrána:</strong> z viditeľného ripstopu nemožno určiť vodný stĺpec, priedušnosť ani prítomnosť náteru.",
            "<strong>Najcitlivejšia vrstva určuje pranie:</strong> podšívka, záter, lepený šev, DWR, zips alebo pena môžu mať prísnejšie hranice než základná tkanina.",
            "<strong>Malú trhlinu riešte skoro:</strong> mriežka môže spomaliť rast, ale otvor sa pri ďalšom napätí a praní môže zväčšovať.",
        ],
        "overview_heading": "Ako funguje ripstopová mriežka a kde má hranice",
        "overview": [
            "V bežnej ripstopovej konštrukcii sa do základnej väzby periodicky pridávajú robustnejšie osnovné a útkové nite. Keď sa v tenšej ploche začne trhlina, zosilnenie môže zmeniť rozloženie napätia a sťažiť jej ďalší priamy rast. Účinok však závisí od toho, či trhlina zasiahla mriežku, akou silou a smerom sa materiál namáha a či nie sú poškodené aj výstužné nite. Slovo ripstop preto opisuje zámer konštrukcie, nie absolútny výsledok v každej situácii.",
            "Aktívna americká vojenská špecifikácia MIL-DTL-43637 je konkrétnym príkladom nylonovej tkaniny v plátnovej ripstopovej väzbe s presne definovanými vlastnosťami a skúškami. Nemožno ju však použiť ako dôkaz, že každý komerčný výrobok s viditeľnou mriežkou spĺňa rovnaké hodnoty. Na také tvrdenie by výrobca musel uviesť zhodu, typ skúšky, smer, kondicionovanie vzorky a výsledok konkrétneho materiálu.",
            "Ripstop sa používa na oblečení, spacích vakoch, plachtách, padákoch, batohoch, stanoch aj pracovných textíliách. Každé použitie kladie iné požiadavky. Vetrovka potrebuje nízku hmotnosť a primeranú priedušnosť, podlaha stanu odolnosť proti vode a oderu, batoh pevné švy a vhodný náter. Domáca starostlivosť preto musí začínať účelom a celou skladbou výrobku, nie iba rozpoznaním mriežky.",
        ],
        "table1_heading": "Čo jednotlivé vlastnosti ripstopu znamenajú a čo nie",
        "table1_intro": "Odolnosť nie je jedna veličina. Každá skúška simuluje iný spôsob poškodenia a jej výsledok treba čítať s jednotkami, smerom a podmienkami.",
        "table1_headers": ["Vlastnosť", "Na akú otázku odpovedá", "Typické zaťaženie", "Čo z nej nemožno automaticky odvodiť"],
        "table1_rows": [
            ("Pevnosť proti roztrhnutiu", "Aká sila je potrebná na pokračovanie už začatej trhliny pri danej metóde.", "Rast rezu alebo natrhnutia cez tkaninu.", "Odolnosť proti prvému prepichnutiu, oderu alebo pevnosť šva."),
            ("Pevnosť v ťahu", "Ako sa pás tkaniny správa pri ťahu do porušenia.", "Plošné napätie v osnovnom alebo útkovom smere.", "Rovnaký výsledok pri trhline, šve alebo ostrých hranách."),
            ("Odolnosť proti oderu", "Ako povrch znáša opakované trenie v konkrétnej skúške.", "Kontakt so skalou, popruhmi alebo pracovnou plochou.", "Vodotesnosť po oderení alebo nemožnosť prerezania."),
            ("Odolnosť proti prepichnutiu", "Ako materiál reaguje na koncentrovaný tlak hrotu.", "Tŕň, drôt, ostrá vetva alebo roh predmetu.", "Schopnosť zastaviť šírenie trhliny po vzniku otvoru."),
            ("Hydrostatická odolnosť", "Akému tlaku vody odolá konkrétna skladba pri skúške.", "Dážď alebo tlak vody na náter a švy.", "Priedušnosť, mechanická pevnosť ani životnosť náteru."),
        ],
        "sections": [
            {
                "heading": "Ripstop nie je polyamid ani polyester",
                "paragraphs": [
                    "Názov ripstop opisuje rozloženie zosilňujúcich priadzí. Samotné vlákna môžu byť polyamidové, polyesterové, bavlnené alebo zmesové a výstuž nemusí mať vždy rovnaké zloženie ako základná plocha. Polyamid býva pri nízkej hmotnosti húževnatý, môže však prijímať viac vlhkosti a reagovať na UV inak než polyester. Polyester môže mať inú rozmerovú stabilitu a schnutie. Bavlnené zmesi zas menia savosť, dotyk a čas sušenia.",
                    "Európske označenie vláknového zloženia uvádza percentá uznaných názvov vlákien, kým ripstop sa môže objaviť v opise výrobku. Pri praní preto najprv nájdite etiketu a potom posúďte náter či membránu. Základné vlastnosti nylonu podrobne vysvetľuje článok <a href=\"/n/co-je-polyamid-alebo-nylon-vlastnosti-odolnost-a-pranie\">čo je polyamid a ako sa perie</a>.",
                ],
            },
            {
                "heading": "Mriežka, rozstup a hrúbka zosilňujúcich nití",
                "paragraphs": [
                    "Viditeľná mriežka môže mať niekoľkomilimetrový aj väčší rozstup a zosilnenie môže tvoriť jedna hrubšia alebo viac združených priadzí. Menšie polia teoreticky skracujú cestu, po ktorej trhlina narazí na výstuž, ale zároveň menia hmotnosť, ohybnosť a spôsob koncentrácie napätia. Bez údajov o priadzi a skúške sa nedá povedať, že jemnejšia alebo hrubšia mriežka je všeobecne lepšia.",
                    "Optická pravidelnosť pomáha odhaliť chyby tkania, no nepreukazuje pevnosť. Lacná dekoratívna mriežka môže vyzerať podobne ako technická látka so zverejnenými výsledkami. Pri nákupe hľadajte gramáž, zloženie, typ náteru, skúšobnú metódu a hodnoty v oboch smeroch. Ak predajca uvádza iba slovo odolný bez podmienok, ide o veľmi neúplnú informáciu.",
                ],
            },
            {
                "heading": "Ako mriežka spomaľuje šírenie trhliny",
                "paragraphs": [
                    "Na špičke trhliny sa koncentruje napätie. Keď otvor dosiahne robustnejšiu priadzu, jej väčší prierez alebo odlišná konštrukcia môže preniesť časť zaťaženia a zmeniť smer rastu. Výsledok nie je zaručený: ostrý rez môže prerušiť aj výstuž, vysoké zaťaženie ju môže vytrhnúť a poškodený náter môže preniesť napätie inak než nová tkanina.",
                    "Ripstop preto neznamená, že malý otvor možno ignorovať. Každým použitím sa okraje trú, voda a nečistoty vstupujú medzi vrstvy a trhlina môže postupovať pozdĺž švu alebo mriežky. Dočasná páska môže zabrániť zachytávaniu, ale trvalá oprava musí zohľadniť zloženie, náter, smer síl a požadovanú vodotesnosť.",
                ],
            },
            {
                "heading": "Roztrhnutie, ťah, oder a prepichnutie sú odlišné deje",
                "paragraphs": [
                    "ISO 13937-1 a ASTM D1424 používajú kyvadlový princíp na stanovenie sily potrebnej na pokračovanie rezu v tkanine za definovaných podmienok. Taká skúška nezačína úplne neporušeným materiálom a nehodnotí všetky smery použitia. Pevnosť v ťahu naopak zaťažuje pás alebo vzorku bez rovnakého počiatočného rezu. Výsledky nemožno zamieňať ani porovnávať bez jednotiek a metódy.",
                    "Ostrý tŕň sústreďuje silu do malého bodu, kým oder opakovane oslabuje povrch. Ripstop môže dobre brzdiť rast niektorej trhliny a súčasne sa ľahko prepichnúť alebo poškodiť trením. Rozdiel medzi skúškami a interpretáciou výsledkov rozoberá článok <a href=\"/n/pevnost-textilu-v-tahu-a-proti-roztrhnutiu-co-skusky-hovoria-o-odolnosti\">o pevnosti textilu v ťahu a proti roztrhnutiu</a>.",
                ],
                "callout": {
                    "title": "Ako čítať údaj o pevnosti bez skratky",
                    "items": [
                        "Hľadajte názov a vydanie skúšobnej metódy, nie iba číslo bez kontextu.",
                        "Overte jednotku, smer osnovy a útku a to, či sa hodnotí nová alebo zostarnutá vzorka.",
                        "Nezamieňajte pokračovanie trhliny s prvým prepichnutím alebo s pevnosťou hotového šva.",
                        "Porovnávajte iba materiály testované rovnakou metódou za porovnateľných podmienok.",
                        "Pri kritickom použití vyžadujte technický list alebo zhodu, nie iba viditeľnú mriežku.",
                    ],
                    "background": "#f7fbf8",
                    "border": "#dbe5de",
                },
            },
            {
                "heading": "Ripstop, náter a membrána nie sú to isté",
                "paragraphs": [
                    "Základná tkanina môže mať na povrchu alebo rubovej strane polyuretánový, silikónový či iný náter, môže byť laminovaná s membránou alebo iba hydrofóbne dokončená. Náter obmedzuje prestup vody či vzduchu iným mechanizmom než zosilňujúca mriežka. Viditeľný ripstop preto nepreukazuje vodný stĺpec a hladký rub automaticky nehovorí, z akej chémie je vrstva vyrobená.",
                    "Pri praní je náter často najcitlivejšou časťou. Nevhodný prostriedok, aviváž, rozpúšťadlo, vysoká teplota alebo silné mechanické namáhanie môžu zmeniť priľnavosť a funkciu. Ak ide o membránovú bundu, riaďte sa výrobcom a pozrite si sprievodcu <a href=\"/n/co-je-membranove-oblecenie-vodny-stlpec-priedusnost-a-pranie-bez-poskodenia\">čo je membránové oblečenie</a>.",
                ],
            },
            {
                "heading": "DWR impregnácia a vodoodpudivosť povrchu",
                "paragraphs": [
                    "DWR je povrchová úprava, ktorá pomáha vode tvoriť kvapky a stekať. Nie je to ripstopová mriežka ani samotná nepremokavá bariéra. Keď sa povrch zašpiní alebo úprava opotrebuje, vrchná tkanina sa môže nasýtiť vodou, hoci membrána pod ňou ešte neprepúšťa. Používateľ potom cíti chlad a horší transport pary bez toho, aby látka bola nutne pretrhnutá.",
                    "Obnovenie DWR môže vyžadovať vyčistenie, teplo alebo osobitnú impregnáciu presne podľa výrobcu. Bežný prací gél nie je univerzálnym riešením pre technické vrstvy a aviváž môže byť nevhodná. Softshell, ktorý často používa tkanú syntetickú vrstvu, má vlastné kombinácie; viac vysvetľuje článok <a href=\"/n/co-je-softshell-vrstvy-membrana-impregnacia-a-spravna-starostlivost\">o vrstvách a impregnácii softshellu</a>.",
                ],
            },
            {
                "heading": "Ripstopové nohavice a ľahké bundy",
                "paragraphs": [
                    "Pri nohaviciach skontrolujte vrecká, suché zipsy, kovové prvky, elastické panely a zosilnené kolená. Zaschnuté blato najprv nechajte uvoľniť alebo ho odstráňte mäkkým spôsobom podľa etikety; tvrdá kefa môže poškodiť tenké vlákna a náter. Oblečenie zapnite, otočte podľa odporúčania a perte s podobne ľahkými kusmi, nie s pracovným náradím či otvorenými zipsami.",
                    "Pri vetrovke je dôležitá nízka hmotnosť látky. Aj ripstopová konštrukcia sa môže poškodiť v preplnenom bubne, pri zachytení o háčik alebo pri vysokom odstreďovaní. Ak výrobca povoľuje domáce pranie, vyberte šetrný režim a presnú dávku kompatibilného prostriedku. Silná vôňa alebo veľa peny nie sú dôkazom čistejšieho technického odevu.",
                ],
            },
            {
                "heading": "Stany, batohy a spacáky vyžadujú iný postup",
                "paragraphs": [
                    "Stanová plachta môže mať silikónový alebo polyuretánový náter, podlepené švy a okná či sieťovinu. Bežné pranie v práčke môže byť zakázané pre veľkosť, trenie aj náter. Batoh obsahuje penové výstuže, chrbtový systém, kovové alebo plastové komponenty a lepidlá. Spací vak zas kombinuje škrupinu, výplň a prepážky. Mriežka na povrchu neumožňuje určiť jeden režim pre celý výrobok.",
                    "Vždy použite návod konkrétnej značky. Často je bezpečnejšie lokálne čistenie mäkkou handričkou alebo ručné ošetrenie vo vhodnej nádobe, no ani to nemožno predpísať bez štítku. Veľký nasiaknutý výrobok je ťažký a pri manipulácii môže preťažiť švy. Pred uložením musí byť dokonale suchý aj v záhyboch, výplni a tuneloch.",
                ],
            },
            {
                "heading": "Ako prať bežný nepoťahovaný ripstop",
                "paragraphs": [
                    "Ak etiketa potvrdzuje pranie a výrobok nemá citlivý náter, membránu ani ďalšie obmedzujúce súčasti, roztrieďte ho podľa farby a zloženia. Odstráňte voľné nečistoty, zatvorte zipsy a suché zipsy, uvoľnite sťahovania a skontrolujte malé trhliny. Jemný syntetický ripstop chráňte pred háčikmi a ťažkými drsnými kusmi.",
                    "Program, teplota a odstreďovanie musia vychádzať zo symbolov. Prostriedok dávkujte podľa návodu a tvrdosti vody; nadbytok môže sťažiť oplach a zanechať film. Po cykle výrobok vyberte bez krútenia, vytvarujte a sušte spôsobom povoleným etiketou. Tento postup platí len pre bežnú prateľnú skladbu, nie automaticky pre technické nátery.",
                ],
            },
            {
                "heading": "Čistenie potu, blata, oleja a živice",
                "paragraphs": [
                    "Pot a kožný maz sa hromadia pri páse, golieri a popruhoch, zatiaľ čo blato prináša minerálne častice, ktoré pri trení pôsobia abrazívne. Najprv odstráňte suché častice bez rozotierania. Lokálny prípravok skúste na skrytom mieste a dodržte čas pôsobenia. Silné drhnutie cez ripstopovú mriežku môže vytiahnuť výstužnú priadzu alebo stenčiť náter.",
                    "Olej, živica, repelent a opaľovací krém môžu vyžadovať odlišný postup, no rozpúšťadlá môžu narušiť lamináciu a farbu. Bez pokynu výrobcu nepoužívajte acetón, technický benzín ani agresívny odmasťovač. Pri drahom technickom výrobku je lepšie kontaktovať výrobcu alebo odbornú čistiareň, ktorá pozná konkrétnu skladbu.",
                ],
            },
            {
                "heading": "Sušenie a teplo pri syntetickom ripstope",
                "paragraphs": [
                    "Polyamid a polyester schnú často rýchlejšie než bavlnená tkanina, no náter, podšívka a švy môžu držať vodu dlhšie. Výrobok nevešajte za jeden slabý bod, keď je nasiaknutý a ťažký. Rozložte hmotnosť, obnovte tvar a chráňte ho pred ostrou hranou. Priame vysoké teplo môže deformovať syntetické vlákna, lepidlá a nátery.",
                    "Sušičku alebo tepelnú reaktiváciu DWR použite iba v režime uvedenom výrobcom. Nezamieňajte odporúčanie pre jednu membránovú bundu s pravidlom pre všetky ripstopy. Aj keď základné vlákno toleruje určitú teplotu, pena, páska šva, potlač alebo elastické diely môžu mať nižšiu hranicu.",
                ],
            },
            {
                "heading": "Zatrhnuté výstužné nite a poškodená mriežka",
                "paragraphs": [
                    "Ak sa hrubšia niť zachytí a vytiahne, nestrihajte ju bez rozmyslu. Odstrihnutie môže prerušiť nosnú cestu mriežky a vytvoriť dva voľné konce. Výrobok prestaňte namáhať, prezrite rub a zistite, či ide o slučku bez pretrhnutia alebo o skutočne poškodenú priadzu. Jemné zatiahnutie späť patrí skôr skúsenému opravárovi než agresívnej manipulácii ihlou.",
                    "Prevenciou je zatváranie suchých zipsov, oddelenie odevu od kovových háčikov a opatrnosť pri kroví či skale. Samostatný článok <a href=\"/n/zatrhavanie-textilu-preco-vznikaju-vytiahnute-ocka-a-ako-im-predchadzat\">o zatrhávaní textilu</a> vysvetľuje, prečo vytiahnutá niť nie je škvrna a ako sa líši poškodenie tkaniny od úpletu.",
                ],
            },
            {
                "heading": "Oprava malej trhliny a zachovanie vodotesnosti",
                "paragraphs": [
                    "Malú čistú trhlinu najprv zbavte nečistôt a dokonale vysušte. Opravná páska alebo záplata musí byť kompatibilná s povrchom; silikónovaný nylon, polyuretánový náter a bežná textília nemusia prijať rovnaké lepidlo. Zaoblite rohy záplaty, nechajte dostatočný presah a dodržte čas vytvrdnutia. Pri nosnom šve či bezpečnostnom vybavení vyhľadajte odbornú opravu.",
                    "Zašitie môže zastaviť otvor, ale ihla vytvorí ďalšie perforácie a pri nepremokavej skladbe treba šev znovu utesniť vhodným systémom. Príliš hustý steh môže prerezať tenkú tkaninu ako perforácia papiera. Po oprave skontrolujte plochu bez extrémneho napínania a pred výpravou ju otestujte v bezpečných podmienkach.",
                ],
            },
            {
                "heading": "Lepkavý, odlupujúci sa alebo zapáchajúci náter",
                "paragraphs": [
                    "Lepkavosť alebo odlupovanie rubovej vrstvy môže signalizovať degradáciu náteru, migráciu zmäkčovadiel, hydrolýzu alebo nevhodné skladovanie. Pranie tento proces spravidla nevráti späť a agresívny prostriedok ho môže urýchliť. Odlupujúce čiastočky nie sú bežná špina a nemožno ich bezpečne prilepiť univerzálnym pracím gélom.",
                    "Výrobok odfoťte, dohľadajte materiál a kontaktujte výrobcu. Ak náter zabezpečuje vodotesnosť, po strate súdržnosti sa na pôvodný výkon nespoliehajte. Zápach riešte spolu s príčinou a úplným vysušením; parfumovanie nesmie zakrývať vlhkosť, pleseň ani rozklad vrstvy.",
                ],
            },
            {
                "heading": "UV žiarenie, vek a postupná strata pevnosti",
                "paragraphs": [
                    "Slnečné žiarenie, teplo, vlhkosť a čas môžu meniť polymérne vlákna aj nátery. Blednutie je viditeľný signál, ale mechanická strata nemusí byť vždy zrejmá. Tenká stanová plachta ponechaná dlhodobo na slnku sa môže roztrhnúť pri menšom zaťažení, hoci mriežka zostáva opticky rozpoznateľná.",
                    "Výrobky po použití vyčistite podľa návodu, dokonale vysušte a skladujte mimo vysokého tepla a priameho svetla. Pri bezpečnostne dôležitom použití dodržte interval kontroly a vyradenia výrobcu. Vplyv žiarenia na textil a limity údajov o ochrane rozoberá článok <a href=\"/n/ochrana-textilu-pred-uv-ziarenim-co-znamena-upf-a-co-ju-znizuje\">o UV a UPF</a>.",
                ],
            },
            {
                "heading": "Ako vybrať ripstop podľa použitia, nie podľa vzhľadu",
                "paragraphs": [
                    "Na ľahkú vetrovku sledujte hmotnosť, priedušnosť, strih a ochranu pred vetrom. Na pracovné nohavice sú dôležité oderové zóny, švy a voľnosť pohybu. Pri stane rozhoduje náter, vodný stĺpec, konštrukcia švov a opraviteľnosť. Pri batohu treba hodnotiť celú nosnú sústavu. Jedna tkanina nemôže maximalizovať nízku hmotnosť, mäkkosť, nepriedušnosť aj mechanickú robustnosť bez kompromisov.",
                    "Vyžadujte presné zloženie, gramáž, typ úpravy a relevantné skúšky. Ak výrobca uvádza pevnosť, hľadajte metódu, smer a jednotku. Skontrolujte detail mriežky, rovnosť švov, miesta vpichov a prechody medzi materiálmi. Pri ultralight výrobku akceptujete nižšiu rezervu výmenou za hmotnosť; pri kritickom vybavení má mať prednosť overená špecifikácia a servis.",
                ],
                "callout": {
                    "title": "Ripstop vyberajte ako celý systém",
                    "items": [
                        "Zloženie a gramáž základnej tkaniny aj výstužných priadzí.",
                        "Relevantné skúšky pevnosti, roztrhnutia, oderu alebo vody podľa zamýšľaného použitia.",
                        "Typ náteru, membrány, DWR a konštrukciu či podlepenie švov.",
                        "Spôsob opravy, dostupnosť kompatibilnej pásky a podmienky záruky.",
                        "Symboly údržby, ktoré dokážete reálne dodržiavať doma alebo v odbornom servise.",
                    ],
                },
            },
        ],
        "table2_heading": "Ripstop po používaní alebo čistení: diagnostika bez hádania",
        "table2_intro": "Najprv určte, či je poškodené vlákno, šev, náter alebo iba povrchová vodoodpudivosť. Každý problém potrebuje iné riešenie.",
        "table2_headers": ["Prejav", "Pravdepodobná oblasť", "Čo skontrolovať", "Bezpečný ďalší krok"],
        "table2_rows": [
            ("Malý otvor sa zastavil pri mriežke", "Základná tkanina je natrhnutá, výstuž ešte môže držať.", "Či je výstuž celá, rub, náter a smer napätia.", "Výrobok nezaťažovať a opraviť kompatibilnou záplatou skôr než otvor rastie."),
            ("Vytiahnutá hrubšia niť", "Zatrhnutá výstužná priadza.", "Či je iba vytiahnutá alebo pretrhnutá a či sa otvorila väzba.", "Niť nestrihať naslepo; chrániť miesto a zvoliť odbornú opravu."),
            ("Rub je lepkavý alebo sa lúpe", "Degradácia náteru alebo laminácie.", "Vek, skladovanie, použitú chémiu a odporúčanie výrobcu.", "Nepridávať teplo ani agresívne čistenie; preveriť servis alebo výmenu."),
            ("Povrch rýchlo nasiakne", "Znečistená alebo opotrebovaná DWR, prípadne poškodená bariéra.", "Či voda preniká cez výrobok alebo iba zmáča vrchnú tkaninu.", "Vyčistiť a obnoviť úpravu iba postupom výrobcu; nepovažovať mriežku za vodotesnú."),
            ("Trhlina vznikla pri šve", "Koncentrácia napätia, slabý šev, malý prídavok alebo poškodené ihlové otvory.", "Niť, pásku šva a zdravú plochu okolo poškodenia.", "Pri nosnom či vodotesnom šve zvoliť profesionálnu opravu."),
            ("Po praní ostal silný zápach", "Zvyšky, nedosušená vrstva, kontaminácia alebo degradácia náteru.", "Dávku, oplach, skryté vlhké miesta a stav rubu.", "Odstrániť príčinu a úplne vysušiť; pach neprekrývať ďalšou vôňou."),
        ],
        "steps_heading": "Bezpečný postup pri čistení ripstopového výrobku",
        "steps": [
            "Identifikujte celý výrobok: zloženie, náter, membránu, DWR, podšívku, výplň, penu, zipsy a lepené švy.",
            "Prečítajte symboly a návod výrobcu; ak sa pokyny pre jednotlivé vrstvy líšia, platí najcitlivejšia hranica.",
            "Odstráňte voľné nečistoty, zatvorte zachytávajúce prvky a malé trhliny opravte ešte pred mechanickým praním.",
            "Pri povolenom domácom praní použite kompatibilný prostriedok, presnú dávku, voľný bubon a šetrný povolený program.",
            "Po cykle výrobok nekrúťte, rovnomerne podoprite a sušte bez neovereného vysokého tepla či priameho radiátora.",
            "Po vysušení skontrolujte mriežku, švy, náter a funkciu; technické vlastnosti obnovujte iba postupom výrobcu.",
        ],
        "remember": [
            "Je výrobok iba z bežného ripstopu alebo obsahuje membránu, náter, DWR, penu či lepené diely?",
            "Povoľuje výrobca práčku, konkrétny prostriedok, odstreďovanie, sušičku a tepelnú obnovu úpravy?",
            "Sú zipsy a suché zipsy zatvorené a malé trhliny zabezpečené pred ďalším rastom?",
            "Je uvádzaná odolnosť podložená konkrétnou skúšobnou metódou, jednotkou a smerom?",
            "Je výrobok po čistení suchý aj v švoch, výplni, záhyboch a pod náterom bez známok odlupovania?",
        ],
        "mistakes": [
            "Považovať mriežku za dôkaz nepremokavosti, membrány alebo certifikovanej pevnosti.",
            "Zamieňať odolnosť proti roztrhnutiu s prepichnutím, oderom alebo pevnosťou šva.",
            "Prať stan, batoh, vetrovku a nepoťahované nohavice rovnakým univerzálnym postupom.",
            "Použiť aviváž, rozpúšťadlo alebo vysoké teplo bez overenia náteru a pokynov výrobcu.",
            "Odstrihnúť vytiahnutú výstužnú niť a oslabiť mriežku na oboch koncoch.",
            "Ignorovať malú trhlinu iba preto, že sa dočasne zastavila pri zosilnení.",
        ],
        "expert_heading": "Odbornejší pohľad: skúška trhania nie je univerzálna odolnosť",
        "expert": [
            "MIL-DTL-43637 je aktívna špecifikácia pre konkrétnu ľahkú nylonovú tkaninu v plátnovej ripstopovej väzbe. Definuje materiál a požiadavky pre daný účel, preto je dobrým dôkazom, že ripstop možno technicky presne špecifikovať. Nie je však všeobecnou definíciou všetkých ripstopov na trhu a jej hodnoty sa nesmú pripisovať výrobku, ktorý zhodu nedeklaruje.",
            "ISO 13937-1:2000, potvrdená ako aktuálna v roku 2023, a ASTM D1424-25 opisujú kyvadlové metódy merania sily potrebnej na pokračovanie rezu v tkanine. Výsledok závisí od prípravy vzorky, smeru, rozsahu prístroja a správania materiálu. Niektoré tkaniny alebo smery môžu byť pre konkrétnu metódu nevhodné; preto číslo bez názvu metódy a podmienok nemožno odborne porovnať.",
            "Označenie vlákien podľa pravidiel EÚ a symboly GINETEX dopĺňajú, ale nenahrádzajú technický list. Etiketa povie zloženie a povolené ošetrovanie hotového výrobku, nie automaticky vodný stĺpec, odolnosť proti oderu alebo životnosť náteru. Praktickým dôsledkom je oddeliť štyri otázky: z čoho je látka, ako je utkaná, aké vrstvy nesie a akými skúškami bol overený výkon.",
        ],
        "source_intro": "Zdroje ukazujú konkrétnu špecifikáciu nylonového ripstopu, normované skúšky pokračovania trhliny a pravidlá označovania. Žiadny z nich nepodporuje tvrdenie, že viditeľná mriežka sama zaručuje nepremokavosť alebo univerzálnu odolnosť.",
        "sources": [
            ("DLA Quick Search: aktívna špecifikácia MIL-DTL-43637", DLA_RIPSTOP),
            ("ISO 13937-1: kyvadlová skúška sily pri trhaní", ISO_TEAR),
            ("ASTM D1424-25: odolnosť tkanín proti trhaniu kyvadlom", ASTM_TEAR),
            ("EÚ 1007/2011: označovanie vláknového zloženia", EU_FIBRE_LABEL),
            ("GINETEX: význam symbolov ošetrovania", GINETEX),
        ],
        "product_intro": "Bežný prací gél môže byť voľbou iba pre ripstopový výrobok, ktorého etiketa povoľuje domáce pranie a nevyžaduje osobitný prostriedok pre membránu, náter alebo impregnáciu.",
        "product_text": "Hypoalergénny prací gél z marseillského mydla je určený na bežnú domácu bielizeň. Použite ho len na kompatibilný nepoťahovaný alebo výrobcom povolený ripstop, v presnej dávke a pri režime uvedenom na etikete.",
        "product_limit": "Produkt nie je automaticky vhodný na membrány, DWR, silikónové či polyuretánové nátery, stany, spacáky alebo batohy. Nenahrádza špeciálnu technickú starostlivosť a neobnovuje poškodenú vodotesnosť ani pevnosť.",
        "category_intro": "Pri technickom textile nie je rozhodujúca iba kategória prostriedku. Najprv overte, či výrobca povoľuje bežný gél alebo vyžaduje osobitný prípravok bez zložiek nevhodných pre danú úpravu.",
        "category_text": "V kategórii nájdete pracie gély pre bežnú domácu bielizeň. Na ripstop ich vyberajte iba vtedy, keď to dovoľuje zloženie a úprava konkrétneho výrobku; pokyn výrobcu má vždy prednosť.",
        "related": [
            ("Čo je polyamid alebo nylon", ARTICLE_POLYAMIDE),
            ("Pevnosť v ťahu a proti roztrhnutiu", ARTICLE_TENSILE),
            ("Ako predchádzať zatrhávaniu textilu", ARTICLE_SNAGGING),
            ("Čo je membránové oblečenie", ARTICLE_MEMBRANE),
            ("Čo je softshell", ARTICLE_SOFTSHELL),
            ("Priedušnosť, savosť a rýchloschnutie", ARTICLE_BREATHABILITY),
        ],
        "faq_title": "ripstop a starostlivosť o outdoorový textil",
        "faq": [
            ("Je ripstop nepremokavý?", "Nie automaticky. Mriežka opisuje zosilnenie tkaniny. Nepremokavosť môže zabezpečovať samostatný náter, membrána a utesnenie švov, ktoré musia mať vlastnú špecifikáciu."),
            ("Je ripstop vždy z nylonu?", "Nie. Často je z polyamidu alebo polyesteru, ale existujú bavlnené a zmesové varianty. Presné vláknové zloženie uvádza etiketa."),
            ("Dá sa ripstop prať v práčke?", "Iba ak to povoľuje výrobca celého výrobku. Nepoťahované nohavice, membránová bunda, batoh a stan môžu mať úplne odlišné obmedzenia."),
            ("Zastaví mriežka každú trhlinu?", "Nie. Môže obmedziť rast niektorých trhlín, ale ostrý rez, veľká sila, poškodená výstuž alebo nevhodný smer môžu mriežku prekonať."),
            ("Ako opraviť malú dieru v ripstope?", "Miesto vyčistite a vysušte a použite záplatu či lepidlo kompatibilné s vláknom a náterom. Pri nosnom, bezpečnostnom alebo vodotesnom diele zvoľte odbornú opravu."),
            ("Čo znamená, keď sa náter lepí alebo odlupuje?", "Môže ísť o degradáciu vrstvy vekom, vlhkosťou, teplom alebo chémiou. Pranie ju spravidla neobnoví; overte pokyny výrobcu, servis alebo výmenu."),
        ],
    },
]


def visible_text(markup: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(TAG_RE.sub(" ", markup))).strip()


def article_hrefs(markup: str) -> list[str]:
    return sorted(set(re.findall(r'href="([^"]+)"', markup)))


def fetch_status(url: str) -> dict[str, object]:
    try:
        response = requests.get(
            url,
            headers={"User-Agent": "Codex VEVO batch 44 link preflight"},
            timeout=45,
            allow_redirects=True,
        )
        host = urlparse(url).netloc.lower()
        allowed_automation_block = (
            (host == "www.iso.org" and response.status_code == 403)
            or (host == "eur-lex.europa.eu" and response.status_code == 202)
        )
        automation_note = None
        if host == "www.iso.org" and response.status_code == 403:
            automation_note = "Official ISO page blocks the automated link checker"
        elif host == "eur-lex.europa.eu" and response.status_code == 202:
            automation_note = "Official EUR-Lex page accepts the automated request asynchronously"
        return {
            "url": url,
            "status": response.status_code,
            "final_url": response.url,
            "allowed_automation_block": allowed_automation_block,
            "automation_note": automation_note,
            "ok": response.status_code == 200 or allowed_automation_block,
        }
    except requests.RequestException as exc:
        return {
            "url": url,
            "status": None,
            "final_url": None,
            "allowed_automation_block": False,
            "ok": False,
            "error": str(exc),
        }


def preflight_links(articles: list[dict[str, object]]) -> dict[str, object]:
    target_urls = {f"{BASE}/n/{article['link']}" for article in articles}
    outgoing_urls = {
        urljoin(BASE, href) if href.startswith("/") else href
        for article in articles
        for href in article_hrefs(str(article["long"]))
    }
    all_urls = sorted(target_urls | outgoing_urls)
    with ThreadPoolExecutor(max_workers=6) as executor:
        checks = list(executor.map(fetch_status, all_urls))
    for check in checks:
        if check["url"] in target_urls:
            check["expected_status"] = 404
            check["ok"] = check["status"] == 404
    report = {
        "batch": "batch-44",
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
    candidate_titles = [
        line.strip()
        for line in CANDIDATES.read_text(encoding="utf-8-sig").splitlines()
        if line.strip()
    ]
    article_by_title = {article["title"]: article for article in ARTICLES}
    if len(article_by_title) != len(ARTICLES) or set(candidate_titles) != set(article_by_title):
        raise SystemExit("Candidate titles and article definitions do not match exactly")
    slugs = [str(article["link"]) for article in ARTICLES]
    if len(slugs) != len(set(slugs)):
        raise SystemExit("Batch contains duplicate slugs")

    rendered: list[dict[str, object]] = []
    for article in ARTICLES:
        body = render_article(article)
        public_text = f"{article['title']} {article['short']} {body}"
        if FORBIDDEN_PUBLIC_RE.search(public_text):
            raise SystemExit(f"Forbidden public wording in {article['title']}")
        if FIXED_PRICE_RE.search(visible_text(public_text)):
            raise SystemExit(f"Fixed price found in {article['title']}")
        rendered.append(
            {
                "title": article["title"],
                "short": article["short"],
                "long": body,
                "link": article["link"],
                "date_posted": PUBLISH_DATE,
                "time_posted": "13:00:00",
                "commenting": False,
                "title_tag": article["title"],
                "description": article["meta"],
            }
        )

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(rendered, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    report = preflight_links(rendered)
    if report["failure_count"]:
        failed = [check for check in report["checks"] if not check["ok"]]
        print(json.dumps({"failed_links": failed}, ensure_ascii=False, indent=2))
        raise SystemExit("Batch 44 link preflight failed")

    metrics = []
    for article in rendered:
        body = str(article["long"])
        metrics.append(
            {
                "title": article["title"],
                "slug": article["link"],
                "words": len(WORD_RE.findall(visible_text(body))),
                "h2": len(re.findall(r"<h2\b", body, re.IGNORECASE)),
                "tables": len(re.findall(r"<table\b", body, re.IGNORECASE)),
                "styled_blocks": len(re.findall(r"<div\b[^>]*style=", body, re.IGNORECASE)),
            }
        )
    print(json.dumps({"article_count": len(rendered), "metrics": metrics, "link_preflight": report["failure_count"] == 0}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
