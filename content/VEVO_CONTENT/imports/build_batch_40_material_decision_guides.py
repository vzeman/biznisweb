import html
import json
import re
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests


BASE = "https://www.vevo.sk"
PUBLISH_DATE = "2026-07-16"
CANDIDATES = Path("content/VEVO_CONTENT/batches/batch-40-candidates-2026-07-16.txt")
OUT_JSON = Path("content/VEVO_CONTENT/imports/batch-40-2026-07-16-articles.json")
OUT_PREFLIGHT = Path("content/VEVO_CONTENT/exports/batch-40-2026-07-16-link-preflight.json")

PRODUCT_NAME = "Prací gél hypoalergénny Vevo Ylang Absolute 1L"
PRODUCT_URL = "/p-1627/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l"
CATEGORY_NAME = "Pracie gély"
CATEGORY_URL = "/c/vevo-home-care/pranie/praci-gel"

ISO_WATER_VAPOUR = "https://www.iso.org/standard/85998.html"
ISO_AIR = "https://www.iso.org/standard/16869.html"
AATCC_MOISTURE = "https://members.aatcc.org/store/tm195/591/"
AATCC_ABSORBENCY = "https://members.aatcc.org/store/tm79/499/"
AATCC_DRYING = "https://members.aatcc.org/store/tm200/954/"
ASTM_TERRY = "https://store.astm.org/d4772-26.html"
GINETEX = "https://www.ginetex.net/share/article/4201/care-symbols"
BEDDING_REVIEW = "https://pubmed.ncbi.nlm.nih.gov/38627879/"
SPORT_ODOR = "https://pubmed.ncbi.nlm.nih.gov/25128346/"
POST_EXERCISE = "https://pubmed.ncbi.nlm.nih.gov/37960939/"
WORKWEAR_STUDY = "https://pmc.ncbi.nlm.nih.gov/articles/PMC12029065/"
EU_TEXTILES = "https://single-market-economy.ec.europa.eu/sectors/textiles-ecosystem/textiles-leather-fur_en"

ARTICLE_PROPERTIES = "/n/priedusnost-savost-a-rychloschnutie-ako-citat-vlastnosti-textilu"
ARTICLE_BEDDING = "/n/bavlna-lan-saten-alebo-flanel-ake-obliecky-vybrat-podla-sezony-a-potenia"
ARTICLE_TOWELS = "/n/frote-bambus-alebo-mikrovlakno-ktory-uterak-vybrat-podla-savosti-a-schnutia"
ARTICLE_SPORT = "/n/polyester-polyamid-merino-alebo-elastan-z-coho-ma-byt-sportove-oblecenie"

FORBIDDEN_PUBLIC_RE = re.compile(
    r"longtail|long-tail|long tail|\bkeyword(?:s)?\b|kľúčov\w*\s+slov\w*|"
    r"\bSEO\b|search\s+intent|sub[- ]?quer(?:y|ies)|sub[- ]?query|"
    r"fan[- ]?out|fanout|\bCTA\b",
    re.IGNORECASE,
)


def esc(value):
    return html.escape(str(value), quote=True)


def table(headers, rows):
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


def callout(title, items, *, background="#fffaf5", border="#e6ded2"):
    bullets = "".join(f"<li>{item}</li>" for item in items)
    return (
        f'<div style="border: 1px solid {border}; border-radius: 8px; padding: 18px; '
        f'margin: 22px 0; background: {background};">'
        f'<h2 style="margin-top: 0;">{esc(title)}</h2><ul>{bullets}</ul></div>'
    )


def source_box(article):
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


def commercial_blocks(article):
    return f"""
<div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 24px 0; background: #f7fbf8;">
<h2 style="margin-top: 0;">Prací prostriedok vyberajte až po prečítaní štítku</h2>
<p>{article['product_intro']}</p>
<div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff; margin: 14px 0;">
<h3 style="margin-top: 0;">{PRODUCT_NAME}</h3>
<p>{article['product_text']}</p>
<p><strong>Dôležitá hranica:</strong> {article['product_limit']}</p>
<p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="{PRODUCT_URL}">Pozrieť produkt</a></p>
</div>
</div>
<div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 24px 0; background: #fffaf5;">
<h2 style="margin-top: 0;">Porovnajte pracie gély podľa typu bielizne</h2>
<p>{article['category_intro']}</p>
<div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff; margin: 14px 0;">
<h3 style="margin-top: 0;">{CATEGORY_NAME}</h3>
<p>{article['category_text']}</p>
<p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="{CATEGORY_URL}">Pozrieť kategóriu</a></p>
</div>
</div>
""".strip()


def related_links(items):
    links = "".join(f'<li><a href="{href}">{esc(label)}</a></li>' for label, href in items)
    return f"<h2>Súvisiace návody na VEVO</h2><ul>{links}</ul>"


def faq(title, items):
    parts = [f"<h2>FAQ: {esc(title)}</h2>"]
    for question, answer in items:
        parts.append(f"<h3>{esc(question)}</h3><p>{answer}</p>")
    return "\n".join(parts)


def render_article(article):
    parts = [
        f"<p><strong>Rýchla odpoveď:</strong> {article['answer']}</p>",
        f"<p>{article['intro']}</p>",
        callout("Najdôležitejšie rozhodnutia v skratke", article["quick"]),
        f"<h2>{esc(article['overview_heading'])}</h2>",
    ]
    parts.extend(f"<p>{paragraph}</p>" for paragraph in article["overview"])
    parts.append(f"<h2>{esc(article['table1_heading'])}</h2>")
    parts.append(f"<p>{article['table1_intro']}</p>")
    parts.append(table(article["table1_headers"], article["table1_rows"]))
    for heading, paragraphs in article["sections"]:
        parts.append(f"<h2>{esc(heading)}</h2>")
        parts.extend(f"<p>{paragraph}</p>" for paragraph in paragraphs)
    parts.append(f"<h2>{esc(article['table2_heading'])}</h2>")
    parts.append(f"<p>{article['table2_intro']}</p>")
    parts.append(table(article["table2_headers"], article["table2_rows"]))
    parts.append("<h2>Praktický postup krok za krokom</h2>")
    parts.append("<ol>" + "".join(f"<li>{step}</li>" for step in article["steps"]) + "</ol>")
    parts.append(
        callout(
            "Kontrolný zoznam pred praním",
            article["remember"],
            background="#f7fbf8",
            border="#dbe5de",
        )
    )
    parts.append("<h2>Najčastejšie chyby pri výbere a starostlivosti</h2>")
    parts.append("<ul>" + "".join(f"<li>{item}</li>" for item in article["mistakes"]) + "</ul>")
    parts.append(f"<h2>{esc(article['expert_heading'])}</h2>")
    parts.extend(f"<p>{paragraph}</p>" for paragraph in article["expert"])
    parts.append(source_box(article))
    parts.append(commercial_blocks(article))
    parts.append(related_links(article["related"]))
    parts.append(faq(article["faq_title"], article["faq"]))
    return "\n".join(parts)


ARTICLES = [
    {
        "title": "Priedušnosť, savosť a rýchloschnutie: ako čítať vlastnosti textilu",
        "link": "priedusnost-savost-a-rychloschnutie-ako-citat-vlastnosti-textilu",
        "meta": "Ako rozlíšiť priedušnosť, savosť, odvod potu a rýchloschnutie textilu. Praktické porovnanie materiálov, konštrukcie, testov a údržby.",
        "short": "Priedušnosť, savosť, odvod vlhkosti a rýchloschnutie nie sú rovnaké vlastnosti. Tento sprievodca ukazuje, čo jednotlivé pojmy znamenajú, ako sa merajú, čo ich mení a ako ich čítať pri výbere oblečenia, uterákov alebo posteľnej bielizne.",
        "answer": "Priedušnosť opisuje, ako cez textil prechádza vzduch alebo vodná para, savosť hovorí o prijatí kvapalnej vody a rýchloschnutie o tom, ako rýchlo sa voda z materiálu odparí. Textília môže dobre odvádzať pot od pokožky, ale nemusí nasať veľa vody; môže byť veľmi savá, ale schnúť pomaly. Pri výbere preto nepozerajte iba na názov vlákna. Rozhoduje aj priadza, väzba alebo pletenina, hrúbka, povrchová úprava, strih a počet vrstiev.",
        "intro": "Výrazy ako priedušná látka, funkčný materiál alebo rýchloschnúce tričko znejú jednoznačne, no v praxi môžu opisovať rozdielne mechanizmy. Pri športovom oblečení je dôležitý presun potu od kože a odparovanie. Uterák má naopak vodu rýchlo prijať do veľkej plochy vlákien. Obliečky majú pracovať s teplom, vlhkosťou a dotykom počas niekoľkých hodín. Jedno číslo ani jeden názov materiálu preto nedokážu spoľahlivo predpovedať komfort vo všetkých situáciách.",
        "quick": [
            "<strong>Priedušnosť vzduchu:</strong> zisťuje, ako ľahko vzduch prechádza štruktúrou textílie pri určenom tlakovom rozdiele.",
            "<strong>Odpor proti vodnej pare:</strong> opisuje, ako textil bráni prestupu pary; nie je to to isté ako prúdenie vzduchu cez póry.",
            "<strong>Savosť:</strong> hodnotí, či a ako rýchlo materiál prijme kvapalnú vodu do povrchu a vnútra.",
            "<strong>Transport vlhkosti:</strong> sleduje presun kvapaliny medzi stranami textílie a jej rozvedenie do plochy.",
            "<strong>Rýchloschnutie:</strong> závisí od množstva zadržanej vody, dostupnej plochy, teploty, prúdenia vzduchu a konštrukcie odevu.",
        ],
        "overview_heading": "Prečo sa tieto vlastnosti nedajú zhrnúť slovom funkčný",
        "overview": [
            "Vzduch, vodná para a kvapalná voda sa v textílii správajú odlišne. Voľná sieťovina môže prepúšťať veľa vzduchu, ale po navlhnutí nemusí odvádzať kvapalinu od pokožky. Hustá pletenina môže vzduch prepúšťať menej, no vďaka priadzi a povrchovej energii rozvedie pot do širšej plochy. Keď výrobca uvádza iba všeobecné označenie, chýba informácia o tom, ktorý z týchto javov bol skúšaný.",
            "Rozhoduje celý textilný systém. Rovnaký polyester možno spracovať do tenkého úpletu s kanálikmi, hutného flísu alebo hladkej podšívky. Bavlna môže byť voľne tkaná, česaná, mercerovaná alebo vytvorená do froté slučiek. Výsledok mení jemnosť a krútenie priadze, hustota očiek, mechanická úprava, hydrofilná alebo vodoodpudivá úprava aj opotrebenie po praní.",
            "Dôležité je aj prostredie. Textília schne inak na vešiaku v prievane, na tele pod bundou a zložená v športovej taške. Laboratórne výsledky pomáhajú porovnávať vzorky za definovaných podmienok, ale nie sú priamym sľubom, že každý človek bude cítiť rovnaký komfort. ISO pri meraní tepelného odporu a odporu proti vodnej pare výslovne pracuje s určenými podmienkami skúšky.",
        ],
        "table1_heading": "Päť vlastností, ktoré sa najčastejšie zamieňajú",
        "table1_intro": "Pri porovnávaní výrobkov si najprv položte otázku, či potrebujete pohyb vzduchu, prestup pary, prijatie kvapaliny, jej presun alebo rýchle odparenie. Nasledujúca tabuľka ukazuje praktický rozdiel.",
        "table1_headers": ["Vlastnosť", "Čo sa deje", "Kde je dôležitá", "Čo ju môže skresliť"],
        "table1_rows": [
            ("Priedušnosť vzduchu", "Vzduch prechádza pórmi a medzerami textílie.", "Letné vrstvy, sieťované zóny, podšívky.", "Napnutie látky, vietor, hustota väzby a ďalšia vrstva."),
            ("Prestup vodnej pary", "Vodná para prechádza cez materiál alebo systém vrstiev.", "Spodná vrstva, membrána, posteľná bielizeň.", "Teplota, vlhkosť, kondenzácia a nesprávne porovnanie jednotiek."),
            ("Nasiakavosť", "Kvapalná voda vstúpi do materiálu a zostane v ňom.", "Uteráky, utierky, bavlnené vrstvy.", "Povrchová úprava, zvyšky aviváže a mastnota."),
            ("Odvod kvapalnej vlhkosti", "Voda sa presunie od pokožky a rozvedie do väčšej plochy.", "Športové tričká, ponožky a funkčná bielizeň.", "Kontakt s telom, smer pleteniny a vrstvenie."),
            ("Rýchloschnutie", "Zadržaná voda sa odparí a výrobok sa vráti do suchého stavu.", "Cestovanie, šport, uteráky v malej kúpeľni.", "Objem vody, hrúbka, švy, guma, prúdenie vzduchu."),
        ],
        "sections": [
            (
                "Ako sa meria priedušnosť vzduchu",
                [
                    "Norma ISO 9237 opisuje skúšku priepustnosti vzduchu cez textíliu pri definovanom tlakovom rozdiele. Výsledok je užitočný pri porovnaní podobných vzoriek, ak poznáte skúšobnú plochu a podmienky. Vysoká priepustnosť môže pomôcť pri ochladzovaní prúdením, ale zároveň môže znamenať menšiu ochranu pred vetrom. Preto sa najvyššie číslo nedá automaticky vyhlásiť za najlepšie.",
                    "Doma možno spraviť iba hrubé porovnanie proti svetlu alebo jemnému prúdu vzduchu. Taký pokus odhalí rozdiel medzi hustou tkaninou a sieťovinou, no nedá laboratórnu hodnotu. Navyše môže zvýhodniť látku s veľkými pórmi, ktorá v reálnom odeve leží pod ďalšou vrstvou. Pri membránach si prečítajte aj sprievodcu <a href=\"/n/co-je-membranove-oblecenie-vodny-stlpec-priedusnost-a-pranie-bez-poskodenia\">vlastnosťami membránového oblečenia</a>.",
                ],
            ),
            (
                "Prestup vodnej pary nie je otvorený otvor v látke",
                [
                    "ISO 11092 používa vyhrievanú dosku na určovanie tepelného odporu a odporu proti vodnej pare. Ide o kontrolovanú metódu, ktorá pomáha hodnotiť textílie, odevné zostavy či prikrývky. Výsledok opisuje odpor skúšanej zostavy, nie pocit konkrétneho človeka počas behu, spánku alebo práce. Komfort mení produkcia tepla, potenie, strih, pohyb a okolité prostredie.",
                    "Pri nepremokavej bunde môže vodná para prechádzať materiálom, hoci vzduch cez neho takmer neprúdi. Ak je však vonkajší povrch premoknutý, póry zanesené alebo pod bundou príliš veľa vrstiev, odvod vlhkosti sa zhorší. Označenie priedušný preto čítajte spolu s určením výrobku a návodom na údržbu, nie ako absolútnu záruku suchého pocitu.",
                ],
            ),
            (
                "Savosť a zmáčavosť: prečo kvapka nemusí hneď vsiaknuť",
                [
                    "AATCC TM79 hodnotí absorpciu vody textíliou pomocou stanoveného postupu. Rýchle zmáčanie môže byť dôležité pri uteráku alebo utierke, ale celková kapacita zadržať vodu je ďalšia vlastnosť. Povrch môže kvapku prijať rýchlo, no tenká látka sa skoro nasýti. Hutné froté môže prijať viac vody na celý kus, zároveň však po použití potrebuje viac času a priestoru na vyschnutie.",
                    "Savosť sa môže meniť používaním. Mastnota, telová kozmetika, nadmerné dávkovanie pracieho prostriedku a zmäkčujúci film môžu obaliť povrch vlákien. To neznamená, že každý mäkký uterák je nefunkčný, ale pri náhlom poklese savosti má zmysel skontrolovať dávku, oplach a spôsob sušenia. Praktické dôsledky rozoberá aj článok <a href=\"/n/silikony-v-avivazach-skryty-nepriatel-vasho-pradla1\">prečo uteráky strácajú savosť</a>.",
                ],
            ),
            (
                "Odvod potu závisí od kontaktu a smeru transportu",
                [
                    "Pri funkčnom úplete sa často sleduje, ako kvapalina prejde z vnútornej strany smerom von, ako rýchlo sa rozšíri a aká plocha zostane mokrá. AATCC TM195 spája viac ukazovateľov riadenia kvapalnej vlhkosti. Dve látky môžu mať podobné zloženie, ale rozdielny výsledok vďaka tvaru priadze, hustote očiek alebo úprave jednotlivých strán.",
                    "Odev musí zároveň priliehať natoľko, aby sa pot dostal na vnútorný povrch. Príliš voľná vrstva môže pri intenzívnej aktivite odvod spomaliť; príliš tesná môže obmedziť komfort a prúdenie. Vrchná vrstva nesmie zablokovať odparovanie. Výber športového systému podrobnejšie rozoberá článok <a href=\"/n/polyester-polyamid-merino-alebo-elastan-z-coho-ma-byt-sportove-oblecenie\">z čoho má byť športové oblečenie</a>.",
                ],
            ),
            (
                "Prečo savý materiál nemusí schnúť rýchlo",
                [
                    "Čas schnutia ovplyvňuje, koľko vody textília po praní alebo použití drží a ako ľahko sa táto voda dostane k vzduchu. AATCC TM200 sa venuje rýchlosti schnutia pri určenej skúške. V domácnosti výsledok mení odstreďovanie, rozloženie na sušiaku, teplota, relatívna vlhkosť, prúdenie vzduchu a hrubé miesta ako lemy, vrecká či pásy.",
                    "Tenké mikrovlákno často drží menej vody na celý kus a má veľkú plochu, preto môže schnúť rýchlo. Hrubý bavlnený uterák prijme viac vody a v zloženom stave vytvorí vlhký stred. To nie je chyba jedného materiálu, ale rozdiel funkcie a konštrukcie. Pri kúpe uteráka preto porovnávajte aj <a href=\"/n/gramaz-latky-co-znamena-gsm-pri-uterakoch-oblieckach-a-trickach\">gramáž látky v GSM</a>.",
                ],
            ),
            (
                "Ako čítať tvrdenia na etikete a v popise výrobku",
                [
                    "Hľadajte konkrétny údaj, názov skúšky, jednotku a podmienky. Slovo priedušný bez ďalšieho vysvetlenia nehovorí, či výrobca meral vzduch, vodnú paru alebo iba použil obchodný opis. Pri porovnávaní dvoch hodnôt overte, že používajú rovnakú metódu a jednotku. Čísla z rozdielnych skúšok nemožno zoradiť do jedného rebríčka.",
                    "Zloženie vlákien je užitočný začiatok, ale nie úplný výsledok. Skontrolujte aj gramáž, typ väzby alebo pleteniny, povrch, počet vrstiev a určenie výrobku. Pri starostlivosti má prednosť ošetrovací štítok. GINETEX vysvetľuje, že symboly určujú najnáročnejší povolený proces; nie sú všeobecným odporúčaním použiť najvyššiu teplotu pri každom praní.",
                ],
            ),
        ],
        "table2_heading": "Výber vlastnosti podľa konkrétneho použitia",
        "table2_intro": "Najprv pomenujte situáciu, potom hľadajte vlastnosť. Tak sa vyhnete nákupu textílie, ktorá má pôsobivé označenie, ale rieši iný problém.",
        "table2_headers": ["Použitie", "Priorita", "Kompromis", "Otázka pred nákupom"],
        "table2_rows": [
            ("Beh a intenzívny tréning", "odvod kvapalnej vlhkosti a rýchle schnutie", "tenká látka môže menej chrániť pred vetrom", "Ako sa správa vnútorná a vonkajšia strana úpletu?"),
            ("Turistika vo vrstvách", "prestup pary celou zostavou", "ochrana pred dažďom a vetrom zvyšuje odpor", "Funguje spodná, izolačná aj vrchná vrstva spolu?"),
            ("Kúpeľňový uterák", "rýchle prijatie vody a dostatočná kapacita", "vyšší objem často schne dlhšie", "Vyschne uterák v mojej kúpeľni medzi použitiami?"),
            ("Posteľná bielizeň", "práca s teplom, parou a dotykom", "hladký povrch nemusí znamenať chladný pocit", "Aká je teplota spálne a ako často sa v noci potím?"),
            ("Cestovanie", "nízka mokrá hmotnosť a krátke schnutie", "tenší kus môže pôsobiť menej mäkko", "Mám priestor výrobok úplne rozložiť?"),
        ],
        "steps": [
            "Určite, či riešite vzduch, vodnú paru, kvapalný pot, celkovú savosť alebo čas schnutia.",
            "Prečítajte zloženie, ale súčasne zistite typ väzby, gramáž, počet vrstiev a povrchovú úpravu.",
            "Pri číselnom tvrdení hľadajte metódu a jednotku; porovnávajte iba výsledky získané rovnakým spôsobom.",
            "Zvážte prostredie: teplotu, vlhkosť, vietor, intenzitu pohybu, možnosť sušenia a frekvenciu prania.",
            "Skontrolujte ošetrovací štítok ešte pred nákupom, najmä pri membráne, vlne, elastane alebo lepených detailoch.",
            "Po prvých použitiach sledujte mokré zóny, čas schnutia a pach. Reálna prevádzka odhalí, či celý systém funguje.",
            "Ak sa vlastnosť zhorší, najprv vylúčte nános produktu, preplnený bubon, slabý oplach a nesprávne sušenie.",
        ],
        "remember": [
            "Priedušnosť vzduchu, prestup pary, savosť, transport kvapaliny a schnutie sú samostatné vlastnosti.",
            "Vlákno je iba jedna časť výsledku; rovnako dôležitá môže byť konštrukcia a povrchová úprava.",
            "Domáci test je vhodný na orientačné porovnanie, nie ako náhrada normovanej skúšky.",
            "Najlepší materiál neexistuje bez určenia použitia, prostredia a spôsobu starostlivosti.",
            "Pri praní má prednosť štítok konkrétneho výrobku pred všeobecným návodom na dané vlákno.",
        ],
        "mistakes": [
            "<strong>Zamieňanie savosti za rýchle schnutie.</strong> Materiál môže prijať veľa vody a práve preto schnúť dlhšie.",
            "<strong>Porovnávanie rozdielnych jednotiek.</strong> Hodnoty z rôznych skúšok nemusia opisovať ten istý jav.",
            "<strong>Výber iba podľa názvu vlákna.</strong> Rovnaké zloženie môže mať úplne inú väzbu, hrúbku a úpravu.",
            "<strong>Ignorovanie celého systému vrstiev.</strong> Priedušná spodná vrstva nepomôže, ak vrchná vrstva vlhkosť zablokuje.",
            "<strong>Prekrytie zhoršenej funkcie vôňou.</strong> Pach alebo pomalé schnutie treba riešiť praním, oplachom a sušením.",
            "<strong>Automatické použitie aviváže.</strong> Pri funkčných, savých a niektorých elastických textíliách sa riaďte štítkom a pokynmi výrobcu.",
        ],
        "expert_heading": "Čo laboratórna hodnota vie a čo už musí rozhodnúť používateľ",
        "expert": [
            "Normované skúšky znižujú počet premenných, aby bolo možné porovnať vzorky. ISO 9237 sa zameriava na vzduch, ISO 11092 na tepelný odpor a odpor proti vodnej pare a metódy AATCC samostatne riešia absorpciu, riadenie kvapalnej vlhkosti či schnutie. Ich výsledky sa dopĺňajú, ale nemožno ich zlúčiť do jednej univerzálnej známky komfortu.",
            "Pri nosení alebo spánku vstupuje do výsledku metabolické teplo, množstvo potu, citlivosť pokožky, tlak odevu, pohyb a mikroklíma medzi vrstvami. Dobre navrhnutý výrobok preto kombinuje viac vlastností a môže zámerne prijať kompromis. Vetruodolná vrstva prepustí menej vzduchu, hutný uterák zadrží viac vody a mäkký flanel vytvorí teplejší povrchový pocit.",
            "Najspoľahlivejší výber vznikne spojením technického údaja s reálnou prevádzkou. Hľadajte meranú vlastnosť, porovnávajte rovnaké metódy, prečítajte štítok a potom sledujte, ako výrobok funguje vo vašom prostredí. Tak sa vyhnete očakávaniu, že jedno marketingové slovo vyrieši všetky situácie.",
        ],
        "source_intro": "Použité normy a metódy opisujú rozdielne fyzikálne javy. Odkazy slúžia na vysvetlenie princípu a podmienok skúšania; konkrétny výrobok treba hodnotiť podľa údajov výrobcu a jeho ošetrovacieho štítku.",
        "sources": [
            ("ISO 11092: tepelné vlastnosti a odpor proti vodnej pare", ISO_WATER_VAPOUR),
            ("ISO 9237: priepustnosť vzduchu textíliou", ISO_AIR),
            ("AATCC TM195: riadenie kvapalnej vlhkosti", AATCC_MOISTURE),
            ("AATCC TM79: absorpcia vody textíliou", AATCC_ABSORBENCY),
            ("AATCC TM200: rýchlosť schnutia", AATCC_DRYING),
            ("GINETEX: význam ošetrovacích symbolov", GINETEX),
        ],
        "product_intro": "Pri bežných prateľných textíliách môže vhodný prací gél pomôcť odstrániť pot, kožný maz a nečistoty, ktoré menia zmáčanie a pach materiálu. Dávku prispôsobte tvrdosti vody, miere znečistenia a veľkosti náplne.",
        "product_text": "Hypoalergénny prací gél Vevo Ylang Absolute je konkrétna voľba pre bežnú bielizeň, ak ju povoľuje štítok. Pri funkčných vrstvách použite primerané množstvo, neprepĺňajte bubon a dbajte na úplný oplach.",
        "product_limit": "Nie je univerzálnym riešením pre každý technický materiál. Membrány, vlna, hodváb, vodoodpudivé úpravy a špeciálne športové odevy môžu vyžadovať odlišný prípravok.",
        "category_intro": "Ak periete viac druhov textilu, porovnajte určenie produktu, odporúčané dávkovanie a kompatibilitu s farbou či citlivým materiálom. Samotná intenzita vône nehovorí o vhodnosti pre funkčnú textíliu.",
        "category_text": "V kategórii pracích gélov nájdete riešenia pre bežnú domácu bielizeň. Pred použitím ich porovnajte s ošetrovacím štítkom a pokynmi výrobcu odevu.",
        "related": [
            ("Gramáž látky a význam GSM", "/n/gramaz-latky-co-znamena-gsm-pri-uterakoch-oblieckach-a-trickach"),
            ("Čo je mikrovlákno", "/n/co-je-mikrovlakno-vyhody-nevyhody-savost-a-pranie"),
            ("Membránové oblečenie a priedušnosť", "/n/co-je-membranove-oblecenie-vodny-stlpec-priedusnost-a-pranie-bez-poskodenia"),
            ("Ako vybrať materiál obliečok", ARTICLE_BEDDING),
            ("Ako vybrať uterák", ARTICLE_TOWELS),
            ("Z čoho má byť športové oblečenie", ARTICLE_SPORT),
        ],
        "faq_title": "priedušnosť, savosť a schnutie textilu",
        "faq": [
            ("Je priedušný materiál automaticky rýchloschnúci?", "Nie. Priedušnosť môže opisovať prestup vzduchu alebo vodnej pary, zatiaľ čo schnutie závisí aj od množstva zadržanej vody, hrúbky, plochy a podmienok okolia."),
            ("Ktorý materiál najlepšie odvádza pot?", "Bez údajov o konštrukcii sa to nedá určiť iba podľa názvu vlákna. Pri športe sledujte smer transportu vlhkosti, priliehanie, hrúbku a fungovanie celej zostavy vrstiev."),
            ("Prečo savý uterák v kúpeľni zapácha?", "Môže zadržiavať veľa vody a nestihnúť vyschnúť. Pach podporí aj nános produktu, organické zvyšky, zloženie na háčiku a slabé vetranie."),
            ("Dá sa priedušnosť otestovať doma?", "Iba orientačne. Pohľad proti svetlu alebo pocit prúdenia odhalí veľké rozdiely v pórovitosti, ale nenahradí normovanú skúšku ani nepredpovie komfort pri nosení."),
            ("Zhorší pranie funkčné vlastnosti?", "Môže ich zmeniť nesprávny program, nevhodný prípravok, zvyšky aviváže, slabý oplach alebo poškodenie povrchovej úpravy. Vždy sa riaďte štítkom výrobku."),
            ("Znamená vyššia gramáž menšiu priedušnosť?", "Často rastie množstvo materiálu na plochu, ale priamy záver neplatí. Voľná hrubšia konštrukcia môže prepúšťať viac vzduchu než ľahká husto tkaná látka."),
        ],
    },
    {
        "title": "Bavlna, ľan, satén alebo flanel: aké obliečky vybrať podľa sezóny a potenia",
        "link": "bavlna-lan-saten-alebo-flanel-ake-obliecky-vybrat-podla-sezony-a-potenia",
        "meta": "Bavlna, ľan, satén či flanel na obliečky: rozdiely podľa sezóny, potenia, dotyku, schnutia a údržby bez zjednodušujúcich sľubov.",
        "short": "Materiál obliečok vyberajte podľa teploty spálne, potenia, dotyku, rýchlosti schnutia a ochoty venovať sa údržbe. Bavlna a ľan sú názvy vlákien, zatiaľ čo satén označuje väzbu a flanel česaný povrch. Preto treba vždy čítať celé zloženie a štítok.",
        "answer": "Do teplej spálne a pri nočnom potení býva praktická ľahšia, priedušná a rýchlejšie schnúca konštrukcia. Do chladnej spálne môže byť príjemný flanel s česaným povrchom. Bavlna je univerzálna, ľan pôsobí vzdušne a časom mäkne, satén ponúka hladký povrch, ale môže byť z bavlny, polyesteru aj hodvábu. Neexistuje jeden najlepší materiál pre každého: rozhoduje zloženie, väzba, gramáž, teplota izby, citlivosť pokožky a spôsob prania.",
        "intro": "Pri obliečkach sa často porovnávajú názvy, ktoré nie sú na rovnakej úrovni. Bavlna a ľan hovoria o vlákne. Satén je spôsob tkania s hladšou lícnou stranou a môže mať rôzne vláknové zloženie. Flanel je tkanina s česaným povrchom, najčastejšie bavlnená, ale zloženie treba overiť. Ak tieto pojmy zoradíte ako štyri jednoduché materiály, ľahko prehliadnete vlastnosť, ktorá bude pri spánku rozhodujúca.",
        "quick": [
            "<strong>Teplá spálňa:</strong> uprednostnite ľahšiu konštrukciu, dobrú prácu s vlhkosťou a možnosť úplného vysušenia po praní.",
            "<strong>Chladná spálňa:</strong> česaný povrch flanelu môže vytvoriť teplejší prvý dotyk, no potrebuje šetrnú údržbu proti žmolkovaniu.",
            "<strong>Nočné potenie:</strong> sledujte nielen savosť, ale aj rýchlosť rozvedenia a odparenia vlhkosti z celej posteľnej zostavy.",
            "<strong>Hladký povrch:</strong> satén nie je automaticky hodváb; vždy skontrolujte vláknové zloženie a ošetrovací štítok.",
            "<strong>Jednoduchá údržba:</strong> bavlna býva univerzálna, ale farba, úprava, zips a gramáž môžu obmedziť teplotu aj sušenie.",
        ],
        "overview_heading": "Najprv rozlíšte vlákno, väzbu a povrch",
        "overview": [
            "Vlákno ovplyvňuje prijímanie vlhkosti, pevnosť za mokra, pružnosť a reakciu na teplo. Väzba určuje, ako sa priadze križujú, aký je povrch a ako ľahko sa textília deformuje. Povrchová úprava môže meniť lesk, mäkkosť, krčivosť aj zmáčanie. Preto dve bavlnené obliečky nemusia pôsobiť rovnako a dve saténové súpravy môžu vyžadovať odlišné pranie.",
            "Pri výbere zohľadnite aj <a href=\"/n/gramaz-latky-co-znamena-gsm-pri-uterakoch-oblieckach-a-trickach\">gramáž látky</a>. Vyššie GSM znamená viac materiálu na rovnakej ploche, nie automaticky vyššiu kvalitu alebo teplejší spánok. Flanel môže pôsobiť teplejšie vďaka zdvihnutému povrchu, zatiaľ čo hustý hladký satén môže obmedziť prúdenie vzduchu inak než voľnejšie tkaný ľan.",
            "Systematický prehľad štúdií o vláknach posteľnej bielizne a oblečenia na spanie našiel rozdiely v niektorých podmienkach, ale dostupné štúdie boli rôznorodé a neumožňujú jednoduché tvrdenie, že jedno vlákno je najlepšie pre všetkých. Praktický výber preto musí spojiť výskum s teplotou spálne, prikrývkou, matracom, osobným potením a údržbou.",
        ],
        "table1_heading": "Porovnanie obliečok podľa reálneho používania",
        "table1_intro": "Tabuľka opisuje typické vlastnosti, nie pevné pravidlá. Konkrétna súprava sa môže správať inak podľa priadze, gramáže a úpravy.",
        "table1_headers": ["Označenie", "Čo presne znamená", "Typický pocit", "Na čo si dať pozor"],
        "table1_rows": [
            ("Bavlna", "Prírodné celulózové vlákno; väzba môže byť plátnová, saténová aj iná.", "Univerzálny, podľa spracovania hladký alebo svieži.", "Zrážanie, krčenie, farebná stálosť a rozdielna gramáž."),
            ("Ľan", "Vlákno zo stonky ľanu, často s prirodzenou nepravidelnosťou.", "Vzdušný, spočiatku pevnejší, používaním mäkne.", "Krčivosť, dlhšie tvarovanie po praní a vyššia hmotnosť za mokra."),
            ("Bavlnený satén", "Bavlnené priadze v saténovej väzbe.", "Hladký, jemne lesklý, menej sviežo zrnitý.", "Citlivejší povrch na trenie; hladkosť neznamená automaticky chlad."),
            ("Polyesterový satén", "Syntetické vlákno v saténovej väzbe.", "Veľmi hladký, často rýchlejšie schne.", "Môže inak pracovať s potom a mastnotou; vyžaduje šetrné teplo."),
            ("Flanel", "Tkanina s česaným povrchom, často bavlnená.", "Mäkký a teplý prvý dotyk.", "Žmolkovanie, objemnejšie pranie a pomalšie schnutie v záhyboch."),
        ],
        "sections": [
            (
                "Bavlnené obliečky: univerzálna voľba s veľkým rozptylom kvality",
                [
                    "Bavlna prijíma vlhkosť a pri bežných obliečkach býva praktická na pravidelné pranie. To však neznamená, že každá bavlnená súprava je rovnaká. Jemnosť priadze, hustota väzby, dĺžka vlákna, mercerácia, farbenie a gramáž menia dotyk aj životnosť. Podrobnejší základ poskytuje článok <a href=\"/n/co-je-bavlna-vlastnosti-vyhody-nevyhody-a-starostlivost\">čo je bavlna</a>.",
                    "Pri nočnom potení môže bavlna vlhkosť prijať, no hutná súprava alebo ťažká prikrývka ju nemusia rýchlo uvoľniť. Dôležité je ranné vetranie postele a úplné vysušenie po praní. Ak chcete jednoduchú údržbu, skontrolujte povolenú teplotu, sušičku, zapínanie a rozmery po zrážaní. Nákup podľa slova bavlna bez týchto údajov je príliš hrubé zjednodušenie.",
                ],
            ),
            (
                "Ľanové obliečky: priedušná konštrukcia, krčivosť a postupné mäknutie",
                [
                    "Ľanové vlákno pôsobí pevne a pri voľnejšej tkanine môže vytvoriť vzdušný povrch. Nová súprava býva na dotyk tuhšia a prirodzene sa krčí. Krčenie nie je automaticky chyba; súvisí s nízkou pružnosťou vlákna a konštrukciou. Viac vysvetľuje sprievodca <a href=\"/n/co-je-lan-preco-sa-krci-ako-ho-prat-a-ako-ho-zjemnit\">vlastnosťami ľanu</a>.",
                    "Ľan po praní opatrne vytraste, vytvarujte a sušte rozprestretý. Prudké presušenie a dlhé ponechanie v pokrčenom bubne môže zvýrazniť záhyby. Pri žehlení sa riaďte štítkom a pracujte s primeranou zvyškovou vlhkosťou. Pri citlivej pokožke rozhoduje konkrétny povrch, nie všeobecné tvrdenie, že prírodné vlákno musí byť vždy jemnejšie.",
                ],
            ),
            (
                "Saténové obliečky: hladký povrch nie je názov jedného vlákna",
                [
                    "Saténová väzba vedie časť priadzí dlhšie po povrchu, preto pôsobí hladšie a lesklejšie. Rovnaká väzba môže byť utkaná z bavlny, polyesteru, viskózy alebo hodvábu a každé zloženie mení savosť, teplotnú citlivosť aj pranie. Pred nákupom sa preto neuspokojte s nápisom satén; skontrolujte percentá vlákien.",
                    "Dlhšie povrchové úseky priadze môžu byť citlivejšie na zachytenie o drsný zips, necht alebo suchý zips. Súpravu perte zapnutú a oddelene od predmetov s háčikmi. Konkrétne rozdiely a údržbu rozoberá článok <a href=\"/n/co-je-saten-nie-je-to-vzdy-hodvab-a-ako-ho-spravne-prat\">čo je satén</a>. Hladký dotyk môže byť príjemný, ale sám osebe nepreukazuje lepší odvod tepla.",
                ],
            ),
            (
                "Flanelové obliečky: prečo hrejú a kedy môžu byť príliš teplé",
                [
                    "Flanel má jemne zdvihnutý povrch, ktorý vytvára mäkký kontakt a zadržiava vrstvu vzduchu pri tele. Preto pôsobí teplejšie už pri prvom dotyku. V chladnej spálni môže zvýšiť komfort bez okamžitého pridávania ďalšej prikrývky. V teplej izbe alebo pri výraznom potení však môže byť rovnaká vlastnosť nevýhodou.",
                    "Česaný povrch sa trením a nesprávnym praním môže žmolkovať. Perte naruby alebo zapnuté, neprepĺňajte bubon a vyhnite sa zbytočne drsnému programu. Podrobný postup nájdete v článku <a href=\"/n/ako-prat-flanelove-obliecky-aby-zostali-maekke\">ako prať flanelové obliečky</a>. Pred sezónnym uložením musí byť textília úplne suchá.",
                ],
            ),
            (
                "Ako vybrať obliečky pri nočnom potení",
                [
                    "Nočné potenie nevyrieši iba savé vlákno. Obliečka musí vlhkosť prijať alebo rozviesť, no zároveň ju musí celá posteľná zostava vedieť uvoľniť. Hustý chránič matraca, nepriedušná prikrývka a vysoká teplota izby môžu prekryť rozdiel medzi dvoma súpravami obliečok. Najprv preto zhodnoťte celý systém a pravidelne vetrajte posteľ.",
                    "Ak sa budíte vo vlhkom textile, vyskúšajte ľahšiu súpravu, nižšiu teplotu spálne a prikrývku primeranú sezóne. Sledujte, či sa vlhkosť drží na povrchu, v pyžame alebo hlbšie v matracovej vrstve. Pri novom alebo nevysvetliteľnom silnom nočnom potení neriešte iba materiál; zdravotnú príčinu konzultujte s lekárom.",
                ],
            ),
            (
                "Pranie, zrážanie a sušenie obliečok bez skrútenej náplne",
                [
                    "Obliečky pred praním zapnite, obráťte podľa potreby a oddelte od predmetov so zipsami či háčikmi. Bubon neplňte po okraj. Veľké návleky sa môžu obaliť okolo menších kusov, zadržať prací roztok a zhoršiť oplach. Praktický základ ponúka návod <a href=\"/n/ako-spravne-prat-obliecky-kompletny-sprievodca-starostlivostou\">ako správne prať obliečky</a>.",
                    "Teplotu neurčuje iba vlákno, ale farbenie, potlač, zapínanie a povrchová úprava. GINETEX uvádza, že symbol prania určuje maximálny povolený proces. Po skončení programu obliečky vyberte, rozmotajte a sušte s čo najväčšou voľnou plochou. Pri ľane a bavlne počítajte s možným zrážaním podľa výrobcu; pri syntetickom saténe chráňte materiál pred nadmerným teplom.",
                ],
            ),
        ],
        "table2_heading": "Rozhodovanie podľa sezóny, spánku a údržby",
        "table2_intro": "Jedna domácnosť môže potrebovať dve súpravy na rozdielne obdobia. Rozhodujte podľa prostredia a nie podľa predstavy, že drahšia alebo hladšia látka musí fungovať celoročne.",
        "table2_headers": ["Situácia", "Praktický smer", "Čo ešte overiť", "Možný kompromis"],
        "table2_rows": [
            ("Horúce leto a teplá spálňa", "ľahšia bavlna alebo vzdušnejší ľan", "gramáž, prikrývku, chránič matraca", "ľan sa viac krčí; veľmi ľahká bavlna môže byť priesvitná"),
            ("Chladná spálňa v zime", "flanel alebo hutnejšia bavlnená konštrukcia", "zloženie flanelu a povolené sušenie", "viac objemu v práčke a dlhšie schnutie"),
            ("Citlivosť na drsný povrch", "hladký bavlnený satén alebo jemná bavlna", "švy, potlač a chemické úpravy", "hladkosť nehovorí sama o odvare tepla"),
            ("Silnejšie nočné potenie", "ľahká zostava s dobrou prácou s vlhkosťou", "pyžamo, prikrývku, matrac a teplotu izby", "savá vrstva môže byť ráno vlhšia a potrebuje vetrať"),
            ("Málo priestoru na sušenie", "ľahšia súprava s kratším časom schnutia", "veľkosť bubna a možnosť rozloženia", "syntetická prímes môže meniť dotyk a prácu s pachom"),
        ],
        "steps": [
            "Zmerajte bežnú teplotu spálne a pomenujte, či sa skôr prehrievate, potíte alebo vám býva chladno.",
            "Rozlíšte názov vlákna od väzby: pri saténe a flaneli vždy dohľadajte presné zloženie.",
            "Porovnajte gramáž, hustotu, dotyk, švy, zapínanie a rozmery po praní, nie iba názov kolekcie.",
            "Prečítajte ošetrovací štítok a overte, či zvládnete odporúčanú teplotu, sušenie a prípadné žehlenie.",
            "Pri potení posúďte aj pyžamo, prikrývku, chránič matraca a vetranie celej postele.",
            "Novú farebnú súpravu prvýkrát perte podľa výrobcu oddelene alebo s podobnými farbami.",
            "Po praní obliečky ihneď rozmotajte, vytvarujte a úplne vysušte pred uložením alebo navlečením.",
        ],
        "remember": [
            "Bavlna a ľan sú vlákna; satén je väzba a flanel opisuje česaný povrch tkaniny.",
            "Hladký povrch, vysoká gramáž ani prírodné zloženie samy neurčujú tepelný komfort.",
            "Pri nočnom potení rozhoduje celá zostava vrátane pyžama, prikrývky, chrániča a matraca.",
            "Ošetrovací štítok konkrétnej súpravy má prednosť pred všeobecným návodom na vlákno.",
            "Úplné vysušenie a ranné vetranie postele sú rovnako dôležité ako samotný výber materiálu.",
        ],
        "mistakes": [
            "<strong>Predpoklad, že satén je hodváb.</strong> Bez zloženia neviete, či ide o bavlnu, polyester, viskózu alebo inú surovinu.",
            "<strong>Výber iba podľa ročného obdobia.</strong> Teplota spálne a osobné potenie môžu byť dôležitejšie než kalendár.",
            "<strong>Preplnenie práčky.</strong> Veľké návleky potrebujú priestor na pohyb, oplach a vyrovnanie.",
            "<strong>Dlhé ponechanie mokrej súpravy v bubne.</strong> Zvyšuje pokrčenie, zatuchnutie a nerovnomerné schnutie.",
            "<strong>Univerzálna vysoká teplota.</strong> Potlač, farba, syntetická prímes alebo zapínanie môžu mať nižší limit.",
            "<strong>Prekrytie zatuchnutia parfumom.</strong> Najprv treba vyriešiť pranie, dávkovanie, oplach, sušenie a vetranie postele.",
        ],
        "expert_heading": "Čo hovorí výskum o vláknach a spánku",
        "expert": [
            "Systematický prehľad z roku 2024 posudzoval štúdie o vplyve vláknového zloženia posteľnej bielizne a oblečenia na spánok. Autori našli iba obmedzený počet vhodných štúdií a značnú rôznorodosť podmienok, materiálov aj výsledkov. To podporuje opatrný záver: konkrétne textílie môžu v určitej teplote a populácii priniesť rozdiel, no nemožno z toho vytvoriť univerzálne poradie materiálov.",
            "ISO 11092 umožňuje porovnávať tepelný odpor a odpor proti vodnej pare za definovaných podmienok. Spánok však prebieha v systéme, kde sa sčítava pyžamo, obliečka, plachta, prikrývka, chránič a matrac. Každá vrstva mení tepelný tok a odvod vlhkosti. Hodnota jednej vzorky preto nie je automatickou predpoveďou celej noci.",
            "Prakticky je vhodné meniť jednu premennú naraz. Ak sa prehrievate, najprv znížte tepelnú záťaž prikrývky alebo izby a až potom porovnávajte dve súpravy. Sledujte rannú vlhkosť, pocit na pokožke, čas schnutia a náročnosť údržby. Tak získate užitočnejšiu odpoveď než z všeobecného tvrdenia, že jeden názov materiálu je vždy chladivý.",
        ],
        "source_intro": "Výskum nepodporuje jednoduché univerzálne poradie všetkých obliečok. Zdroje vysvetľujú meranie tepla a vodnej pary, obmedzenia dostupných štúdií a význam správnej interpretácie ošetrovacích symbolov.",
        "sources": [
            ("Systematický prehľad vláknového zloženia posteľnej bielizne a spánku", BEDDING_REVIEW),
            ("ISO 11092: tepelný odpor a odpor proti vodnej pare", ISO_WATER_VAPOUR),
            ("GINETEX: význam ošetrovacích symbolov", GINETEX),
        ],
        "product_intro": "Obliečky zachytávajú pot, kožný maz a zvyšky kozmetiky, preto potrebujú primeraný prací proces a dobrý oplach. Prací gél dávkujte podľa tvrdosti vody, znečistenia a skutočnej veľkosti náplne.",
        "product_text": "Hypoalergénny prací gél Vevo Ylang Absolute je konkrétna voľba pre bežné prateľné obliečky, ak to povoľuje štítok. Pri veľkých návlekoch nechajte v bubne priestor a po praní ich ihneď rozmotajte.",
        "product_limit": "Hodvábny satén, vlna, špeciálne úpravy a citlivé dekoratívne prvky môžu vyžadovať iný prostriedok alebo ručné či profesionálne čistenie.",
        "category_intro": "Pri porovnávaní pracích gélov sledujte určenie pre farbu a materiál, dávkovanie aj spôsob oplachu. Väčšie množstvo gélu nezaručuje čistejšiu posteľnú bielizeň a môže sa horšie vypláchnuť.",
        "category_text": "V kategórii pracích gélov môžete vybrať prostriedok pre bežné domáce pranie. Konečné rozhodnutie prispôsobte štítku obliečok a citlivosti konkrétnej textílie.",
        "related": [
            ("Ako správne prať obliečky", "/n/ako-spravne-prat-obliecky-kompletny-sprievodca-starostlivostou"),
            ("Čo je bavlna", "/n/co-je-bavlna-vlastnosti-vyhody-nevyhody-a-starostlivost"),
            ("Čo je ľan", "/n/co-je-lan-preco-sa-krci-ako-ho-prat-a-ako-ho-zjemnit"),
            ("Čo je satén", "/n/co-je-saten-nie-je-to-vzdy-hodvab-a-ako-ho-spravne-prat"),
            ("Ako prať flanelové obliečky", "/n/ako-prat-flanelove-obliecky-aby-zostali-maekke"),
            ("Ako čítať priedušnosť a savosť textilu", ARTICLE_PROPERTIES),
        ],
        "faq_title": "výber obliečok podľa materiálu a sezóny",
        "faq": [
            ("Aké obliečky sú najlepšie na leto?", "Často vyhovuje ľahšia bavlnená alebo ľanová konštrukcia, ale rozhoduje teplota izby, prikrývka, gramáž a osobné potenie. Univerzálny víťaz neexistuje."),
            ("Je bavlnený satén chladivejší než bavlna?", "Bavlnený satén je stále bavlna v konkrétnej väzbe. Hladší dotyk môže pôsobiť inak, no tepelný pocit závisí aj od hustoty, gramáže a celej posteľnej zostavy."),
            ("Sú flanelové obliečky vhodné pri potení?", "V chladnej spálni môžu byť príjemné, no česaný povrch pôsobí teplejšie. Pri výraznom potení alebo teplej izbe môže byť praktickejšia ľahšia súprava."),
            ("Prečo sa ľanové obliečky tak krčia?", "Ľanové vlákno má nízku pružnú návratnosť. Záhyby sú prirodzenou vlastnosťou; znížite ich okamžitým vybratím, vytvarovaním a vhodným sušením podľa štítku."),
            ("Ako často prať obliečky pri nočnom potení?", "Frekvenciu prispôsobte intenzite potenia, zdravotnému stavu a podmienkam. Dôležité je aj denné vetranie postele a úplné vysušenie každej súpravy."),
            ("Môžem prať bavlnené, ľanové a saténové obliečky spolu?", "Iba ak majú kompatibilnú farbu, povolenú teplotu, mechanické namáhanie a spôsob sušenia. Názov kategórie nestačí; porovnajte všetky štítky."),
        ],
    },
    {
        "title": "Froté, bambus alebo mikrovlákno: ktorý uterák vybrať podľa savosti a schnutia",
        "link": "frote-bambus-alebo-mikrovlakno-ktory-uterak-vybrat-podla-savosti-a-schnutia",
        "meta": "Ako vybrať uterák podľa savosti, gramáže a schnutia. Rozdiel medzi froté, bavlnou, bambusovou viskózou a mikrovláknom pre kúpeľňu aj šport.",
        "short": "Froté označuje slučkovú konštrukciu, nie konkrétne vlákno. Bambusový uterák býva často z viskózy vyrobenej z bambusovej celulózy a mikrovlákno je veľmi jemná syntetická priadza. Vyberajte podľa použitia, gramáže, savosti, času schnutia a možností vetrania.",
        "answer": "Do bežnej kúpeľne je praktické bavlnené froté so strednou gramážou, ak má uterák priestor úplne vyschnúť. Na cestovanie alebo do fitka býva výhodné ľahšie mikrovlákno, ktoré zaberie menej miesta a spravidla schne rýchlejšie. Označenie bambus si overte v zložení: často ide o viskózu z bambusovej celulózy, nie o mechanicky spracované bambusové vlákno. Najvyššia gramáž ani najmäkší povrch automaticky neznamenajú najlepšiu savosť po desiatkach praní.",
        "intro": "Pri uterákoch sa miešajú názvy konštrukcie, suroviny aj obchodné označenia. Froté tvorí povrch zo slučiek a môže byť bavlnené, zmesové alebo vyrobené z iných priadzí. Mikrovlákno opisuje veľmi jemné syntetické vlákna, najčastejšie polyester a polyamid. Výraz bambusový uterák často skracuje presnejšie označenie viskóza vyrobená z bambusovej celulózy. Bez percentuálneho zloženia a gramáže teda nemožno materiály férovo porovnať.",
        "quick": [
            "<strong>Kúpeľňa s dobrým vetraním:</strong> stredne hutné bavlnené froté ponúka vyváženie dotyku, savosti a údržby.",
            "<strong>Malá vlhká kúpeľňa:</strong> ľahší uterák môže byť hygienicky praktickejší, pretože medzi použitiami skôr vyschne.",
            "<strong>Cestovanie a šport:</strong> mikrovlákno šetrí miesto a schne rýchlo, ale na pokožke pôsobí inak než froté.",
            "<strong>Vlasy:</strong> jemný ľahší povrch a šetrné pritláčanie sú dôležitejšie než agresívne trenie alebo maximálna gramáž.",
            "<strong>Označenie bambus:</strong> prečítajte presný názov vlákna a percentá; mäkkosť sama nepotvrdzuje pôvod ani vlastnosti.",
        ],
        "overview_heading": "Froté nie je materiál a mäkkosť nie je laboratórna savosť",
        "overview": [
            "Slučky froté zväčšujú kontaktnú plochu s vodou. Ich výška, hustota, krútenie a kvalita priadze ovplyvňujú, ako uterák prijíma vodu a ako sa správa po praní. Dve bavlnené froté osušky s rovnakým GSM môžu mať rozdielnu pružnosť slučiek, pevnosť okrajov aj čas schnutia. Pri kúpe preto skontrolujte povrch, lemy a rovnomernosť, nie iba číslo na etikete.",
            "ASTM D4772 opisuje skúšku povrchovej absorpcie vody pri froté uterákoch. AATCC TM79 sa zameriava na absorpciu vody textíliou a TM200 na schnutie za definovaných podmienok. Každá skúška odpovedá na inú otázku. Rýchle prijatie kvapky nehovorí priamo, koľko vody celý uterák udrží, a vysoká kapacita automaticky neznamená krátky čas schnutia.",
            "Mäkký nový uterák môže byť príjemný, no povrchová úprava z výroby alebo nános zmäkčujúcich látok môže ovplyvniť zmáčanie. Savosť sa často stabilizuje po prvých praniach podľa štítku. Ak sa neskôr prudko zhorší, skontrolujte dávkovanie, aviváž, mastné zvyšky a oplach predtým, než materiál označíte za nekvalitný.",
        ],
        "table1_heading": "Bavlnené froté, bambusová viskóza a mikrovlákno",
        "table1_intro": "Porovnanie platí pre typické výrobky. Konkrétny uterák môže mať inú zmes, konštrukciu alebo úpravu, preto je rozhodujúci štítok.",
        "table1_headers": ["Typ uteráka", "Silná stránka", "Slabšia stránka", "Vhodné použitie"],
        "table1_rows": [
            ("Bavlnené froté", "Príjemný slučkový povrch a dobrá celková kapacita vody.", "Hutný kus je ťažký za mokra a v záhybe schne pomaly.", "Kúpeľňa, sauna, každodenné osušenie."),
            ("Zmes bavlny a viskózy z bambusu", "Mäkký povrch a odlišný dotyk podľa zmesi.", "Označenie bambus môže zakryť skutočný názov vlákna; zmes má vlastný štítok.", "Domáce uteráky, ak vyhovuje dotyk a údržba."),
            ("Mikrovlákno", "Nízky objem, veľká plocha jemných vlákien a krátke schnutie.", "Iný pocit na koži, citlivosť na vysoké teplo a zmäkčujúci film.", "Cestovanie, fitko, turistika, rýchle striedanie."),
            ("Ľahké froté", "Rýchlejšie schnutie a menšia mokrá hmotnosť.", "Menej plyšový pocit a nižšia kapacita na jeden kus.", "Malá kúpeľňa, deti, časté pranie."),
            ("Veľmi hutné froté", "Plný dotyk a veľká plocha slučiek.", "Náročnejšie odstreďovanie, sušenie a skladovanie.", "Dobre vetraná kúpeľňa a dostatok priestoru."),
        ],
        "sections": [
            (
                "Ako vybrať gramáž uteráka bez naháňania najvyššieho čísla",
                [
                    "GSM vyjadruje hmotnosť jedného štvorcového metra textílie. Pri podobnej konštrukcii vyššie číslo často znamená viac materiálu, plnší pocit a väčšiu mokrú hmotnosť. Nehodnotí však kvalitu bavlny, pevnosť lemu, výšku slučky ani schopnosť povrchu rýchlo sa zmáčať. Podrobný výpočet a limity nájdete v článku <a href=\"/n/gramaz-latky-co-znamena-gsm-pri-uterakoch-oblieckach-a-trickach\">čo znamená GSM</a>.",
                    "V malej kúpeľni bez okna môže byť stredná alebo nižšia gramáž praktickejšia než luxusne hutná osuška. Ak sa uterák nedá rozprestrieť a zostane preložený na háčiku, vysoká kapacita vody predĺži vlhký stav. Naopak v dobre vetranom priestore môže hutnejší uterák poskytovať požadovaný dotyk bez problémov so schnutím.",
                ],
            ),
            (
                "Čo znamená bambusový uterák na etikete",
                [
                    "Textil označený ako bambusový býva často vyrobený z regenerovaného celulózového vlákna, napríklad viskózy, pričom zdrojom celulózy bol bambus. To nie je rovnaké ako mechanicky získané prírodné bambusové vlákno. V Európskej únii sa vláknové zloženie riadi pravidlami označovania; spotrebiteľ by mal dostať presný názov a podiel vlákien.",
                    "Pre domáci výber je najdôležitejší údaj na štítku. Ak vidíte zmes bavlny a viskózy, porovnávajte ju ako konkrétnu zmes, nie ako abstraktnú rastlinu. Mäkkosť alebo lesk nepotvrdzujú antibakteriálny účinok ani ekologickú výhodu. Viac k rozdielu vysvetľujú články <a href=\"/n/co-je-bambusova-viskoza-maekkost-marketingove-tvrdenia-a-realna-starostlivost\">čo je bambusová viskóza</a> a <a href=\"/n/bambusove-vlakno-vs-bavlna-vyhody-nevyhody-a-pranie-pri-citlivej-pokozke\">bambusové vlákno verzus bavlna</a>.",
                ],
            ),
            (
                "Mikrovláknový uterák na šport a cestovanie",
                [
                    "Mikrovlákno vytvára veľkú kontaktnú plochu z veľmi jemných syntetických vlákien. Tenký cestovný uterák prijme vodu iným spôsobom než froté a po vyžmýkaní či odstreďovaní zadrží menší objem vody v celom kuse. Vďaka tomu sa ľahko balí a rýchlo suší, čo je výhoda v športovej taške alebo na turistike.",
                    "Na dotyk môže pôsobiť priľnavejšie a niekomu nevyhovuje pri celotelovom osušení. Perte ho bez aviváže, ktorá môže obaliť jemné vlákna, a chráňte pred vysokým teplom podľa štítku. Samostatný materiálový sprievodca je v článku <a href=\"/n/co-je-mikrovlakno-vyhody-nevyhody-savost-a-pranie\">čo je mikrovlákno</a>.",
                ],
            ),
            (
                "Uterák na vlasy, tvár a citlivú pokožku",
                [
                    "Pri vlasoch nie je cieľom čo najagresívnejšie trenie, ale šetrné prijatie vody. Ľahší jemný uterák alebo turban sa dá obtočiť bez veľkej hmotnosti a znižuje potrebu krútiť mokré vlasy. Materiál pritláčajte a nechajte vodu preniesť do textílie. Drsný lem, tvrdé slučky alebo príliš ťažká osuška môžu byť nepraktické bez ohľadu na deklarovanú savosť.",
                    "Na tvár používajte samostatný čistý uterák a meňte ho podľa používania, stavu pokožky a podmienok schnutia. Jemnosť hodnotíte dotykom konkrétneho povrchu, nie iba názvom vlákna. Ak uterák zostáva vlhký alebo prichádza do kontaktu s kozmetikou, potrebuje častejšie pranie a úplné vysušenie.",
                ],
            ),
            (
                "Prečo uterák po praní tvrdne alebo prestane sať",
                [
                    "Tvrdosť môže súvisieť s minerálmi z vody, nadmerným množstvom pracieho prostriedku, slabým oplachom, presušením alebo zle rozvoľnenými slučkami. Strata savosti často súvisí s filmom zo zmäkčujúcich látok, kozmetiky a kožného mazu. Riešením nie je automaticky pridať viac gélu, ale upraviť dávku, náplň a oplach.",
                    "Uteráky perte s dostatkom priestoru, aby sa voda dostala medzi slučky. Aviváž používajte iba vtedy, ak ju povoľuje výrobca a ak neprekáža požadovanej savosti. Pri športe pomôže návod <a href=\"/n/ako-prat-sportove-uteraky-aby-nezapachali\">ako prať športové uteráky</a>; pri bežnej kúpeľni postup <a href=\"/n/ako-prat-uteraky-rady-a-tipy-na-ciste-a-maekke-uteraky\">ako prať uteráky</a>.",
                ],
            ),
            (
                "Sušenie rozhoduje o vôni aj praktickej životnosti",
                [
                    "Po použití uterák rozprestrite po celej šírke. Preloženie cez úzky háčik vytvorí niekoľko vrstiev, medzi ktorými sa drží vlhkosť. Po praní ho vytraste, aby sa slučky rozvoľnili, a sušte spôsobom povoleným na štítku. Sušička môže zmeniť mäkkosť aj zrážanie; vysoké teplo môže poškodiť syntetické mikrovlákno alebo ozdobné prvky.",
                    "Uterák neukladajte ani nehádžte do koša na bielizeň vlhký. Ak ho nemôžete hneď prať, nechajte ho najprv preschnúť. Trvalý zatuchnutý pach po správnom praní môže znamenať, že je textília hlboko zanesená, poškodená alebo sa v kúpeľni nevie vysušiť. Vtedy je rozumnejšie zmeniť gramáž, miesto zavesenia alebo kus vymeniť.",
                ],
            ),
        ],
        "table2_heading": "Ktorý uterák vybrať pre konkrétnu situáciu",
        "table2_intro": "Funkčný výber často znamená mať viac typov uterákov. Veľká kúpeľňová osuška, cestovný uterák a uterák na vlasy nemusia mať rovnaký materiál ani gramáž.",
        "table2_headers": ["Situácia", "Odporúčaný smer", "Prečo", "Kontrolná otázka"],
        "table2_rows": [
            ("Každodenná kúpeľňa", "stredne hutné bavlnené froté", "vyvážený dotyk a kapacita", "Má priestor vyschnúť do ďalšieho použitia?"),
            ("Vlhká kúpeľňa bez okna", "ľahšie froté alebo rýchlejšie schnúci kus", "kratší vlhký stav", "Viete ho zavesiť rozprestretý?"),
            ("Fitko a plaváreň", "ľahké mikrovlákno alebo tenšie froté", "nižší objem v taške a rýchle schnutie", "Budete ho po tréningu hneď vyberať?"),
            ("Dlhé alebo jemné vlasy", "ľahký jemný uterák či turban", "menšia hmotnosť a menej potreby trieť", "Je povrch hladký a bez drsných lemov?"),
            ("Cestovanie", "mikrovlákno s nízkym objemom", "ľahko sa balí a schne", "Máte možnosť ho po použití rozložiť?"),
        ],
        "steps": [
            "Určite použitie: kúpeľňa, šport, cestovanie, vlasy, tvár alebo kuchyňa.",
            "Prečítajte presné percentá vlákien a rozlíšte froté konštrukciu od materiálového zloženia.",
            "Porovnajte GSM, rozmery a celkovú hmotnosť; veľká hutná osuška bude za mokra výrazne ťažšia.",
            "Skontrolujte slučky, lemy, švy a povrch na oboch stranách, nie iba mäkkosť nového kusu.",
            "Zvážte reálne vetranie kúpeľne a priestor, kde bude uterák visieť úplne rozprestretý.",
            "Pred prvým použitím vyperte uterák podľa štítku a sledujte savosť po niekoľkých cykloch.",
            "Dávkujte prací prostriedok primerane, neprepĺňajte bubon a uterák vždy úplne vysušte.",
        ],
        "remember": [
            "Froté je slučková konštrukcia; bavlna, viskóza a syntetické mikrovlákno sú zloženia.",
            "Najvyššia gramáž nie je automaticky najpraktickejšia do malej alebo vlhkej kúpeľne.",
            "Označenie bambus treba overiť presným názvom vlákna a percentuálnym zložením.",
            "Mikrovlákno je praktické na cestovanie, no vyžaduje šetrné teplo a pranie bez zmäkčujúceho filmu.",
            "Savosť po čase ovplyvňuje dávkovanie, aviváž, kozmetika, oplach a úplné vysušenie.",
        ],
        "mistakes": [
            "<strong>Nákup podľa najvyššieho GSM.</strong> Veľmi hutný uterák môže byť v zle vetranej kúpeľni stále vlhký.",
            "<strong>Zamieňanie froté za bavlnu.</strong> Froté opisuje povrch, nie automaticky vláknové zloženie.",
            "<strong>Prijatie slova bambus bez kontroly etikety.</strong> Často ide o viskózu alebo zmes s bavlnou.",
            "<strong>Veľa aviváže pre väčšiu mäkkosť.</strong> Film môže znížiť zmáčanie a savosť slučiek alebo mikrovlákna.",
            "<strong>Preplnený bubon.</strong> Hutné uteráky po nasiaknutí potrebujú priestor na pohyb a oplach.",
            "<strong>Skladanie vlhkého uteráka.</strong> Zvyšuje riziko zatuchnutia bez ohľadu na materiál.",
        ],
        "expert_heading": "Ako čítať skúšky savosti a schnutia uterákov",
        "expert": [
            "ASTM D4772 sa zameriava na povrchovú absorpciu vody pri froté uterákoch. AATCC TM79 hodnotí absorpciu vody textíliou a TM200 rýchlosť schnutia. Rozdiel v metóde je dôležitý: jedna skúška môže sledovať čas prijatia vody, iná zmenu hmotnosti počas schnutia. Výsledok jednej metódy preto nemožno bez vysvetlenia vydávať za celkovú kvalitu uteráka.",
            "Domáce porovnanie môže sledovať, ako rýchlo rovnaké množstvo vody vsiakne, koľko uterák váži po rovnakom odstreďovaní a ako dlho schne na rovnakom mieste. Taký pokus však nie je presná skúška, pretože kusy majú rozdielne rozmery, lemy, predchádzajúce pranie a zvyškovú vlhkosť. Je vhodný iba na praktické rozhodnutie vo vlastnej domácnosti.",
            "Dôležitý je životný cyklus vlastnosti. Nový uterák môže mať výrobnú úpravu, po prvých praniach sa slučky otvoria a neskôr sa môžu zanášať. Sledujte preto nielen prvý dojem, ale aj savosť po opakovanom praní, pevnosť okrajov, čas schnutia a návrat pachu. Kvalitný výber je ten, ktorý dokážete správne prať a sušiť vo svojich podmienkach.",
        ],
        "source_intro": "Skúšobné metódy oddeľujú povrchovú absorpciu, všeobecné prijatie vody a schnutie. Európska komisia zároveň uvádza rámec označovania vláknového zloženia; spotrebiteľ má vychádzať z etikety konkrétneho výrobku.",
        "sources": [
            ("ASTM D4772-26: povrchová absorpcia vody pri froté uterákoch", ASTM_TERRY),
            ("AATCC TM79: absorpcia vody textíliou", AATCC_ABSORBENCY),
            ("AATCC TM200: rýchlosť schnutia", AATCC_DRYING),
            ("Európska komisia: textilný ekosystém a pravidlá označovania", EU_TEXTILES),
            ("GINETEX: význam ošetrovacích symbolov", GINETEX),
        ],
        "product_intro": "Uteráky zachytávajú vodu, kožný maz a kozmetiku. Dobre zvolený prací gél musí pracovať spolu s dostatočným priestorom v bubne a úplným oplachom, aby medzi slučkami neostal nános.",
        "product_text": "Hypoalergénny prací gél Vevo Ylang Absolute je konkrétna voľba pre bežné prateľné uteráky, ak ju povoľuje štítok. Pri hutnom froté dávkujte primerane a nedávajte do jednej náplne príliš veľa ťažkých kusov.",
        "product_limit": "Pri mikrovlákne, špeciálnych športových uterákoch, výrazných farbách alebo dekoratívnych lemoch má prednosť návod výrobcu; nepoužívajte automaticky aviváž.",
        "category_intro": "Pri výbere pracieho gélu pre uteráky sledujte účel, dávku a oplach. Priveľa produktu môže zostať medzi slučkami, zatiaľ čo primálo pri silnom znečistení nemusí odstrániť mastný film.",
        "category_text": "Kategória pracích gélov ponúka riešenia pre bežné domáce pranie. Výsledok však závisí aj od tvrdosti vody, kapacity práčky, programu a správneho vysušenia.",
        "related": [
            ("Ako prať uteráky", "/n/ako-prat-uteraky-rady-a-tipy-na-ciste-a-maekke-uteraky"),
            ("Prečo uteráky strácajú savosť", "/n/silikony-v-avivazach-skryty-nepriatel-vasho-pradla1"),
            ("Čo je mikrovlákno", "/n/co-je-mikrovlakno-vyhody-nevyhody-savost-a-pranie"),
            ("Čo je bambusová viskóza", "/n/co-je-bambusova-viskoza-maekkost-marketingove-tvrdenia-a-realna-starostlivost"),
            ("Bambusové vlákno verzus bavlna", "/n/bambusove-vlakno-vs-bavlna-vyhody-nevyhody-a-pranie-pri-citlivej-pokozke"),
            ("Ako čítať priedušnosť a savosť textilu", ARTICLE_PROPERTIES),
        ],
        "faq_title": "výber uteráka podľa savosti a schnutia",
        "faq": [
            ("Aká gramáž uteráka je najlepšia?", "Nie je jedna správna hodnota. Stredná gramáž býva univerzálna, ľahšia rýchlejšie schne a veľmi hutná poskytne plnší dotyk, ale potrebuje viac priestoru a času."),
            ("Je bambusový uterák naozaj z bambusového vlákna?", "Často je z viskózy vyrobenej z bambusovej celulózy alebo zo zmesi s bavlnou. Rozhodujúci je presný názov a percentá na etikete."),
            ("Prečo nový uterák málo saje?", "Môže mať zvyškovú výrobnú úpravu. Vyperte ho podľa štítku bez nadmernej dávky a savosť hodnotíte až po niekoľkých použitiach a praniach."),
            ("Je mikrovlákno vhodné na telo?", "Áno, ak vám vyhovuje jeho dotyk. Je praktické na šport a cestovanie, no niekomu prekáža priľnavejší pocit oproti slučkovému froté."),
            ("Môžem používať aviváž na uteráky?", "Riaďte sa štítkom a požadovanou funkciou. Zmäkčujúci film môže pri niektorých uterákoch a mikrovlákne znížiť zmáčanie alebo savosť."),
            ("Ako zabrániť zatuchnutiu uteráka?", "Po každom použití ho rozprestrite, vetrajte kúpeľňu, mokrý kus neskladajte do koša a po praní ho úplne vysušte. Pri pretrvávajúcom pachu upravte dávkovanie a oplach."),
        ],
    },
    {
        "title": "Polyester, polyamid, merino alebo elastan: z čoho má byť športové oblečenie",
        "link": "polyester-polyamid-merino-alebo-elastan-z-coho-ma-byt-sportove-oblecenie",
        "meta": "Polyester, polyamid, merino a elastan v športovom oblečení: výber podľa aktivity, potu, schnutia, pachu, pružnosti a správneho prania.",
        "short": "Športové oblečenie nevyberajte iba podľa percenta jedného vlákna. Polyester a polyamid tvoria základ rýchloschnúcich úpletov, merino pracuje s vlhkosťou a tepelným komfortom a elastan pridáva pružnosť. Výsledok mení pletenina, strih, hrúbka, vrstvenie aj údržba.",
        "answer": "Na intenzívny tréning a rýchle schnutie býva praktický dobre navrhnutý polyesterový alebo polyamidový úplet. Polyamid často prináša hladkosť a odolnosť, polyester široké možnosti konštrukcie a krátke schnutie. Merino môže byť príjemné pri dlhšej aktivite a premenlivej teplote, ale potrebuje šetrnejšiu starostlivosť. Elastan zvyčajne tvorí menšiu časť zmesi a zabezpečuje pružnosť; sám nie je hlavnou vrstvou. Najlepšia voľba závisí od aktivity, teploty, dĺžky výkonu, potenia a možností prania.",
        "intro": "Nápis funkčné tričko nehovorí, ako sa odev bude správať pri behu, silovom tréningu, turistike alebo pokojnej joge. Dve tričká z polyesteru môžu mať rozdielnu hrúbku, smer transportu vlhkosti a vetracie zóny. Merino zmes sa môže líšiť podielom vlny a syntetiky. Legíny s rovnakým percentom elastanu môžu mať inú pružnosť vďaka väzbe a konštrukcii. Preto treba čítať zloženie spolu s určením odevu.",
        "quick": [
            "<strong>Intenzívny tréning:</strong> hľadajte odvod kvapalnej vlhkosti, krátke schnutie, ploché švy a dostatočné vetranie.",
            "<strong>Dlhá turistika:</strong> zohľadnite prácu s vlhkosťou, tepelný komfort, trenie batohu a možnosť usušiť vrstvu počas presunu.",
            "<strong>Legíny a kompresné kusy:</strong> rozhoduje návratnosť pružnosti, nepriehľadnosť pri natiahnutí a citlivosť elastanu na teplo.",
            "<strong>Merino:</strong> nie je automaticky bez zápachu ani nezničiteľné; potrebuje vhodný program, prostriedok a sušenie.",
            "<strong>Zápach po športe:</strong> ovplyvňuje materiál, mikrobiálne osídlenie, kožný maz, čas do prania a nános z predchádzajúcich cyklov.",
        ],
        "overview_heading": "Vlákno určuje možnosti, o výsledku rozhoduje celý odev",
        "overview": [
            "Polyester a polyamid prijímajú do samotného vlákna menej vody než bavlna, no povrch a tvar priadze môžu rozvádzať kvapalinu do väčšej plochy. To podporuje odparovanie, ak má odev kontakt s pokožkou a vonkajšia strana zostáva vystavená vzduchu. Hrubý úplet alebo nepriedušná vrchná vrstva však môže túto výhodu znížiť.",
            "Merino vlna dokáže viazať vodnú paru a pri správnej konštrukcii pomáha pracovať s mikroklímou pri tele. Za mokra schne inak než tenká syntetika a môže byť citlivejšia na trenie, teplo a mechanické namáhanie. Elastan dáva zmesi pružnosť, ale vysoká teplota, chlórové bielidlo alebo agresívne sušenie môžu urýchliť stratu návratnosti.",
            "Štúdia pachov tričiek po fitness cvičení zistila v konkrétnych podmienkach intenzívnejší a menej príjemný pach pri skúmaných polyesterových tričkách než pri bavlnených. Nie je to dôkaz, že každý polyester zapácha viac v každej situácii. Výsledok závisí od konštrukcie, používateľa, prania a mikroorganizmov; slúži ako upozornenie, že pach nie je iba otázka intenzity potenia.",
        ],
        "table1_heading": "Úloha jednotlivých vlákien v športovom oblečení",
        "table1_intro": "Tabuľka opisuje typické úlohy. Konečné správanie mení percentuálna zmes, priadza, úplet, povrch a strih.",
        "table1_headers": ["Vlákno", "Čo prináša", "Na čo si dať pozor", "Typické použitie"],
        "table1_rows": [
            ("Polyester", "Nízku nasiakavosť vlákna, široké možnosti priadze a rýchle schnutie tenkých úpletov.", "Pach a mastný film sa môžu držať na povrchu; rozhoduje konštrukcia.", "Tričká, mikiny, cyklistické dresy, podšívky."),
            ("Polyamid / nylon", "Hladkosť, pevnosť, odolnosť proti oderu a príjemný dotyk v jemných úpletoch.", "Citlivosť na vysoké teplo a rozdielne správanie zmesí.", "Legíny, pančuchové úplety, plavky, odolné zóny."),
            ("Merino vlna", "Prácu s vodnou parou a tepelný komfort v širšom rozsahu podmienok.", "Jemnosť sa líši; potrebuje šetrný proces a ochranu pred plstnatením.", "Spodné vrstvy, turistika, chladnejšie aktivity."),
            ("Elastan", "Pružnosť a návrat do tvaru už pri menšom podiele v zmesi.", "Teplo, chlór, trenie a starnutie znižujú pružnú návratnosť.", "Legíny, spodná bielizeň, priliehavé tričká."),
            ("Zmesi", "Kombináciu vlastností a cielené rozloženie funkcie.", "Náročnosť údržby určuje najcitlivejšia zložka a konštrukcia.", "Takmer všetky technické športové odevy."),
        ],
        "sections": [
            (
                "Polyester pri behu a intenzívnom tréningu",
                [
                    "Polyesterový úplet môže byť veľmi ľahký, perforovaný alebo vytvorený z profilovaných priadzí, ktoré vedú vlhkosť po povrchu. Pri kontakte s pokožkou dokáže presunúť pot do väčšej plochy a urýchliť odparenie. Samotné percento polyesteru však nehovorí, či má látka túto konštrukciu. Hľadajte informácie o pletenine, gramáži a určení.",
                    "Po tréningu odev nenechávajte stlačený v taške. Vyvetrajte ho alebo čo najskôr vyperte podľa štítku. Mastný film a zvyšky deodorantu môžu zadržiavať pach aj v materiáli, ktorý rýchlo vyschne. Ak sa problém vracia, upravte dávku, náplň a oplach namiesto vrstvenia ďalšej vône.",
                ],
            ),
            (
                "Polyamid v legínach, ponožkách a odolných zónach",
                [
                    "Polyamid, často označený ako nylon, sa používa pre pevnosť, odolnosť proti oderu a hladký dotyk. Jemné polyamidové úplety bývajú príjemné v legínach a spodných vrstvách, no konkrétna nepriehľadnosť závisí od hustoty a napnutia. Pri nákupe skúste látku natiahnuť v smere, v ktorom pracuje počas pohybu.",
                    "Rozdiely medzi syntetikami rozoberá článok <a href=\"/n/polyamid-vs-polyester-ktory-material-lepsie-znasa-pot-sport-a-caste-pranie\">polyamid verzus polyester</a> a základ materiálu článok <a href=\"/n/co-je-polyamid-alebo-nylon-vlastnosti-odolnost-a-pranie\">čo je polyamid</a>. Pri praní chráňte jemný povrch pred suchými zipsami a ostrými zipsami.",
                ],
            ),
            (
                "Merino na turistiku, vrstvenie a premenlivú teplotu",
                [
                    "Merino môže viazať vodnú paru a poskytovať tepelný komfort pri striedaní intenzity. Tenká spodná vrstva sa však správa inak než hrubý sveter. Pri dlhšej turistike je dôležité, či vrchné vrstvy umožnia vlhkosti pokračovať smerom von. Mokré merino sušte rozložené a chráňte pred zbytočným trením.",
                    "Vlna nie je automaticky bez zápachu a každý človek ju vníma inak na pokožke. Zmes so syntetikou môže zvýšiť odolnosť alebo skrátiť schnutie, zároveň však zmení dotyk a údržbu. Presný postup ponúka článok <a href=\"/n/co-je-merino-vlna-vyhody-nevyhody-a-pranie-bez-zrazenia\">čo je merino vlna</a>.",
                ],
            ),
            (
                "Elastan: malý podiel, veľký vplyv na tvar",
                [
                    "Elastan sa do športového oblečenia pridáva v menšom podiele, aby sa látka natiahla a vrátila do pôvodného tvaru. Vyššie percento samo nezaručuje lepšiu kompresiu alebo životnosť. Rozhoduje hrúbka priadze, smer pružnosti, väzba a to, ako je odev ušitý. Príliš tesný kus môže obmedzovať pohyb aj komfort.",
                    "Pružné vlákno je citlivé na vysoké teplo, chlórové bielidlo, dlhodobé pôsobenie olejov a mechanické opotrebovanie. Legíny nesušte na prudkom zdroji tepla a nežehlite mimo limitu štítku. Viac vysvetľuje článok <a href=\"/n/co-je-elastan-preco-je-v-leginach-spodnej-bielizni-a-sportovom-obleceni\">čo je elastan</a>.",
                ],
            ),
            (
                "Ako vybrať materiál podľa typu aktivity",
                [
                    "Pri intervalovom behu je prioritou rýchly presun potu a schnutie. Pri pokojnej joge môže byť dôležitejší mäkký dotyk, nepriehľadnosť a pružnosť. Pri turistike sa strieda výkon, oddych, vietor a zmena teploty, preto funguje systém viacerých vrstiev. Pri cyklistike rozhoduje aj aerodynamický strih, zadné vrecká, zips a oder od sedla či popruhov.",
                    "Namiesto univerzálnej odpovede si spíšte teplotu, trvanie, intenzitu, množstvo potu a možnosť prezlečenia. Potom hľadajte konštrukciu, ktorá rieši dominantný problém. Praktickú starostlivosť o špecifický kus rozoberá návod <a href=\"/n/ako-prat-cyklisticky-dres-a-elasticke-sportove-oblecenie\">ako prať cyklistický dres a elastické športové oblečenie</a>.",
                ],
            ),
            (
                "Pranie športového oblečenia bez zatuchnutia a straty pružnosti",
                [
                    "Prepotený odev po použití rozložte, neuzatvárajte ho mokrý do koša a perte podľa štítku. Zapnite zipsy, uvoľnite vrecká a jemné kúsky vložte do ochranného vrecka. Bubon neprepĺňajte, aby sa prací roztok dostal cez husté elastické vrstvy a následne sa úplne vypláchol.",
                    "Aviváž nepoužívajte automaticky. Pri niektorých funkčných úpletoch môže zanechať film a pri elastických vláknach sa riaďte pokynmi výrobcu. Vysoká teplota nie je univerzálnym riešením pachu a môže poškodiť elastan, potlač alebo lepené časti. Základný postup nájdete aj v článku <a href=\"/n/ako-prat-syntetiku-polyester-a-elastan-aby-nezapachali-a-drzali-tvar\">ako prať syntetiku, polyester a elastan</a>.",
                ],
            ),
        ],
        "table2_heading": "Materiál podľa aktivity a podmienok",
        "table2_intro": "Výber je vždy kompromis medzi schnutím, teplom, odolnosťou, pružnosťou, dotykom a údržbou. Nasledujúce smery sú praktický začiatok, nie pevný predpis.",
        "table2_headers": ["Aktivita", "Praktický smer", "Kľúčová vlastnosť", "Čo skontrolovať"],
        "table2_rows": [
            ("Beh v teple", "ľahký polyesterový alebo polyamidový úplet", "odvod kvapalnej vlhkosti a vetranie", "ploché švy, sieťované zóny, priliehanie"),
            ("Silový tréning", "odolná pružná zmes s elastanom", "návratnosť, nepriehľadnosť a odolnosť", "švy pri drepe, pás, trenie o náradie"),
            ("Turistika v chlade", "merino alebo zmes ako spodná vrstva", "práca s parou a tepelný komfort", "vrstvenie, oder batohu, možnosť sušenia"),
            ("Cyklistika", "priliehavá syntetická zmes", "smer pružnosti a rýchle schnutie", "zips, vrecká, vložka, reflexné prvky"),
            ("Plávanie", "polyamidová alebo polyesterová zmes s elastanom podľa určenia", "odolnosť, tvar a rýchle schnutie", "chlór, opaľovací prípravok a pokyny na oplach"),
        ],
        "steps": [
            "Pomenujte aktivitu, teplotu, trvanie, intenzitu potenia a to, či sa môžete počas výkonu prezliecť.",
            "Prečítajte celé zloženie a zistite úlohu menšinového elastanu alebo merina v zmesi.",
            "Skontrolujte gramáž, priesvitnosť pri natiahnutí, smer pružnosti, švy, zipsy a vetracie zóny.",
            "Pri vrstvení overte, či každá ďalšia vrstva umožní vlhkosti pokračovať smerom von.",
            "Prečítajte ošetrovací štítok ešte pred nákupom; špeciálny kus musí byť udržateľný vo vašej rutine.",
            "Po použití odev vyvetrajte a nenechávajte ho mokrý stlačený v športovej taške.",
            "Perte s primeranou dávkou, dostatkom priestoru a šetrným sušením bez nadmerného tepla.",
        ],
        "remember": [
            "Polyester, polyamid, merino a elastan plnia rozdielne úlohy; zmes treba hodnotiť ako celý systém.",
            "Rýchle schnutie neurčuje iba vlákno, ale hrúbka, priadza, pletenina, strih a vrstvenie.",
            "Elastan zaisťuje pružnosť, no je citlivý na teplo, chlór a dlhodobé mechanické opotrebovanie.",
            "Pach po športe súvisí s materiálom, kožným mazom, mikroorganizmami, časom do prania a nánosmi.",
            "Ošetrovací štítok konkrétneho odevu má prednosť pred všeobecným návodom na syntetiku alebo vlnu.",
        ],
        "mistakes": [
            "<strong>Výber iba podľa percenta polyesteru.</strong> Bez údajov o úplete, hrúbke a strihu neviete, ako bude odev odvádzať vlhkosť.",
            "<strong>Predpoklad, že viac elastanu znamená lepšie legíny.</strong> Kompresiu a návratnosť tvorí celý materiál a konštrukcia.",
            "<strong>Uzavretie mokrého oblečenia v taške.</strong> Predlžuje kontakt potu a kožného mazu s textíliou a podporuje pach.",
            "<strong>Veľmi vysoká teplota proti pachu.</strong> Môže poškodiť elastan, potlač, lepidlá alebo jemné merino.",
            "<strong>Automatická aviváž.</strong> Pri funkčných vrstvách môže meniť povrch a odvod vlhkosti; riaďte sa výrobcom.",
            "<strong>Nesprávne vrstvenie.</strong> Rýchloschnúce tričko pod nepriedušnou vrstvou nedokáže samo odviesť všetku vlhkosť.",
        ],
        "expert_heading": "Čo vieme o pachu a komforte po záťaži",
        "expert": [
            "Kontrolovaná štúdia pachov tričiek po cvičení ukázala rozdiel medzi skúmaným polyesterom a bavlnou v konkrétnych podmienkach. Autori spájali výsledok aj s rozdielnym mikrobiálnym osídlením materiálov. Zistenie je dôležité pre mechanizmus, no nemožno ho rozšíriť na každý polyesterový úplet, každého používateľa a každý spôsob prania.",
            "Ďalšia štúdia porovnávala komfort po cvičení pri odevoch z vlny, bavlny, viskózy a polyesteru. Také práce ukazujú, že subjektívny pocit súvisí s vlhkosťou, teplotou a vlastnosťami materiálu po záťaži. Reálny výsledok však mení strih, gramáž, intenzita výkonu a prostredie. Preto je vhodné čítať záver ako podklad pre výber, nie ako univerzálnu tabuľku víťazov.",
            "Pri novších pracovných textíliách sa hodnotí viac ukazovateľov riadenia vlhkosti naraz. To zodpovedá praxi: používateľ potrebuje, aby vnútorná strana prijala pot, preniesla ho, vonkajšia plocha ho rozviedla a celý odev následne uschol. Materiál, ktorý vyniká iba v jednej fáze, nemusí byť najlepší pre konkrétnu aktivitu.",
        ],
        "source_intro": "Zdroje podporujú rozlišovanie odvodu kvapalnej vlhkosti, prestupu pary, komfortu a pachu. Výsledky jednotlivých štúdií platia pre ich materiály a podmienky; nepredstavujú univerzálne poradie všetkých športových odevov.",
        "sources": [
            ("AATCC TM195: riadenie kvapalnej vlhkosti textílie", AATCC_MOISTURE),
            ("ISO 11092: tepelný odpor a odpor proti vodnej pare", ISO_WATER_VAPOUR),
            ("Štúdia pachu polyesterových a bavlnených tričiek po cvičení", SPORT_ODOR),
            ("Štúdia komfortu textílií po cvičení", POST_EXERCISE),
            ("Štúdia riadenia vlhkosti pri pracovných textíliách", WORKWEAR_STUDY),
            ("GINETEX: význam ošetrovacích symbolov", GINETEX),
        ],
        "product_intro": "Športové oblečenie zachytáva pot, kožný maz, deodorant a opaľovacie prípravky. Vhodný prací gél môže pomôcť pri bežných prateľných kusoch, ak ho použijete v primeranej dávke a necháte odev dôkladne opláchnuť.",
        "product_text": "Hypoalergénny prací gél Vevo Ylang Absolute je konkrétna voľba pre bežné športové textílie, ak ju povoľuje štítok. Odevy perte čo najskôr po použití, v primeranej náplni a bez zbytočne vysokej teploty.",
        "product_limit": "Merino, membrány, plavky, kompresné kusy, vodoodpudivé úpravy a lepené prvky môžu vyžadovať špeciálny prípravok alebo odlišný program.",
        "category_intro": "Pri porovnávaní pracích gélov pre športové oblečenie sledujte kompatibilitu s vláknom, dávkovanie a oplach. Silnejšia vôňa nerieši príčinu pachu ani poškodenú pružnosť.",
        "category_text": "V kategórii pracích gélov nájdete produkty pre bežnú domácu bielizeň. Pred praním technických alebo vlnených kúskov vždy overte pokyny výrobcu.",
        "related": [
            ("Polyamid verzus polyester", "/n/polyamid-vs-polyester-ktory-material-lepsie-znasa-pot-sport-a-caste-pranie"),
            ("Čo je polyamid", "/n/co-je-polyamid-alebo-nylon-vlastnosti-odolnost-a-pranie"),
            ("Čo je merino vlna", "/n/co-je-merino-vlna-vyhody-nevyhody-a-pranie-bez-zrazenia"),
            ("Čo je elastan", "/n/co-je-elastan-preco-je-v-leginach-spodnej-bielizni-a-sportovom-obleceni"),
            ("Ako prať syntetiku, polyester a elastan", "/n/ako-prat-syntetiku-polyester-a-elastan-aby-nezapachali-a-drzali-tvar"),
            ("Ako čítať priedušnosť a savosť textilu", ARTICLE_PROPERTIES),
        ],
        "faq_title": "materiály športového oblečenia",
        "faq": [
            ("Je na šport lepší polyester alebo polyamid?", "Závisí od úpletu a aktivity. Polyester ponúka široké možnosti rýchloschnúcich konštrukcií, polyamid býva hladký a odolný. Dôležitejší je celý odev než samotný názov."),
            ("Zapácha polyester vždy viac než merino?", "Nie vždy. Materiál môže ovplyvniť pach, ale výsledok mení človek, mikroorganizmy, kožný maz, čas do prania, konštrukcia a nánosy z predchádzajúcich cyklov."),
            ("Koľko elastanu majú mať dobré legíny?", "Neexistuje univerzálne percento. Skontrolujte návrat do tvaru, nepriehľadnosť pri natiahnutí, švy, pás a to, či látka pruží v potrebných smeroch."),
            ("Môžem športové oblečenie prať na vysokej teplote?", "Iba ak ju povoľuje štítok. Vysoké teplo môže poškodiť elastan, potlač, membránu alebo lepené časti a nie je automatickým riešením pachu."),
            ("Treba športové oblečenie prať po každom tréningu?", "Kusy nasiaknuté potom a v priamom kontakte s pokožkou je vhodné prať po použití podľa štítku. Ak pranie odkladáte, aspoň ich úplne rozložte a vyvetrajte."),
            ("Je merino vhodné na letný šport?", "Tenké merino alebo zmes môže niekomu vyhovovať aj v teple, no schne inak než ľahká syntetika. Rozhoduje gramáž, intenzita, strih a osobný tepelný komfort."),
        ],
    },
]


def article_hrefs(markup):
    return re.findall(r'href="([^"]+)"', markup)


def preflight_links(articles):
    rows = []
    seen = set()
    batch_paths = {f"/n/{article['link']}" for article in articles}
    headers = {"User-Agent": "Codex VEVO batch 40 link preflight"}

    for article in articles:
        target_url = f"{BASE}/n/{article['link']}"
        response = requests.get(target_url, timeout=35, allow_redirects=True, headers=headers)
        rows.append(
            {
                "url": target_url,
                "kind": "target_slug_precheck",
                "ok": response.status_code == 404,
                "status": response.status_code,
                "final_url": response.url,
            }
        )

        for href in article_hrefs(article["long"]):
            url = urljoin(BASE, href)
            if url in seen:
                continue
            seen.add(url)
            response = requests.get(url, timeout=35, allow_redirects=True, headers=headers)
            path = urlparse(url).path.rstrip("/")
            is_intra_batch = path in batch_paths
            expected_status = 404 if is_intra_batch else 200
            rows.append(
                {
                    "url": url,
                    "kind": "intra_batch_target_precheck" if is_intra_batch else "article_link",
                    "ok": response.status_code == expected_status,
                    "expected_status": expected_status,
                    "status": response.status_code,
                    "final_url": response.url,
                }
            )

    return {
        "checked_count": len(rows),
        "failure_count": sum(1 for row in rows if not row["ok"]),
        "links": rows,
    }


def main():
    candidate_titles = [
        line.strip()
        for line in CANDIDATES.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    article_by_title = {article["title"]: article for article in ARTICLES}
    if len(article_by_title) != len(ARTICLES) or set(candidate_titles) != set(article_by_title):
        raise SystemExit("Batch 40 titles do not exactly match the duplicate-guard candidate file")

    rendered = []
    for index, title in enumerate(candidate_titles):
        article = article_by_title[title]
        long_html = render_article(article)
        if not 120 <= len(article["meta"]) <= 165:
            raise SystemExit(
                f"Meta description length must be 120-165 for {article['title']}: {len(article['meta'])}"
            )
        for value in (article["title"], article["short"], article["meta"], long_html):
            hits = FORBIDDEN_PUBLIC_RE.findall(value)
            if hits:
                raise SystemExit(f"Forbidden public wording in {article['title']}: {hits}")
        rendered.append(
            {
                "title": article["title"],
                "title_tag": article["title"],
                "description": article["meta"],
                "short": article["short"],
                "long": long_html,
                "date_posted": PUBLISH_DATE,
                "time_posted": f"13:{index * 10:02d}:00",
                "active": True,
                "link": article["link"],
                "commenting": False,
            }
        )

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(
        json.dumps(rendered, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    report = preflight_links(rendered)
    OUT_PREFLIGHT.parent.mkdir(parents=True, exist_ok=True)
    OUT_PREFLIGHT.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "article_count": len(rendered),
                "output": str(OUT_JSON),
                "preflight": str(OUT_PREFLIGHT),
                "checked_count": report["checked_count"],
                "failure_count": report["failure_count"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    if report["failure_count"]:
        raise SystemExit("Batch 40 link preflight failed")


if __name__ == "__main__":
    main()
