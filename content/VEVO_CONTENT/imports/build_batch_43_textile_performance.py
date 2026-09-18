import html
import json
import re
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests


BASE = "https://www.vevo.sk"
PUBLISH_DATE = "2026-08-14"
CANDIDATES = Path("content/VEVO_CONTENT/batches/batch-43-candidates-2026-08-14.txt")
OUT_JSON = Path("content/VEVO_CONTENT/imports/batch-43-2026-08-14-articles.json")
OUT_PREFLIGHT = Path("content/VEVO_CONTENT/exports/batch-43-2026-08-14-link-preflight.json")

PRODUCT_NAME = "Prací gél hypoalergénny Vevo Ylang Absolute 1L"
PRODUCT_URL = "/p-1627/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l"
CATEGORY_NAME = "Pracie gély"
CATEGORY_URL = "/c/vevo-home-care/pranie/praci-gel"

ISO_TENSILE = "https://www.iso.org/standard/60676.html"
ISO_TEAR = "https://www.iso.org/standard/23370.html"
ISO_DRAPE = "https://www.iso.org/standard/41375.html"
ASTM_STIFFNESS = "https://store.astm.org/standards/d1388"
ISO_THERMAL = "https://www.iso.org/standard/85998.html"
ASTM_THERMAL = "https://store.astm.org/f1868-23.html"
AATCC_UV = "https://members.aatcc.org/store/tm183/579/"
WHO_UV = "https://www.who.int/news-room/questions-and-answers/item/radiation-protecting-against-skin-cancer"
UK_UV = "https://www.gov.uk/government/publications/ultraviolet-radiation-and-sunscreen/ultraviolet-radiation-frequently-asked-questions"
GINETEX = "https://www.ginetex.net/share/article/4201/care-symbols"

ARTICLE_SEAMS = "/n/pevnost-sva-a-posun-niti-preco-oblecenie-praska-pri-svoch"
ARTICLE_MARTINDALE = "/n/odolnost-textilu-proti-oderu-co-znamena-martindale-pri-obleceni-a-bytovych-latkach"
ARTICLE_SNAGGING = "/n/zatrhavanie-textilu-preco-vznikaju-vytiahnute-ocka-a-ako-im-predchadzat"
ARTICLE_HOLES = "/n/ako-predist-dierkam-v-trickach-po-prani-a-suseni"
ARTICLE_GSM = "/n/gramaz-latky-co-znamena-gsm-pri-uterakoch-oblieckach-a-trickach"
ARTICLE_COTTON_ELASTANE = "/n/bavlna-a-elastan-starostlivost-o-tricka-rifle-a-spodnu-bielizen"
ARTICLE_VISCOSE = "/n/co-je-viskoza-vlastnosti-krcivost-zrazanie-a-starostlivost"
ARTICLE_LYOCELL = "/n/co-je-lyocell-alebo-tencel-priedusnost-jemnost-a-starostlivost"
ARTICLE_SATIN = "/n/co-je-saten-nie-je-to-vzdy-hodvab-a-ako-ho-spravne-prat"
ARTICLE_MODAL_COMPARE = "/n/modal-vs-lyocell-vs-viskoza-ako-sa-lisia-pri-prani-a-noseni"
ARTICLE_LINEN = "/n/co-je-lan-preco-sa-krci-ako-ho-prat-a-ako-ho-zjemnit"
ARTICLE_FLEECE = "/n/co-je-fleece-hrejivost-zmolkovanie-a-starostlivost-pri-prani"
ARTICLE_MERINO = "/n/co-je-merino-vlna-vyhody-nevyhody-a-pranie-bez-zrazenia"
ARTICLE_SPORT_MATERIALS = "/n/polyester-polyamid-merino-alebo-elastan-z-coho-ma-byt-sportove-oblecenie"
ARTICLE_BREATHABILITY = "/n/priedusnost-savost-a-rychloschnutie-ako-citat-vlastnosti-textilu"
ARTICLE_WOOL_POLYAMIDE = "/n/vlna-a-polyamid-preco-sa-miesaju-vlakna-a-ako-to-ovplyvnuje-pranie"
ARTICLE_SUNSCREEN_STAIN = "/n/ako-vyprat-opalovaci-olej-z-plazovej-tuniky-a-uteraka"
ARTICLE_POLYAMIDE = "/n/co-je-polyamid-alebo-nylon-vlastnosti-odolnost-a-pranie"
ARTICLE_COLORFASTNESS = "/n/stalofarebnost-textilu-preco-farby-blednu-pri-prani-svetle-a-treni"
ARTICLE_SOFTSHELL = "/n/co-je-softshell-vrstvy-membrana-impregnacia-a-spravna-starostlivost"
ARTICLE_REPELLENT = "/n/ako-odstranit-repelent-z-outdoorovej-ciapky-a-navlekov-na-ruky"

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
<h2 style="margin-top: 0;">Prací prostriedok prispôsobte štítku a konštrukcii</h2>
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


def related_links(items):
    links = "".join(f'<li><a href="{href}">{esc(label)}</a></li>' for label, href in items)
    return f"<h2>Súvisiace návody na VEVO</h2><ul>{links}</ul>"


def faq(article):
    parts = [f"<h2>FAQ: {esc(article['faq_title'])}</h2>"]
    for question, answer in article["faq"]:
        parts.append(f"<h3>{esc(question)}</h3><p>{answer}</p>")
    return "\n".join(parts)


def render_article(article):
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
    parts.append("<h2>Praktický postup krok za krokom</h2>")
    parts.append("<ol>" + "".join(f"<li>{step}</li>" for step in article["steps"]) + "</ol>")
    parts.append(
        callout(
            "Kontrolný zoznam pred praním alebo hodnotením textilu",
            article["remember"],
            background="#f7fbf8",
            border="#dbe5de",
        )
    )
    parts.append("<h2>Najčastejšie chyby a nepresné závery</h2>")
    parts.append("<ul>" + "".join(f"<li>{item}</li>" for item in article["mistakes"]) + "</ul>")
    parts.append(f"<h2>{esc(article['expert_heading'])}</h2>")
    parts.extend(f"<p>{paragraph}</p>" for paragraph in article["expert"])
    parts.append(source_box(article))
    parts.append(commercial_blocks(article))
    parts.append(related_links(article["related"]))
    parts.append(faq(article))
    return "\n".join(parts)


ARTICLES = [
    {
        "title": "Pevnosť textilu v ťahu a proti roztrhnutiu: čo skúšky hovoria o odolnosti",
        "link": "pevnost-textilu-v-tahu-a-proti-roztrhnutiu-co-skusky-hovoria-o-odolnosti",
        "meta": "Pevnosť v ťahu a proti roztrhnutiu nie je to isté. Zistite, čo skúšky merajú, prečo látka zlyhá a ako jej životnosť chrániť praním.",
        "short": "Látka môže zniesť veľký rovnomerný ťah a pritom sa ľahko roztrhnúť od malého zárezu. Sprievodca odlišuje pevnosť textilu, šva, oder, poškodenie a praktickú starostlivosť.",
        "answer": "Pevnosť v ťahu opisuje, akú najväčšiu silu znesie skúšobný pás látky pri naťahovaní, kým pevnosť proti roztrhnutiu sleduje pokračovanie už začatého natrhnutia. Nie sú zameniteľné. Výsledok mení smer vlákien, väzba alebo úplet, hustota, priadza, vlhkosť, povrchová úprava aj poškodenie. V domácnosti preto nemožno odolnosť odhadnúť iba podľa hrúbky či materiálu na etikete; najviac pomáha chrániť textil pred ostrými hranami, preplneným bubnom, nadmerným trením a teplom, ktoré štítok nepovoľuje.",
        "intro": "Roztrhnuté koleno, diera pri vrecku a látka prasknutá vedľa šva vyzerajú podobne, ale nemusia mať rovnakú príčinu. Pri jednom výrobku zlyhá samotná plocha textilu, pri inom šitie, posun nití alebo miesto oslabené oderom. Odborné skúšky preto nehovoria o jednej všeobecnej pevnosti. Každá zaťažuje presne pripravenú vzorku určitým spôsobom a výsledok platí iba v hraniciach danej metódy.",
        "quick": [
            "<strong>Ťah nie je trhanie:</strong> rovnomerné napínanie celej šírky a šírenie existujúceho natrhnutia sú dva odlišné mechanizmy.",
            "<strong>Smer rozhoduje:</strong> tkanina môže mať iný výsledok v osnove a iný v útku; úplet sa správa inak než pevná tkanina.",
            "<strong>Malé poškodenie mení situáciu:</strong> zárez, zatrhnutie alebo prepálené vlákna sú miestom koncentrácie napätia.",
            "<strong>Hrúbka sama nestačí:</strong> dôležitá je priadza, konštrukcia, hustota, väzba, dokončenie aj kvalita šitia.",
            "<strong>Pranie nevytvorí chýbajúcu pevnosť:</strong> rozumná starostlivosť môže obmedziť ďalšie oslabovanie, nie opraviť pretrhnuté vlákna.",
        ],
        "overview_heading": "Čo znamená pevnosť textilu v bežnom používaní",
        "overview": [
            "Textil počas nosenia nezažíva iba jeden druh sily. Sed nohavíc sa napína pri pohybe, roh plachty sa ťahá pri navliekaní, ramienko tašky prenáša sústredené zaťaženie a uterák sa zachytí o ostrú hranu. Materiál môže byť veľmi dobrý v jednej situácii a slabší v druhej. Preto výraz odolná látka bez vysvetlenia skúšky, konštrukcie a účelu zostáva iba všeobecným opisom.",
            "Pri hodnotení hotového výrobku navyše vstupujú do výsledku švy, otvory po ihle, zipsy, vrecká, strih a miesta, kde sa spájajú rozdielne vrstvy. Pevná tkanina môže zlyhať pri nekvalitnom šve a dobre ušitý odev môže prasknúť v ploche, ktorú predtým stenčil oder. Súvisiaci článok o <a href=\"/n/pevnost-sva-a-posun-niti-preco-oblecenie-praska-pri-svoch\">pevnosti šva a posune nití</a> preto rieši inú otázku než tento sprievodca.",
            "Domáce porovnanie dvoch látok ťahaním rukami nie je meranie. Neviete udržať rovnakú šírku vzorky, smer, rýchlosť, upnutie ani počiatočné poškodenie. Zmysluplnejšie je sledovať varovné znaky: rednutie, presvitanie, vyťahané očká, ostré zalomenie, odreté miesto alebo začínajúcu dierku. Takéto signály hovoria, že ďalšie lokálne namáhanie môže poškodenie urýchliť.",
        ],
        "table1_heading": "Pevnosť, trhanie, šev a oder: čo sa nesmie zamieňať",
        "table1_intro": "Jednotlivé skúšky vytvárajú odlišné podmienky. Výsledok z jedného stĺpca nemožno použiť ako náhradu za ostatné vlastnosti ani ako priamu predpoveď životnosti každého hotového odevu.",
        "table1_headers": ["Vlastnosť", "Čo sa zaťažuje", "Typická otázka", "Čo výsledok sám nepovie"],
        "table1_rows": [
            ("Pevnosť v ťahu", "Pripravená vzorka sa naťahuje v určenom smere až po maximálnu silu alebo porušenie.", "Koľko rovnomerného ťahu znesie nepoškodená plocha?", "Ako ľahko pokračuje už začaté natrhnutie."),
            ("Pevnosť proti roztrhnutiu", "Sleduje sa šírenie zámerne začatého roztrhnutia podľa konkrétnej metódy.", "Ako látka odoláva pokračovaniu trhliny?", "Maximálnu silu celej neporušenej šírky."),
            ("Pevnosť šva", "Zaťažuje sa zošitý spoj a jeho okolie.", "Praskne niť, látka alebo sa spoj otvorí?", "Odolnosť celej plochy bez šva."),
            ("Odolnosť proti oderu", "Povrch sa opakovane trie za definovaných podmienok.", "Ako rýchlo sa povrch opotrebuje?", "Koľko jednorazového ťahu znesie látka."),
            ("Pevnosť pri pretlaku", "Plocha je namáhaná viac smermi naraz tlakom.", "Ako sa správa pružná plocha pri viacsmerej deformácii?", "Výsledok prúžkového ťahu v jednom smere."),
        ],
        "sections": [
            {
                "heading": "Ako sa meria pevnosť v ťahu",
                "paragraphs": [
                    "Pri prúžkovej metóde sa vzorka presnej šírky upne do skúšobného zariadenia a predlžuje riadenou rýchlosťou. Zaznamenáva sa sila a predĺženie, pričom dôležitým údajom je maximálna sila a predĺženie pri tejto sile. Podmienky, rozmery a atmosféra sú stanovené preto, aby sa výsledky dali porovnávať v rámci rovnakej metódy.",
                    "Číslo však nepredstavuje univerzálnu hranicu pre hotové nohavice alebo posteľnú bielizeň. Skúšobný pás nemá všetky švy, záhyby, vrecká ani lokálne opotrebovanie výrobku. Výsledok je technická vlastnosť vzorky, ktorú treba spojiť s konštrukciou a zamýšľaným použitím, nie sľub, že odev nikdy nepraskne.",
                ],
            },
            {
                "heading": "Maximálna sila a predĺženie nie sú to isté",
                "paragraphs": [
                    "Dve látky môžu dosiahnuť podobnú maximálnu silu, no jedna sa pred ňou výrazne natiahne a druhá zostáva tuhá. To zásadne mení pocit pri nosení aj rozloženie zaťaženia. Pružný úplet sa prispôsobí pohybu, kým pevná tkanina prenesie napätie na šev, záhyb alebo na miesto, kde strih neposkytuje dostatok voľnosti.",
                    "Vyššie predĺženie nie je automaticky lepšie. Pri legínach môže byť žiadané, pri popruhu či poťahu môže nadmerná deformácia prekážať. Záleží aj na tom, či sa materiál po odľahčení vráti, alebo zostane vyťahaný. Preto treba odlíšiť jednorazové predĺženie, pružnú obnovu a trvalú deformáciu.",
                ],
            },
            {
                "heading": "Prečo je roztrhnutie samostatná vlastnosť",
                "paragraphs": [
                    "Natrhnutie začína na konkrétnom mieste. Okraj vrecka zachytený o kľučku, malý rez od kovania alebo dierka po ostrých zuboch zipsu vytvoria špičku, pri ktorej sa sila sústredí na menší počet priadzí. Látka, ktorá v neporušenom páse znáša veľký ťah, môže od takého bodu pokračovať v trhaní prekvapivo ľahko.",
                    "Rozdiel ukazuje, prečo sa malé poškodenie neoplatí ignorovať. Keď sa trhlina zväčšuje, mení sa geometria zaťaženia a postupne sa preťažujú ďalšie priadze. Včasné zašitie alebo vhodná záplata môže rozložiť silu do väčšej plochy; samotné ďalšie pranie pretrhnuté vlákna nespojí.",
                ],
                "callout": {
                    "title": "Kedy prestať odev bežne používať",
                    "items": [
                        "Trhlina je pri vrecku, rozkroku, kolene alebo inom mieste, ktoré sa pri pohybe opakovane napína.",
                        "Okolie dierky je priesvitné, chlpaté alebo odreté a záplata by držala iba na oslabenej ploche.",
                        "Poškodenie zasahuje bezpečnostný prvok, popruh, ochrannú vrstvu alebo technický odev.",
                        "Látka sa trhá pri veľmi malej sile aj mimo pôvodného poškodenia; ďalší postup má posúdiť opravovňa alebo výrobca.",
                    ],
                },
            },
            {
                "heading": "Osnova, útok, očká a smer zaťaženia",
                "paragraphs": [
                    "Tkanina má osnovné a útkové priadze, ktoré môžu mať rozdielnu jemnosť, hustotu, zvlnenie aj pevnosť. Výsledok v jednom smere preto nemusí platiť v druhom. Pri šikmom zaťažení sa navyše mení geometria väzby a priadze sa môžu najprv presúvať, až potom výraznejšie napínať.",
                    "Úplet tvorí systém očiek. Pri pohybe sa časť deformácie odohrá zmenou ich tvaru, čo prináša pružnosť, ale aj iný spôsob šírenia poškodenia. Zatrhnuté očko môže vytvoriť reťazový problém, zatiaľ čo pevná tkanina sa skôr rozstrapká alebo roztrhne pozdĺž určitého smeru. Praktické riziko vysvetľuje aj návod o <a href=\"/n/zatrhavanie-textilu-preco-vznikaju-vytiahnute-ocka-a-ako-im-predchadzat\">zatrhávaní textilu</a>.",
                ],
            },
            {
                "heading": "Vlákno, priadza a konštrukcia pôsobia spoločne",
                "paragraphs": [
                    "Názov vlákna na etikete je iba jedna časť skladačky. Dôležitá je dĺžka a kvalita vlákien, spôsob spriadania, zákrut priadze, hrúbka, hustota, väzba alebo úplet a dokončovacia úprava. Dve stopercentne bavlnené látky môžu mať úplne inú pevnosť aj spôsob poškodenia.",
                    "Ani vyššia <a href=\"/n/gramaz-latky-co-znamena-gsm-pri-uterakoch-oblieckach-a-trickach\">gramáž GSM</a> automaticky nezaručuje lepšiu odolnosť. Viac hmoty na plochu môže pomôcť v určitom výrobku, ale rozhoduje jej usporiadanie. Voľná ťažšia konštrukcia a hustá ľahšia konštrukcia rozkladajú silu rozdielne; bez rovnakej metódy ich nemožno zoradiť jedným číslom.",
                ],
            },
            {
                "heading": "Ako oder pripraví miesto na neskoršie pretrhnutie",
                "paragraphs": [
                    "Opakované trenie môže stenčiť priadze skôr, než vznikne viditeľná diera. Typické sú vnútorné stehná, lakte, kolená, okraje manžiet, plocha pod batohom alebo roh obliečky. Keď časť vlákien stratí prierez alebo sa uvoľní z konštrukcie, zostávajúce priadze preberajú väčšie zaťaženie a textil zlyhá pri bežnom pohybe.",
                    "Takéto zlyhanie sa niekedy pripíše poslednému praniu, hoci rozhodujúce poškodenie vznikalo mesiace. <a href=\"/n/odolnost-textilu-proti-oderu-co-znamena-martindale-pri-obleceni-a-bytovych-latkach\">Martindale a odolnosť proti oderu</a> opisujú povrchové opotrebovanie, nie priamo maximálnu silu v ťahu. V praxi sa však vlastnosti stretávajú: oder vytvorí slabé miesto a ťah ho následne otvorí.",
                ],
                "callout": {
                    "title": "Rýchla diagnostika podľa miesta poškodenia",
                    "items": [
                        "<strong>Vedľa šva:</strong> skontrolujte posun nití, tesný strih, otvory po ihle a kvalitu šitia.",
                        "<strong>V strede odretej plochy:</strong> pravdepodobne pôsobil dlhodobý kontakt a stenčenie priadzí.",
                        "<strong>Od ostrého okraja:</strong> išlo skôr o lokálny zárez alebo zachytenie než o bežné plošné opotrebovanie.",
                        "<strong>Viac náhodných dierok:</strong> prezrite bubon, zipsy, kovania, poškodenie vláken aj spôsob skladovania.",
                    ],
                    "background": "#f7fbf8",
                    "border": "#dbe5de",
                },
            },
            {
                "heading": "Čo s pevnosťou robí voda a prací cyklus",
                "paragraphs": [
                    "Niektoré vlákna a konštrukcie sa za mokra správajú inak než za sucha. Voda mení trenie medzi vláknami, napučanie, pohyblivosť priadzí aj hmotnosť výrobku. Preto odborná metóda presne uvádza, či sa skúša kondicionovaná suchá alebo mokrá vzorka; výsledky z rozdielnych stavov nemožno bez vysvetlenia miešať.",
                    "V práčke sa k vode pridáva ohýbanie, trenie a nárazy o inú bielizeň. Preplnený bubon neznamená iba slabšie pranie: textil sa môže stláčať, naťahovať medzi ťažkými kusmi a zachytávať o zipsy či háčiky. Jemné alebo už oslabené oblečenie perte naruby, zapnite kovania podľa štítku a oddeľte ho od uterákov či ťažkých riflí.",
                ],
            },
            {
                "heading": "Teplo, sušenie a chemické poškodenie",
                "paragraphs": [
                    "Teplota nad povolenie štítku môže meniť elastické vlákna, povrchové úpravy, lepené vrstvy aj rozmer výrobku. Presušenie zvyšuje mechanické namáhanie, pretože textil sa v bubne sušičky ďalej ohýba a trie aj po odparení väčšiny vody. Pri zmesi rozhoduje najcitlivejšia časť, nie najodolnejšie vlákno.",
                    "Bieliace alebo škvrnové prostriedky použité na nevhodnú farbu a vlákno môžu oslabiť miesto bez okamžitého roztrhnutia. Poškodenie sa prejaví až pri ďalšom natiahnutí. Lokálny prostriedok preto najprv skúšajte podľa etikety na skrytom mieste, nenechávajte ho pôsobiť dlhšie a textil po zásahu dôkladne opláchnite.",
                ],
            },
            {
                "heading": "Prečo laboratórny výsledok nie je životnosť odevu",
                "paragraphs": [
                    "Skúška znižuje množstvo premenných, aby sa dala jedna vlastnosť opakovateľne porovnať. Reálny odev má tvar tela, opakované cykly, pot, slnko, pranie, švy a lokálne tlaky. Výrobca preto môže kombinovať viac skúšok a požiadaviek podľa účelu. Jedna vysoká hodnota nemôže zastúpiť kvalitu strihu ani dlhodobé používanie.",
                    "Pri nákupe má väčší význam jasne uvedený účel a zodpovedajúca konštrukcia než izolovaný superlatív. Na pracovné nohavice hľadajte zosilnené namáhané miesta a opraviteľnosť, na jemnú blúzku primeranú voľnosť a čisté švy, na posteľnú bielizeň kvalitné okraje a dostatok materiálu okolo zapínania.",
                ],
                "callout": {
                    "title": "Čo sa dá zistiť doma a čo nie",
                    "items": [
                        "Doma viete nájsť rednutie, poškodenie, nesprávnu veľkosť, ostré kovania a opakované miesto trenia.",
                        "Bez laboratória neviete spoľahlivo určiť maximálnu silu ani porovnať dve látky podľa normovanej metódy.",
                        "Značka vlákna, cena ani hrúbka samy osebe nenahrádzajú údaje o konštrukcii a skúške.",
                        "Pri ochrannom, nosnom alebo bezpečnostnom textile sa riaďte výrobcom a poškodený kus nehodnoťte iba pohľadom.",
                    ],
                },
            },
            {
                "heading": "Ako predĺžiť životnosť bez prehnaných sľubov",
                "paragraphs": [
                    "Začnite správnou veľkosťou a používaním. Príliš tesný odev prenáša veľké sily do švov, rozkroku, lakťov a kolien; preťažovaná taška namáha uchytenie popruhov. Po nosení opravte malé poškodenie skôr, než sa zväčší, a odstráňte ostrý zdroj zachytávania v práčke, skrini alebo na nábytku.",
                    "Pri praní rešpektujte štítok, nepreplňujte bubon, zatvorte háčiky a citlivé kusy chráňte v sieťke. Odev vyberte po skončení cyklu a nesušte ho spôsobom, ktorý vytvára bodový ťah mokrou hmotnosťou. Návod <a href=\"/n/ako-predist-dierkam-v-trickach-po-prani-a-suseni\">ako predísť dierkam v tričkách</a> pomôže odlíšiť poškodenie v práčke od opotrebovania pri nosení.",
                ],
            },
        ],
        "table2_heading": "Čo naznačujú viditeľné prejavy na textílii",
        "table2_intro": "Pohľad nenahradí skúšku, ale môže ukázať mechanizmus a najrozumnejší ďalší krok. Poškodený kus pred ďalším praním oddeľte, aby sa trhlina nezachytila o inú bielizeň.",
        "table2_headers": ["Prejav", "Pravdepodobné vysvetlenie", "Čo skontrolovať", "Ďalší krok"],
        "table2_rows": [
            ("Trhlina od ostrého zárezu", "Sústredenie napätia v už poškodenom bode.", "Kľučku, kovanie, zips, hranu bubna alebo pracovné prostredie.", "Obmedziť ťah a opraviť s dostatočným presahom do zdravej plochy."),
            ("Diera v priesvitnom odretom mieste", "Dlhodobé stenčenie priadzí trením.", "Miesto kontaktu s telom, batohom, sedadlom alebo nábytkom.", "Znížiť trenie; záplatu ukotviť mimo oslabenej zóny."),
            ("Otvor vedľa šva", "Napätie, posun nití, nevhodná ihla alebo oslabenie pri šití.", "Veľkosť odevu, rezervu šva a rovnaké poškodenie na druhej strane.", "Najprv vyriešiť príčinu, potom spoj odborne spevniť."),
            ("Viac dierok po cykle", "Zachytávanie, ostré kovania alebo už oslabené vlákna.", "Bubon, tesnenie, zipsy, háčiky a obsah vreciek.", "Zdroj odstrániť; citlivé kusy prať oddelene alebo v sieťke."),
            ("Vyťahaná plocha bez trhliny", "Trvalá deformácia, slabá obnova alebo nevhodné zaťaženie.", "Zloženie, teplo, veľkosť a spôsob sušenia.", "Nesnažiť sa materiál sťahovať vyšším teplom mimo štítku."),
        ],
        "steps": [
            "Prečítajte celé zloženie a symboly, potom si všimnite konštrukciu, švy, zosilnenia a namáhané miesta.",
            "Pred praním nájdite dierky, zatrhnutia a odreté plochy; poškodenie zabezpečte, aby sa ďalej nezachytilo.",
            "Rozdeľte ľahké citlivé kusy od ťažkých riflí, uterákov, kovových zapínaní a drsných povrchov.",
            "Zvoľte program, teplotu, otáčky a dávku podľa štítku, miery znečistenia, tvrdosti vody a veľkosti náplne.",
            "Po cykle textil vyberte bez ťahania za jediný mokrý bod a sušte ho s rovnomernou oporou.",
            "Ak sa poškodenie opakuje na rovnakom mieste, hľadajte zdroj pri nosení alebo praní skôr, než vymeníte prostriedok.",
        ],
        "remember": [
            "Je výrobok nepoškodený, alebo už má zárez, dierku, rednutie či zatrhnuté očko?",
            "Namáha sa plocha, šev alebo miesto pri kovaní a vrecku?",
            "Periete s podobne ťažkými a hladkými kusmi, alebo sa jemná látka zachytáva o tvrdé prvky?",
            "Povoľuje štítok zvolenú teplotu, sušičku, bielenie a otáčky?",
            "Je oprava ukotvená v zdravej ploche a nemení bezpečnosť funkčného výrobku?",
        ],
        "mistakes": [
            "Považovať pevnosť v ťahu, roztrhnutie, oder a pevnosť šva za jedno číslo.",
            "Usúdiť z vyššej gramáže alebo hrúbky, že výrobok sa nemôže roztrhnúť.",
            "Ignorovať malý zárez a pokračovať v nosení na silno namáhanom mieste.",
            "Za každú dieru viniť práčku bez kontroly dlhodobého oderu, veľkosti a ostrých predmetov.",
            "Prať jemný poškodený kus spolu s uterákmi, rifľami, otvorenými zipsami a háčikmi.",
            "Skúšať stiahnuť vyťahaný materiál horúcou vodou alebo sušičkou mimo pokynov výrobcu.",
        ],
        "expert_heading": "Odbornejší pohľad: porovnávajte iba rovnaké metódy",
        "expert": [
            "ISO 13934-1:2013, potvrdená pri systematickom preskúmaní v roku 2024, opisuje určenie maximálnej sily a predĺženia pri maximálnej sile prúžkovou metódou. Je zameraná najmä na tkaniny vrátane niektorých pružných tkanín a vymedzuje aj materiály, pre ktoré sa bežne nepoužíva. Dôležitá je rovnováha vzorky v štandardnej atmosfére a možnosť skúšania v mokrom stave.",
            "ISO 13937-2:2000, potvrdená v roku 2023, sa venuje sile pri jednom roztrhnutí vzorky tvaru nohavíc. Už názov a rozsah ukazujú, že ide o inú geometriu než pri priamom ťahu neporušeného pásu. Výsledky sa preto nesmú prepočítať jednoduchým pravidlom ani používať bez uvedenia metódy, smeru a stavu vzorky.",
            "Pri hotovom výrobku sa k materiálovej skúške pridávajú švy, strih, lokálne zosilnenia, opakované cykly a spôsob používania. Odborný údaj je užitočný na kontrolu kvality alebo porovnanie špecifikácií, ale nie je úplnou simuláciou života odevu. Praktická starostlivosť má odstraňovať zbytočné poškodenie, nie predstierať laboratórne zvýšenie pevnosti.",
        ],
        "source_intro": "Nasledujúce normy presne oddeľujú maximálnu silu pri prúžkovom ťahu od sily potrebnej na šírenie jedného roztrhnutia. Pokyny GINETEX pomáhajú preniesť hranice výrobcu do domácej starostlivosti.",
        "sources": [
            ("ISO 13934-1:2013: maximálna sila a predĺženie prúžkovou metódou", ISO_TENSILE),
            ("ISO 13937-2:2000: sila pri jednom roztrhnutí", ISO_TEAR),
            ("GINETEX: význam symbolov ošetrovania textilu", GINETEX),
        ],
        "product_intro": "Primerané dávkovanie a dobrý oplach pomáhajú obmedziť zvyšky, stuhnutosť a zbytočné opakované pranie. Pri poškodenom alebo špeciálnom textile je však rozhodujúci štítok a mechanická ochrana pred ďalším trhaním.",
        "product_text": "Prací gél z marseillského mydla je určený na bežnú domácu bielizeň. Použite dávku z etikety s ohľadom na tvrdosť vody a znečistenie, nepreplňujte bubon a citlivý kus oddeľte od tvrdých zapínaní.",
        "product_limit": "Žiadny prací gél neobnoví pretrhnuté vlákna ani nezmení laboratórnu pevnosť nekvalitnej konštrukcie. Opravu urobte pred praním a pri technickom alebo ochrannom výrobku sa riaďte výrobcom.",
        "category_intro": "Pri výbere gélu sledujte farbu, typ vlákna, povrchovú úpravu a symboly. Odolnosť textilu chráni najmä správna mechanika, primeraná teplota, dostatok priestoru a šetrné sušenie.",
        "category_text": "V kategórii nájdete pracie gély na bežnú bielizeň. Pred použitím si prečítajte etiketu produktu aj odevu; vlna, membrány, ochranné povrchy a veľmi jemné materiály môžu vyžadovať osobitný prostriedok alebo postup.",
        "related": [
            ("Pevnosť šva a posun nití", ARTICLE_SEAMS),
            ("Odolnosť textilu proti oderu a Martindale", ARTICLE_MARTINDALE),
            ("Zatrhávanie textilu a vytiahnuté očká", ARTICLE_SNAGGING),
            ("Ako predísť dierkam v tričkách", ARTICLE_HOLES),
            ("Čo znamená gramáž GSM", ARTICLE_GSM),
            ("Bavlna a elastan v tričkách, rifliach a bielizni", ARTICLE_COTTON_ELASTANE),
        ],
        "faq_title": "pevnosť textilu a roztrhnutie",
        "faq": [
            ("Je pevnosť v ťahu to isté ako pevnosť proti roztrhnutiu?", "Nie. Prvá skúška naťahuje pripravenú neporušenú vzorku, druhá sleduje pokračovanie už začatého natrhnutia podľa určenej geometrie."),
            ("Je hrubšia látka vždy pevnejšia?", "Nie. Hrúbka a gramáž sú iba časťou výsledku; rozhoduje priadza, hustota, väzba alebo úplet, smer, úprava a existujúce poškodenie."),
            ("Prečo sa nohavice roztrhli vedľa šva?", "Príčinou môže byť tesný strih, posun nití, poškodenie pri šití, slabá plocha vedľa šva alebo dlhodobé namáhanie. Samotný šev nemusí byť jediným problémom."),
            ("Môže pranie oslabiť textil?", "Nevhodná teplota, bielenie, preplnený bubon, drsné kovania a opakované silné trenie môžu poškodenie urýchliť. Správny postup vychádza zo štítku."),
            ("Dá sa roztrhnutá látka spevniť pracím prostriedkom?", "Nie. Prací prostriedok čistí; pretrhnuté vlákna treba opraviť a záplatu alebo šev ukotviť v zdravej ploche."),
            ("Ako doma porovnať pevnosť dvoch tričiek?", "Spoľahlivú normovanú hodnotu doma nezískate. Môžete však porovnať konštrukciu, hustotu, švy, priesvitné odreté miesta, poškodenia a vhodnosť pre konkrétne použitie."),
        ],
    },
    {
        "title": "Splývavosť textilu: prečo niektoré látky držia tvar a iné kopírujú postavu",
        "link": "splyvavost-textilu-preco-niektore-latky-drzia-tvar-a-ine-kopiruju-postavu",
        "meta": "Čo je splývavosť textilu, ako súvisí s tuhosťou, hmotnosťou a strihom a prečo sa vzhľad látky môže po praní alebo zvlhnutí zmeniť.",
        "short": "Splývavosť nie je iba mäkkosť ani vlastnosť jedného vlákna. Vzniká zo súhry hmotnosti, ohybovej tuhosti, väzby, úpletu, úpravy, vlhkosti a strihu odevu.",
        "answer": "Splývavosť opisuje, ako sa plocha textilu pod vlastnou hmotnosťou ohýba a vytvára záhyby. Látka s voľným, hlbokým splývaním kopíruje tvar a pohyb, kým tuhšia konštrukcia viac odstáva alebo drží siluetu. Výsledok neurčuje iba názov vlákna: mení ho jemnosť a hustota priadze, väzba či úplet, gramáž, hrúbka, smer, povrchová úprava, vlhkosť, švy aj strih. Praním možno vzhľad zachovať alebo zhoršiť, ale prací prostriedok nevytvorí konštrukciu, ktorú látka nemá.",
        "intro": "Pri letných šatách očakávame, že látka mäkko padne, pri saku chceme presnejšiu líniu a pri závese pravidelné záhyby. Všetky tieto situácie pracujú so splývavosťou, no bežný opis mäkká látka je príliš úzky. Textil môže byť príjemný na dotyk a zároveň držať objem; iný môže mať hladký povrch, ale pod vlastnou hmotnosťou sa výrazne deformovať. Rozhoduje správanie celej plošnej konštrukcie.",
        "quick": [
            "<strong>Splývavosť nie je iba mäkkosť:</strong> mäkkosť hodnotí dotyk, splývanie opisuje tvar celej plochy pod gravitáciou.",
            "<strong>Hmotnosť a tuhosť pôsobia spolu:</strong> ťažšia látka nemusí splývať, ak má veľmi vysokú ohybovú tuhosť.",
            "<strong>Smer môže meniť výsledok:</strong> osnova, útok a šikmý smer sa pri ohýbaní nemusia správať rovnako.",
            "<strong>Vlhkosť a úprava menia dojem:</strong> mokrá látka je ťažšia a priadze môžu byť pohyblivejšie; zvyšky produktu ju môžu stuhnúť.",
            "<strong>Strih je rovnako dôležitý ako materiál:</strong> záševky, podšívka, švy, riasenie a množstvo látky určujú konečnú siluetu.",
        ],
        "overview_heading": "Čo presne znamená, keď látka dobre splýva",
        "overview": [
            "Keď kruhovú alebo inú definovanú vzorku podopriete na menšej ploche, jej voľný okraj klesne a vytvorí obrys. Miera a tvar tohto poklesu súvisia s tým, ako ľahko sa textil ohýba pod vlastnou hmotnosťou. Odborné hodnotenie používa kontrolované rozmery a podmienky; domáce prehodenie cez ruku môže dať iba orientačný dojem.",
            "Veľmi splývavá látka vytvára užšie a hlbšie záhyby, reaguje na pohyb a často viac kopíruje telo. Tuhšia látka tvorí širšie oblúky, odstáva a drží objem. Ani jedna možnosť nie je všeobecne kvalitnejšia. Mäkké šaty, košeľa s čistým golierom, ochranná bunda a dekoračný záves majú odlišný cieľ.",
            "Splývavosť sa môže meniť počas života výrobku. Pranie odstráni dočasnú výrobnú úpravu, vlákna napučia, úplet sa uvoľní alebo sa naopak usadia minerály a zvyšky prostriedku. Dôležité je sledovať celý štítok a neskúšať dosiahnuť mäkší pád vysokou dávkou aviváže, najmä pri funkčných, pružných alebo špeciálne upravených materiáloch.",
        ],
        "table1_heading": "Čo ovplyvňuje splývanie a čo si všimnúť",
        "table1_intro": "Každý faktor pôsobí v kombinácii s ostatnými. Preto dve látky z rovnakého vlákna môžu padať úplne inak a rovnaká látka sa môže zmeniť po navlhčení, podšití alebo inom strihu.",
        "table1_headers": ["Faktor", "Ako pôsobí", "Praktický prejav", "Hranica záveru"],
        "table1_rows": [
            ("Ohybová tuhosť", "Vyjadruje odpor plochy proti ohnutiu v daných podmienkach.", "Vyššia tuhosť podporuje odstávanie a širšie oblúky.", "Nie je totožná s drsnosťou ani pevnosťou v ťahu."),
            ("Plošná hmotnosť", "Vlastná hmotnosť vytvára silu, ktorá voľnú plochu sťahuje nadol.", "Ťažšia pružná látka môže padať hlbšie.", "Gramáž bez tuhosti a hrúbky nestačí."),
            ("Väzba alebo úplet", "Určuje, ako sa priadze pohybujú, krížia a menia geometriu.", "Voľnejší úplet často reaguje inak než hustá plátnová tkanina.", "Názov konštrukcie nehovorí všetko o priadzi a úprave."),
            ("Smer", "Osnova, útok, riadky očiek a šikmý smer majú inú ohybnosť.", "Šikmo strihaná sukňa môže kopírovať postavu viac.", "Výsledok jedného smeru nemožno preniesť na všetky."),
            ("Povrchová úprava a vlhkosť", "Živice, apretúra, zmäkčenie, voda alebo zvyšky menia trenie a hmotnosť.", "Nová látka po prvom praní padá inak alebo stuhne.", "Zmenu treba oddeliť od zrazenia a deformácie."),
        ],
        "sections": [
            {
                "heading": "Splývavosť, mäkkosť a tuhosť sú rozdielne pojmy",
                "paragraphs": [
                    "Mäkkosť je najmä dotykový vnem. Ovplyvňuje ju povrch vlákien, chĺpok, hladkosť, teplota a tlak ruky. Splývavosť sleduje priestorový tvar plochy, keď pôsobí gravitácia. Zamat môže pôsobiť mäkko, no pri určitej konštrukcii držať objem; hladká tenká tkanina môže byť na dotyk chladná a pritom veľmi pohyblivá.",
                    "Ohybová tuhosť je technickejšia časť vysvetlenia. Hovorí o odpore proti ohnutiu za definovaných podmienok a môže sa líšiť podľa smeru aj strany. Drape, teda celkové splývanie, zahŕňa viac smerov a vlastnú hmotnosť vzorky. Preto jeden pocit medzi prstami nemôže nahradiť pozorovanie celej plochy.",
                ],
            },
            {
                "heading": "Prečo gramáž sama nepredpovie pád látky",
                "paragraphs": [
                    "Vyššia plošná hmotnosť zvyšuje silu, ktorou voľná časť smeruje nadol. Ak sa však súčasne zvýši hrúbka a tuhosť, látka môže stále držať pevný oblúk. Naopak tenká, jemná a ohybná tkanina môže pri nižšej gramáži vytvárať množstvo úzkych záhybov. Hmotnosť a odpor proti ohnutiu treba vnímať spoločne.",
                    "Článok o <a href=\"/n/gramaz-latky-co-znamena-gsm-pri-uterakoch-oblieckach-a-trickach\">gramáži GSM</a> vysvetľuje hmotnosť na jednotku plochy, nie splývavosť. Pri nákupe šiat alebo závesu preto nehľadajte jedno ideálne číslo. Porovnávajte vzorky rovnakého účelu, sledujte tvar záhybov a overte, či výrobca uvádza zloženie, konštrukciu a spôsob starostlivosti.",
                ],
            },
            {
                "heading": "Tkanina, úplet a pohyb priadzí",
                "paragraphs": [
                    "V tkanine sa osnovné a útkové priadze krížia vo väzbe. Hustota, dĺžka väzby, zákrut a zvlnenie určujú, ako ľahko sa priadze pri ohýbaní preskupia. Plátnová väzba má veľa väzných bodov a často pôsobí stabilnejšie, kým iná väzba môže vytvoriť hladší povrch a voľnejší pohyb. Konkrétny výsledok však stále mení priadza a úprava.",
                    "Úplet sa deformuje zmenou tvaru očiek. Vďaka tomu sa môže prispôsobiť telu a pohybu bez rovnakého napätia ako pevná tkanina. Súčasne sa ľahšie vyťahuje vlastnou mokrou hmotnosťou alebo na vešiaku. Splývavé úpletové šaty preto potrebujú pri sušení rovnomernejšiu oporu než ľahká stabilná košeľovina.",
                ],
                "callout": {
                    "title": "Rýchle porovnanie bez poškodzovania látky",
                    "items": [
                        "Položte dve vzorky rovnakej veľkosti cez rovnakú zaoblenú oporu a porovnajte hĺbku aj počet záhybov.",
                        "Otočte vzorku o deväťdesiat stupňov; výrazná zmena ukazuje smerovú odlišnosť.",
                        "Nenaťahujte neznámy úplet silou a nerobte závery z malej vzorky s pevným okrajom.",
                        "Domáci test je iba orientačný, pretože nekontroluje kondicionovanie, rozmery ani meranie obrysu.",
                    ],
                },
            },
            {
                "heading": "Prečo názov vlákna nestačí",
                "paragraphs": [
                    "Viskóza sa často spája so splývavým vzhľadom, no nie každá viskózová látka padá rovnako. Rozdiel vytvorí jemnosť priadze, hustota, tkanie alebo pletenie, zmes, gramáž a dokončenie. Rovnaké platí pre polyester, hodváb, bavlnu aj lyocell. Vlákno určuje časť možností, nie konečný tvar každého výrobku.",
                    "Pri materiálovom porovnaní má zmysel čítať články <a href=\"/n/co-je-viskoza-vlastnosti-krcivost-zrazanie-a-starostlivost\">čo je viskóza</a> a <a href=\"/n/co-je-lyocell-alebo-tencel-priedusnost-jemnost-a-starostlivost\">čo je lyocell alebo Tencel</a>, no rozhodnutie urobte podľa konkrétnej látky. V obchode sledujte pohyb celej plochy, nie iba zloženie na malej etikete.",
                ],
            },
            {
                "heading": "Osnova, útok a šikmý smer pri strihu",
                "paragraphs": [
                    "Tkanina môže mať inú ohybnosť v osnove a inú v útku. Šikmý smer často umožní väčšie geometrické prispôsobenie, pretože priadze sa voči sebe natáčajú. Strih po šikmom smere môže vytvoriť elegantné kopírovanie tela, ale zároveň zvýšiť riziko vytiahnutia alebo nerovného spodného okraja.",
                    "Pri šití je dôležité nechať niektoré šikmo strihané diely pred konečným zarovnaním ustáliť a zvoliť šev, ktorý nebude látku násilne brzdiť. Pri hotovom odeve doma tento proces nevrátite. Môžete však zabrániť bodovému ťahu: mokré šaty nevešajte za úzke ramienka, ak to štítok alebo konštrukcia nezvláda.",
                ],
            },
            {
                "heading": "Ako vlhkosť mení pád textilu",
                "paragraphs": [
                    "Mokrý výrobok nesie hmotnosť prijatej vody. Niektoré vlákna napučia, mení sa trenie medzi priadzami a konštrukcia sa ľahšie preskupí. Látka preto môže za mokra padať hlbšie, lepiť sa na telo alebo sa dočasne javiť tuhšia. Tento stav nemožno automaticky považovať za konečný vzhľad po úplnom vysušení.",
                    "Najväčšie riziko vzniká, keď ťažký mokrý kus visí na malom bode. Rameno, šev alebo výstrih sa predĺži a po vyschnutí zostane nerovný. Pri viskóze a jemných úpletoch používajte nízke otáčky podľa štítku, vyberajte ich bez krútenia a sušte rozložené alebo s dostatočnou oporou.",
                ],
                "callout": {
                    "title": "Keď odev po praní zrazu visí inak",
                    "items": [
                        "Najprv ho nechajte úplne vyschnúť v správnom tvare; mokrý vzhľad nie je konečný výsledok.",
                        "Porovnajte dĺžku, šírku a rovnosť švov, aby ste odlíšili splývanie od zrazenia či vytiahnutia.",
                        "Skontrolujte, či látku nestužili zvyšky gélu, tvrdá voda alebo pomalé sušenie v záhyboch.",
                        "Nesnažte sa tvar napraviť vyšším teplom, pokiaľ ho výslovne nepovoľuje štítok.",
                    ],
                    "background": "#f7fbf8",
                    "border": "#dbe5de",
                },
            },
            {
                "heading": "Apretúra, zmäkčenie a prvé pranie",
                "paragraphs": [
                    "Nová látka môže obsahovať dokončovaciu úpravu, ktorá uľahčila výrobu, zlepšila hladkosť alebo nastavila požadovaný tvar. Po prvom praní sa časť úpravy odstráni a textil sa začne správať prirodzenejšie. Zmena nemusí znamenať chybu prania, ak výrobca s týmto vývojom počítal a rozmery zostali v poriadku.",
                    "Opačný problém vytvárajú zvyšky pracieho prostriedku, minerály z tvrdej vody alebo nadmerná vrstva aviváže. Látka môže byť vosková, lepkavá alebo papierovo tuhá a záhyby stratia pohyb. Riešením je upraviť dávku, náplň a oplach, nie pridávať stále viac prípravku. Pri funkčnom textile aviváž nepoužívajte bez súhlasu výrobcu.",
                ],
            },
            {
                "heading": "Pranie, žmýkanie a sušenie bez deformácie",
                "paragraphs": [
                    "Splývavý textil býva často jemný, ale nie je to univerzálne pravidlo. Začnite symbolmi, otočte odev naruby, zapnite prvky, ktoré by sa mohli zachytiť, a použite menšiu náplň s podobne ľahkými kusmi. Veľmi vysoké otáčky môžu vytlačiť ostré záhyby a namáhať švy; ručné krútenie zas deformuje mokrú plochu.",
                    "Po praní odev jemne pretraste iba vtedy, ak to konštrukcia znesie, vyrovnajte švy a vytvarujte ho bez násilného ťahania. Sušenie naplocho pomáha ťažkým úpletom, široký vešiak ľahkým stabilným blúzkam. Horúca sušička môže zmeniť rozmer, povrch aj pružné časti, preto ju použite len podľa štítku.",
                ],
            },
            {
                "heading": "Strih, podšívka a švy menia konečný vzhľad",
                "paragraphs": [
                    "Rovnaká metráž vytvorí inú siluetu v kruhovej sukni, rovnom tričku a saku s výstužou. Množstvo látky určuje počet záhybov, podšívka obmedzí pohyb vrchnej vrstvy a švy pridajú lokálnu tuhosť. Vrecká, zipsy a lemy zaťažia konkrétne miesta, preto hotový výrobok nemožno posudzovať iba podľa malej voľnej vzorky.",
                    "Pri obliekaní skontrolujte, či textil padá voľne alebo je napnutý cez ramená, boky a sed. Príliš tesný strih mení splývanie na ťah a môže viesť k posunu švov alebo trvalému vytiahnutiu. Príliš veľký kus zas nemusí pôsobiť elegantne, aj keď samotná látka vytvára krásne záhyby.",
                ],
                "callout": {
                    "title": "Výber látky podľa účelu",
                    "items": [
                        "<strong>Voľné šaty a blúzky:</strong> sledujte mäkký pohyb, priehľadnosť, návrat po pokrčení a správanie na tele.",
                        "<strong>Sako a tvarovaná sukňa:</strong> potrebujú primeranú oporu, čisté hrany a súhru s výstužou či podšívkou.",
                        "<strong>Záves:</strong> hodnotí sa pravidelnosť záhybov, hmotnosť, dĺžka a zmena po zavesení.",
                        "<strong>Posteľná bielizeň:</strong> dôležitý je komfort, priedušnosť, rozmerová stabilita a údržba, nie iba dramatický pád.",
                    ],
                },
            },
            {
                "heading": "Ako vyberať splývavú látku bez marketingových skratiek",
                "paragraphs": [
                    "Najlepšie je porovnávať materiál v podmienkach blízkych použitiu. Zdvihnite väčšiu plochu, nechajte ju voľne klesnúť, otočte smer a pozrite sa, či záhyby zostávajú hladké alebo sa látka láme. Pri odeve sa prejdite, sadnite si a zdvihnite ruky. Pohyb odhalí viac než fotografia na figuríne.",
                    "Pýtajte sa na zloženie, podšívku, zrážanie a pokyny pre pranie. Pri saténovom vzhľade odlíšte väzbu od vlákna pomocou sprievodcu <a href=\"/n/co-je-saten-nie-je-to-vzdy-hodvab-a-ako-ho-spravne-prat\">čo je satén</a>. Pri regenerovaných celulózových vláknach pomôže porovnanie <a href=\"/n/modal-vs-lyocell-vs-viskoza-ako-sa-lisia-pri-prani-a-noseni\">modal, lyocell a viskóza</a>. Potom rozhodujte podľa konkrétneho výrobku, nie predstavy o názve materiálu.",
                ],
            },
        ],
        "table2_heading": "Diagnostika zmien po praní a nosení",
        "table2_intro": "Zmena vzhľadu nemusí byť zmena splývavosti samotnej. Najprv odlíšte zvyšky, zrazenie, vytiahnutie, pokrčenie a chybu strihu; každý problém potrebuje iný zásah.",
        "table2_headers": ["Prejav", "Možná príčina", "Ako ho odlíšiť", "Rozumný postup"],
        "table2_rows": [
            ("Látka je papierovo tuhá", "Zvyšky produktu, tvrdá voda, presušenie alebo strata dočasnej úpravy.", "Skontrolovať povrch, pach, oplach a rovnakú zmenu na celej ploche.", "Upraviť dávku a náplň; pri ďalšom cykle zlepšiť oplach podľa štítku."),
            ("Šaty sú dlhšie a ramená vytiahnuté", "Bodové vešanie ťažkého mokrého odevu.", "Porovnať švy, ramená a smer deformácie.", "Sušiť s oporou a vytvarovať bez násilného sťahovania."),
            ("Záhyby sú ostré a nepravidelné", "Dlhé státie mokré v bubne, vysoké otáčky alebo stlačené sušenie.", "Pozrieť, či rozmery zostali rovnaké a zhyby kopírujú stlačenie.", "Vybrať skôr, vyhladiť a žehliť alebo naparovať iba podľa štítku."),
            ("Odev kopíruje telo viac než predtým", "Uvoľnenie úpravy, vytiahnutie alebo zmena podšívky.", "Zmerať rozmer a skontrolovať vrstvy aj švy.", "Pri trvalej deformácii konzultovať úpravu strihu; nepridávať teplo naslepo."),
            ("Jedna strana padá inak", "Smerový rozdiel, krivý šev, šikmé vytiahnutie alebo nerovné sušenie.", "Zavesiť na rovnej ploche a porovnať bočné švy.", "Nechať ustáliť; pri chybe konštrukcie vyhľadať krajčírsku opravu."),
        ],
        "steps": [
            "Určte účel: voľný pohyb šiat, čistý tvar saka, pravidelný záves alebo stabilná posteľná bielizeň.",
            "Prečítajte zloženie, konštrukciu, podšívku a symboly, no nerobte záver iba z názvu vlákna.",
            "Pozorujte väčšiu plochu vo viacerých smeroch a vyskúšajte výrobok v pohybe bez násilného naťahovania.",
            "Pred praním zmerajte citlivý kus, zatvorte zapínanie a perte ho s podobne ľahkými textíliami.",
            "Po cykle ho vyberte hneď, vyrovnajte švy a sušte so spôsobom opory, ktorý zodpovedá mokrej hmotnosti.",
            "Ak sa vzhľad zmení, odlíšte zrazenie, vytiahnutie, zvyšky a pokrčenie skôr, než zmeníte teplotu alebo pridáte prípravok.",
        ],
        "remember": [
            "Hodnotíte dotyk, ohybovú tuhosť alebo tvar celej voľnej plochy?",
            "Porovnávate rovnaký smer, veľkosť vzorky a podobný účel?",
            "Mení vzhľad podšívka, šev, lem, vrecko alebo príliš tesný strih?",
            "Je kus za mokra ťažký a potrebuje sušenie naplocho alebo širokú oporu?",
            "Povoľuje štítok zvolenú teplotu, otáčky, sušičku, žehlenie a použitie aviváže?",
        ],
        "mistakes": [
            "Zamieňať splývavosť s mäkkosťou na dotyk alebo s hladkým lesklým povrchom.",
            "Predpovedať pád látky iba podľa názvu vlákna, gramáže alebo ceny.",
            "Porovnať malú pevnú vzorku s celými šatami bez vplyvu strihu, švov a podšívky.",
            "Vešať ťažký mokrý úplet za úzke ramená a následné vytiahnutie pripísať zlému materiálu.",
            "Pridávať aviváž bez ohľadu na funkčnú úpravu, elastan, savosť a pokyny výrobcu.",
            "Naprávať zmenu tvaru vysokou teplotou bez rozlíšenia zrazenia, zvyškov a mechanickej deformácie.",
        ],
        "expert_heading": "Odbornejší pohľad: koeficient splývavosti a ohybová tuhosť",
        "expert": [
            "ISO 9073-9:2008 opisuje metódu na stanovenie splývavosti vrátane koeficientu splývavosti a podľa svojho rozsahu sa používa pri netkaných, tkaných aj pletených textíliách. Kontrolované podopretie a vyhodnotenie obrysu umožňujú porovnávať vzorky za rovnakých podmienok. Hodnota však nie je estetický rozsudok; hovorí o konkrétnej reakcii vzorky.",
            "ASTM D1388-23 sa venuje tuhosti látok, meria dĺžku ohybu a počíta ohybovú tuhosť. Uvádza metódu konzoly, pri ktorej sa pás ohýba vlastnou hmotnosťou, aj metódu zavesenej slučky. Norma zároveň upozorňuje na obmedzenia podľa typu látky, zvlnenie okrajov a skladovanie. To vysvetľuje, prečo treba porovnávať rovnakú metódu, smer a stav.",
            "Splývavosť hotového odevu zahŕňa ešte viac premenných: vzorové diely, šikmý smer, švy, podšívku, závažia lemov, veľkosť a pohyb tela. Laboratórna skúška plochy je dôležitá materiálová informácia, no nie úplná simulácia konkrétnych šiat. Pri domácej starostlivosti je cieľom zachovať rozmer a povrch, nie meniť technickú vlastnosť neovereným prípravkom.",
        ],
        "source_intro": "Technické zdroje oddeľujú celkové splývanie od smerovej ohybovej tuhosti. Symboly ošetrovania určujú hranice, v ktorých možno konkrétny odev prať, sušiť a žehliť bez zbytočnej deformácie.",
        "sources": [
            ("ISO 9073-9:2008: splývavosť a koeficient splývavosti", ISO_DRAPE),
            ("ASTM D1388-23: dĺžka ohybu a ohybová tuhosť látok", ASTM_STIFFNESS),
            ("GINETEX: význam symbolov ošetrovania textilu", GINETEX),
        ],
        "product_intro": "Dobre opláchnutá látka bez nadbytočných zvyškov si zachová prirodzenejší dotyk a pohyb. Prací prostriedok však vyberajte podľa najcitlivejšej zložky, úpravy a symbolov konkrétneho odevu.",
        "product_text": "Prací gél z marseillského mydla je určený na bežnú domácu bielizeň. Pri splývavých blúzkach a šatách použite presnú dávku, menšiu náplň a program povolený štítkom; mokrý kus potom sušte bez bodového ťahu.",
        "product_limit": "Gél nevytvorí splývavosť, neopraví vytiahnutý strih a nenahrádza osobitný prostriedok pre vlnu, hodváb, membránu alebo inú citlivú úpravu. Pri takých kusoch rozhodujú pokyny výrobcu.",
        "category_intro": "Gél porovnávajte podľa materiálu, farby a účelu prania. Mäkší pocit nevzniká najvyššou dávkou; dôležitý je čistý cyklus, dostatok priestoru, oplach a šetrné sušenie.",
        "category_text": "V kategórii nájdete gély na bežnú bielizeň. Pred použitím skontrolujte etiketu produktu aj odevu a pri jemných zmesiach, výstuži, podšívke alebo funkčnej vrstve zvoľte najopatrnejší povolený postup.",
        "related": [
            ("Čo je viskóza a ako sa o ňu starať", ARTICLE_VISCOSE),
            ("Čo je lyocell alebo Tencel", ARTICLE_LYOCELL),
            ("Čo je satén a ako ho správne prať", ARTICLE_SATIN),
            ("Modal, lyocell a viskóza v porovnaní", ARTICLE_MODAL_COMPARE),
            ("Čo je ľan a prečo sa krčí", ARTICLE_LINEN),
            ("Čo znamená gramáž GSM", ARTICLE_GSM),
        ],
        "faq_title": "splývavosť a pád látky",
        "faq": [
            ("Čo je splývavosť textilu?", "Je to spôsob, akým sa plocha látky pod vlastnou hmotnosťou ohýba a vytvára priestorové záhyby. Súvisí s hmotnosťou, tuhosťou, konštrukciou, smerom aj úpravou."),
            ("Je splývavá látka vždy mäkká?", "Nie. Mäkkosť je dotykový vnem, kým splývanie hodnotí správanie celej plochy. Vlastnosti sa môžu prekrývať, ale nie sú totožné."),
            ("Ktorý materiál najlepšie splýva?", "Jedno vlákno nemožno vyhlásiť za víťaza. Viskóza, lyocell, hodváb aj syntetické látky môžu splývať, no rozhoduje konkrétna priadza, konštrukcia, gramáž a úprava."),
            ("Prečo šaty po praní visia inak?", "Môže ísť o mokrú hmotnosť, vytiahnutie, zrazenie, odstránenie dočasnej úpravy, zvyšky prostriedku alebo nerovné sušenie. Najprv ich nechajte správne vyschnúť a zmerajte."),
            ("Pomôže aviváž, aby látka lepšie padala?", "Niekedy zmení dotyk, ale nie je univerzálnym riešením a môže prekážať funkcii savých, pružných či upravených materiálov. Rozhoduje štítok a primerané dávkovanie."),
            ("Ako sušiť splývavé šaty?", "Podľa štítku a mokrej hmotnosti. Ťažký úplet alebo viskózu nenechávajte visieť na úzkych bodoch; použite rovnomernú oporu alebo sušenie naplocho, ak to výrobca odporúča."),
        ],
    },
    {
        "title": "Tepelný odpor textilu: prečo niektoré vrstvy hrejú viac pri rovnakej hrúbke",
        "link": "tepelny-odpor-textilu-preco-niektore-vrstvy-hreju-viac-pri-rovnakej-hrubke",
        "meta": "Ako textil obmedzuje únik tepla, čo znamená tepelný odpor Rct a prečo o pocite tepla rozhodujú vzduch, vlhkosť, vietor, vrstvy aj strih.",
        "short": "Textil teplo nevyrába; spomaľuje jeho prenos a pracuje so vzduchom medzi vláknami a vrstvami. Sprievodca odlišuje tepelný odpor, priedušnosť, vlhkosť a reálny komfort.",
        "answer": "Tepelný odpor vyjadruje, ako materiál alebo zostava bráni suchému toku tepla pri definovanom rozdiele teplôt. Hrejivosť preto neurčuje iba názov vlákna ani hrúbka na pohľad. Dôležité je množstvo stabilného vzduchu v rúne či medzi vrstvami, hustota, stlačenie, vietor, vlhkosť, strih a pohyb človeka. Dve rovnako hrubé vrstvy môžu mať inú štruktúru a inú schopnosť udržať vzduch. Laboratórny Rct je užitočný údaj, ale sám neurčuje pohodlie ani bezpečnosť v konkrétnom počasí.",
        "intro": "Tenký merino úplet môže pôsobiť príjemnejšie než ťažká hladká mikina, nadýchaný fleece hreje inak pod vetruodolnou bundou a páperová vrstva po stlačení stráca časť účinku. V týchto príkladoch nejde o jednoduchú súťaž vlákien. Odev tvorí systém, v ktorom sa vedie teplo materiálom a vzduchom, vzduch sa pohybuje, vlhkosť sa odparuje a vrstvy menia objem aj kontakt s telom.",
        "quick": [
            "<strong>Textil nie je zdroj tepla:</strong> spomaľuje prenos tepla vytvoreného telom alebo okolím.",
            "<strong>Vzduch v štruktúre je zásadný:</strong> nadýchaná vrstva funguje inak než rovnako hrubá, ale stlačená a hustá plocha.",
            "<strong>Rct a Ret sú rozdielne:</strong> tepelný odpor a odpor proti prechodu vodnej pary opisujú iné procesy.",
            "<strong>Vietor a vlhkosť menia realitu:</strong> otvorená izolačná vrstva môže bez vrchnej ochrany rýchlo strácať účinok.",
            "<strong>Jedna skúška nie je predpoveď komfortu:</strong> strih, pohyb, potenie, vrstvenie a počasie rozhodujú o hotovom odeve.",
        ],
        "overview_heading": "Ako textil spomaľuje únik tepla",
        "overview": [
            "Teplo sa medzi telom a okolím prenáša viacerými cestami. V samotnom textile sa uplatňuje vedenie cez vlákna a zachytený vzduch, na povrchu prúdenie a sálanie a pri potení aj odparovanie. Materiálový tepelný odpor sa snaží jednu časť tohto zložitého systému zmerať za ustálených kontrolovaných podmienok.",
            "Vzduch vedie teplo pomerne slabo, pokiaľ zostáva v malých stabilných priestoroch a voľne necirkuluje. Preto rúno, česaný povrch, výplň a viac vrstiev môžu vytvoriť izolačnú medzeru. Keď sa štruktúra stlačí pod popruhom, lakťom alebo tesnou vrchnou vrstvou, množstvo zachyteného vzduchu sa zmenší a s ním môže klesnúť aj tepelný účinok.",
            "Pocit tepla však nie je len tepelný odpor. Studený dotyk hladkej vlhkej látky, prievan cez otvorený úplet, odparovanie potu a studené kovanie môžu vytvoriť iný vnem, než naznačuje údaj materiálu. Preto sa pri výbere pozerajte na celý odev a spôsob vrstvenia, nie na jedno marketingové slovo.",
        ],
        "table1_heading": "Tepelný odpor, para, vietor a pocit: štyri rôzne otázky",
        "table1_intro": "Nasledujúce vlastnosti spolu súvisia, ale nie sú zameniteľné. Vysoký výsledok v jednej oblasti môže byť vhodný pre určitú situáciu a nevhodný pre inú.",
        "table1_headers": ["Vlastnosť", "Čo opisuje", "Praktický význam", "Čo z nej nemožno určiť"],
        "table1_rows": [
            ("Tepelný odpor Rct", "Odpor materiálu alebo zostavy proti suchému toku tepla pri definovaných podmienkach.", "Vyšší odpor znamená menší tok tepla pri rovnakom rozdiele teplôt v skúške.", "Kompletný komfort pri pohybe, vetre a premenlivej vlhkosti."),
            ("Odpor proti vodnej pare Ret", "Odpor pri prenose vodnej pary a súvisiacom odparovacom toku.", "Pomáha opisovať, ako zostava prepúšťa paru v skúške.", "Koľko kvapalného potu odev odvedie z pokožky pri každom použití."),
            ("Priepustnosť vzduchu", "Ako ľahko vzduch prechádza materiálom pri tlakovom rozdiele.", "Otvorený fleece môže izolovať v pokoji, ale prefukovať vo vetre.", "Tepelný odpor celej vrstvy bez znalosti hrúbky a zostavy."),
            ("Subjektívny tepelný komfort", "Vnem človeka ovplyvnený telom, aktivitou, vlhkosťou a prostredím.", "Rozhoduje o tom, či sa cítime príjemne, chladno alebo prehriato.", "Jednu univerzálnu hodnotu platnú pre všetkých ľudí a podmienky."),
            ("Hodnotenie celého odevu", "Zahŕňa strih, medzery, uzávery, vrstvy a plochu pokrytia.", "Odhaľuje vplyv goliera, rukávov, zipsu, kapucne a netesností.", "Samostatnú vlastnosť každej použitej látky bez ďalšieho testovania."),
        ],
        "sections": [
            {
                "heading": "Tepelný odpor Rct v jednoduchých slovách",
                "paragraphs": [
                    "Rct sa dá chápať ako pomer teplotného rozdielu medzi stranami materiálu k výslednému suchému tepelnému toku na jednotku plochy. Jednotka je štvorcový meter kelvin na watt. Vyššia hodnota v rovnakej metóde a podmienkach znamená, že cez vzorku prechádza pri danom rozdiele teplôt menej suchého tepla.",
                    "Dôležité sú slová rovnaká metóda a podmienky. Hrúbka, zostava, orientácia, kondicionovanie a prístroj ovplyvňujú výsledok. Údaj bez informácie, či ide o jednu látku, výplň, laminát alebo viac vrstiev, je ťažko porovnateľný. Rct tiež nie je teplotný limit odevu a nehovorí, pri koľkých stupňoch bude každému človeku teplo.",
                ],
            },
            {
                "heading": "Prečo rozhoduje zachytený vzduch",
                "paragraphs": [
                    "Vlákna vytvárajú priestorovú kostru a medzi nimi zostáva vzduch. Ak sú priestory malé a vzduch sa v nich výrazne nepohybuje, obmedzuje sa konvekčný prenos. Nadýchané rúno alebo výplň preto môže poskytnúť vysoký odpor pri relatívne malej hmotnosti. Nie je to však vlastnosť vzduchu oddelená od konštrukcie; kostra ho musí udržať.",
                    "Príliš otvorená štruktúra prepúšťa prúdenie a vo vetre stráca výhodu. Príliš hustá pevná plocha môže mať menej vzduchových priestorov alebo viac tepelných mostov cez materiál. Dizajn izolačnej vrstvy preto hľadá rovnováhu medzi loftom, stabilitou, hmotnosťou, priedušnosťou a ochranou pred prúdením.",
                ],
            },
            {
                "heading": "Hrúbka, loft a stlačenie nie sú rovnaké",
                "paragraphs": [
                    "Hrúbka je geometrický rozmer pri určenom tlaku. Loft opisuje nadýchaný objem, ktorý vytvára izolačný priestor, no v bežnej reči sa používa nepresne. Dve vrstvy s rovnakou zmeranou hrúbkou môžu mať inú hustotu a mikroštruktúru; jedna drží vzduch v jemných priestoroch, druhá je ťažká a kompaktnejšia.",
                    "Stlačenie mení hrúbku aj množstvo vzduchu. Tesný opasok, ramenný popruh, sedenie na izolačnej vrstve alebo príliš malá vrchná bunda môžu vytvoriť chladnejšie zóny. Pri skladovaní nadýchanej výplne dlhodobé silné stlačenie tiež nemusí byť vhodné; riaďte sa výrobcom konkrétneho výrobku.",
                ],
                "callout": {
                    "title": "Prečo rovnako hrubé vrstvy nehřejú rovnako",
                    "items": [
                        "Majú inú hustotu, priadzu, veľkosť a prepojenie vzduchových priestorov.",
                        "Jedna sa pod oblečením stlačí viac alebo prepúšťa viac pohybujúceho sa vzduchu.",
                        "Rozdielna vlhkosť zmení vodivosť, hmotnosť aj odparovanie.",
                        "Pocit ovplyvní povrch pri pokožke, strih, medzery a aktivita, nielen laboratórny odpor.",
                    ],
                },
            },
            {
                "heading": "Vlákno je iba začiatok, konštrukcia robí vrstvu",
                "paragraphs": [
                    "Vlna, polyester, polyamid, bavlna aj regenerované vlákna majú rozdielne chemické a fyzikálne vlastnosti, no tepelný výsledok textilu vzniká až po spracovaní do priadze a plochy. Jemný merino úplet, hrubý vlnený sveter a zlisovaná plsť z podobného vlákna nemajú rovnakú štruktúru ani použitie.",
                    "Syntetické vlákna možno vytvoriť ako veľmi jemné, duté, zvlnené alebo objemné a prírodné vlákna majú vlastnú morfológiu. Zloženie pomáha vysvetliť savosť, schnutie a údržbu, ale názov sám neudáva Rct. Pri výbere športovej vrstvy pomáha článok <a href=\"/n/polyester-polyamid-merino-alebo-elastan-z-coho-ma-byt-sportove-oblecenie\">z čoho má byť športové oblečenie</a>, ktorý porovnáva materiály v širšom kontexte.",
                ],
            },
            {
                "heading": "Ako funguje vrstvenie a vzduchové medzery",
                "paragraphs": [
                    "Viac vrstiev pridáva materiálové odpory aj medzery medzi nimi. Základná vrstva rieši kontakt s pokožkou a vlhkosť, stredná vytvára izolačný objem a vrchná môže obmedziť vietor alebo zrážky. Systém funguje iba vtedy, keď vrstvy nie sú zbytočne tesné a vlhkosť má cestu smerom von.",
                    "Dve voľné tenšie vrstvy môžu v niektorých situáciách poskytnúť viac flexibility než jedna hrubá. Pri aktivite jednu odložíte, pri zastavení pridáte. Nie je však pravda, že každý ďalší kus vždy pomôže: tesná vrstva stlačí izoláciu, nepriedušná zostava zadrží vlhkosť a príliš voľné otvory umožnia výmenu teplého vzduchu.",
                ],
            },
            {
                "heading": "Vietor, strih a netesnosti v odeve",
                "paragraphs": [
                    "Materiál meraný bez prúdenia môže v otvorenom priestore fungovať inak. Vietor odnáša teplú hraničnú vrstvu pri povrchu a preniká otvorenou konštrukciou. Fleece preto môže byť príjemne izolačný pod vetruodolnou bundou a nedostatočný ako jediná vrstva na hrebeni. Viac o jeho konštrukcii vysvetľuje článok <a href=\"/n/co-je-fleece-hrejivost-zmolkovanie-a-starostlivost-pri-prani\">čo je fleece</a>.",
                    "Hotový odev stráca teplo aj cez golier, manžety, spodný lem, zipsy a nedostatočne zakryté časti tela. Príliš voľný strih vymieňa veľa vzduchu, príliš tesný stláča izoláciu. Kapucňa, vysoký golier a nastaviteľné otvory preto môžu mať v praxi väčší vplyv než malý rozdiel medzi dvoma samotnými látkami.",
                ],
                "callout": {
                    "title": "Rýchla diagnostika, keď je vám v oblečení zima",
                    "items": [
                        "Cítite prúdenie cez materiál alebo skôr pri golieri, zipse, rukávoch a spodnom leme?",
                        "Je stredná vrstva nadýchaná, alebo ju tesná bunda a popruhy úplne stlačili?",
                        "Je základná vrstva mokrá od potu a pokračuje odparovanie počas zastavenia?",
                        "Zodpovedá zostava aktivite, vetru, zrážkam a dĺžke pobytu, alebo sa spolieha na jedno označenie materiálu?",
                    ],
                    "background": "#f7fbf8",
                    "border": "#dbe5de",
                },
            },
            {
                "heading": "Vlhkosť, pot a odparovanie menia tepelný pocit",
                "paragraphs": [
                    "Voda má iné tepelné vlastnosti než suchý vzduch a môže obsadiť priestory v textile. Mokrá vrstva pri pokožke býva nepríjemne chladná najmä v pokoji a pri vetre. Súčasne odparovanie spotrebúva teplo, čo pri aktivite pomáha ochladzovať, ale po zastavení môže zvyšovať pocit chladu.",
                    "Odpor proti prechodu vodnej pary Ret nie je opačná hodnota Rct ani synonymum priedušnosti v každom význame. ISO 11092 meria obe veličiny na príbuznom zariadení, ale ide o samostatné procesy. Praktické rozdiely medzi savosťou, priedušnosťou a schnutím rozoberá článok <a href=\"/n/priedusnost-savost-a-rychloschnutie-ako-citat-vlastnosti-textilu\">ako čítať vlastnosti textilu</a>.",
                ],
            },
            {
                "heading": "Čo môže pranie urobiť s izolačnou štruktúrou",
                "paragraphs": [
                    "Pranie odstraňuje maz, pot a nečistoty, ktoré môžu zlepiť vlákna a zhoršiť hygienu, no zároveň mechanicky zaťažuje rúno, výplň a povrch. Nevhodný program môže splstiť vlnu, zraziť odev, poškodiť lepenú vrstvu alebo zmeniť loft. Preto nie je správne prať každý hrejivý kus ako hrubú bavlnenú mikinu.",
                    "Pri merine a vlne použite iba postup povolený štítkom a vhodný prostriedok; všeobecný gél nemusí byť určený pre tieto vlákna. Návod <a href=\"/n/co-je-merino-vlna-vyhody-nevyhody-a-pranie-bez-zrazenia\">čo je merino vlna</a> vysvetľuje riziko tepla a mechaniky. Pri syntetickom rúne zatvorte zipsy, perte naruby a vyhnite sa zbytočne drsnému cyklu, ktorý podporuje povrchové opotrebovanie.",
                ],
            },
            {
                "heading": "Zvyšky prostriedku, sušenie a obnova loftu",
                "paragraphs": [
                    "Nadmerná dávka a preplnený bubon môžu zhoršiť oplach. Zvyšky spájajú jemné vlákna, menia dotyk a zadržiavajú nečistoty. To neznamená, že každý tuhší kus stratil tepelný odpor, ale je to signál na kontrolu dávkovania a náplne. Opakované pranie len na odstránenie prebytku zvyšuje ďalšiu mechanickú záťaž.",
                    "Sušte presne podľa štítku. Niektoré výplne potrebujú špecifický postup na rovnomerné preschnutie a obnovenie objemu, iné neznesú bubnovú sušičku. Hrubý izolačný kus musí byť suchý aj vnútri, pretože vlhká výplň môže vytvárať studené zóny a zatuchnúť. Neskladujte ho, kým si nie ste istí úplným vysušením.",
                ],
            },
            {
                "heading": "Ako vybrať hrejivú vrstvu podľa reálneho použitia",
                "paragraphs": [
                    "Na pokojné státie v chlade potrebujete viac izolácie než na rýchly výstup. Pri vetre sa zvyšuje význam vrchnej ochrany, pri daždi vodoodolnosti a pri intenzívnej aktivite riadenia vlhkosti. Hmotnosť, zbaliteľnosť, odolnosť, hlučnosť, schnutie a opraviteľnosť môžu byť rovnako dôležité ako samotný tepelný odpor.",
                    "Pýtajte sa, či údaj patrí látke, viacvrstvovej zostave alebo hotovému odevu a podľa akej metódy vznikol. Skontrolujte strih v pohybe aj s plánovanými vrstvami. Pri ponožkách, rukaviciach a obuvi nechajte dostatok priestoru na cirkuláciu a pohyb; príliš tesný výrobok môže zhoršiť tepelný pocit aj komfort bez ohľadu na názov izolácie.",
                ],
                "callout": {
                    "title": "Kedy jedno číslo nestačí",
                    "items": [
                        "Pri bezpečnosti v chlade potrebujete zohľadniť počasie, aktivitu, trvanie, rezervnú vrstvu a individuálnu citlivosť.",
                        "Materiálový výsledok neodhaľuje všetky netesnosti, strih a stlačenie v hotovom odeve.",
                        "Hrejivá, ale vlhkosť zadržiavajúca zostava môže byť pri námahe menej pohodlná než vyvážené vrstvenie.",
                        "Na pracovné a ochranné použitie vyberajte certifikovaný výrobok podľa príslušných požiadaviek, nie iba podľa tohto všeobecného sprievodcu.",
                    ],
                },
            },
        ],
        "table2_heading": "Prejavy po nosení alebo praní a ich možné príčiny",
        "table2_intro": "Tepelný pocit môže zmeniť stav textilu aj spôsob vrstvenia. Pred záverom, že materiál prestal hriať, skontrolujte objem, vlhkosť, strih, prúdenie a rovnomernosť výplne.",
        "table2_headers": ["Prejav", "Možná príčina", "Čo skontrolovať", "Ďalší krok"],
        "table2_rows": [
            ("Vrstva je plochá a miestami tenká", "Stlačenie, zhluknutie alebo posun výplne.", "Komory, švy, pokyny výrobcu a stav po úplnom vysušení.", "Obnoviť iba povoleným postupom; poškodenú komoru opraviť."),
            ("Odev hreje v bezvetrí, ale prefukuje", "Otvorená konštrukcia a rýchla výmena vzduchu.", "Priedušnosť, otvory a chýbajúcu vrchnú vrstvu.", "Pridať vhodnú vetruodolnú ochranu bez zbytočného stlačenia."),
            ("Po zastavení je vnútro studené a mokré", "Pot, kondenzácia, slabý odvod pary alebo nevhodné vrstvenie.", "Základnú vrstvu, intenzitu pohybu a vetranie počas aktivity.", "Upraviť vrstvy skôr, než sa úplne prepotia; mokré vymeniť."),
            ("Povrch je tuhý a zle sa nadvihuje", "Zvyšky produktu, nečistoty, splstnatenie alebo tepelná zmena.", "Dávku, oplach, zloženie a symboly.", "Neskúšať agresívnu obnovu; pri ďalšom cykle opraviť príčinu."),
            ("Chladné pásy pod popruhmi", "Lokálne stlačenie izolačnej vrstvy.", "Nastavenie batoha a hrúbku zostavy pod tlakom.", "Zmeniť rozloženie tlaku alebo pridať vhodnú vrstvu inde."),
        ],
        "steps": [
            "Určte podmienky: teplota, vietor, vlhkosť, zrážky, aktivita a čas mimo tepla.",
            "Rozdeľte úlohy vrstiev na kontakt s pokožkou, izoláciu a ochranu pred vetrom alebo vodou.",
            "Vyskúšajte kompletnú zostavu v pohybe a skontrolujte, či vrchná vrstva nestláča izoláciu.",
            "Po nosení vetrajte a perte až podľa znečistenia a štítku; každý cyklus prispôsobte konkrétnej výplni alebo vláknu.",
            "Dávkujte presne, dôkladne opláchnite a úplne vysušte aj vnútorné komory bez nepovoleného tepla.",
            "Pred náročným použitím skontrolujte rovnomernosť, švy, zipsy a záložný plán; poškodenú ochrannú výbavu nahraďte alebo odborne opravte.",
        ],
        "remember": [
            "Patrí údaj samotnej látke, viacvrstvovej vzorke alebo hotovému odevu?",
            "Bola použitá aktuálna a uvedená skúšobná metóda s porovnateľnými podmienkami?",
            "Zostáva medzi vrstvami priestor, alebo je izolácia stlačená strihom a popruhmi?",
            "Ako zostava pracuje s vetrom, kvapalnou vodou, parou a potením pri plánovanej aktivite?",
            "Povoľuje štítok zvolený prostriedok, program, sušičku a spôsob obnovenia objemu?",
        ],
        "mistakes": [
            "Tvrdiť, že jedno vlákno vždy hreje viac bez znalosti konštrukcie, hrúbky a podmienok.",
            "Zamieňať tepelný odpor s odporom proti vodnej pare, vetruodolnosťou alebo osobným komfortom.",
            "Porovnať hodnotu samotnej látky s údajom celého odevu bez uvedenia metódy.",
            "Obliecť príliš tesné vrstvy, stlačiť loft a očakávať rovnakú izoláciu ako vo voľnom stave.",
            "Prať vlnu, výplň alebo laminát univerzálnym postupom iba preto, že výrobok pôsobí odolne.",
            "Skladovať hrubý kus mierne vlhký alebo dlhodobo silno stlačený napriek opačným pokynom výrobcu.",
        ],
        "expert_heading": "Odbornejší pohľad: čo meria horúca platňa a čo nie",
        "expert": [
            "ISO 11092:2026 je aktuálne tretie vydanie normy pre meranie tepelného odporu a odporu proti vodnej pare za ustálených podmienok na potivej chránenej horúcej platni. Rozsah zahŕňa látky, filmy, povlaky, peny, kožu aj viacvrstvové zostavy pre odevy, prikrývky, spacie vaky či čalúnenie. Norma výslovne uvádza, že skúšobné podmienky nereprezentujú konkrétne situácie komfortu a neurčuje výkonové požiadavky na fyziologické pohodlie.",
            "ASTM F1868-23 podobne meria tepelný a odparovací odpor a celkovú stratu tepla materiálov odevných systémov na potivej horúcej platni. V časti o význame upozorňuje, že prostredie výsledky výrazne ovplyvňuje a materiálové hodnoty nemajú dokázanú priamu koreláciu s výkonom kompletného oblečenia na človeku. Hmotnosť, splývanie, tesnosť strihu a ďalšie faktory môžu rozdiely zmenšiť alebo neutralizovať.",
            "Technicky správny záver preto znie užšie: Rct pomáha porovnať suchý tok tepla cez definovanú vzorku alebo zostavu. Pre rozhodnutie v praxi treba doplniť odparovací odpor, prúdenie vzduchu, vlhkosť, stlačenie, strih, veľkosť pokrytej plochy a aktivitu. Pri zdravotnom alebo pracovnom riziku chladu sa riaďte odbornými odporúčaniami a certifikáciou výrobku.",
        ],
        "source_intro": "Aktuálna ISO norma z roku 2026 oddeľuje tepelný odpor od odporu proti vodnej pare a jasne obmedzuje prenášanie skúšky na konkrétny komfort. ASTM dopĺňa hranice pri interpretácii materiálu ako hotového odevu.",
        "sources": [
            ("ISO 11092:2026: tepelný a parný odpor na potivej horúcej platni", ISO_THERMAL),
            ("ASTM F1868-23: tepelný a odparovací odpor odevných materiálov", ASTM_THERMAL),
            ("GINETEX: význam symbolov ošetrovania textilu", GINETEX),
        ],
        "product_intro": "Čistá, dobre opláchnutá a úplne vysušená vrstva si lepšie zachováva pôvodný povrch a objem. Každá výplň, vlna, laminát a technická úprava však môže mať osobitné požiadavky.",
        "product_text": "Prací gél z marseillského mydla je určený na bežnú domácu bielizeň. Použite ho len tam, kde je zlučiteľný so štítkom; dávku prispôsobte vode a náplni a hrubý kus dôkladne opláchnite aj vysušte.",
        "product_limit": "Tento gél nie je univerzálny prostriedok na vlnu, perie, membrány ani všetky technické výplne a nezvyšuje tepelný odpor. Pri špeciálnom výrobku použite iba postup a prostriedok odporúčaný výrobcom.",
        "category_intro": "Pri bežnej bielizni porovnávajte gély podľa materiálu, farby a spôsobu prania. Pri izolačných vrstvách má prednosť kompatibilita s výplňou, zachovanie povrchu a úplné vysušenie.",
        "category_text": "V kategórii nájdete pracie gély pre bežné textílie. Pred výberom si prečítajte obe etikety a pri merine, vlne, páperí, membráne alebo ochrannej vrstve siahnite po osobitnom prostriedku, ak ho výrobca vyžaduje.",
        "related": [
            ("Čo je fleece a ako ho prať", ARTICLE_FLEECE),
            ("Čo je merino vlna", ARTICLE_MERINO),
            ("Materiály pre športové oblečenie", ARTICLE_SPORT_MATERIALS),
            ("Priedušnosť, savosť a rýchloschnutie", ARTICLE_BREATHABILITY),
            ("Vlna a polyamid v zmesi", ARTICLE_WOOL_POLYAMIDE),
            ("Čo znamená gramáž GSM", ARTICLE_GSM),
        ],
        "faq_title": "tepelný odpor a hrejivosť textilu",
        "faq": [
            ("Čo je tepelný odpor textilu?", "Je to odpor materiálu alebo zostavy proti suchému toku tepla pri definovaných podmienkach. Pri textile sa často označuje Rct a vyjadruje v m²·K/W."),
            ("Znamená vyšší Rct vždy teplejší odev?", "V rovnakej skúške znamená vyšší odpor menší suchý tok tepla, no hotový odev ovplyvňuje strih, stlačenie, vietor, vlhkosť, vrstvenie a pohyb."),
            ("Prečo fleece hreje, ale prefukuje?", "Jeho objemná štruktúra zachytáva vzduch, no otvorená konštrukcia môže prepúšťať prúdenie. Vo vetre často potrebuje vhodnú vrchnú ochranu."),
            ("Je hrubšia mikina vždy teplejšia?", "Nie. Dôležitá je hustota, stabilný vzduch, stlačenie, vlhkosť a prúdenie. Rovnaká hrúbka môže ukrývať odlišnú štruktúru."),
            ("Prečo je mokré oblečenie studené?", "Voda mení prenos tepla a vypĺňa vzduchové priestory; odparovanie navyše odoberá teplo. Účinok závisí od materiálu, vetra, aktivity a vrstiev."),
            ("Môže nesprávne pranie znížiť hrejivosť?", "Môže zmeniť loft, rozmer, splstiť vlnu, zlepiť jemné vlákna zvyškami alebo poškodiť výplň. Vždy sa riaďte štítkom konkrétneho výrobku."),
        ],
    },
    {
        "title": "Ochrana textilu pred UV žiarením: čo znamená UPF a čo ju znižuje",
        "link": "ochrana-textilu-pred-uv-ziarenim-co-znamena-upf-a-co-ju-znizuje",
        "meta": "Čo znamená UPF pri oblečení, ako sa meria prienik UV žiarenia a prečo ochranu ovplyvňuje štruktúra, natiahnutie, vlhkosť, opotrebovanie aj strih.",
        "short": "UPF je skúšaná vlastnosť látky alebo odevu, nie odhad podľa farby či hrúbky. Sprievodca vysvetľuje meranie, vplyv natiahnutia, vlhkosti, opotrebovania a bezpečné používanie.",
        "answer": "UPF vyjadruje, o koľko textil pri skúške znižuje erytémovo vážené ultrafialové žiarenie, ktoré cez neho prechádza. Vyššie číslo znamená menší podiel prechádzajúceho UV, ale platí pre skúšaný materiál a podmienky. Ochranu môže znížiť otvorená konštrukcia, silné natiahnutie, opotrebovanie, diery a pri niektorých látkach aj zvlhnutie. Spoľahlivý údaj preto poskytne skúška a označenie podľa konkrétneho systému, nie domáci pohľad proti svetlu. Oblečenie chráni iba zakryté miesta a dopĺňa, nie nahrádza tieň, klobúk, okuliare a ochranu odkrytej pokožky.",
        "intro": "Biele tričko, tmavé plavky a športová mikina môžu vyzerať nepriehľadne, no viditeľné svetlo a ultrafialové žiarenie nie sú to isté. Výrobok, ktorý má poskytovať deklarovanú ochranu, sa preto hodnotí meraním spektrálneho prenosu a podľa pravidiel príslušného trhu. Spotrebiteľ zároveň potrebuje vedieť, že označenie látky nerieši krátke rukávy, medzery pri krku ani odhalenú tvár a ruky.",
        "quick": [
            "<strong>UPF patrí textilu:</strong> SPF sa používa pri opaľovacích prípravkoch, UPF pri látkach a oblečení.",
            "<strong>Skúška je dôležitejšia než odhad:</strong> farba, vlákno a hustota pomáhajú vysvetliť výsledok, ale samy nepotvrdia konkrétne číslo.",
            "<strong>Natiahnutie otvára štruktúru:</strong> tesný úplet môže prepustiť viac žiarenia než rovnaký materiál v uvoľnenom stave.",
            "<strong>Vlhkosť a opotrebovanie treba zohľadniť:</strong> účinok závisí od konkrétnej látky a spôsobu skúšania, nie od jedného všeobecného pravidla.",
            "<strong>Pokrytie rozhoduje:</strong> aj výborná látka chráni iba kožu, ktorú skutočne zakrýva a ktorá nezostáva odkrytá pri pohybe.",
        ],
        "overview_heading": "Čo UPF hovorí a čo nehovorí",
        "overview": [
            "Ultrafialové žiarenie, ktoré dopadá na zemský povrch, zahŕňa najmä UVA a menšiu časť UVB. Pri hodnotení ochrany textilu sa meria, koľko žiarenia pri rôznych vlnových dĺžkach prejde vzorkou, a výsledok sa váži podľa biologického účinku súvisiaceho so začervenaním kože. UPF je pomer expozície bez textilu k expozícii pod skúšaným materiálom podľa danej metódy.",
            "Matematicky preto UPF 50 zodpovedá približne jednej päťdesiatine váženého UV, teda asi dvom percentám, ktoré prejdú v skúšobných podmienkach. Tento prepočet nevytvára časový prísľub bezpečného pobytu na slnku. Intenzita UV, fototyp, lieky, nadmorská výška, odraz, čas a veľkosť odkrytej kože zostávajú dôležité.",
            "Označenie sa môže riadiť rozdielnymi normami a klasifikačnými pravidlami podľa krajiny. Preto porovnávajte výrobky s jasným systémom, skúšobnou metódou, hodnotou a návodom. Neprenášajte hranice jednej schémy na inú bez overenia. Ak výrobca uvádza iba neurčité slovné spojenie bez hodnoty a podmienok, nepovažujte ho automaticky za certifikovanú ochranu.",
        ],
        "table1_heading": "Faktory, ktoré menia prienik UV cez textil",
        "table1_intro": "Vplyv nie je pri každej látke rovnako veľký ani rovnakým smerom. Tabuľka vysvetľuje mechanizmus, ale konkrétnu hodnotu UPF môže potvrdiť iba príslušná skúška.",
        "table1_headers": ["Faktor", "Ako môže pôsobiť", "Praktická situácia", "Čo z neho nemožno vyvodiť"],
        "table1_rows": [
            ("Hustota a póry", "Menšie a menej početné otvory zvyčajne obmedzujú priamy prienik žiarenia.", "Riedky úplet presvitá medzi očkami viac než kompaktná plocha.", "Presnú hodnotu UPF iba z pohľadu proti lampe."),
            ("Natiahnutie", "Zväčšuje očká alebo vzdialenosť medzi priadzami a zmenšuje množstvo materiálu na ploche.", "Tesné plavky alebo legíny sú na bokoch a kolenách viac roztiahnuté.", "Že každá elastická látka má nízku ochranu v uvoľnenom stave."),
            ("Vlhkosť", "Mení optické vlastnosti, geometriu, priliehanie a prenos podľa konkrétnej látky.", "Mokré tričko sa prilepí na kožu a môže sa natiahnuť.", "Jedno univerzálne percento poklesu pre všetky vlákna a konštrukcie."),
            ("Farbivo a úprava", "Niektoré farbivá, pigmenty a absorbéry pohlcujú časť UV.", "Dve rovnako tkané farby môžu mať rozdielny výsledok.", "Že každá tmavá látka automaticky spĺňa deklarovanú úroveň."),
            ("Opotrebovanie", "Rednutie, vyblednutie, oder a dierky menia materiál aj otvory.", "Ošúchané ramená a vytiahnuté kolená už nie sú rovnakou vzorkou ako nový odev.", "Zostávajúcu ochranu bez opätovného testovania."),
        ],
        "sections": [
            {
                "heading": "UV žiarenie a úloha oblečenia",
                "paragraphs": [
                    "Svetová zdravotnícka organizácia uvádza, že nadmerná expozícia UV súvisí s poškodením kože a očí a odporúča kombináciu ochranných opatrení. Oblečenie vytvára fyzickú bariéru a pri dobrom pokrytí je praktickou súčasťou ochrany. Funguje priebežne bez potreby nanášania na zakrytú plochu, no musí zostať na mieste a v stave, pre ktorý je určené.",
                    "Žiadny odev však nepokrýva celé telo. Krátke rukávy nechávajú predlaktia, výstrih hrudník a pohyb môže odhaliť pás alebo členky. Pri vysokej hodnote UV indexu sa riaďte aktuálnymi zdravotnými odporúčaniami: vyhľadávajte tieň, obmedzte pobyt v najsilnejšom slnku a chráňte odkrytú pokožku, hlavu a oči vhodnými prostriedkami.",
                ],
            },
            {
                "heading": "Rozdiel medzi UPF a SPF",
                "paragraphs": [
                    "UPF je určený pre textil a vyjadruje útlm UVA aj UVB v rámci používanej metódy. SPF je označenie ochrany opaľovacích prípravkov a vychádza z iného typu hodnotenia. Rovnaké číslo preto neznamená, že tričko a krém sú zameniteľné alebo že možno spočítať ich hodnoty do jedného výsledku.",
                    "Na zakrytej ploche má správne nosený odev výhodu stálej fyzickej vrstvy. Na odkryté miesta však patrí širokospektrálny prípravok podľa zdravotných odporúčaní a treba ho používať v dostatočnom množstve a obnovovať. Tento článok rieši textilnú vlastnosť, nie výber liečby ani individuálne dermatologické riziko.",
                ],
            },
            {
                "heading": "Ako sa prenos UV cez látku meria",
                "paragraphs": [
                    "AATCC TM183 opisuje meranie UV žiarenia preneseného cez vzorku spektrofotometrom alebo spektrorádiometrom v známych intervaloch vlnových dĺžok. Z údajov sa počíta erytémovo vážený výsledok a podiel blokovania UVA a UVB. Metóda umožňuje skúšať materiál v suchom aj mokrom stave podľa určeného postupu.",
                    "Prístrojové meranie je potrebné preto, že oko vníma viditeľné svetlo, nie celý relevantný UV rozsah ani biologické váženie. Priloženie látky k oknu môže odhaliť veľké póry alebo mechanické poškodenie, ale nepotvrdí UPF. Rovnako aplikácia v telefóne bez kalibrovaného merania nemôže nahradiť akreditovanú skúšku.",
                ],
                "callout": {
                    "title": "Čo hľadať na označení výrobku",
                    "items": [
                        "Konkrétnu hodnotu alebo triedu UPF, nie iba všeobecné tvrdenie o ochrane pred slnkom.",
                        "Uvedený skúšobný alebo klasifikačný systém a návod na používanie či starostlivosť.",
                        "Informáciu, či sa tvrdenie týka metráže, jednej časti alebo celého odevu s viacerými materiálmi.",
                        "Veľkosť, ktorá nevyžaduje nadmerné natiahnutie, a strih, ktorý pri pohybe zakrýva zamýšľanú plochu.",
                    ],
                },
            },
            {
                "heading": "Štruktúra, hustota a otvory medzi priadzami",
                "paragraphs": [
                    "Časť žiarenia môže prechádzať priamo cez póry a časť interaguje s vláknami, farbivami a úpravami. Hustejšia tkanina alebo úplet s menšími otvormi preto často poskytuje lepší základ než veľmi otvorená konštrukcia. Zároveň musí zostať nositeľná v teple, preto výrobcovia hľadajú rovnováhu s prúdením vzduchu a odvodom vlhkosti.",
                    "Vyššia <a href=\"/n/gramaz-latky-co-znamena-gsm-pri-uterakoch-oblieckach-a-trickach\">gramáž GSM</a> môže súvisieť s väčším množstvom materiálu na ploche, ale nie je priamym prevodníkom na UPF. Voľná ťažká konštrukcia môže mať väčšie póry než ľahšia, veľmi hustá priadza. Ochranu preto nevyberajte podľa hmotnosti balenia alebo hrúbky medzi prstami.",
                ],
            },
            {
                "heading": "Natiahnutie pri plavkách, legínach a tričkách",
                "paragraphs": [
                    "Keď sa úplet natiahne, očká sa zväčšia a na rovnakú plochu kože pripadá menej materiálu. Najväčšie napätie býva na ramenách, hrudi, bokoch, sedacej časti, lakťoch a kolenách. Odev, ktorý je malý alebo navrhnutý na extrémne kompresné nosenie, sa preto môže správať inak než vo voľnej skúšobnej vzorke.",
                    "Vyberte správnu veľkosť a pri skúšaní sa posaďte, predkloňte a zdvihnite ruky. Sledujte, či materiál výrazne presvitá medzi očkami a či sa neposúvajú lemy. Presvitanie je varovný praktický signál, nie meranie UPF. Pri deklarovanom ochrannom odeve sa spoliehajte na výrobcu, ktorý má zohľadniť určené použitie a systém označenia.",
                ],
            },
            {
                "heading": "Čo robí vlhkosť a prečo neplatí jedno pravidlo",
                "paragraphs": [
                    "Voda mení index lomu, napučanie, hmotnosť a kontakt látky s pokožkou. Mokré tričko sa môže prilepiť, natiahnuť a zmeniť svoje optické vlastnosti. Pri niektorých materiáloch ochrana klesá, pri iných môže byť zmena menšia alebo odlišná. Preto sa nespoliehajte na všeobecnú vetu platnú pre všetky vlákna.",
                    "Ak má odev chrániť pri plávaní alebo vodných športoch, hľadajte výrobok určený na mokré použitie a jeho konkrétne pokyny. AATCC TM183 počíta s postupmi pre suché alebo mokré vzorky, kým európske označenie používa vlastné klasifikačné pravidlá. Dôležité je, v akom stave a podľa akého systému vznikla deklarácia vášho výrobku.",
                ],
                "callout": {
                    "title": "Po kúpaní skontrolujte viac než len suchosť",
                    "items": [
                        "Je látka stále v správnej polohe, alebo sa mokrá vyhrnula a odhalila kožu?",
                        "Je materiál silno natiahnutý a presvitá na miestach pohybu?",
                        "Nezostali na ňom poškodenia od drsného bazéna, piesku, suchého zipsu alebo dosky?",
                        "Chráňte odkryté miesta a riaďte sa zdravotnými odporúčaniami aj vtedy, keď má odev vysoké UPF.",
                    ],
                    "background": "#f7fbf8",
                    "border": "#dbe5de",
                },
            },
            {
                "heading": "Farba, vlákno a povrchová úprava",
                "paragraphs": [
                    "Farbivá a pigmenty môžu pohlcovať časť UV, preto sa pri inak podobnej látke môžu farby líšiť. Tmavší odtieň často poskytuje lepší základ, no nie je zárukou konkrétneho UPF. Veľký otvor v tmavom úplete zostáva otvorom a svetlá látka so špeciálnou konštrukciou alebo úpravou môže mať vysoký skúšaný výsledok.",
                    "Rozdiely existujú aj medzi vláknami, no zloženie sa nesmie používať ako jediný test. Polyester, polyamid, bavlna či viskóza môžu mať rôznu jemnosť, lesk, pigment, hustotu a úpravu. Pri outdoorovom textile preto čítajte aj sprievodcu <a href=\"/n/co-je-polyamid-alebo-nylon-vlastnosti-odolnost-a-pranie\">čo je polyamid alebo nylon</a>, ale ochranné číslo prevezmite iba z dôveryhodného označenia konkrétneho výrobku.",
                ],
            },
            {
                "heading": "Opotrebovanie, vyblednutie a poškodené miesta",
                "paragraphs": [
                    "Oder stenčuje priadze, vyťahuje očká a vytvára póry. Typické sú ramená pod batohom, sed, kolená, lakte a okraje plaviek. Dierka znamená miesto bez materiálovej bariéry a výrazne odretá plocha už nemusí zodpovedať novému skúšanému stavu. Pri ochrannom účele poškodenie neprekrývajte iba neurčitou domnienkou.",
                    "Vyblednutie môže znamenať zmenu farbiva a často sprevádza ďalšie starnutie, no samo nepovie, koľko UPF zostalo. Článok o <a href=\"/n/stalofarebnost-textilu-preco-farby-blednu-pri-prani-svetle-a-treni\">stálofarebnosti textilu</a> vysvetľuje svetlo, pranie a trenie ako odlišné mechanizmy. Ak je ochrana zdravotne dôležitá a odev je výrazne opotrebovaný, najbezpečnejšie je nahradiť ho výrobkom s jasnou deklaráciou.",
                ],
            },
            {
                "heading": "Pranie ochranného oblečenia bez poškodenia",
                "paragraphs": [
                    "Najprv postupujte podľa etikety výrobcu. Zatvorte zipsy, oddeľte suché zipsy a drsné kusy, nepreplňujte bubon a použite povolenú teplotu a program. Cieľom je odstrániť pot, soľ, chlór, piesok a opaľovacie prípravky bez zbytočného oderu, vytiahnutia alebo degradácie elastických častí.",
                    "Opaľovací olej môže vytvoriť mastnú škvrnu, ale agresívne lokálne odmasťovanie nemusí byť vhodné pre farbu, elastan alebo špeciálnu úpravu. Postupujte podľa návodu <a href=\"/n/ako-vyprat-opalovaci-olej-z-plazovej-tuniky-a-uteraka\">ako vyprať opaľovací olej z textilu</a> a najprv testujte na skrytom mieste. Žiadny domáci zásah nesľubuje zachovanie certifikácie, ak ho výrobca nepovoľuje.",
                ],
            },
            {
                "heading": "Pokrytie tela a kompletná ochrana pred slnkom",
                "paragraphs": [
                    "UPF látky je užitočné len na mieste, ktoré zostáva zakryté. Dlhší rukáv, vyšší golier, priliehavé manžety bez silného natiahnutia a širší okraj klobúka rozširujú chránenú plochu. Pri pohybe skontrolujte medzery medzi tričkom a nohavicami, vyhrnuté rukávy aj odkrytý krk.",
                    "WHO odporúča ochranné oblečenie ako jednu časť kombinácie spolu s tieňom, klobúkom, slnečnými okuliarmi a širokospektrálnou ochranou odkrytej kože. Pri deťoch, citlivej pokožke, liekoch zvyšujúcich fotosenzitivitu alebo kožnom ochorení sa poraďte so zdravotníckym odborníkom. Všeobecný text nemôže určiť individuálnu bezpečnú expozíciu.",
                ],
                "callout": {
                    "title": "Bezpečná hranica tohto sprievodcu",
                    "items": [
                        "UPF nie je povolenie predĺžiť pobyt na prudkom slnku o vypočítaný počet hodín.",
                        "Odev chráni iba zakryté miesta a jeho deklarácia platí v hraniciach výrobku, skúšky a návodu.",
                        "Pri vysokom UV indexe používajte kombináciu opatrení a riaďte sa aktuálnymi zdravotnými odporúčaniami.",
                        "Pri kožných zmenách, popálení alebo osobitnej citlivosti vyhľadajte primeranú zdravotnú radu.",
                    ],
                },
            },
        ],
        "table2_heading": "Kedy ochranný odev skontrolovať alebo vymeniť",
        "table2_intro": "Bez opätovnej skúšky nemožno doma vypočítať zostávajúce UPF. Viditeľné a funkčné zmeny však pomáhajú rozhodnúť, kedy sa na starý kus už nespoliehať ako na hlavnú ochrannú vrstvu.",
        "table2_headers": ["Situácia", "Prečo je dôležitá", "Čo skontrolovať", "Bezpečný ďalší krok"],
        "table2_rows": [
            ("Materiál je príliš tesný", "Natiahnutie môže zväčšiť póry a zmenšiť krytie.", "Presvitanie, posun lemov a stav pri pohybe.", "Zvoliť správnu veľkosť alebo strih určený na danú aktivitu."),
            ("Odev má dierky alebo odreté plochy", "Chýbajúci a stenčený materiál nevytvára pôvodnú bariéru.", "Ramená, kolená, sed, švy a miesta pod výstrojom.", "Pri významnej ochrane kus nahradiť; opravu konzultovať s výrobcom."),
            ("Používa sa najmä mokrý", "Vlhkosť a natiahnutie môžu zmeniť prenos podľa látky.", "Či deklarácia a návod počítajú s mokrým použitím.", "Vybrať výrobok skúšaný a určený pre vodné prostredie."),
            ("Označenie je nejasné alebo chýba", "Nie je známa hodnota, systém ani stav vzorky.", "Etiketu, dokumentáciu výrobcu a dôveryhodnosť tvrdenia.", "Nespoliehať sa na neurčitý nápis ako na potvrdenú úroveň."),
            ("Strih odhaľuje kožu pri pohybe", "Na odkrytej ploche textil neposkytuje ochranu.", "Výstrih, pás, zápästia, členky a krk v reálnom pohybe.", "Doplniť pokrytie a chrániť odkryté miesta odporúčaným spôsobom."),
        ],
        "steps": [
            "Skontrolujte aktuálny UV index a naplánujte kombináciu tieňa, času, oblečenia, klobúka, okuliarov a ochrany odkrytej kože.",
            "Vyberte výrobok s jasnou hodnotou UPF, uvedeným systémom a návodom pre zamýšľané suché alebo mokré použitie.",
            "Vyskúšajte správnu veľkosť v pohybe a overte, že látka nie je nadmerne natiahnutá ani vyhrnutá.",
            "Pred použitím prezrite dierky, rednutie, vyblednuté odreté miesta, poškodené švy a stratu pružnosti.",
            "Po nosení odstráňte soľ, chlór, piesok, pot a škvrny iba postupom povoleným výrobcom a odev úplne vysušte.",
            "Ak sa ochranný kus výrazne zmenil alebo označenie nie je dôveryhodné, nespoliehajte sa na odhad a nahraďte ho vhodným výrobkom.",
        ],
        "remember": [
            "Má výrobok konkrétne UPF a uvedený skúšobný alebo klasifikačný systém?",
            "Platí deklarácia pre suchý, mokrý, nový alebo upravený stav relevantný pre vaše použitie?",
            "Zostáva odev voľný a zakrýva rovnakú plochu aj pri sedení, plávaní a pohybe?",
            "Je látka bez dier, výrazného oderu, vyťahaných očiek a miest, ktoré presvitajú viac než zvyšok?",
            "Dopĺňate textil tieňom, klobúkom, okuliarmi a ochranou odkrytej pokožky podľa zdravotných odporúčaní?",
        ],
        "mistakes": [
            "Odhadnúť konkrétne UPF iba podľa tmavej farby, hrúbky, ceny alebo názvu vlákna.",
            "Zamieňať UPF textilu so SPF opaľovacieho prípravku alebo ich mechanicky sčítavať.",
            "Ignorovať silné natiahnutie plaviek a legín na miestach, ktoré majú zostať chránené.",
            "Predpokladať, že voda znižuje ochranu každej látky rovnakým percentom bez znalosti skúšky.",
            "Spoliehať sa na vysoké UPF krátkeho trička, hoci veľká časť kože zostáva odkrytá.",
            "Prať ochranný odev agresívnejšie než povoľuje štítok alebo pokračovať v používaní cez dierky a silný oder.",
        ],
        "expert_heading": "Odbornejší pohľad: spektrálny prenos a podmienky skúšky",
        "expert": [
            "AATCC TM183 opisuje prístrojové meranie UV žiarenia preneseného cez textil v intervaloch vlnových dĺžok a výpočet UPF z erytémovo váženého žiarenia. Metóda zahŕňa postupy pre suché aj mokré vzorky a počíta aj percento blokovania UVA a UVB. Samotná metóda merania nie je totožná s každým klasifikačným a označovacím pravidlom; tie treba uviesť osobitne.",
            "UK Health Security Agency odkazuje pri ochrannom oblečení na európske normy EN 13758 a uvádza požiadavku tejto konkrétnej klasifikačnej schémy. AATCC TM183 je metóda merania prenosu, nie automaticky rovnaké európske označenie. Hranice rozdielnych systémov sa preto nesmú miešať bez kontextu ani prenášať na výrobok, ktorý neuvádza použitú schému.",
            "WHO zdôrazňuje kombinovanú ochranu a uvádza, že výsledok textilu ovplyvňuje štruktúra, veľkosť pórov, hrúbka a zloženie vlákien, farba, úprava aj mokrý alebo suchý stav. Zároveň upozorňuje na nadmerné UV ako zdravotné riziko. Technický údaj UPF má preto slúžiť na informovaný výber bariéry, nie na výpočet individuálne bezpečného času na slnku.",
        ],
        "source_intro": "Meranie UPF vyžaduje spektrálny prístroj a definovaný stav vzorky. Zdravotnícke a radiačné autority zároveň zdôrazňujú pokrytie kože a kombináciu viacerých ochranných opatrení.",
        "sources": [
            ("AATCC TM183: prenos a blokovanie UV žiarenia cez látku", AATCC_UV),
            ("WHO: ochrana kože pred UV a úloha oblečenia", WHO_UV),
            ("UK Health Security Agency: UV a európske textilné normy", UK_UV),
            ("GINETEX: význam symbolov ošetrovania textilu", GINETEX),
        ],
        "product_intro": "Pot, soľ, chlór, piesok a opaľovacie prípravky treba odstrániť bez zbytočného poškodenia farby, elastanu, povrchu alebo ochrannej úpravy. Vždy má prednosť návod konkrétneho odevu.",
        "product_text": "Prací gél z marseillského mydla je určený na bežnú domácu bielizeň. Použite ho na ochranný odev iba vtedy, ak etiketa výrobku povoľuje bežný gél; dávkujte presne a oddeľte látku od zipsov a drsných textílií.",
        "product_limit": "Prací gél nevytvára ani neobnovuje certifikované UPF a nemusí byť vhodný pre plavky, elastan, membránu alebo špeciálnu UV úpravu. Pri nejasnosti použite prostriedok odporúčaný výrobcom.",
        "category_intro": "Pri bežnej bielizni vyberajte podľa materiálu a farby. Pri odeve s deklarovanou UV ochranou rozhoduje zlučiteľnosť s konkrétnou úpravou, zachovanie rozmeru a čo najmenšie mechanické poškodenie.",
        "category_text": "V kategórii nájdete gély na bežné pranie. Pred použitím porovnajte etiketu gélu so symbolmi odevu; ak výrobca predpisuje osobitný prostriedok, ručné pranie alebo zákaz určitej prísady, jeho pokyn má prednosť.",
        "related": [
            ("Ako vyprať opaľovací olej z plážového textilu", ARTICLE_SUNSCREEN_STAIN),
            ("Čo je polyamid alebo nylon", ARTICLE_POLYAMIDE),
            ("Stálofarebnosť textilu pri svetle a praní", ARTICLE_COLORFASTNESS),
            ("Čo je softshell a ako sa oň starať", ARTICLE_SOFTSHELL),
            ("Ako odstrániť repelent z outdoorového textilu", ARTICLE_REPELLENT),
            ("Materiály pre športové oblečenie", ARTICLE_SPORT_MATERIALS),
        ],
        "faq_title": "UPF a UV ochrana oblečenia",
        "faq": [
            ("Čo znamená UPF 50?", "V skúšobných podmienkach zodpovedá približne jednej päťdesiatine erytémovo váženého UV, teda asi dvom percentám, ktoré prejdú materiálom. Nie je to časový prísľub bezpečného pobytu."),
            ("Je UPF to isté ako SPF?", "Nie. UPF sa používa pri textile a hodnotí prenos UV cez látku, SPF pri opaľovacích prípravkoch. Výrobky sa používajú na odlišné plochy a hodnoty sa nesčítavajú."),
            ("Chráni každé tmavé tričko pred UV?", "Každá látka časť UV tlmí, no tmavá farba sama nepotvrdzuje konkrétnu ochranu. Dôležitá je hustota, póry, natiahnutie, vlákno, úprava a skúška."),
            ("Znižuje mokrá látka vždy UPF?", "Nie je správne uviesť jedno pravidlo pre všetky látky. Voda mení optické a mechanické vlastnosti a účinok závisí od konštrukcie; hľadajte údaj pre zamýšľaný mokrý stav."),
            ("Môže sa UV ochrana praním stratiť?", "Pranie môže odstrániť alebo poškodiť niektoré úpravy, ale pri iných látkach sa štruktúra môže zmeniť odlišne. Rozhoduje výrobok, jeho testovanie a dodržanie návodu."),
            ("Stačí UPF oblečenie bez ďalšej ochrany?", "Nie. Chráni len zakryté miesta. WHO odporúča kombinovať oblečenie s tieňom, klobúkom, okuliarmi a širokospektrálnou ochranou odkrytej pokožky."),
        ],
    },
]


def article_hrefs(markup):
    return re.findall(r'href="([^"]+)"', markup)


def fetch_for_preflight(url, headers):
    errors = []
    for read_timeout in (30, 60):
        try:
            return (
                requests.get(
                    url,
                    timeout=(15, read_timeout),
                    allow_redirects=True,
                    headers=headers,
                ),
                None,
            )
        except requests.RequestException as exc:
            errors.append(f"{type(exc).__name__}: {exc}")
    return None, " | ".join(errors)


def preflight_links(articles):
    rows = []
    seen = set()
    batch_paths = {f"/n/{article['link']}" for article in articles}
    headers = {"User-Agent": "Codex VEVO batch 43 link preflight"}

    for article in articles:
        target_url = f"{BASE}/n/{article['link']}"
        response, error = fetch_for_preflight(target_url, headers)
        rows.append(
            {
                "url": target_url,
                "kind": "target_slug_precheck",
                "ok": bool(response is not None and response.status_code == 404),
                "expected_status": 404,
                "status": response.status_code if response is not None else None,
                "final_url": response.url if response is not None else None,
                "error": error,
            }
        )

        for href in article_hrefs(article["long"]):
            url = urljoin(BASE, href)
            if url in seen:
                continue
            seen.add(url)
            response, error = fetch_for_preflight(url, headers)
            path = urlparse(url).path.rstrip("/")
            host = urlparse(url).hostname or ""
            is_intra_batch = path in batch_paths
            expected_status = 404 if is_intra_batch else 200
            expected_statuses = [expected_status]
            if host == "www.iso.org" and expected_status == 200:
                expected_statuses.append(403)
            rows.append(
                {
                    "url": url,
                    "kind": "intra_batch_target_precheck" if is_intra_batch else "article_link",
                    "ok": bool(response is not None and response.status_code in expected_statuses),
                    "expected_status": expected_status,
                    "expected_statuses": expected_statuses,
                    "status": response.status_code if response is not None else None,
                    "final_url": response.url if response is not None else None,
                    "error": error,
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
        raise SystemExit("Batch 43 titles do not exactly match the duplicate-guard candidate file")

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
        raise SystemExit("Batch 43 link preflight failed")


if __name__ == "__main__":
    main()
