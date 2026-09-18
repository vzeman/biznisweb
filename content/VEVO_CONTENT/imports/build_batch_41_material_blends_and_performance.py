import html
import json
import re
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests


BASE = "https://www.vevo.sk"
PUBLISH_DATE = "2026-07-21"
CANDIDATES = Path("content/VEVO_CONTENT/batches/batch-41-candidates-2026-07-21.txt")
OUT_JSON = Path("content/VEVO_CONTENT/imports/batch-41-2026-07-21-articles.json")
OUT_PREFLIGHT = Path("content/VEVO_CONTENT/exports/batch-41-2026-07-21-link-preflight.json")

PRODUCT_NAME = "Prací gél hypoalergénny Vevo Ylang Absolute 1L"
PRODUCT_URL = "/p-1627/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l"
CATEGORY_NAME = "Pracie gély"
CATEGORY_URL = "/c/vevo-home-care/pranie/praci-gel"

ASTM_TEXTILES = "https://regional.astm.org/industry/textiles"
AATCC_DIMENSION = "https://members.aatcc.org/store/tm135/543/"
AATCC_STATIC = "https://members.aatcc.org/store/tm115/525/"
GINETEX = "https://www.ginetex.net/share/article/4201/care-symbols"
COTTONWORKS_STRETCH = "https://cottonworks.com/product-innovation/product-technologies/natural-stretch-technology/"
WOOLMARK_CORE = "https://www.woolmark.com/industry/product-development/product-innovations/core-spun-technology/"
WOOLMARK_CARE = "https://www.woolmark.com/care"
ISO_STATIC = "https://www.iso.org/standard/61384.html"
ISO_STATIC_FILAMENT = "https://www.iso.org/standard/77996.html"
NIST_MOISTURE = "https://nvlpubs.nist.gov/nistpubs/jres/24/jresv24n6p645_A1b.pdf"
ISO_MARTINDALE = "https://www.iso.org/standard/61058.html"
ISO_COATED_ABRASION = "https://www.iso.org/standard/77552.html"
ISO_PILLING = "https://www.iso.org/standard/75376.html"

ARTICLE_COTTON = "/n/co-je-bavlna-vlastnosti-vyhody-nevyhody-a-starostlivost"
ARTICLE_ELASTANE = "/n/co-je-elastan-preco-je-v-leginach-spodnej-bielizni-a-sportovom-obleceni"
ARTICLE_JEANS = "/n/ako-prat-rifle-aby-nevybledli-nezmaekli-a-drzali-tvar"
ARTICLE_UNDERWEAR = "/n/ako-prat-podprsenku-a-jemnu-spodnu-bielizen-bez-deformacie"
ARTICLE_POLYAMIDE = "/n/co-je-polyamid-alebo-nylon-vlastnosti-odolnost-a-pranie"
ARTICLE_MERINO = "/n/co-je-merino-vlna-vyhody-nevyhody-a-pranie-bez-zrazenia"
ARTICLE_PILLING = "/n/preco-sa-oblecenie-zmolkuje-vlakna-trenie-pranie-a-susenie"
ARTICLE_BLEND = "/n/co-je-zmesovy-material-preco-sa-oblecenie-zraza-alebo-sprava-inak-nez-cakate"
ARTICLE_GSM = "/n/gramaz-latky-co-znamena-gsm-pri-uterakoch-oblieckach-a-trickach"
ARTICLE_SYNTHETIC = "/n/ako-prat-syntetiku-polyester-a-elastan-aby-nezapachali-a-drzali-tvar"

ARTICLE_COTTON_ELASTANE = "/n/bavlna-a-elastan-starostlivost-o-tricka-rifle-a-spodnu-bielizen"
ARTICLE_WOOL_POLYAMIDE = "/n/vlna-a-polyamid-preco-sa-miesaju-vlakna-a-ako-to-ovplyvnuje-pranie"
ARTICLE_STATIC = "/n/staticka-elektrina-v-obleceni-preco-latky-prilnu-a-ako-obmedzit-iskrenie"
ARTICLE_MARTINDALE = "/n/odolnost-textilu-proti-oderu-co-znamena-martindale-pri-obleceni-a-bytovych-latkach"

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
<h2 style="margin-top: 0;">Prací prostriedok prispôsobte najcitlivejšej časti výrobku</h2>
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
        callout("Najdôležitejšie zistenia v skratke", article["quick"]),
        f"<h2>{esc(article['overview_heading'])}</h2>",
    ]
    parts.extend(f"<p>{paragraph}</p>" for paragraph in article["overview"])
    parts.append(f"<h2>{esc(article['table1_heading'])}</h2>")
    parts.append(f"<p>{article['table1_intro']}</p>")
    parts.append(table(article["table1_headers"], article["table1_rows"]))
    for index, (heading, paragraphs) in enumerate(article["sections"], start=1):
        parts.append(f"<h2>{esc(heading)}</h2>")
        parts.extend(f"<p>{paragraph}</p>" for paragraph in paragraphs)
        if index in article["notes"]:
            note = article["notes"][index]
            parts.append(
                callout(
                    note[0],
                    note[1],
                    background=note[2],
                    border=note[3],
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
    parts.append(faq(article["faq_title"], article["faq"]))
    return "\n".join(parts)


ARTICLES = [
    {
        "title": "Bavlna a elastan: starostlivosť o tričká, rifle a spodnú bielizeň",
        "link": "bavlna-a-elastan-starostlivost-o-tricka-rifle-a-spodnu-bielizen",
        "meta": "Ako prať bavlnu s elastanom bez zrazenia a straty pružnosti. Rozdiely medzi tričkom, strečovými rifľami a spodnou bielizňou.",
        "short": "Bavlna s elastanom spája príjemný bavlnený základ s pružnosťou, no výsledok neurčuje iba percento vlákien. Praktický sprievodca vysvetľuje pranie, sušenie a riešenie straty tvaru pri tričkách, rifliach a spodnej bielizni.",
        "answer": "Bavlnu s elastanom perte podľa štítku, zvyčajne naruby, s primeranou náplňou bubna a bez zbytočne vysokej teploty. Bavlnená časť môže meniť rozmery, kým elastan môže vplyvom tepla, chlóru a dlhého mechanického namáhania strácať pružný návrat. Tričko, strečové rifle a spodná bielizeň preto nemusia zniesť rovnaký program, hoci majú podobné percentá vlákien. Rozhoduje aj pletenina alebo tkanina, farbenie, švy, guma, potlač a pokyny výrobcu.",
        "intro": "Označenie 95 % bavlna a 5 % elastan vyzerá ako jednoduchý návod na správanie látky, ale nie je ním. Päť percent pružného vlákna môže výrazne zmeniť priliehanie a návrat odevu do tvaru. Zároveň však dve látky s rovnakým zložením môžu mať inú hustotu, smer pružnosti, stabilitu rozmerov aj citlivosť na teplo. Pri údržbe preto nestačí poznať názvy vlákien. Treba rozlíšiť, či ide o ľahké tričko, hustý denim, jemný úplet alebo výrobok s gumou, čipkou a tvarovanými časťami.",
        "quick": [
            "<strong>Bavlna a elastan majú rozdielne úlohy:</strong> bavlna tvorí väčšinu povrchu a objemu, elastan zabezpečuje natiahnutie a návrat.",
            "<strong>Percento nie je celý príbeh:</strong> pružnosť môže vznikať aj pletením, tkaním a mechanickou úpravou bez veľkého podielu elastanu.",
            "<strong>Zrazenie nie je to isté ako vytiahnutie:</strong> zmena rozmerov bavlny a únava pružného vlákna sú odlišné procesy s odlišnými príčinami.",
            "<strong>Teplo treba posudzovať podľa celého výrobku:</strong> citlivá môže byť guma, potlač, lepidlo, farba alebo elastická priadza.",
            "<strong>Štítok má prednosť:</strong> symboly sú určené pre konkrétny hotový odev, nie iba pre jeho väčšinové vlákno.",
        ],
        "overview_heading": "Čo v zmesi robí bavlna a čo elastan",
        "overview": [
            "Bavlnené vlákno prijíma vlhkosť, poskytuje známy dotyk a dá sa spracovať do veľmi rozdielnych priadzí, úpletov a tkanín. Jeho správanie po praní ovplyvňuje napätie vložené pri výrobe, krútenie priadze, hustota štruktúry, povrchová úprava a spôsob sušenia. Preto sa jedno bavlnené tričko môže po prvých cykloch mierne skrátiť, zatiaľ čo iné zostane stabilné. Podrobnejší základ ponúka článok <a href=\"/n/co-je-bavlna-vlastnosti-vyhody-nevyhody-a-starostlivost\">čo je bavlna a ako sa o ňu starať</a>.",
            "Elastan je pružné syntetické vlákno, ktoré sa v odeve zvyčajne nachádza v menšom podiele. Jeho úlohou nie je zvýšiť savosť ani pevnosť bavlny, ale umožniť natiahnutie a následný návrat. To zlepšuje priliehanie trička, pohodlie strečových riflí či stabilitu lemu spodnej bielizne. Vysoká teplota, chlórové bielidlo, opakované presúšanie a dlhodobé napnutie môžu návratnosť postupne zhoršovať. Viac vysvetľuje samostatný sprievodca <a href=\"/n/co-je-elastan-preco-je-v-leginach-spodnej-bielizni-a-sportovom-obleceni\">čo je elastan</a>.",
            "Hotový výrobok treba chápať ako systém. V rifliach môže byť elastická priadza uložená prevažne v jednom smere, pri legínovom úplete v dvoch smeroch a v spodnej bielizni môže byť ďalšia guma všitá do pása. Aj malý detail môže určiť najšetrnejší povolený proces. Zloženie na etikete preto používajte na pochopenie materiálu, ale rozhodnutie o praní robte podľa ošetrovacích symbolov celého odevu.",
        ],
        "table1_heading": "Tri typy výrobkov s podobným zložením, ale odlišnou údržbou",
        "table1_intro": "Nasledujúce porovnanie ukazuje, prečo nemožno nastaviť jeden program pre všetko, na čom je uvedená bavlna a elastan. Hodnoty zo štítku konkrétneho kusu majú vždy prednosť.",
        "table1_headers": ["Výrobok", "Čo drží tvar", "Hlavné riziko", "Praktický smer"],
        "table1_rows": [
            ("Tričko", "Pletenina, bočné švy, golier a malý podiel elastanu.", "Skrátenie, skrútenie švov, poškodenie potlače alebo vytiahnutie výstrihu.", "Prať naruby, neprepĺňať bubon a sušiť bez nadmerného tepla."),
            ("Strečové rifle", "Denimová väzba, elastická priadza, pás, švy a kovové prvky.", "Vyblednutie, vytiahnuté kolená, oslabenie pružnosti a oder povrchu.", "Zapnúť, obrátiť naruby, prať s podobnými farbami a obmedziť presúšanie."),
            ("Spodná bielizeň", "Jemný úplet, lemovacie gumy, čipka, košíky a švy.", "Deformácia gumy, zatrhnutie, poškodenie výstuže alebo lepidla.", "Použiť ochranné vrecko, šetrný cyklus podľa štítku a voľné sušenie."),
            ("Ponožky a priliehavé kúsky", "Úplet, elastické zóny a zosilnené časti.", "Vyťahaný lem, žmolkovanie a nerovnomerné vysušenie.", "Triediť podľa farby a povrchu, páry nesťahovať tesnou gumičkou."),
        ],
        "sections": [
            (
                "Ako čítať údaj 95 % bavlna a 5 % elastan",
                [
                    "Percentá vyjadrujú hmotnostný podiel deklarovaných textilných vlákien, nie podiel na každej časti výrobku ani veľkosť ich účinku. Päť percent elastanu môže byť sústredených v tenkej elastickej priadzi, ktorá prechádza celou plochou úpletu. Malá hmotnosť tak neznamená malý funkčný význam. Zároveň údaj nehovorí, ako ďaleko sa látka natiahne, akou silou odporuje ani ako dobre sa vráti po stovkách použití.",
                    "Pri kombinovanom výrobku si všimnite, či etiketa uvádza osobitné zloženie hlavnej látky, čipky, podšívky alebo lemu. Podprsenka môže mať bavlnené košíky, polyamidovú čipku a samostatnú elastickú gumu. Rifle môžu obsahovať inú priadzu vo vreckovine než v denime. Najcitlivejšia časť môže obmedziť teplotu a spôsob sušenia aj vtedy, keď tvorí iba malú časť celku.",
                ],
            ),
            (
                "Pružnosť látky nemusí pochádzať iba z elastanu",
                [
                    "Pletenina sa dokáže natiahnuť už geometriou očiek. Niektoré tkaniny získavajú mechanickú pružnosť vhodnou väzbou, zvlnením priadze alebo výrobnou úpravou. CottonWorks opisuje technológiu prirodzenej pružnosti bavlnených tkanín, ktorá využíva konštrukciu bez elastomérového vlákna. To je dobrý príklad, prečo nemožno pružnosť predpovedať iba z percent na etikete.",
                    "Pri nákupe odev jemne natiahnite v smere, v ktorom bude pracovať, a sledujte návrat bez trvalých vĺn. Skontrolujte priehľadnosť, švy a to, či sa látka po uvoľnení nekrúti. Domáca skúška nenahrádza laboratórne meranie a nesmie poškodiť výrobok, ale pomôže odhaliť rozdiel medzi voľnou pleteninou, strečovou tkaninou a pevným materiálom s malým komfortným prieťahom.",
                ],
            ),
            (
                "Ako prať bavlnené tričko s elastanom",
                [
                    "Tričko obráťte naruby, najmä ak má potlač, tmavú farbu alebo povrch citlivý na trenie. Trieďte ho podľa farby a hmotnosti, nie iba podľa zloženia. Ľahký úplet nedávajte k uterákom, hrubým rifliam a kusom so suchým zipsom. Zvoľte teplotu a program zo štítku; pri bežnom znečistení nepomáha automaticky zvyšovať teplotu, ak tým riskujete zmenu rozmerov alebo poškodenie pružných častí.",
                    "Po praní tričko vyberte bez dlhého státia v bubne, jemne ho vyrovnajte a upravte bočné švy. Neťahajte mokrý výstrih ani spodný lem. Ak štítok nepovoľuje bubnové sušenie, sušte na vzduchu s rovnomerne rozloženou hmotnosťou. Pri povolenom sušení nepokračujte dlhšie, než je potrebné; presušenie zvyšuje tepelnú a mechanickú záťaž bez úžitku.",
                ],
            ),
            (
                "Ako prať strečové rifle bez vyblednutia a vytiahnutých kolien",
                [
                    "Rifle pred praním vyprázdnite, zapnite zips a gombík a obráťte naruby. Tým obmedzíte trenie lícnej strany a zachytávanie kovových prvkov o inú bielizeň. Perte s podobne tmavými a približne rovnako ťažkými kusmi. Veľmi plný bubon nešetrí tkaninu: obmedzuje pohyb pracieho roztoku, zvyšuje lokálne trenie a môže zhoršiť vypláchnutie hustého denimu.",
                    "Po cykle rifle vytiahnite, vyrovnajte pás a nohavice a sušte podľa štítku. Vysoké teplo môže urýchliť zmenu farby aj únavu pružnej priadze. Vytiahnuté kolená však nevznikajú iba praním; súvisia so strihom, dlhým sedením, napätím v látke a kvalitou návratu. Podrobný postup nájdete v návode <a href=\"/n/ako-prat-rifle-aby-nevybledli-nezmaekli-a-drzali-tvar\">ako prať rifle, aby držali tvar</a>.",
                ],
            ),
            (
                "Ako prať bavlnenú spodnú bielizeň s pružnými lemami",
                [
                    "Pri spodnej bielizni nemusí byť najcitlivejšia bavlna, ale guma, čipka, výstuž, háčiky alebo lepený detail. Zapnite háčiky a jemné kusy vložte do správne veľkého ochranného vrecka. Vrecko znižuje zachytávanie, ale neumožňuje ignorovať teplotný limit ani preplniť bubon. Košíky nestláčajte medzi ťažké uteráky a rifle.",
                    "Po praní bielizeň netočte a nevešajte za jeden tenký ramienok alebo elastický okraj. Vodu nechajte odtiecť a výrobok sušte v tvare odporúčanom výrobcom. Sušička je vhodná iba vtedy, ak ju symbol výslovne povoľuje. Podprsenky, čipku a tvarované kúsky rozoberá samostatný návod <a href=\"/n/ako-prat-podprsenku-a-jemnu-spodnu-bielizen-bez-deformacie\">ako prať podprsenku a jemnú bielizeň</a>.",
                ],
            ),
            (
                "Teplota, mechanika a čas pôsobia spoločne",
                [
                    "Poškodenie nevzniká iba jedným číslom na ovládači. Výsledok tvorí kombinácia teploty vody, dĺžky cyklu, pohybu bubna, odstreďovania, chemického prostredia a následného sušenia. Krátky program nemusí byť vždy šetrnejší, ak používa intenzívnejšiu mechaniku alebo nezvládne opláchnuť hustú náplň. Jemný program zase neznamená automatické povolenie pre každý výrobok s elastanom.",
                    "GINETEX vysvetľuje, že ošetrovacie symboly určujú najnáročnejší povolený proces. Vašou úlohou nie je pri každom cykle využiť maximum, ale zostať v jeho hraniciach a prispôsobiť proces znečisteniu. Ak výrobok dovoľuje 40 °C, pri ľahkom znečistení môže postačiť nižšia povolená voľba; pri hygienicky náročnom kuse však treba rešpektovať účel, pokyny výrobcu a možnosti pracieho prostriedku.",
                ],
            ),
            (
                "Dávkovanie a oplach pri hustých elastických materiáloch",
                [
                    "Prací prostriedok dávkujte podľa tvrdosti vody, množstva bielizne, znečistenia a údajov výrobcu. Viac gélu neznamená automaticky čistejší výsledok. Pri príliš vysokej dávke alebo preplnenom bubne môže zostať v hustej látke viac peny a zvyškov, ktoré zhoršujú dotyk a komplikujú oplach. Pri malej dávke zase nemusí byť dostatok účinných látok na mastnotu a bežnú špinu.",
                    "Ak je bielizeň po praní lepkavá, klzká alebo nezvyčajne tuhá, najprv skontrolujte dávku, náplň a oplach. Nepridávajte automaticky ďalší produkt do nasledujúceho cyklu. Pri citlivej pokožke je dôležité, aby bol prací roztok správne nadávkovaný a dôkladne odstránený. Samostatné extra plákanie môže pomôcť pri konkrétnej náplni, ale nenahrádza správne dávkovanie ani údržbu práčky.",
                ],
            ),
            (
                "Sušenie: najčastejšie miesto zbytočnej tepelnej záťaže",
                [
                    "Mokrý odev je ťažší a pružná konštrukcia môže byť citlivejšia na bodové zaťaženie. Vešajte ho tak, aby sa hmotnosť rozložila, a nenechávajte ho dlho napnutý na úzkom kolíku alebo vešiaku. Pri tmavých rifliach môže priame silné slnko urýchliť zmenu farby. Pri tričku môže nevhodný vešiak vytvoriť výstupky na ramenách, ktoré si používateľ ľahko pomýli s trvalým poškodením vlákna.",
                    "Bubnové sušenie používajte iba podľa symbolu. Aj pri povolenom programe vyberte zodpovedajúcu teplotu a nenechávajte suchý odev zbytočne pokračovať v horúcom bubne. Opakované presúšanie spája teplo, trenie a napätie, teda tri faktory, ktoré zaťažujú farbu, povrch aj pružné časti. Dosušenie na vzduchu býva rozumné, keď sú hrubšie švy ešte vlhké, ale plocha už suchá.",
                ],
            ),
            (
                "Zrazené alebo vytiahnuté: ako rozlíšiť problém",
                [
                    "Ak sa tričko skrátilo v dĺžke alebo zúžilo v šírke, môže ísť o rozmerovú zmenu bavlnenej štruktúry. Ak pás nedrží, kolená ostávajú vyduté alebo lem vytvára vlny, pravdepodobnejšia je strata pružného návratu, zmena konštrukcie alebo poškodenie gumy. Niekedy sa objavia oba javy naraz: bavlnená časť sa zmenší a elastická priadza zostane relatívne dlhšia, takže povrch pôsobí zvlnený.",
                    "Pred záverom porovnajte výrobok s pôvodnými rozmermi, ak ich máte, a nechajte ho úplne vyschnúť bez napnutia. Mokrá bavlna a teplý odev zo sušičky sa môžu dočasne správať inak. Neodporúčame násilné naťahovanie, horúce namáčanie ani domáce chemické pokusy; môžu poškodiť farbu, švy a elastan. Pri drahom alebo konštrukčne zložitom kuse je bezpečnejšie obrátiť sa na výrobcu alebo odbornú čistiareň.",
                ],
            ),
            (
                "Ako vybrať trvácnejšiu zmes už pri nákupe",
                [
                    "Skontrolujte rovnomernosť úpletu alebo tkaniny, návrat po jemnom natiahnutí, rovné švy a spôsob upevnenia gumy. Pri rifliach sa posaďte a urobte bežný pohyb; pás nemá nepríjemne tlačiť ani odstávať a kolená nesmú byť napnuté na hranici. Pri tričku sledujte, či bočné švy zostávajú zvislé. Pri spodnej bielizni skontrolujte, či lem pruží rovnomerne bez miest, ktoré už v predajni pôsobia zvlnené.",
                    "Prečítajte si štítok ešte pred kúpou. Odev, ktorý vyžaduje proces nezlučiteľný s vašou rutinou, sa bude používať ťažšie bez ohľadu na kvalitu materiálu. Pýtajte sa aj na rozmerovú stabilitu a skúšky pružného návratu, ak ich výrobca uvádza. Bez názvu metódy a podmienok však číslo neporovnávajte s iným výrobkom. Laboratórny výsledok je najužitočnejší pri porovnaní vzoriek skúšaných rovnakým postupom.",
                ],
            ),
        ],
        "notes": {
            3: (
                "Rýchle rozhodnutie podľa typu kusu",
                [
                    "Tričko: chráňte potlač, golier a bočné švy pred trením a presušením.",
                    "Rifle: obmedzte trenie lícnej strany, perte s podobnou hmotnosťou a vyrovnajte ich ešte vlhké.",
                    "Spodná bielizeň: rozhoduje guma, čipka, háčiky a tvarovanie, nie iba bavlnený podiel.",
                    "Pri neistote zvoľte prísnejšie obmedzenie uvedené na štítku hotového výrobku.",
                ],
                "#f7fbf8",
                "#dbe5de",
            ),
            7: (
                "Keď výsledok po praní nie je dobrý",
                [
                    "Zvyšky na povrchu: skontrolujte dávku, tvrdosť vody, veľkosť náplne a oplach.",
                    "Skrátenie: preverte teplotu prania aj sušenia a porovnajte rozmery až po úplnom vychladnutí.",
                    "Vlnitý lem: môže ísť o rozdielnu rozmerovú zmenu látky a pružnej časti.",
                    "Vytiahnuté kolená: zohľadnite strih, dlhé napnutie a kvalitu návratu, nielen posledný cyklus.",
                ],
                "#fffaf5",
                "#e6ded2",
            ),
        },
        "table2_heading": "Príznak, pravdepodobná príčina a bezpečný prvý krok",
        "table2_intro": "Jedna vizuálna zmena môže mať viac príčin. Tabuľka pomáha začať kontrolu bez agresívneho zásahu, ktorý by problém zhoršil.",
        "table2_headers": ["Príznak", "Čo môže byť v pozadí", "Čo skontrolovať", "Čomu sa vyhnúť"],
        "table2_rows": [
            ("Tričko sa skrátilo", "Rozmerová zmena bavlny, teplota alebo napätie uvoľnené po praní.", "Štítok, teplotu vody, sušičku a pôvodné rozmery.", "Násilnému napínaniu a ďalšiemu horúcemu cyklu."),
            ("Rifle majú vyduté kolená", "Slabší pružný návrat, príliš tesný strih alebo dlhé napnutie.", "Návrat po odpočinku, sušenie a stav látky v okolí švov.", "Horúcemu sušeniu ako pokusu o rýchlu opravu."),
            ("Pás alebo lem sa vlní", "Rozdielna zmena látky a gumy, prehriatie alebo poškodený šev.", "Či je guma prasknutá, pretočená alebo nerovnomerne všitá.", "Žehleniu priamo cez elastickú časť bez povolenia."),
            ("Bielizeň je tuhá alebo klzká", "Nevhodná dávka, preplnenie alebo slabé vypláchnutie.", "Tvrdosť vody, dávkovanie, náplň a čistotu zásuvky.", "Pridávaniu ďalšieho prostriedku bez zistenia príčiny."),
            ("Potlač praská", "Trenie, teplo, nevhodné žehlenie alebo starnutie povrchu.", "Pokyny pre pranie naruby, sušenie a žehlenie.", "Žehličke priamo na potlači a vysokému teplu."),
        ],
        "steps": [
            "Prečítajte celé zloženie a ošetrovacie symboly vrátane osobitných častí výrobku.",
            "Rozdeľte bielizeň podľa farby, hmotnosti, povrchu a citlivých detailov; zapnite zipsy a háčiky.",
            "Odevy s potlačou, tmavý denim a jemný povrch obráťte naruby; malé citlivé kusy chráňte vreckom.",
            "Zvoľte program a teplotu v rámci štítku, bubon neprepĺňajte a prací gél dávkujte podľa podmienok.",
            "Po cykle výrobok vyberte, jemne vyrovnajte švy a obnovte tvar bez násilného naťahovania.",
            "Sušte spôsobom povoleným na štítku a ukončite tepelné sušenie, keď je výrobok suchý.",
            "Pri zmene tvaru najprv rozlíšte rozmerovú zmenu bavlny od straty pružného návratu.",
        ],
        "remember": [
            "Rovnaké percentá vlákien neznamenajú rovnakú pružnosť, hrúbku ani povolený program.",
            "Najcitlivejší detail výrobku môže byť dôležitejší než väčšinová bavlna.",
            "Zrazenie a vytiahnutie sú rozdielne javy; neodstraňujú sa jedným univerzálnym trikom.",
            "Primeraná dávka a dostatok priestoru v bubne pomáhajú praniu aj oplachu.",
            "Teplo pri sušení sa počíta do celkovej záťaže rovnako ako teplota vody.",
        ],
        "mistakes": [
            "<strong>Pranie všetkých zmesí na programe bavlna.</strong> Hotový výrobok môže obmedzovať elastan, guma, čipka, potlač alebo lepidlo.",
            "<strong>Hodnotenie pružnosti iba podľa percenta elastanu.</strong> Výsledok mení priadza, väzba, smer pružnosti a konštrukcia odevu.",
            "<strong>Preplnenie bubna v snahe obmedziť pohyb.</strong> Môže zvýšiť lokálne trenie a zhoršiť pranie aj oplach.",
            "<strong>Horúca sušička ako oprava vytiahnutých kolien.</strong> Krátkodobý pocit stiahnutia môže sprevádzať ďalšie poškodenie pružnej zložky.",
            "<strong>Vešanie mokrej bielizne za gumu.</strong> Koncentruje hmotnosť do citlivej elastickej časti.",
            "<strong>Agresívne domáce pokusy o roztiahnutie zrazeného kusa.</strong> Môžu deformovať švy, farbu a elastickú priadzu.",
        ],
        "expert_heading": "Ako sa oddelene hodnotí pružnosť a zmena rozmerov",
        "expert": [
            "ASTM v prehľade textilných skúšok uvádza samostatné metódy pre pružnosť pletenín a tkanín, napríklad D2594 a D6614, aj metódu D4964 pre napínanie a predĺženie elastických textílií. Takéto rozdelenie je podstatné: skúška musí zodpovedať typu konštrukcie a otázke, ktorú chceme zodpovedať. Výsledok jednej metódy nemožno bez podmienok preniesť na inú látku alebo zameniť za životnosť hotového odevu.",
            "AATCC TM135 sa venuje rozmerovým zmenám textílií po domácom praní. Sleduje iný jav než pružný návrat. Látka môže mať prijateľnú pružnosť, ale zmeniť dĺžku po praní; alebo si môže zachovať rozmery, no v namáhanom mieste sa vracať pomalšie. Pri reklamácii alebo vývoji výrobku preto treba presne pomenovať, či sa hodnotí zrazenie, predĺženie, rast materiálu alebo poškodenie pružnej časti.",
            "GINETEX pripomína, že symboly starostlivosti sa vzťahujú na hotový výrobok. To prepája laboratórnu a domácu prax: aj keď poznáme vlastnosti bavlny a elastanu, výrobca musí zohľadniť farbenie, šitie, potlač, kovové prvky a ďalšie materiály. Používateľ by preto nemal odvodiť vyššiu teplotu iba z toho, že väčšinu zloženia tvorí bavlna.",
        ],
        "source_intro": "Zdroje rozlišujú pružnosť, rozmerovú stabilitu, konštrukciu látky a povolenú starostlivosť. Neurčujú jednu teplotu pre všetky zmesi; tú stanovuje výrobca konkrétneho hotového odevu.",
        "sources": [
            ("ASTM: prehľad textilných skúšok vrátane pružnosti", ASTM_TEXTILES),
            ("AATCC TM135: rozmerové zmeny po domácom praní", AATCC_DIMENSION),
            ("CottonWorks: pružnosť bavlnenej tkaniny vytvorená konštrukciou", COTTONWORKS_STRETCH),
            ("GINETEX: význam ošetrovacích symbolov", GINETEX),
        ],
        "product_intro": "Pri bavlnených tričkách, strečových rifliach a bežnej spodnej bielizni je dôležitý prací prostriedok, ale rovnako aj správna dávka, veľkosť náplne, mechanika a teplota. Najprv overte, že štítok povoľuje domáce pranie a že výrobok nepotrebuje špeciálny postup.",
        "product_text": "Hypoalergénny prací gél Vevo Ylang Absolute je konkrétna možnosť pre bežné prateľné bavlnené zmesi. Dávku prispôsobte tvrdosti vody, množstvu a znečisteniu a ponechajte v bubne priestor na pranie aj oplach.",
        "product_limit": "Nie je univerzálnym riešením pre každý elastický výrobok. Vlna, membrána, lepené športové prvky, veľmi jemná čipka alebo špeciálna povrchová úprava môžu vyžadovať iný prípravok či profesionálnu starostlivosť.",
        "category_intro": "Pri výbere gélu pre zmesi bavlny a elastanu porovnávajte určenie výrobku, odporúčané dávkovanie a kompatibilitu s farbami. Vôňa ani veľké množstvo peny nie sú meradlom účinného oplachu alebo ochrany pružnosti.",
        "category_text": "V kategórii pracích gélov nájdete riešenia pre bežnú domácu bielizeň. Pri citlivom alebo drahom odeve vždy spojte pokyny na obale s ošetrovacím štítkom konkrétneho kusu.",
        "related": [
            ("Čo je bavlna", ARTICLE_COTTON),
            ("Čo je elastan", ARTICLE_ELASTANE),
            ("Ako prať rifle", ARTICLE_JEANS),
            ("Ako prať podprsenku a jemnú bielizeň", ARTICLE_UNDERWEAR),
            ("Čo je zmesový materiál", ARTICLE_BLEND),
            ("Ako vzniká statická elektrina v oblečení", ARTICLE_STATIC),
        ],
        "faq_title": "bavlna a elastan",
        "faq": [
            ("Na koľkých stupňoch prať bavlnu s elastanom?", "Na teplote uvedenej na štítku konkrétneho výrobku. Zloženie 95 % bavlna a 5 % elastan samo neurčuje povolených 30 alebo 40 °C, pretože limit môže vytvárať farba, potlač, guma alebo konštrukcia."),
            ("Môže sa bavlna s elastanom zraziť?", "Áno, bavlnená štruktúra môže zmeniť rozmery, najmä pri nevhodnom praní alebo sušení. Zrazenie treba odlíšiť od straty pružného návratu, pri ktorej sa odev skôr vyťahuje alebo vlní."),
            ("Môžu ísť strečové rifle do sušičky?", "Iba ak to povoľuje symbol na konkrétnych rifliach. Aj potom zvoľte povolenú teplotu a nepresúšajte ich; dlhé teplo a trenie zbytočne zaťažujú farbu aj pružnú priadzu."),
            ("Prečo sa tričko s elastanom po praní skrútilo?", "Príčinou môže byť konštrukcia úpletu, napätie pri výrobe, nerovnomerné sušenie alebo mechanické namáhanie. Elastan nie je jediná možná príčina a domáce násilné naťahovanie nemusí problém vyriešiť."),
            ("Je viac elastanu vždy lepšie?", "Nie. Vyšší podiel môže zmeniť priliehanie a pružnosť, ale kvalitu návratu určujú aj priadza, väzba a spracovanie. Pre konkrétny odev je dôležitejší funkčný výsledok než samotné percento."),
            ("Treba na bavlnu s elastanom aviváž?", "Nie automaticky. Riaďte sa štítkom a účelom výrobku. Pri funkčných, veľmi elastických alebo špeciálne upravených kusoch môže povrchový film zmeniť vlastnosti, preto je vhodné overiť pokyny výrobcu."),
        ],
    },
    {
        "title": "Vlna a polyamid: prečo sa miešajú vlákna a ako to ovplyvňuje pranie",
        "link": "vlna-a-polyamid-preco-sa-miesaju-vlakna-a-ako-to-ovplyvnuje-pranie",
        "meta": "Prečo sa vlna mieša s polyamidom, čo zmes získava a ako ju prať bez plstnatenia či straty tvaru. Svetre, ponožky aj merino vrstvy.",
        "short": "Vlna s polyamidom môže byť pevnejšia a odolnejšia, stále však nemusí zniesť bežný program na syntetiku. Sprievodca vysvetľuje priadzu, plstnatenie, štítok, pranie a sušenie svetrov, ponožiek aj merino vrstiev.",
        "answer": "Vlna sa s polyamidom mieša najmä preto, aby výrobok získal pevnosť, odolnosť proti oderu alebo stabilnejšiu konštrukciu pri nižšej hmotnosti. Prítomnosť polyamidu však automaticky nerobí sveter či merino ponožky vhodnými na bežný syntetický program. O praní rozhoduje ošetrovací štítok, typ vlny, úprava proti plstnateniu, konštrukcia priadze, švy a ďalšie časti. Ak je povolené domáce pranie, používajte určený program na vlnu alebo jemnú bielizeň, vhodný prípravok, nízku mechaniku a sušenie v tvare.",
        "intro": "Zloženie 80 % vlna a 20 % polyamid môže označovať jemnú merino spodnú vrstvu, odolnú ponožku, ľahký sveter aj bytový textil. V každom výrobku plní zmes inú úlohu. Polyamid môže byť premiešaný medzi vlnenými vláknami, tvoriť nosné jadro priadze alebo spevňovať iba pätu a špičku. Bez informácie o konštrukcii preto nevieme z percent vypočítať mäkkosť, životnosť ani povolené pranie. Najbezpečnejší postup začína štítkom a pochopením, že citlivosť vlny zostáva dôležitá aj v zmesi.",
        "quick": [
            "<strong>Polyamid môže spevniť výrobok:</strong> zvyšuje pevnosť a odolnosť namáhaných častí, no výsledok závisí od umiestnenia v priadzi alebo látke.",
            "<strong>Vlna zostáva rozhodujúca pre starostlivosť:</strong> trenie, teplo a nevhodný chemický proces môžu podporiť plstnatenie alebo zmenu tvaru.",
            "<strong>Machine washable je samostatná informácia:</strong> samotný podiel polyamidu nepotvrdzuje, že výrobok možno prať v práčke.",
            "<strong>Žmolky a oder nie sú to isté:</strong> povrch môže žmolkovať bez predratia a odolná zmes môže mať viditeľné žmolky.",
            "<strong>Mokrá vlna potrebuje oporu:</strong> sveter po praní vyrovnajte a sušte rozložený, ak výrobca neurčil inak.",
        ],
        "overview_heading": "Prečo sa k vlne pridáva polyamid",
        "overview": [
            "Vlna prináša pružné zvlnenie vlákna, tepelný komfort a schopnosť pracovať s vodnou parou. Jej vlastnosti sa menia podľa plemena, jemnosti, dĺžky vlákna, spriadania, pletenia a povrchovej úpravy. Jemná priadza môže byť príjemná pri pokožke, no na miestach intenzívneho trenia potrebuje inú konštrukciu než hrubý vrchný sveter. Základy rozoberá článok <a href=\"/n/co-je-merino-vlna-vyhody-nevyhody-a-pranie-bez-zrazenia\">čo je merino vlna</a>.",
            "Polyamid, známy aj ako nylon, sa používa pre pevnosť, odolnosť a možnosť vytvoriť veľmi jemné filamenty. V ponožke môže spevniť pätu a špičku, v priadzi môže niesť vlnené vlákna a v úplete pomôcť stabilite. To neznamená, že každá zmes vydrží rovnaký oder alebo že sa nikdy nežmolkuje. Správanie polyamidu podrobnejšie vysvetľuje sprievodca <a href=\"/n/co-je-polyamid-alebo-nylon-vlastnosti-odolnost-a-pranie\">čo je polyamid</a>.",
            "Woolmark pri technológii core-spun opisuje polyamidový filament obalený vlnenými vláknami. Takéto usporiadanie môže zlepšiť pevnosť priadze, odolnosť proti oderu a parametre súvisiace so žmolkovaním. Je to konkrétny výrobný príklad, nie dôkaz, že každá bežná zmes na etikete má rovnakú konštrukciu alebo výsledok. Pri porovnaní výrobkov je preto užitočné pýtať sa nielen koľko polyamidu obsahujú, ale kde a ako je použitý.",
        ],
        "table1_heading": "Spôsoby použitia polyamidu vo vlnenom výrobku",
        "table1_intro": "Rovnaké percentuálne zloženie môže vzniknúť rozdielnym usporiadaním vlákien. Tabuľka vysvetľuje, čo jednotlivé možnosti znamenajú pre používanie a pranie.",
        "table1_headers": ["Konštrukcia", "Úloha polyamidu", "Typický výrobok", "Čo z etikety nezistíte"],
        "table1_rows": [
            ("Zmiešané strižové vlákna", "Polyamid je rozptýlený v priadzi spolu s vlnou.", "Sveter, úplet, deka.", "Dĺžku vlákien, krútenie priadze a skutočný povrch."),
            ("Nosné jadro priadze", "Jemný filament nesie alebo spevňuje vlnený obal.", "Ľahký úplet, technická merino vrstva.", "Či ide práve o túto technológiu a ako je skúšaná."),
            ("Zónové spevnenie", "Polyamid je najmä v miestach namáhania.", "Ponožka so spevnenou pätou a špičkou.", "Presné rozloženie môže byť uvedené len v technickom popise."),
            ("Samostatná podšívka alebo detail", "Tvorí inú vrstvu, lem alebo funkčný diel.", "Kabát, rukavice, čiapka.", "Najcitlivejší spôsob spojenia, výplň a príslušenstvo."),
        ],
        "sections": [
            (
                "Pevnosť priadze nie je totožná so životnosťou celého výrobku",
                [
                    "Spevnená priadza môže lepšie odolávať napätiu pri pletení aj používaní, ale hotový výrobok má švy, lemy, zipsy, gumu a plochy s odlišným trením. Ponožka sa môže prederaviť v šve alebo pri nechtovej hrane, hoci samotná priadza má dobrú laboratórnu odolnosť. Sveter môže stratiť tvar nevhodným sušením bez toho, aby sa vlákna mechanicky pretrhli.",
                    "Pri hodnotení trvácnosti preto oddeľte pevnosť priadze, odolnosť povrchu proti oderu, tvorbu žmolkov, rozmerovú stabilitu a pevnosť švov. Každý parameter používa inú skúšku. Číslo bez názvu metódy a podmienok nemožno zmysluplne porovnať s iným výrobkom. Praktický význam skúšok oderu rozoberá článok <a href=\"/n/odolnost-textilu-proti-oderu-co-znamena-martindale-pri-obleceni-a-bytovych-latkach\">čo znamená Martindale</a>.",
                ],
            ),
            (
                "Prečo zmes môže žmolkovať",
                [
                    "Žmolok vzniká, keď sa uvoľnené konce vlákien trením zamotajú na povrchu. Pevnejšie syntetické vlákno môže niekedy žmolok držať dlhšie, ale výsledok závisí od dĺžky vlákien, krútenia priadze, väzby a trenia pri nosení. Preto nie je presné tvrdiť, že polyamid žmolkovanie vždy spôsobí alebo mu vždy zabráni.",
                    "Najviac namáhané bývajú boky pod rukami, miesto pod popruhom tašky, manžety a vnútorné strany. Pri praní pomôže oddeliť jemný vlnený úplet od hrubých uterákov, zipsov a suchých zipsov. Existujúci postup <a href=\"/n/preco-sa-oblecenie-zmolkuje-vlakna-trenie-pranie-a-susenie\">prečo sa oblečenie žmolkuje</a> vysvetľuje aj bezpečné odstránenie žmolkov bez vytrhávania nosných vlákien.",
                ],
            ),
            (
                "Plstnatenie vlny: kombinácia povrchu, vlhkosti a mechaniky",
                [
                    "Vlnené vlákna majú šupinkovitý povrch. Pri vhodnej kombinácii vlhkosti, teploty, chemického prostredia a mechanického pohybu sa môžu vzájomne zachytávať a štruktúra sa zhutní. Výsledkom nemusí byť iba menší rozmer; výrobok môže byť tuhší, hrubší a menej pružný. Polyamidová zložka sama tento mechanizmus nevypína.",
                    "Úpravy označované ako machine washable alebo easy care môžu meniť správanie povrchu a umožniť určený strojový proces. Povolenie však musí byť uvedené na konkrétnom výrobku. Neodvodzujte ho z mäkkosti, názvu merino ani percenta syntetiky. Ak štítok povoľuje iba ručné pranie alebo profesionálne čistenie, zmesový pomer nie je dôvod na použitie programu v práčke.",
                ],
            ),
            (
                "Ako čítať štítok vlneno-polyamidovej zmesi",
                [
                    "Najprv skontrolujte symbol prania: preškrtnutá vaňa, ručné pranie, jemný alebo vlnový proces majú odlišný význam. Potom pozrite bielenie, sušenie, žehlenie a profesionálne ošetrenie. GINETEX zdôrazňuje, že symbol označuje najnáročnejší povolený proces pre hotový výrobok. Nižšia záťaž môže byť vhodná, ak stále zabezpečí potrebné vyčistenie.",
                    "Textové pokyny môžu dopĺňať symboly: prať naruby, prať samostatne, použiť prostriedok na vlnu alebo sušiť rozložené. Tieto informácie neignorujte. Vlnený kabát s polyamidovou podšívkou môže mať výstuž a lepené časti, ktoré znemožňujú domáce pranie. Ponožka so spevnenou špičkou môže naopak povoľovať strojový vlnový program.",
                ],
            ),
            (
                "Príprava na pranie: triedenie, škvrny a ochrana povrchu",
                [
                    "Vlnu triedite podľa farby, povoleného procesu a jemnosti. Jemný sveter neperte s rifľami, uterákmi ani odevmi so suchým zipsom. Zapnite zipsy na iných kompatibilných kusoch a vlnený výrobok obráťte naruby, ak to odporúča výrobca. Ochranné vrecko môže obmedziť zachytávanie, ale nesmie byť naplnené natesno.",
                    "Škvrnu riešte čo najskôr a lokálne bez silného trenia. Najprv odstráňte pevnú nečistotu a overte pokyny pre daný typ škvrny aj farbiva. Horúca voda, kefovanie a univerzálny odstraňovač môžu narušiť povrch alebo farbu. Neznámy drahý kus je rozumnejšie zveriť odbornej čistiarni než skúšať viac agresívnych zásahov za sebou.",
                ],
            ),
            (
                "Prací program, teplota a pohyb bubna",
                [
                    "Ak je strojové pranie povolené, zvoľte program určený pre vlnu alebo konkrétny jemný proces podľa štítku. Taký program zvyčajne pracuje s odlišným rytmom pohybu, množstvom vody a odstreďovaním než bežná syntetika. Názov programu medzi práčkami nie je úplne štandardizovaný, preto skontrolujte návod k spotrebiču a povolenú hmotnosť náplne.",
                    "Teplotu nastavte podľa symbolu a vyhnite sa prudkým neodporúčaným zmenám. Dôležité je aj odstreďovanie: príliš vysoké otáčky alebo nevhodná mechanika môžu deformovať ťažký mokrý úplet. Na druhej strane, veľmi mokrý sveter sa ťažšie prenáša bez natiahnutia. Použite nastavenie odporúčané výrobcom a po skončení kus podoprite oboma rukami.",
                ],
            ),
            (
                "Aký prací prostriedok použiť na vlnu s polyamidom",
                [
                    "Pri výrobku, ktorý vyžaduje prostriedok na vlnu, použite prípravok určený na tento účel a dávkujte ho podľa obalu, tvrdosti vody a náplne. Bežný gél pre bavlnu a syntetiku nie je automaticky vhodný pre vlnu iba preto, že zmes obsahuje polyamid. Rozhodujú pokyny na odeve aj na prípravku.",
                    "Nadmerné dávkovanie môže sťažiť oplach a zanechať nepríjemný povrch, kým nedostatočná dávka nemusí zvládnuť mastnotu. Aviváž nepridávajte automaticky; môže meniť dotyk a funkciu niektorých vrstiev. Bielidlo používajte len vtedy, ak ho povoľuje štítok a je kompatibilné s výrobkom. Pri neistote je bezpečnejšie zvoliť špecializovaný postup než improvizovať.",
                ],
            ),
            (
                "Sušenie svetra bez vytiahnutých ramien",
                [
                    "Mokrý vlnený úplet môže zadržať veľa vody a jeho vlastná hmotnosť ho natiahne. Po vybratí ho nezdvíhajte za rukáv ani výstrih. Jemne ho podoprite, bez krútenia odstráňte prebytočnú vodu spôsobom povoleným výrobcom a rozložte na suchú savú podložku. Upravte dĺžku, šírku, rukávy a švy do prirodzeného tvaru.",
                    "Počas sušenia zabezpečte prúdenie vzduchu a podľa potreby vymeňte mokrú podložku. Nedávajte výrobok priamo na horúci radiátor ani prudké slnko. Vešiak môže vytiahnuť ramená a dĺžku, preto ho použite iba vtedy, ak to konštrukcia a výrobca umožňujú. Bubnové sušenie patrí iba výrobkom s príslušným symbolom a programom.",
                ],
            ),
            (
                "Ponožky z merina a polyamidu: iné namáhanie než sveter",
                [
                    "Ponožka čelí potu, tlaku, opakovanému ohybu a oderu v topánke. Polyamid býva použitý práve preto, aby namáhanie lepšie zvládla. Napriek tomu ju treba prať podľa štítku, obrátiť naruby a chrániť pred suchým zipsom. Pár nespájajte tesnou gumičkou, ktorá môže dlhodobo deformovať lem.",
                    "Pred praním odstráňte piesok a drobné nečistoty, ktoré by pôsobili ako abrazívum. Ponožky nechajte po nosení vyschnúť a neuzatvárajte ich vlhké do vaku. Ak sú spevnené zóny viditeľne stenčené, ďalšie vysoké trenie pri praní už konštrukciu neobnoví. Oprava malej dierky včas môže predĺžiť používanie viac než agresívny pokus o odstránenie každého povrchového žmolku.",
                ],
            ),
            (
                "Skladovanie a obnova tvaru medzi noseniami",
                [
                    "Vlnený sveter ukladajte čistý, úplne suchý a poskladaný, aby sa nevyťahali ramená. Medzi noseniami ho nechajte vyvetrať a odpočinúť v prirodzenom tvare. To môže znížiť potrebu častého prania, ale nenahrádza vyčistenie pri viditeľnom znečistení, pote alebo pokynoch výrobcu. Pri dlhom skladovaní chráňte textil pred škodcami a kontrolujte uzavretý priestor.",
                    "Povrchové vlákna po nosení jemne uhladzujte vhodnou kefou alebo odžmolkovačom určeným na konkrétnu štruktúru. Postup najprv vyskúšajte na nenápadnom mieste a nepritláčajte. Vyčnievajúce očko nestrihajte naslepo, ak môže patriť k nosnej priadzi. Pri drahom úplete je odborná oprava bezpečnejšia než zásah, ktorý otvorí väčšiu dieru.",
                ],
            ),
        ],
        "notes": {
            3: (
                "Čo polyamid zmení a čo nezaručí",
                [
                    "Môže zvýšiť pevnosť priadze alebo odolnosť namáhaných zón.",
                    "Nezaručí automaticky strojové pranie ani sušičku.",
                    "Nevypína mechanizmus plstnatenia vlneného povrchu.",
                    "Neurčuje sám mäkkosť, žmolkovanie ani životnosť švov.",
                ],
                "#f7fbf8",
                "#dbe5de",
            ),
            7: (
                "Bezpečné poradie rozhodovania",
                [
                    "Najprv symboly a textové pokyny hotového výrobku.",
                    "Potom určenie programu a maximálna náplň v návode práčky.",
                    "Následne kompatibilný prací prostriedok a jeho správna dávka.",
                    "Napokon sušenie s oporou a úpravou do pôvodného tvaru.",
                ],
                "#fffaf5",
                "#e6ded2",
            ),
        },
        "table2_heading": "Starostlivosť podľa typu vlneno-polyamidového výrobku",
        "table2_intro": "Tabuľka ponúka orientačné rozdiely. Presný štítok môže pre konkrétny výrobok stanoviť prísnejší alebo odlišný postup.",
        "table2_headers": ["Výrobok", "Najväčšie namáhanie", "Pred praním", "Po praní"],
        "table2_rows": [
            ("Merino spodná vrstva", "Pot, popruhy, trenie vrstiev a časté nosenie.", "Obrátiť naruby, oddeliť zipsy a skontrolovať škvrny.", "Vyrovnať a sušiť podľa štítku bez prudkého tepla."),
            ("Sveter", "Lakte, podpazušie, popruh tašky a hmotnosť za mokra.", "Prať samostatne alebo s jemnou vlnou v malej náplni.", "Sušiť rozložený v tvare, ak výrobca neurčí inak."),
            ("Ponožky", "Päta, špička, pot, piesok a trenie topánky.", "Vytriasť nečistoty, obrátiť naruby a oddeliť suchý zips.", "Dosušiť úplne pred uložením, neprehrievať lem."),
            ("Čiapka alebo rukavice", "Pot, kozmetika, okraj a tvarované švy.", "Overiť podšívku, brmbolec, membránu a ozdoby.", "Obnoviť tvar a sušiť s dostatkom vzduchu."),
            ("Kabát alebo konštruovaný kus", "Podšívka, výstuž, lepidlá a lokálne škvrny.", "Nerozhodovať len podľa zloženia vrchnej látky.", "Dodržať profesionálne ošetrenie, ak ho vyžaduje symbol."),
        ],
        "steps": [
            "Zistite, či je polyamid premiešaný v priadzi, použitý ako jadro, zónové spevnenie alebo samostatná vrstva.",
            "Prečítajte všetky ošetrovacie symboly a textové pokyny; zloženie nepoužívajte ako náhradu štítku.",
            "Oddeľte jemnú vlnu od hrubých a ostrých povrchov, zapnite zipsy a lokálne škvrny netrite.",
            "Ak je pranie povolené, vyberte určený vlnový alebo jemný program a dodržte maximálnu náplň.",
            "Použite kompatibilný prostriedok v správnej dávke a nepridávajte automaticky aviváž či bielidlo.",
            "Mokrý výrobok podoprite, upravte do tvaru a sušte spôsobom uvedeným na štítku.",
            "Po úplnom vysušení skontrolujte povrch, švy a namáhané zóny pred ďalším nosením alebo uložením.",
        ],
        "remember": [
            "Polyamid môže spevniť vlnený výrobok, ale neprepisuje pravidlá starostlivosti o vlnu.",
            "Strojové pranie musí povoľovať štítok; nedá sa odvodiť z percenta syntetiky.",
            "Žmolkovanie, oder, pevnosť a rozmerová stabilita sú rozdielne vlastnosti.",
            "Mokrý sveter prenášajte s oporou a sušte v tvare, aby ho nenaťahovala vlastná hmotnosť.",
            "Konkrétny výrobok môže obsahovať podšívku, gumu, membránu alebo lepidlo s prísnejším limitom.",
        ],
        "mistakes": [
            "<strong>Program na syntetiku len preto, že zmes obsahuje polyamid.</strong> Vlnený povrch môže stále vyžadovať výrazne šetrnejšiu mechaniku.",
            "<strong>Horúca voda na odolné merino ponožky.</strong> Odolnosť proti oderu neznamená odolnosť proti plstnateniu alebo teplu.",
            "<strong>Vešanie mokrého svetra.</strong> Hmotnosť vody môže vytiahnuť dĺžku a ramená bez pretrhnutia vlákna.",
            "<strong>Silné trenie škvrny.</strong> Môže zmatnieť povrch, podporiť plstnatenie a poškodiť farbu.",
            "<strong>Porovnávanie zmesí iba podľa percent.</strong> Umiestnenie polyamidu a konštrukcia priadze môžu byť dôležitejšie.",
            "<strong>Zámena žmolkovania za predratie.</strong> Ide o odlišné javy a vyžadujú iné hodnotenie.",
        ],
        "expert_heading": "Ako konštrukcia priadze mení výsledok zmesi",
        "expert": [
            "Technológia Woolmark core-spun používa jemný polyamidový filament, okolo ktorého sú obalené vlnené vlákna. Podľa opisu technológie môže taká priadza priniesť vyššiu pevnosť, odolnosť proti oderu a lepšie výsledky súvisiace so žmolkovaním pri zachovaní vlneného povrchu. Ide o špecifickú konštrukciu s vlastnými skúškami, nie o automatickú vlastnosť každej etikety vlna/polyamid.",
            "ASTM uvádza pre textílie samostatné metódy na oder, pevnosť, pružnosť a ďalšie parametre. Ak výrobca zverejní výsledok, treba poznať skúšobnú metódu, ukončovacie kritérium a porovnávanú zostavu. Výsledok samotnej látky nemusí zahŕňať šev, podšívku alebo reálne opakované pranie. Preto je presnejšie hovoriť o konkrétnom meranom parametri než o všeobecnej nezničiteľnosti zmesi.",
            "Woolmark vo svojich návodoch na starostlivosť rozlišuje strojovo prateľnú vlnu, ručné pranie a ďalšie spôsoby podľa označenia výrobku. To potvrdzuje praktické pravidlo: konštrukčná výhoda polyamidu a povolený proces prania sú dve samostatné informácie. Pri domácej údržbe má rozhodujúce slovo štítok hotového kusa.",
        ],
        "source_intro": "Zdroje vysvetľujú konkrétnu spevnenú priadzu, skúšanie textílií a pravidlá starostlivosti o vlnu. Výsledky jednej technológie nemožno automaticky pripísať každej zmesi vlna/polyamid.",
        "sources": [
            ("Woolmark: technológia vlneného core-spun vlákna", WOOLMARK_CORE),
            ("Woolmark: návody na starostlivosť o vlnu", WOOLMARK_CARE),
            ("ASTM: prehľad skúšok textílií", ASTM_TEXTILES),
            ("GINETEX: význam ošetrovacích symbolov", GINETEX),
        ],
        "product_intro": "Bežný prací gél môže byť vhodný pre niektoré prateľné zmesi, ale vlnený výrobok často vyžaduje prostriedok určený na vlnu. Pred výberom produktu preto najprv prečítajte štítok a návod výrobcu.",
        "product_text": "Hypoalergénny prací gél Vevo Ylang Absolute je možnosť pre bežnú domácu bielizeň, ak ho dovoľuje materiál aj štítok. Pri vlne ho nepoužívajte ako náhradu špecializovaného prípravku bez overenia kompatibility.",
        "product_limit": "Pri vlne, merine, kašmíre a výrobkoch označených iba na ručné alebo profesionálne čistenie má prednosť špecializovaný postup. Polyamid v zmesi toto obmedzenie automaticky neruší.",
        "category_intro": "V kategórii pracích gélov porovnávajte určenie produktu a pokyny na obale. Na vlnený kus vyberte iba prípravok, ktorý je s vlnou preukázateľne kompatibilný a zodpovedá štítku.",
        "category_text": "Bežné pracie gély sú určené na rôzne druhy domácej bielizne. Pri citlivej vlne si overte, či potrebujete samostatný prostriedok na vlnu a jemné textílie.",
        "related": [
            ("Čo je merino vlna", ARTICLE_MERINO),
            ("Čo je polyamid", ARTICLE_POLYAMIDE),
            ("Prečo sa oblečenie žmolkuje", ARTICLE_PILLING),
            ("Čo je zmesový materiál", ARTICLE_BLEND),
            ("Čo znamená Martindale", ARTICLE_MARTINDALE),
            ("Bavlna a elastan", ARTICLE_COTTON_ELASTANE),
        ],
        "faq_title": "vlna a polyamid",
        "faq": [
            ("Prečo sa do merina pridáva polyamid?", "Najčastejšie pre spevnenie priadze alebo namáhaných zón a zvýšenie odolnosti. Konkrétny prínos závisí od toho, či je polyamid v jadre priadze, premiešaný vo vláknach alebo umiestnený iba v určitých častiach."),
            ("Môže sa vlna s polyamidom prať v práčke?", "Iba ak to povoľuje ošetrovací štítok. Samotný podiel polyamidu nie je povolením; výrobok môže byť strojovo prateľný, určený na ručné pranie alebo iba na profesionálne čistenie."),
            ("Na akom programe prať merino ponožky s polyamidom?", "Na programe a teplote uvedenej na štítku, často na určenom programe na vlnu. Skontrolujte aj maximálnu náplň programu, vhodný prostriedok a povolené odstreďovanie."),
            ("Zabráni polyamid žmolkovaniu vlny?", "Nie automaticky. Konkrétna konštrukcia môže výsledky zlepšiť, no žmolkovanie závisí aj od dĺžky vlákien, priadze, väzby, trenia pri nosení a prania."),
            ("Môže ísť vlneno-polyamidový sveter do sušičky?", "Len s príslušným symbolom. Väčšinu citlivých úpletov je bezpečné sušiť rozloženú v tvare podľa pokynov výrobcu, pretože teplo a pohyb môžu podporiť zmenu rozmerov."),
            ("Je zmes vlna a polyamid menej teplá než čistá vlna?", "Nedá sa to určiť iba z percent. Tepelný pocit mení gramáž, hrúbka, množstvo zachyteného vzduchu, úplet, strih, vlhkosť aj umiestnenie polyamidu."),
        ],
    },
    {
        "title": "Statická elektrina v oblečení: prečo látky priľnú a ako obmedziť iskrenie",
        "link": "staticka-elektrina-v-obleceni-preco-latky-prilnu-a-ako-obmedzit-iskrenie",
        "meta": "Prečo oblečenie elektrizuje, priľne k telu alebo iskrí po sušičke. Praktické riešenia pre syntetiku, suchý vzduch, vrstvenie aj pranie.",
        "short": "Statická elektrina vzniká pri kontakte a oddeľovaní povrchov, najmä keď je vzduch suchý a bielizeň presušená. Sprievodca vysvetľuje materiály, sušičku, vrstvenie, bezpečné riešenia aj situácie, keď domáci trik nestačí.",
        "answer": "Oblečenie sa elektrizuje, keď sa pri kontakte a následnom oddelení povrchov presunie elektrický náboj a nemá sa rýchlo rozptýliť. Problém býva výraznejší pri suchom vzduchu, kombinácii rozdielnych materiálov a dlhom dosúšaní už suchej bielizne. Najprv ukončite sušenie včas, oddeľte problematické syntetické kusy, dodržte správne pranie a oplach a skontrolujte vlhkosť prostredia. Aviváž alebo antistatický prípravok použite iba vtedy, ak ho povoľuje štítok a nepoškodí funkciu textilu.",
        "intro": "Sukňa sa lepí na pančuchy, tričko praská pri vyzliekaní a po dotyku kľučky preskočí malá iskra. Tieto prejavy majú spoločný základ, ale nevznikajú vždy z jedného materiálu ani z chyby práčky. Dôležitý je pár povrchov, trenie a oddeľovanie, vodivosť materiálu, relatívna vlhkosť, obuv, podlaha aj spôsob sušenia. Preto funguje lepšie systematická kontrola podmienok než jeden univerzálny domáci trik.",
        "quick": [
            "<strong>Kontakt a oddelenie:</strong> náboj sa môže presunúť medzi dvoma povrchmi a po ich oddelení zostať na horšie vodivom materiáli.",
            "<strong>Suché prostredie:</strong> pri nízkej vlhkosti sa náboj na mnohých povrchoch rozptyľuje pomalšie, preto je problém častejší počas vykurovacej sezóny.",
            "<strong>Presušenie v sušičke:</strong> suché kusy sa ďalej trú a oddeľujú bez vlhkosti, ktorá by pomáhala náboj odvádzať.",
            "<strong>Rozhoduje dvojica materiálov:</strong> jedna látka nemusí elektrizovať vždy; výsledok sa zmení s ďalšou vrstvou, obuvou alebo poťahom.",
            "<strong>Bezpečnosť má hranice:</strong> pri horľavých parách, medicínskych zariadeniach alebo citlivej elektronike sa riaďte odbornými pravidlami pracoviska.",
        ],
        "overview_heading": "Ako vzniká statický náboj na textile",
        "overview": [
            "Pri kontakte dvoch materiálov sa ich povrchy dostanú na veľmi malú vzdialenosť. Po oddelení môže zostať na jednom povrchu prebytok a na druhom nedostatok elektrického náboja. Trenie zvyšuje počet kontaktov a oddeľovaní, ale nie je jedinou podmienkou. Odev sa môže nabiť pri chôdzi, vyzliekaní, pohybe v sušičke, trení o sedadlo alebo vrstvení sukne a pančúch.",
            "Textil a bežná obuv často neodvádzajú náboj okamžite. Ak sa človek následne dotkne vodivého predmetu s iným potenciálom, náboj sa môže rýchlo vyrovnať a vznikne citeľná iskra. Malý domáci výboj je väčšinou krátke nepohodlie, no presnú energiu nemožno posúdiť iba pocitom. V prostredí s horľavou atmosférou sa statická elektrina rieši osobitnými technickými a bezpečnostnými postupmi.",
            "Voda a vlhkosť menia elektrické vlastnosti povrchov, ale účinok nie je rovnaký pri každom vlákne a úprave. Historické merania NIST ukazujú vzťah medzi obsahom vlhkosti a elektrickým odporom rôznych textilných materiálov. Moderné skúšky preto kondicionujú vzorky v stanovenom prostredí. Domáce pozorovanie v suchom januári sa nedá priamo porovnať s vlhkým letným dňom.",
        ],
        "table1_heading": "Najčastejšie situácie, v ktorých oblečenie elektrizuje",
        "table1_intro": "Statický náboj vzniká v systéme materiálov a prostredia. Tabuľka pomáha nájsť podmienku, ktorú možno bezpečne zmeniť ako prvú.",
        "table1_headers": ["Situácia", "Čo podporuje náboj", "Prvý rozumný krok", "Čo ešte overiť"],
        "table1_rows": [
            ("Bielizeň po sušičke", "Dlhé trenie už suchých kusov a nízka zvyšková vlhkosť.", "Skrátiť dosúšanie alebo použiť senzorický program podľa návodu.", "Čistotu snímačov, náplň, program a povolenie štítku."),
            ("Sukňa a pančuchy", "Opakovaný kontakt dvoch rozdielnych povrchov pri chôdzi.", "Zmeniť jednu vrstvu alebo použiť kompatibilné antistatické riešenie.", "Podšívku, obuv, vlhkosť vzduchu a citlivosť materiálu."),
            ("Deka, posteľná bielizeň", "Veľká plocha kontaktu, suché vlákna a trenie pri skladaní.", "Nepresúšať, skladať po vychladnutí a vetrať miestnosť.", "Zloženie oboch vrstiev a povolený spôsob prania."),
            ("Výboj pri kľučke", "Nabitie človeka pri chôdzi a izolujúca obuv alebo podlaha.", "Pred citlivou elektronikou sa dotknúť uzemneného vodivého bodu podľa pravidiel pracoviska.", "Koberec, podrážku, pracovné prostredie a ochranné postupy."),
        ],
        "sections": [
            (
                "Prečo sa statická elektrina zhoršuje v zime",
                [
                    "Počas vykurovania býva vnútorný vzduch suchší. Na mnohých materiáloch sa potom vytvorí menej vodivá povrchová vrstva a náboj sa rozptyľuje pomalšie. Súčasne nosíme viac vrstiev, ktoré sa pri pohybe dotýkajú a oddeľujú. Výsledkom je častejšie priľnutie látky aj malé výboje, hoci prací postup zostal rovnaký.",
                    "Zvýšenie vlhkosti nie je bezpodmienečný cieľ. Nadmerná vlhkosť môže podporovať kondenzáciu a plesne a nie je vhodná pre každú budovu ani zdravotný stav. Vlhkosť merajte spoľahlivým vlhkomerom, vetrajte a riaďte sa podmienkami miestnosti. Zvlhčovač udržiavajte podľa návodu, aby sa nestal zdrojom usadenín alebo mikrobiálneho znečistenia.",
                ],
            ),
            (
                "Ktoré materiály elektrizujú najviac",
                [
                    "Nedá sa vytvoriť jedno poradie platné pre všetky odevy. Nabíjanie závisí od dvojice povrchov, ich čistoty, vlhkosti, úpravy, tlaku a rýchlosti oddelenia. Syntetické vlákna ako polyester či polyamid často držia náboj dlhšie v suchom prostredí, no aj vlna, podšívka, koberec a obuv môžu byť súčasťou problémovej kombinácie.",
                    "Pri pátraní zmeňte iba jednu premennú. Skúste rovnakú sukňu s inými pančuchami alebo inú spodnú vrstvu pod syntetickým svetrom. Ak problém zmizne, našli ste významný pár materiálov. Základy syntetických vlákien a prania nájdete v návode <a href=\"/n/ako-prat-syntetiku-polyester-a-elastan-aby-nezapachali-a-drzali-tvar\">ako prať syntetiku</a>.",
                ],
            ),
            (
                "Sušička a statický náboj: prečo rozhodujú posledné minúty",
                [
                    "Kým bielizeň obsahuje vodu, jej povrchové elektrické vlastnosti sa líšia od úplne suchého stavu. Ak sušička pokračuje dlho po vysušení, kusy sa stále prevaľujú, trú a oddeľujú. To vytvára vhodné podmienky na nabíjanie. Najpraktickejším krokom je preto nepresúšať, nie pridávať čoraz viac prípravkov.",
                    "Použite program a hmotnosť náplne z návodu spotrebiča, vyčistite filtre a overte snímače vlhkosti podľa pokynov výrobcu. Rôzne hrubé kusy schnú nerovnomerne: tenká syntetika môže byť presušená, kým hrubý bavlnený lem zostáva vlhký. Rozdelenie náplne podľa hrúbky môže zlepšiť výsledok aj skrátiť čas zbytočného pohybu.",
                ],
            ),
            (
                "Pomôže aviváž proti elektrizovaniu",
                [
                    "Niektoré aviváže a antistatické prípravky menia povrchové trenie alebo elektrické vlastnosti vlákien, takže môžu statické priľnutie obmedziť. Účinok však závisí od materiálu, dávky, oplachu a spôsobu sušenia. Produkt používajte iba podľa návodu a neprekračujte dávku v snahe dosiahnuť silnejší účinok.",
                    "Aviváž nie je vhodná automaticky na funkčné športové oblečenie, mikrovlákno, uteráky alebo výrobky so špeciálnou úpravou. Povrchový film môže meniť savosť, odvod vlhkosti alebo ďalšiu funkciu. Pred použitím skontrolujte štítok a pokyny výrobcu. Ak je príčinou presušenie alebo nevhodná kombinácia vrstiev, produkt rieši iba časť mechanizmu.",
                ],
            ),
            (
                "Pranie a oplach: čo môžu zmeniť zvyšky na povrchu",
                [
                    "Mastnota, kozmetika, nevypláchnutý prací prostriedok aj nadmerná vrstva ďalšieho prípravku menia trenie a správanie povrchu. Neexistuje však jednoduché pravidlo, že každý zvyšok statiku zvýši alebo zníži. Ak bielizeň po praní pôsobí lepkavo, klzko alebo nezvyčajne tuho, opravte najprv dávkovanie, náplň a oplach.",
                    "Práčku neprepĺňajte a dávku stanovte podľa tvrdosti vody, množstva a znečistenia. Pri hustých syntetických vrstvách nechajte dostatok priestoru na pretekanie roztoku. Extra plákanie môže byť užitočné pri konkrétnej situácii, no nie je náhradou za správnu dávku. Čistite dávkovač, filter a bubon podľa návodu spotrebiča.",
                ],
            ),
            (
                "Ako obmedziť priľnutie sukne, šiat a podšívky",
                [
                    "Najprv identifikujte dvojicu povrchov: sukňa a pančuchy, šaty a spodná vrstva alebo podšívka a pokožka. Skúste zmeniť materiál jednej vrstvy a obmedziť nadmerné trenie. Odev po praní nepresúšajte a pred oblečením ho nechajte vychladnúť v bežnom prostredí. Veľmi suchá pokožka môže meniť komfort, no kozmetiku nanášajte s predstihom a nechajte ju vstrebať, aby nezanechala škvrny.",
                    "Antistatický sprej použite iba na materiál, pre ktorý je určený, v množstve z návodu a najprv na nenápadnom mieste. Nestriekajte neznámy prípravok na hodváb, citlivú farbu alebo povrchovú úpravu. Vodu nerozprašujte na odev ako univerzálny trik tesne pred kontaktom s elektrickým zariadením; môže vytvoriť škvrny a nejde o kontrolovanú ochranu.",
                ],
            ),
            (
                "Posteľná bielizeň, deky a vlasy",
                [
                    "Veľké plochy posteľných textílií sa pri pohybe opakovane dotýkajú a oddeľujú. Ak je syntetická deka alebo obliečka veľmi suchá, pri skladaní môže praskať a priťahovať vlasy či prach. Perte a sušte ju podľa štítku, nepreťažujte sušičku a ukončite cyklus bez zbytočného presúšania. Kombináciu materiálu prikrývky a obliečky hodnotíte ako pár.",
                    "Statický náboj môže priťahovať ľahké vlákna a vlasy, ale nie je jedinou príčinou prachu na textile. Dôležité je aj uvoľňovanie vlákien, prúdenie vzduchu, upratovanie a povrchová štruktúra. Mikrovlákno preto neperte s výrazne púšťajúcimi bavlnenými kusmi. Samostatný materiálový základ ponúka článok <a href=\"/n/co-je-zmesovy-material-preco-sa-oblecenie-zraza-alebo-sprava-inak-nez-cakate\">ako sa správajú zmesové materiály</a>.",
                ],
            ),
            (
                "Obuv, koberec a čalúnenie môžu byť dôležitejšie než tričko",
                [
                    "Človek sa môže nabiť chôdzou po koberci alebo vstávaním zo sedadla a výboj pocíti až pri dotyku kľučky. V takej situácii posledné oblečené tričko nemusí byť hlavnou príčinou. Sledujte, či sa problém objavuje iba v jednej miestnosti, pri konkrétnej obuvi, na určitom kresle alebo počas vykurovacej sezóny.",
                    "V domácnosti nerobte neodborné zásahy do elektrickej inštalácie ani improvizované uzemňovanie spotrebičov. Pri opakovaných nezvyčajne silných výbojoch zo zariadenia, poškodenom kábli alebo podozrení na elektrickú poruchu zariadenie odpojte bezpečným spôsobom a kontaktujte kvalifikovaného technika. Statická elektrina z oblečenia nesmie zakryť reálnu poruchu.",
                ],
            ),
            (
                "Bezpečný domáci test príčiny bez poškodenia odevu",
                [
                    "Počas niekoľkých nosení si poznačte miestnosť, počasie, vrstvy, obuv a spôsob posledného sušenia. Zmeňte vždy iba jednu podmienku: napríklad vyberte bielizeň zo sušičky skôr alebo nahraďte jednu syntetickú vrstvu. Ak sa prejav výrazne zmení, máte užitočnejšiu stopu než po náhodnom použití viacerých prípravkov naraz.",
                    "Odev nepoškodzujte skúšaním elektrických zariadení, vysokého napätia ani neznámych chemikálií. Domáca diagnostika má sledovať bežné používanie, nie reprodukovať laboratórnu skúšku. Normy ISO a AATCC používajú definované napätie, trenie, kondicionovanie a meracie prístroje, aby boli výsledky porovnateľné a bezpečné.",
                ],
            ),
            (
                "Kedy už nejde iba o nepríjemnosť pri obliekaní",
                [
                    "V priestore s horľavými plynmi, parami, prachom alebo citlivou výrobou môže byť elektrostatický výboj rizikom. Riešením nie je domáci sprej ani zmena aviváže, ale posúdenie pracoviska, vhodné oblečenie, obuv, podlaha, uzemnenie a kontrolné postupy podľa zodpovednej osoby. Bežný módny odev nie je automaticky ochranný antistatický odev.",
                    "Pri práci s citlivou elektronikou používajte schválené pracovné ochranné pomôcky a postupy ESD. Pri zdravotníckych pomôckach alebo implantovaných zariadeniach sa riaďte pokynmi výrobcu a zdravotníka, nie všeobecným článkom. Ak výboj sprevádza zápach po spálení, dym, prehrievanie alebo poškodenie spotrebiča, prestaňte ho používať a riešte elektrickú bezpečnosť.",
                ],
            ),
        ],
        "notes": {
            3: (
                "Najprv upravte proces sušenia",
                [
                    "Rozdeľte veľmi rozdielne hrubé kusy, aby tenké neboli dlho presušené.",
                    "Použite správnu náplň a senzorický program podľa návodu sušičky.",
                    "Vyčistite filtre a snímače spôsobom určeným výrobcom.",
                    "Prípravok pridávajte až po overení štítku a príčiny problému.",
                ],
                "#f7fbf8",
                "#dbe5de",
            ),
            7: (
                "Čo nerobiť pri elektrizujúcej bielizni",
                [
                    "Nezvyšujte ľubovoľne dávku aviváže alebo pracieho gélu.",
                    "Nestriekajte neznámu zmes na citlivé látky a elektroniku.",
                    "Neimprovizujte zásahy do uzemnenia zásuviek a spotrebičov.",
                    "Nezamieňajte opakovaný výboj zo zariadenia za bežnú statiku odevu.",
                ],
                "#fffaf5",
                "#e6ded2",
            ),
        },
        "table2_heading": "Riešenia zoradené od najmenšieho zásahu",
        "table2_intro": "Začnite opatrením, ktoré nemení povrch textilu. Chemický prípravok má zmysel až po kontrole kompatibility a dávky.",
        "table2_headers": ["Opatrenie", "Kedy pomáha", "Obmedzenie", "Kontrola"],
        "table2_rows": [
            ("Ukončiť sušenie včas", "Keď statika vzniká najmä po dlhom horúcom cykle.", "Bielizeň musí zostať skutočne dosušená podľa účelu.", "Senzor, hrúbka kusov a čistota filtrov."),
            ("Zmeniť kombináciu vrstiev", "Keď sa konkrétna sukňa lepí na konkrétne pančuchy.", "Nemusí vyriešiť náboj z koberca alebo sedadla.", "Materiál podšívky, spodnej vrstvy a obuvi."),
            ("Upraviť vnútorné prostredie", "Keď sa problém objavuje najmä v suchom vykurovanom byte.", "Nevytvárať nadmernú vlhkosť ani kondenzáciu.", "Vlhkomer, vetranie a stav zvlhčovača."),
            ("Antistatický alebo avivážny produkt", "Keď je kompatibilný s textilom a problém pretrváva.", "Môže meniť savosť alebo funkčný povrch.", "Štítok, návod, dávka a skúška na nenápadnom mieste."),
            ("Odborné ESD opatrenia", "Pri horľavinách, elektronike a riadenom pracovisku.", "Nie sú nahraditeľné domácim trikom.", "Rizikové posúdenie a interné bezpečnostné pravidlá."),
        ],
        "steps": [
            "Určite, či problém vzniká po sušičke, pri konkrétnom vrstvení, na koberci alebo iba v jednej miestnosti.",
            "Skontrolujte štítky všetkých dotýkajúcich sa vrstiev a spôsob ich posledného prania a sušenia.",
            "Pri sušičke rozdeľte rozdielne hrúbky, použite správnu náplň a ukončite cyklus bez presušenia.",
            "Overte dávkovanie, oplach a čistotu práčky; nezvyšujte množstvo produktov naslepo.",
            "Zmeňte jednu vrstvu alebo obuv a sledujte, či sa prejav opakovane zníži.",
            "Vlhkosť prostredia merajte a upravujte iba v bezpečnom rozsahu pre budovu a používateľov.",
            "V rizikovom pracovnom prostredí prestaňte experimentovať a použite schválený ESD postup.",
        ],
        "remember": [
            "Statika je výsledkom kontaktu materiálov, prostredia a možnosti odviesť náboj.",
            "Najjednoduchším zásahom po sušičke býva obmedzenie presušenia.",
            "Jedna látka nemá nemennú hodnotu elektrizovania vo všetkých kombináciách.",
            "Aviváž ani sprej nie sú vhodné automaticky na každý funkčný alebo savý textil.",
            "Horľavé prostredie a citlivá elektronika vyžadujú odbornú ochranu, nie domáci experiment.",
        ],
        "mistakes": [
            "<strong>Pridanie dvojnásobnej dávky aviváže.</strong> Môže zanechať nános a poškodiť funkciu bez vyriešenia presušenia.",
            "<strong>Hľadanie jedného vinného vlákna.</strong> Výsledok určuje pár povrchov, vlhkosť, trenie, obuv a okolie.",
            "<strong>Veľmi dlhé dosúšanie pre istotu.</strong> Suché kusy sa ďalej trú a môžu sa nabiť výraznejšie.",
            "<strong>Rozprašovanie vody alebo neznámej zmesi na citlivý odev.</strong> Hrozia škvrny, poškodenie povrchu a nepredvídateľný výsledok.",
            "<strong>Ignorovanie podlahy a obuvi.</strong> Výboj pri kľučke môže vzniknúť chôdzou po koberci, nie praním trička.",
            "<strong>Domáce riešenie v rizikovom pracovisku.</strong> Ochrana pred ESD musí vychádzať z technických pravidiel a posúdenia rizika.",
        ],
        "expert_heading": "Ako sa elektrostatické vlastnosti textilu skúšajú",
        "expert": [
            "ISO 18080-3 opisuje skúšku náboja vytvoreného ručným trením textílie. Norma stanovuje skúšobný postup, pretože výsledok závisí od materiálu, protimateriálu, podmienok a merania. Domáce praskanie pri vyzliekaní nie je číselne porovnateľné s laboratórnym výsledkom bez rovnakého kondicionovania a metódy.",
            "ISO 24180 sa venuje elektrickému odporu syntetických filamentových priadzí. AATCC TM115 hodnotí elektrostatickú priľnavosť textílií. Už samotná existencia odlišných metód ukazuje, že elektrický odpor, vytvorený náboj a viditeľné priľnutie nie sú jedna vlastnosť meraná jediným univerzálnym číslom.",
            "NIST skúmal vzťah vlhkosti a elektrického odporu textilných materiálov. Hoci ide o historickú prácu a dnešné odevy používajú nové úpravy a zmesi, podporuje základný mechanizmus: obsah vlhkosti môže výrazne meniť schopnosť povrchu odvádzať náboj. Preto sa pri odbornej skúške kontroluje atmosféra a pri domácej diagnostike treba zaznamenať sezónu a prostredie.",
        ],
        "source_intro": "Zdroje opisujú rozdielne skúšky náboja, elektrického odporu a priľnavosti. Ich výsledky sú viazané na definované materiály a podmienky; nejde o univerzálny rebríček všetkých odevov.",
        "sources": [
            ("ISO 18080-3: náboj po ručnom trení textílie", ISO_STATIC),
            ("ISO 24180: elektrický odpor syntetických filamentových priadzí", ISO_STATIC_FILAMENT),
            ("AATCC TM115: elektrostatická priľnavosť textílií", AATCC_STATIC),
            ("NIST: vlhkosť a elektrický odpor textilných materiálov", NIST_MOISTURE),
        ],
        "product_intro": "Správne pranie a oplach udržiavajú povrch bez zbytočných nánosov, no prací gél nie je samostatným antistatickým riešením. Najprv upravte sušenie, kombináciu materiálov a podmienky prostredia.",
        "product_text": "Hypoalergénny prací gél Vevo Ylang Absolute možno použiť na bežné prateľné textílie, ak ho povoľuje štítok. Dodržte dávku podľa tvrdosti vody a náplne a nechajte bielizeň dôkladne opláchnuť.",
        "product_limit": "Produkt nenahrádza antistatický prípravok, odborné ESD oblečenie ani bezpečnostné opatrenia. Pri funkčných materiáloch, vlne a špeciálnych úpravách overte kompatibilitu osobitne.",
        "category_intro": "Pri porovnávaní pracích gélov sledujte určenie a dávkovanie, nie množstvo peny alebo intenzitu vône. Elektrizovanie po sušičke sa často rieši úpravou cyklu, nie zmenou gélu.",
        "category_text": "Kategória pracích gélov ponúka produkty pre bežnú domácu bielizeň. Vyberajte podľa farieb, materiálu, znečistenia a citlivosti používateľa a vždy rešpektujte štítok.",
        "related": [
            ("Ako prať syntetiku", ARTICLE_SYNTHETIC),
            ("Čo je polyamid", ARTICLE_POLYAMIDE),
            ("Čo je elastan", ARTICLE_ELASTANE),
            ("Čo je zmesový materiál", ARTICLE_BLEND),
            ("Bavlna a elastan", ARTICLE_COTTON_ELASTANE),
            ("Čo znamená Martindale", ARTICLE_MARTINDALE),
        ],
        "faq_title": "statická elektrina v oblečení",
        "faq": [
            ("Prečo oblečenie elektrizuje najmä v zime?", "Vykurovaný vzduch býva suchší, náboj sa na mnohých povrchoch rozptyľuje pomalšie a nosíme viac vrstiev, ktoré sa pri pohybe dotýkajú a oddeľujú."),
            ("Ako odstrániť statickú elektrinu z bielizne po sušičke?", "Najprv obmedzte presušenie: použite správnu náplň, program podľa návodu, čisté filtre a funkčné snímače. Veľmi rozdielne hrubé kusy sušte oddelene, aby tenké neostali v bubne zbytočne dlho."),
            ("Pomáha aviváž proti elektrizovaniu?", "Niektoré produkty môžu statické priľnutie znížiť, ale nie sú vhodné na každý textil. Skontrolujte štítok, funkciu materiálu a dávku; pri uterákoch či športových vrstvách môže povrchový film meniť požadované vlastnosti."),
            ("Prečo sukňa priľne k pančuchám?", "Pri chôdzi sa dva povrchy opakovane dotýkajú a oddeľujú. Výsledok mení ich materiál, podšívka, suchosť vzduchu, pokožka, obuv aj ďalšie vrstvy."),
            ("Je malá iskra pri kľučke nebezpečná?", "V bežnej domácnosti býva krátky výboj najmä nepríjemný. V prostredí s horľavými parami, prachom, citlivou elektronikou alebo osobitnými zdravotníckymi zariadeniami však platia špecializované bezpečnostné pravidlá."),
            ("Pomôže zvýšiť vlhkosť vzduchu?", "Môže pomôcť, ak je prostredie veľmi suché, ale vlhkosť najprv zmerajte a nevytvárajte kondenzáciu. Zvlhčovač udržiavajte podľa návodu a zohľadnite stav budovy aj zdravie obyvateľov."),
        ],
    },
    {
        "title": "Odolnosť textilu proti oderu: čo znamená Martindale pri oblečení a bytových látkach",
        "link": "odolnost-textilu-proti-oderu-co-znamena-martindale-pri-obleceni-a-bytovych-latkach",
        "meta": "Čo meria Martindale, ako čítať počet cyklov a prečo nie je univerzálnym skóre kvality. Oder oblečenia, čalúnenia, žmolky aj údržba.",
        "short": "Martindale je štandardizovaný spôsob hodnotenia oderu textilu, nie jedno univerzálne skóre kvality. Sprievodca vysvetľuje cykly, kritériá ukončenia, rozdiel oproti žmolkovaniu a praktický význam pre odevy aj čalúnenie.",
        "answer": "Martindale označuje skúšku, pri ktorej sa textilná vzorka v definovanom pohybe trie o určený abrazívny materiál pod stanoveným zaťažením. Výsledkom môže byť počet cyklov do poškodenia, strata hmotnosti alebo zmena vzhľadu podľa použitej časti normy a kritéria. Samotné číslo preto nie je univerzálnym skóre kvality. Porovnávajte iba výsledky z rovnakej metódy, s rovnakým zaťažením, abrazívom a spôsobom vyhodnotenia a zohľadnite švy, konštrukciu výrobku, žmolkovanie aj reálny spôsob používania.",
        "intro": "Pri poťahovej látke sa často objaví údaj 20 000, 50 000 alebo 100 000 cyklov Martindale. Číslo pôsobí ako jednoduchý rebríček, ale bez skúšobných podmienok môže zvádzať k nesprávnemu záveru. Laboratórna skúška kontroluje jeden druh opakovaného povrchového namáhania. Nevie sama posúdiť roztrhnutie šva, poškodenie zipsom, vyblednutie, mačací pazúr, čistiacu chémiu ani komfort. Užitočná je vtedy, keď vieme, čo presne bolo skúšané a na aké použitie materiál vyberáme.",
        "quick": [
            "<strong>Martindale je metóda, nie materiál:</strong> skúša sa konkrétna vzorka s určeným abrazívom, tlakom, pohybom a intervalmi kontroly.",
            "<strong>Cyklus nie je univerzálny bod kvality:</strong> význam závisí od kritéria ukončenia, zaťaženia a verzie skúšky.",
            "<strong>Oder a žmolkovanie sú rozdielne:</strong> majú príbuzné trenie, ale odlišný cieľ, postup a vyhodnotenie.",
            "<strong>Hotový výrobok môže zlyhať inde:</strong> šev, lem, povlak, zips alebo výplň nemusia byť zahrnuté vo výsledku plochej vzorky.",
            "<strong>Starostlivosť mení povrch:</strong> pranie, zvyšky, sušenie a kontakt s hrubými prvkami môžu ovplyvniť vzhľad aj životnosť.",
        ],
        "overview_heading": "Čo sa pri skúške Martindale deje",
        "overview": [
            "Vzorka textilu sa upevní do skúšobného zariadenia a pri definovanom zaťažení sa pohybuje voči určenému abrazívnemu materiálu. Trajektória vytvára opakované kontakty v rôznych smeroch, aby sa povrch nenamáhal iba jedným jednoduchým ťahom. V stanovených intervaloch sa vzorka kontroluje. Norma určuje prípravu, kondicionovanie, zostavu, zaťaženie a spôsob vyhodnotenia.",
            "ISO 12947-2 sa zameriava na určenie poškodenia vzorky. Ďalšie časti série môžu hodnotiť stratu hmotnosti alebo zmenu vzhľadu. Keď dva predajcovia uvedú rovnaký názov Martindale, ale jeden uvádza cykly do pretrhnutia a druhý vizuálnu zmenu pri inom zaťažení, čísla nemusia byť priamo porovnateľné. V technickom liste preto hľadajte plné označenie metódy a kritérium.",
            "ASTM v prehľade textilných noriem uvádza D4966 pre odolnosť textilných látok proti oderu metódou Martindale. ISO a ASTM dokumenty môžu mať rozdielne detaily a vydania. Nie je presné spojiť výsledky len preto, že obe skúšky používajú názov Martindale. Porovnanie má zmysel pri rovnakom alebo preukázateľne ekvivalentnom postupe.",
        ],
        "table1_heading": "Čo môže znamenať výsledok skúšky oderu",
        "table1_intro": "Výraz počet cyklov je neúplný bez informácie, čo sa po danom počte hodnotilo. Tabuľka oddeľuje najčastejšie typy výsledku.",
        "table1_headers": ["Typ výsledku", "Čo sa sleduje", "Praktický význam", "Čo číslo nehovorí"],
        "table1_rows": [
            ("Poškodenie vzorky", "Definované pretrhnutie nití alebo iné ukončovacie kritérium.", "Odolnosť plochej vzorky voči danému opakovanému oderu.", "Pevnosť šva, zipsu, lemu ani celého výrobku."),
            ("Strata hmotnosti", "Koľko materiálu vzorka stratila po určenom namáhaní.", "Porovnanie úbytku pri rovnakej metóde a zostave.", "Či bude zmena pre používateľa vizuálne prijateľná."),
            ("Zmena vzhľadu", "Vizuálna zmena povrchu po stanovenom počte otáčok.", "Skoršie kozmetické opotrebovanie pri porovnateľných vzorkách.", "Automaticky počet rokov používania v domácnosti."),
            ("Žmolkovanie", "Tvorba chumáčikov a zmena povrchového vzhľadu osobitnou metódou.", "Sklon povrchu k žmolkom za daných podmienok.", "Predratie, pevnosť alebo odolnosť proti zatrhnutiu."),
        ],
        "sections": [
            (
                "Prečo počet cyklov nemožno prepočítať na roky používania",
                [
                    "Laboratórny cyklus má definovaný pohyb, tlak a protimateriál. Jeden deň na pohovke zahŕňa inú hmotnosť používateľa, pot, oblečenie, švy na nohaviciach, omrvinky, posúvanie aj čistenie. Pri odeve sa pridáva pohyb tela, batoh, pracovná plocha a pranie. Neexistuje spoľahlivý univerzálny prepočet, podľa ktorého určitý počet cyklov znamená presný počet rokov.",
                    "Výsledok je najužitočnejší na porovnanie materiálov skúšaných rovnakým spôsobom pre podobné použitie. Ak výrobca stanoví vlastnú kategóriu ľahkého, bežného alebo náročného použitia, pýtajte sa, z akej normy alebo interného kritéria vychádza. Označenie môže byť praktické, ale nemalo by sa zameniť za všeobecne platnú hranicu pre všetky trhy a výrobky.",
                ],
            ),
            (
                "Zaťaženie, abrazívum a kritérium ukončenia menia výsledok",
                [
                    "Vyššie zaťaženie zvyšuje tlak v kontakte a môže urýchliť poškodenie. Iný abrazívny materiál mení drsnosť a spôsob pôsobenia. Intervaly, v ktorých sa vzorka kontroluje, ovplyvňujú presnosť určenia bodu zlyhania. Aj príprava a kondicionovanie vzorky sú dôležité, pretože vlhkosť a napätie textilu môžu meniť jeho správanie.",
                    "Technický list by mal uvádzať aspoň použitú normu a výsledok s jasnou jednotkou alebo kritériom. Ak tieto údaje chýbajú, považujte číslo za neúplné marketingové tvrdenie, nie za presný základ výpočtu životnosti. Pri drahom čalúnení si vyžiadajte protokol alebo vyhlásenie dodávateľa, najmä ak sa materiál kupuje pre hotel, kanceláriu či verejný priestor.",
                ],
            ),
            (
                "Martindale pri oblečení: stehná, lakte a popruhy",
                [
                    "Oblečenie sa odiera lokálne. Vnútorné stehná nohavíc, lakte svetra, manžety a ramená pod batohom dostávajú odlišný tlak a smer pohybu. Laboratórny výsledok látky môže naznačiť odolnosť povrchu, ale strih rozhoduje, koľko napätia sa sústredí do miesta. Príliš tesné nohavice môžu kombinovať trenie s vysokým ťahom vo šve.",
                    "Pri športovej vrstve vstupuje do hry pot, popruh, blato a časté pranie. Pri vlnenom svetri môže byť viditeľný žmolok skôr než predratie. Pri jemnej blúzke môže byť dôležitejšie zatrhnutie ostrým predmetom než plošný oder. Preto pri nákupe odevu skontrolujte zosilnenie, švy a určenie, nielen všeobecnú odolnosť základnej látky.",
                ],
            ),
            (
                "Martindale pri sedačke, kresle a jedálenskej stoličke",
                [
                    "Čalúnenie sedačky má veľkú plochu, no nie je namáhané rovnomerne. Predná hrana sedáka, miesto pod kolenami a obľúbené sedadlo dostávajú viac oderu. Jedálenská stolička môže čeliť častejšiemu vstávaniu, škvrnám a čisteniu. Dekoratívny vankúš má zase nižšiu mechanickú záťaž, ale môže ležať na drsnej látke.",
                    "Okrem oderu posudzujte stálofarebnosť voči treniu a svetlu, čistiteľnosť, pevnosť šva, rozmerovú stabilitu a prípadnú nehorľavosť požadovanú pre konkrétne použitie. V domácnosti so zvieraťom je dôležité zatrhnutie pazúrom, ktoré Martindale priamo nesimuluje. Pri snímateľnom poťahu prečítajte štítok ešte pred kúpou, aby bol povolený proces reálne vykonateľný.",
                ],
            ),
            (
                "Povrstvené textílie potrebujú odlišné hodnotenie",
                [
                    "Látka s polyuretánovým alebo iným povlakom sa môže poškodzovať odieraním povrchovej vrstvy, praskaním alebo oddelením povlaku od nosného textilu. ISO 5470-2 opisuje Martindale skúšku pre pogumované alebo plastom povrstvené textílie. To je odlišný rozsah než bežná metóda pre nepotiahnutú textilnú vzorku.",
                    "Pri koženkovom poťahu alebo nepremokavej vrstve sa pýtajte, čo bolo skúšané: samotný nosič, hotový povrch alebo viacvrstvový systém. Vysoké číslo základnej tkaniny nepovie, kedy sa odlúpne povlak. Údržba je rozhodujúca, pretože nevhodné rozpúšťadlo, mastnota, teplo alebo dezinfekčný prípravok môžu povrch poškodiť iným mechanizmom než mechanický oder.",
                ],
            ),
            (
                "Oder nie je žmolkovanie",
                [
                    "Žmolkovanie opisuje tvorbu chumáčikov zo zamotaných povrchových vlákien. ISO 12945-3 používa upravenú Martindale metódu na hodnotenie žmolkovania, rozstrapatenia a zmatnenia. Názov zariadenia je príbuzný, ale cieľ a vyhodnotenie sú iné než pri predratí podľa ISO 12947.",
                    "Materiál môže dosiahnuť vysokú odolnosť proti predratiu a pritom pomerne skoro zmeniť vzhľad žmolkami. Opačne hladký povrch nemusí dlho maskovať stenčovanie v namáhanom mieste. Pri reklamácii alebo porovnaní presne pomenujte prejav. Podrobný mechanizmus nájdete v článku <a href=\"/n/preco-sa-oblecenie-zmolkuje-vlakna-trenie-pranie-a-susenie\">prečo sa oblečenie žmolkuje</a>.",
                ],
            ),
            (
                "Čo Martindale neodhalí o švoch a konštrukcii",
                [
                    "Plochá vzorka spravidla neobsahuje šev, zips, gombík, lem ani prechod medzi hrúbkami. Hotový výrobok môže zlyhať rozchádzaním šva, vytrhnutím nite alebo poškodením v mieste upevnenia. Pevnosť šva závisí od nite, hustoty stehov, prídavku, smeru látky a napätia pri šití.",
                    "Rovnako nemusí zachytiť zatrhnutie, rez, prepichnutie, roztavenie teplom alebo chemickú degradáciu. Pri pracovnom odeve a ochrannom vybavení sa používajú ďalšie skúšky podľa konkrétneho rizika. Bežný údaj o Martindale nesmie byť interpretovaný ako potvrdenie ochrannej funkcie, ktorú výrobca výslovne nedeklaruje podľa príslušnej normy.",
                ],
            ),
            (
                "Ako pranie ovplyvňuje povrch a odolnosť",
                [
                    "Pranie spája vlhkosť, chemické prostredie, teplotu a mechanický pohyb. Hrubé uteráky, zipsy a suché zipsy môžu trieť jemný povrch. Preplnený bubon zhoršuje prúdenie roztoku a vytvára stlačené kontakty, zatiaľ čo príliš agresívny program môže namáhať švy a povrch. Odevy obráťte naruby a ostré prvky zaistite podľa štítku.",
                    "Správne dávkovanie a oplach pomáhajú odstrániť špinu, ktorá môže medzi povrchmi pôsobiť abrazívne. Nadmerné množstvo produktu však môže zanechať nános a meniť vzhľad. Sušenie pri vysokej teplote môže ovplyvniť syntetiku, elastan, povlaky a lepidlá bez súvisu s laboratórnym počtom cyklov. Starostlivosť preto posudzujte ako samostatnú časť životnosti.",
                ],
            ),
            (
                "Ako doma spomaliť lokálny oder oblečenia",
                [
                    "Vyberte správnu veľkosť a strih, aby sa látka v kritickom mieste súčasne silno nenaťahovala a netrela. Upravte ostrú hranu stola, sedadla alebo popruhu, ak opakovane ničí rovnaké miesto. Odev po nosení skontrolujte a malý uvoľnený šev opravte skôr, než otvorí väčšiu plochu. Pri praní oddeľte drsné a jemné povrchy.",
                    "Žmolky odstraňujte vhodným nástrojom bez hlbokého zarezania do látky. Holiaci strojček na textil používajte na rovnej ploche a s nízkym tlakom. Na slučkové, veľmi tenké alebo konštrukčne zložité materiály nemusí byť vhodný. Ak už vidíte stenčenie alebo presvitajúcu nosnú štruktúru, ďalšie odoberanie povrchu môže urýchliť dieru.",
                ],
            ),
            (
                "Ako sa pýtať na Martindale pri nákupe",
                [
                    "Požiadajte o názov normy, vydanie alebo rok, hodnotu, kritérium ukončenia a informáciu, či sa skúšala dodaná finálna úprava. Pri čalúnení zistite aj odporúčané použitie dodávateľa, spôsob čistenia, stálofarebnosť a ďalšie relevantné parametre. Pri profesionálnom projekte nech výsledok posúdi osoba, ktorá pozná požiadavky priestoru.",
                    "Ak máte pred sebou dve čísla bez podmienok, vyššie nemusí automaticky znamenať lepší nákup. Materiál s veľmi vysokou odolnosťou môže byť príliš tuhý, ťažký, nepohodlný alebo náročný na čistenie pre zamýšľaný účel. Rozhodnutie vyvažuje oder, vzhľad, dotyk, údržbu, konštrukciu, cenu a možnosť opravy alebo výmeny poťahu.",
                ],
            ),
        ],
        "notes": {
            3: (
                "Štyri otázky k číslu Martindale",
                [
                    "Podľa ktorej normy a jej časti bola vzorka skúšaná?",
                    "Aké zaťaženie a abrazívny materiál sa použili?",
                    "Čo presne bolo kritériom ukončenia alebo hodnotenia?",
                    "Bola skúšaná finálna látka s rovnakou úpravou ako dodaný výrobok?",
                ],
                "#f7fbf8",
                "#dbe5de",
            ),
            7: (
                "Vysoké číslo nepreukazuje tieto vlastnosti",
                [
                    "Pevnosť švov, zipsov, gombíkov a spojov hotového výrobku.",
                    "Odolnosť proti pazúrom, ostrému rezu, prepichnutiu alebo zatrhnutiu.",
                    "Stálofarebnosť na svetle, voči potu, čisteniu alebo mokrému treniu.",
                    "Kompatibilitu s dezinfekciou, sušičkou alebo konkrétnym pracím prostriedkom.",
                ],
                "#fffaf5",
                "#e6ded2",
            ),
        },
        "table2_heading": "Ktorý parameter rieši konkrétny problém",
        "table2_intro": "Ak chcete zmysluplne porovnať textil, najprv pomenujte spôsob možného zlyhania. Martindale pokrýva iba časť obrazu.",
        "table2_headers": ["Otázka", "Vhodný typ hodnotenia", "Príklad použitia", "Poznámka"],
        "table2_rows": [
            ("Kedy sa plocha prederie?", "Odolnosť proti oderu s definovaným poškodením.", "Sedák, vnútorné stehná, pracovný odev.", "Porovnávajte rovnakú metódu a kritérium."),
            ("Kedy sa zmení vzhľad?", "Zmena vzhľadu po stanovenom namáhaní.", "Dekoratívny poťah, viditeľná plocha odevu.", "Vizuálny limit môže nastať pred predratím."),
            ("Bude sa látka žmolkovať?", "Osobitná skúška žmolkovania.", "Sveter, sedačka, mikina.", "Žmolok nie je automaticky strata pevnosti."),
            ("Vydrží šev?", "Skúška pevnosti a posunu šva.", "Tesné nohavice, čalúnená hrana.", "Výsledok plochej látky to nenahrádza."),
            ("Znesie povlak čistenie?", "Skúška povrstveného systému a chemickej kompatibility.", "Koženka, zdravotnícky alebo gastro poťah.", "Treba zohľadniť konkrétny čistič a postup."),
        ],
        "steps": [
            "Pomenujte účel textilu a miesto najvyššieho reálneho namáhania.",
            "Vyžiadajte si plný názov skúšobnej normy, výsledok a kritérium ukončenia.",
            "Porovnávajte iba materiály skúšané rovnakou alebo preukázateľne porovnateľnou metódou.",
            "Doplňte posúdenie o žmolkovanie, švy, stálofarebnosť, čistiteľnosť a špecifické riziká.",
            "Pri hotovom výrobku skontrolujte strih, namáhané hrany, zipsy, lemy a možnosť opravy.",
            "Počas používania obmedzte zbytočný kontakt s ostrými a hrubými povrchmi.",
            "Perte a čistite podľa štítku, aby ste laboratórnu odolnosť neznehodnotili iným mechanizmom.",
        ],
        "remember": [
            "Martindale je definovaná skúška oderu, nie univerzálny prepočet na roky používania.",
            "Číslo potrebuje normu, zaťaženie, abrazívum a kritérium hodnotenia.",
            "Oder, žmolkovanie, pevnosť šva a zatrhnutie sú odlišné spôsoby zlyhania.",
            "Povrstvené textílie sa hodnotia osobitným postupom a nosná látka nestačí.",
            "Šetrná údržba, správny strih a včasná oprava ovplyvnia životnosť hotového výrobku.",
        ],
        "mistakes": [
            "<strong>Prepočet cyklov na presný počet rokov.</strong> Laboratórny pohyb nereprodukuje všetky podmienky reálneho používania.",
            "<strong>Porovnanie dvoch čísel bez normy.</strong> Mohli vzniknúť pri inom zaťažení, abrazíve alebo kritériu.",
            "<strong>Zámena žmolkovania za predratie.</strong> Viditeľný žmolok a porušenie nosných nití nie sú rovnaký výsledok.",
            "<strong>Ignorovanie švov a hrán.</strong> Hotový výrobok môže zlyhať v spoji skôr než plocha látky.",
            "<strong>Hodnotenie povlaku podľa nosnej tkaniny.</strong> Povrchová vrstva môže praskať alebo sa odlupovať iným mechanizmom.",
            "<strong>Agresívne čistenie odolnej látky.</strong> Chemikália alebo teplo môže poškodiť farbu a úpravu bez súvisu s oderom.",
        ],
        "expert_heading": "Ako spolu súvisia normy ISO 12947, ISO 12945 a ISO 5470",
        "expert": [
            "ISO 12947-2 stanovuje určenie poškodenia vzorky pri Martindale metóde pre textilné látky. Patrí do série, ktorá oddeľuje rôzne spôsoby vyhodnotenia. Už toto členenie je dôvodom, prečo má technický údaj uvádzať konkrétnu časť normy, nie iba slovo Martindale a veľké číslo.",
            "ISO 12945-3 používa upravenú Martindale metódu na hodnotenie žmolkovania, rozstrapatenia a zmatnenia. Rovnaký typ pohybu zariadenia neznamená rovnaký meraný parameter. Výsledok žmolkovania sa hodnotí podľa vzhľadu a nemožno ho zameniť za cykly do poškodenia nosných nití.",
            "ISO 5470-2 pokrýva odolnosť pogumovaných alebo plastom povrstvených textílií proti oderu pomocou Martindale abradera. Pri takom systéme je dôležitý povlak aj jeho spojenie s podkladom. Technický list by mal preto jasne uviesť, či výsledok patrí bežnej textilnej látke alebo povrstvenému výrobku a aký jav bol hodnotený.",
        ],
        "source_intro": "Normy oddeľujú poškodenie textilnej vzorky, žmolkovanie a oder povrstvených textílií. Hodnoty sú porovnateľné iba pri zhodnej metóde, podmienkach a kritériu.",
        "sources": [
            ("ISO 12947-2: Martindale a poškodenie vzorky", ISO_MARTINDALE),
            ("ASTM: D4966 v prehľade textilných skúšok", ASTM_TEXTILES),
            ("ISO 12945-3: žmolkovanie upravenou Martindale metódou", ISO_PILLING),
            ("ISO 5470-2: oder povrstvených textílií", ISO_COATED_ABRASION),
            ("GINETEX: význam ošetrovacích symbolov", GINETEX),
        ],
        "product_intro": "Odolná látka stále potrebuje správne pranie. Gél pomáha odstrániť bežnú špinu, no nenahradí výber programu, ochranu pred zipsami ani opravu oslabeného šva.",
        "product_text": "Hypoalergénny prací gél Vevo Ylang Absolute je konkrétna možnosť pre bežné prateľné textílie, ak ju povoľuje štítok. Dávkujte podľa podmienok a kombinujte iba s kompatibilnými materiálmi v náplni.",
        "product_limit": "Povrstvenie, vlna, membrána, nehorľavá úprava, čalúnenie alebo profesionálny textil môžu vyžadovať špecializované čistenie. Vysoký výsledok Martindale nie je povolením použiť bežný gél.",
        "category_intro": "Pri výbere pracieho gélu zohľadnite vlákno, farbu, povrchovú úpravu a povolenú teplotu. Mechanickú odolnosť nemožno chrániť nadmerným množstvom produktu.",
        "category_text": "V kategórii pracích gélov nájdete produkty na bežnú domácu bielizeň. Pri technickom alebo povrstvenom textile najprv overte špecifické požiadavky výrobcu.",
        "related": [
            ("Prečo sa oblečenie žmolkuje", ARTICLE_PILLING),
            ("Čo znamená gramáž GSM", ARTICLE_GSM),
            ("Čo je polyamid", ARTICLE_POLYAMIDE),
            ("Čo je zmesový materiál", ARTICLE_BLEND),
            ("Vlna a polyamid", ARTICLE_WOOL_POLYAMIDE),
            ("Statická elektrina v oblečení", ARTICLE_STATIC),
        ],
        "faq_title": "Martindale a odolnosť textilu proti oderu",
        "faq": [
            ("Čo znamená 50 000 cyklov Martindale?", "Znamená výsledok konkrétnej skúšky pri uvedených podmienkach a kritériu. Bez názvu normy, zaťaženia a spôsobu vyhodnotenia nemožno presne povedať, ako sa číslo porovnáva s inou látkou alebo koľko rokov vydrží."),
            ("Koľko Martindale je dosť na sedačku?", "Neexistuje jedna univerzálna hranica pre každý trh, konštrukciu a domácnosť. Zohľadnite odporúčanie dodávateľa pre zamýšľané použitie, metódu skúšky, švy, čistiteľnosť, stálofarebnosť a reálnu záťaž."),
            ("Je vyššie číslo Martindale vždy lepšie?", "Vyšší porovnateľný výsledok môže znamenať vyššiu odolnosť v danej skúške, ale nie automaticky lepší výrobok. Materiál môže byť tuhší, horšie čistiteľný alebo môže zlyhať v šve, povlaku či vzhľade."),
            ("Meria Martindale žmolkovanie?", "Existuje upravená Martindale metóda pre žmolkovanie podľa inej normy, no nejde o rovnaký výsledok ako cykly do poškodenia pri skúške oderu. Vždy treba uviesť konkrétnu metódu."),
            ("Platí výsledok látky aj pre hotové kreslo alebo nohavice?", "Iba čiastočne. Plochá vzorka nehodnotí všetky švy, hrany, výplň, zipsy, strih ani lokálne napätie. Hotový výrobok treba posudzovať ako celok."),
            ("Môže pranie znížiť odolnosť textilu?", "Nevhodné teplo, mechanika, chémia alebo kontakt s ostrými prvkami môžu poškodiť povrch, farbu, švy a úpravu. Perte podľa štítku, aj keď má látka vysoký laboratórny výsledok proti oderu."),
        ],
    },
]


def article_hrefs(markup):
    return re.findall(r'href="([^"]+)"', markup)


def preflight_links(articles):
    rows = []
    seen = set()
    batch_paths = {f"/n/{article['link']}" for article in articles}
    headers = {"User-Agent": "Codex VEVO batch 41 link preflight"}

    for article in articles:
        target_url = f"{BASE}/n/{article['link']}"
        response = requests.get(target_url, timeout=45, allow_redirects=True, headers=headers)
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
            response = requests.get(url, timeout=45, allow_redirects=True, headers=headers)
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
        raise SystemExit("Batch 41 titles do not exactly match the duplicate-guard candidate file")

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
        raise SystemExit("Batch 41 link preflight failed")


if __name__ == "__main__":
    main()
