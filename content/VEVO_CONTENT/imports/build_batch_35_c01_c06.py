import html
import json
import re
import unicodedata
from pathlib import Path
from urllib.parse import urljoin

import requests


BASE = "https://www.vevo.sk"
BATCH = 35
TODAY = "2026-07-08"
OUT_JSON = Path("content/VEVO_CONTENT/imports/batch-35-2026-07-08-articles.json")
OUT_PREFLIGHT = Path("content/VEVO_CONTENT/exports/batch-35-2026-07-08-link-preflight.json")

FORBIDDEN_PUBLIC_RE = re.compile(
    r"longtail|long-tail|long tail|"
    r"\bkeyword(?:s)?\b|klucov\w*\s+slov\w*|kľúčov\w*\s+slov\w*|"
    r"\bSEO\b|search\s+intent|sub[- ]?quer(?:y|ies)|sub[- ]?query|fan[- ]?out|fanout|"
    r"cielene\s+pokr[yý]vame|cielene\s+odpoved[aá]|"
    r"\bCTA\b",
    re.IGNORECASE,
)


def slugify(value):
    value = unicodedata.normalize("NFKD", value)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = re.sub(r"[^a-zA-Z0-9]+", "-", value.lower())
    return re.sub(r"-+", "-", value).strip("-")


def esc(value):
    return html.escape(str(value), quote=True)


def table(headers, rows):
    head = "".join(
        f'<th style="border: 1px solid #e5e5e5; padding: 10px; text-align: left;">{esc(header)}</th>'
        for header in headers
    )
    body = "\n".join(
        "<tr>"
        + "".join(f'<td style="border: 1px solid #e5e5e5; padding: 10px;">{cell}</td>' for cell in row)
        + "</tr>"
        for row in rows
    )
    return (
        '<table style="width: 100%; border-collapse: collapse; margin: 20px 0;">\n'
        f"<thead><tr>{head}</tr></thead>\n<tbody>\n{body}\n</tbody>\n</table>"
    )


def callout(title, bullets, background="#fffaf5", border="#e6ded2"):
    items = "".join(f"<li>{item}</li>" for item in bullets)
    return f"""
<div style="border: 1px solid {border}; border-radius: 8px; padding: 18px; margin: 22px 0; background: {background};">
<h2 style="margin-top: 0;">{esc(title)}</h2>
<ul>{items}</ul>
</div>
""".strip()


def product_block(article):
    product = article["product"]
    category = article["category"]
    return f"""
<div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 24px 0; background: #f7fbf8;">
<h2 style="margin-top: 0;">{esc(product["heading"])}</h2>
<p>{product["intro"]}</p>
<div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff; margin: 14px 0;">
<h3 style="margin-top: 0;">{esc(product["name"])}</h3>
<p><strong>Kedy dáva zmysel:</strong> {product["fit"]}</p>
<p><strong>Kedy najprv riešiť príčinu:</strong> {product["boundary"]}</p>
<p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="{product["href"]}">{esc(product["button"])}</a></p>
</div>
</div>
<div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 24px 0; background: #fffaf5;">
<h2 style="margin-top: 0;">{esc(category["heading"])}</h2>
<p>{category["intro"]}</p>
<ul>{"".join(f"<li><strong>{label}:</strong> {text}</li>" for label, text in category["bullets"])}</ul>
<p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="{category["href"]}">{esc(category["button"])}</a></p>
</div>
""".strip()


def source_box(article):
    rows = "".join(
        f'<li><a rel="noopener" href="{href}" target="_blank">{esc(label)}</a></li>'
        for label, href in article["sources"]
    )
    return f"""
<div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; margin: 22px 0; background: #fbfbfb;">
<h2 style="margin-top: 0;">Zdroje a odborný kontext</h2>
<p>{article["source_intro"]}</p>
<ul>{rows}</ul>
</div>
""".strip()


def related_links(items):
    links = "".join(f'<li><a href="{href}">{esc(label)}</a></li>' for label, href in items)
    return f"<h2>Súvisiace návody na VEVO</h2>\n<ul>{links}</ul>"


def faq(items, title):
    parts = [f"<h2>FAQ: {esc(title)}</h2>"]
    for question, answer in items:
        parts.append(f"<h3>{esc(question)}</h3><p>{answer}</p>")
    return "\n".join(parts)


FRAGRANCE_PRODUCT = {
    "heading": "Vyskúšajte vôňu najprv v menšom množstve",
    "name": "Sada vzoriek najpredávanejších vôní VEVO 3 x 10 ml",
    "href": "/p-1574/sada-vzoriek-najpredavanejsich-voni-vevo-3-x-10ml",
    "intro": "Pri vôni do prania sa oplatí najprv zistiť, aká intenzita vám sedí pri vašej práčke, vode, sušení a type bielizne. Vzorky sú praktické práve vtedy, keď nechcete kupovať veľké balenie naslepo.",
    "fit": "keď hľadáte prvú vôňu, porovnávate intenzitu alebo riešite, či vám viac sedí svieži, čistý, kvetinový alebo hrejivejší tón.",
    "boundary": "ak bielizeň zapácha po praní, najprv riešte práčku, dávkovanie gélu, oplach a sušenie. Vôňa má dopĺňať čistotu, nie prekrývať zatuchnutie.",
    "button": "Pozrieť sadu vzoriek",
}

FRAGRANCE_CATEGORY = {
    "heading": "Vyberte si vôňu podľa bielizne a situácie",
    "href": "/c/vevo-fragrance/parfum-do-prania",
    "button": "Pozrieť parfumy do prania",
    "intro": "Parfum do prania vyberajte podľa toho, čo periete, ako sušíte a ako výraznú vôňu reálne chcete cítiť po usušení.",
    "bullets": [
        ("Jemná každodenná bielizeň", "začnite nižšou intenzitou a sledujte pocit pri nosení."),
        ("Uteráky a posteľná bielizeň", "vôňa by mala byť čistá, nie ťažká alebo rušivá."),
        ("Šport a funkčné oblečenie", "najprv odstráňte pot a zvyšky pracieho prostriedku, vôňu dávkujte opatrne."),
    ],
}

DETERGENT_PRODUCT = {
    "heading": "Začnite čistým pracím základom",
    "name": "Prací gél hypoalergénny z Marseillského mydla 1L",
    "href": "/p-1551/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l",
    "intro": "Pri bežnom praní je najdôležitejšie, aby prací prostriedok zodpovedal typu bielizne, vode, teplote a veľkosti náplne. Až keď je bielizeň dobre vypraná a vypláchnutá, má zmysel riešiť vôňu.",
    "fit": "pri pravidelnom praní, kde chcete tekutý prací prostriedok s jednoduchým dávkovaním a šetrnejším prístupom k pokožke.",
    "boundary": "ak je práčka zanesená, bielizeň je tvrdá alebo zostáva lepkavá, najprv upravte dávku, veľkosť náplne a oplach.",
    "button": "Pozrieť prací gél",
}

DETERGENT_CATEGORY = {
    "heading": "Porovnajte pracie gély podľa typu prania",
    "href": "/c/vevo-home-care/pranie/praci-gel",
    "button": "Pozrieť pracie gély",
    "intro": "Prací gél vyberajte podľa farby, materiálu, citlivosti pokožky a toho, či periete pri nízkej alebo bežnej teplote.",
    "bullets": [
        ("Bežné pranie", "dôležité je správne dávkovanie a nepreplnený bubon."),
        ("Citlivá pokožka", "voľte jemnejší produkt a pridajte dôkladný oplach."),
        ("Vôňa na záver", "parfumy do prania používajte až na čistú bielizeň, nie ako náhradu prania."),
    ],
}

WASHER_PRODUCT = {
    "heading": "Keď sa problém vracia, skontrolujte aj práčku",
    "name": "VEVO Shot koncentrát na čistenie práčky",
    "href": "/p-1549/vevo-shot-koncentrat-na-cistenie-pracky",
    "intro": "Preplnený bubon, priveľa gélu a zvyšky v tesnení dokážu zhoršiť výsledok prania. Ak sa zápach alebo sivý film opakuje, oplatí sa riešiť aj vnútro práčky.",
    "fit": "keď práčka zapácha, v zásobníku sú nánosy alebo sa čistá bielizeň po praní necíti sviežo.",
    "boundary": "ak je problém len v preplnenej náplni, najprv ju zmenšite. Čistič práčky nenahradí správne plnenie bubna.",
    "button": "Pozrieť čistič práčky",
}

WASHER_CATEGORY = {
    "heading": "Starostlivosť o práčku je súčasť dobrého prania",
    "href": "/c/vevo-home-care/pranie/detox-pracky",
    "button": "Pozrieť produkty na čistenie práčky",
    "intro": "Ak sa v práčke drží vlhkosť, zvyšky gélu alebo zápach, bielizeň môže pôsobiť horšie aj pri dobrom pracom prostriedku.",
    "bullets": [
        ("Tesnenie", "po praní ho nechajte preschnúť a pravidelne kontrolujte nánosy."),
        ("Zásobník", "zvyšky gélu a aviváže môžu časom zapáchať."),
        ("Bubon", "preplnenie zhoršuje mechanický pohyb aj oplachovanie."),
    ],
}


COMMON_RELATED_FRAGRANCE = [
    ("Parfum do prania: čo to je a ako funguje", "/n/parfum-do-prania-co-to-je-a-ako-funguje"),
    ("Ako dávkovať parfum do prania podľa množstva bielizne", "/n/ako-davkovat-parfum-do-prania-podla-mnozstva-bielizne"),
    ("Vône do prania podľa bielizne, štýlu a intenzity", "/n/vone-do-prania-ako-vybrat-vonu-podla-bielizne-stylu-a-intenzity"),
    ("Parfum do prania a tvrdá voda", "/n/parfum-do-prania-a-tvrda-voda-preco-bielizen-nevonia-a-co-s-tym"),
    ("Prečo oblečenie zapácha po praní", "/n/preco-moje-oblecenie-zapacha-po-prani-priciny-a-riesenia"),
]

COMMON_RELATED_DETERGENT = [
    ("Hypoalergénny prací gél", "/n/hypoalergenny-praci-gel"),
    ("Ako vyrobiť prací gél doma", "/n/ako-vyrobit-praci-gel-doma-casto-kladene-otazky-a-odpovede"),
    ("Prečo oblečenie zapácha po praní", "/n/preco-moje-oblecenie-zapacha-po-prani-priciny-a-riesenia"),
    ("Ako vyčistiť zásobník práčky", "/n/ako-vycistit-zasobnik-pracky-od-usadenin-pracieho-gelu-a-avivaze"),
    ("Ako vyčistiť filter práčky", "/n/ako-vycistit-filter-pracky-ked-bielizen-zapacha-alebo-voda-odteka-pomaly"),
]

FRAGRANCE_SOURCES = [
    ("DermNet: fragrance allergy", "https://dermnetnz.org/topics/fragrance-allergy"),
    ("EPA: volatile organic compounds and indoor air", "https://www.epa.gov/indoor-air-quality-iaq/volatile-organic-compounds-impact-indoor-air-quality"),
]

DETERGENT_SOURCES = [
    ("NHS: atopic eczema", "https://www.nhs.uk/conditions/atopic-eczema/"),
    ("DermNet: textile contact dermatitis", "https://dermnetnz.org/topics/textile-contact-dermatitis"),
]

WASHER_SOURCES = [
    ("EPA: mold and moisture in the home", "https://www.epa.gov/mold/mold-cleanup-your-home"),
    ("EPA: volatile organic compounds and indoor air", "https://www.epa.gov/indoor-air-quality-iaq/volatile-organic-compounds-impact-indoor-air-quality"),
]


def fragrance_article(topic):
    title = topic["title"]
    focus = topic["focus"]
    setting = topic["setting"]
    return {
        "title": title,
        "short": topic["short"],
        "answer": topic["answer"],
        "quick": [
            f"<strong>Najprv čistota:</strong> {topic['quick_clean']}",
            f"<strong>Dávkovanie:</strong> {topic['quick_dose']}",
            f"<strong>Sušenie:</strong> {topic['quick_dry']}",
            "<strong>Citlivá pokožka:</strong> pri deťoch, alergii alebo ekzéme začnite nízkou intenzitou a sledujte reakciu.",
            '<strong>Výber vône:</strong> pri neistote začnite vzorkami a porovnajte vôňu po úplnom usušení.',
        ],
        "intro": [
            f"{focus} je častá situácia, pri ktorej ľudia očakávajú jasný voňavý výsledok, ale realita závisí od viacerých detailov. Parfum do prania sa používa v závere prania, no výsledok ovplyvní aj prací prostriedok, množstvo bielizne, tvrdosť vody, teplota programu, oplach a spôsob sušenia. Ak niektorý z týchto krokov nefunguje, vôňa sa môže stratiť alebo pôsobiť inak, než ste čakali.",
            f"Pri téme {setting} je dôležité nepoužívať vôňu ako riešenie špinavej alebo zle vypláchnutej bielizne. Ak oblečenie zapácha po pote, práčka je zatuchnutá alebo je v tkanine priveľa zvyškov gélu, silnejšia vôňa problém nevyrieši. Najprv treba upraviť pranie a až potom doladiť vôňu.",
            "Dobrý článok o vôni do prania musí preto začať prakticky: kedy ju pridať, koľko jej použiť, čo sledovať po usušení a kedy radšej ubrať. Vôňa má byť príjemný detail, ktorý si všimnete pri obliekaní alebo ukladaní bielizne, nie agresívny oblak, ktorý prekáža pri nosení.",
            "Veľmi často rozhoduje aj typ textilu. Inak sa správa bavlnené tričko, inak froté uterák, syntetické športové oblečenie alebo posteľná bielizeň. Práve preto sa oplatí testovať vôňu na menšej dávke bielizne a až potom ju používať pravidelne.",
        ],
        "why_heading": f"Prečo pri situácii {setting} vôňa nemusí dopadnúť podľa očakávania",
        "why": [
            "Vôňa na textile nie je izolovaný jav. Drží sa na vláknach, ale ovplyvňuje ju aj vlhkosť, teplota, množstvo vody v oplachu a to, či na bielizni nezostal film z pracieho prostriedku. Ak je bubon preplnený, voda sa nedostane medzi jednotlivé kusy a vôňa sa nemusí rozložiť rovnomerne.",
            "Druhý častý dôvod je sušenie. Bielizeň, ktorá schne príliš dlho v nevetranej miestnosti, môže stratiť svieži dojem a získať zatuchnutý podtón. Naopak, veľmi horúce alebo intenzívne sušenie môže jemnú vôňu oslabiť. Výsledok preto neposudzujte hneď po vybratí z práčky, ale až po úplnom usušení.",
            "Treba rátať aj s tým, že čuch si na vôňu zvyká. Vy ju po chvíli môžete vnímať slabšie, zatiaľ čo iný človek ju cíti výraznejšie. Preto je lepšie začať opatrne a zvyšovať dávku až vtedy, keď viete, že bielizeň je čistá a dobre vypláchnutá.",
        ],
        "decision_heading": "Rýchla diagnostika výsledku",
        "decision_rows": topic["decision_rows"],
        "steps": topic["steps"],
        "check_rows": [
            ("Bavlnené tričká", "vôňa sa môže miešať s potom, ak sa tričko perie neskoro", "perte skôr a nepreplňte bubon"),
            ("Uteráky", "priveľa produktu môže zhoršiť savosť alebo pocit čistoty", "použite menšiu intenzitu a dobrý oplach"),
            ("Posteľná bielizeň", "príliš ťažká vôňa môže prekážať pri spánku", "voľte jemnejšiu vôňu a vetrajte"),
            ("Športová syntetika", "pach potu sa môže vrátiť pri zahriatí tela", "najprv riešte pranie syntetiky, vôňu dávkujte mierne"),
            ("Detské oblečenie", "citlivá pokožka môže reagovať na parfumáciu", "začnite veľmi opatrne alebo parfumáciu vynechajte"),
        ],
        "mistakes": [
            "Pridávať viac vône bez kontroly, či bielizeň nie je zle vypláchnutá.",
            "Používať vôňu na prekrytie potu, zatuchnutia alebo zápachu z práčky.",
            "Testovať vôňu len na mokrej bielizni a nepočkať na úplné usušenie.",
            "Prať príliš veľkú náplň, v ktorej sa vôňa nerozloží rovnomerne.",
            "Používať rovnakú intenzitu na uteráky, posteľ, šport aj detské oblečenie.",
            "Kombinovať viac výrazných parfumovaných produktov naraz a potom nevedieť určiť, čo prekáža.",
        ],
        "sections": topic["sections"]
        + [
            (
                "Ako testovať vôňu bez zbytočného rizika",
                [
                    "Najpraktickejší test je menšia dávka bielizne, ktorú dobre poznáte. Vyberte niekoľko tričiek, uterák alebo obliečku a použite nižšiu dávku. Po praní bielizeň usušte rovnakým spôsobom, ako ju sušíte bežne. Vôňu hodnotíte až vtedy, keď je textil suchý a uložený aspoň niekoľko hodín.",
                    "Ak je vôňa slabá, neupravujte naraz všetko. Najprv skontrolujte, či nebol bubon preplnený, či ste nepoužili priveľa gélu a či bielizeň neschla pridlho. Až potom jemne zvýšte množstvo vône. Takto viete, či problém bol vo vôni alebo v procese prania.",
                ],
            ),
            (
                "Kedy vôňu radšej nepoužiť alebo výrazne znížiť",
                [
                    "Pri bielizni pre malé deti, ľudí s citlivou pokožkou, alergikov alebo pri výraznom ekzéme je rozumnejšie začať neparfumovaným alebo veľmi jemným praním. Ak už vôňu skúšate, použite minimálne množstvo a sledujte komfort pri nosení.",
                    "Vôňu tiež netreba pridávať do každej dávky bielizne. Na pracovné oblečenie, športové veci alebo textil, ktorý ide priamo na citlivú pokožku, môže byť vhodnejšia nižšia intenzita. Na posteľnú bielizeň sa často hodí jemnosť, nie maximálna sila.",
                ],
            ),
        ],
        "rule": [
            "Parfum do prania používajte až na dobre vypranú a vypláchnutú bielizeň.",
            "Ak výsledok nie je dobrý, najprv upravte pranie, až potom dávku vône.",
        ],
        "caution": [
            "Vonné produkty obsahujú látky, ktoré môžu niektorým ľuďom prekážať. Pri citlivej pokožke, astme, alergii alebo malých deťoch používajte opatrné dávkovanie a sledujte, či vôňa pri nosení alebo spánku neruší.",
            "Ak bielizeň po praní zapácha, nehľadajte prvé riešenie vo výraznejšej vôni. Skontrolujte práčku, filter, zásobník, veľkosť náplne, dávku gélu a sušenie. Vôňa má byť posledný krok po čistote.",
        ],
        "expert": [
            "Vonné produkty fungujú tak, že uvoľňujú prchavé zložky, ktoré človek vníma čuchom. Pri textile sa časť vône zachytí na vláknach, časť sa stratí počas prania, oplachu a sušenia. Preto je prirodzené, že intenzita na suchej bielizni nie je rovnaká ako vôňa priamo z fľaštičky.",
            "Odborné zdroje pri parfumovaných produktoch upozorňujú najmä na rozumné používanie, vetranie a citlivosť niektorých ľudí na vône. Pre domáce pranie z toho vyplýva praktická zásada: vôňa má byť primeraná priestoru, textilu a osobe, ktorá bude bielizeň nosiť.",
        ],
        "source_intro": "Pri parfumovaných produktoch sa oplatí myslieť na primerané dávkovanie, vetranie a individuálnu citlivosť. Zdroje nižšie uvádzame ako širší kontext k vôňam a ich vnímaniu.",
        "sources": FRAGRANCE_SOURCES,
        "product": FRAGRANCE_PRODUCT,
        "category": FRAGRANCE_CATEGORY,
        "related": COMMON_RELATED_FRAGRANCE,
        "faq": topic["faq"],
        "faq_title": topic["faq_title"],
    }


def detergent_article(topic):
    title = topic["title"]
    return {
        "title": title,
        "short": topic["short"],
        "answer": topic["answer"],
        "quick": [
            f"<strong>Hlavné pravidlo:</strong> {topic['quick_main']}",
            "<strong>Dávka:</strong> viac gélu neznamená čistejšiu bielizeň; môže zhoršiť oplach.",
            "<strong>Náplň:</strong> bubon nesmie byť nadoraz, inak sa prací prostriedok nerozloží rovnomerne.",
            "<strong>Oplach:</strong> pri citlivej pokožke, tmavom oblečení alebo nízkej teplote pomáha dôkladné vypláchnutie.",
            '<strong>Vôňa:</strong> <a href="/c/vevo-fragrance/parfum-do-prania">parfumy do prania</a> dávajú zmysel až po tom, keď je bielizeň dobre vypraná.',
        ],
        "intro": [
            f"{topic['intro_focus']} Ľudia pri praní často riešia, či problém vyrieši iný produkt, vyššia teplota alebo väčšie množstvo pracieho prostriedku. V praxi však rozhoduje kombinácia: druh textilu, farba, znečistenie, tvrdosť vody, veľkosť náplne, program a oplach.",
            "Prací gél má výhodu v tom, že sa ľahko dávkuje a dobre sa používa pri bežnom praní. Nie je však automatickou odpoveďou na všetko. Pri bielych veciach, silných škvrnách, pracovnom oblečení alebo zanedbanej práčke môže byť potrebné riešiť aj predpranie, odstraňovanie škvŕn, teplotu alebo čistenie práčky.",
            "Najčastejšia chyba je pridať viac gélu v nádeji, že bielizeň bude čistejšia a voňavejšia. Ak je gélu priveľa alebo je bubon preplnený, zvyšky sa horšie vypláchnu a textil môže pôsobiť tvrdý, lepkavý alebo zatuchnutý. Čistota preto nevzniká z množstva produktu, ale zo správneho pomeru voda, pohyb, čas a dávkovanie.",
            "Dobrý prací postup má byť jednoduchý a opakovateľný. Triediť bielizeň, nepreplniť bubon, dávkovať podľa špinavosti a vody, vybrať vhodný program a rýchlo sušiť. Až potom má zmysel riešiť jemnú vôňu alebo špeciálnu starostlivosť.",
        ],
        "why_heading": topic["why_heading"],
        "why": topic["why"],
        "decision_heading": "Ako sa rozhodnúť v bežnej domácnosti",
        "decision_rows": topic["decision_rows"],
        "steps": topic["steps"],
        "check_rows": topic["check_rows"],
        "mistakes": [
            "Dávať prací prostriedok od oka bez ohľadu na tvrdosť vody a veľkosť náplne.",
            "Plniť bubon tak, že sa bielizeň nemá kde pohybovať.",
            "Používať krátky program na výrazne znečistené veci a potom pridávať viac gélu.",
            "Sušiť bielizeň pomaly v nevetranej miestnosti a viniť prací prostriedok.",
            "Kombinovať príliš veľa produktov naraz: gél, aviváž, vôňu a ďalšie prísady bez jasného dôvodu.",
            "Ignorovať zvyšky v zásobníku, zápach práčky alebo zanesené tesnenie.",
        ],
        "sections": topic["sections"]
        + [
            (
                "Ako upraviť postup podľa vody, náplne a programu",
                [
                    "Ak máte tvrdšiu vodu, veľmi úsporný program alebo plný bubon, rovnaká dávka pracieho gélu sa môže správať inak než v ideálnych podmienkach. Pri tvrdej vode býva bielizeň po usušení tuhšia, pri preplnení sa horšie opláchne a pri krátkom programe nemá produkt dosť času na prácu. Preto sledujte výsledok po usušení, nie iba vôňu po otvorení práčky.",
                    "Najpraktickejší test je vyprať jednu menšiu dávku bielizne s presne odmeraným množstvom gélu a potom ju porovnať s bežným praním. Ak je menšia dávka mäkšia, čistejšia a menej zatuchnutá, problém bol pravdepodobne v náplni alebo oplachu. Ak sa nič nezmení, až potom má zmysel meniť produkt alebo program.",
                ],
            ),
            (
                "Čo sledovať po usušení, nie iba po praní",
                [
                    "Mokrá bielizeň môže pôsobiť čistejšie, než bude po úplnom usušení. Skutočný výsledok zistíte až vtedy, keď je textil suchý, vyvetraný a chvíľu uložený. Tvrdosť, zatuchnutie, lepkavosť alebo návrat pachu pri nosení sú signály, že pranie nebolo vyvážené.",
                    "Ak používate aj vôňu do prania, hodnotenie po usušení je ešte dôležitejšie. Silný prvý dojem z práčky môže rýchlo ustúpiť, ak textil nebol dobre vypraný alebo schne pomaly. Preto má čistý prací základ prednosť pred intenzitou vône.",
                ],
            ),
            (
                "Ako spoznať, že je pracieho gélu priveľa",
                [
                    "Typické signály sú lepkavý pocit, tvrdšia bielizeň, zvyšky v zásobníku, slabšia sviežosť alebo svrbenie pokožky po nosení. Pri tmavých veciach sa môžu objaviť matné mapy a pri uterákoch zhoršený pocit savosti. Vtedy nepomôže ďalšia vôňa, ale menšia dávka a lepší oplach.",
                    "Skúste jednu dávku vyprať s nižším množstvom gélu a menšou náplňou. Ak sa výsledok zlepší, problém nebol v slabom produkte, ale v pomere produktu, vody a pohybu v bubne. Toto je časté najmä pri úsporných programoch s menším množstvom vody.",
                ],
            ),
            (
                "Ako zapojiť vôňu bez zhoršenia prania",
                [
                    "Vôňa do prania by mala nasledovať až po dobre nastavenom praní. Ak je bielizeň čistá, dobre vypláchnutá a rýchlo usušená, jemná vôňa pôsobí prirodzene. Ak sa snažíte prekryť zatuchnutie, výsledok bude ťažký a nestály.",
                    'Pri výbere vône sa oplatí začať menším balením alebo vzorkami. Kategória <a href="/c/vevo-fragrance/parfum-do-prania">parfumy do prania</a> dáva zmysel najmä pri posteľnej bielizni, uterákoch a bežnom oblečení, kde chcete pocit čistoty predĺžiť, nie nahradiť.',
                ],
            ),
        ],
        "rule": [
            "Najprv nastavte dávku, náplň a oplach; až potom meňte produkt.",
            "Ak sa problém opakuje, skontrolujte aj práčku a spôsob sušenia.",
        ],
        "caution": [
            "Pri citlivej pokožke, detskom oblečení alebo ekzéme nepíšte výsledok len vôni. Sledujte zloženie, množstvo pracieho prostriedku a kvalitu oplachu. Pri výrazných kožných prejavoch je vhodné poradiť sa s odborníkom.",
            "Pri jemných materiáloch má prednosť štítok výrobcu. Nie každý textil znáša rovnakú teplotu, rovnaké otáčky alebo rovnaký typ pracieho prostriedku.",
        ],
        "expert": [
            "Pranie je fyzikálno-chemický proces: nečistoty sa uvoľňujú pôsobením vody, pohybu, teploty, času a pracích látok. Ak jeden prvok chýba, ďalší ho nemusí bezpečne nahradiť. Preto nadmerné dávkovanie gélu nie je riešenie preplneného bubna alebo krátkeho programu.",
            "Pri citlivej pokožke odborné zdroje často odporúčajú jednoduchšiu rutinu, dôkladný oplach a opatrnosť pri parfumácii. V praxi to znamená držať pranie čo najčitateľnejšie: jeden hlavný prací produkt, primeraná dávka a jasné oddelenie vône od samotného čistenia.",
        ],
        "source_intro": "Pri pracích produktoch je dôležité rozlišovať medzi čistotou, oplachom, vôňou a citlivosťou pokožky. Zdroje nižšie používame ako širší rámec k starostlivosti o textil a pokožku.",
        "sources": DETERGENT_SOURCES,
        "product": DETERGENT_PRODUCT,
        "category": DETERGENT_CATEGORY,
        "related": COMMON_RELATED_DETERGENT,
        "faq": topic["faq"],
        "faq_title": topic["faq_title"],
    }


def washer_article(topic):
    return {
        "title": topic["title"],
        "short": topic["short"],
        "answer": topic["answer"],
        "quick": [
            "<strong>Priestor v bubne:</strong> bielizeň sa musí voľne prehadzovať, inak sa nevyperie rovnomerne.",
            "<strong>Oplach:</strong> preplnená práčka horšie vyplaví pot, špinu aj prací prostriedok.",
            "<strong>Vôňa:</strong> ak je bielizeň stlačená a zle vypláchnutá, parfum do prania výsledok nezachráni.",
            "<strong>Signál problému:</strong> tvrdé, lepkavé, nevonné alebo fľakaté veci po praní často súvisia s náplňou.",
            "<strong>Riešenie:</strong> perte menšie dávky, dávkujte presnejšie a nechajte práčku pravidelne preschnúť.",
        ],
        "intro": [
            "Preplnená práčka je jeden z najnenápadnejších dôvodov, prečo bielizeň po praní nepôsobí čisto. Človek má pocit, že šetrí čas, vodu a energiu, ale textil sa v bubne nemá kde pohybovať. Prací gél sa nerozloží rovnomerne, voda neprejde cez všetky vrstvy a oplach nechá v tkanine zvyšky.",
            "Výsledok môže vyzerať rôzne: tričká stále zapáchajú, uteráky sú tvrdé, čierne oblečenie má mapy, posteľná bielizeň je pokrčená a syntetika začne cítiť po zahriatí tela. Veľa ľudí vtedy pridá viac gélu alebo viac vône, ale tým sa problém často zhorší.",
            "Práčka potrebuje priestor na mechanický pohyb. Bielizeň sa nemá len namočiť, ale prehadzovať, trieť o vodu a priebežne sa oplachovať. Ak je bubon natlačený, väčšina pracieho procesu sa zmení na stlačený mokrý balík textilu.",
            "Tento článok vysvetľuje, ako spoznať preplnenú práčku, čo sa deje pri praní, ako veľkú náplň zvoliť pri uterákoch, syntetike, posteľnej bielizni a bežnom oblečení a kedy už treba riešiť aj čistenie práčky.",
        ],
        "why_heading": "Prečo preplnený bubon zhorší pranie",
        "why": [
            "V bubne musí byť dostatok miesta na pohyb. Keď sa bielizeň nemá kde prehadzovať, prací roztok sa nedostane rovnomerne k vláknam. Niektoré kusy sú premokrené, iné len stlačené v strede náplne. Špina, pot a zvyšky produktu potom zostávajú v tkanine.",
            "Preplnenie zhoršuje aj oplachovanie. Aj keď práčka napustí vodu, nemá šancu prepláchnuť všetky vrstvy rovnomerne. Zvyšky gélu sa môžu zachytiť na oblečení a po usušení sa prejavia ako tvrdosť, sivý film alebo slabá vôňa.",
            "Pri uterákoch, posteľnej bielizni a mikinách je problém ešte výraznejší, pretože textil nasaje veľa vody a zväčší objem. To, čo vyzeralo ako prijateľná suchá dávka, môže byť po nasiaknutí príliš ťažké a stlačené.",
        ],
        "decision_heading": "Ako zistiť, či je práčka preplnená",
        "decision_rows": [
            ("Ruka sa nedá vložiť nad bielizeň", "bubon je natlačený až po vrch", "odoberte časť náplne"),
            ("Veci sú po praní zauzlené", "textil sa neprehadzoval voľne", "perte menšie dávky a zapínajte zipsy"),
            ("Na tmavom oblečení sú mapy", "zvyšky gélu alebo nedostatočný oplach", "znížte dávku a náplň"),
            ("Uteráky sú tvrdé", "málo vody a pohybu na objemný textil", "perte uteráky samostatne v menšej dávke"),
            ("Bielizeň nevonia", "špina, pot alebo gél zostali vo vláknach", "nepridávajte vôňu, najprv opravte proces"),
        ],
        "steps": [
            "Pred praním skontrolujte objem, nie iba hmotnosť. Nad suchou bielizňou má zostať voľný priestor.",
            "Objemné veci ako uteráky, župany a posteľnú bielizeň perte oddelene alebo v menšej dávke.",
            "Dávku pracieho gélu prispôsobte menšej náplni. Pri preplnení nepomáha pridať viac produktu.",
            "Vyberte program, ktorý má dosť vody a času na typ textilu. Krátky program nie je na všetko.",
            "Ak po praní cítite zvyšky gélu, pridajte oplach alebo ďalšíkrát znížte dávku.",
            "Bielizeň vyberte hneď po doprání a rozprestrite ju, aby nezačala zatuchnúť.",
            "Ak sa problém opakuje, vyčistite zásobník, tesnenie a bubon práčky.",
        ],
        "check_rows": [
            ("Tričká a spodná bielizeň", "stlačenie a slabší oplach", "neplniť bubon nadoraz"),
            ("Uteráky", "veľký objem po nasiaknutí", "prať samostatne a nepreplniť"),
            ("Posteľná bielizeň", "zauzlenie a zlé opláchnutie vnútri", "zapnúť obliečky a prať menšiu dávku"),
            ("Športová syntetika", "pach sa vracia po zahriatí", "prať skoro a v menšej dávke"),
            ("Tmavé oblečenie", "mapy po géli", "menej produktu, lepší oplach"),
        ],
        "mistakes": [
            "Riadiť sa iba tým, či sa dvierka ešte dajú zavrieť.",
            "Pridať viac pracieho gélu, keď bielizeň po praní nevonia.",
            "Prať uteráky, posteľnú bielizeň a oblečenie v jednej veľkej dávke.",
            "Použiť krátky program na veľkú a zmiešanú náplň.",
            "Nenechať práčku po praní otvorenú a preschnúť.",
            "Prekrývať zatuchnutie vôňou namiesto úpravy náplne a oplachu.",
        ],
        "sections": [
            (
                "Preplnená práčka a zápach bielizne",
                [
                    "Ak bielizeň po praní zapácha, preplnenie je jedna z prvých vecí, ktorú treba skontrolovať. Textil sa síce namočí, ale pot a kožný maz sa nemusia dobre uvoľniť. Pri sušení sa potom objaví zatuchnutý alebo kyslastý podtón.",
                    'Vôňu riešte až po čistote. Ak chcete bielizeň po správnom praní jemne prevoňať, môžete neskôr siahnuť po kategórii <a href="/c/vevo-fragrance/parfum-do-prania">parfumy do prania</a>, ale pri preplnenom bubne najprv znížte náplň.',
                ],
            ),
            (
                "Preplnená práčka a zvyšky pracieho gélu",
                [
                    "Prací gél potrebuje vodu a pohyb, aby sa rozložil a potom vypláchol. V preplnenom bubne môže časť gélu zostať v záhyboch a hrubších kusoch. Po usušení to cítiť ako tvrdosť, lepivosť alebo slabý film na textile.",
                    "Ak máte podozrenie na zvyšky gélu, ďalšie pranie spravte s menšou náplňou a miernejšou dávkou. Pri tmavom oblečení sledujte mapy, pri uterákoch savosť a pri citlivej pokožke komfort pri nosení.",
                ],
            ),
            (
                "Koľko bielizne dať do práčky",
                [
                    "Jednoduché pravidlo je nechať v bubne priestor na pohyb. Pri bežnom oblečení by sa mala nad bielizeň zmestiť ruka. Pri uterákoch, dekách a posteľnej bielizni nechajte viac priestoru, pretože po nasiaknutí zväčšia objem a hmotnosť.",
                    "Nie každá práčka s veľkou kapacitou zvládne veľký objem rovnako dobre na každom programe. Sledujte návod k práčke a symboly programu. Krátky program pri veľkej náplni často znamená slabší oplach a horšie rozloženie pracieho prostriedku.",
                ],
            ),
            (
                "Kedy vyčistiť práčku",
                [
                    "Ak ste upravili náplň aj dávkovanie a bielizeň stále nevonia, problém môže byť v práčke. Zásobník, tesnenie, filter a bubon dokážu držať nánosy, ktoré sa pri praní vracajú na textil.",
                    'Súvisiace postupy nájdete v návodoch <a href="/n/ako-vycistit-zasobnik-pracky-od-usadenin-pracieho-gelu-a-avivaze">ako vyčistiť zásobník práčky</a> a <a href="/n/ako-vycistit-filter-pracky-ked-bielizen-zapacha-alebo-voda-odteka-pomaly">ako vyčistiť filter práčky</a>.',
                ],
            ),
            (
                "Praktická kapacita podľa typu bielizne",
                [
                    "Pri ľahkých tričkách vyzerá bubon plný rýchlejšie, ale textil sa ešte vie pohybovať. Pri uterákoch, županoch, mikinách a posteľnej bielizni je situácia iná: po nasiaknutí vodou výrazne zväčšia hmotnosť a objem. Preto pri týchto veciach nechajte v bubne viac priestoru než pri bežnom oblečení.",
                    "Ak periete zmiešanú dávku, najťažší a najobjemnejší kus určuje limit. Jedna hrubá mikina alebo veľký uterák vie zmeniť celý pohyb v bubne. Pri pochybnosti je lepšie rozdeliť pranie na dve menšie dávky, než neskôr riešiť zápach, mapy a zle vypláchnuté veci.",
                ],
            ),
            (
                "Ako zachrániť bielizeň po zle vypratej preplnenej dávke",
                [
                    "Ak ste už vybrali bielizeň a cítiť z nej zvyšky gélu alebo zatuchnutie, nesušte ju nasilu a neprevoňajte ju ďalším produktom. Najprv ju rozdeľte na menšie dávky a spustite oplach alebo kratšie prepranie bez zbytočnej dávky gélu. Cieľom je dostať z vlákien zvyšky, nie pridať ďalšiu vrstvu.",
                    "Pri tmavom oblečení sledujte mapy, pri uterákoch savosť a pri športových veciach návrat pachu po zahriatí. Ak sa problém opakuje, zapíšte si, koľko vecí bolo v bubne, aký program ste použili a ako dlho bielizeň schla. Tak najrýchlejšie zistíte, kde sa pranie kazí.",
                ],
            ),
        ],
        "rule": [
            "Bubon nie je úložný box; bielizeň potrebuje priestor na pohyb.",
            "Ak výsledok prania zlyháva, zmenšite náplň skôr, než pridáte viac produktu.",
        ],
        "caution": [
            "Ak práčka skáče, vibruje alebo nedokáže správne odstreďovať, neberte to ako normálny stav. Môže ísť o preťaženie, zle rozloženú náplň alebo technický problém.",
            "Pri veľkých kusoch, dekách a objemnej posteľnej bielizni rešpektujte kapacitu práčky aj odporúčanie výrobcu textilu. Niektoré kusy patria do väčšej práčky alebo čistiarne.",
        ],
        "expert": [
            "Účinné pranie stojí na rovnováhe mechanického pohybu, vody, času, teploty a pracieho prostriedku. Preplnenie obmedzí najmä mechanický pohyb a oplach. Tým zníži účinnosť aj vtedy, keď použijete kvalitný prací gél.",
            "Z pohľadu hygieny domácnosti je dôležitá aj vlhkosť po praní. Zatvorená práčka, mokré tesnenie a zvyšky pracích produktov vytvárajú prostredie, v ktorom sa ľahšie drží zápach. Preto je čistenie práčky a sušenie tesnenia praktická prevencia.",
        ],
        "source_intro": "Pri preplnenej práčke nejde len o pohodlie, ale o pohyb textilu, oplach a vlhkosť v spotrebiči. Zdroje nižšie uvádzame ako širší kontext k hygiene, vlhkosti a praniu.",
        "sources": WASHER_SOURCES,
        "product": WASHER_PRODUCT,
        "category": WASHER_CATEGORY,
        "related": COMMON_RELATED_DETERGENT,
        "faq": [
            ("Ako spoznám preplnenú práčku?", "Ak sa nad bielizeň nedá vložiť ruka, veci sú po praní zauzlené alebo zostávajú nevypláchnuté, náplň je pravdepodobne príliš veľká."),
            ("Pomôže pridať viac pracieho gélu?", "Nie. Pri preplnení sa gél horšie rozloží a vypláchne. Lepšie je zmenšiť náplň a dávkovať presnejšie."),
            ("Prečo bielizeň po preplnení nevonia?", "Pretože špina, pot a zvyšky pracieho prostriedku sa nemusia dobre uvoľniť ani vypláchnuť. Vôňa potom nemá čistý základ."),
            ("Mám pri preplnenej práčke čistiť aj spotrebič?", "Ak sa problém opakuje aj po zmenšení náplne, áno. Skontrolujte zásobník, tesnenie, filter a bubon."),
        ],
        "faq_title": "preplnená práčka",
    }


FRAGRANCE_TOPICS = [
    {
        "title": "Prečo parfum do prania necítiť po usušení",
        "short": "Ak parfum do prania necítiť po usušení, príčina často nie je len v dávke. Skontrolujte čistotu bielizne, preplnený bubon, tvrdú vodu, oplach, spôsob sušenia a to, či vôňu nehodnotíte ešte na mokrom textile.",
        "focus": "Slabá alebo úplne stratená vôňa po usušení",
        "setting": "vôňa po usušení",
        "answer": "Ak parfum do prania necítiť po usušení, najprv skontrolujte, či bielizeň nie je zle vypláchnutá, preplnená alebo sušená príliš dlho vo vlhku. Až potom upravujte dávku vône. Často pomôže menšia náplň, presnejšie dávkovanie pracieho gélu, rýchlejšie sušenie a test vône na menšej dávke.",
        "quick_clean": "bielizeň musí byť skutočne čistá, nie iba prevoňaná hneď po praní.",
        "quick_dose": "zvyšujte ju postupne, nie skokom; priveľa vône môže pôsobiť ťažko.",
        "quick_dry": "pomalé sušenie v kúpeľni vie prekryť alebo znehodnotiť jemnú vôňu.",
        "decision_rows": [
            ("Vôňa je silná za mokra, slabá po usušení", "časť vône prirodzene vyprchá a časť zmení intenzitu", "hodnotiť až suchú bielizeň a testovať dávku postupne"),
            ("Bielizeň je zatuchnutá", "sušenie trvalo dlho alebo práčka nie je čistá", "riešiť vlhkosť, práčku a sušenie"),
            ("Vôňa je nerovnomerná", "bubon bol preplnený alebo sa produkt nerozložil", "prať menšiu dávku"),
            ("Vôňu cíti len niekto iný", "čuch si na vôňu zvyká", "nepridávať automaticky viac"),
            ("Vôňa mizne pri uterákoch", "hrubý textil potrebuje dobrý oplach a sušenie", "nepreplniť bubon a neprevoňať nánosy"),
        ],
        "steps": [
            "Vyberte jednu menšiu dávku bielizne a vyperte ju s bežnou dávkou pracieho prostriedku.",
            "Nepreplňte bubon, aby sa vôňa aj oplach dostali medzi kusy textilu.",
            "Použite nižšiu až strednú dávku parfumu do prania a výsledok si zapíšte.",
            "Bielizeň vyberte hneď po doprání a sušte ju vzdušne.",
            "Vôňu hodnotíte až po úplnom usušení, nie po otvorení práčky.",
            "Ak je bielizeň zatuchnutá, riešte práčku a sušenie, nie ďalšiu dávku vône.",
            "Ak je vôňa príjemná, ale slabá, zvýšte dávku iba mierne pri ďalšom praní.",
        ],
        "sections": [
            ("Prečo vôňa zmizne práve pri sušení", [
                "Sušenie je fáza, v ktorej sa z textilu odparuje voda a s ňou sa mení aj vnímanie vône. Bielizeň môže voňať výrazne po otvorení práčky, ale po niekoľkých hodinách je jemnejšia. To nie je vždy chyba; často ide o prirodzený rozdiel medzi mokrým a suchým textilom.",
                "Ak však vôňa zmizne úplne alebo sa zmení na zatuchnutý tón, treba hľadať príčinu v sušení. Vlhká kúpeľňa, príliš husté zavesenie, zatvorené okná alebo ponechanie bielizne v práčke môžu výsledok zhoršiť viac než samotná dávka vône.",
            ]),
            ("Tvrdá voda, oplach a zvyšky gélu", [
                "Tvrdá voda a zvyšky pracieho prostriedku môžu meniť pocit z bielizne. Textil potom nie je ľahký a čistý, ale tvrdší alebo matný. Vôňa sa na takom základe nerozvinie prirodzene.",
                'Ak máte podozrenie na tvrdú vodu, pozrite aj článok <a href="/n/parfum-do-prania-a-tvrda-voda-preco-bielizen-nevonia-a-co-s-tym">parfum do prania a tvrdá voda</a>. Pri opakovanom zápachu skontrolujte aj <a href="/n/preco-moje-oblecenie-zapacha-po-prani-priciny-a-riesenia">prečo oblečenie zapácha po praní</a>.',
            ]),
            ("Kedy zvýšiť dávku a kedy nie", [
                "Dávku má zmysel zvýšiť vtedy, keď je bielizeň čistá, dobre vypláchnutá, rýchlo usušená a vôňa je len jemnejšia, než chcete. Ak je bielizeň zatuchnutá, tvrdá alebo lepkavá, zvýšenie dávky iba prekryje slabý proces.",
                "Pri posteľnej bielizni a uterákoch začnite opatrnejšie než pri bežných tričkách. Tieto textílie sú pri tvári a na pokožke dlhšie, preto je príjemnejšia čistá jemnosť než ťažká parfumácia.",
            ]),
        ],
        "faq": [
            ("Prečo parfum do prania necítim po vysušení?", "Často preto, že vôňa sa prirodzene oslabí pri sušení alebo ju prekrýva vlhkosť, zle vypláchnutý textil či zápach práčky."),
            ("Mám dať viac parfumu do prania?", "Až keď viete, že bielizeň je čistá, dobre vypláchnutá a rýchlo usušená. Inak najprv opravte proces prania."),
            ("Je normálne, že vôňa za mokra vonia viac?", "Áno. Mokrá bielizeň môže pôsobiť intenzívnejšie než suchá. Reálny výsledok hodnotíte až po usušení."),
            ("Čo ak vôňu necítim ja, ale cíti ju okolie?", "Čuch si na vôňu zvyká. Preto nepridávajte dávku len podľa vlastného dojmu po niekoľkých minútach."),
        ],
        "faq_title": "parfum do prania po usušení",
    },
    {
        "title": "Parfum do prania pri ručnom praní",
        "short": "Pri ručnom praní používajte parfum do prania opatrne a v malej dávke. Rozhoduje dobré vypláchnutie, jemné žmýkanie, materiál a to, či ide o spodnú bielizeň, sveter, šatku alebo jemný textil pri pokožke.",
        "focus": "Ručné pranie s vôňou",
        "setting": "ručné pranie",
        "answer": "Parfum do prania pri ručnom praní používajte len v malej dávke a až po tom, čo je textil dobre vypraný. Najdôležitejší je dôkladný oplach, pretože pri ručnom praní sa zvyšky produktu môžu vo vláknach držať ľahšie než v práčke.",
        "quick_clean": "ručné pranie musí mať jasné pranie aj oplach, nie iba namočenie vo voňavej vode.",
        "quick_dose": "začnite výrazne nižšie než pri práčke a sledujte výsledok po usušení.",
        "quick_dry": "jemné textílie sušte rozložené alebo podľa štítku, nie v zatuchnutom priestore.",
        "decision_rows": [
            ("Jemná blúzka alebo šatka", "materiál môže držať zvyšky produktu", "použiť minimum a dôkladne opláchnuť"),
            ("Spodná bielizeň", "dlhý kontakt s pokožkou", "pri citlivosti vôňu vynechať alebo výrazne znížiť"),
            ("Sveter", "vlákno môže meniť tvar a vôňu držať dlho", "nekrútiť, sušiť podľa materiálu"),
            ("Športová vec", "pot potrebuje najprv odstrániť", "vôňa až po skutočnom vypraní"),
            ("Nová vôňa", "neviete intenzitu v malej nádobe", "testovať na jednom kuse"),
        ],
        "steps": [
            "Skontrolujte štítok a uistite sa, že ručné pranie je pre daný textil vhodné.",
            "Najprv textil vyperte jemným spôsobom bez zbytočného trenia.",
            "Vodu vymeňte a textil dobre opláchnite, aby nezostal prací produkt.",
            "Parfum do prania dávkujte do čistej vody veľmi opatrne.",
            "Textil krátko premiešajte, nenechávajte ho zbytočne dlho stáť vo voňavej vode.",
            "Znova jemne opláchnite, ak je vôňa pre vás príliš výrazná.",
            "Sušte podľa materiálu a vôňu hodnotíte až po úplnom usušení.",
        ],
        "sections": [
            ("Ako dávkovať pri ručnom praní", [
                "Pri ručnom praní je objem vody menší než v práčke, preto rovnaká dávka pôsobí oveľa silnejšie. Začnite minimom. Ak sa vôňa po usušení stratí, ďalšíkrát ju mierne zvýšte, ale nerobte z ručného prania parfumovaný kúpeľ.",
                'Pre bežné dávkovanie v práčke použite ako orientáciu článok <a href="/n/ako-davkovat-parfum-do-prania-podla-mnozstva-bielizne">ako dávkovať parfum do prania podľa množstva bielizne</a>, pri ručnom praní však vždy rátajte s menším množstvom vody.',
            ]),
            ("Na ktoré textílie si dať pozor", [
                "Vlna, kašmír, hodváb, viskóza alebo jemné zmesi môžu byť citlivejšie na teplotu, trenie aj zvyšky produktu. Vôňa nie je problém sama o sebe, ale pri jemnom textile je lepšie mať čo najjednoduchší postup.",
                "Pri spodnej bielizni, šatkách a textíliách pri tvári je opatrnosť ešte dôležitejšia. Ak vôňa pri nosení ruší alebo dráždi, znížte ju alebo vynechajte.",
            ]),
            ("Ručné pranie a zápach potu", [
                "Ak je textil spotený, samotná vôňa nestačí. Pot a kožný maz treba najprv odstrániť jemným praním. Inak sa vôňa zmieša so zápachom a výsledok bude ťažší.",
                "Pri športovej syntetike býva ručné pranie len dočasné riešenie. Ak sa zápach opakuje, pomôže dôkladnejšie pranie, menšia dávka produktu a rýchle sušenie.",
            ]),
        ],
        "faq": [
            ("Môžem dať parfum do prania do ručného prania?", "Áno, ale len v malej dávke a pri textíliách, kde vám parfumácia neprekáža."),
            ("Treba parfum pri ručnom praní oplachovať?", "Ak je vôňa príliš výrazná alebo ide o citlivý textil, krátky oplach pomôže znížiť zvyšky produktu."),
            ("Je ručné pranie vhodné pre každý materiál?", "Nie. Vždy má prednosť štítok výrobcu, najmä pri vlne, hodvábe, viskóze a zmesiach."),
            ("Prečo textil po ručnom praní zatuchne?", "Najčastejšie pre pomalé sušenie, príliš veľa produktu alebo nedostatočný oplach."),
        ],
        "faq_title": "parfum do prania pri ručnom praní",
    },
    {
        "title": "Parfum do prania pri praní na 30 stupňov",
        "short": "Pri praní na 30 °C môže parfum do prania fungovať veľmi dobre, ale iba ak je bielizeň skutočne čistá. Nízka teplota vyžaduje správnu dávku gélu, menšiu náplň, dobrý oplach a rýchle sušenie.",
        "focus": "Vôňa pri praní na 30 °C",
        "setting": "pranie na 30 stupňov",
        "answer": "Parfum do prania pri praní na 30 stupňov používajte na ľahko až bežne znečistenú bielizeň, ktorá sa dobre vyperie aj pri nízkej teplote. Ak je textil spotený, mastný alebo zatuchnutý, najprv riešte prací program a čistotu, inak vôňa len prekryje slabý výsledok.",
        "quick_clean": "30 °C je vhodných najmä na ľahšie znečistenie a citlivejšie farby, nie na každý zápach.",
        "quick_dose": "pri nízkej teplote nepoužívajte vôňu ako náhradu silnejšieho pracieho procesu.",
        "quick_dry": "po nízkej teplote bielizeň vyberte hneď, aby nezačala zatuchnúť.",
        "decision_rows": [
            ("Bežné tričká", "30 °C často stačí pri ľahkom nosení", "nepreplniť a sušiť rýchlo"),
            ("Spotené oblečenie", "nízka teplota nemusí stačiť", "zvážiť vhodný program a predpranie"),
            ("Farebná bielizeň", "nižšia teplota chráni farby", "triediť a neprevoňať zvyšky gélu"),
            ("Uteráky", "30 °C nemusí byť ideál na hygienu a zápach", "riadiť sa štítkom a používaním"),
            ("Jemné vône", "môžu pôsobiť prirodzene", "testovať po usušení"),
        ],
        "steps": [
            "Triedte bielizeň podľa farby, materiálu a miery znečistenia.",
            "Na 30 °C nedávajte veľmi spotené alebo zatuchnuté veci bez predchádzajúceho riešenia.",
            "Použite primeranú dávku pracieho gélu, ktorý funguje aj pri nízkej teplote.",
            "Nechajte v bubne priestor, aby sa bielizeň dobre opláchla.",
            "Parfum do prania pridajte v primeranej dávke a sledujte výsledok po usušení.",
            "Bielizeň vyberte hneď po praní.",
            "Ak sa zápach vracia, upravte program a dávkovanie, nie len vôňu.",
        ],
        "sections": [
            ("Kedy 30 stupňov stačí", [
                "Pranie na 30 stupňov dáva zmysel pri ľahko nosenom oblečení, farbách, jemnejších materiáloch a situáciách, kde nechcete zbytočne zaťažovať textil. Vtedy môže vôňa pôsobiť veľmi prirodzene, pretože bielizeň nie je výrazne znečistená.",
                "Ak však tričko zapácha po pote alebo uterák zostal vlhký, nízka teplota nemusí stačiť. Vôňa potom zakryje problém len na chvíľu a pri nosení alebo zahriatí sa zápach môže vrátiť.",
            ]),
            ("Prací gél pri nízkej teplote a vôňa", [
                'Pri nízkej teplote je dôležité, aby prací prostriedok pracoval dobre aj v chladnejšej vode. Preto vyberajte vhodný <a href="/c/vevo-home-care/pranie/praci-gel">prací gél</a> a sledujte, či dávka, čas a oplach fungujú lepšie než silná parfumácia.',
                "Ak je pracieho gélu priveľa, môže zostať vo vláknach a brániť ľahkému pocitu čistoty. Vôňa potom nepôsobí sviežo, ale ťažko.",
            ]),
            ("Ako kombinovať 30 stupňov, farby a vôňu", [
                "Farebné oblečenie perte naruby, triedené a bez preplnenia. Vôňu voľte jemnejšiu, aby neprebíjala charakter textilu. Pri nových farbách najprv sledujte púšťanie farby a nepoužívajte zbytočne veľa produktov naraz.",
                "Ak periete oblečenie pri pokožke, napríklad tričká alebo spodné vrstvy, vôňa by mala byť príjemná aj po celom dni nosenia. Menej je v tomto prípade často praktickejšie než viac.",
            ]),
        ],
        "faq": [
            ("Funguje parfum do prania pri 30 stupňoch?", "Áno, ak je bielizeň vhodná na nízku teplotu a dobre sa vyperie. Vôňa však nenahrádza čistotu."),
            ("Prečo bielizeň pri 30 stupňoch nevonia?", "Môže byť preplnený bubon, slabý oplach, príliš veľa gélu alebo príliš znečistený textil na daný program."),
            ("Je 30 stupňov vhodných na uteráky?", "Závisí od štítku a použitia. Pri vlhkých alebo zatuchnutých uterákoch často treba dôkladnejší program."),
            ("Môžem pridať viac vône pri nízkej teplote?", "Najprv overte, že pranie funguje. Až potom dávku vône zvyšujte postupne."),
        ],
        "faq_title": "parfum do prania pri 30 stupňoch",
    },
    {
        "title": "Parfum do prania pri praní na 60 stupňov",
        "short": "Pri praní na 60 °C sa často perú uteráky, posteľná bielizeň alebo hygienicky náročnejší textil. Parfum do prania má zmysel až po dobrom praní, primeranom oplachu a správnom sušení.",
        "focus": "Vôňa pri praní na 60 °C",
        "setting": "pranie na 60 stupňov",
        "answer": "Parfum do prania pri praní na 60 stupňov používajte najmä vtedy, keď textil túto teplotu povoľuje a cieľom je čistý, svieži výsledok. Pri uterákoch a posteľnej bielizni dávkujte opatrne, aby vôňa nepôsobila ťažko a neprekrývala problém so zatuchnutím.",
        "quick_clean": "60 °C má zmysel len pri textile, ktorý túto teplotu povoľuje.",
        "quick_dose": "pri uterákoch a posteľnej bielizni začnite jemne, lebo textil je blízko pokožky.",
        "quick_dry": "hrubý textil po 60 °C potrebuje rýchle a úplné sušenie.",
        "decision_rows": [
            ("Uteráky", "60 °C môže pomôcť pri hygiene, ak to štítok dovolí", "nepreplniť a dobre usušiť"),
            ("Posteľná bielizeň", "vyššia teplota môže byť vhodná pri potrebe dôkladnejšieho prania", "voliť jemnejšiu vôňu"),
            ("Biele bavlnené veci", "lepšie znášajú vyššiu teplotu než jemné zmesi", "sledovať štítok a zrážanie"),
            ("Farebné a jemné textílie", "riziko blednutia alebo poškodenia", "60 °C nepoužiť bez odporúčania"),
            ("Zatuchnutie", "vyššia teplota sama nemusí stačiť", "riešiť sušenie a práčku"),
        ],
        "steps": [
            "Skontrolujte štítok a neperte na 60 °C textil, ktorý to neznesie.",
            "Triedte uteráky, posteľnú bielizeň a oblečenie oddelene.",
            "Neplňte bubon nadoraz, hlavne pri hrubom froté.",
            "Použite primeranú dávku pracieho prostriedku.",
            "Parfum do prania dávkujte opatrne, najmä pri textiliách pri tvári.",
            "Po doprání bielizeň vyberte a úplne usušte.",
            "Ak textil stále nevonia, skontrolujte práčku a spôsob sušenia.",
        ],
        "sections": [
            ("Kedy má 60 stupňov zmysel", [
                "Vyššia teplota sa často spája s uterákmi, bavlnenou posteľnou bielizňou alebo textilom, kde chcete dôkladnejšie pranie. Nemá však byť automatická voľba pre všetko. Mnohé moderné materiály, farby a zmesi si vyššiu teplotu nezaslúžia.",
                "Ak štítok povoľuje 60 °C, stále platí, že bielizeň musí mať v bubne priestor. Objemné uteráky a obliečky sa pri preplnení nevyperú rovnomerne ani pri vyššej teplote.",
            ]),
            ("Vôňa pri uterákoch a posteľnej bielizni", [
                "Uteráky a posteľ sú v priamom kontakte s pokožkou, preto je lepšia čistá a primeraná vôňa. Príliš výrazná parfumácia môže pri spánku alebo utieraní pôsobiť rušivo.",
                'Ak riešite uteráky, pozrite aj návod <a href="/n/ako-prat-uteraky-rady-a-tipy-na-ciste-a-maekke-uteraky">ako prať uteráky</a> alebo článok <a href="/n/parfum-do-prania-pri-uterakoch-ako-zachovat-savost-a-vonu">parfum do prania pri uterákoch</a>. Pri posteľnej bielizni je dôležité aj úplné sušenie pred uložením.',
            ]),
            ("Prečo 60 stupňov nevyrieši všetko", [
                "Ak je práčka zanesená, zásobník zapácha alebo bielizeň schne dva dni v kúpeľni, vyššia teplota sama nestačí. Zápach sa môže vrátiť aj po dôkladnejšom programe.",
                "Pri opakovanom probléme skontrolujte filter, tesnenie a zásobník. Vôňa má fungovať na čistý textil, nie ako náhrada údržby práčky.",
            ]),
        ],
        "faq": [
            ("Môžem používať parfum do prania pri 60 stupňoch?", "Áno, ak textil 60 °C povoľuje a vôňu dávkujete primerane."),
            ("Je 60 stupňov vhodných na všetku bielizeň?", "Nie. Vždy rozhoduje štítok, farba, materiál a konštrukcia textilu."),
            ("Prečo uteráky po 60 stupňoch stále zapáchajú?", "Môže byť preplnený bubon, zanesená práčka alebo pomalé sušenie."),
            ("Akú vôňu zvoliť na posteľnú bielizeň?", "Skôr jemnú a čistú, aby pri spánku nerušila."),
        ],
        "faq_title": "parfum do prania pri 60 stupňoch",
    },
]


DETERGENT_TOPICS = [
    {
        "title": "Prací gél alebo prášok: čo sa hodí na bežné pranie",
        "short": "Prací gél sa hodí na veľkú časť bežného prania, najmä pri farebnej bielizni a nižších teplotách. Prášok môže dávať zmysel pri niektorých bielych alebo silnejšie znečistených veciach. Rozhoduje typ textilu, škvrny, teplota a oplach.",
        "answer": "Na bežné pranie je praktický prací gél, lebo sa ľahko dávkuje, dobre sa používa pri nižších teplotách a je vhodný pre veľa farebnej bielizne. Prášok môže byť vhodnejší pri niektorých bielych a odolných textíliách. Najlepšia voľba závisí od farby, materiálu, znečistenia, teploty a citlivosti pokožky.",
        "quick_main": "gél je univerzálny každodenný základ, prášok niekedy dáva zmysel pri bielej a odolnej bielizni.",
        "intro_focus": "Rozhodovanie medzi pracím gélom a práškom nie je o tom, ktorý produkt je vždy lepší.",
        "why_heading": "Prečo neexistuje jeden víťaz pre každé pranie",
        "why": [
            "Prací gél a prášok sa líšia formou, rozpúšťaním, dávkovaním a praktickým použitím. Gél je pohodlný pri bežnom praní, najmä ak periete častejšie, pri nižších teplotách alebo nechcete riešiť nerozpustené zvyšky. Prášok môže byť užitočný pri odolnejšej bielej bielizni, ale treba ho dobre rozpustiť a vypláchnuť.",
            "Dôležité je aj to, čo periete. Farebné tričká, tmavé nohavice, posteľná bielizeň, uteráky a pracovné veci majú rozdielne potreby. Ak používate jeden produkt na všetko bez triedenia, výsledok bude raz dobrý a inokedy slabý.",
            "Rozhoduje aj teplota. Pri nízkych teplotách sa niektoré práškové zvyšky môžu horšie rozpúšťať, najmä pri rýchlom programe a preplnenom bubne. Pri géli zase treba dávať pozor na prehnané dávkovanie, ktoré môže zanechať film.",
        ],
        "decision_rows": [
            ("Farebné oblečenie", "časté bežné pranie a ochrana farieb", "často praktickejší gél"),
            ("Biela odolná bavlna", "niekedy potrebuje dôkladnejšie pranie", "zvážiť prášok alebo špeciálny postup"),
            ("Nízka teplota", "produkt sa musí dobre rozptýliť", "gél býva praktický"),
            ("Citlivá pokožka", "dôležitý je oplach a jednoduchosť rutiny", "jemný gél a dôkladný oplach"),
            ("Silné škvrny", "bežný produkt nemusí stačiť", "riešiť škvrnu pred praním"),
        ],
        "steps": [
            "Rozdeľte bielizeň podľa farby, materiálu a miery znečistenia.",
            "Na bežnú farebnú bielizeň zvoľte prací gél a držte sa dávkovania.",
            "Pri bielej a odolnej bielizni zvážte, či nestačí gél alebo treba špecifickejší postup.",
            "Pri nízkych teplotách nepreplňte bubon a sledujte oplach.",
            "Škvrny riešte pred praním, nie zvýšením dávky hlavného produktu.",
            "Ak bielizeň po praní nevonia, skontrolujte práčku a sušenie.",
            "Vôňu pridávajte až vtedy, keď je výsledok čistý.",
        ],
        "check_rows": [
            ("Gél", "priveľa produktu a slabý oplach", "dávkovať presne"),
            ("Prášok", "nerozpustené zvyšky pri nízkej teplote", "použiť vhodný program"),
            ("Farebné veci", "blednutie a zvyšky", "triediť a prať naruby"),
            ("Biele veci", "šednutie alebo škvrny", "riešiť špinavosť, nie len vôňu"),
            ("Citlivá pokožka", "zvyšky vo vláknach", "pridať oplach"),
        ],
        "sections": [
            ("Kedy je lepší prací gél", [
                "Prací gél je praktický pri bežnom praní tričiek, spodnej bielizne, farebných vecí, syntetiky a textilu, ktorý periete často. Dobre sa dávkuje a v domácnosti je prehľadný.",
                "Dáva zmysel najmä vtedy, keď periete pri nižších alebo bežných teplotách a chcete mať jednoduchú rutinu. Stále však platí, že množstvo treba prispôsobiť náplni a vode.",
            ]),
            ("Kedy môže dávať zmysel prášok", [
                "Prášok môže byť vhodný pri odolnej bielej bielizni alebo pri situáciách, kde potrebujete iný typ účinku. Nie je však automaticky lepší na všetko. Pri tmavých veciach a nízkych teplotách môže byť dôležitejší dobrý oplach.",
                "Ak používate prášok a vidíte zvyšky na oblečení, skontrolujte program, dávku, tvrdosť vody a plnosť bubna. Problém nemusí byť v značke, ale v podmienkach prania.",
            ]),
            ("Ako do rozhodovania zapojiť vôňu", [
                "Vôňa nerieši rozdiel medzi gélom a práškom. Najprv si vyberte produkt, ktorý textil vyperie a dobre sa vypláchne. Až potom pridajte vôňu, ak chcete predĺžiť pocit čistoty.",
                'Pri vôni začnite opatrne a pozrite si kategóriu <a href="/c/vevo-fragrance/parfum-do-prania">parfumy do prania</a>. Vôňa by nemala slúžiť na maskovanie zle vypranej bielizne.',
            ]),
        ],
        "faq": [
            ("Je lepší prací gél alebo prášok?", "Na bežné farebné pranie je často praktickejší gél, pri niektorých bielych a odolných veciach môže dávať zmysel prášok."),
            ("Môžem používať iba prací gél?", "Áno, pri veľkej časti bežného prania. Pri špecifických škvrnách alebo bielej bielizni však treba zvoliť vhodný postup."),
            ("Prečo mám na oblečení zvyšky po praní?", "Môže ísť o priveľa produktu, nízku teplotu, krátky program alebo preplnenú práčku."),
            ("Kedy pridať parfum do prania?", "Až keď je bielizeň čistá a dobre vypláchnutá."),
        ],
        "faq_title": "prací gél alebo prášok",
    },
    {
        "title": "Prací gél na čierne oblečenie",
        "short": "Čierne oblečenie potrebuje triedenie, pranie naruby, primeranú dávku gélu a dobrý oplach. Najväčší nepriateľ čiernej nie je len farba, ale aj práškové zvyšky, tvrdá voda, trenie a preplnený bubon.",
        "answer": "Na čierne oblečenie používajte prací gél, ktorý sa dobre rozptýli, perte naruby, pri nižšej teplote podľa štítku a nepreplňte bubon. Dôležité je neprehnať dávku, aby na tmavom textile nezostali mapy alebo matný film.",
        "quick_main": "čierne oblečenie perte naruby, s menšou dávkou a dobrým oplachom.",
        "intro_focus": "Čierne oblečenie vie ukázať každú chybu v praní: prach, žmolky, mapy, zvyšky gélu aj blednutie.",
        "why_heading": "Prečo čierne oblečenie po praní bledne alebo má mapy",
        "why": [
            "Tmavý textil zvýrazní všetko, čo by na svetlom oblečení nebolo vidieť: minerálne zvyšky, nerozptýlený prací prostriedok, vlákna z inej bielizne alebo prach. Preto je pri čiernom praní taký dôležitý oplach a triedenie.",
            "Blednutie spôsobuje kombinácia trenia, teploty, času, nevhodného programu a opakovaného prania. Prací gél môže byť praktický, lebo sa používa jednoducho, ale nezachráni čiernu farbu, ak periete spolu uteráky, svetlé veci a drsný textil.",
            "Čierne oblečenie často nepotrebuje silnejšiu dávku, ale jemnejší postup. Menej trenia, menšia náplň, pranie naruby a rýchle sušenie mimo priameho ostrého slnka sú dôležitejšie než veľké množstvo produktu.",
        ],
        "decision_rows": [
            ("Čierne tričká", "riziko blednutia a mapiek", "prať naruby a nepreplniť"),
            ("Čierne rifle", "trenie a púšťanie farby", "oddeliť od jemných vecí"),
            ("Čierna syntetika", "pach a statika", "riešiť pot, nie len vôňu"),
            ("Čierne mikiny", "žmolky a vlákna", "prať s podobnými materiálmi"),
            ("Nové čierne veci", "púšťanie farby", "prať samostatne alebo s podobnými farbami"),
        ],
        "steps": [
            "Otočte čierne oblečenie naruby.",
            "Oddeľte ho od uterákov, svetlých vecí a textilu, ktorý púšťa vlákna.",
            "Použite primeranú dávku pracieho gélu.",
            "Vyberte program podľa štítku a miery znečistenia.",
            "Neplňte bubon nadoraz, aby sa gél dobre vypláchol.",
            "Po praní oblečenie vyberte a sušte mimo ostrého priameho slnka.",
            "Ak sa objavia mapy, ďalšíkrát znížte dávku a pridajte oplach.",
        ],
        "check_rows": [
            ("Mapy na čiernom", "zvyšky produktu alebo tvrdá voda", "menej gélu, lepší oplach"),
            ("Blednutie", "teplo, trenie, časté pranie", "prať naruby a šetrnejšie"),
            ("Žmolky", "miešanie materiálov", "triediť textil"),
            ("Pach", "pot vo vláknach", "prať skôr a nepreplniť"),
            ("Statika", "syntetika a sušenie", "nepresúšať a voliť vhodný postup"),
        ],
        "sections": [
            ("Ako zabrániť mapám na čiernom oblečení", [
                "Mapy často vzniknú z prebytku pracieho prostriedku alebo z toho, že sa textil v preplnenom bubne dobre neopláchol. Na čiernom textile je tenký film viditeľný okamžite.",
                "Riešením je menšia dávka gélu, voľnejší bubon a prípadne extra oplach. Ak mapy vznikajú opakovane, skontrolujte aj zásobník práčky.",
            ]),
            ("Ako prať čierne oblečenie, aby nebledlo", [
                "Blednutie spomaľuje pranie naruby, šetrnejší program a triedenie podľa farieb. Dôležité je aj neprepierať veci po jednom krátkom nosení, ak stačí vyvetranie.",
                "Čierne rifle a hrubšie kusy perte oddelene od jemných čiernych tričiek. Hrubý textil zvyšuje trenie a môže urýchliť opotrebovanie povrchu.",
            ]),
            ("Vôňa pri čiernom oblečení", [
                "Pri čiernom oblečení má vôňa dávať pocit čistoty, ale nesmie prekrývať zvyšky produktu. Ak sú na textile mapy, najprv riešte oplach.",
                'Až pri čistom výsledku má zmysel pridať jemnú vôňu. K tejto téme sa hodia <a href="/c/vevo-fragrance/parfum-do-prania">parfumy do prania</a> v nižšej intenzite.',
            ]),
        ],
        "faq": [
            ("Aký prací gél použiť na čierne oblečenie?", "Taký, ktorý sa dobre dávkuje a vyplachuje. Dôležitejší než silná vôňa je správny postup."),
            ("Prečo mám biele mapy na čiernom oblečení?", "Často ide o zvyšky pracieho prostriedku, tvrdú vodu alebo preplnený bubon."),
            ("Môžem čierne oblečenie prať na 30 stupňov?", "Ak to štítok a znečistenie dovoľujú, áno. Pri pote alebo zápachu treba zvoliť účinnejší postup."),
            ("Ako prevoňať čierne oblečenie?", "Až po dobrom praní a oplachu, radšej jemnejšou dávkou vône."),
        ],
        "faq_title": "prací gél na čierne oblečenie",
    },
    {
        "title": "Prací gél na biele oblečenie",
        "short": "Biele oblečenie potrebuje správne triedenie, riešenie škvŕn pred praním, primeranú teplotu a dobrý oplach. Prací gél môže byť praktický základ, ale pri zašednutí alebo škvrnách treba riešiť aj príčinu.",
        "answer": "Prací gél na biele oblečenie používajte pri bežnom praní v správnej dávke a s dostatočným oplachom. Ak biela šedne alebo má škvrny, nestačí pridať viac gélu. Skontrolujte triedenie, tvrdosť vody, teplotu, preplnený bubon a predpranie škvŕn.",
        "quick_main": "biele oblečenie potrebuje čisté triedenie a riešenie škvŕn pred praním.",
        "intro_focus": "Biele oblečenie je citlivé na zašednutie, mapy, škvrny a zvyšky pracieho prostriedku.",
        "why_heading": "Prečo biele oblečenie šedne alebo nevonia",
        "why": [
            "Biela bielizeň šedne najmä vtedy, keď sa perie so zle vytriedenými farbami, v preplnenom bubne alebo s príliš veľkým množstvom produktu. Nečistoty sa potom znovu usádzajú na textile a biela stratí jas.",
            "Škvrny na bielom oblečení treba riešiť pred praním. Ak zaschnutú mastnotu, pot, make-up alebo jedlo hodíte rovno do bubna, bežný prací gél nemusí stačiť. Vyššia dávka gélu nie je náhrada za lokálne ošetrenie škvrny.",
            "Dôležitý je aj stav práčky. Zanesený zásobník, zápach v tesnení alebo špinavý filter môžu zhoršiť výsledok prania. Biele veci potom nevyzerajú čisto ani pri správnom produkte.",
        ],
        "decision_rows": [
            ("Biele tričká", "pot a deodorant", "riešiť miesta pod pazuchami pred praním"),
            ("Biela spodná bielizeň", "kontakt s pokožkou", "dôkladný oplach a správna teplota"),
            ("Biele uteráky", "objem a savosť", "nepreplniť a dobre sušiť"),
            ("Biele košele", "golier a manžety", "predošetriť mastnotu"),
            ("Zašednuté veci", "opakovane slabý proces", "upraviť triedenie a náplň"),
        ],
        "steps": [
            "Triedte biele veci od farebných a tmavých.",
            "Škvrny ošetrite pred praním, najmä pot, mastnotu a kozmetiku.",
            "Vyberte program podľa materiálu a štítku.",
            "Použite primeranú dávku pracieho gélu.",
            "Bubon nepreplňte, aby sa biele veci dobre opláchli.",
            "Po praní bielizeň rýchlo vyberte a sušte vzdušne.",
            "Ak biela šedne opakovane, skontrolujte práčku a tvrdosť vody.",
        ],
        "check_rows": [
            ("Potné mapy", "zvyšky deodorantu a potu", "predošetrenie pred praním"),
            ("Zašednutie", "miešanie farieb a slabý oplach", "triediť a nepreplniť"),
            ("Mastné škvrny", "bežné pranie nestačí", "riešiť lokálne"),
            ("Tvrdé uteráky", "zvyšky produktu alebo tvrdá voda", "menej gélu a oplach"),
            ("Zápach", "práčka alebo sušenie", "skontrolovať spotrebič"),
        ],
        "sections": [
            ("Ako prať biele tričká", [
                "Biele tričká často trpia v podpazuší a na golieri. Tieto miesta obsahujú pot, kožný maz a zvyšky deodorantu. Ak ich neošetríte, škvrna sa pri opakovanom praní fixuje.",
                "Perte ich naruby alebo podľa štítku, nepreplňte bubon a nepoužívajte viac gélu len preto, že chcete silnejšiu vôňu. Vôňa má nasledovať až po odstránení potu.",
            ]),
            ("Ako zabrániť zašednutiu bielej", [
                "Zašednutie je často výsledok malých opakovaných chýb: miešanie s farbami, krátky program, preplnený bubon, priveľa produktu a pomalé sušenie. Jedna zmena nemusí stačiť, preto sledujte celý postup.",
                "Ak biela bielizeň stratila jas, najprv upravte rutinu. Prací gél sám o sebe nezmení bielizeň, ktorá sa dlhodobo perie v zlej kombinácii.",
            ]),
            ("Biela bielizeň a vôňa", [
                "Pri bielom oblečení je lákavé pridať viac vône, aby pôsobilo čistejšie. Ak však zostali škvrny alebo šedý nádych, vôňa môže pôsobiť umelo.",
                'Najprv riešte čistotu a až potom jemné prevoňanie. K tomu sa hodí kategória <a href="/c/vevo-fragrance/parfum-do-prania">parfumy do prania</a>, najmä pri posteľnej bielizni alebo bežných tričkách.',
            ]),
        ],
        "faq": [
            ("Je prací gél vhodný na biele oblečenie?", "Áno, pri bežnom praní môže byť praktický. Pri škvrnách alebo zašednutí však treba riešiť aj príčinu."),
            ("Prečo biele oblečenie šedne?", "Najčastejšie pre zlé triedenie, preplnený bubon, slabý oplach alebo opakované usádzanie nečistôt."),
            ("Mám použiť viac gélu na biele veci?", "Nie automaticky. Priveľa gélu môže zhoršiť oplach a zanechať film."),
            ("Kedy pridať vôňu do bielej bielizne?", "Až keď je čistá, vypláchnutá a bez zatuchnutia."),
        ],
        "faq_title": "prací gél na biele oblečenie",
    },
    {
        "title": "Prací gél na farebné oblečenie",
        "short": "Farebné oblečenie perte triedené, naruby, s primeranou dávkou gélu a podľa štítku. Farby najviac ničia zlé triedenie, vysoká teplota, trenie, preplnený bubon a pomalé sušenie.",
        "answer": "Na farebné oblečenie sa hodí prací gél pri správnej dávke, triedení a teplote podľa štítku. Perte naruby, nepreplňte bubon a nové farby najprv perte opatrne. Vôňu pridávajte až po tom, keď farby nepúšťajú a bielizeň je dobre vypláchnutá.",
        "quick_main": "farebné oblečenie chráni triedenie, pranie naruby a primeraná teplota.",
        "intro_focus": "Farebné oblečenie potrebuje rovnováhu medzi čistotou a ochranou farieb.",
        "why_heading": "Prečo farebné oblečenie púšťa farbu alebo bledne",
        "why": [
            "Farby blednú najmä vplyvom trenia, tepla, opakovaného prania a nevhodného miešania textílií. Nové oblečenie môže púšťať farbu aj vtedy, keď je prané správne, preto je prvé pranie dôležité.",
            "Prací gél je pri farebnej bielizni praktický, ale nesmie ho byť priveľa. Nadmerná dávka môže zostať vo vláknach a zhoršiť pocit z textilu. Pri farebných veciach sa zvyšky často prejavia ako matný povrch.",
            "Farebná bielizeň sa nesmie posudzovať iba podľa vône. Ak farby púšťajú, textil je zle vypláchnutý alebo zostal vlhký v práčke, vôňa nebude pôsobiť prirodzene.",
        ],
        "decision_rows": [
            ("Nové farebné oblečenie", "môže púšťať farbu", "prať samostatne alebo s podobnými farbami"),
            ("Jasné farby", "blednutie pri teple a trení", "prať naruby a podľa štítku"),
            ("Tmavé farby", "mapy po produkte", "menej gélu a lepší oplach"),
            ("Zmesové materiály", "rôzna citlivosť vlákien", "riadiť sa najcitlivejšou zložkou"),
            ("Farebná posteľná bielizeň", "veľký objem", "nepreplniť bubon"),
        ],
        "steps": [
            "Rozdeľte farby na tmavé, jasné a svetlé.",
            "Nové farby perte prvýkrát opatrne a oddelene.",
            "Oblečenie otočte naruby.",
            "Dávkujte prací gél podľa náplne a vody.",
            "Zvoľte teplotu podľa štítku, nie podľa zvyku.",
            "Nenechávajte farebnú bielizeň dlho mokrú v práčke.",
            "Vôňu testujte najprv na menšej dávke.",
        ],
        "check_rows": [
            ("Púšťanie farby", "nový alebo nestály textil", "prať oddelene"),
            ("Blednutie", "teplo a trenie", "naruby a šetrnejšie"),
            ("Matný povrch", "zvyšky produktu", "menej gélu"),
            ("Zápach", "pomalé sušenie", "rýchlo vybrať z práčky"),
            ("Žmolky", "miešanie materiálov", "triediť podľa textilu"),
        ],
        "sections": [
            ("Ako prať nové farebné oblečenie", [
                "Nové farebné veci perte prvýkrát s podobnými farbami alebo samostatne. Aj kvalitný textil môže pri prvom praní pustiť trochu farby. Ak ho vložíte medzi svetlé veci, problém je hotový.",
                "Použite šetrnejší program, primeranú dávku gélu a nepreplňte bubon. Po praní oblečenie vyberte a nenechajte farby stáť vo vlhku.",
            ]),
            ("Ako udržať farby dlhšie sýte", [
                "Pranie naruby znižuje trenie vonkajšej strany. Pomáha aj triedenie podobných materiálov, pretože hrubé kusy môžu jemný farebný textil zbytočne opotrebovať.",
                "Vyššia teplota nie je automaticky lepšia. Pri bežnom nosení často stačí nižšia teplota podľa štítku, ale pri zápachu alebo pote treba zvoliť postup, ktorý neobetuje čistotu.",
            ]),
            ("Farebné oblečenie a vôňa", [
                "Pri farebnom oblečení má vôňa pôsobiť ako jemný záver. Ak je textil zle vypláchnutý alebo púšťa farbu, vôňa nebude hlavný problém.",
                'Keď je pranie stabilné, môžete skúsiť jemné <a href="/c/vevo-fragrance/parfum-do-prania">parfumy do prania</a>. Začnite nižšou dávkou, hlavne pri tričkách a textíliách pri pokožke.',
            ]),
        ],
        "faq": [
            ("Je prací gél vhodný na farebné oblečenie?", "Áno, pri správnej dávke a triedení je praktický pre veľa farebnej bielizne."),
            ("Ako prať nové farebné veci?", "Prvýkrát samostatne alebo s podobnými farbami, naruby a podľa štítku."),
            ("Prečo farby blednú?", "Najmä pre trenie, teplo, časté pranie, zlé triedenie a nevhodný program."),
            ("Môžem pridať parfum do prania?", "Áno, ale až keď farby nepúšťajú a bielizeň je dobre vypláchnutá."),
        ],
        "faq_title": "prací gél na farebné oblečenie",
    },
    {
        "title": "Prací gél pri nízkych teplotách",
        "short": "Pri nízkych teplotách rozhoduje správny prací gél, menšia náplň, dostatočný čas a dobrý oplach. Nízka teplota chráni niektoré textílie, ale nie je vhodná na každý zápach alebo silné znečistenie.",
        "answer": "Prací gél pri nízkych teplotách používajte na ľahko až bežne znečistenú bielizeň, kde to povoľuje štítok a program. Neperte pri nízkej teplote všetko naslepo: pot, mastnota, uteráky alebo zatuchnuté veci môžu potrebovať dôkladnejší postup.",
        "quick_main": "nízka teplota funguje len vtedy, keď sedí textilu aj miere znečistenia.",
        "intro_focus": "Nízke teploty sú praktické, ale ľahko zvádzajú k tomu, že sa nimi perie úplne všetko.",
        "why_heading": "Prečo nízka teplota niekedy nestačí",
        "why": [
            "Pri nízkej teplote sa niektoré nečistoty uvoľňujú pomalšie. Pot, kožný maz, mastnota alebo zatuchnutie potrebujú viac než len krátky program a voňavý produkt. Ak textil nie je vypraný, vôňa sa rýchlo stratí alebo zmení.",
            "Prací gél je pri nízkych teplotách praktický, pretože sa dobre dávkuje a používa. Stále však potrebuje dostatok vody, času a pohybu. Preplnený bubon pokazí výsledok aj pri dobrom géli.",
            "Nízka teplota chráni niektoré farby a materiály, ale nemá byť univerzálnou odpoveďou. Rozlišujte ľahko nosené tričká, spotené športové veci, uteráky, posteľnú bielizeň a detský textil.",
        ],
        "decision_rows": [
            ("Ľahko nosené oblečenie", "nízka teplota často stačí", "nepreplniť bubon"),
            ("Spotené tričká", "pach sa môže vrátiť", "zvoliť účinnejší program"),
            ("Farebné veci", "nižšia teplota chráni farby", "triediť a prať naruby"),
            ("Uteráky", "vlhkosť a zápach", "riadiť sa štítkom a hygienou"),
            ("Krátky program", "menej času na pranie aj oplach", "použiť len pri ľahkej náplni"),
        ],
        "steps": [
            "Vyhodnoťte, či je bielizeň ľahko alebo výrazne znečistená.",
            "Triedte podľa farby, materiálu a zápachu.",
            "Použite prací gél vhodný na danú teplotu.",
            "Nedávajte veľkú dávku bielizne do krátkeho programu.",
            "Pri nízkej teplote venujte pozornosť oplachu.",
            "Bielizeň vyberte hneď po praní a sušte vzdušne.",
            "Ak sa zápach vracia, neperte daný textil stále na nízku teplotu.",
        ],
        "check_rows": [
            ("30 °C", "ľahšie pranie", "vhodné pre bežné veci"),
            ("Studenšia voda", "slabšie uvoľnenie mastnoty", "nepoužiť na všetko"),
            ("Krátky program", "málo času", "len na malé ľahké dávky"),
            ("Syntetika", "pach potu", "prať skôr a dobre sušiť"),
            ("Citlivá pokožka", "zvyšky produktu", "pridať oplach"),
        ],
        "sections": [
            ("Kedy nízka teplota funguje dobre", [
                "Nízka teplota je vhodná pri ľahko nosenom oblečení, citlivejších farbách a textíliách, ktoré výrobca neodporúča prať teplejšie. Výhodou je šetrnosť k farbám a menšie tepelné namáhanie.",
                "Aby fungovala, nesmie byť bubon preplnený a program nesmie byť príliš krátky na danú náplň. Pri nízkej teplote sú mechanický pohyb a čas ešte dôležitejšie.",
            ]),
            ("Kedy nízka teplota nestačí", [
                "Ak oblečenie výrazne zapácha po pote, zostalo vlhké v taške alebo ide o uteráky po kúpaní, nízka teplota nemusí priniesť svieži výsledok. Vtedy treba zvoliť účinnejší program alebo riešiť zápach pred praním.",
                "Pri mastných škvrnách tiež nestačí len pridať viac gélu. Škvrnu treba ošetriť a zvoliť postup podľa materiálu.",
            ]),
            ("Nízka teplota a vôňa bielizne", [
                "Pri nízkej teplote sa vôňa najlepšie ukáže na bielizni, ktorá bola skutočne čistá. Ak sa zápach len prekryje, po usušení alebo nosení sa vráti.",
                'Keď máte prací proces stabilný, môžete vôňu doladiť cez <a href="/c/vevo-fragrance/parfum-do-prania">parfumy do prania</a>. Pri nízkych teplotách začnite jemne a sledujte suchý výsledok.',
            ]),
        ],
        "faq": [
            ("Je prací gél vhodný na nízke teploty?", "Áno, často je praktický, ale treba dodržať dávkovanie, náplň a vhodný program."),
            ("Môžem všetko prať na 30 stupňov?", "Nie. Silno spotené, mastné alebo zatuchnuté veci môžu potrebovať dôkladnejší postup."),
            ("Prečo bielizeň pri nízkej teplote nevonia?", "Môže byť zle vypraná, preplnená, zle opláchnutá alebo pomaly sušená."),
            ("Pomôže viac parfumu do prania?", "Nie, ak problém je v nedostatočnom praní. Najprv opravte proces."),
        ],
        "faq_title": "prací gél pri nízkych teplotách",
    },
]


ARTICLES = [*(fragrance_article(topic) for topic in FRAGRANCE_TOPICS), *(detergent_article(topic) for topic in DETERGENT_TOPICS), washer_article({
    "title": "Preplnená práčka: ako kazí pranie",
    "short": "Preplnená práčka zhorší pohyb textilu, rozpustenie pracieho prostriedku aj oplach. Výsledkom môže byť zápach, tvrdá bielizeň, mapy na tmavom oblečení a slabá vôňa aj po použití dobrého pracieho gélu.",
    "answer": "Preplnená práčka kazí pranie tým, že bielizeň sa v bubne nemá kde pohybovať. Voda, prací gél a oplach sa nedostanú rovnomerne medzi textil, takže zostáva pot, špina aj zvyšky produktu. Najprv zmenšite náplň, až potom riešte silnejší produkt alebo vôňu.",
})]


def render_article(article):
    parts = [
        f"<p><strong>Rýchla odpoveď:</strong> {article['answer']}</p>",
        callout("Rýchla orientácia", article["quick"], background="#f7fbff", border="#d7e2ec"),
    ]
    parts.extend(f"<p>{paragraph}</p>" for paragraph in article["intro"])
    parts.append(f"<h2>{esc(article['why_heading'])}</h2>")
    parts.extend(f"<p>{paragraph}</p>" for paragraph in article["why"])
    parts.append(f"<h2>{esc(article['decision_heading'])}</h2>")
    parts.append(table(["Situácia", "Čo sa deje", "Rozumný postup"], article["decision_rows"]))
    parts.append("<h2>Postup krok za krokom</h2>")
    parts.append("<ol>" + "".join(f"<li>{step}</li>" for step in article["steps"]) + "</ol>")
    parts.append("<h2>Kontrolná tabuľka podľa textilu alebo príčiny</h2>")
    parts.append(table(["Textil alebo problém", "Riziko", "Praktická poznámka"], article["check_rows"]))
    parts.append("<h2>Najčastejšie chyby</h2>")
    parts.append("<ul>" + "".join(f"<li>{item}</li>" for item in article["mistakes"]) + "</ul>")
    for heading, paragraphs in article["sections"]:
        parts.append(f"<h2>{esc(heading)}</h2>")
        for paragraph in paragraphs:
            parts.append(f"<p>{paragraph}</p>")
    parts.append(callout("Najdôležitejšie pravidlo", article["rule"], background="#fffaf5", border="#e6ded2"))
    parts.append("<h2>Kedy byť opatrný</h2>")
    parts.extend(f"<p>{paragraph}</p>" for paragraph in article["caution"])
    parts.append("<h2>Odbornejší pohľad</h2>")
    parts.extend(f"<p>{paragraph}</p>" for paragraph in article["expert"])
    parts.append(source_box(article))
    parts.append(product_block(article))
    parts.append(related_links(article["related"]))
    parts.append(faq(article["faq"], article["faq_title"]))
    return "\n".join(parts)


def hrefs(markup):
    return re.findall(r'href="([^"]+)"', markup)


def preflight_links(articles):
    rows = []
    seen = set()
    for article in articles:
        for href in hrefs(article["long"]):
            url = urljoin(BASE, href)
            if url in seen:
                continue
            seen.add(url)
            status = None
            error = None
            try:
                response = requests.get(url, timeout=25, allow_redirects=True)
                status = response.status_code
                ok = 200 <= status < 400
            except Exception as exc:  # pragma: no cover
                ok = False
                error = str(exc)
            rows.append({"url": url, "ok": ok, "status": status, "error": error})
    return {"checked_count": len(rows), "failure_count": sum(1 for row in rows if not row["ok"]), "links": rows}


def main():
    rendered = []
    for index, article in enumerate(ARTICLES):
        long = render_article(article)
        for field in ("title", "short"):
            hits = FORBIDDEN_PUBLIC_RE.findall(article[field])
            if hits:
                raise SystemExit(f"Forbidden public wording in {article['title']}: {hits}")
        hits = FORBIDDEN_PUBLIC_RE.findall(long)
        if hits:
            raise SystemExit(f"Forbidden public wording in {article['title']}: {hits}")
        rendered.append(
            {
                "title": article["title"],
                "short": article["short"],
                "long": long,
                "date_posted": TODAY,
                "time_posted": f"{9 + index // 4:02d}:{(index % 4) * 12:02d}",
                "active": True,
                "link": slugify(article["title"]),
                "commenting": False,
            }
        )

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(rendered, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    report = preflight_links(rendered)
    OUT_PREFLIGHT.parent.mkdir(parents=True, exist_ok=True)
    OUT_PREFLIGHT.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"article_count": len(rendered), "output": str(OUT_JSON), **report}, ensure_ascii=False, indent=2))
    if report["failure_count"]:
        raise SystemExit("Link preflight failed")


if __name__ == "__main__":
    main()
