import html
import json
import re
import unicodedata
from pathlib import Path
from urllib.parse import urljoin

import requests

try:
    import xlwt
except ImportError:  # pragma: no cover
    xlwt = None


BASE = "https://www.vevo.sk"
BATCH = 33
BATCH_DATE = "2025-09-19"
OUT_JSON = Path("content/VEVO_CONTENT/imports/batch-33-2026-07-06-articles.json")
OUT_PREFLIGHT = Path("content/VEVO_CONTENT/exports/batch-33-2026-07-06-link-preflight.json")
OUT_XLS = Path.home() / "AppData/Local/Temp/vevo-batch-33-floor-kitchen-cleaning-clean-urls.xls"


FORBIDDEN_PUBLIC_RE = re.compile(
    r"longtail|long-tail|long tail|"
    r"\bkeyword(?:s)?\b|kľúčov\w*\s+slov\w*|klucov\w*\s+slov\w*|"
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


def source_box(items):
    rows = "".join(
        f'<li><a rel="noopener" href="{href}" target="_blank">{esc(label)}</a></li>'
        for label, href in items
    )
    return f"""
<div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; margin: 22px 0; background: #fbfbfb;">
<h2 style="margin-top: 0;">Zdroje a odborný kontext</h2>
<p>Pri čistení domácnosti rozhoduje povrch, vlhkosť, dávkovanie a vetranie. Nižšie uvedené zdroje používame ako širší rámec k bezpečnému čisteniu, hygiene a vnútornému prostrediu; pri konkrétnom povrchu má vždy prednosť odporúčanie výrobcu.</p>
<ul>{rows}</ul>
</div>
""".strip()


def recommendation_block(article):
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


def related_links(items):
    links = "".join(f'<li><a href="{href}">{esc(label)}</a></li>' for label, href in items)
    return f"<h2>Súvisiace návody na VEVO</h2>\n<ul>{links}</ul>"


def faq(items, title):
    parts = [f"<h2>FAQ: {esc(title)}</h2>"]
    for question, answer in items:
        parts.append(f"<h3>{esc(question)}</h3><p>{answer}</p>")
    return "\n".join(parts)


def common_diagnostic(article):
    return [
        (
            "Ako rozpoznať, či je problém v produkte alebo v postupe",
            [
                article["diagnostic_one"],
                "Ak sa problém opakuje po každom čistení, najprv znížte dávku produktu, vymeňte vodu alebo handričku a skontrolujte, či povrch pred čistením nebol zaprášený. Veľa domácich šmúh nevzniká preto, že produkt nefunguje, ale preto, že sa na povrchu mieša prach, mastnota, tvrdá voda a príliš veľká dávka čistiaceho prostriedku.",
            ],
        ),
        (
            "Ako si nastaviť jednoduchú rutinu",
            [
                article["routine_one"],
                "Dobrá rutina má byť krátka a opakovateľná: odstrániť hrubú špinu, použiť primerané množstvo produktu, nepracovať so špinavou handričkou a nechať povrch doschnúť. Ak z toho spravíte zložitý rituál, pravdepodobne ho nebudete robiť pravidelne a nečistoty sa budú vracať.",
            ],
        ),
        (
            "Kedy nepoužívať silnejší prípravok",
            [
                article["careful_one"],
                "Silnejší alebo abrazívnejší prípravok patrí až po overení, že ho povrch znesie. Pri lakovaných, lesklých, prírodných, poréznych alebo špeciálne upravených povrchoch je bezpečnejšie začať jemnejším postupom na malej skrytej časti.",
            ],
        ),
    ]


def render_article(article):
    sections = list(article["sections"]) + common_diagnostic(article)
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
    parts.append("<h2>Kontrolná tabuľka podľa povrchu a rizika</h2>")
    parts.append(table(["Povrch alebo miesto", "Najväčšie riziko", "Praktická poznámka"], article["check_rows"]))
    parts.append("<h2>Najčastejšie chyby</h2>")
    parts.append("<ul>" + "".join(f"<li>{item}</li>" for item in article["mistakes"]) + "</ul>")
    for heading, paragraphs in sections:
        parts.append(f"<h2>{esc(heading)}</h2>")
        if isinstance(paragraphs, str):
            paragraphs = [paragraphs]
        for paragraph in paragraphs:
            parts.append(f"<p>{paragraph}</p>")
    parts.append(callout("Najdôležitejšie pravidlo", article["rule"], background="#fffaf5", border="#e6ded2"))
    parts.append("<h2>Kedy byť opatrný</h2>")
    parts.extend(f"<p>{paragraph}</p>" for paragraph in article["caution"])
    parts.append("<h2>Odbornejší pohľad</h2>")
    parts.extend(f"<p>{paragraph}</p>" for paragraph in article["expert"])
    parts.append(source_box(article["sources"]))
    parts.append(recommendation_block(article))
    parts.append(related_links(article["related"]))
    parts.append(faq(article["faq"], article["faq_title"]))
    return "\n".join(parts)


ARTICLES = [
    {
        "title": "Ako umyť podlahu bez šmúh: laminát, vinyl, dlažba a mopovanie v praxi",
        "short": "Podlaha bez šmúh nie je len otázka dobrého prípravku. Rozhoduje prach pred umývaním, čistá voda, správne dávkovanie, typ povrchu, mop a doschnutie. Pri lamináte a vinyle používajte menej vody, pri dlažbe riešte škáry a pri robotickom mopovaní neprelievajte nádržku.",
        "answer": "Podlahu bez šmúh umyjete tak, že najprv odstránite prach a piesok, použijete čistý mop, menšiu dávku prípravku a povrch nenecháte premokrený. Laminát a vinyl potrebujú menej vody než dlažba. Pri robotickom mopu používajte len prípravok vhodný do nádržky, napríklad <a href=\"/p-1583/cistic-podlah-do-robotickeho-mopu\">čistič podláh do robotického mopu</a>, a po umytí sledujte, či na podlahe neostal lepkavý film.",
        "quick": [
            "<strong>Najprv povysávať:</strong> prach a piesok robia pri mokrom mopu sivý film.",
            "<strong>Menej prípravku:</strong> veľká dávka často zanechá šmuhy a lepivosť.",
            "<strong>Čistá voda:</strong> špinavá voda iba roznáša nečistoty po väčšej ploche.",
            "<strong>Podľa povrchu:</strong> laminát a vinyl neznášajú premáčanie, dlažba znesie viac.",
            "<strong>Vôňa až po čistote:</strong> <a href=\"/c/vevo-home-care/upratovanie/cistiace-prostriedky/vona-na-umyvanie-podlah\">vôňa na umývanie podláh</a> má doplniť čistý výsledok, nie prekrývať špinu.",
        ],
        "intro": [
            "Šmuhy na podlahe vznikajú aj v domácnostiach, kde sa upratuje pravidelne. Najčastejšie nejde o jednu veľkú chybu, ale o kombináciu prachu, špinavej vody, priveľa prípravku a nevhodného množstva vlhkosti pre konkrétny povrch. Podlaha po umytí potom síce vonia, ale pri svetle z okna vidno sivé ťahy alebo lepkavé mapy.",
            "Pri lamináte, vinyle, dlažbe a plávajúcej podlahe sa oplatí postupovať od suchého čistenia k mokrému. Ak na podlahe ostanú vlasy, omrvinky, piesok alebo prach, mop ich spojí s vodou a vytvorí povlak. Preto je vysávanie alebo zametanie pred mopovaním dôležitejšie, než sa zdá.",
            "Robotický vysávač s mopom môže výrazne pomôcť s pravidelnou údržbou, ale nerieši zaschnuté škvrny ani veľké nánosy. Ak používate robotický mop, sledujte dávkovanie vody, čistotu handričky a vhodnosť prípravku do nádržky. Pri ručnom mopu rozhoduje podobná logika: čistá hlavica, primeraná vlhkosť a dobrý oplach.",
            "Cieľom nie je umývať podlahu silnejšie, ale rozumnejšie. Keď odstránite prach nasucho, použijete menej prípravku a necháte povrch dobre doschnúť, výsledok býva čistejší, bez šmúh a bez pocitu lepkavej podlahy.",
        ],
        "why_heading": "Prečo ostávajú šmuhy na podlahe",
        "why": [
            "Šmuhy sú často zvyšky toho, čo malo byť z podlahy odstránené: prach, mastnota z kuchyne, minerály z vody, prebytok čistiaceho produktu alebo špinavá voda. Keď sa tieto vrstvy rozotrú tenkým filmom, podlaha vyzerá nerovnomerne a pri dopade svetla sa objavia ťahy.",
            "Druhým problémom je nesprávna vlhkosť. Laminát a niektoré plávajúce podlahy nemajú rady veľa vody, pretože vlhkosť môže preniknúť do spojov. Vinyl je odolnejší, ale aj na ňom môže priveľa produktu zanechať povlak. Dlažba znesie viac vody, no škáry často držia špinu, ktorá sa pri umývaní uvoľňuje postupne.",
            "Veľa domácností pridáva prípravok podľa vône alebo pocitu, nie podľa dávkovania. Príjemná vôňa však nie je dôkaz čistoty. Ak je prípravku priveľa, zvyšky ostanú na povrchu a pri chôdzi sa na ne lepí ďalší prach.",
        ],
        "decision_heading": "Ako postupovať podľa typu podlahy",
        "decision_rows": [
            ("Laminátová podlaha", "riziko premáčania spojov a šmúh po prebytku vody", "použite dobre vyžmýkaný mop a menšiu dávku produktu"),
            ("Vinylová podlaha", "znesie bežné mopovanie, ale môže lepiť pri veľkej dávke", "umývajte pravidelne, bez hrubého filmu a s čistou vodou"),
            ("Dlažba", "špina sa drží v škárach a pri okrajoch", "najprv povysávajte, potom riešte plochu aj škáry"),
            ("Kuchyňa", "mastnota z varenia sa mieša s prachom", "pred mopovaním odstráňte lokálne mastné miesta"),
            ("Robotický mop", "šmuhy vzniknú zo špinavej handričky alebo nevhodného roztoku", "použite čistú handričku a vhodný <a href=\"/c/vevo-home-care/upratovanie/cistiace-prostriedky/cistic-do-robotickeho-vysavaca\">čistič do robotického vysávača</a>"),
        ],
        "steps": [
            "Odstráňte z podlahy prach, piesok, vlasy a omrvinky vysávačom alebo suchým mopom.",
            "Pripravte si čistú vodu a čistý mop. Ak je hlavica špinavá, šmuhy vzniknú aj pri dobrom prípravku.",
            "Dávkujte prípravok podľa odporúčania, nie podľa toho, ako výraznú vôňu chcete cítiť.",
            "Pri lamináte a plávajúcej podlahe mop poriadne vyžmýkajte a nepracujte s mlákami.",
            "Podlahu umývajte po menších častiach, aby ste neroznášali špinavú vodu po celej miestnosti.",
            "Ak voda stmavne, vymeňte ju. Špinavá voda je častý dôvod sivých máp.",
            "Po umytí nechajte podlahu prirodzene doschnúť a nechoďte po nej, kým je mokrá.",
        ],
        "check_rows": [
            ("Laminát", "veľa vody v spojoch", "mop má byť vlhký, nie mokrý"),
            ("Vinyl", "lepkavý film po prebytku prípravku", "pri šmuhách znížte dávku"),
            ("Dlažba", "špina v škárach", "raz za čas vyčistiť škáry samostatne"),
            ("Robotický mop", "zanesená handrička", "handričku prať alebo meniť podľa intenzity používania"),
            ("Kuchynská podlaha", "mastnota pri sporáku", "najprv lokálne odmastiť, potom mopovať celú plochu"),
        ],
        "mistakes": [
            "Umývať podlahu bez predchádzajúceho vysatia prachu a piesku.",
            "Pridávať viac prípravku, keď ostávajú šmuhy.",
            "Používať rovnakú špinavú vodu na celý byt.",
            "Premáčať laminát alebo plávajúcu podlahu.",
            "Naliať do robotického mopu hustý alebo penivý produkt.",
            "Hodnotiť výsledok ešte pred úplným doschnutím.",
        ],
        "sections": [
            ("Ako umyť laminátovú podlahu bez šmúh", [
                "Pri lamináte je najdôležitejšie obmedziť vodu. Povrch zotrite vlhkým, dobre vyžmýkaným mopom a nepracujte s mlákami. Ak sa voda dostane do spojov, môže zhoršiť vzhľad hrán a časom spôsobiť problémy, ktoré už čistenie nevyrieši.",
                "Ak na lamináte ostávajú šmuhy, skúste najprv menej prípravku a čistejšiu vodu. Často pomôže aj nový alebo dôkladne vypraný mop, pretože stará hlavica môže na podlahu vracať zvyšky predchádzajúceho čistenia.",
            ]),
            ("Ako umyť vinylovú podlahu", [
                "Vinyl býva praktický na bežnú údržbu, ale aj na ňom sa môžu objaviť šmuhy. Najčastejšie vznikajú po prebytku čističa alebo po tom, čo sa mokrým mopom rozotrie mastnota z kuchyne. Preto je dobré mastné miesta riešiť lokálne ešte pred celým mopovaním.",
                "Pri vinyle funguje pravidelnosť. Radšej menšia dávka prípravku a častejšie čistenie než silný roztok raz za čas. Ak je podlaha lepkavá, prejdite ju čistou vodou a nabudúce znížte dávku.",
            ]),
            ("Ako umývať dlažbu a škáry", [
                "Dlažba znesie viac vody než laminát, ale špina sa rada drží v škárach. Bežný mop umyje plochu, no mastnota a prach sa môžu usádzať pri okrajoch a medzi dlaždicami. Pri kuchyni alebo predsieni preto občas venujte pozornosť aj škáram.",
                "Ak dlažba po umytí vyzerá matne, môže ísť o zvyšky čistiaceho prostriedku alebo tvrdú vodu. Pomôže menej produktu, čistejšia voda a dôkladnejšie zotretie pri poslednom prechode.",
            ]),
            ("Robotický vysávač s mopom a šmuhy", [
                "Pri robotickom mopu sledujte čistotu handričky, stav nádržky a roztok. Ak handrička prejde veľkú plochu bez výmeny alebo vyprania, začne špinu roznášať. Robot je výborný na pravidelnú údržbu, nie na zaschnuté veľké škvrny.",
                "Do nádržky dávajte iba prípravok vhodný na také použitie. Pri podlahách po robotickom mopovaní sa oplatí pozrieť aj návod <a href=\"/n/ako-vycistit-roboticky-vysavac\">ako vyčistiť robotický vysávač</a>, pretože šmuhy môžu súvisieť so zanesenou handričkou alebo nádržkou.",
            ]),
        ],
        "rule": [
            "Podlaha bez šmúh začína nasucho: najprv prach a piesok, až potom voda.",
            "Ak ostáva film, nepridávajte viac prípravku. Skôr znížte dávku, vymeňte vodu a skontrolujte mop.",
        ],
        "caution": [
            "Pri drevenej, olejovanej alebo špeciálne upravenej podlahe sa riaďte odporúčaním výrobcu. Nie každý čistič je vhodný na každý povrch a príliš veľa vody môže byť väčší problém než samotná špina.",
            "Ak je doma malé dieťa, alergik alebo zviera, dávkujte vôňu a čistiace produkty mierne. Čistota má byť prvá, vôňa až druhá.",
        ],
        "expert": [
            "Z odborného pohľadu je mokré čistenie účinné len vtedy, keď sa špina z povrchu skutočne odstráni. Ak sa špinavá voda alebo prebytok produktu nechá zaschnúť, výsledkom je nový film. Preto sú pri domácom mopovaní dôležité aj mechanické kroky: vysatie, čistý mop, výmena vody a sušenie.",
            "Vnútorné prostredie ovplyvňuje aj spôsob používania čistiacich produktov. Pri silne parfumovaných alebo nesprávne dávkovaných produktoch môže byť výsledok nepríjemný, najmä v malých a slabo vetraných miestnostiach. Rozumné dávkovanie a vetranie sú praktickejšie než snaha prevoňať celý byt jedným silným roztokom.",
        ],
        "diagnostic_one": "Ak podlaha ostáva lepkavá, skúste ju raz pretrieť iba čistou vodou a novým mopom. Keď sa lepkavosť zmenší, problém bol pravdepodobne v prebytku produktu alebo v špinavej hlavici, nie v samotnej podlahe.",
        "routine_one": "Pri bežnej domácnosti stačí rýchlo vysať viditeľný prach, potom mopovať frekventované zóny a väčšie umývanie nechať na deň, keď máte čas vymeniť vodu a dôkladne vysušiť povrch.",
        "careful_one": "Pri lamináte, dreve a plávajúcej podlahe nepoužívajte veľa vody ani agresívne drhnutie. Ak neviete, čo povrch znesie, otestujte prípravok na malej skrytej časti.",
        "product": {
            "heading": "Odporúčané riešenie na svieže umývanie podláh",
            "name": "Parfum na podlahy Vevo Premium No.01 Cotton Paradise",
            "href": "/p-1553/parfum-na-podlahy-vevo-no-01-cotton-paradise",
            "intro": "Keď je podlaha reálne čistá a nechcete len prekrývať špinu, vôňa na umývanie podláh môže doplniť príjemný pocit z upratanej domácnosti.",
            "fit": "pri pravidelnom mopovaní tvrdých podláh, keď už je odstránený prach, piesok a mastnota a chcete jemný svieži dojem.",
            "boundary": "ak podlaha lepí, je mastná alebo má sivý film, najprv znížte dávku produktu, vymeňte vodu a vyčistite mop.",
            "button": "Pozrieť parfum na podlahy",
        },
        "category": {
            "heading": "Vyberte riešenie podľa spôsobu mopovania",
            "href": "/c/vevo-home-care/upratovanie/cistiace-prostriedky/vona-na-umyvanie-podlah",
            "button": "Pozrieť vône na umývanie podláh",
            "intro": "Pri podlahách vyberajte produkt podľa povrchu, dávkovania a toho, či mopujete ručne alebo robotom.",
            "bullets": [
                ("Ručný mop", "najprv čistá voda a primeraná dávka, až potom vôňa."),
                ("Robotický mop", "použiť iba prípravok vhodný do nádržky a sledovať pokyny výrobcu."),
                ("Mastná kuchyňa", "lokálne odmastiť problémové miesto pred celým mopovaním."),
            ],
        },
        "related": [
            ("Ako vyčistiť robotický vysávač", "/n/ako-vycistit-roboticky-vysavac"),
            ("Ako vybrať robotický vysávač s mopom", "/n/ako-vybrat-roboticky-vysavac-s-mopom"),
            ("Ako vyčistiť rohožku a textílie v predsieni od posypovej soli", "/n/ako-vycistit-rohozku-a-textilie-v-predsieni-od-posypovej-soli"),
            ("Ako vyčistiť sušiak na bielizeň, aby neprenášal špinu na prádlo", "/n/ako-vycistit-susiak-na-bielizen-aby-neprenasal-spinu-na-pradlo"),
        ],
        "sources": [
            ("CDC: Cleaning and disinfecting", "https://www.cdc.gov/hygiene/cleaning-disinfecting/index.html"),
            ("EPA: Introduction to indoor air quality", "https://www.epa.gov/indoor-air-quality-iaq/introduction-indoor-air-quality"),
            ("EPA Safer Choice", "https://www.epa.gov/saferchoice"),
        ],
        "faq_title": "umývanie podlahy bez šmúh",
        "faq": [
            ("Prečo je podlaha po umytí lepkavá?", "Najčastejšie pre priveľa čistiaceho produktu, špinavý mop alebo vodu, ktorá sa nevymenila včas."),
            ("Môžem dať vôňu na podlahy do robotického mopu?", "Iba ak je produkt aj zariadenie na také použitie vhodné. Pri pochybnosti použite radšej produkt určený pre robotické mopovanie."),
            ("Ako umyť laminát bez poškodenia?", "Použite dobre vyžmýkaný mop, menej vody, primeranú dávku produktu a nenechávajte stáť vodu v spojoch."),
        ],
    },
    {
        "title": "Ako vyčistiť kuchynskú linku od mastnoty, prachu a šmúh bez poškodenia povrchu",
        "short": "Kuchynskú linku čistite podľa povrchu a podľa typu špiny. Mastnota pri sporáku potrebuje iný postup než prach na horných skrinkách alebo šmuhy na lesklých dvierkach. Najprv odstráňte omrvinky a prach, potom riešte mastnotu a až nakoniec leštenie.",
        "answer": "Kuchynskú linku najprv zbavte omrviniek a prachu, až potom používajte čistiaci prípravok. Mastné miesta pri sporáku nechajte krátko pôsobiť, nešúchajte ich nasucho a pri lesklých, lakovaných alebo drevených povrchoch najprv skúšajte jemný postup. Čistiaca pasta dáva zmysel na odolnejšiu špinu, ale nie na každý citlivý povrch.",
        "quick": [
            "<strong>Začať nasucho:</strong> omrvinky a prach najprv odstráňte, aby ste ich nerozotreli.",
            "<strong>Mastnota potrebuje čas:</strong> nechajte prípravok krátko pôsobiť, nešúchajte hneď silou.",
            "<strong>Lesklé dvierka sú citlivé na šmuhy:</strong> používajte jemnú handričku a menej produktu.",
            "<strong>Drevo a lak opatrne:</strong> vyhnite sa premočeniu a agresívnemu drhnutiu.",
            "<strong>Úchytky čistite pravidelne:</strong> dotýkame sa ich mastnými rukami najčastejšie.",
        ],
        "intro": [
            "Kuchynská linka sa špiní inak než podlaha alebo kúpeľňa. Na jednom mieste sa stretáva mastnota z varenia, para, prach, omrvinky, kvapky vody, odtlačky rúk a zvyšky jedla. Preto jeden univerzálny pohyb handričkou často nestačí a môže vytvoriť len väčšiu mapu.",
            "Najväčšia chyba je začať mokrým čistením bez odstránenia suchých nečistôt. Omrvinky, múka, káva alebo prach sa potom rozotrú po pracovnej doske a lesklé dvierka ukážu šmuhy ešte viac. Rýchle suché zotretie pred čistením ušetrí veľa práce.",
            "Druhým rozdielom je povrch. Laminátová pracovná doska, lesklé dvierka, matná fólia, nerez pri dreze, drevo a kamenný alebo kompozitný povrch nemusia zniesť rovnaký produkt ani rovnaké drhnutie. Pri nejasnom materiáli je bezpečnejšie začať jemne a testovať na malej skrytej časti.",
            "Dobrý postup má tri fázy: najprv odstrániť voľnú špinu, potom rozpustiť alebo uvoľniť mastnotu a nakoniec povrch zotrieť tak, aby nezostal film. Tak linka pôsobí čisto aj pri dennom svetle, nielen tesne po prevoňaní kuchyne.",
        ],
        "why_heading": "Prečo sa kuchynská linka čistí ťažšie než vyzerá",
        "why": [
            "Mastnota z varenia je lepkavá a zachytáva prach. Na horných skrinkách alebo pri digestore sa z nej postupne stáva tenký film, ktorý sa obyčajnou vodou iba rozmazáva. Preto je pri kuchyni dôležité rozdeliť prach a mastnotu na dva kroky.",
            "Lesklé dvierka a tmavé povrchy ukážu aj malé zvyšky produktu. Ak handrička nie je čistá alebo je prípravku priveľa, na povrchu ostane šmuha. Matné povrchy zas môžu zle znášať silné drhnutie, ktoré zmení ich vzhľad.",
            "Pri pracovnej doske je navyše dôležitý kontakt s potravinami. Povrch musí byť čistý, ale po čistení nemá zostať zbytočný film. Preto je vhodné finálne zotretie čistou vlhkou handričkou a vysušenie miest, kde sa drží voda.",
        ],
        "decision_heading": "Ako zvoliť postup podľa miesta v kuchyni",
        "decision_rows": [
            ("Pracovná doska", "omrvinky, voda, káva, mastné kvapky", "najprv nasucho, potom jemné čistenie podľa materiálu"),
            ("Okolie sporáka", "mastnota a zaschnuté kvapky z varenia", "nechať krátko pôsobiť, potom zotrieť bez hrubého drhnutia"),
            ("Lesklé dvierka", "odtlačky a šmuhy viditeľné pri svetle", "menej produktu, čistá mikrovláknová handrička"),
            ("Horné skrinky", "prach nalepený na mastnom filme", "najprv uvoľniť mastnotu, potom zotrieť čistou handričkou"),
            ("Úchytky", "dotyk rukami, tuk, zvyšky jedla", "čistiť častejšie než zvyšok skriniek"),
        ],
        "steps": [
            "Odložte potraviny a malé spotrebiče, aby ste nečistili len okolo nich.",
            "Suchou handričkou alebo papierovou utierkou odstráňte omrvinky, múku, prach a voľnú špinu.",
            "Mastné miesta pri sporáku ošetrite lokálne a nechajte prípravok krátko pôsobiť.",
            "Povrch zotierajte jemnou handričkou, nie drsnou hubkou, kým neviete, že povrch drhnutie znesie.",
            "Úchytky a hrany čistite osobitne, pretože sa ich dotýkame najviac.",
            "Pracovnú dosku nakoniec pretrite čistou vlhkou handričkou a nechajte doschnúť.",
            "Pri novom produkte alebo citlivom povrchu najprv testujte na nenápadnom mieste.",
        ],
        "check_rows": [
            ("Laminátová doska", "voda v spojoch a pri hranách", "nepremáčať a dobre vysušiť"),
            ("Lesklé dvierka", "šmuhy po prebytku produktu", "menej produktu a čistá handrička"),
            ("Matný povrch", "zmena vzhľadu po drhnutí", "bez abrazívnej hubky"),
            ("Drevo", "vlhkosť, fľaky, poškodenie úpravy", "čistiť podľa odporúčania výrobcu"),
            ("Nerez pri dreze", "vodné mapy a odtlačky", "zotrieť a dosušiť v smere povrchu"),
        ],
        "mistakes": [
            "Začať mokrou handričkou na omrvinkách a prachu.",
            "Drhnúť lesklé dvierka drsnou hubkou.",
            "Použiť čistiacu pastu na povrch, ktorý ju nemusí zniesť.",
            "Nechať vodu stáť pri spojoch pracovnej dosky.",
            "Čistiť celú linku jednou špinavou handričkou.",
            "Prevoňať kuchyňu bez odstránenia mastného filmu pri sporáku.",
        ],
        "sections": [
            ("Ako odstrániť mastnotu z kuchynskej linky", [
                "Mastnotu najprv nešúchajte nasucho. Ak je na povrchu prach, odstráňte ho jemne a potom mastné miesto navlhčite vhodným čistiacim prípravkom. Krátke pôsobenie často pomôže viac než tlak rukou.",
                "Pri sporáku a digestore sa mastnota vrství postupne. Ak ste ju dlhšie neriešili, postup opakujte mierne namiesto jedného agresívneho drhnutia. Cieľom je odstrániť film bez poškodenia povrchu.",
            ]),
            ("Ako čistiť lesklú kuchynskú linku bez šmúh", [
                "Lesklé povrchy ukazujú každú šmuhu. Používajte čistú mäkkú handričku, malé množstvo produktu a po čistení povrch jemne dosušte. Ak sa šmuhy vracajú, problém môže byť v handričke alebo prebytku prípravku.",
                "Pri tmavých lesklých dvierkach sa oplatí pracovať po menších plochách. Veľká plocha zotretá naraz často zaschne skôr, než ju stihnete rovnomerne prejsť.",
            ]),
            ("Ako čistiť drevenú alebo dyhovanú pracovnú dosku", [
                "Drevo a dyha potrebujú opatrnosť s vodou. Nepremáčajte hrany, spoje ani miesta pri dreze. Ak je povrch olejovaný alebo špeciálne upravený, bežné univerzálne rady nemusia stačiť a treba rešpektovať odporúčanie výrobcu.",
                "Pri dreve je prevencia často dôležitejšia než silné čistenie. Rozliatu vodu utrite hneď, mastnotu nenechávajte zasychať a nepoužívajte drsné pasty bez overenia.",
            ]),
            ("Kedy použiť čistiacu pastu", [
                "Čistiaca pasta sa hodí na odolnejšiu špinu na povrchoch, ktoré znesú jemné mechanické čistenie. Nedáva však zmysel na každý lakovaný, lesklý alebo citlivý materiál. Pred použitím si vždy overte, či povrch pastu znesie.",
                "Ak použijete <a href=\"/p-1562/strong-pink-cistiaca-pasta-500g\">Strong PINK čistiacu pastu</a>, pracujte skôr jemne a lokálne. Potom povrch zotrite čistou vlhkou handričkou, aby nezostal zvyšok produktu.",
            ]),
        ],
        "rule": [
            "Kuchynská linka sa čistí od najľahšej špiny k najťažšej: prach, omrvinky, mastnota, finálne zotretie.",
            "Silné drhnutie patrí až po tom, čo viete, že povrch ho bezpečne znesie.",
        ],
        "caution": [
            "Pri povrchoch v kontakte s potravinami nenechávajte zvyšky čistiaceho produktu. Po čistení ich podľa potreby zotrite čistou vlhkou handričkou a nechajte doschnúť.",
            "Pri lesklých, lakovaných, drevených, kamenných alebo špeciálne upravených povrchoch najprv testujte na malej skrytej časti. Viditeľné poškodenie povrchu sa opravuje ťažšie než mastný fľak.",
        ],
        "expert": [
            "Základná hygiena kuchyne stojí na oddelení čistenia a dezinfekcie. Čistenie odstraňuje špinu a mastnotu, dezinfekcia má zmysel až na povrchu, ktorý je najprv čistý. V bežnej domácnosti preto netreba preskočiť mechanické odstránenie nečistôt.",
            "Pri kuchynskej linke je dôležité aj vnútorné prostredie. Silné vône a nadmerné dávkovanie čistiacich produktov v malej kuchyni nemusia byť príjemné. Rozumné vetranie, primerané množstvo a finálne zotretie sú praktickejšie než vrstvenie produktov.",
        ],
        "diagnostic_one": "Ak po vyčistení ostáva lesklá mapa, prejdite povrch čistou vlhkou handričkou bez ďalšieho produktu. Ak mapa mizne, pravdepodobne išlo o zvyšky čističa alebo mastnoty, nie o trvalé poškodenie.",
        "routine_one": "Po varení utrite okolie sporáka a úchytky, raz za pár dní prejdite pracovnú dosku dôkladnejšie a horné skrinky čistite podľa toho, ako často varíte mastné jedlá.",
        "careful_one": "Na lesklé dvierka, lak a drevo nepoužívajte automaticky pastu ani drsnú hubku. Začnite jemnou handričkou a až potom riešte silnejší postup.",
        "product": {
            "heading": "Odporúčané riešenie na odolnejšiu špinu v kuchyni",
            "name": "Strong PINK čistiaca pasta 500g",
            "href": "/p-1562/strong-pink-cistiaca-pasta-500g",
            "intro": "Pri odolnejšej mastnote alebo zaschnutej špine môže pomôcť čistiaca pasta, ale iba na povrchoch, ktoré ju znesú. Nepoužívajte ju ako univerzálne riešenie na každý citlivý povrch.",
            "fit": "na lokálne čistenie odolnejšej špiny tam, kde výrobca povrchu umožňuje jemné mechanické čistenie.",
            "boundary": "ak ide o lesklý lak, dyhu, drevo, prírodný kameň alebo neznámy povrch, najprv testujte a začnite jemnejším čistením.",
            "button": "Pozrieť čistiacu pastu",
        },
        "category": {
            "heading": "Vyberte čistenie podľa povrchu kuchyne",
            "href": "/c/vevo-home-care/upratovanie/cistiace-prostriedky/cistiaca-pasta",
            "button": "Pozrieť čistiace pasty",
            "intro": "Kuchynská linka má viac typov povrchov, preto je lepšie vyberať produkt podľa miesta a rizika poškodenia.",
            "bullets": [
                ("Pracovná doska", "najprv omrvinky, potom jemné čistenie a finálne zotretie."),
                ("Sporákové okolie", "mastnotu nechajte krátko pôsobiť, netlačte hneď silou."),
                ("Lesklé dvierka", "menej produktu a mäkká čistá handrička."),
            ],
        },
        "related": [
            ("Ako odstrániť vajíčko z oblečenia, obrusu a kuchynskej utierky", "/n/ako-odstranit-vajicko-z-oblecenia-obrusu-a-kuchynskej-utierky"),
            ("Ako odstrániť červenú papriku z trička a kuchynskej utierky", "/n/ako-odstranit-cervenu-papriku-z-tricka-a-kuchynskej-utierky"),
            ("Ako odstrániť majonézu a dressing z obrusu bez mastného fľaku", "/n/ako-odstranit-majonezu-a-dressing-z-obrusu-bez-mastneho-flaku"),
            ("Ako vyčistiť sušiak na bielizeň, aby neprenášal špinu na prádlo", "/n/ako-vycistit-susiak-na-bielizen-aby-neprenasal-spinu-na-pradlo"),
        ],
        "sources": [
            ("CDC: Food safety", "https://www.cdc.gov/food-safety/index.html"),
            ("CDC: Cleaning and disinfecting", "https://www.cdc.gov/hygiene/cleaning-disinfecting/index.html"),
            ("EPA Safer Choice", "https://www.epa.gov/saferchoice"),
        ],
        "faq_title": "čistenie kuchynskej linky",
        "faq": [
            ("Ako často čistiť kuchynskú linku?", "Pracovnú dosku po varení alebo príprave jedla, úchytky a okolie sporáka podľa používania. Mastnotu je lepšie riešiť skôr, než sa navrství."),
            ("Môžem použiť čistiacu pastu na lesklé dvierka?", "Nie automaticky. Najprv overte povrch a testujte na malej skrytej časti, pretože niektoré lesklé povrchy sa môžu poškriabať alebo zmatnieť."),
            ("Prečo linka po čistení stále robí šmuhy?", "Často ide o prebytok produktu, špinavú handričku alebo mastný film, ktorý sa nerozpustil úplne."),
        ],
    },
    {
        "title": "Ako vyčistiť drez a batériu: vodný kameň, mastnota, zápach a bezpečné čistenie",
        "short": "Drez a batériu čistite podľa materiálu. Nerez, granitový drez, keramika, chróm a silikón okolo drezu nemusia zniesť rovnaký postup. Vodný kameň riešte opatrne, mastnotu nenechávajte v odpade a zápach z drezu nezačínajte prekrývať vôňou.",
        "answer": "Drez najprv zbavte zvyškov jedla a mastnoty, potom čistite podľa materiálu. Na vodný kameň okolo batérie môže pomôcť biely ocot, ale nie je vhodný na každý prírodný alebo citlivý povrch. Zápach z drezu riešte cez sitko, odpad, mastnotu a pravidelné preplachovanie, nie iba cez vôňu v kuchyni.",
        "quick": [
            "<strong>Najprv zvyšky jedla:</strong> sitko a odpad čistite skôr, než začnete leštiť batériu.",
            "<strong>Vodný kameň opatrne:</strong> kyslé produkty nepatria na každý kameň alebo citlivý povrch.",
            "<strong>Mastnotu nelejte do odpadu:</strong> časom vytvára nános a zápach.",
            "<strong>Nerez sušte:</strong> vodné kvapky zanechávajú mapy a fľaky.",
            "<strong>Zápach má zdroj:</strong> nestačí prevoňať kuchyňu, treba vyčistiť miesto, kde sa drží špina.",
        ],
        "intro": [
            "Drez je v kuchyni miesto, kde sa stretáva voda, zvyšky jedla, mastnota, vodný kameň, čistiace produkty a vlhkosť. Preto môže byť na prvý pohľad umytý, ale pri batérii ostanú biele mapy alebo z odpadu cítiť zápach. Riešenie závisí od toho, či riešite povrch drezu, batériu, sitko alebo odpad.",
            "Najčastejšie otázky sú jednoduché: ako vyčistiť nerezový drez, ako odstrániť vodný kameň z batérie, ako vyčistiť granitový drez, prečo drez zapácha a či pomôže biely ocot. Odpoveď nie je rovnaká pre každý materiál. Kyslé produkty môžu byť užitočné na vodný kameň, ale nie na povrchy, ktoré kyseliny neznášajú.",
            "Pri dreze sa neoplatí riešiť iba lesk. Ak v sitku ostávajú zvyšky jedla alebo sa mastnota pravidelne leje do odpadu, zápach sa vráti aj po vyleštení batérie. Dobré čistenie preto začína mechanicky: odstrániť zvyšky, vyčistiť sitko, prejsť okolie batérie a až potom povrch.",
            "Rovnako ako pri podlahe a kuchynskej linke platí, že príliš veľa produktu nie je lepšie. Zvyšky čističa môžu vytvoriť film a pri batérii stačí aj tvrdá voda, aby sa za deň objavila nová mapa. Preto pomáha pravidelná krátka údržba.",
        ],
        "why_heading": "Prečo vzniká vodný kameň, mastný film a zápach",
        "why": [
            "Vodný kameň vzniká najmä tam, kde kvapky vody pravidelne zasychajú. Okolie batérie, spoj pri dreze, odkvapkávač a hrany sú preto typické miesta bielych máp. Ak ich iba pretriete mokrou handričkou, minerály sa môžu presunúť, ale nezmiznú.",
            "Mastnota je problém hlavne preto, že sa viaže na zvyšky jedla a prach. V odpade môže vytvoriť vrstvu, na ktorej sa drží ďalšia špina. Preto je lepšie mastnotu neutierať do drezu vo veľkom množstve a sitko čistiť pravidelne.",
            "Zápach z drezu nie je samostatná vôňa, ale signál, že sa niekde drží organická špina, mastnota alebo vlhkosť. Prekrytie interiérovou vôňou nepomôže, ak zostane znečistené sitko, odpad alebo okolie prepadu.",
        ],
        "decision_heading": "Ako čistiť drez podľa materiálu a problému",
        "decision_rows": [
            ("Nerezový drez", "vodné mapy a odtlačky po kvapkách", "čistiť jemne a po oplachu dosušiť"),
            ("Granitový alebo kompozitný drez", "riziko nevhodného kyslého alebo abrazívneho postupu", "riadiť sa výrobcom a testovať opatrne"),
            ("Batéria", "vodný kameň pri perlátore a spodku", "použiť mierny postup a nenechať kyslý roztok pôsobiť zbytočne dlho"),
            ("Sitko", "zvyšky jedla a mastnota", "vybrať, umyť a nenechávať zvyšky zasychať"),
            ("Odpad", "zápach po mastnote a organických zvyškoch", "neprekrývať, ale odstrániť zdroj zápachu"),
        ],
        "steps": [
            "Vyberte z drezu riad, špongie a zvyšky jedla, aby ste čistili samotný povrch.",
            "Vyberte sitko a odstráňte zachytené zvyšky. Tie sú častým zdrojom zápachu.",
            "Povrch drezu opláchnite a zistite, či riešite mastnotu, vodný kameň alebo farebnú mapu.",
            "Na vodný kameň pri batérii použite mierny kyslý postup len tam, kde je povrch vhodný.",
            "Mastné miesta najprv uvoľnite, potom zotrite a opláchnite.",
            "Batériu a okolie drezu po čistení dosušte, aby kvapky nevytvorili nové mapy.",
            "Ak zápach pretrváva, skontrolujte odpad, prepad a to, či do drezu nelejete mastnotu.",
        ],
        "check_rows": [
            ("Nerez", "škrabance a vodné mapy", "jemná handrička a dosušenie"),
            ("Chrómová batéria", "vodný kameň a poškodenie povrchu", "krátke pôsobenie, oplach, dosušenie"),
            ("Granitový drez", "citlivosť na kyseliny alebo agresívne produkty", "najprv návod výrobcu"),
            ("Silikón pri dreze", "vlhkosť a tmavnutie", "udržiavať suchší, nepoškodzovať drhnutím"),
            ("Odpad", "mastnota a zvyšky jedla", "pravidelne čistiť sitko a neprelievať tuk"),
        ],
        "mistakes": [
            "Použiť ocot na každý povrch bez overenia materiálu.",
            "Drhnúť nerez tvrdou hubkou a vytvoriť škrabance.",
            "Leštiť batériu, ale nevyčistiť sitko a odpad.",
            "Liať mastnotu z panvice do drezu.",
            "Nechať kyslý prípravok pôsobiť príliš dlho pri citlivom povrchu.",
            "Prekrývať zápach kuchyne vôňou bez odstránenia zdroja.",
        ],
        "sections": [
            ("Ako vyčistiť nerezový drez", [
                "Nerezový drez čistite jemnou handričkou a po oplachu ho dosušte. Práve dosušenie robí veľký rozdiel, pretože kvapky tvrdej vody po zaschnutí vytvoria biele mapy. Nepoužívajte tvrdé drôtenky, ak nechcete poškriabať povrch.",
                "Ak je nerez mastný, najprv uvoľnite mastnotu vhodným čističom a až potom riešte lesk. Lesk bez odstránenia mastnoty vytvorí len nerovnomerný povlak.",
            ]),
            ("Ako vyčistiť batériu od vodného kameňa", [
                "Vodný kameň sa najčastejšie drží pri spodku batérie, perlátore a miestach, kde voda zasychá. <a href=\"/p-1561/biely-ocot-v-spreji-500-ml\">Biely ocot v spreji</a> môže byť praktický pomocník, ale len na povrchoch, ktoré kyslý postup znesú.",
                "Pri chróme a lesklých povrchoch nenechávajte kyslý roztok pôsobiť zbytočne dlho. Po čistení opláchnite a dosušte. Pri prírodnom kameni alebo citlivých materiáloch si najprv overte odporúčanie výrobcu.",
            ]),
            ("Ako riešiť zápach z drezu", [
                "Zápach často nevychádza z viditeľnej plochy drezu, ale zo sitka, prepadu alebo odpadu. Vyberte sitko, odstráňte zvyšky jedla a skontrolujte, či sa v okolí nedrží mastný film. Až potom má zmysel riešiť zvyšok drezu.",
                "Ak sa zápach vracia, sledujte zvyky v domácnosti: zvyšky jedla v sitku, tuk z panvice, pomalé odtekanie a vlhkú špongiu vedľa drezu. Čistenie povrchu bez zmeny týchto zvykov prinesie iba krátky efekt.",
            ]),
            ("Ako čistiť granitový alebo kompozitný drez", [
                "Granitový a kompozitný drez môže vyzerať odolne, ale neznamená to, že znesie každý kyslý alebo abrazívny produkt. Pri takýchto povrchoch sa riaďte odporúčaním výrobcu a pri novom produkte začnite na malej nenápadnej časti.",
                "Ak sa na tmavom dreze objavujú biele mapy, nemusí ísť o trvalú škvrnu. Často ide o vodný kameň alebo zvyšky produktu. Postupujte mierne, oplachujte a dosušujte, aby sa film nevracal.",
            ]),
        ],
        "rule": [
            "Drez čistite od zdroja problému: zvyšky jedla, sitko, mastnota, vodný kameň, až potom lesk.",
            "Ocot a kyslé produkty používajte cielene, nie automaticky na každý povrch.",
        ],
        "caution": [
            "Biely ocot a kyslé čističe nemusia byť vhodné na prírodný kameň, mramor, niektoré kompozity a citlivé povrchové úpravy. Pri nejasnom materiáli najprv testujte.",
            "Ak drez zle odteká, zápach je výrazný alebo sa problém opakovane vracia, samotné povrchové čistenie nemusí stačiť. Vtedy riešte odpad, sifón alebo odborný servis.",
        ],
        "expert": [
            "Pri kuchynskom dreze sa spája čistenie povrchu s hygienou miesta, kde sa spracúvajú potraviny. Mechanické odstránenie zvyškov jedla a mastnoty je základ, pretože bez neho sa zápach a film vracajú aj po použití voňavého produktu.",
            "Vlhkosť a zasychajúca voda podporujú tvorbu máp. Preto je pri batérii a nereze praktické dosušovanie. Nejde o estetický detail, ale o prevenciu opakovaného vodného kameňa na miestach, kde voda pravidelne zostáva.",
        ],
        "diagnostic_one": "Ak batéria po čistení znovu zbelie do jedného dňa, pozrite sa na tvrdosť vody a dosušenie po použití. Ak drez zapácha, začnite sitkom a odpadom, nie leskom povrchu.",
        "routine_one": "Po varení odstráňte zvyšky jedla zo sitka, po umývaní riadu opláchnite mastné miesta a večer utrite okolie batérie do sucha. Raz za čas skontrolujte prepad a miesta, kde sa drží vlhkosť.",
        "careful_one": "Nepoužívajte ocot alebo čistiacu pastu bez overenia na prírodný kameň, citlivý kompozit, farebný povrch alebo lakované okolie drezu.",
        "product": {
            "heading": "Odporúčané riešenie na vodný kameň v kuchyni",
            "name": "Biely ocot v spreji 500 ml",
            "href": "/p-1561/biely-ocot-v-spreji-500-ml",
            "intro": "Biely ocot v spreji môže pomôcť pri bežných mapách od vody a vodnom kameni, keď ho použijete na vhodnom povrchu a nenecháte ho pôsobiť zbytočne dlho.",
            "fit": "pri bežnom vodnom kameni okolo batérie, na miestach, kde povrch kyslý postup znesie.",
            "boundary": "ak ide o prírodný kameň, neznámy kompozit alebo citlivú povrchovú úpravu, najprv overte odporúčanie výrobcu.",
            "button": "Pozrieť biely ocot v spreji",
        },
        "category": {
            "heading": "Vyberte čistenie podľa problému pri dreze",
            "href": "/c/vevo-home-care/upratovanie/cistiace-prostriedky/biely-ocot",
            "button": "Pozrieť biely ocot",
            "intro": "Vodný kameň, mastnota a zápach z drezu majú odlišné príčiny, preto ich neriešte jedným náhodným postupom.",
            "bullets": [
                ("Vodný kameň", "mierny kyslý postup iba na vhodnom povrchu."),
                ("Mastnota", "nechať uvoľniť a nelepiť ju do odpadu vo veľkom množstve."),
                ("Zápach", "začať sitkom, prepadom a zdrojom nečistôt."),
            ],
        },
        "related": [
            ("Ako vyčistiť zásobník práčky od usadenín pracieho gélu a aviváže", "/n/ako-vycistit-zasobnik-pracky-od-usadenin-pracieho-gelu-a-avivaze"),
            ("Ako vyčistiť filter práčky, keď bielizeň zapácha alebo voda odteká pomaly", "/n/ako-vycistit-filter-pracky-ked-bielizen-zapacha-alebo-voda-odteka-pomaly"),
            ("Ako vyčistiť sušiak na bielizeň, aby neprenášal špinu na prádlo", "/n/ako-vycistit-susiak-na-bielizen-aby-neprenasal-spinu-na-pradlo"),
            ("Prečo oblečenie zapácha po praní", "/n/preco-moje-oblecenie-zapacha-po-prani-priciny-a-riesenia"),
        ],
        "sources": [
            ("CDC: Food safety", "https://www.cdc.gov/food-safety/index.html"),
            ("CDC: Cleaning and disinfecting", "https://www.cdc.gov/hygiene/cleaning-disinfecting/index.html"),
            ("EPA: A brief guide to mold, moisture and your home", "https://www.epa.gov/mold/brief-guide-mold-moisture-and-your-home"),
        ],
        "faq_title": "čistenie drezu a batérie",
        "faq": [
            ("Môžem čistiť batériu octom?", "Áno, ak povrch kyslý postup znesie. Nepoužívajte ho automaticky na prírodný kameň alebo citlivé povrchové úpravy a po čistení opláchnite."),
            ("Prečo drez zapácha aj po umytí?", "Zdroj môže byť v sitku, prepade, odpade alebo mastnote. Povrch drezu môže byť čistý, ale zápach sa bude vracať, ak zostane špina v odpade."),
            ("Ako vyčistiť nerezový drez bez máp?", "Po čistení ho opláchnite a dosušte mäkkou handričkou. Mapy často vznikajú zo zaschnutých kvapiek tvrdej vody."),
        ],
    },
]


def build_articles():
    out = []
    for index, article in enumerate(ARTICLES):
        long = render_article(article)
        forbidden = FORBIDDEN_PUBLIC_RE.findall(" ".join([article["title"], article["short"], long]))
        if forbidden:
            raise RuntimeError(f"Forbidden public wording in {article['title']}: {forbidden}")
        out.append(
            {
                "title": article["title"],
                "short": article["short"],
                "long": long,
                "date_posted": BATCH_DATE,
                "time_posted": f"08:{index * 12:02d}:00",
                "active": 1,
                "link": slugify(article["title"]),
                "commenting": 0,
            }
        )
    return out


def write_xls(articles):
    if xlwt is None:
        return None
    book = xlwt.Workbook()
    sheet = book.add_sheet("news")
    columns = ["title", "short", "long", "date_posted", "time_posted", "active", "link", "commenting"]
    for col, name in enumerate(columns):
        sheet.write(0, col, name)
    for row, article in enumerate(articles, start=1):
        for col, name in enumerate(columns):
            sheet.write(row, col, article[name])
    OUT_XLS.parent.mkdir(parents=True, exist_ok=True)
    book.save(str(OUT_XLS))
    return str(OUT_XLS)


def preflight_links(articles):
    href_re = re.compile(r'href="([^"]+)"')
    records = []
    session = requests.Session()
    for article in articles:
        seen = []
        for href in href_re.findall(article["long"]):
            if href.startswith("#") or href.startswith("mailto:") or href.startswith("tel:"):
                continue
            url = urljoin(BASE, href)
            if url in seen:
                continue
            seen.append(url)
        for url in seen:
            try:
                response = session.get(url, timeout=30, allow_redirects=True, headers={"User-Agent": "Mozilla/5.0"})
                status = response.status_code
                ok = status < 400
                final_url = response.url
            except Exception as exc:  # pragma: no cover
                status = None
                ok = False
                final_url = None
                records.append({"title": article["title"], "url": url, "ok": ok, "status": status, "error": str(exc)})
                continue
            records.append(
                {
                    "title": article["title"],
                    "url": url,
                    "status": status,
                    "ok": ok,
                    "final_url": final_url,
                }
            )
    report = {
        "batch": BATCH,
        "article_count": len(articles),
        "checked_links": len(records),
        "failed_links": [record for record in records if not record["ok"]],
        "records": records,
    }
    OUT_PREFLIGHT.parent.mkdir(parents=True, exist_ok=True)
    OUT_PREFLIGHT.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    if report["failed_links"]:
        raise RuntimeError(f"Link preflight failed: {report['failed_links']}")
    return report


def main():
    articles = build_articles()
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(articles, ensure_ascii=False, indent=2), encoding="utf-8")
    xls = write_xls(articles)
    preflight = preflight_links(articles)
    print(
        json.dumps(
            {
                "batch": BATCH,
                "article_count": len(articles),
                "json": str(OUT_JSON),
                "xls": xls,
                "preflight": str(OUT_PREFLIGHT),
                "checked_links": preflight["checked_links"],
                "failed_links": len(preflight["failed_links"]),
                "slugs": [article["link"] for article in articles],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
