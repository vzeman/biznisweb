import json
import re
import unicodedata
from datetime import datetime
from html import escape
from pathlib import Path
from urllib.parse import urljoin

import requests

try:
    import xlwt
except ImportError:  # pragma: no cover
    xlwt = None


BASE = "https://www.vevo.sk"
BATCH_DATE = "2025-09-22"
OUT_JSON = Path("content/VEVO_CONTENT/imports/batch-30-2026-06-29-articles.json")
OUT_PREFLIGHT = Path("content/VEVO_CONTENT/exports/batch-30-2026-06-29-preflight.json")
OUT_XLS = Path.home() / "AppData/Local/Temp/vevo-batch-30-bathroom-bedding-clean-urls.xls"

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


def table(headers, rows):
    head = "".join(
        f'<th style="border: 1px solid #e5e5e5; padding: 10px; text-align: left;">{escape(str(header))}</th>'
        for header in headers
    )
    body = "\n".join(
        "<tr>"
        + "".join(f'<td style="border: 1px solid #e5e5e5; padding: 10px;">{escape(str(cell))}</td>' for cell in row)
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
<h2 style="margin-top: 0;">{title}</h2>
<ul>{items}</ul>
</div>
""".strip()


def situation_box(items):
    rows = "".join(f"<li>{escape(item)}</li>" for item in items)
    return f"""
<div style="border: 1px solid #d7e2ec; border-radius: 8px; padding: 18px; margin: 22px 0; background: #f7fbff;">
<h2 style="margin-top: 0;">Riešite jednu z týchto situácií?</h2>
<p>Nižšie nájdete praktické odpovede pre bežné domáce prípady, kde čistota, vlhkosť, pranie a vôňa spolu úzko súvisia.</p>
<ul>{rows}</ul>
</div>
""".strip()


def source_box(items):
    rows = "".join(f'<li><a rel="noopener" href="{href}" target="_blank">{escape(label)}</a></li>' for label, href in items)
    return f"""
<div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; margin: 22px 0; background: #fbfbfb;">
<h2 style="margin-top: 0;">Zdroje a odborný kontext</h2>
<p>Odkazy nižšie používame ako širší odborný rámec k praniu, vlhkosti, textíliám a vnútornému prostrediu. Nenahrádzajú pokyny výrobcu textilu ani individuálne odporúčanie odborníka.</p>
<ul>{rows}</ul>
</div>
""".strip()


def sales_block(sales):
    bullets = "".join(
        f"<li><strong>{escape(label)}:</strong> {escape(text)}</li>"
        for label, text in sales["category_bullets"]
    )
    return f"""
<div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 24px 0; background: #f7fbf8;">
<h2 style="margin-top: 0;">{escape(sales["heading"])}</h2>
<p>{escape(sales["intro"])}</p>
<div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff; margin: 14px 0;">
<h3 style="margin-top: 0;">{escape(sales["product_name"])}</h3>
<p><strong>Kedy dáva zmysel:</strong> {escape(sales["fit"])}</p>
<p><strong>Kedy najprv riešiť príčinu:</strong> {escape(sales["boundary"])}</p>
<p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="{sales["product_href"]}">{escape(sales["product_button"])}</a></p>
</div>
</div>
<div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 24px 0; background: #fffaf5;">
<h2 style="margin-top: 0;">{escape(sales["category_title"])}</h2>
<p>{escape(sales["category_intro"])}</p>
<ul>{bullets}</ul>
<p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="{sales["category_href"]}">{escape(sales["category_button"])}</a></p>
</div>
""".strip()


def related(items):
    links = "".join(f'<li><a href="{href}">{escape(label)}</a></li>' for label, href in items)
    return f"<h2>Súvisiace návody na VEVO</h2>\n<ul>{links}</ul>"


def faq(items):
    parts = ["<h2>FAQ</h2>"]
    for question, answer in items:
        parts.append(f"<h3>{escape(question)}</h3><p>{escape(answer)}</p>")
    return "\n".join(parts)


def render_article(article):
    parts = [
        f"<p><strong>Rýchla odpoveď:</strong> {escape(article['answer'])}</p>",
        situation_box(article["situations"]),
        callout("Rýchly praktický postup", article["quick"]),
    ]
    parts.extend(f"<p>{escape(paragraph)}</p>" for paragraph in article["intro"])
    parts.append("<h2>Prečo tento problém vzniká</h2>")
    parts.extend(f"<p>{escape(paragraph)}</p>" for paragraph in article["why"])
    parts.append("<h2>Rozhodovanie podľa situácie</h2>")
    parts.append(table(["Situácia", "Čo sa deje", "Prvý rozumný krok"], article["rows"]))
    parts.append("<h2>Postup krok za krokom</h2>")
    parts.append("<ol>" + "".join(f"<li>{escape(step)}</li>" for step in article["steps"]) + "</ol>")
    parts.append("<h2>Kontrolná tabuľka pred praním alebo osviežením</h2>")
    parts.append(table(["Signál", "Možná príčina", "Čo urobiť"], article["decision_rows"]))
    parts.append("<h2>Čomu sa vyhnúť</h2>")
    parts.append("<ul>" + "".join(f"<li>{escape(item)}</li>" for item in article["mistakes"]) + "</ul>")
    for heading, body in article["detail_sections"]:
        parts.append(f"<h2>{escape(heading)}</h2><p>{escape(body)}</p>")
    parts.append(callout("Najdôležitejšie pravidlo", article["rule"], background="#fffaf5", border="#e6ded2"))
    parts.append("<h2>Kedy byť opatrný</h2>")
    parts.extend(f"<p>{escape(paragraph)}</p>" for paragraph in article["caution"])
    parts.append("<h2>Odbornejší pohľad</h2>")
    parts.extend(f"<p>{escape(paragraph)}</p>" for paragraph in article["expert"])
    parts.append(source_box(article["sources"]))
    parts.append(sales_block(article["sales"]))
    parts.append(related(article["related"]))
    parts.append(faq(article["faq"]))
    return "\n".join(parts)


ARTICLES = [
    {
        "title": "Ako prať kúpeľňovú predložku: guma, vlhkosť, chlpy a zápach po sprchovaní",
        "short": "Kúpeľňovú predložku perte až po vytrasení vlasov, prachu a hrubej špiny. Skontrolujte štítok, najmä ak má gumovú alebo protišmykovú spodnú vrstvu. Najväčší problém nebýva samotné pranie, ale vlhkosť po sprchovaní, pomalé sušenie a zvyšky vlasov alebo prachu v spodnej vrstve.",
        "answer": "Kúpeľňovú predložku najprv vytraste, odstráňte vlasy a prach, skontrolujte štítok a perte ju samostatne alebo s podobnými textíliami. Ak má gumovú spodnú vrstvu, nepoužívajte horúcu vodu, agresívne odstreďovanie ani sušičku bez povolenia výrobcu. Zápach riešte hlavne rýchlym sušením medzi sprchovaniami, nie silnou vôňou.",
        "situations": [
            "predložka po sprchovaní zostáva dlho mokrá a cítiť zatuchnutie",
            "na spodnej strane je guma alebo protišmyková vrstva a bojíte sa poškodenia",
            "v predložke sa držia vlasy, prach, chlpy zo zvierat alebo zvyšky kozmetiky",
            "predložka po praní stvrdla, skrútila sa alebo zanecháva zápach v kúpeľni",
            "neviete, či ju prať s uterákmi, samostatne alebo ručne",
            "chcete kúpeľňu udržať čistú bez toho, aby ste len prekrývali pach vôňou",
            "po praní predložka schne pomaly a zápach sa vráti o deň neskôr",
            "riešite rozdiel medzi bavlnenou, froté, mikrovláknovou a pogumovanou predložkou",
        ],
        "quick": [
            "<strong>Najprv vytriasť.</strong> Vlasy, prach a chlpy nepatria do bubna práčky.",
            "<strong>Skontrolovať spodnú vrstvu.</strong> Guma a protišmykový náter nemusia zniesť teplo a silné odstreďovanie.",
            "<strong>Prať oddelene od oblečenia.</strong> Predložka z kúpeľne má inú špinu než tričká alebo spodná bielizeň.",
            "<strong>Sušiť rýchlo a vzdušne.</strong> Vlhká predložka v kúpeľni je najčastejší zdroj zatuchnutia.",
            "<strong>Vôňa je až posledný krok.</strong> Ak je textil mokrý alebo špinavý, parfumovanie problém nevyrieši.",
        ],
        "intro": [
            "Kúpeľňová predložka je malý kus textilu, ale pracuje v ťažkých podmienkach. Denne stojí na vlhkej podlahe, zachytáva vodu zo sprchy, vlasy, prach, zvyšky mydla, kozmetiku a niekedy aj chlpy domácich zvierat. Preto sa môže zdať čistá zvrchu, no v spodnej vrstve už drží zápach.",
            "Pri praní predložky nestačí pozerať iba na farbu a materiál vrchnej strany. Rozhoduje aj spodok. Niektoré predložky majú gumovú, latexovú alebo protišmykovú vrstvu, ktorá je citlivejšia na teplo, trenie a sušičku. Ak sa poškodí, predložka môže začať púšťať kúsky, šmýkať sa alebo sa vlniť.",
            "Ľudia často hľadajú jednoduchú odpoveď na otázky ako ako prať kúpeľňovú predložku, či môže ísť do práčky, prečo zapácha po sprchovaní alebo ako odstrániť vlasy z predložky. Dôležité je oddeliť mechanickú špinu, prací cyklus a sušenie. Každý krok rieši inú časť problému.",
            "Ak predložka zapácha, nemusí to znamenať, že potrebujete silnejší prací prostriedok. Často stačí častejšie vyvesenie, lepšie vetranie kúpeľne, menej preplnený bubon a úplné vysušenie pred ďalším položením na zem.",
        ],
        "why": [
            "Zápach vzniká najmä tam, kde sa stretne vlhkosť, zvyšky kože, vlasy, prach a slabé prúdenie vzduchu. Kúpeľňa býva teplá a vlhká, takže textil môže zostať vlhký aj po tom, ako povrch na dotyk pôsobí suchšie.",
            "Predložka navyše leží na podlahe, kde spodná vrstva dýcha horšie než uterák zavesený na háčiku. Ak je spodok pogumovaný, vzduch cez neho prechádza ešte slabšie. Preto sa zápach často drží práve v spodnej vrstve alebo v okrajoch.",
            "Vlasy a chlpy zhoršujú pranie dvoma spôsobmi. Najprv mechanicky držia špinu v textílii a potom môžu skončiť v práčke, filtri alebo na inej bielizni. Preto je vytrasenie a povrchové odstránenie vlasov pred praním praktický krok, nie detail.",
            "Pri gumovej vrstve je rizikom aj teplo. Horúca voda alebo sušička môžu urýchliť praskanie alebo odlupovanie. Ak štítok nie je jasný, bezpečnejší je miernejší program a voľné sušenie.",
        ],
        "rows": [
            ("Bavlnená alebo froté predložka", "znesie viac vody, ale dlho schne", "prať oddelene a sušiť s priestorom"),
            ("Predložka s gumou", "spodná vrstva môže reagovať na teplo", "voliť nižšiu teplotu a nepreháňať odstreďovanie"),
            ("Mikrovláknová predložka", "rýchlo sa plní vlasmi a prachom", "najprv vytriasť a neprať s uterákmi, ktoré púšťajú vlákna"),
            ("Predložka po domácich zvieratách", "chlpy sa držia v štruktúre", "odstrániť chlpy pred práčkou a skontrolovať filter"),
            ("Predložka s pachom vlhkosti", "problém je sušenie alebo kúpeľňa", "vyprať, vysušiť mimo vlhkej kúpeľne a vetrať"),
        ],
        "steps": [
            "Predložku najprv vytraste vonku alebo nad vaňou, aby z nej odišli vlasy, prach a uvoľnené nečistoty.",
            "Skontrolujte štítok a spodnú vrstvu. Ak je guma popraskaná, pranie v práčke môže poškodenie zväčšiť.",
            "Povrch prejdite rukou, valčekom alebo kefou, ak sú v ňom chlpy a vlasy.",
            "Perte samostatne alebo s podobnými kúpeľňovými textíliami. Nedávajte ju k bežnému oblečeniu.",
            "Zvoľte šetrnejší program a primeranú dávku pracieho gélu. Viac gélu nepomôže, ak sa zle vypláchne.",
            "Po praní predložku hneď vyberte, vytvarujte a sušte tak, aby vzduch prúdil aj cez spodnú stranu.",
            "Na podlahu ju vráťte až úplne suchú. Ak je kúpeľňa vlhká, nechajte ju doschnúť mimo nej.",
        ],
        "decision_rows": [
            ("Predložka smrdí aj po praní", "ostala vlhkosť alebo špina v spodnej vrstve", "sušiť mimo kúpeľne a skontrolovať pogumovanie"),
            ("Guma sa drobí", "materiál starne alebo nezvládol teplo", "neprať horúco a zvážiť výmenu predložky"),
            ("Na predložke sú vlasy", "mechanická špina pred praním", "odstrániť nasucho, až potom prať"),
            ("Predložka je tvrdá", "zvyšky pracieho prostriedku alebo teplo", "znížiť dávku a dôkladne opláchnuť"),
            ("Kúpeľňa zapácha po vlhkosti", "nejde len o predložku", "vetrať, riešiť uteráky, podlahu a sušenie textílií"),
        ],
        "mistakes": [
            "Prať predložku plnú vlasov bez vytrasenia.",
            "Dávať pogumovanú predložku do horúcej sušičky bez pokynu na štítku.",
            "Prať ju s tričkami, spodnou bielizňou alebo jemným oblečením.",
            "Použiť veľa gélu a potom sa čudovať, že predložka pôsobí ťažko.",
            "Vrátiť ju na podlahu ešte vlhkú.",
            "Prekrývať zápach kúpeľne vôňou bez riešenia vlhkosti.",
        ],
        "detail_sections": [
            ("Ako prať predložku s gumovou spodnou vrstvou", "Pri pogumovanej spodnej vrstve voľte opatrnosť. Guma nemá rada vysoké teploty, agresívne otáčky a dlhé horúce sušenie. Ak štítok povoľuje pranie, držte sa nižšej teploty a po praní predložku vytvarujte. Ak je spodok už popraskaný alebo lepkavý, práčka môže stav zhoršiť."),
            ("Ako odstrániť vlasy a chlpy pred praním", "Vlasy a chlpy najprv odstráňte nasucho. Pomôže vytrasenie, rukavica, mäkká kefa alebo valček. Ak ich necháte v predložke, časť sa môže zachytiť vo filtri práčky alebo skončiť na ďalšej bielizni. Pri domácich zvieratách je tento krok ešte dôležitejší."),
            ("Ako sušiť kúpeľňovú predložku", "Po praní ju nenechávajte zloženú. Zaveste ju tak, aby schnutiu nebránila spodná vrstva. Ak je kúpeľňa vlhká alebo bez okna, lepšie je sušiť predložku v inej miestnosti s prúdením vzduchu. Až úplne suchá predložka má ísť späť na podlahu."),
            ("Ako často prať kúpeľňovú predložku", "Frekvencia závisí od počtu ľudí, vetrania a toho, ako často je mokrá. V domácnosti, kde sa sprchuje viac ľudí denne, potrebuje predložka častejšie sušenie a pravidelnejšie pranie. Ak je na nej cítiť vlhkosť, nečakajte iba na viditeľnú špinu."),
            ("Kedy predložku radšej vymeniť", "Ak sa guma drobí, predložka sa šmýka, drží trvalý zápach alebo sa po praní deformuje, ďalšie pranie nemusí byť riešenie. Bezpečnosť na mokrej podlahe je dôležitejšia než snaha zachrániť starý kus za každú cenu."),
        ],
        "rule": [
            "Kúpeľňovú predložku neperte ako obyčajný uterák, kým neviete, čo má na spodnej strane.",
            "Najprv odstráňte vlasy a prach, potom perte a nakoniec riešte rýchle sušenie.",
        ],
        "caution": [
            "Ak má predložka latexovú, gumovú alebo lepenú spodnú vrstvu, rešpektujte štítok. Niektoré kúsky sa v práčke môžu poškodiť aj pri zdanlivo miernom programe.",
            "Pri zápachu v kúpeľni sledujte aj uteráky, sprchový kút, podlahu a vetranie. Predložka môže byť iba jeden z viacerých zdrojov vlhkosti.",
        ],
        "expert": [
            "Z odborného pohľadu je hlavný problém kúpeľňového textilu vlhkosť. Textil, ktorý sa opakovane namočí a nemá čas preschnúť, vytvára vhodné prostredie pre zatuchnutý pach. Pranie pomôže, ale len vtedy, keď po ňom nasleduje dobré sušenie.",
            "Domáce odporúčania k vlhkosti opakovane zdôrazňujú vetranie, zníženie vlhkosti a odstránenie zdroja problému. Pri predložke to znamená nenechávať ju trvalo mokrú na podlahe, ale dať jej priestor preschnúť.",
            "Pri textilnom štítku nejde o formalitu. Kombinácia vlákna, farby, lepidla a spodnej vrstvy rozhoduje, či výrobok znesie práčku, teplotu a odstreďovanie.",
        ],
        "sources": [
            ("US EPA: A Brief Guide to Mold, Moisture and Your Home", "https://www.epa.gov/mold/brief-guide-mold-moisture-and-your-home"),
            ("Energy.gov: Laundry tips", "https://www.energy.gov/energysaver/laundry"),
            ("GINETEX: Textile care labelling", "https://www.ginetex.net/GB/labelling/labelling.asp"),
        ],
        "sales": {
            "heading": "Riešenie na pranie kúpeľňových textílií",
            "intro": "Pri predložke potrebujete najmä šetrné pranie, primerané dávkovanie a dobrý oplach. Produkt nenahrádza vytrasenie vlasov ani sušenie v suchej miestnosti.",
            "product_name": "Prací gél hypoalergénny z Marseillského mydla 1L",
            "product_href": "/p-1551/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l",
            "fit": "pri bežných kúpeľňových textíliách, keď chcete prať bez zbytočne ťažkej parfumácie a potrebujete dobrý základný prací krok.",
            "boundary": "ak je predložka plesnivá, má poškodenú gumu alebo zostáva mokrá na podlahe, najprv riešte stav výrobku a vlhkosť v kúpeľni.",
            "product_button": "Pozrieť prací gél",
            "category_title": "Vyberte prací gél podľa textilu",
            "category_intro": "Kúpeľňové textílie perte oddelene od bežného oblečenia a dávkujte skôr rozumne než silno.",
            "category_bullets": [
                ("Predložka", "najprv vytriasť a prať podľa štítku."),
                ("Uteráky", "sledovať savosť a nepreťažovať ich zvyškami produktu."),
                ("Župan", "riešiť mäkkosť, savosť a pomalé sušenie."),
            ],
            "category_href": "/c/vevo-home-care/pranie/praci-gel",
            "category_button": "Pozrieť pracie gély",
        },
        "related": [
            ("Ako prevoňať bielizeň v malej kúpeľni", "/n/ako-prevonat-bielizen-v-malej-kupelni-vlhkost-susenie-a-jemna-vona-bez-zatuchnutia"),
            ("Preplnená práčka: prečo sa bielizeň nevyperie", "/n/preplnena-pracka-preco-sa-bielizen-nevyperie-neoplachne-a-zapacha"),
            ("Ako vyčistiť sušiak na bielizeň", "/n/ako-vycistit-susiak-na-bielizen-aby-neprenasal-spinu-na-pradlo"),
            ("Prečo je bielizeň po praní tvrdá alebo lepkavá", "/n/preco-je-bielizen-po-prani-tvrda-alebo-lepkava-zvysky-gelu-davkovanie-a-oplach"),
            ("Ako vyčistiť filter práčky", "/n/ako-vycistit-filter-pracky-ked-bielizen-zapacha-alebo-voda-odteka-pomaly"),
        ],
        "faq": [
            ("Môže ísť kúpeľňová predložka do práčky?", "Iba ak to povoľuje štítok. Pri gumovej alebo lepenéj spodnej vrstve buďte opatrní s teplotou, otáčkami a sušičkou."),
            ("Prečo predložka zapácha aj po praní?", "Často zostala dlho vlhká, nepreschla spodná vrstva alebo je problém vo vlhkej kúpeľni."),
            ("Mám predložku prať s uterákmi?", "Len ak to dáva zmysel podľa materiálu a špiny. S bežným oblečením ju radšej nemiešajte."),
            ("Ako odstrániť vlasy z predložky?", "Najprv nasucho vytriasť, prejsť kefou alebo valčekom a až potom prať."),
            ("Pomôže vôňa do kúpeľne na zapáchajúcu predložku?", "Nie ako hlavné riešenie. Najprv treba predložku vyčistiť a vysušiť."),
        ],
    },
    {
        "title": "Ako prať župan, aby zostal mäkký, savý a nezatuchol po sprche",
        "short": "Župan perte podľa materiálu, samostatne alebo s podobnými uterákmi, bez preplnenia bubna a s dôkladným sušením. Mäkkosť nevzniká len avivážou. Ak župan po sprche zatuchne, problém je často v tom, že zostáva dlho vlhký na háčiku alebo je v ňom priveľa zvyškov pracieho prostriedku.",
        "answer": "Župan perte s dostatkom priestoru v bubne, primeranou dávkou pracieho gélu a bez ťažkého avivážového filmu. Po praní ho hneď vyberte, pretrepte a sušte vzdušne. Ak má zostať mäkký aj savý, najdôležitejšie je dobré opláchnutie, nepreplnená práčka a to, aby po sprche nikdy neostal dlho zavesený vlhký v nevetranej kúpeľni.",
        "situations": [
            "župan po sprche zostáva mokrý až do ďalšieho dňa",
            "froté župan je tvrdý, drsný alebo menej savý",
            "mikrovláknový župan drží pach aj po praní",
            "župan vonia pekne z práčky, ale v kúpeľni zatuchne",
            "neviete, či použiť aviváž, octovú aviváž alebo len prací gél",
            "chcete dosiahnuť mäkký pocit ako v hoteli bez ťažkého povlaku",
            "župan je hrubý a v práčke zaberie veľa miesta",
            "riešite župan po saune, bazéne alebo častom používaní",
        ],
        "quick": [
            "<strong>Župan potrebuje priestor.</strong> Hrubý froté župan neperte natlačený medzi obliečkami.",
            "<strong>Savosť je dôležitejšia než parfumácia.</strong> Ťažké nánosy môžu zhoršiť pocit aj funkciu.",
            "<strong>Sušenie rozhoduje.</strong> Župan po sprche rozložte alebo zaveste tak, aby preschol.",
            "<strong>Materiál mení postup.</strong> Froté, bavlna, bambusová viskóza a mikrovlákno sa nesprávajú rovnako.",
            "<strong>Pri tvrdosti najprv riešte zvyšky.</strong> Priveľa gélu alebo aviváže môže spraviť župan ťažkým.",
        ],
        "intro": [
            "Župan je medzi uterákom, domácim oblečením a posteľným textilom. Dotýka sa pokožky po sprche, nasáva vodu, často visí v kúpeľni a používa sa opakovane medzi praniami. Preto sa pri ňom rieši viac než len otázka, na akej teplote ho vyprať.",
            "Najčastejšie problémy sú tvrdý župan, zatuchnutý župan po sprche, slabá savosť, príliš ťažká vôňa alebo pocit, že textil je po praní hutný. Všetky tieto prejavy môžu súvisieť s preplneným bubnom, dávkovaním, oplachom, tvrdou vodou alebo pomalým sušením.",
            "Ak chcete mäkký župan, nemusí to znamenať viac aviváže. Pri savých textíliách je niekedy lepšia jednoduchšia rutina: správny prací gél, primeraná dávka, dobrý oplach a sušenie s priestorom. Až potom má zmysel riešiť vôňu alebo jemnejší pocit.",
            "Pri župane je dôležité aj to, čo sa deje po sprche. Ak ho po každom použití zavesíte v nevetranej kúpeľni na úzky háčik, hrubé časti pri golieri a páse nemusia preschnúť. Práve tam sa zatuchnutie vracia najrýchlejšie.",
        ],
        "why": [
            "Froté župan má slučkovú štruktúru podobnú uteráku. Tá je dobrá na savosť, ale drží vodu aj zvyšky produktu. Ak sa zle opláchne, vlákna môžu pôsobiť tvrdšie a menej príjemne.",
            "Mikrovláknový župan môže schnúť rýchlejšie, ale niektoré syntetické vlákna držia pach z potu alebo vlhkosti inak než bavlna. Pri ňom je dôležité prať skôr, nepreťažovať bubon a nenechávať ho zatvorený vlhký.",
            "Aviváž môže zlepšiť pocit na dotyk, ale pri savých textíliách treba byť opatrný. Ak sa tvorí film, župan môže pôsobiť mäkko na povrchu, ale horšie saje a ťažšie schne.",
            "Zatuchnutie po sprche vzniká často mimo práčky. Hrubý župan schne pomaly, kúpeľňa je vlhká a textil sa niekedy dotýka steny alebo iných mokrých uterákov. Bez prúdenia vzduchu sa čistý výsledok rýchlo stratí.",
        ],
        "rows": [
            ("Froté bavlnený župan", "veľa saje a dlho schne", "prať vo voľnejšej dávke a dôkladne sušiť"),
            ("Mikrovláknový župan", "môže držať pach z vlhkosti", "nepreplniť bubon, sušiť rýchlo"),
            ("Bambusová alebo viskózová zmes", "môže byť jemnejšia na tvar", "rešpektovať štítok a vyhnúť sa horúcemu zásahu"),
            ("Župan po saune", "pot a vlhkosť v hrubých miestach", "vetrať pred košom a prať oddelene"),
            ("Župan s kapucňou", "kapucňa schne pomalšie", "po praní ju rozložiť a nenechať zlepenú"),
        ],
        "steps": [
            "Skontrolujte štítok a rozlíšte, či ide o froté, mikrovlákno, bavlnu alebo zmes.",
            "Župan pred praním vyvetrajte, ak je mokrý. Nevhadzujte ho vlhký do zatvoreného koša na niekoľko dní.",
            "Perte ho s podobnými savými textíliami, ale nepreplňte bubon. Hrubý župan potrebuje priestor na oplach.",
            "Použite primeranú dávku pracieho gélu. Pri tvrdosti alebo lepkavosti je častou chybou priveľa produktu.",
            "Aviváž používajte opatrne. Pri savosti sledujte, či župan po čase nepôsobí ťažko alebo menej nasiakavo.",
            "Po praní župan pretrepte, vyrovnajte pás, kapucňu a golier a sušte s dostatkom vzduchu.",
            "Po sprche ho nevešajte zložený na malý háčik. Lepšie schne na širšom vešiaku alebo cez tyč.",
        ],
        "decision_rows": [
            ("Župan je tvrdý", "zvyšky gélu, tvrdá voda alebo preplnenie", "znížiť dávku, pridať oplach a prať vo voľnejšej dávke"),
            ("Župan horšie saje", "nános aviváže alebo produktu", "obmedziť ťažké zmäkčovanie a skontrolovať oplach"),
            ("Župan zatuchne po sprche", "pomalé sušenie v kúpeľni", "rozložiť, vetrať a neponechať ho pri stene"),
            ("Kapucňa ostáva vlhká", "hrubšia časť nedoschla", "po praní aj používaní ju rozprestrieť"),
            ("Župan silno vonia", "veľa parfumácie alebo zvyškov", "znížiť intenzitu a hodnotiť výsledok po vysušení"),
        ],
        "mistakes": [
            "Prať hrubý župan v plnom bubne s obliečkami.",
            "Pridávať viac aviváže vždy, keď je župan tvrdý.",
            "Nechať mokrý župan visieť zložený v nevetranej kúpeľni.",
            "Sušiť župan horúco bez kontroly štítku.",
            "Hodnotiť mäkkosť len podľa vône, nie podľa savosti.",
            "Miešať župan po saune s jemným oblečením.",
        ],
        "detail_sections": [
            ("Ako prať froté župan", "Froté župan perte podobne ako kvalitný uterák: s priestorom, rozumnou dávkou a dôkladným oplachom. Slučky potrebujú zostať otvorené, nie obalené ťažkým filmom. Ak župan stvrdne, často pomôže menšia dávka pracieho produktu a lepšie vypláchnutie."),
            ("Ako prať mikrovláknový župan", "Mikrovlákno väčšinou nevyžaduje ťažké zmäkčovanie. Dôležité je neprať ho s textíliami, ktoré púšťajú vlákna, a sušiť ho tak, aby nezostal vlhký v záhyboch. Pri syntetike sledujte aj pach po zahriatí tela."),
            ("Ako dosiahnuť mäkký pocit bez straty savosti", "Mäkkosť županu nie je iba výsledok aviváže. Vzniká aj tým, že vo vláknach nezostanú zvyšky gélu, že župan nie je prepratý v natlačenom bubne a že pri sušení nezostane stuhnutý v jednom tvare. Pri savých textíliách myslite najprv na čistotu a oplach."),
            ("Ako sušiť župan po sprche", "Po použití ho zaveste tak, aby neležal na sebe. Ak má kapucňu, rozložte ju. Ak má pás, nenechajte ho omotaný okolo mokrého textilu. V malej kúpeľni pomáha krátke vetranie alebo presunutie županu mimo najvlhkejšej zóny."),
            ("Kedy župan prať častejšie", "Častejšie pranie dáva zmysel po saune, bazéne, chorobe, intenzívnom potení alebo ak župan používa viac ľudí. Ak ho používate len na krátke oblečenie po sprche a dobre schne, rozhoduje skôr pach, dotyk a hygienický kontext než pevný kalendár."),
            ("Ako riešiť župan tvrdý po praní", "Tvrdý župan nemusí znamenať zlý materiál. Často ide o kombináciu tvrdej vody, väčšej dávky pracieho gélu a slabého oplachu. Skúste prať menšiu dávku textílií naraz, nepoužiť produkt od oka a po praní župan poriadne vytriasť. Ak je tvrdosť najmä na ramenách, kapucni a páse, problém býva v tom, že hrubšie časti sa v bubne horšie prepláchli a potom schli príliš dlho."),
            ("Ako prať župan po bazéne alebo wellnesse", "Po bazéne, vírivke alebo wellness pobyte nenechávajte župan zabalený v taške. Chlór, pot, kozmetika a vlhkosť sa v hrubom textile držia dlhšie než v tričku. Ak ho nemôžete vyprať hneď, aspoň ho doma rozložte a nechajte preschnúť. Pri praní nepoužívajte preplnený bubon, pretože župan potrebuje pohyb a vodu, aby sa z vlákien dostali aj zvyšky po bazéne."),
        ],
        "rule": [
            "Župan nemá byť len voňavý. Má byť čistý, savý, dobre opláchnutý a úplne suchý medzi používaniami.",
            "Ak zatuchne, najprv riešte sušenie a zvyšky vo vláknach, nie silnejšiu parfumáciu.",
        ],
        "caution": [
            "Pri citlivej pokožke, detskom župane alebo textílii po chorobe voľte jednoduchšiu praciu rutinu a dôkladný oplach. Výrazná vôňa nemá nahradiť hygienu.",
            "Ak župan mení tvar, púšťa vlákna alebo má nejasný materiál, riaďte sa štítkom. Nie každý mäkký župan znesie rovnakú teplotu, sušičku alebo odstreďovanie.",
        ],
        "expert": [
            "Savosť textilu závisí od štruktúry vlákna, povrchu a zvyškov v materiáli. Ak sa na vláknach drží povlak, župan môže pôsobiť mäkšie, ale horšie prijíma vodu. Preto pri savých výrobkoch netreba automaticky pridávať viac zmäkčovadla.",
            "Pri vlhkosti platí rovnaký princíp ako pri uterákoch: čistý textil môže zatuchnúť, ak sa po použití nevysuší. Pranie vyrieši časť problému, ale denný spôsob vešania rozhoduje o tom, ako dlho vydrží svieži.",
            "Odborné odporúčania k starostlivosti o textil zdôrazňujú čítanie štítkov a výber šetrného postupu podľa materiálu. Pri župane je to obzvlášť dôležité, pretože jeden výrobok môže kombinovať slučkovú väzbu, kapucňu, pás, výšivku a zmes vlákien.",
        ],
        "sources": [
            ("Energy.gov: Laundry tips", "https://www.energy.gov/energysaver/laundry"),
            ("GINETEX: Textile care labelling", "https://www.ginetex.net/GB/labelling/labelling.asp"),
            ("USGS: Hardness of Water", "https://www.usgs.gov/water-science-school/science/hardness-water"),
        ],
        "sales": {
            "heading": "Riešenie pre mäkší pocit bez ťažkého povlaku",
            "intro": "Ak je župan tvrdý, najprv skontrolujte dávkovanie, oplach a sušenie. Keď je problém najmä v pocite z vlákna, môže pomôcť jemnejšia alternatíva ku klasickej aviváži.",
            "product_name": "Pravá octová aviváž lesná zmes 1L",
            "product_href": "/p-1626/prava-octova-avivaz-lesna-zmes-1l",
            "fit": "pri savých domácich textíliách, kde chcete sviežejší pocit bez ťažkého klasického avivážového filmu.",
            "boundary": "ak je župan lepkavý alebo zatuchnutý, najprv ho dobre vypláchajte, upravte dávkovanie a vyriešte sušenie.",
            "product_button": "Pozrieť octovú aviváž",
            "category_title": "Porovnajte octové aviváže pre domáce textílie",
            "category_intro": "Pri županoch a uterákoch sledujte hlavne savosť, oplach a pocit na pokožke.",
            "category_bullets": [
                ("Froté", "potrebuje savosť a vzdušné sušenie."),
                ("Mikrovlákno", "často nepotrebuje ťažké zmäkčovanie."),
                ("Hrubé župany", "perte vo voľnejšej dávke, aby sa dobre opláchli."),
            ],
            "category_href": "/c/vevo-home-care/pranie/avivaz/octova-avivaz",
            "category_button": "Pozrieť octové aviváže",
        },
        "related": [
            ("Prečo je bielizeň po praní tvrdá alebo lepkavá", "/n/preco-je-bielizen-po-prani-tvrda-alebo-lepkava-zvysky-gelu-davkovanie-a-oplach"),
            ("Extra oplach v práčke", "/n/extra-oplach-v-pracke-kedy-pomoze-pri-zapachu-tvrdej-bielizni-a-citlivej-pokozke"),
            ("Koľko bielizne dať do práčky", "/n/kolko-bielizne-dat-do-pracky-prakticka-kapacita-podla-uterakov-obliecok-a-sportu"),
            ("Ako odstrániť mastnú masť z uteráka", "/n/ako-odstranit-mastnu-mast-z-uteraka-pyzama-a-tricka"),
            ("Ako prať oblečenie pri peľovej alergii", "/n/ako-prat-oblecenie-pri-pelovej-alergii-po-prichode-zvonka"),
        ],
        "faq": [
            ("Môžem župan prať s uterákmi?", "Áno, ak sú podobnej farby a materiálu, ale nepreplňte bubon. Hrubý župan potrebuje veľa priestoru."),
            ("Prečo je župan po praní tvrdý?", "Často pre zvyšky pracieho prostriedku, tvrdú vodu, preplnenie bubna alebo nevhodné sušenie."),
            ("Je aviváž vhodná na župan?", "Opatrne. Pri savých textíliách sledujte, či neznižuje savosť alebo nevytvára ťažký povlak."),
            ("Ako zabrániť zatuchnutiu po sprche?", "Župan po použití rozložte, vetrajte kúpeľňu a nenechajte hrubé časti dlho vlhké."),
            ("Ako často prať župan?", "Podľa používania, potenia a sušenia. Pri zápachu, saune, bazéne alebo chorobe ho perte skôr."),
        ],
    },
    {
        "title": "Ako osviežiť posteľ medzi praniami: vetranie, pyžamo, matrac a jemná vôňa",
        "short": "Posteľ medzi praniami osviežite najmä vetraním, odhrnutím periny po spánku, pravidelnou výmenou pyžama a kontrolou matraca, vankúša a vlhkosti v spálni. Jemná vôňa môže pomôcť s príjemným dojmom v miestnosti, ale nenahrádza pranie obliečok, riešenie potu ani čistenie matraca.",
        "answer": "Posteľ medzi praniami osviežte tak, že ráno odhrniete perinu, necháte matrac a obliečky vyvetrať, pravidelne meníte pyžamo a sledujete pot, vlhkosť a pach vankúša. Interiérovú vôňu používajte jemne do priestoru, nie ako náhradu za pranie posteľnej bielizne alebo riešenie matraca, ktorý zapácha.",
        "situations": [
            "posteľ po pár nociach nepôsobí sviežo, ale obliečky ešte nechcete prať",
            "ráno je matrac alebo vankúš vlhký od potu",
            "pyžamo drží pach a prenáša ho späť do postele",
            "spálňa potrebuje jemnú vôňu, ale nechcete ťažkú parfumáciu",
            "neviete, či osviežiť posteľ sprejom, vetrať alebo rovno prať obliečky",
            "vankúš, paplón alebo matrac majú vlastný pach",
            "posteľ používajú deti, alergik alebo človek s citlivejším nosom",
            "chcete udržať hosťovskú posteľ príjemnú medzi návštevami",
        ],
        "quick": [
            "<strong>Ráno posteľ hneď nezakrývajte.</strong> Nechajte odísť vlhkosť zo spánku.",
            "<strong>Pyžamo je súčasť postele.</strong> Ak zapácha, čisté obliečky dlho nevydržia.",
            "<strong>Vankúš a matrac sledujte samostatne.</strong> Nie každý pach je z obliečok.",
            "<strong>Vôňu používajte do priestoru.</strong> Jemný interiérový sprej má doplniť čistotu, nie zakryť pot.",
            "<strong>Pri vlhkosti perte alebo vetrajte, neprekrývajte.</strong> Zatuchnutie je signál, nie estetický detail.",
        ],
        "intro": [
            "Posteľ je miesto, kde sa každý deň stretáva pot, kožný maz, pyžamo, vlasy, prach, matrac, vankúš a vlhkosť z miestnosti. Preto môže prestať pôsobiť sviežo skôr, než nastane bežný deň prania obliečok. Osvieženie medzi praniami však neznamená iba nastriekať vôňu.",
            "Rozumná rutina začína vetraním. Po spánku je posteľ často teplá a mierne vlhká. Ak ju hneď zakryjete dekou alebo usteliete natesno, vlhkosť zostane pod perinou a pach sa stabilizuje. Krátke odhrnutie vie spraviť veľký rozdiel.",
            "Druhý dôležitý bod je pyžamo. Čisté obliečky neostanú dlho svieže, ak sa do nich každý večer vracia pyžamo, ktoré drží pot alebo telové krémy. Preto je otázka ako osviežiť posteľ medzi praniami zároveň otázkou, ako často prať pyžamo a ako vetrať spálňu.",
            "Jemná vôňa do spálne môže byť príjemná, najmä pred návštevou alebo pri hosťovskej posteli. Musí však prísť až po tom, čo je jasné, že posteľ nie je vlhká, matrac nezapácha a obliečky nepotrebujú pranie.",
        ],
        "why": [
            "Počas spánku telo prirodzene uvoľňuje vlhkosť a teplo. Časť sa dostane do pyžama, časť do obliečok, vankúša a matraca. Ak posteľ nemá čas preschnúť, sviežosť sa stratí aj bez viditeľnej špiny.",
            "Matrac a vankúš sa perú alebo čistia inak než obliečky. Keď zdroj pachu leží hlbšie, výmena posteľnej bielizne pomôže iba krátko. Preto sa oplatí rozlíšiť pach obliečky, pyžama, vankúša, matraca a vzduchu v miestnosti.",
            "Vôňa v spálni je citlivá téma, pretože človek ju vníma pri dlhom oddychu a dýchaní počas noci. Silná parfumácia môže pôsobiť rušivo. Lepšia je jemná, krátka a dobre vyvetraná rutina.",
            "Pri alergikoch, deťoch alebo citlivých ľuďoch má prednosť hygiena, pranie a nízka prašnosť. Vôňa je voliteľný doplnok pre komfort, nie hygienické riešenie.",
        ],
        "rows": [
            ("Ráno po spánku", "posteľ je teplá a mierne vlhká", "odhrnúť perinu a vyvetrať miestnosť"),
            ("Pyžamo po viacerých nociach", "prenáša pot späť do obliečok", "vymeniť alebo vyprať skôr"),
            ("Vankúš má pach", "zdroj nie je iba obliečka", "skontrolovať pranie poťahu a stav vankúša"),
            ("Matrac je cítiť", "pach môže byť hlbšie v textílii", "vetrať, chrániť chráničom a riešiť čistenie"),
            ("Hosťovská posteľ", "dlho stojí nepoužívaná", "vyvetrať, skontrolovať prach a jemne prevoňať priestor"),
        ],
        "steps": [
            "Ráno odhrňte perinu a nechajte posteľ niekoľko minút dýchať, najmä ak sa v noci potíte.",
            "Vyvetrajte spálňu. Krátke intenzívne vetranie pomáha viac než snaha prekryť ťažký vzduch vôňou.",
            "Skontrolujte pyžamo. Ak je cítiť pri golieri, podpazuší alebo v páse, vymeňte ho skôr.",
            "Všímajte si vankúš a chránič matraca. Ak pach zostáva po výmene obliečky, zdroj je inde.",
            "Pred použitím interiérového spreja odstráňte zdroj pachu a použite ho jemne do priestoru.",
            "Pri návšteve osviežte najmä vzduch v spálni a posteľnú bielizeň pripravte až po vyvetraní.",
            "Ak je posteľ vlhká, zatuchnutá alebo po chorobe, neodkladajte pranie obliečok.",
        ],
        "decision_rows": [
            ("Posteľ je cítiť ráno", "vlhkosť a teplo po spánku", "odhrnúť, vetrať a neustielať okamžite natesno"),
            ("Vôňa sa rýchlo stratí", "zdroj pachu ostal v pyžame alebo matraci", "skontrolovať textílie pri tele"),
            ("Spálňa je ťažká", "slabé vetranie alebo vlhkosť", "vyvetrať pred použitím vône"),
            ("Obliečky sú čisté, ale vankúš zapácha", "vankúš alebo poťah potrebuje starostlivosť", "riešiť vankúš samostatne"),
            ("Človek má citlivý nos", "vôňa môže rušiť spánok", "použiť menej alebo vôňu vynechať"),
        ],
        "mistakes": [
            "Hneď ráno ustlať posteľ tak, že vlhkosť zostane pod perinou.",
            "Prevoňať posteľ, ktorá potrebuje pranie obliečok.",
            "Striekať vôňu priamo na citlivý textil alebo tvárny materiál bez testu.",
            "Zabudnúť, že pyžamo môže byť hlavný zdroj pachu.",
            "Riešiť matrac iba výmenou obliečky.",
            "Použiť silnú vôňu tesne pred spaním v nevetranej spálni.",
        ],
        "detail_sections": [
            ("Ako vetrať posteľ po spánku", "Najjednoduchší krok je nechať posteľ chvíľu odkrytú. Odhrnutá perina, otvorené okno a prúdenie vzduchu pomôžu odviesť teplo a vlhkosť. Nemusí ísť o dlhé vetranie; dôležité je nezatvoriť vlhkosť hneď pod prehoz."),
            ("Ako často meniť pyžamo", "Pyžamo sa dotýka pokožky rovnako dlho ako obliečky. Ak sa potíte, používate telové krémy alebo spíte v teplej miestnosti, perte ho častejšie. Čisté pyžamo predĺži svieži pocit z obliečok viac než silná vôňa."),
            ("Ako rozlíšiť obliečky, vankúš a matrac", "Ak posteľ zapácha krátko po výmene obliečok, privoňajte samostatne k vankúšu, chrániču a matracu. Zdroj nemusí byť v bielizni. Vtedy pomôže pranie poťahu, vetranie matraca alebo samostatné čistenie podľa materiálu."),
            ("Ako používať vôňu v spálni", "Vôňu používajte skôr do priestoru a s predstihom, nie ako poslednú silnú vrstvu tesne pred spaním. V spálni funguje menej. Cieľom je príjemný prvý dojem po vstupe, nie trvalý ťažký oblak vône počas noci."),
            ("Ako pripraviť hosťovskú posteľ", "Hosťovská posteľ často stojí dlhšie nevyužitá. Pred návštevou vyvetrajte miestnosť, skontrolujte prach, nechajte obliečky nadýchať sa vzduchu a vôňu použite iba jemne. Ak bielizeň zapácha zo skrine, treba ju vyprať, nie len osviežiť."),
            ("Ako osviežiť posteľ v byte s vyššou vlhkosťou", "Vo vlhkom byte je najdôležitejšie nenechať perinu a matrac uzavreté. Posteľ po spánku najprv odkryte, potom krátko vyvetrajte a až po vyschnutí ju ustelte. Ak sa vlhkosť opakuje, pomáha chránič matraca prať samostatne a sledovať, či sa pach netvorí pod ním. Vôňa má v takom prípade prísť až na konci, keď je priestor suchší."),
            ("Ako zistiť, kedy už nestačí osvieženie", "Ak sa pach vráti pár hodín po vyvetraní, nestačí iba osviežiť vzduch. Skontrolujte pyžamo, plachtu, vankúš, chránič matraca a samotný matrac. Pri pote, moči, chorobe alebo výraznom telesnom zápachu je lepšie prať konkrétnu textíliu a riešiť zdroj. Jemné prevoňanie má zmysel vtedy, keď je posteľ hygienicky v poriadku a chcete len príjemnejší prvý dojem."),
        ],
        "rule": [
            "Posteľ medzi praniami osviežite najmä vzduchom, suchom a čistým pyžamom.",
            "Vôňa má doplniť čistý priestor, nie prekryť pot, vlhkosť alebo matracový pach.",
        ],
        "caution": [
            "Interiérový sprej nepoužívajte ako náhradu prania obliečok po chorobe, silnom potení, nehode alebo viditeľnom znečistení.",
            "Pri alergikoch, astmatikoch, malých deťoch alebo citlivom nose začnite bez vône alebo veľmi jemne. Spálňa je priestor na dlhý oddych, nie na silnú parfumáciu.",
        ],
        "expert": [
            "Vnútorné prostredie spálne ovplyvňuje vlhkosť, prach, textílie a vetranie. Ak sa zdroj pachu drží v matraci alebo vankúši, samotná vôňa vo vzduchu vytvorí iba krátky dojem. Hygienický základ je pranie a sušenie textílií, ktoré sa dotýkajú tela.",
            "Z pohľadu vlhkosti je posteľ po spánku podobná iným domácim textíliám: ak sa teplo a voda uzavrú, pach sa rozvíja ľahšie. Preto má zmysel krátke odokrytie a vetranie pred ustlaním.",
            "Pri vôňach platí, že v spálni ich človek vníma dlhšie a pri nižšej aktivite. Jemné dávkovanie a odstup pred spaním sú praktickejšie než silná aplikácia na poslednú chvíľu.",
        ],
        "sources": [
            ("US EPA: A Brief Guide to Mold, Moisture and Your Home", "https://www.epa.gov/mold/brief-guide-mold-moisture-and-your-home"),
            ("US EPA: Volatile Organic Compounds and Indoor Air Quality", "https://www.epa.gov/indoor-air-quality-iaq/volatile-organic-compounds-impact-indoor-air-quality"),
            ("Energy.gov: Laundry tips", "https://www.energy.gov/energysaver/laundry"),
        ],
        "sales": {
            "heading": "Jemné osvieženie spálne po vyvetraní",
            "intro": "Ak je posteľ čistá, suchá a miestnosť vyvetraná, môžete doladiť atmosféru jemnou vôňou do priestoru. Nepoužívajte ju ako náhradu prania alebo čistenia matraca.",
            "product_name": "Interiérový sprej Vevo Premium Škorica & Ihličie 150ml",
            "product_href": "/p-1585/interierovy-sprej-vevo-premium-skorica-ihlicie",
            "fit": "keď chcete krátko osviežiť spálňu alebo hosťovskú izbu po vyvetraní a uprataní.",
            "boundary": "ak obliečky, pyžamo, vankúš alebo matrac zapáchajú, najprv ich vyperte, vyvetrajte alebo vyčistite podľa materiálu.",
            "product_button": "Pozrieť interiérový sprej",
            "category_title": "Vyberte vôňu do interiéru podľa miestnosti",
            "category_intro": "Do spálne sa hodí jemnejší prístup. Vôňa má byť ľahká a použitá s predstihom, aby nerušila pri zaspávaní.",
            "category_bullets": [
                ("Spálňa", "jemne a po vyvetraní."),
                ("Hosťovská izba", "krátke osvieženie pred príchodom návštevy."),
                ("Textílie", "najprv prať a sušiť, vôňu riešiť až potom."),
            ],
            "category_href": "/c/vevo-fragrance/interierovy-sprej",
            "category_button": "Pozrieť interiérové spreje",
        },
        "related": [
            ("Ako často prať posteľné prádlo", "/n/ako-casto-prat-postelne-pradlo"),
            ("Ako správne prať obliečky", "/n/ako-spravne-prat-obliecky-kompletny-sprievodca-starostlivostou"),
            ("Ako odstrániť moč z matraca", "/n/ako-odstranit-moc-z-matraca-plachty-a-detskeho-pyzama"),
            ("Ako vybrať vôňu do prania na zimu", "/n/ako-vybrat-vonu-do-prania-na-zimu-deky-svetre-saly-a-sezonne-textilie"),
            ("Prečo oblečenie zapácha po praní", "/n/preco-moje-oblecenie-zapacha-po-prani-priciny-a-riesenia"),
        ],
        "faq": [
            ("Ako osviežiť posteľ bez prania?", "Odhrňte perinu, vyvetrajte, skontrolujte pyžamo a vankúš. Jemnú vôňu použite až po odstránení príčiny pachu."),
            ("Môžem striekať interiérovú vôňu priamo na obliečky?", "Opatrne. Bezpečnejšie je použiť vôňu do priestoru a rešpektovať citlivé povrchy aj ľudí v domácnosti."),
            ("Prečo posteľ zatuchne aj pri čistých obliečkach?", "Zdroj môže byť pyžamo, vankúš, matrac, vlhkosť v spálni alebo príliš rýchle ustlanie po spánku."),
            ("Kedy už treba obliečky vyprať?", "Po silnom potení, chorobe, viditeľnom znečistení, zápachu alebo ak osvieženie vzduchom nepomáha."),
            ("Aká vôňa je vhodná do spálne?", "Skôr jemná a použitá s predstihom. V spálni je menej často lepšie než silná vôňa tesne pred spaním."),
        ],
    },
]


def link_status(url):
    try:
        response = requests.get(url, timeout=25, headers={"User-Agent": "Mozilla/5.0 Codex preflight"})
        return {"url": url, "status": response.status_code, "final_url": response.url, "ok": 200 <= response.status_code < 400}
    except Exception as exc:  # pragma: no cover
        return {"url": url, "status": None, "final_url": None, "ok": False, "error": f"{type(exc).__name__}: {exc}"}


def write_xls(articles):
    if xlwt is None:
        return None
    workbook = xlwt.Workbook()
    sheet = workbook.add_sheet("news")
    headers = ["title", "short", "long", "date_posted", "time_posted", "active", "link", "commenting"]
    for col, header in enumerate(headers):
        sheet.write(0, col, header)
    for row, article in enumerate(articles, start=1):
        for col, header in enumerate(headers):
            sheet.write(row, col, article.get(header, ""))
    workbook.save(str(OUT_XLS))
    return str(OUT_XLS)


def main():
    articles = []
    for index, article in enumerate(ARTICLES):
        long_html = render_article(article)
        if FORBIDDEN_PUBLIC_RE.search(long_html):
            raise SystemExit(f"Forbidden public wording in article: {article['title']}")
        if "Cena:" in long_html or "€" in long_html or "&euro;" in long_html:
            raise SystemExit(f"Fixed price found in article: {article['title']}")
        articles.append(
            {
                "title": article["title"],
                "short": article["short"],
                "long": long_html,
                "date_posted": BATCH_DATE,
                "time_posted": f"08:{index * 12:02d}:00",
                "active": 1,
                "link": slugify(article["title"]),
                "commenting": "none",
            }
        )

    links = []
    for article in articles:
        links.extend(re.findall(r'href="([^"]+)"', article["long"]))
    unique_links = []
    for href in links:
        if href.startswith("#") or href.startswith("mailto:") or href.startswith("tel:"):
            continue
        full = urljoin(BASE, href)
        if full not in unique_links:
            unique_links.append(full)
    checks = [link_status(url) for url in unique_links]

    preflight = {
        "batch": 30,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "article_count": len(articles),
        "date_posted": BATCH_DATE,
        "all_links_ok": all(item["ok"] for item in checks),
        "link_checks": checks,
        "articles": [
            {
                "title": article["title"],
                "slug": article["link"],
                "words": len(re.sub(r"<[^>]+>", " ", article["long"]).split()),
                "h2_count": article["long"].count("<h2"),
                "table_count": article["long"].count("<table"),
                "styled_block_count": article["long"].count("border-radius"),
                "product_link_count": article["long"].count("/p-"),
                "category_link_count": article["long"].count("/c/"),
            }
            for article in articles
        ],
    }
    if not preflight["all_links_ok"]:
        bad = [item for item in checks if not item["ok"]]
        raise SystemExit(json.dumps({"bad_links": bad}, ensure_ascii=False, indent=2))

    OUT_JSON.write_text(json.dumps(articles, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    OUT_PREFLIGHT.write_text(json.dumps(preflight, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    xls_path = write_xls(articles)
    print(json.dumps({"json": str(OUT_JSON), "preflight": str(OUT_PREFLIGHT), "xls": xls_path, "preflight_summary": preflight}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
