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
BATCH = 34
BATCH_DATE = "2025-09-18"
OUT_JSON = Path("content/VEVO_CONTENT/imports/batch-34-2026-07-08-articles.json")
OUT_PREFLIGHT = Path("content/VEVO_CONTENT/exports/batch-34-2026-07-08-link-preflight.json")
OUT_XLS = Path.home() / "AppData/Local/Temp/vevo-batch-34-bathroom-cleaning-clean-urls.xls"

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


def source_box(items):
    rows = "".join(
        f'<li><a rel="noopener" href="{href}" target="_blank">{esc(label)}</a></li>'
        for label, href in items
    )
    return f"""
<div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; margin: 22px 0; background: #fbfbfb;">
<h2 style="margin-top: 0;">Zdroje a odborný kontext</h2>
<p>Pri kúpeľni rozhoduje povrch, vlhkosť, vetranie, tvrdosť vody a bezpečné dávkovanie. Zdroje nižšie používame ako širší rámec k čisteniu, vlhkosti a bezpečnému používaniu čistiacich produktov; pri konkrétnom materiáli má vždy prednosť odporúčanie výrobcu.</p>
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
<p><strong>Kedy najprv spomaliť:</strong> {product["boundary"]}</p>
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
    parts.append("<h2>Kontrolná tabuľka podľa povrchu a rizika</h2>")
    parts.append(table(["Miesto alebo povrch", "Najväčšie riziko", "Praktická poznámka"], article["check_rows"]))
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
    parts.append(source_box(article["sources"]))
    parts.append(recommendation_block(article))
    parts.append(related_links(article["related"]))
    parts.append(faq(article["faq"], article["faq_title"]))
    return "\n".join(parts)


ARTICLES = [
    {
        "title": "Ako vyčistiť sprchový kút: vodný kameň, mydlové usadeniny, škáry a sklo bez šmúh",
        "short": "Sprchový kút čistite podľa toho, či riešite sklo, škáry, silikón, vaničku alebo batériu. Najprv odstráňte mydlový film, potom vodný kameň a nakoniec povrch vysušte. Pri citlivých materiáloch netestujte silné kyslé ani abrazívne produkty na celej ploche naraz.",
        "answer": "Sprchový kút vyčistíte najlepšie po častiach: sklo a steny opláchnuť, mydlové usadeniny uvoľniť jemným čistením, vodný kameň riešiť len na vhodnom povrchu a po oplachu všetko vysušiť stierkou alebo handričkou. Na bežný vodný kameň môže pomôcť <a href=\"/p-1561/biely-ocot-v-spreji-500-ml\">biely ocot v spreji</a>, ale nie na prírodný kameň alebo citlivé povrchy.",
        "quick": [
            "<strong>Sklo:</strong> najprv odstráňte mydlový film, až potom riešte lesk bez šmúh.",
            "<strong>Škáry:</strong> čistite opatrne, aby ste nevytrhali alebo nepoškodili výplň.",
            "<strong>Silikón:</strong> tmavé bodky môžu súvisieť s vlhkosťou; nestačí ich len prevoňať.",
            "<strong>Vanička:</strong> akrylový povrch nemá rád hrubé abrazívne drhnutie.",
            "<strong>Prevencia:</strong> po sprche pomôže stierka, otvorené dvere a vetranie.",
        ],
        "intro": [
            "Sprchový kút je malé miesto s veľkým množstvom vody, pary, mydla, šampónov, zvyškov kozmetiky a minerálov z vody. Preto sa na skle objavia biele mapy, v rohoch sivý povlak, pri batérii vodný kameň a v škárach tmavé miesta. Ak sa všetko rieši jedným silným zásahom, ľahko sa poškodí povrch alebo sa problém vráti o pár dní.",
            "Pri otázke ako vyčistiť sprchový kút treba rozlíšiť, či ide o bežné umytie po týždni, zanedbaný sprchový kút, sklo bez šmúh, vodný kameň na batérii, usadeniny v škárach alebo vlhkosť pri silikóne. Každá časť má trochu inú logiku. Sklo potrebuje odstrániť film, škáry potrebujú opatrnosť a batéria potrebuje pravidelné dosušenie.",
            "Najlepší výsledok často neprinesie silnejší prípravok, ale lepšie poradie: najprv oplach a mechanické odstránenie zvyškov, potom čistenie podľa povrchu, následne dôkladný oplach a sušenie. Ak z povrchu necháte zaschnúť zvyšky produktu, získate novú vrstvu, ktorá sa bude tváriť ako špina.",
            "Sprchový kút tiež súvisí s textilom v kúpeľni. Ak ostávajú mokré uteráky, nevetrá sa a rohožka je stále vlhká, kúpeľňa bude pôsobiť zatuchnuto aj po umytom skle. Preto sa oplatí prepojiť čistenie sprchy s praním uterákov, sušením kúpeľňovej predložky a krátkou každodennou prevenciou.",
        ],
        "why_heading": "Prečo sa sprchový kút zanáša tak rýchlo",
        "why": [
            "Mydlové usadeniny vznikajú z kombinácie mydla, kožného mazu, šampónu a minerálov z vody. Na skle vytvoria sivý alebo biely film, ktorý obyčajná voda nerozpustí úplne. Ak sa film nechá zasychať, zachytáva ďalšiu špinu a sprchový kút vyzerá zanedbane aj po rýchlom oplachu.",
            "Vodný kameň je najviditeľnejší tam, kde kvapky zasychajú: na skle, pri spodku batérie, na sprchovej hlavici a v rohoch. Tvrdá voda zanechá minerálnu mapu a pri každom sprchovaní sa vrstva obnovuje. Pravidelné zotretie vody je preto praktickejšie než raz mesačne drhnúť hrubú vrstvu.",
            "Vlhkosť pri silikóne a škárach je samostatná téma. Ak sprchový kút po použití zostane zatvorený a nevetrá sa, voda sa drží v rohoch. Tmavé bodky treba brať vážne: niekedy ide o povrchovú špinu, inokedy o problém s dlhodobou vlhkosťou.",
        ],
        "decision_heading": "Ako postupovať podľa časti sprchového kúta",
        "decision_rows": [
            ("Sklenená zástena", "mydlový film, vodný kameň a šmuhy po zlom oplachu", "najprv uvoľniť film, potom opláchnuť a dosušiť"),
            ("Škáry", "porézny materiál drží špinu a môže sa poškodiť drhnutím", "jemná kefka, krátke pôsobenie a dôkladný oplach"),
            ("Silikón", "vlhkosť, tmavnutie a usadeniny v rohoch", "udržiavať suchší, nepoškodzovať ostrým nástrojom"),
            ("Batéria", "vodný kameň pri spojoch a perlátore", "kyslý postup iba na vhodnom povrchu, potom oplach a dosušenie"),
            ("Vanička", "škrabance a klzký film po kozmetike", "nepoužívať hrubé abrazívne drhnutie na citlivý povrch"),
        ],
        "steps": [
            "Sprchový kút najprv opláchnite teplou vodou, aby ste uvoľnili čerstvé zvyšky mydla a šampónu.",
            "Vyberte vlasy zo sitka a skontrolujte rohy, spodnú lištu a miesta pri silikóne.",
            "Na sklo a steny použite jemné čistenie podľa typu povrchu. Nezačínajte drsnou hubkou.",
            "Vodný kameň riešte cielene na miestach, kde povrch kyslý postup znesie.",
            "Škáry čistite malou kefkou a netlačte tak silno, aby ste ich mechanicky ničili.",
            "Všetko dôkladne opláchnite, aby na skle ani vaničke nezostal čistiaci film.",
            "Na záver použite stierku alebo suchú handričku a nechajte sprchový kút otvorený.",
        ],
        "check_rows": [
            ("Sklo", "šmuhy po zvyškoch produktu", "posledný krok je oplach a dosušenie"),
            ("Akrylátová vanička", "poškriabanie", "nepoužívať hrubú pastu bez testu"),
            ("Dlažba", "vodný kameň a škáry", "čistiť plochu aj spoje samostatne"),
            ("Prírodný kameň", "poškodenie kyselinou", "nepoužiť ocot bez odporúčania výrobcu"),
            ("Silikón", "vlhkosť a tmavé bodky", "riešiť príčinu vlhkosti, nielen povrch"),
        ],
        "mistakes": [
            "Použiť ocot alebo kyslý produkt na povrch, ktorý kyseliny neznáša.",
            "Drhnúť sklo alebo vaničku hrubou stranou hubky bez testu.",
            "Umyť sprchu, ale neopláchnuť zvyšky čistiaceho produktu.",
            "Nechať sklenenú zástenu mokrú po každom sprchovaní.",
            "Riešiť tmavé škáry iba parfumovaným produktom namiesto vlhkosti a čistenia.",
            "Zabudnúť na sitko, spodnú lištu a rohy, kde sa držia vlasy a kozmetika.",
        ],
        "sections": [
            ("Ako vyčistiť sklo v sprchovom kúte bez šmúh", [
                "Sklo v sprche nevyzerá zle iba pre vodný kameň. Často je na ňom najprv tenký mydlový film, ktorý sa pri čistení rozmazáva. Preto začnite uvoľnením filmu, nie leštením. Až po oplachu má zmysel riešiť finálne zotretie stierkou alebo suchou mikrovláknovou handričkou.",
                "Ak sklo po čistení stále robí mapy, skontrolujte dávkovanie produktu. Príliš veľa čistiaceho prostriedku môže zanechať film podobne ako pri podlahe. Súvisiaci princíp nájdete aj v článku <a href=\"/n/ako-umyt-podlahu-bez-smuh-laminat-vinyl-dlazba-a-mopovanie-v-praxi\">ako umyť podlahu bez šmúh</a>.",
            ]),
            ("Ako odstrániť vodný kameň zo sprchového kúta", [
                "Vodný kameň riešte tam, kde skutočne je: pri batérii, na skle, v okolí sprchovej hlavice a na miestach, kde kvapky zasychajú. <a href=\"/c/vevo-home-care/upratovanie/cistiace-prostriedky/biely-ocot\">Biely ocot</a> môže byť praktický pomocník, ale iba na povrchoch, ktoré kyslý postup znesú.",
                "Pri prírodnom kameni, mramore, špeciálnych povrchových úpravách alebo neznámej dlažbe najprv overte odporúčanie výrobcu. Kyslý produkt môže vodný kameň rozpustiť, ale zároveň poškodiť citlivý materiál.",
            ]),
            ("Ako čistiť škáry a rohy v sprche", [
                "Škáry čistite oddelene od skla. Potrebujú menšiu kefku, menej tlaku a viac trpezlivosti. Ak ich drhnete príliš silno, môžete povrch narušiť a špina sa bude zachytávať ešte ľahšie.",
                "Pri tmavých miestach najprv zistite, či ide o povrchový film alebo dlhodobú vlhkosť. Ak sa tmavnutie vracia po každom čistení, problém môže byť vo vetraní, zatekaní alebo trvale mokrom rohu.",
            ]),
            ("Ako udržať sprchový kút čistý dlhšie", [
                "Najlacnejšia prevencia je stierka po sprchovaní. Trvá pár sekúnd, ale znižuje množstvo kvapiek, ktoré zaschnú na skle. Ak sa pridá krátke vetranie a otvorené dvere sprchového kúta, vlhkosť sa nebude držať tak dlho.",
                "Raz za týždeň sa oplatí prejsť sklo, rohy a batériu skôr, než sa vytvorí tvrdá vrstva. Pri kúpeľňových textíliách pomôže aj návod <a href=\"/n/ako-prat-kupelnovu-predlozku-guma-vlhkost-chlpy-a-zapach-po-sprchovani\">ako prať kúpeľňovú predložku</a>, pretože vlhký textil dokáže pokaziť dojem z celej kúpeľne.",
            ]),
            ("Sprchový kút, vôňa a vlhkosť", [
                "Vôňa v kúpeľni má zmysel až vtedy, keď je odstránený zdroj zápachu. Ak zapácha vlhký silikón, uteráky alebo nevetraná kúpeľňa, interiérová vôňa problém len prekryje. Najprv riešte vodu, textil a vetranie.",
                "Ak chcete kúpeľňu po uprataní jemne prevoňať, robte to mierne a v dobre vetranom priestore. Pri bielizni v malej kúpeľni nadväzuje článok <a href=\"/n/ako-prevonat-bielizen-v-malej-kupelni-vlhkost-susenie-a-jemna-vona-bez-zatuchnutia\">ako prevoňať bielizeň v malej kúpeľni</a>.",
            ]),
        ],
        "rule": [
            "Sprchový kút čistite od najjemnejšieho postupu k silnejšiemu, nie opačne.",
            "Najlepší lesk vzniká po dobrom oplachu a dosušení, nie po väčšej dávke produktu.",
        ],
        "caution": [
            "Kyslé produkty nepoužívajte na prírodný kameň, mramor, citlivé kompozity alebo povrchy, pri ktorých si nie ste istí odporúčaním výrobcu. Aj pri vhodnom povrchu začnite krátkym pôsobením a testom na menej viditeľnom mieste.",
            "Ak sa v škárach alebo silikóne opakovane objavujú tmavé bodky, riešte aj vetranie a trvalú vlhkosť. Povrchové čistenie bez zmeny podmienok bude mať krátky efekt.",
        ],
        "expert": [
            "V sprche sa stretáva tvrdá voda, organické zvyšky z pokožky, povrchovo aktívne látky zo šampónov a dlhá vlhkosť. Preto má čistenie dve časti: rozpustiť alebo uvoľniť nános a potom ho fyzicky odstrániť z povrchu. Ak sa zvyšok len rozotrie a nechá zaschnúť, problém sa rýchlo vráti.",
            "Vlhkosť je dôležitý faktor vnútorného prostredia. Odborné zdroje k vlhkosti a plesniam opakovane zdôrazňujú kontrolu vody a sušenie povrchov. V domácnosti to znamená prakticky: stierka, vetranie, suché textílie a pravidelné čistenie rohov.",
        ],
        "product": {
            "heading": "Odporúčané riešenie na kúpeľňové usadeniny",
            "name": "Strong Pink čistiaca pasta 500g",
            "href": "/p-1562/strong-pink-cistiaca-pasta-500g",
            "intro": "Čistiaca pasta sa hodí pri odolnejších usadeninách na povrchoch, ktoré znášajú jemné mechanické čistenie. Pri citlivých sprchových kútoch vždy začnite testom.",
            "fit": "na vybrané odolnejšie povrchy, kde treba pomôcť mydlovému filmu alebo bežnej špine.",
            "boundary": "ak ide o akrylát, lesklý lakovaný povrch, prírodný kameň alebo neznámy materiál, najprv testujte a rešpektujte výrobcu.",
            "button": "Pozrieť čistiacu pastu",
        },
        "category": {
            "heading": "Vyberte čistenie podľa typu usadeniny",
            "href": "/c/vevo-home-care/upratovanie/cistiace-prostriedky/cistiaca-pasta",
            "button": "Pozrieť čistiace pasty",
            "intro": "Sprchový kút nemá jeden univerzálny problém. Inak sa rieši mydlový film, inak vodný kameň a inak dlhodobá vlhkosť v rohoch.",
            "bullets": [
                ("Mydlový film", "najprv uvoľniť a opláchnuť, až potom leštiť."),
                ("Vodný kameň", "riešiť cielene a iba na vhodnom povrchu."),
                ("Vlhkosť", "znížiť vodu, vetrať a sušiť po použití."),
            ],
        },
        "related": [
            ("Ako vyčistiť drez a batériu", "/n/ako-vycistit-drez-a-bateriu-vodny-kamen-mastnota-zapach-a-bezpecne-cistenie"),
            ("Ako prať kúpeľňovú predložku", "/n/ako-prat-kupelnovu-predlozku-guma-vlhkost-chlpy-a-zapach-po-sprchovani"),
            ("Ako prevoňať bielizeň v malej kúpeľni", "/n/ako-prevonat-bielizen-v-malej-kupelni-vlhkost-susenie-a-jemna-vona-bez-zatuchnutia"),
            ("Prečo bolí hlava z vône", "/n/preco-boli-hlava-z-vone-a-ako-pouzivat-vone-doma-jemnejsie"),
        ],
        "sources": [
            ("EPA: A brief guide to mold, moisture and your home", "https://www.epa.gov/mold/brief-guide-mold-moisture-and-your-home"),
            ("CDC: Cleaning and disinfecting your home", "https://www.cdc.gov/hygiene/cleaning-disinfecting/index.html"),
            ("EPA Safer Choice", "https://www.epa.gov/saferchoice"),
        ],
        "faq_title": "čistenie sprchového kúta",
        "faq": [
            ("Ako často čistiť sprchový kút?", "Krátke zotretie vody po sprche pomáha denne. Dôkladnejšie čistenie závisí od tvrdosti vody, vetrania a počtu ľudí v domácnosti, často raz týždenne alebo podľa nánosu."),
            ("Môžem použiť ocot na sklo v sprche?", "Na bežné sklo často áno, ale nie na prírodný kameň, citlivé povrchy alebo miesta, kde výrobca kyslé produkty neodporúča. Vždy opláchnite a dosušte."),
            ("Prečo je sprchový kút po čistení stále šmuhový?", "Najčastejšie ostal mydlový film, prebytok čistiaceho produktu alebo zaschnuté kvapky tvrdej vody. Pomôže lepší oplach, menšia dávka produktu a dosušenie."),
        ],
    },
    {
        "title": "Ako vyčistiť škáry v kúpeľni: pleseň, vodný kameň a zažltnuté miesta bez poškodenia",
        "short": "Škáry v kúpeľni čistite opatrne, pretože sú poréznejšie než dlažba a pri hrubom drhnutí sa môžu poškodiť. Rozlišujte povrchovú špinu, vodný kameň, zažltnutie a podozrenie na pleseň. Najdôležitejšia prevencia je menej stojacej vody a lepšie vetranie.",
        "answer": "Škáry v kúpeľni čistite malou kefkou, primeraným produktom a krátkym pôsobením. Najprv odstráňte povrchový film, potom riešte vodný kameň alebo zažltnutie podľa materiálu. Ak sa tmavé bodky vracajú, problém nemusí byť len v špine, ale aj v trvalej vlhkosti. Na vybrané odolné miesta môže pomôcť <a href=\"/p-1562/strong-pink-cistiaca-pasta-500g\">čistiaca pasta</a>, ale pri citlivých škárach vždy testujte.",
        "quick": [
            "<strong>Nezačínajte silou:</strong> tvrdé drhnutie môže škáru narušiť.",
            "<strong>Rozlíšte problém:</strong> sivý film, vodný kameň, žltý odtieň a tmavé bodky nemajú rovnakú príčinu.",
            "<strong>Vlhkosť je kľúčová:</strong> bez vetrania sa škáry budú špiniť rýchlejšie.",
            "<strong>Kyslé produkty opatrne:</strong> nie každá škára alebo dlažba ich znesie.",
            "<strong>Po čistení opláchnuť:</strong> zvyšky produktu môžu vytvoriť nový film.",
        ],
        "intro": [
            "Škáry v kúpeľni sú náročnejšie než hladká dlažba. Majú póry, zachytávajú vodu, mydlový film, prach a zvyšky kozmetiky. Preto môžu vyzerať sivé, žlté alebo tmavé aj v kúpeľni, ktorá sa pravidelne umýva. Ak sa čistia príliš agresívne, môžu sa poškodiť a potom chytajú špinu ešte rýchlejšie.",
            "Pri otázkach ako vyčistiť škáry v kúpeľni, ako odstrániť pleseň zo škár, ako vyčistiť zažltnuté škáry alebo ako vyčistiť vodný kameň zo škár je dôležité neskočiť hneď k najsilnejšiemu zásahu. Najprv zistite, či ide o povrchový nános, minerálnu mapu, zmenu farby alebo problém s vlhkosťou.",
            "Škáry v sprche, pri vani a pri umývadle majú rozdielne zaťaženie. V sprche sa stretáva teplá voda a kozmetika, pri vani sa voda drží na spodných hranách a pri umývadle zasychajú kvapky s mydlom. Preto je dobré čistiť podľa miesta, nie iba podľa farby škáry.",
            "Dlhodobý cieľ nie je vybieliť škáry za každú cenu. Cieľom je odstrániť nános bez poškodenia, znížiť vlhkosť a nastaviť rutinu, ktorú viete opakovať. Ak sú škáry staré, vydrolené alebo trvalo zafarbené, samotné čistenie nemusí vrátiť pôvodný stav.",
        ],
        "why_heading": "Prečo škáry v kúpeľni menia farbu",
        "why": [
            "Sivý alebo hnedastý tón často vzniká z prachu, mydla, kožného mazu a vlhkosti. Tento film sa drží v póroch škáry a pri rýchlom utretí hladkej dlažby zostane na mieste. Preto dlažba vyzerá čistá, ale škáry stále nie.",
            "Biele mapy a drsnejší povrch sú typické pri tvrdej vode. Minerály z vody sa usádzajú najmä tam, kde kvapky pravidelne zasychajú. Kyslý postup môže pomôcť, ale iba vtedy, keď ho materiál znesie a nepoužijete ho príliš dlho.",
            "Tmavé bodky sú signál na opatrnosť. Môžu byť povrchová špina, ale môžu súvisieť aj s dlhodobou vlhkosťou. Ak sa opakujú na rovnakom mieste, nestačí škáru vydrhnúť. Treba riešiť prúdenie vzduchu, dosušovanie a prípadné zatekanie.",
        ],
        "decision_heading": "Ako zvoliť postup podľa typu znečistenia",
        "decision_rows": [
            ("Sivý film", "zvyšky mydla, prachu a kozmetiky", "jemná kefka, oplach, opakovanie bez veľkého tlaku"),
            ("Biele mapy", "vodný kameň a minerály z vody", "mierny kyslý postup iba na vhodnom povrchu"),
            ("Zažltnutie", "starší nános, kozmetika alebo zmena materiálu", "najprv čistiť mierne, nečakať okamžité vybielenie"),
            ("Tmavé bodky", "povrchová špina alebo problém vlhkosti", "riešiť čistenie aj vetranie"),
            ("Vydrolená škára", "mechanické poškodenie alebo vek", "nepokračovať hrubým drhnutím, zvážiť opravu škár"),
        ],
        "steps": [
            "Škáry najprv opláchnite alebo utrite, aby ste odstránili voľný prach a povrchové zvyšky.",
            "Vyberte malú kefku s primerane jemnými štetinami. Tvrdý kovový nástroj nie je vhodný.",
            "Produkt naneste lokálne, nie na celú kúpeľňu naraz, aby ste vedeli sledovať reakciu povrchu.",
            "Nechajte pôsobiť krátko podľa odporúčania a škáru čistite smerom po línii, nie chaoticky do dlažby.",
            "Dôkladne opláchnite, aby v póroch nezostal produktový film.",
            "Miesto vysušte a nechajte kúpeľňu vetrať.",
            "Ak sa problém vracia, sledujte vlhkosť a stav silikónu alebo škárovania.",
        ],
        "check_rows": [
            ("Škáry v sprche", "dlhá vlhkosť a mydlový film", "po sprche vetrať a stierať vodu"),
            ("Škáry pri vani", "voda stojí na spodnej hrane", "kontrolovať rohy a silikón"),
            ("Škáry pri umývadle", "zubná pasta a mydlo", "čistiť často, kým nános nezatvrdne"),
            ("Podlaha v kúpeľni", "vlhkosť z uterákov a predložky", "textil sušiť mimo mokrej podlahy"),
            ("Staré škáry", "vydrolenie a trvalé sfarbenie", "nečistiť agresívne bez posúdenia stavu"),
        ],
        "mistakes": [
            "Použiť kovový alebo príliš tvrdý nástroj a mechanicky poškodiť škáru.",
            "Nechať kyslý produkt pôsobiť príliš dlho na citlivom materiáli.",
            "Opláchnuť iba dlažbu a nechať produkt v škárach.",
            "Riešiť tmavé miesta opakovane, ale nikdy nevetrať kúpeľňu.",
            "Očakávať, že stará poškodená škára bude po jednom čistení ako nová.",
            "Prekryť zatuchnutie vôňou bez odstránenia vlhkosti.",
        ],
        "sections": [
            ("Ako vyčistiť škáry v sprche", [
                "Škáry v sprche sú najviac vystavené teplej vode a kozmetike. Pri čistení postupujte po menších úsekoch. Najprv odstráňte film, potom sa rozhodnite, či treba riešiť vodný kameň alebo tmavnutie. Ak začnete silným tlakom, môžete škáru narušiť.",
                "Po čistení sprchu opláchnite a vysušte. Ak sa škáry v sprche špinia rýchlo, prepojte tento postup s návodom <a href=\"/n/ako-prat-kupelnovu-predlozku-guma-vlhkost-chlpy-a-zapach-po-sprchovani\">ako prať kúpeľňovú predložku</a>, pretože vlhký textil pred sprchou predlžuje vlhkosť v priestore.",
            ]),
            ("Ako riešiť pleseň alebo tmavé bodky v škárach", [
                "Pri tmavých bodkách nerozhodujte len podľa farby. Povrchová špina môže ísť dole jemným čistením, ale opakujúce sa tmavnutie v rohoch často znamená, že miesto zostáva príliš dlho mokré. Vtedy je čistenie iba polovica riešenia.",
                "Skontrolujte vetranie, sprchové dvere, silikón a to, či po sprchovaní zostáva voda v rohu. Ak je škára poškodená alebo mäkká, ďalšie drhnutie môže problém zhoršiť.",
            ]),
            ("Ako vyčistiť zažltnuté škáry", [
                "Zažltnutie môže byť starší nános, reakcia na kozmetiku, voda alebo zmena materiálu. Najprv skúste mierny postup a sledujte, či sa farba skutočne mení. Ak ide o staré sfarbenie v hĺbke škáry, čistenie nemusí obnoviť pôvodnú bielu farbu.",
                "Pri zažltnutých škárach je dôležité neprehnať abrazívne čistenie. Povrch môžete na chvíľu zosvetliť, ale ak ho narušíte, neskôr sa bude špiniť rýchlejšie.",
            ]),
            ("Vodný kameň v škárach a na dlažbe", [
                "Vodný kameň sa v kúpeľni často objaví na dlažbe aj v škárach. <a href=\"/p-1561/biely-ocot-v-spreji-500-ml\">Biely ocot v spreji</a> môže byť užitočný na vhodných povrchoch, ale pri prírodnom kameni alebo citlivých materiáloch ho nepoužívajte bez overenia.",
                "Ak čistíte vodný kameň, pracujte lokálne a krátko. Po rozpustení minerálnej vrstvy musí prísť oplach. Zvyšky kyslého produktu alebo uvoľnený nános nenechávajte v škáre zaschnúť.",
            ]),
            ("Ako predísť špinavým škáram", [
                "Prevencia je jednoduchá, ale musí byť pravidelná: voda nesmie stáť v rohoch, kúpeľňa musí vetrať a textil nemá ležať mokrý na podlahe. Stierka po sprchovaní chráni nielen sklo, ale aj spodné škáry.",
                "Pri kúpeľni, ktorá stále pôsobí zatuchnuto, nehľadajte riešenie iba v silnejšej vôni. Súvisiaca téma je <a href=\"/n/ako-prevonat-bielizen-v-malej-kupelni-vlhkost-susenie-a-jemna-vona-bez-zatuchnutia\">bielizeň v malej kúpeľni</a>, kde je kľúčová vlhkosť a sušenie.",
            ]),
            ("Prečo sa škáry zašpinia krátko po uprataní", [
                "Ak sa škáry rýchlo zašpinia už pár dní po čistení, často nejde o slabý produkt. V škárach mohol zostať zvyšok čistiaceho prostriedku, uvoľnený nános alebo voda, ktorá zaschla s minerálmi. Preto je posledný oplach a vysušenie dôležitejšie, než sa pri škárach zdá.",
                "Druhá príčina býva rutina po sprchovaní. Keď sa voda nechá stáť pri spodnej hrane a dvere sprchy ostanú zatvorené, škáry zostávajú vlhké dlhšie než dlažba. Povrch potom znovu zachytí prach, mydlový film a kozmetiku. Krátke zotretie spodných hrán má preto väčší efekt než občasné silné drhnutie.",
            ]),
        ],
        "rule": [
            "Škáry nečistite ako dlažbu: sú citlivejšie, poréznejšie a viac reagujú na vlhkosť.",
            "Ak sa tmavé miesta vracajú, hľadajte zdroj vody a slabé vetranie, nie iba silnejší produkt.",
        ],
        "caution": [
            "Pri prírodnom kameni, starých škárach alebo špeciálnych povrchoch sa vyhnite náhodnému miešaniu produktov a silným zásahom. Test na malej časti je bezpečnejší než plošné čistenie celej steny.",
            "Ak je škára vydrolená, mäkká alebo sa uvoľňuje, čistenie už nemusí byť správny postup. Vtedy je lepšie riešiť opravu alebo preškárovanie, aby sa vlhkosť nedostávala hlbšie.",
        ],
        "expert": [
            "Škárovacia hmota je spravidla pórovitejšia než glazovaná dlažba. Preto na nej dlhšie drží voda aj nános. Mechanické čistenie pomáha, ale iba v rozumnej miere. Ak sa povrch naruší, zväčší sa plocha, kde sa špina a vlhkosť môžu držať.",
            "Odborné odporúčania k vlhkosti v domácnosti zdôrazňujú odstránenie zdroja vody a sušenie povrchov. Pri kúpeľni to znamená, že čistenie škár má byť spojené s vetraním, stierkou po sprche a kontrolou miest, kde voda zostáva stáť.",
        ],
        "product": {
            "heading": "Odporúčané riešenie na odolnejší nános v kúpeľni",
            "name": "Strong Pink čistiaca pasta 500g",
            "href": "/p-1562/strong-pink-cistiaca-pasta-500g",
            "intro": "Čistiaca pasta vie pomôcť pri odolnejších nánosoch, keď povrch znáša jemné mechanické čistenie. Pri škárach je dôležité pracovať lokálne a netlačiť zbytočne silno.",
            "fit": "pri povrchovom nánose na odolnejších miestach, kde jemné čistenie nestačí.",
            "boundary": "pri starých, vydrolených alebo citlivých škárach najprv testujte a nepoužívajte hrubú silu.",
            "button": "Pozrieť čistiacu pastu",
        },
        "category": {
            "heading": "Kategória na kúpeľňové usadeniny a nánosy",
            "href": "/c/vevo-home-care/upratovanie/cistiace-prostriedky/cistiaca-pasta",
            "button": "Pozrieť čistiace pasty",
            "intro": "Pri škárach je dôležité vyberať podľa povrchu a typu nánosu, nie podľa toho, ako rýchlo chcete vidieť biely výsledok.",
            "bullets": [
                ("Odolný nános", "riešiť lokálne a s testom na malej časti."),
                ("Vodný kameň", "použiť vhodný postup iba tam, kde to materiál dovolí."),
                ("Vlhkosť", "riešiť vetranie a dosušovanie, inak sa problém vráti."),
            ],
        },
        "related": [
            ("Ako prať kúpeľňovú predložku", "/n/ako-prat-kupelnovu-predlozku-guma-vlhkost-chlpy-a-zapach-po-sprchovani"),
            ("Ako vyčistiť drez a batériu", "/n/ako-vycistit-drez-a-bateriu-vodny-kamen-mastnota-zapach-a-bezpecne-cistenie"),
            ("Ako vyčistiť kuchynskú linku", "/n/ako-vycistit-kuchynsku-linku-od-mastnoty-prachu-a-smuh-bez-poskodenia-povrchu"),
            ("Prečo moje oblečenie zapácha po praní", "/n/preco-moje-oblecenie-zapacha-po-prani-priciny-a-riesenia"),
        ],
        "sources": [
            ("EPA: A brief guide to mold, moisture and your home", "https://www.epa.gov/mold/brief-guide-mold-moisture-and-your-home"),
            ("CDC: Cleaning and disinfecting your home", "https://www.cdc.gov/hygiene/cleaning-disinfecting/index.html"),
            ("EPA Safer Choice", "https://www.epa.gov/saferchoice"),
        ],
        "faq_title": "čistenie škár v kúpeľni",
        "faq": [
            ("Ako vyčistiť škáry bez poškodenia?", "Použite malú kefku, primeraný produkt a krátke pôsobenie. Netlačte kovovým alebo príliš tvrdým nástrojom a po čistení dôkladne opláchnite."),
            ("Čo ak sú škáry po čistení stále žlté?", "Môže ísť o staré sfarbenie alebo zmenu materiálu. Opakujte mierny postup, ale nepreháňajte drhnutie. Pri poškodených škárach môže byť potrebná oprava."),
            ("Ako zabrániť plesni v škárach?", "Znížte vlhkosť: vetrajte, stierajte vodu po sprche, nenechávajte mokrý textil na podlahe a kontrolujte rohy, kde voda stojí."),
        ],
    },
    {
        "title": "Ako vyčistiť sprchovú hlavicu: vodný kameň, slabý prúd a hygienická údržba",
        "short": "Sprchová hlavica sa zanáša najmä vodným kameňom a zvyškami z vody. Ak prúd slabne alebo strieka do strán, najprv skontrolujte otvory, sitko a možnosť demontáže. Čistenie robte podľa materiálu a nepoužívajte ocot na povrchy, ktoré kyslý postup neznášajú.",
        "answer": "Sprchovú hlavicu vyčistíte tak, že ju najprv odpojíte alebo aspoň skontrolujete otvory, odstránite povrchový vodný kameň, podľa materiálu krátko namočíte vhodné časti a potom ich dôkladne opláchnete. Pri bežnom vodnom kameni môže pomôcť <a href=\"/p-1561/biely-ocot-v-spreji-500-ml\">biely ocot v spreji</a>, ale vždy rešpektujte materiál hlavice, chróm, tesnenia a odporúčanie výrobcu.",
        "quick": [
            "<strong>Slabý prúd:</strong> často znamená zanesené otvory alebo sitko.",
            "<strong>Striekanie do strán:</strong> jednotlivé dýzy môžu byť čiastočne upchaté.",
            "<strong>Odnímateľná hlavica:</strong> čistí sa ľahšie, ale dávajte pozor na tesnenia.",
            "<strong>Pevná hlavica:</strong> čistite lokálne a neprelievajte spoje zbytočne dlho.",
            "<strong>Po čistení:</strong> dôkladný oplach je rovnako dôležitý ako samotné pôsobenie.",
        ],
        "intro": [
            "Sprchová hlavica je malá časť kúpeľne, ale výrazne ovplyvňuje komfort sprchovania. Keď sa zanáša, prúd vody slabne, voda strieka do strán alebo sa jednotlivé otvory úplne upchajú. Príčinou býva najmä vodný kameň, minerály z vody a nános v otvoroch alebo sitku.",
            "Pri čistení sprchovej hlavice treba myslieť na tri veci: materiál, tesnenia a spôsob pripevnenia. Inak sa čistí odnímateľná ručná sprcha, inak pevná dažďová hlavica a inak hlavica s citlivejšou povrchovou úpravou. Príliš dlhé namáčanie alebo nevhodný kyslý postup môže poškodiť povrch.",
            "Sprchová hlavica tiež súvisí s celým sprchovým kútom. Ak je na nej vodný kameň, pravdepodobne sa rovnaká vrstva tvorí aj na skle, batérii a v rohoch. Preto je dobré spojiť jej údržbu s čistením skla a batérie, nie čakať, kým prúd citeľne zoslabne.",
            "Dobrý domáci postup je mierny, pravidelný a kontrolovaný. Najprv skontrolujte, či ide o povrchové usadeniny alebo upchaté otvory, potom použite vhodný produkt, opláchnite a nechajte vodu chvíľu pretiecť. Cieľom nie je len vzhľad, ale aj normálny prietok vody.",
        ],
        "why_heading": "Prečo sprchová hlavica slabne alebo strieka do strán",
        "why": [
            "Otvory v hlavici sú malé a voda cez ne prechádza denne. Minerály z tvrdej vody sa usádzajú na okrajoch otvorov a postupne ich zužujú. Keď sa niektoré otvory upchajú viac než iné, prúd začne striekať nerovnomerne alebo do strán.",
            "Druhým miestom je sitko alebo spoj medzi hadicou a hlavicou. Ak sa tam zachytí drobný nános, prietok klesne aj vtedy, keď otvory na povrchu vyzerajú pomerne čisté. Preto pri slabom prúde nestačí pozrieť iba prednú stranu hlavice.",
            "Nános sa zrýchľuje v kúpeľniach s tvrdou vodou a slabším vetraním. Kvapky na hlavici zasychajú a zanechávajú minerálnu vrstvu. Pravidelné pretrenie a občasné čistenie je jednoduchšie než riešiť úplne upchaté dýzy.",
        ],
        "decision_heading": "Ako zvoliť postup podľa typu hlavice",
        "decision_rows": [
            ("Ručná sprchová hlavica", "dá sa odskrutkovať a vyčistiť samostatne", "odpojiť opatrne, skontrolovať tesnenie a sitko"),
            ("Pevná dažďová hlavica", "ťažšie sa namáča a môže mať citlivé spoje", "čistiť lokálne, bez zbytočného prelievania spojov"),
            ("Chrómovaný povrch", "riziko matných máp po agresívnom postupe", "krátke pôsobenie, oplach a dosušenie"),
            ("Silikónové dýzy", "vodný kameň v otvoroch", "jemne premasírovať podľa odporúčania výrobcu a opláchnuť"),
            ("Neznámy materiál", "neistota pri kyslom produkte", "najprv test a návod výrobcu"),
        ],
        "steps": [
            "Skontrolujte, či je hlavica odnímateľná a či viete bezpečne odskrutkovať hadicu bez poškodenia tesnenia.",
            "Pozrite sa na otvory a sitko. Ak sú biele alebo nerovnomerne zanesené, problém je pravdepodobne vodný kameň.",
            "Povrch hlavice najprv opláchnite a odstráňte voľnú špinu.",
            "Vhodné časti ošetrite miernym postupom proti vodnému kameňu, pričom rešpektujte materiál a čas pôsobenia.",
            "Otvory čistite jemne. Nepoužívajte tvrdý kovový predmet, ktorý môže poškodiť dýzy.",
            "Hlavicu dôkladne opláchnite, vráťte tesnenie a nechajte vodu chvíľu pretiecť.",
            "Po čistení osušte povrch, aby nové kvapky nevytvorili čerstvé mapy.",
        ],
        "check_rows": [
            ("Otvory hlavice", "upchatie vodným kameňom", "čistiť jemne a pravidelne"),
            ("Sitko", "nános z vody a znížený prietok", "skontrolovať pri odskrutkovaní"),
            ("Tesnenie", "zlé dosadnutie po čistení", "nevyhodiť a vrátiť správne"),
            ("Chróm", "matné fľaky po dlhom pôsobení", "krátko pôsobiť, opláchnuť a dosušiť"),
            ("Hadica", "zvyšky vody a povrchový kameň", "pretrieť a skontrolovať spoje"),
        ],
        "mistakes": [
            "Namáčať celú hlavicu príliš dlho bez ohľadu na materiál a tesnenia.",
            "Čistiť otvory ihlou alebo kovovým predmetom tak, že sa dýzy poškodia.",
            "Zabudnúť na sitko medzi hadicou a hlavicou.",
            "Použiť ocot na povrch, ktorý kyslé produkty neznáša.",
            "Po čistení nechať zvyšky produktu v otvoroch bez prepláchnutia.",
            "Riešiť hlavicu až vtedy, keď prúd výrazne zoslabne.",
        ],
        "sections": [
            ("Ako vyčistiť sprchovú hlavicu octom", [
                "Biely ocot môže pomôcť pri bežnom vodnom kameni, ale nepoužívajte ho automaticky na každý povrch. Ak je hlavica chrómovaná, farebná, s neznámou úpravou alebo má citlivé prvky, začnite krátkym pôsobením a testom.",
                "Pri odnímateľnej hlavici je jednoduchšie ošetriť iba tú časť, ktorá to potrebuje. Pri pevnej hlavici postupujte opatrne, aby sa roztok nedostal zbytočne do spojov alebo na materiály, ktoré nie sú určené na namáčanie.",
            ]),
            ("Ako vyčistiť zanesené otvory v sprchovej hlavici", [
                "Zanesené otvory neprepichujte hrubo. Ak majú silikónové dýzy, často pomôže jemné premasírovanie po uvoľnení vodného kameňa. Pri tvrdých otvoroch použite mäkkú kefku a trpezlivosť.",
                "Ak po vyčistení stále niektoré otvory nestriekajú správne, skontrolujte sitko a spoj. Problém nemusí byť iba v prednej strane hlavice.",
            ]),
            ("Ako vyčistiť dažďovú sprchu alebo pevnú hlavicu", [
                "Dažďová hlavica býva väčšia a často pevne namontovaná. Vyhnite sa dlhému prelievaniu spojov a nepracujte tak, aby roztok stekal po citlivých častiach. Lepšie je lokálne ošetrenie a následné prepláchnutie.",
                "Pri veľkej hlavici sa oplatí čistiť pravidelne menšie nánosy. Ak čakáte príliš dlho, niektoré otvory sa upchajú nerovnomerne a sprcha stráca komfort.",
            ]),
            ("Slabý prúd vody po čistení", [
                "Ak je prúd stále slabý, skontrolujte tesnenie, sitko a hadicu. Niekedy sa po manipulácii uvoľní nános a zachytí sa na inom mieste. Hlavicu znovu opláchnite a nechajte vodu chvíľu tiecť.",
                "Ak slabý prúd nesúvisí iba so sprchou, môže ísť o širší problém prívodu vody alebo batérie. Vtedy samotné čistenie hlavice nepomôže úplne.",
            ]),
            ("Ako často čistiť sprchovú hlavicu", [
                "Frekvencia závisí od tvrdosti vody. V domácnosti s tvrdou vodou sa oplatí hlavicu kontrolovať častejšie, najmä ak vidíte biele okraje alebo cítite zmenu prúdu. Pri mäkšej vode stačí dlhší interval.",
                "Sprchovú hlavicu môžete zaradiť do rutiny so sklom a batériou. Súvisiaci článok <a href=\"/n/ako-vycistit-drez-a-bateriu-vodny-kamen-mastnota-zapach-a-bezpecne-cistenie\">ako vyčistiť drez a batériu</a> rieši podobný princíp vodného kameňa na armatúrach.",
            ]),
            ("Sprchová hlavica po čistení: čo skontrolovať", [
                "Po vyčistení skontrolujte, či hlavica netečie pri závite, či tesnenie sedí správne a či prúd vody vychádza rovnomerne. Ak voda strieka do strán len z jedného miesta, môže byť v dýze ešte uvoľnený kúsok nánosu. Vtedy pomôže krátke prepláchnutie a jemné premasírovanie otvoru, nie prudké prepichovanie.",
                "Sledujte aj povrch po zaschnutí. Ak sa na chróme objaví matný kruh alebo mapa, pravdepodobne zostal zvyšok prípravku alebo voda s minerálmi. Pri ďalšom čistení skráťte pôsobenie, dôkladnejšie opláchnite a dosušte hlavicu mäkkou handričkou. Táto kontrola chráni funkciu aj vzhľad.",
            ]),
        ],
        "rule": [
            "Pri sprchovej hlavici čistite najprv príčinu slabého prúdu: otvory, sitko, spoj a vodný kameň.",
            "Kyslý postup používajte len tam, kde ho materiál znesie, a po čistení vždy dôkladne prepláchnite.",
        ],
        "caution": [
            "Ak má hlavica špeciálnu povrchovú úpravu, farebný kov, prírodný detail alebo neznáme tesnenia, neponárajte ju naslepo do octu. Krátky test je bezpečnejší než dlhé namáčanie.",
            "Pri opakovanom slabom prúde, ktorý sa nelepší ani po vyčistení sitka a otvorov, môže byť problém v batérii, hadici alebo tlaku vody. Vtedy je správne zastaviť domáce pokusy a skontrolovať technickú príčinu.",
        ],
        "expert": [
            "Vodný kameň je minerálny nános z vody. V malých otvoroch sprchovej hlavice sa prejaví rýchlo, pretože aj tenká vrstva zmenší prietok alebo smer prúdu. Mechanické odstránenie bez poškodenia dýz a dobrý oplach sú rovnako dôležité ako samotný čistiaci produkt.",
            "Pri kúpeľňových armatúrach treba myslieť na materiály v kontakte s vodou: kovový povrch, plastové diely, silikónové dýzy a gumové tesnenia. Univerzálne dlhé namáčanie nemusí byť šetrné. Pravidelná mierna údržba je bezpečnejšia než nárazový agresívny zásah.",
        ],
        "product": {
            "heading": "Odporúčané riešenie na bežný vodný kameň",
            "name": "Biely ocot v spreji 500 ml",
            "href": "/p-1561/biely-ocot-v-spreji-500-ml",
            "intro": "Biely ocot v spreji je praktický pri bežnom vodnom kameni na povrchoch, ktoré kyslý postup znesú. Pri sprchovej hlavici ho používajte cielene a s dôkladným oplachom.",
            "fit": "na bežné minerálne mapy a nános v okolí otvorov, ak je materiál vhodný.",
            "boundary": "pri neznámom povrchu, citlivých úpravách a tesneniach začnite testom a krátkym pôsobením.",
            "button": "Pozrieť biely ocot v spreji",
        },
        "category": {
            "heading": "Kategória na vodný kameň v kúpeľni",
            "href": "/c/vevo-home-care/upratovanie/cistiace-prostriedky/biely-ocot",
            "button": "Pozrieť biely ocot",
            "intro": "Pri vodnom kameni pomáha pravidelnosť: menší nános sa čistí jednoduchšie než úplne upchaté otvory a hrubá vrstva na batérii.",
            "bullets": [
                ("Otvory hlavice", "čistiť jemne, aby sa nepoškodili dýzy."),
                ("Batéria", "po čistení opláchnuť a dosušiť."),
                ("Sklo v sprche", "vodný kameň riešiť spolu s mydlovým filmom."),
            ],
        },
        "related": [
            ("Ako vyčistiť drez a batériu", "/n/ako-vycistit-drez-a-bateriu-vodny-kamen-mastnota-zapach-a-bezpecne-cistenie"),
            ("Ako umyť podlahu bez šmúh", "/n/ako-umyt-podlahu-bez-smuh-laminat-vinyl-dlazba-a-mopovanie-v-praxi"),
            ("Ako prať župan, aby zostal mäkký a nezatuchol", "/n/ako-prat-zupan-aby-zostal-makky-savy-a-nezatuchol-po-sprche"),
            ("Ako prať uteráky", "/n/ako-prat-uteraky-rady-a-tipy-na-ciste-a-maekke-uteraky"),
        ],
        "sources": [
            ("CDC: Cleaning and disinfecting your home", "https://www.cdc.gov/hygiene/cleaning-disinfecting/index.html"),
            ("EPA Safer Choice", "https://www.epa.gov/saferchoice"),
            ("EPA: A brief guide to mold, moisture and your home", "https://www.epa.gov/mold/brief-guide-mold-moisture-and-your-home"),
        ],
        "faq_title": "čistenie sprchovej hlavice",
        "faq": [
            ("Ako dlho nechať pôsobiť ocot na sprchovej hlavici?", "Len primerane a podľa materiálu. Pri citlivom povrchu začnite krátko, opláchnite a skontrolujte výsledok. Dlhé namáčanie naslepo nie je bezpečný univerzálny postup."),
            ("Prečo sprchová hlavica strieka do strán?", "Niektoré otvory sú pravdepodobne čiastočne upchaté vodným kameňom. Skontrolujte aj sitko a spoj s hadicou."),
            ("Môžem hlavicu čistiť ihlou?", "Radšej nie hrubo. Tvrdý kovový predmet môže poškodiť dýzy. Skúste jemné uvoľnenie nánosu, mäkkú kefku a dôkladný oplach."),
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
        seen = set()
        for href in href_re.findall(article["long"]):
            if href.startswith("#") or href.startswith("mailto:") or href.startswith("tel:"):
                continue
            url = urljoin(BASE, href)
            if url in seen:
                continue
            seen.add(url)
            try:
                response = session.get(
                    url,
                    timeout=30,
                    allow_redirects=True,
                    headers={"User-Agent": "Mozilla/5.0"},
                )
                status = response.status_code
                ok = status < 400
                final_url = response.url
                error = None
            except Exception as exc:  # pragma: no cover
                status = None
                ok = False
                final_url = None
                error = str(exc)
            records.append(
                {
                    "title": article["title"],
                    "url": url,
                    "status": status,
                    "ok": ok,
                    "final_url": final_url,
                    "error": error,
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
    OUT_JSON.write_text(json.dumps(articles, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
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
