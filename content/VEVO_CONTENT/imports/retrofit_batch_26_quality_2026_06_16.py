import json
import re
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from urllib.parse import urljoin

import requests


BASE = "https://www.vevo.sk"
IN_JSON = Path("content/VEVO_CONTENT/imports/batch-26-2026-06-16-articles.json")
MAPPING_JSON = Path("content/VEVO_CONTENT/exports/batch-26-2026-06-16-mapping.json")
OUT_UPDATE = Path("content/VEVO_CONTENT/imports/batch-26-2026-06-16-quality-update.json")


QUALITY = {
    "ako-funguje-praci-gel-tenzidy-enzymy-ph-a-davkovanie-pri-beznom-prani": {
        "queries": [
            "ako dávkovať prací gél podľa tvrdosti vody",
            "koľko pracieho gélu do práčky pri 4 kg bielizne",
            "koľko pracieho gélu do práčky pri 7 kg bielizne",
            "prečo viac pracieho gélu neperie lepšie",
            "čo robia tenzidy v pracom géli",
            "čo robia enzýmy v pracom prostriedku",
            "pH pracieho gélu a citlivá pokožka",
            "prací gél a krátky program",
            "zvyšky pracieho gélu v oblečení",
            "prací gél pri uterákoch a posteľnej bielizni",
        ],
        "expert_title": "Detailnejší pohľad: dávka, voda a zvyšky prostriedku",
        "expert_paragraphs": [
            "Pri pracom géli je dôležité rozlišovať medzi čistiacou schopnosťou a množstvom zvyškov, ktoré zostanú po oplachu. Ak dáte príliš málo gélu, časť mastnoty a potu sa nemusí uvoľniť. Ak dáte príliš veľa gélu, prací roztok sa môže horšie vypláchnuť, najmä pri krátkom programe, tvrdej vode alebo preplnenom bubne.",
            "Praktické pravidlo je jednoduché: dávku nezvyšujte podľa toho, ako silno chcete bielizeň cítiť. Dávku upravujte podľa špiny, tvrdosti vody a veľkosti náplne. Vôňa patrí až na koniec dobre zvládnutého prania, nie ako náhrada za oplach.",
        ],
        "diagnostic_rows": [
            ("Bielizeň lepí alebo je príliš voňavá", "pravdepodobne veľa gélu alebo slabý oplach", "znížiť dávku, pridať oplach, neprať nadoraz"),
            ("Športové tričko stále cítiť potom", "pot a maz zostali vo vlákne", "menšia náplň, dlhší program, neprekryť vôňou"),
            ("Uteráky sú tvrdé", "zvyšky prostriedku, tvrdá voda alebo avivážový film", "menej gélu, viac oplachu, riešiť vodu"),
        ],
        "sales": {
            "heading": "Riešenie podľa dávkovania a typu prania",
            "intro": "Ak riešite bežné pranie, začnite správnou dávkou a dostatočným oplachom. Produkt má podporiť čistotu, nie prekryť preplnený bubon alebo zle vypláchnutý gél.",
            "product_name": "Prací gél hypoalergénny z Marseillského mydla 1L",
            "product_href": "/p-1551/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l",
            "fit": "pri bežnom praní tričiek, spodnej bielizne, posteľnej bielizne a domácich textílií, kde chcete jemný prací základ a jasné dávkovanie.",
            "boundary": "ak bielizeň po praní lepí, zapácha alebo ostáva tvrdá, najprv upravte dávku, náplň a oplach. Samotná zmena produktu nevyrieši preplnenú alebo zanesenú práčku.",
            "product_button": "Pozrieť prací gél",
            "category_title": "Vyberte prací prostriedok podľa situácie",
            "category_intro": "Pri výbere pracieho prostriedku porovnávajte nielen vôňu, ale aj typ bielizne, citlivosť pokožky, tvrdosť vody a to, či periete rýchly alebo plný program.",
            "category_bullets": [
                ("Bežné pranie", "pracujte s dávkou podľa náplne a tvrdosti vody."),
                ("Citlivá pokožka", "uprednostnite jemnejší prací základ a dôkladný oplach."),
                ("Vôňa", "pridávajte až po vyriešení čistoty, nie ako maskovanie zvyškov potu."),
            ],
            "category_href": "/c/vevo-home-care/pranie/praci-gel",
            "category_button": "Porovnať pracie gély",
        },
    },
    "predpieranie-v-pracke-kedy-ma-zmysel-a-kedy-len-mina-vodu-cas-a-praci-prostriedok": {
        "queries": [
            "kedy použiť predpieranie v práčke",
            "predpieranie detského oblečenia od blata",
            "predpieranie pracovného oblečenia",
            "predpieranie kuchynských utierok",
            "predpieranie posteľnej bielizne po chorobe",
            "predpierka alebo predčistenie škvrny",
            "koľko pracieho prostriedku do predpierky",
            "predpieranie a spotreba vody",
            "predpieranie pri jemnej bielizni",
            "čo robiť po praní veľmi špinavých vecí",
        ],
        "expert_title": "Detailnejší pohľad: predpieranie nie je univerzálne predčistenie",
        "expert_paragraphs": [
            "Predpieranie pomáha hlavne pri objemovej špine: blato, prach, piesok, chlpy alebo zvyšky z textílií, ktoré by v hlavnom praní zbytočne zaťažili prací kúpeľ. Pri lokálnej škvrne je často lepšie cielene ošetriť konkrétne miesto a až potom spustiť hlavný program.",
            "Ak predpierate veľmi špinavé textílie pravidelne, sledujte aj práčku. Zvyšky blata, vlasov, tukov a pracieho prostriedku sa môžu ukladať v zásobníku, tesnení a filtri. Vtedy už problém nie je len v programe, ale aj v hygiene spotrebiča.",
        ],
        "diagnostic_rows": [
            ("Blato na detských nohaviciach", "najprv vysušiť alebo striasť hrubú špinu", "predpieranie až po mechanickom odstránení blata"),
            ("Mastná škvrna na utierke", "lokálna škvrna", "predčistiť miesto, nepoužívať predpierku ako jediný krok"),
            ("Práčka po praní pracovných vecí zapácha", "špina ostala v spotrebiči", "vyčistiť zásobník, filter alebo pustiť čistiaci cyklus"),
        ],
        "sales": {
            "heading": "Riešenie po silne špinavých dávkach",
            "intro": "Predpieranie môže pomôcť bielizni, ale po opakovanom praní blata, pracovných vecí alebo pelechov treba myslieť aj na samotnú práčku.",
            "product_name": "Vevo Shot - koncentrát na čistenie práčky 100ml",
            "product_href": "/p-1549/vevo-shot-koncentrat-na-cistenie-pracky",
            "fit": "keď sa po špinavých dávkach vracia zápach z bubna, zásobníka alebo tesnenia a chcete vyčistiť práčku bez oblečenia.",
            "boundary": "nepoužívajte ho ako náhradu predčistenia textilu. Blato, chlpy a hrubú špinu odstráňte ešte pred vložením do práčky.",
            "product_button": "Pozrieť čistič práčky",
            "category_title": "Kedy riešiť bielizeň a kedy práčku",
            "category_intro": "Ak sa problém opakuje aj po správnom predpierke, pravdepodobne už nejde iba o program, ale aj o usadeniny v práčke.",
            "category_bullets": [
                ("Jednorazová špina", "pomôže vytrasenie, predčistenie a vhodný program."),
                ("Opakovaný zápach", "skontrolujte zásobník, tesnenie, filter a bubon."),
                ("Pravidelné pracovné pranie", "zaveďte samostatnú hygienu práčky."),
            ],
            "category_href": "/c/vevo-home-care/pranie/detox-pracky",
            "category_button": "Pozrieť detox práčky",
        },
    },
    "otacky-pri-odstredovani-ako-ovplyvnuju-vlhkost-krcenie-a-opotrebovanie-oblecenia": {
        "queries": [
            "koľko otáčok nastaviť pri praní",
            "800 alebo 1200 otáčok pri odstreďovaní",
            "otáčky pri uterákoch",
            "otáčky pri obliečkach",
            "otáčky pri viskóze a jemnej bielizni",
            "otáčky pri športovom oblečení",
            "odstreďovanie a krčenie oblečenia",
            "odstreďovanie a opotrebovanie vlákien",
            "ako skrátiť sušenie bielizne",
            "kedy nepoužiť vysoké otáčky",
        ],
        "expert_title": "Detailnejší pohľad: vlhkosť po praní nie je jediný cieľ",
        "expert_paragraphs": [
            "Vyššie otáčky znižujú množstvo vody v textílii, ale zároveň zvyšujú mechanické namáhanie. To je výhodné pri uterákoch a pevnej bavlne, no rizikové pri jemných vláknach, elastane, viskóze, vlnenej zmesi alebo oblečení s potlačou.",
            "Ak sušíte v sušičke, otáčky riešte spolu s náplňou sušičky. Bielizeň môže byť menej mokrá, ale ak sa v sušičke zlepí do veľkých kusov, sušenie aj tak nebude rovnomerné. Pri objemných kusoch často pomôže lepšie oddelenie textilu počas sušenia.",
        ],
        "diagnostic_rows": [
            ("Bielizeň schne príliš dlho", "nízke otáčky alebo preplnený bubon", "zvýšiť otáčky len pri materiáli, ktorý to znesie"),
            ("Blúzka je pokrčená a zdeformovaná", "otáčky boli príliš vysoké", "voliť jemnejší program a nižšie otáčky"),
            ("Obliečky sa zlepia do klbka", "veľké kusy sa zle rozložili", "prať a sušiť s väčším priestorom"),
        ],
        "sales": {
            "heading": "Riešenie pre rovnomernejšie sušenie po odstreďovaní",
            "intro": "Otáčky nastavujte podľa materiálu. Ak následne sušíte v sušičke, dôležité je aj to, aby sa textil v bubne oddelil a vzduch mohol cirkulovať.",
            "product_name": "Prírodné vlnené gule do sušičky 3 ks",
            "product_href": "/p-1612/prirodne-vlnene-gule-do-susicky-3-ks",
            "fit": "pri uterákoch, posteľnej bielizni a väčších dávkach v sušičke, kde chcete podporiť oddelenie textilu a rovnomernejšie sušenie.",
            "boundary": "nevkladajte do sušičky textil, ktorý tam podľa štítku nepatrí. Gule neriešia nesprávne pranie ani príliš vysoké otáčky pri jemných materiáloch.",
            "product_button": "Pozrieť vlnené gule",
            "category_title": "Vyberte pomôcky podľa spôsobu sušenia",
            "category_intro": "Pri sušení nerozhoduje iba to, koľko vody práčka vytiahla. Rozhoduje aj objem dávky, cirkulácia vzduchu a to, či materiál znesie sušičku.",
            "category_bullets": [
                ("Sušička", "pomôcky vyberajte podľa typu textilu a veľkosti dávky."),
                ("Sušenie v byte", "riešte hlavne otáčky, vetranie a rozloženie bielizne."),
                ("Jemné materiály", "radšej nižšie otáčky a prirodzené sušenie podľa štítku."),
            ],
            "category_href": "/c/vevo-home-care/pranie/gule-do-susicky",
            "category_button": "Pozrieť gule do sušičky",
        },
    },
    "preplnena-pracka-preco-sa-bielizen-nevyperie-neoplachne-a-zapacha": {
        "queries": [
            "koľko bielizne dať do práčky",
            "ako zistiť preplnenú práčku",
            "preplnená práčka a zápach bielizne",
            "preplnená práčka a zvyšky gélu",
            "koľko uterákov do práčky",
            "koľko obliečok do práčky",
            "prečo sa bielizeň v práčke nevyperie",
            "prečo sa práčka zle oplachuje",
            "preplnený bubon a pokrčené oblečenie",
            "čo robiť po opakovane preplnenej práčke",
        ],
        "expert_title": "Detailnejší pohľad: kapacita v kilogramoch nie je vždy praktická kapacita",
        "expert_paragraphs": [
            "Menovitá kapacita práčky vyzerá jednoducho, ale v praxi závisí od programu a typu textilu. Uteráky, mikiny a obliečky zaberú viac priestoru a držia viac vody než tenké tričká. Preto plný bubon pri jednom programe nemusí znamenať vhodnú náplň pri inom programe.",
            "Preplnenie zhoršuje mechaniku prania aj oplach. Prací roztok sa nedostane rovnomerne medzi vrstvy textilu, zvyšky potu a gélu ostávajú v záhyboch a mokrá zhutnená dávka potom schne pomalšie. Zápach sa tak môže tváriť ako problém vône, hoci vznikol už pri náplni.",
        ],
        "diagnostic_rows": [
            ("Bielizeň je po praní ťažká a zle rozmotaná", "bubon bol pravdepodobne preplnený", "prať menšiu dávku a oddeliť veľké kusy"),
            ("Na textílii ostali mapy alebo zvyšky gélu", "slabý pohyb a oplach", "znížiť náplň, nezdvojnásobovať gél"),
            ("Zápach sa vracia po vysušení", "pot alebo prostriedok ostal vo vláknach", "menšia dávka, dlhší program, kontrola práčky"),
        ],
        "sales": {
            "heading": "Riešenie, keď sa zápach vracia aj po úprave náplne",
            "intro": "Prvým riešením preplnenej práčky je menšia dávka, nie silnejšia vôňa. Ak sa však zápach drží v bubne alebo tesnení, treba vyčistiť aj spotrebič.",
            "product_name": "Vevo Shot - koncentrát na čistenie práčky 100ml",
            "product_href": "/p-1549/vevo-shot-koncentrat-na-cistenie-pracky",
            "fit": "keď sa po opakovane preplnených alebo vlhkých dávkach drží zápach v práčke a čistá bielizeň ho znovu preberá.",
            "boundary": "nepomôže, ak budete práčku ďalej plniť nadoraz. Najprv upravte veľkosť náplne, dávkovanie a sušenie.",
            "product_button": "Pozrieť čistič práčky",
            "category_title": "Zvoľte riešenie podľa zdroja zápachu",
            "category_intro": "Ak zapácha len jedna dávka, riešte náplň a oplach. Ak zapácha práčka, riešte hygienu bubna, tesnenia a zásobníka.",
            "category_bullets": [
                ("Preplnená dávka", "rozdeliť bielizeň a znížiť dávku gélu."),
                ("Zvyšky v práčke", "vyčistiť zásobník, filter, tesnenie a bubon."),
                ("Pomalé sušenie", "vyberať bielizeň hneď po praní a zlepšiť cirkuláciu vzduchu."),
            ],
            "category_href": "/c/vevo-home-care/pranie/detox-pracky",
            "category_button": "Pozrieť detox práčky",
        },
    },
    "preco-je-bielizen-po-prani-tvrda-alebo-lepkava-zvysky-gelu-davkovanie-a-oplach": {
        "queries": [
            "prečo je bielizeň po praní tvrdá",
            "prečo je bielizeň po praní lepkavá",
            "ako odstrániť zvyšky pracieho gélu z oblečenia",
            "tvrdá voda a tvrdé uteráky",
            "lepkavé tričko po praní",
            "zle opláchnutá bielizeň",
            "príliš veľa pracieho gélu v práčke",
            "tvrdá bielizeň bez aviváže",
            "octová aviváž pri tvrdej vode",
            "ako zmäkčiť uteráky bez ťažkého filmu",
        ],
        "expert_title": "Detailnejší pohľad: tvrdosť, film na vláknach a citlivý dotyk",
        "expert_paragraphs": [
            "Tvrdosť bielizne môže mať viac príčin naraz. Minerály z tvrdej vody menia pocit z textilu, zvyšky pracieho prostriedku vytvárajú film a klasická aviváž môže pri uterákoch znížiť savosť. Preto sa problém neoplatí riešiť jedným univerzálnym pridaním produktu.",
            "Ak je textil lepkavý, začnite opätovným oplachom bez ďalšieho gélu. Ak je skôr tvrdý a drsný, sledujte tvrdosť vody, dávkovanie a to, či nepoužívate príliš veľa aviváže. Pri uterákoch je cieľ svieži a savý textil, nie ťažký povlak.",
        ],
        "diagnostic_rows": [
            ("Lepkavý povrch", "zvyšky gélu alebo parfumácie", "opätovný oplach, menej prostriedku"),
            ("Drsné uteráky", "tvrdá voda alebo usadeniny vo vláknach", "upraviť dávkovanie, nepreplniť, zvážiť octovú aviváž"),
            ("Silná vôňa a škrabanie", "veľa produktu ostalo v textile", "znížiť parfumáciu a pridať oplach"),
        ],
        "sales": {
            "heading": "Riešenie pre tvrdšiu bielizeň bez ťažkého filmu",
            "intro": "Ak je bielizeň lepkavá, najprv odstráňte zvyšky pracieho gélu. Ak je tvrdá najmä pre vodu a pocit z vlákien, vyberajte riešenie, ktoré nezanechá ťažký avivážový povlak.",
            "product_name": "Pravá octová aviváž lesná zmes 1L",
            "product_href": "/p-1626/prava-octova-avivaz-lesna-zmes-1l",
            "fit": "pri uterákoch a bežných domácich textíliách, kde chcete sviežejší pocit bez klasickej ťažkej aviváže.",
            "boundary": "nepoužívajte ju ako opravu pre priveľa pracieho gélu. Ak je textil lepkavý, najprv ho vypláchajte a upravte dávkovanie.",
            "product_button": "Pozrieť octovú aviváž",
            "category_title": "Porovnajte riešenia na mäkší pocit bielizne",
            "category_intro": "Mäkší pocit nevzniká iba pridaním aviváže. Rozhoduje tvrdosť vody, oplach, typ textilu a to, či má bielizeň zostať savá.",
            "category_bullets": [
                ("Uteráky", "nechajte im priestor, dôkladný oplach a vyhnite sa ťažkému filmu."),
                ("Bežné oblečenie", "riešte zvyšky gélu a správnu dávku."),
                ("Citlivý čuch", "voľte jemnejšiu sviežosť až po vyriešení príčiny tvrdosti."),
            ],
            "category_href": "/c/vevo-home-care/pranie/avivaz/octova-avivaz",
            "category_button": "Pozrieť octové aviváže",
        },
    },
}


def remove_between(text, start, end):
    while start in text and end in text:
        before, rest = text.split(start, 1)
        _, after = rest.split(end, 1)
        text = before + after
    return text


def table(headers, rows):
    head = "".join(
        f'<th style="border: 1px solid #e5e5e5; padding: 10px; text-align: left;">{escape(str(h))}</th>'
        for h in headers
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


def query_box(queries):
    items = "".join(f"<li>{escape(item)}</li>" for item in queries)
    return f"""<div style="border: 1px solid #d7e2ec; border-radius: 8px; padding: 18px; margin: 22px 0; background: #f7fbff;">
<h2 style="margin-top: 0;">Čo tento článok rieši do hĺbky</h2>
<p>Nižšie nájdete praktické odpovede aj na súvisiace otázky, ktoré ľudia pri praní často hľadajú samostatne.</p>
<ul>{items}</ul>
</div>"""


def expert_addendum(config):
    paragraphs = "\n".join(f"<p>{escape(text)}</p>" for text in config["expert_paragraphs"])
    rows = [
        (escape(a), escape(b), escape(c))
        for a, b, c in config["diagnostic_rows"]
    ]
    return f"""<h2>{escape(config["expert_title"])}</h2>
{paragraphs}
<h2>Diagnostická tabuľka: čo skontrolovať ako prvé</h2>
{table(["Príznak", "Pravdepodobná príčina", "Prvý praktický krok"], rows)}"""


def sales_block(config):
    sales = config["sales"]
    bullets = "".join(
        f"<li><strong>{escape(label)}:</strong> {escape(text)}</li>"
        for label, text in sales["category_bullets"]
    )
    return f"""<div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 24px 0; background: #f7fbf8;">
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
</div>"""


def replace_generic_recommendation(long, replacement):
    marker = "\n<h2>Súvisiace návody na VEVO</h2>"
    if marker not in long:
        raise ValueError("Related-guides marker not found")
    before, after = long.split(marker, 1)
    generic_start = before.rfind('<div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 24px 0; background: #f7fbf8;">')
    if generic_start == -1:
        return before.rstrip() + "\n" + replacement + marker + after
    before = before[:generic_start].rstrip()
    return before + "\n" + replacement + marker + after


def retrofit_article(article):
    slug = article["link"]
    if slug not in QUALITY:
        raise ValueError(f"No quality config for {slug}")

    config = QUALITY[slug]
    long = article["long"]
    long = remove_between(long, "<!-- VEVO batch 26 quality fanout start -->", "<!-- VEVO batch 26 quality fanout end -->")
    long = remove_between(long, "<!-- VEVO batch 26 expert expansion start -->", "<!-- VEVO batch 26 expert expansion end -->")
    long = remove_between(long, "<!-- VEVO batch 26 sales block start -->", "<!-- VEVO batch 26 sales block end -->")

    quick_marker = '\n<div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">'
    if quick_marker not in long:
        raise ValueError(f"Quick box marker not found for {slug}")
    long = long.replace(quick_marker, "\n" + query_box(config["queries"]) + quick_marker, 1)

    source_marker = '\n<div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; margin: 22px 0; background: #fbfbfb;">\n<h2 style="margin-top: 0;">Zdroje'
    if source_marker not in long:
        raise ValueError(f"Source marker not found for {slug}")
    long = long.replace(source_marker, "\n" + expert_addendum(config) + source_marker, 1)

    long = replace_generic_recommendation(long, sales_block(config))
    article["long"] = long
    return article


def check_links(articles):
    hrefs = sorted({href for row in articles for href in re.findall(r'href="([^"]+)"', row["long"])})
    checks = []
    for href in hrefs:
        if href.startswith("/"):
            url = urljoin(BASE, href)
        elif href.startswith("http"):
            url = href
        else:
            continue
        response = requests.get(
            url,
            headers={"User-Agent": "Codex VEVO batch 26 quality retrofit"},
            timeout=45,
            allow_redirects=True,
        )
        checks.append({"url": url, "status": response.status_code, "final_url": response.url, "ok": response.status_code == 200})
        if response.status_code != 200:
            raise SystemExit(f"Link check failed: {url} -> {response.status_code} {response.url}")
    return checks


def main():
    articles = json.loads(IN_JSON.read_text(encoding="utf-8"))
    mapping = json.loads(MAPPING_JSON.read_text(encoding="utf-8"))
    post_by_slug = {post["slug"]: post for post in mapping["posts"]}

    updated = []
    for article in articles:
        article = retrofit_article(article)
        long = article["long"]
        if re.search(r"\bCTA\b", long, re.IGNORECASE):
            raise SystemExit(f"Forbidden internal acronym in article: {article['title']}")
        if "Cena:" in long or "€" in long:
            raise SystemExit(f"Fixed price marker in article: {article['title']}")
        if long.count("Čo tento článok rieši do hĺbky") != 1:
            raise SystemExit(f"Fan-out block count mismatch: {article['title']}")
        if long.count("Kedy dáva zmysel:") != 1 or long.count("Kedy najprv riešiť príčinu:") != 1:
            raise SystemExit(f"Product block count mismatch: {article['title']}")
        post = post_by_slug[article["link"]]
        updated.append(
            {
                "post_id": post["id"],
                "url": post["url"],
                "title": article["title"],
                "short": article["short"],
                "long": long,
                "length": len(long),
                "href_count": len(re.findall(r'href="([^"]+)"', long)),
            }
        )

    checks = check_links(articles)
    IN_JSON.write_text(json.dumps(articles, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    OUT_UPDATE.write_text(
        json.dumps(
            {
                "batch": "batch-26",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "purpose": "Quality retrofit for last published VEVO articles 2255-2259.",
                "updates": updated,
                "link_checks": checks,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    print(
        json.dumps(
            {
                "updated_articles": len(updated),
                "links_checked": len(checks),
                "update_export": str(OUT_UPDATE),
                "lengths": {item["post_id"]: item["length"] for item in updated},
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
