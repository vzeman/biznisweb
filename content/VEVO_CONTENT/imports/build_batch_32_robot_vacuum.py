import html
import json
import re
import unicodedata
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

import requests

try:
    import xlwt
except ImportError:  # pragma: no cover
    xlwt = None


BASE = "https://www.vevo.sk"
BATCH = 32
BATCH_DATE = "2025-09-20"
OUT_JSON = Path("content/VEVO_CONTENT/imports/batch-32-2026-07-05-articles.json")
OUT_PREFLIGHT = Path("content/VEVO_CONTENT/exports/batch-32-2026-07-05-link-preflight.json")
OUT_XLS = Path.home() / "AppData/Local/Temp/vevo-batch-32-robot-vacuum-clean-urls.xls"

REQUIRED_CATEGORY = "https://www.vevo.sk/c/vevo-home-care/upratovanie/cistiace-prostriedky/cistic-do-robotickeho-vysavaca"
REQUIRED_PRODUCT = "https://www.vevo.sk/p-1635/vevo-cistic-podlah-pre-vsetky-vysavace-ylang-absolute"

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
    rows = "".join(f'<li><a rel="noopener" href="{href}" target="_blank">{esc(label)}</a></li>' for label, href in items)
    return f"""
<div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; margin: 22px 0; background: #fbfbfb;">
<h2 style="margin-top: 0;">Zdroje a odborný kontext</h2>
<p>Technické postupy pri robotických vysávačoch sa líšia podľa modelu. Preto sú nižšie uvedené zdroje rámec k párovaniu, offline stavu, nabíjaniu, navigácii a používaniu čistiacich roztokov; pri konkrétnom modeli má prednosť návod výrobcu.</p>
<ul>{rows}</ul>
</div>
""".strip()


def recommendation_block():
    return f"""
<div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 24px 0; background: #f7fbf8;">
<h2 style="margin-top: 0;">Odporúčané riešenie na mopovanie v robotickom vysávači</h2>
<p>Ak robotický vysávač používa mopovaciu nádržku, vyberajte prípravok určený na tento typ použitia a dávkujte ho podľa odporúčania výrobcu zariadenia aj produktu. Nevhodné husté alebo penivé roztoky môžu zanechávať povlak, upchávať vedenie vody alebo zhoršiť rovnomerné dávkovanie.</p>
<div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff; margin: 14px 0;">
<h3 style="margin-top: 0;">VEVO čistič podláh pre všetky vysávače Ylang Absolute</h3>
<p>Praktická voľba pri domácnostiach, kde robotický vysávač nielen vysáva, ale aj mopuje tvrdé podlahy. Používajte ho rozumne, bez prelievania nádržky a vždy s ohľadom na návod vášho robotického vysávača.</p>
<p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="{REQUIRED_PRODUCT}">Pozrieť produkt</a></p>
</div>
<p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="{REQUIRED_CATEGORY}">Pozrieť čističe do robotického vysávača</a></p>
</div>
""".strip()


RELATED = [
    ("Ako odstrániť chlpy z oblečenia pri praní, keď máte psa alebo mačku", "/n/ako-odstranit-chlpy-z-oblecenia-pri-prani-ked-mate-psa-alebo-macku"),
    ("Ako prať textílie v domácnosti so psom počas pĺznutia", "/n/ako-prat-textilie-v-domacnosti-so-psom-pocas-plznutia"),
    ("Ako vyčistiť pelech pre psa alebo mačku, aby nezapáchal", "/n/ako-vycistit-pelech-pre-psa-alebo-macku-aby-nezapachal"),
    ("Prečo oblečenie zapácha po praní", "/n/preco-moje-oblecenie-zapacha-po-prani-priciny-a-riesenia"),
    ("Ako vyčistiť filter práčky", "/n/ako-vycistit-filter-pracky-ked-bielizen-zapacha-alebo-voda-odteka-pomaly"),
]


def related_links(extra=None):
    items = list(extra or []) + RELATED[:4]
    links = "".join(f'<li><a href="{href}">{esc(label)}</a></li>' for label, href in items)
    return f"<h2>Súvisiace návody na VEVO</h2>\n<ul>{links}</ul>"


def faq(items):
    parts = ["<h2>FAQ</h2>"]
    for question, answer in items:
        parts.append(f"<h3>{esc(question)}</h3><p>{esc(answer)}</p>")
    return "\n".join(parts)


def render_article(article):
    parts = [
        f"<p><strong>Rýchla odpoveď:</strong> {article['answer']}</p>",
        callout("Rýchla orientácia", article["quick"], background="#f7fbff", border="#d7e2ec"),
    ]
    parts.extend(f"<p>{p}</p>" for p in article["intro"])
    parts.append(f"<h2>{esc(article['decision_heading'])}</h2>")
    parts.append(table(["Situácia", "Čo to znamená", "Čo urobiť"], article["decision_rows"]))
    parts.append("<h2>Postup krok za krokom</h2>")
    parts.append("<ol>" + "".join(f"<li>{step}</li>" for step in article["steps"]) + "</ol>")
    parts.append("<h2>Najčastejšie chyby</h2>")
    parts.append("<ul>" + "".join(f"<li>{item}</li>" for item in article["mistakes"]) + "</ul>")
    parts.append("<h2>Rýchla kontrolná tabuľka</h2>")
    parts.append(table(["Kontrola", "Prečo je dôležitá", "Praktická poznámka"], article["check_rows"]))
    for heading, paragraphs in article["sections"]:
        parts.append(f"<h2>{esc(heading)}</h2>")
        if isinstance(paragraphs, str):
            paragraphs = [paragraphs]
        for paragraph in paragraphs:
            parts.append(f"<p>{paragraph}</p>")
    parts.append(callout("Dôležité pravidlo", article["rule"], background="#fffaf5", border="#e6ded2"))
    parts.append("<h2>Praktická rutina po mesiaci používania</h2>")
    parts.append(
        "<p>Po prvom mesiaci si všimnite, čo sa opakuje: či robot vynecháva rovnaké miesto, či sa vracia do stanice bez problémov, či sa do kefy motajú vlasy, či mop robí šmuhy a či aplikácia pravidelne hlási offline stav. Tieto drobné signály sú dôležitejšie než jednorazový dojem z prvého spustenia, pretože ukazujú, ako robot zapadol do reálnej domácnosti.</p>"
    )
    parts.append(
        "<p>Raz mesačne sa oplatí urobiť malú kontrolu: vyčistiť nádobu, filter, kefu, senzory, kontakty a pri mopovacom modeli aj nádržku a handričku. Skontrolujte aj umiestnenie stanice, káble okolo nej a podlahy, kde robot najčastejšie bojuje. Ak sa problém opakuje, upravte priestor alebo nastavenie skôr, než začnete meniť produkt alebo robiť tvrdý reset.</p>"
    )
    parts.append("<h2>Ako si urobiť domácu diagnostiku bez rizika</h2>")
    parts.append(
        "<p>Pri robotickom vysávači sa oplatí rozlišovať medzi problémom s priestorom, problémom so samotným robotom a problémom v aplikácii. Ak sa robot zasekáva vždy pri rovnakom prahu, ide skôr o priestor. Ak sa zhoršilo vysávanie po niekoľkých týždňoch, hľadajte plnú nádobu, zanesený filter alebo vlasy na kefe. Ak aplikácia ukazuje zvláštne stavy, najprv overte Wi-Fi a účet.</p>"
    )
    parts.append(
        "<p>Bezpečný postup je jednoduchý: najprv robot vypnite, vyberte nádobu, pozrite kefu, filter, bočnú kefu, kolesá, senzory a kontakty nabíjania. Potom skontrolujte stanicu, káble, priestor pred stanicou a signál Wi-Fi. Až keď tieto veci sedia, má zmysel riešiť reset, nové párovanie alebo servis.</p>"
    )
    parts.append("<h2>Čo si všímať podľa príznakov</h2>")
    parts.append(
        "<p>Ak robot vynecháva miesta, často ide o mapu, prekážky alebo zle umiestnenú stanicu. Ak robí šmuhy, zamerajte sa na mopovaciu handričku, nádržku, dávkovanie vody a vhodný čistiaci roztok. Ak je hlučnejší než zvyčajne, skontrolujte hlavnú kefu, bočné kefy, kolieska a cudzie predmety. Ak sa nevracia do stanice, riešte najmä priestor, kontakty a polohu stanice.</p>"
    )
    parts.append(
        "<p>Pri opakovanom probléme pomáha zapísať si, kedy nastáva: po nabíjaní, po mopovaní, po zmene Wi-Fi, po presune stanice alebo po výmene filtra. Tak rýchlejšie odlíšite bežnú údržbu od technickej poruchy. Je to praktickejšie než robiť rovno továrenský reset, ktorý môže vymazať mapy a nastavenia.</p>"
    )
    parts.append("<h2>Kedy pomôže údržba a kedy servis</h2>")
    parts.append(
        "<p>Údržba pomáha pri prachu, vlasoch, chlpoch, šmuhách, zápachu z mopovacej handričky, slabšom prietoku vody a horšom návrate do stanice spôsobenom špinavými kontaktmi. Vtedy má zmysel vyčistiť diely, vyprať mop, vyprázdniť nádržku a používať vhodný prípravok do robotického vysávača podľa odporúčaní výrobcu.</p>"
    )
    parts.append(
        "<p>Servis je namieste, ak sa robot nenabíja ani po očistení kontaktov, nereaguje na tlačidlá, hlási chybu motora, opakovane sa vypína alebo má mechanicky poškodenú kefu, koleso či nádržku. V takom prípade nepomáha pridávať viac čističa ani opakovane resetovať aplikáciu. Potrebné je riešiť konkrétny model podľa návodu.</p>"
    )
    parts.append("<h2>Ako nastaviť rutinu, aby robot dával zmysel</h2>")
    parts.append(
        "<p>Robotický vysávač funguje najlepšie ako pravidelná údržba, nie ako nárazové riešenie veľkého neporiadku. Pred plánovaným upratovaním odstráňte káble, ponožky, malé hračky a voľné textílie. Pri mopovaní nenechávajte na podlahe veľké mokré škvrny ani lepkavé zvyšky jedla. Robot potom pracuje predvídateľnejšie a menej často sa zasekne.</p>"
    )
    parts.append(
        "<p>Dobrá rutina môže byť jednoduchá: vysýpať nádobu podľa množstva prachu, filter čistiť pravidelne, mop prať po použití, nádržku nenechávať plnú celé dni a raz za čas skontrolovať mapu v aplikácii. Pri domácnosti so zvieratami skráťte interval čistenia kefy, pretože chlpy vedia výkon zhoršiť veľmi rýchlo.</p>"
    )
    parts.append("<h2>Ako často robiť malé kontroly</h2>")
    parts.append(
        "<p>V bežnom byte stačí rýchla kontrola po niekoľkých cykloch: vysypať nádobu, pozrieť kefu a utrieť viditeľný prach zo senzorov. Pri deťoch, zvieratách, dlhých vlasoch alebo piesku z chodby skráťte interval. Nie je potrebné robiť veľký servis každý deň, ale krátka pravidelná kontrola zabráni tomu, aby sa malý problém zmenil na slabý výkon alebo chybové hlásenie.</p>"
    )
    parts.append(
        "<p>Pri mopovaní je interval ešte dôležitejší. Handričku nenechávajte zaschnúť špinavú, nádržku po použití vyprázdnite a čistič dávkujte mierne. Ak sa na podlahe objaví povlak, šmuhy alebo zatuchnutý pach, nehľadajte hneď chybu v robote. Často stačí vyprať mop, znížiť dávku roztoku, vyčistiť nádržku a nechať diely dobre vyschnúť.</p>"
    )
    parts.append("<h2>Odbornejší pohľad</h2>")
    parts.extend(f"<p>{p}</p>" for p in article["expert"])
    parts.append(source_box(article["sources"]))
    parts.append(recommendation_block())
    parts.append(related_links(article.get("related")))
    parts.append(faq(article["faq"]))
    return "\n".join(parts)


ARTICLES = [
    {
        "title": "Ako vybrať robotický vysávač",
        "short": "Robotický vysávač vyberajte podľa členitosti bytu, typu podlahy, prahov, kobercov, domácich zvierat, výšky nábytku, navigácie, dokovacej stanice a údržby. Najlepší model nie je vždy ten s najvyšším výkonom, ale ten, ktorý zvládne váš pôdorys a bude sa ľahko čistiť.",
        "answer": "Robotický vysávač vyberajte podľa toho, aký máte byt, koľko máte prahov, kobercov, káblov, zvierat a tvrdých podláh. Do členitej domácnosti má zmysel presnejšia navigácia a dobrá mapa; pri zvieratách sledujte kefu, filter a nádobu na prach; pri mopovaní riešte aj nádržku, dávkovanie vody a vhodný čistič do robotického vysávača.",
        "quick": [
            "<strong>Najprv byt, potom značka:</strong> členitý pôdorys, prahy a nábytok rozhodujú viac než marketingový výkon.",
            "<strong>Pri zvieratách sledujte kefu:</strong> vlasy a chlpy vedia zhoršiť výsledok aj pri drahom modeli.",
            "<strong>Mapa je praktická:</strong> zóny, zakázané miesta a návrat do dokovacej stanice šetria čas.",
            "<strong>Mopovanie nie je umývanie podlahy rukou:</strong> ide skôr o pravidelnú ľahkú údržbu povrchu.",
            "<strong>Údržba rozhoduje o životnosti:</strong> prachová nádoba, filter, senzory, kolesá a mop musia byť dostupné.",
        ],
        "intro": [
            "Výber robotického vysávača nezačína otázkou, ktorý model je najlacnejší alebo najvýkonnejší. Začína tým, ako vyzerá vaša domácnosť. Iný vysávač potrebuje byt s hladkou podlahou a minimom nábytku, iný dom so schodmi, prahmi, kobercami, stolmi, detskými hračkami a pelechom pre psa.",
            "Robotický vysávač má fungovať pravidelne. Preto je dôležité, aby sa nezasekával, vedel sa vrátiť do dokovacej stanice a aby ste ho po každom upratovaní nemuseli pol hodiny rozoberať. Silné sanie pomôže, ale samo o sebe nevyrieši zlú navigáciu, príliš vysoké prahy alebo zamotané káble.",
            "Ak má vysávač aj mop, riešite navyše vodnú nádržku, mopovaciu handričku a to, čo môžete dať do nádržky. Pri tvrdých podlahách môže byť robotický vysávač s mopom veľmi užitočný, ale stále ide o údržbové mopovanie, nie náhradu za dôkladné ručné umývanie po rozliatom oleji alebo zaschnutom blate.",
            "Dobrý výber je taký, pri ktorom myslíte aj na čistenie samotného robota. Nádoba na prach, filter, bočná kefa, hlavná kefa, senzory a kolieska sú miesta, ktoré budú rozhodovať o tom, či bude robot po mesiaci stále upratovať rovnako dobre.",
        ],
        "decision_heading": "Ako vybrať robotický vysávač podľa domácnosti",
        "decision_rows": [
            ("Malý byt s tvrdou podlahou", "robot má jednoduchý pôdorys a menej prekážok", "stačí spoľahlivá navigácia, nízka výška a dobrá dostupnosť náhradných dielov"),
            ("Členitý byt alebo dom", "robot musí rozumieť miestnostiam a návratu do stanice", "zvážte presnejšie mapovanie, zóny a zakázané oblasti"),
            ("Domácnosť so psom alebo mačkou", "chlpy sa motajú do kefy a plnia nádobu", "sledujte typ kefy, veľkosť nádoby, filter a ľahké čistenie"),
            ("Koberce a prahy", "robot môže stratiť výkon alebo sa zaseknúť", "overte výšku prahov, režim kobercov a zdvih mopu"),
            ("Tvrdé podlahy a mopovanie", "prach sa kombinuje s jemným filmom na podlahe", f"vyberajte model s mopom a používajte vhodný <a href=\"{REQUIRED_CATEGORY}\">čistič do robotického vysávača</a>"),
        ],
        "steps": [
            "Zmerajte najnižší nábytok, pod ktorý má robot zachádzať, najmä sedačku, posteľ a skrinky.",
            "Spočítajte problematické prahy, káble, úzke miesta a miesta, kde sa často nechávajú hračky alebo papuče.",
            "Rozhodnite, či potrebujete iba vysávanie alebo aj mopovanie tvrdých podláh.",
            "Pri zvieratách uprednostnite model, ktorý sa dá ľahko vyčistiť od vlasov a chlpov.",
            "Skontrolujte, či aplikácia podporuje mapy, zóny, zakázané miesta a plánovanie upratovania.",
            "Overte dostupnosť filtrov, kief, mopovacích handričiek a spôsob čistenia senzorov.",
            "Pri mopovaní používajte len roztok vhodný pre robotické vysávače a vždy rešpektujte návod výrobcu zariadenia.",
        ],
        "mistakes": [
            "Vybrať robot iba podľa sacieho výkonu a ignorovať členitosť bytu.",
            "Zabudnúť na výšku robota pod sedačkou alebo posteľou.",
            "Kúpiť mopovací model a potom používať agresívny alebo penivý čistič.",
            "Nechať káble, šnúrky a malé predmety na zemi a potom viniť robota zo zasekávania.",
            "Ignorovať dostupnosť náhradných filtrov, kief a mopovacích handričiek.",
            "Očakávať, že robotický vysávač nahradí dôkladné ručné čistenie každej škvrny.",
        ],
        "check_rows": [
            ("Výška robota", "rozhoduje, či sa dostane pod nábytok", "zmerajte priestor pred kúpou"),
            ("Navigácia", "ovplyvní systematické upratovanie", "pri členitom byte sa oplatí presnejšia mapa"),
            ("Údržba kefy", "vlasy znižujú účinnosť", "skontrolujte, ako sa kefa vyberá"),
            ("Mopovanie", "nádržka a handrička potrebujú údržbu", "vhodný roztok pomáha obmedziť povlak"),
            ("Dokovacia stanica", "robot potrebuje priestor na návrat", "neumiestňujte ju do úzkej medzery"),
        ],
        "sections": [
            ("Robotický vysávač do bytu", [
                "V byte často rozhoduje výška robota, hluk a schopnosť jazdiť popri nábytku. Ak máte otvorený priestor a tvrdé podlahy, robotický vysávač môže udržať prach pod kontrolou veľmi dobre. Pri menších izbách s množstvom stoličiek a káblov je dôležitejšie, aby ste vedeli nastaviť zóny a upratať podlahu pred štartom.",
                "Ak bývate v byte s domácimi zvieratami, sledujte aj to, ako často bude treba vysypať nádobu. Veľké množstvo chlpov rýchlo ukáže slabú údržbu filtra a kefy.",
            ]),
            ("Robotický vysávač do domu", [
                "V dome býva viac prahov, miestností a často aj viac typov podláh. Robotický vysávač nemusí zvládnuť schody, preto treba počítať s presúvaním medzi poschodiami alebo s viacerými mapami. Dôležitá je stabilná dokovacia stanica a dobré mapovanie miestností.",
                "Pri dome s dvorom sa do interiéru nosí viac piesku, blata a drobných kamienkov. Robot pomôže s pravidelným prachom, ale mokré blato alebo veľké nečistoty treba odstrániť skôr, než sa roznesú po podlahe.",
            ]),
            ("S mopom alebo bez mopu", [
                "Model s mopom dáva zmysel tam, kde máte väčšinu tvrdých podláh a chcete pravidelnú ľahkú údržbu. Ak máte prevažne koberce, mopovanie využijete menej. Pri kombinácii kobercov a tvrdých podláh sledujte, či robot vie rozpoznať koberec, zdvihnúť mop alebo sa mu vyhnúť.",
                f"Pri mopovaní podlahy neriešte len vodu. Vhodný prípravok, napríklad <a href=\"{REQUIRED_PRODUCT}\">VEVO čistič podláh pre všetky vysávače Ylang Absolute</a>, má byť použitý s rozumným dávkovaním a podľa odporúčaní výrobcu robota.",
            ]),
            ("Ako myslieť na údržbu už pri kúpe", [
                "Robotický vysávač sa nekupuje iba na prvé spustenie. Po týždňoch používania rozhodne, či sa ľahko čistí nádoba, filter, senzory a kefa. Ak je údržba komplikovaná, človek ju začne odkladať a robot postupne stráca výkon.",
                "Sledujte aj cenu a dostupnosť spotrebných dielov, ale nefixujte sa iba na ne. Dôležitejšie je, či ich budete reálne meniť a čistiť v intervaloch, ktoré zodpovedajú vašej domácnosti.",
            ]),
            ("Kedy robotický vysávač nemusí byť dobrá voľba", [
                "Ak máte na zemi trvalo veľa káblov, hračiek, textílií alebo vysoké prahy, robot sa bude často zastavovať. To neznamená, že je zlý; znamená to, že domácnosť nie je pripravená na automatické upratovanie. Vtedy pomôže zmeniť organizáciu priestoru alebo vybrať model s lepším rozpoznávaním prekážok.",
                "Robotický vysávač tiež nie je nástroj na veľké nehody: rozliate lepkavé nápoje, veľké kusy jedla alebo mokré blato patria najprv pod ručné čistenie. Robot má udržiavať čistotu pravidelne, nie zachraňovať extrémne situácie.",
            ]),
        ],
        "rule": [
            "Najlepší robotický vysávač je ten, ktorý zvládne konkrétnu domácnosť a ktorý budete vedieť pravidelne čistiť.",
            "Pri mopovaní používajte iba vhodné prípravky a neprelievajte nádržku nad odporúčanie výrobcu.",
        ],
        "expert": [
            "Moderné robotické vysávače kombinujú pohyb, senzory, mapovanie a algoritmy návratu do dokovacej stanice. Niektoré používajú LiDAR, iné kamerovú navigáciu alebo kombináciu nárazových a infračervených senzorov. To, čo je pre zákazníka viditeľné ako „uprataná mapa“, je výsledok orientácie v priestore, detekcie prekážok a plánovania trasy.",
            "Oficiálne zdroje výrobcov ukazujú, že robotická navigácia nie je jedna univerzálna technológia. LiDAR meria vzdialenosť laserom, kamerové systémy sa orientujú podľa vizuálnych prvkov v miestnosti a jednoduchšie modely sa pohybujú viac reaktívne. Preto sa líši presnosť mapy, schopnosť jazdiť po tme aj správanie pri zrkadlách, oknách a lesklom nábytku.",
            "Pri mopovaní je technické obmedzenie ešte praktickejšie: robot nesie malé množstvo vody a mopovacia handrička sa postupne špiní. Preto je dôležitý vhodný čistič, údržba handričky a pravidelné ručné dočistenie miest, kde sa tvorí mastnota alebo zaschnutá špina.",
        ],
        "sources": [
            ("iRobot: How does my Robot Navigate?", "https://homesupport.irobot.com/s/article/31056"),
            ("iRobot: Guide to ClearView LiDAR Maps", "https://homesupport.irobot.com/s/article/20500"),
            ("ECOVACS: Can you use cleaning solution in a robot mop?", "https://www.ecovacs.com/us/blog/can-you-put-cleaning-solution-in-robot-mop"),
        ],
        "faq": [
            ("Aký robotický vysávač vybrať do bytu?", "Do bytu je dôležitá nízka výška, spoľahlivý návrat do dokovacej stanice a dobré správanie okolo nábytku. Pri zvieratách sledujte najmä kefu, filter a nádobu na prach."),
            ("Je lepší robotický vysávač s mopom?", "Ak máte tvrdé podlahy, často áno. Ak máte prevažne koberce, mopovanie využijete menej alebo budete potrebovať model, ktorý sa kobercom pri mopovaní vyhne."),
            ("Čo je dôležitejšie: výkon alebo navigácia?", "Pri bežnej domácnosti je navigácia veľmi dôležitá. Silný robot, ktorý sa zasekáva alebo vynecháva miestnosti, neprinesie lepší výsledok."),
            ("Môžem dať do nádržky bežný čistič podláh?", "Nie vždy. Používajte iba prípravok vhodný pre robotické vysávače a riaďte sa návodom výrobcu zariadenia."),
        ],
    },
]


def add_more_articles():
    articles = [
        make_article(
            "Ako vybrať robotický vysávač s mopom",
            "Robotický vysávač s mopom vyberajte podľa typu podlahy, veľkosti nádržky, spôsobu dávkovania vody, zdvihu mopu, čistenia mopovacej handričky a toho, či máte koberce. Mopovanie berte ako pravidelnú údržbu, nie ako náhradu za ručné umývanie zaschnutých škvŕn.",
            "Pri robotickom vysávači s mopom sledujte nielen sanie, ale aj nádržku, mopovaciu handričku, dávkovanie vody, ochranu kobercov a vhodný čistič do robotického vysávača.",
            "mopovanie",
            [
                ("Tvrdé podlahy", "mopovací modul má najväčší zmysel", "overte dávkovanie vody a vhodný čistič"),
                ("Koberce v byte", "mop môže koberec zbytočne navlhčiť", "hľadajte zdvih mopu alebo zóny bez mopovania"),
                ("Drevená podlaha", "citlivá na nadmernú vlhkosť", "nastavte nízky prietok vody"),
                ("Domáce zvieratá", "chlpy sa miešajú s vlhkosťou", "najprv vysávať, potom mopovať"),
            ],
            [
                "Skontrolujte, či robot vie mop odpojiť, zdvihnúť alebo vynechať koberce.",
                "Overte, či má elektronické dávkovanie vody alebo len pasívne vlhčenie handričky.",
                "Zistite, ako často sa má prať alebo meniť mopovacia handrička.",
                "Pri väčšej domácnosti sledujte kapacitu nádržky a dĺžku trasy.",
                "Nepoužívajte penivé ani agresívne čističe, ak ich výrobca robota nepovoľuje.",
            ],
            [
                "Použiť bežný hustý univerzálny čistič bez overenia kompatibility.",
                "Nechať mokrú handričku na robote celý deň.",
                "Pustiť mopovanie cez koberce bez zón alebo zdvihu mopu.",
                "Dolievat viac prípravku s nádejou, že podlaha bude čistejšia.",
                "Ignorovať údržbu nádržky, trysiek a mopovacej dosky.",
            ],
            [
                ("Mopovanie tvrdých podláh", "Tvrdé podlahy zvládnu pravidelnú ľahkú údržbu najlepšie. Robot roznesie malé množstvo vody a čističa, ale potrebuje čistú handričku. Pri mastnej kuchyni alebo zaschnutých škvrnách je rozumné najprv lokálne dočistenie."),
                ("Koberce a mopovací robot", "Ak máte koberce, skontrolujte, či robot dokáže mop zdvihnúť alebo nastaviť zakázané zóny. Vlhký koberec je zbytočný problém a môže zadržiavať pach."),
                ("Dávkovanie čističa", "Pri čistiacom roztoku nejde o silnejšiu vôňu, ale o kompatibilitu s nádržkou, hadičkami a podlahou. Menej vhodného prípravku je lepšie než veľa penivého roztoku."),
                ("Údržba nádržky", "Nádržku po mopovaní vyprázdnite, najmä ak robot dlho stojí. Vlhké prostredie a zvyšky špiny môžu vytvoriť zápach alebo nerovnomerné dávkovanie vody."),
                ("Mop a domáce zvieratá", "Pri zvieratách najprv vysajte chlpy. Ak ich robot namočí mopom, môžu sa zlepiť na okrajoch miestnosti alebo na handričke."),
            ],
            [
                "Robotický mop používa malé množstvo vody, preto je závislý od pravidelnosti a čistej handričky.",
                "Výrobcovia robotických mopov často upozorňujú, že nevhodné čistiace roztoky môžu zanechávať zvyšky alebo ovplyvniť vodné vedenie.",
                "Pri dreve, lamináte a citlivých podlahách rozhoduje skôr nízka vlhkosť a časté ľahké mopovanie než agresívne premáčanie.",
            ],
            [
                ("ECOVACS: Can you use cleaning solution in a robot mop?", "https://www.ecovacs.com/us/blog/can-you-put-cleaning-solution-in-robot-mop"),
                ("ECOVACS: robot vacuum mop not dispensing water", "https://www.ecovacs.com/us/blog/robot-vacuum-mop-not-dispensing-water"),
            ],
            [
                ("Je robotický vysávač s mopom vhodný na drevenú podlahu?", "Áno, ak výrobca podlahy aj robota povoľuje vlhké mopovanie a nastavíte nízky prietok vody."),
                ("Môže ísť čistič do nádržky?", "Iba vhodný prípravok a podľa návodu. Penivé, agresívne alebo husté roztoky nepoužívajte naslepo."),
                ("Prečo sú po mopovaní šmuhy?", "Často ide o špinavú handričku, priveľa vody alebo nevhodné dávkovanie prípravku."),
            ],
        ),
        make_article(
            "Ako vyčistiť robotický vysávač",
            "Robotický vysávač čistite po častiach: nádoba na prach, filter, hlavná kefa, bočná kefa, kolesá, senzory, kontakty nabíjania, mopovacia handrička a nádržka. Pri mopovaní venujte pozornosť aj zvyškom vody a čistiaceho roztoku.",
            "Robotický vysávač vyčistíte tak, že ho vypnete, vyberiete nádobu na prach, vyprázdnite ju, očistíte filter, odstránite vlasy z kefy, utriete senzory a kontakty, skontrolujete kolesá a pri mopovacom modeli vypláchnete nádržku a operiete mopovaciu handričku.",
            "čistenie robota",
            [
                ("Slabšie vysávanie", "nádoba alebo filter sú plné", "vyprázdniť nádobu a očistiť filter"),
                ("Robot naráža", "senzory môžu byť zaprášené", "utrieť senzory mäkkou suchou handričkou"),
                ("Kefa sa netočí", "vlasy sú namotané okolo kefy", "vybrať kefu a odstrániť vlasy"),
                ("Mop zapácha", "handrička alebo nádržka ostala vlhká", "umyť a vysušiť"),
            ],
            [
                "Vypnite robot a odpojte ho od dokovacej stanice.",
                "Vyprázdnite nádobu na prach a skontrolujte, či nie je zanesený filter.",
                "Vyberte hlavnú kefu a odstráňte vlasy, nite a chlpy.",
                "Utrite bočné senzory, spodné senzory a nabíjacie kontakty suchou handričkou.",
                "Skontrolujte kolesá, či v nich nie sú nitky, piesok alebo vlasy.",
                "Pri mopovacom modeli vypláchnite nádržku, operte mop a nechajte všetko vyschnúť.",
            ],
            [
                "Čistiť iba nádobu a ignorovať kefu.",
                "Umývať filter vodou, ak to výrobca nepovoľuje.",
                "Použiť mokrú handru na nabíjacie kontakty bez vysušenia.",
                "Nechať špinavý mop zaschnúť na robote.",
                "Zabudnúť na senzory, keď robot začne jazdiť chaoticky.",
            ],
            [
                ("Nádoba na prach a filter", "Nádoba ukáže, koľko prachu robot reálne zbiera. Filter je citlivejší: nie každý je umývateľný. Ak je zanesený, robot môže stratiť výkon a prach sa môže vracať do vzduchu."),
                ("Hlavná kefa a vlasy", "Vlasy a nite sa často navinú na okraje kefy, kde nie sú na prvý pohľad viditeľné. Pravidelné odstránenie vlasov predlžuje životnosť kefy aj motora."),
                ("Senzory a kontakty", "Senzory pomáhajú robotu orientovať sa, vyhýbať sa pádu a vrátiť sa do stanice. Ak sú zaprášené, robot môže narážať, blúdiť alebo sa horšie dokovať."),
                ("Mopovacia nádržka", f"Ak používate <a href=\"{REQUIRED_CATEGORY}\">čistič do robotického vysávača</a>, nádržku aj tak pravidelne vyplachujte a nenechávajte v nej starý roztok."),
                ("Dokovacia stanica", "Čistite aj okolie stanice. Prach na kontaktoch alebo vo vjazde môže zhoršiť nabíjanie a návrat robota."),
            ],
            [
                "Robotický vysávač je malý pohyblivý systém, ktorý kombinuje sanie, kefy, senzory a prípadne dávkovanie vody.",
                "Ak sa zanesie filter alebo kefa, problém sa neprejaví iba slabším výsledkom. Robot môže byť hlučnejší, dlhšie upratovať alebo sa zastavovať.",
                "Pri mopovacích modeloch výrobcovia často riešia aj zápach z nádržky, mopu a častí, ktoré prichádzajú do kontaktu so špinavou vodou.",
            ],
            [
                ("Xiaomi Robot Vacuum 5 FAQ", "https://www.mi.com/global/support/faq/details/KA-597579/"),
                ("ECOVACS: How to clean robot vacuum cleaner", "https://www.ecovacs.com/us/blog/how-to-clean-robot-vacuum-cleaner"),
            ],
            [
                ("Ako často čistiť robotický vysávač?", "Nádobu kontrolujte po každom upratovaní, filter a kefy podľa zaťaženia domácnosti. Pri zvieratách častejšie."),
                ("Môžem umývať filter vodou?", "Iba ak to povoľuje výrobca konkrétneho filtra. Niektoré filtre sa majú iba vyklepať alebo vymeniť."),
                ("Prečo robot po čistení stále nevysáva dobre?", "Skontrolujte kefu, filter, tesnenie nádoby, nasávací otvor a správne založenie dielov."),
            ],
        ),
        make_article("Ako reštartovať robotický vysávač", "Reštart robotického vysávača pomôže pri zaseknutej aplikácii, nereagujúcom tlačidle, chybe mapy alebo dočasnom výpadku Wi-Fi. Najprv skúste obyčajné vypnutie a zapnutie; obnovenie Wi-Fi alebo továrenský reset používajte až vtedy, keď viete, čo tým stratíte.", "Robotický vysávač reštartujte najprv jemne: zastavte upratovanie, vráťte ho do stanice, podržte hlavné tlačidlo podľa návodu a počkajte na nové spustenie. Reset Wi-Fi alebo továrenské nastavenia robte až po kontrole aplikácie, nabitia, stanice a modelového návodu.", "reštart", [("Robot nereaguje", "softvér alebo batéria môže byť zaseknutá", "vypnúť a znovu zapnúť podľa návodu"), ("Aplikácia nevidí robot", "môže ísť o Wi-Fi alebo cloud", "najprv skontrolovať internet a aplikáciu"), ("Mapa je chybná", "robot stratil orientáciu", "reštartovať a nechať ho začať zo stanice"), ("Po aktualizácii je problém", "aplikácia alebo robot potrebuje obnovu", "reštart pred továrenským resetom")], ["Ukončite aktuálne upratovanie v aplikácii alebo tlačidlom.", "Skontrolujte, či má robot dostatok batérie.", "Vráťte ho do stanice alebo ho položte na rovný povrch.", "Podržte vypínač podľa návodu konkrétneho modelu.", "Počkajte, kým sa robot znovu spustí a ozve sa hlásenie alebo sa rozsvieti kontrolka.", "Až potom riešte Wi-Fi reset, obnovenie mapy alebo továrenské nastavenia."], ["Zamieňať reštart, reset Wi-Fi a továrenské nastavenia.", "Resetovať mapy pri každej drobnej chybe.", "Reštartovať robot s vybitou batériou mimo stanice.", "Ignorovať špinavé senzory a hľadať iba softvérovú chybu.", "Opakovať reset bez kontroly aplikácie a routera."], [("Soft reštart", "Soft reštart je najmiernejší zásah. Pomáha, keď robot nereaguje, aplikácia ukazuje zvláštny stav alebo sa po upratovaní nevie správne ukončiť režim. Väčšinou nezmaže mapu ani pripojenie."), ("Reset Wi-Fi", "Reset Wi-Fi riešte vtedy, keď robot nevidí sieť alebo meníte router. Pri Xiaomi modeloch sa často používa podržanie kombinácie tlačidiel a následné nové pridanie v aplikácii Xiaomi Home."), ("Továrenské nastavenia", "Továrenský reset je posledná možnosť. Môže zmazať mapu, nastavenia miestností, plány a prepojenie s účtom. Pred ním si overte návod k modelu."), ("Kedy reštart nepomôže", "Ak je problém v špinavých senzoroch, zaseknutej kefe, slabom nabíjaní alebo poškodenom kolese, reštart bude iba dočasný. Mechanickú príčinu treba odstrániť fyzicky."), ("Po reštarte upratať okolie", "Po reštarte nechajte robot začať zo stanice, nie z náhodného miesta uprostred bytu. Pomáha to orientácii aj návratu do stanice.")], ["Reštart rieši dočasný stav zariadenia, nie fyzické zanesenie alebo zlú polohu stanice.", "Výrobcovia pri sieťových problémoch často odporúčajú najprv kontrolu Wi-Fi, vzdialenosti od routera a až potom reset pripojenia.", "Pri smart zariadeniach je dôležité rozlišovať lokálnu chybu robota, problém aplikácie a problém domácej siete."], [("Xiaomi: failed to connect to network", "https://www.mi.com/global/support/faq/details/KA-617976/"), ("Xiaomi: connect Mi Robot Vacuum with Mi Home app", "https://www.mi.com/global/support/article/KA-05122/")], [("Vymaže reštart mapu?", "Bežný reštart zvyčajne nie. Továrenský reset alebo niektoré resetovacie postupy už mapu a nastavenia zmazať môžu."), ("Kedy resetovať Wi-Fi?", "Keď meníte router, heslo alebo robot dlhodobo nevie sieť nájsť ani pri dobrej polohe pri routeri."), ("Čo ak robot nereaguje ani po reštarte?", "Skontrolujte nabíjanie, kontakty, kefu, senzory a návod k modelu. Pri chybe hardvéru treba servis.")]),
        make_article("Ako spárovať robotický vysávač Xiaomi", "Robotický vysávač Xiaomi spárujete cez aplikáciu Xiaomi Home tak, že ho dáte do režimu párovania, pripojíte telefón na správnu Wi-Fi, pridáte zariadenie v aplikácii a dokončíte pokyny. Pri problémoch pomáha priblížiť robot k routeru, skontrolovať heslo, región aplikácie a reset Wi-Fi.", "Robotický vysávač Xiaomi spárujete cez Xiaomi Home: pripravte účet, zapnite robot, resetujte Wi-Fi podľa modelu, v aplikácii pridajte robotický vysávač, vyberte domácu Wi-Fi a dokončite párovanie. Ak zlyhá, presuňte robot bližšie k routeru, skontrolujte heslo a skúste Wi-Fi reset znova.", "párovanie Xiaomi", [("Prvé spustenie", "robot ešte nie je v účte", "pridať zariadenie v Xiaomi Home"), ("Nový router", "uložené Wi-Fi už neplatí", "resetovať Wi-Fi a spárovať znovu"), ("Robot je offline", "môže byť slabý signál alebo vypnutie", "skontrolovať napájanie a sieť"), ("Aplikácia ho nenájde", "robot nie je v režime párovania", "podržať tlačidlá podľa návodu")], ["Nainštalujte Xiaomi Home a prihláste sa do správneho účtu.", "Zapnite robotický vysávač a nechajte ho blízko routera.", "Resetujte Wi-Fi podľa návodu modelu, často podržaním kombinácie tlačidiel.", "V aplikácii pridajte nové zariadenie a vyberte robotický vysávač.", "Zadajte heslo domácej Wi-Fi a počkajte na dokončenie párovania.", "Po pridaní pomenujte miestnosti a nechajte robot vytvoriť prvú mapu zo stanice."], ["Párovať robot ďaleko od routera.", "Zadať zlé heslo alebo použiť sieť, ktorú robot nepodporuje.", "Mať aplikáciu v inom účte alebo regióne, než používa domácnosť.", "Resetovať celé zariadenie, keď stačí reset Wi-Fi.", "Začať mapovanie z náhodného miesta mimo stanice."], [("Reset Wi-Fi pri Xiaomi", "Oficiálna podpora Xiaomi pri starších Mi Robot Vacuum postupoch opisuje podržanie tlačidiel na reset Wi-Fi a následné pridanie v aplikácii. Presná kombinácia sa môže líšiť podľa modelu, preto sledujte návod k svojmu vysávaču."), ("Blízkosť routera", "Pri problémoch s pripojením Xiaomi odporúča kontrolu siete a priblíženie zariadenia k routeru. Hrubé steny, skrinky a dokovacia stanica v rohu môžu signál zhoršiť."), ("Správny účet a región", "Ak robot v aplikácii nevidíte, skontrolujte, či ste v správnom účte a či zariadenie nie je už pridané v inom účte člena domácnosti."), ("Prvá mapa po spárovaní", "Po úspešnom párovaní nechajte robot začať zo stanice a prejsť byt systematicky. Nepresúvajte ho počas prvého mapovania."), ("Údržba po spárovaní", f"Ak model mopuje, nastavte vhodné zóny a používajte kompatibilný <a href=\"{REQUIRED_CATEGORY}\">čistič do robotického vysávača</a>.")], ["Párovanie je kombinácia stavu robota, aplikácie, Wi-Fi siete a účtu. Chyba v ktoromkoľvek bode môže vyzerať rovnako: aplikácia jednoducho robot nenájde.", "Oficiálne Xiaomi postupy pri offline alebo sieťových problémoch zdôrazňujú kontrolu napájania, Wi-Fi, vzdialenosti od routera a reset pripojenia.", "Pri smart domácnosti je najbezpečnejšie meniť vždy len jednu premennú: najprv poloha pri routeri, potom heslo, potom Wi-Fi reset a až nakoniec hlbší reset zariadenia."], [("Xiaomi: How does Mi Robot Vacuum connect with Mi Home APP?", "https://www.mi.com/global/support/article/KA-05122/"), ("Xiaomi: robot vacuum fails to connect to the network", "https://www.mi.com/global/support/faq/details/KA-617976/"), ("Xiaomi: Robot Vacuum offline", "https://www.mi.com/global/support/faq/details/KA-617950/")], [("Musí byť robot pri párovaní v stanici?", "Nie vždy, ale je praktické mať ho nabitý a blízko routera. Prvé mapovanie nechajte začať zo stanice."), ("Čo ak Xiaomi Home robot nenájde?", "Resetujte Wi-Fi podľa modelu, priblížte robot k routeru a skontrolujte heslo aj účet."), ("Vymaže reset Wi-Fi mapu?", "Reset Wi-Fi zvyčajne rieši pripojenie. Továrenský reset môže zmazať aj mapy a nastavenia.")]),
        make_article("Ako dlho sa nabíja robotický vysávač", "Robotický vysávač sa bežne nabíja približne niekoľko hodín, ale presný čas závisí od modelu, kapacity batérie, veku batérie, dokovacej stanice a toho, či sa iba dobíja po prerušení upratovania. Pri novom alebo úplne vybitom robote počítajte s dlhším nabitím podľa návodu.", "Robotický vysávač sa najčastejšie nabíja približne 2 až 4 hodiny, no presný čas určuje model a stav batérie. Ak sa robot nenabíja, skontrolujte dokovaciu stanicu, kontakty, polohu robota, zásuvku a to, či batéria nie je príliš vybitá alebo stará.", "nabíjanie", [("Prvé nabitie", "batéria môže potrebovať dlhší štart", "nechajte robot nabiť podľa návodu"), ("Krátke dobitie počas upratovania", "robot sa vráti dokončiť mapu", "nechajte ho v stanici"), ("Nenabíja sa", "kontakty alebo stanica môžu byť špinavé", "utrieť kontakty a skontrolovať zásuvku"), ("Rýchlo sa vybíja", "batéria starne alebo robot bojuje s prekážkami", "skontrolovať batériu, kefu a trasu")], ["Položte dokovaciu stanicu na rovný povrch a zapojte ju do stabilnej zásuvky.", "Skontrolujte, či sa robot správne dotýka kontaktov.", "Pri novom robote nechajte prvé nabitie dobehnúť podľa návodu výrobcu.", "Ak sa nabíjanie prerušuje, utrite kontakty na robote aj stanici.", "Pri rýchlom vybíjaní skontrolujte zamotané kefy, kolesá a príliš členitú trasu.", "Ak batéria po rokoch nevydrží, riešte výmenu podľa modelu."], ["Súdiť batériu podľa jedného prerušeného nabitia.", "Nechať stanicu na koberci alebo v úzkej medzere.", "Ignorovať zaprášené kontakty.", "Presúvať robot počas nabíjania a potom čakať správnu mapu.", "Mopovať s plnou nádržkou a špinavou handričkou, čo zvyšuje záťaž."], [("Bežný čas nabíjania", "Mnohé robotické vysávače sa nabíjajú v rozmedzí niekoľkých hodín. iRobot pri viacerých sériách uvádza ako normálne podmienky nabíjania aspoň dve hodiny. Iné modely môžu potrebovať dlhšie, najmä pri väčšej batérii."), ("Nabíjanie a návrat k upratovaniu", "Niektoré roboty sa pri väčšej ploche vrátia do stanice, dobijú sa a pokračujú. Vtedy nie je cieľom plné nabitie, ale dostatok energie na dokončenie trasy."), ("Kontakty a stanica", "Zaprášené kontakty môžu spôsobiť, že robot v stanici stojí, ale reálne sa nenabíja spoľahlivo. Jemné utretie suchou handričkou je jednoduchý prvý krok."), ("Batéria a údržba", "Ak je kefa plná vlasov alebo sa robot zasekáva, spotrebuje viac energie. Batéria potom pôsobí slabšie, hoci skutočný problém je mechanický odpor."), ("Mopovanie a batéria", "Mopovanie pridáva hmotnosť vody a ďalšiu údržbu. Pri veľkej ploche sledujte, či robot zvláda trasu aj s vodnou nádržkou a či sa vracia do stanice bez problémov.")], ["Nabíjanie lítium-iónových batérií riadi elektronika zariadenia. Používateľ by mal riešiť najmä dobrý kontakt so stanicou, čistotu kontaktov a teplotné podmienky.", "Oficiálne zdroje iRobot uvádzajú pri normálnych podmienkach nabíjanie aspoň dve hodiny pri vybraných sériách a odporúčajú nechávať robot na základni, keď sa nepoužíva.", "Dlhodobá výdrž batérie závisí od cyklov, veku, zaťaženia robota a údržby. Zanesená kefa alebo filter môžu nepriamo zvyšovať spotrebu energie."], [("iRobot: Roomba battery and charging overview", "https://homesupport.irobot.com/s/article/7272"), ("iRobot: Battery maintenance tips", "https://homesupport.irobot.com/s/article/7273"), ("iRobot: Recharge and Resume", "https://homesupport.irobot.com/s/article/27087")], [("Ako dlho sa nabíja nový robotický vysávač?", "Riaďte sa návodom. Pri mnohých modeloch počítajte s niekoľkými hodinami a prvé nabitie neponáhľajte."), ("Môže byť robot stále v nabíjačke?", "Väčšina moderných robotov je navrhnutá na státie v stanici, ale riaďte sa návodom výrobcu."), ("Prečo sa robot nabíja, ale nevydrží?", "Môže ísť o starú batériu, zanesené kefy, ťažký koberec, špinavý filter alebo zlú trasu.")]),
        make_article("Ako funguje robotický vysávač", "Robotický vysávač funguje kombináciou sania, rotačných kief, senzorov, navigácie, batérie, dokovacej stanice a aplikácie. Lepšie modely si vytvoria mapu, plánujú trasu, rozpoznajú prekážky a vrátia sa nabiť; mopovacie modely navyše dávkujú vodu na handričku.", "Robotický vysávač nasáva prach, kefami posúva nečistoty k saciemu otvoru, pomocou senzorov sa orientuje v priestore a podľa typu navigácie jazdí systematicky alebo reaktívne. Dokovacia stanica slúži na nabíjanie a pri niektorých modeloch aj na vyprázdnenie alebo umývanie mopu.", "fungovanie", [("LiDAR model", "meria vzdialenosť laserom", "vhodný pre presnejšie mapovanie"), ("Kamerová navigácia", "orientuje sa podľa vizuálnych prvkov", "potrebuje vhodné svetelné podmienky podľa modelu"), ("Jednoduchý model", "reaguje na prekážky viac priebežne", "lacnejší, ale menej presná mapa"), ("Mopovací model", "kombinuje vodu a handričku", "vyžaduje čistú nádržku a mop")], ["Robot začne zo stanice a zistí polohu v priestore.", "Kefy zhŕňajú prach, chlpy a drobné nečistoty k saciemu otvoru.", "Senzory sledujú steny, prekážky, pády a návrat do stanice.", "Navigácia vytvára trasu alebo reaguje na okolie podľa modelu.", "Po upratovaní alebo pri nízkej batérii sa robot vracia do stanice.", "Mopovací model zároveň dávkuje vodu na handričku a utiera tvrdú podlahu."], ["Myslieť si, že každý robot mapuje rovnako.", "Zakryť alebo znečistiť senzory a očakávať presnú navigáciu.", "Nechať na zemi káble a malé predmety.", "Použiť mop bez čistej handričky.", "Ignorovať, že robot nie je určený na schody a veľké nehody."], [("Sanie a kefy", "Sanie nie je jediné, čo zbiera špinu. Bočné kefy posúvajú nečistoty od stien, hlavná kefa ich zdvíha a sací otvor ich presúva do nádoby. Pri vlasoch rozhoduje tvar kefy a údržba."), ("Senzory", "Robot používa senzory na hrany, prekážky, steny, niekedy kameru alebo LiDAR. Ak sú zaprášené, môže sa zhoršiť mapa aj návrat do stanice."), ("Mapa a trasa", "Presnejšie modely si ukladajú mapu miestností, umožňujú zóny a plánujú trasu. Jednoduchšie modely môžu upratovať menej systematicky, ale v jednoduchom byte stále pomôžu."), ("Dokovacia stanica", "Stanica nie je len nabíjačka. Je to orientačný bod, z ktorého robot začína a ku ktorému sa vracia. Preto potrebuje pevné a viditeľné miesto."), ("Mopovací modul", f"Pri mopovaní je dôležitá handrička, voda a vhodný roztok. Hustý alebo nevhodný prípravok môže zhoršiť dávkovanie; siahnite po produkte určenom na robotické mopovanie, napríklad <a href=\"{REQUIRED_PRODUCT}\">VEVO čistič podláh pre všetky vysávače Ylang Absolute</a>.")], ["Robotické vysávače kombinujú mechaniku a softvér. Mechanická časť zbiera nečistoty, softvér rozhoduje, kadiaľ robot pôjde a kedy sa vráti.", "LiDAR systémy merajú vzdialenosť laserom a pomáhajú vytvárať mapu. Kamerové systémy sa orientujú podľa viditeľných prvkov v miestnosti. Každý prístup má výhody aj obmedzenia.", "Pri mopovaní sa do systému pridáva voda. Tá zvyšuje nároky na údržbu, pretože nádržka, handrička a dávkovanie môžu vytvárať zápach alebo šmuhy, ak sa zanedbajú."], [("iRobot: How does my Robot Navigate?", "https://homesupport.irobot.com/s/article/31056"), ("iRobot: Guide to ClearView LiDAR Maps", "https://homesupport.irobot.com/s/article/20500"), ("iRobot: Guide to Imprint Smart Maps", "https://homesupport.irobot.com/s/article/64102")], [("Funguje robotický vysávač potme?", "Závisí od navigácie. Niektoré LiDAR modely zvládajú slabšie svetlo lepšie, kamerové modely môžu potrebovať viac svetla."), ("Prečo robot vynechá miesto?", "Môže ísť o prekážku, zónu v aplikácii, zlú mapu, nízku batériu alebo znečistené senzory."), ("Je mopovanie skutočné umývanie?", "Je to skôr pravidelná ľahká údržba tvrdých podláh, nie náhrada za ručné čistenie zaschnutých nečistôt.")]),
        make_article("Ako zapnúť robotický vysávač", "Robotický vysávač zapnete hlavne cez tlačidlo na tele, aplikáciu alebo plán upratovania. Pred prvým zapnutím ho nabite, odstráňte prepravné poistky, pripravte dokovaciu stanicu a skontrolujte, či je nádoba, filter, mop alebo nádržka správne založená.", "Robotický vysávač zapnete podržaním alebo stlačením hlavného tlačidla podľa návodu modelu, prípadne cez aplikáciu. Pred prvým štartom ho nabite v stanici, odstráňte ochranné prvky, pripravte podlahu a pri mopovacom režime správne založte nádržku aj mopovaciu handričku.", "zapnutie", [("Prvé zapnutie", "robot môže byť v prepravnom režime", "odstrániť poistky a nabiť"), ("Bežné upratovanie", "robot čaká v stanici", "spustiť tlačidlom alebo aplikáciou"), ("Mopovanie", "treba nádržku a handričku", "naplniť správne a založiť mop"), ("Robot nereaguje", "batéria alebo kontakty môžu byť problém", "skontrolovať nabíjanie")], ["Postavte dokovaciu stanicu na rovný povrch a pripojte ju do zásuvky.", "Položte robot do stanice a nechajte ho nabiť podľa návodu.", "Odstráňte ochranné fólie, prepravné poistky a predmety z podlahy.", "Stlačte hlavné tlačidlo alebo robot spustite v aplikácii.", "Pri prvom mapovaní nechajte robot začať zo stanice a neprenášajte ho.", "Pri mopovaní založte čistú handričku, vhodne naplnenú nádržku a nastavte zakázané zóny pre koberce."], ["Zapnúť robot s vybitou batériou a považovať ho za pokazený.", "Začať prvú jazdu zo stredu miestnosti.", "Nechať káble a šnúrky na zemi.", "Zabudnúť na vodnú nádržku pri mopovaní.", "Spustiť robot po mokrej nehode bez ručného odstránenia špiny."], [("Prvé spustenie", "Pri prvom spustení počítajte s tým, že robot sa učí priestor. Ideálne je pustiť ho zo stanice, nechať otvorené dvere do miestností a odstrániť voľné predmety."), ("Zapnutie tlačidlom", "Väčšina modelov má hlavné tlačidlo na spustenie alebo pozastavenie upratovania. Dlhé podržanie môže znamenať vypnutie alebo reštart, preto si overte návod."), ("Zapnutie cez aplikáciu", "Aplikácia je praktická na zóny, plánovanie a režimy. Ak robot cez aplikáciu nereaguje, najprv skontrolujte, či je online a v stanici."), ("Zapnutie mopovania", f"Mopovanie nespúšťajte so špinavou handričkou. Ak pridávate <a href=\"{REQUIRED_CATEGORY}\">čistič do robotického vysávača</a>, držte sa dávkovania a návodu."), ("Kedy robot nezapínať", "Nespúšťajte robot na veľké kusy jedla, rozliatu lepkavú tekutinu, mokré blato alebo miesto s množstvom káblov. Najprv upracte rizikové prekážky.")], ["Zapnutie robotického vysávača je jednoduché, ale úspešné prvé upratovanie závisí od prípravy priestoru.", "Robot potrebuje stabilný štartovací bod, nabitie a čisté senzory. Ak začne z náhodného miesta, môže si horšie vytvoriť mapu alebo sa nevrátiť do stanice.", "Pri smart modeloch je rozdiel medzi fyzickým zapnutím, spustením upratovania, plánom v aplikácii a obnovením po chybe. Keď jeden spôsob nefunguje, druhý môže pomôcť odlíšiť problém tlačidla od problému aplikácie."], [("iRobot: robot not turn on or charge properly", "https://homesupport.irobot.com/s/article/19952"), ("Xiaomi Robot Vacuum S20+ FAQ", "https://www.mi.com/global/support/faq/details/KA-256971/")], [("Prečo robot nejde zapnúť?", "Skontrolujte nabitie, kontakty, stanicu, hlavný vypínač a návod modelu. Pri úplne vybitej batérii potrebuje čas v stanici."), ("Dá sa spustiť bez aplikácie?", "Väčšina modelov áno, základným tlačidlom. Aplikácia je potrebná na mapy, zóny a plánovanie."), ("Môžem hneď zapnúť mopovanie?", "Áno, ak je nádržka správne založená, mop čistý a podlaha vhodná na vlhké mopovanie.")]),
        make_article("Ako odvápniť robotický vysávač", "Odvápnenie robotického vysávača riešte veľmi opatrne. Väčšina problémov s vodným kameňom sa týka mopovacej nádržky, trysiek alebo dávkovania vody, nie vysávacej časti. Nepoužívajte ocot ani agresívny odvápňovač, ak to výrobca výslovne nepovoľuje.", "Robotický vysávač neodvápňujte ako rýchlovarnú kanvicu. Ak má mopovaciu nádržku a tvrdá voda zhoršuje prietok, najprv nádržku vyprázdnite, vypláchnite čistou vodou, skontrolujte trysky podľa návodu a používajte vhodný čistič do robotického vysávača. Ocot a agresívne odvápňovače nepoužívajte bez súhlasu výrobcu.", "odvápnenie", [("Voda tečie slabo", "môžu byť zvyšky minerálov alebo roztoku", "vypláchnuť nádržku a skontrolovať trysky"), ("Mop robí šmuhy", "špinavý mop alebo nevhodný roztok", "vyprať handričku a znížiť dávku"), ("Nádržka zapácha", "stará voda ostala v robote", "vyprázdniť, vypláchnuť a vysušiť"), ("Tvrdá voda v domácnosti", "minerály môžu zanechať povlak", "nenechávať vodu stáť v nádržke")], ["Vypnite robot a vyberte mopovaciu nádržku podľa návodu.", "Vylejte zvyšnú vodu alebo roztok a nádržku vypláchnite čistou vodou.", "Skontrolujte, či nie sú viditeľne zanesené výstupy vody alebo mopovacia doska.", "Nepoužívajte ocot, kyseliny ani agresívny odvápňovač, ak ich výrobca nepovoľuje.", "Nechajte nádržku vyschnúť a používajte iba vhodné prípravky v rozumnej koncentrácii.", "Ak voda stále netečie, riešte servisný postup konkrétneho modelu."], ["Liať ocot do nádržky bez návodu.", "Použiť silný kúpeľňový odvápňovač.", "Nechať čistič stáť v robote celé týždne.", "Zamieňať vodný kameň so špinavou handričkou.", "Prepichovať trysky ostrým predmetom a poškodiť ich."], [("Čo sa vlastne odvápňuje", "Pri robotickom vysávači sa nerieši vysávací motor, ale mopovací vodný okruh: nádržka, výstupy vody, mopovacia doska a handrička. Ak robot iba vysáva, odvápnenie nie je bežná údržba."), ("Tvrdá voda a nádržka", "Tvrdá voda môže zanechať minerálny povlak, najmä ak stojí v nádržke. Prevencia je jednoduchá: po mopovaní nádržku vyprázdniť a nenechávať roztok dlhodobo v zariadení."), ("Ocot a riziko poškodenia", "Ocot môže byť užitočný v niektorých domácich situáciách, ale v robotickom vysávači môže poškodiť tesnenia, povrchy alebo zanechať pach. Bez návodu výrobcu ho nepoužívajte."), ("Vhodný čistič", f"Ak mopujete pravidelne, používajte prípravok určený na robotické vysávače, napríklad <a href=\"{REQUIRED_PRODUCT}\">VEVO čistič podláh pre všetky vysávače Ylang Absolute</a>. Dôležité je dávkovanie, nie sila vône."), ("Keď odvápnenie nepomáha", "Ak je prietok vody stále slabý, problém môže byť čerpadlo, ventil, elektronika alebo zaschnutá špina. Vtedy je bezpečnejšie pozrieť modelový návod alebo servis.")], ["Výrobcovia robotických mopov upozorňujú najmä na kompatibilitu čistiacich roztokov a údržbu vodnej časti.", "Vodný kameň je chemický problém minerálov, ale robotický vysávač je zároveň elektronické zariadenie s plastmi, tesneniami a malými kanálikmi. Preto sa nedá čistiť rovnakým spôsobom ako sanitárna keramika.", "Prevencia je bezpečnejšia než agresívne odvápňovanie: vyprázdniť nádržku, nenechať stáť starú vodu, prať mop a používať vhodný roztok."], [("ECOVACS: robot vacuum mop not dispensing water", "https://www.ecovacs.com/us/blog/robot-vacuum-mop-not-dispensing-water"), ("ECOVACS: Can you use cleaning solution in a robot mop?", "https://www.ecovacs.com/us/blog/can-you-put-cleaning-solution-in-robot-mop")], [("Môžem dať ocot do robotického vysávača?", "Nie naslepo. Použite ho iba vtedy, ak to výrobca konkrétneho modelu výslovne povoľuje."), ("Ako predísť vodnému kameňu?", "Nenechávajte vodu stáť v nádržke, po mopovaní ju vyprázdnite a používajte vhodný čistiaci roztok."), ("Prečo robot nemopuje rovnomerne?", "Môže byť špinavý mop, zanesené dávkovanie vody, nevhodný roztok alebo nesprávne nastavenie prietoku.")]),
        make_article("Robotický vysávač je offline", "Ak je robotický vysávač offline, najprv skontrolujte, či je zapnutý, nabitý, v dosahu Wi-Fi, či funguje router a či aplikácia používa správny účet. Pri Xiaomi modeloch pomáha priblíženie k routeru, kontrola hesla, reset Wi-Fi a nové spárovanie v Xiaomi Home.", "Robotický vysávač je offline najčastejšie preto, že je vypnutý, vybitý, mimo dosahu Wi-Fi, router má výpadok, zmenilo sa heslo siete alebo aplikácia nie je v správnom účte. Skôr než urobíte továrenský reset, skontrolujte napájanie, stanicu, Wi-Fi signál, router a resetujte iba pripojenie.", "offline stav", [("Robot je vypnutý", "aplikácia ho nemá ako kontaktovať", "zapnúť a nabiť"), ("Slabý Wi-Fi signál", "stanica je ďaleko alebo za stenou", "presunúť bližšie k routeru"), ("Zmenené heslo", "robot má uloženú starú sieť", "reset Wi-Fi a nové párovanie"), ("Nesprávny účet", "robot je v inom účte domácnosti", "skontrolovať prihlásenie")], ["Skontrolujte, či robot svieti, reaguje a má batériu.", "Pozrite, či je router online a internet funguje aj na telefóne.", "Priblížte robot alebo stanicu k routeru, ak je signál slabý.", "Reštartujte aplikáciu a skontrolujte správny účet.", "Ak sa menilo heslo Wi-Fi, resetujte Wi-Fi pripojenie robota podľa návodu.", "Až po týchto krokoch zvažujte hlbší reset alebo servis."], ["Urobiť továrenský reset ako prvý krok.", "Ignorovať, že robot je vybitý alebo mimo stanice.", "Riešiť aplikáciu, keď nefunguje domáci internet.", "Nechať stanicu v skrinke s veľmi slabým signálom.", "Zabudnúť na zmenu hesla alebo routera."], [("Offline po výpadku internetu", "Po výpadku môže chvíľu trvať, kým sa robot znovu pripojí. Najprv overte, či funguje internet a router. Aplikáciu zatvorte a znovu otvorte."), ("Offline po presune stanice", "Ak ste stanicu presunuli za nábytok, do rohu alebo ďalej od routera, signál môže byť slabší. Robot môže upratovať lokálne, ale aplikácia ho nemusí vidieť."), ("Offline pri Xiaomi", "Xiaomi podpora uvádza ako možné príčiny vypnutie robota, slabý alebo nestabilný signál a výpadok Wi-Fi. Pri pripájaní odporúča kontrolovať aj vzdialenosť od routera."), ("Offline a mopovanie", f"Offline stav neznamená, že máte ignorovať fyzickú údržbu. Ak robot mopuje, po upratovaní vyprázdnite nádržku a použite vhodný <a href=\"{REQUIRED_CATEGORY}\">čistič do robotického vysávača</a>."), ("Kedy volať servis", "Ak robot nie je viditeľný ani po resetovaní Wi-Fi, nereaguje na tlačidlá a nenabíja sa, problém môže byť hardvérový.")], ["Offline stav je sieťový alebo napájací problém, nie dôkaz, že robot prestal fyzicky fungovať.", "Smart zariadenie potrebuje tri veci naraz: napájanie, lokálnu Wi-Fi a funkčnú aplikáciu alebo cloudovú službu. Ak zlyhá jedna, v aplikácii vyzerá robot ako offline.", "Pri riešení postupujte od najjednoduchšieho: napájanie, router, vzdialenosť, účet, Wi-Fi reset. Továrenský reset môže vymazať nastavenia a nie vždy vyrieši zlý signál."], [("Xiaomi: Why does Robot Vacuum show offline?", "https://www.mi.com/global/support/faq/details/KA-617950/"), ("Xiaomi: failed to connect to the network", "https://www.mi.com/global/support/faq/details/KA-617976/")], [("Prečo je robotický vysávač offline?", "Najčastejšie pre napájanie, Wi-Fi, slabý signál, zmenené heslo, router alebo nesprávny účet v aplikácii."), ("Pomôže reštart routera?", "Ak internet nefunguje alebo je router zaseknutý, áno. Najprv však overte, či problém nemajú aj iné zariadenia."), ("Musím vymazať robota z aplikácie?", "Nie hneď. Najprv skúste napájanie, signál a reset Wi-Fi. Vymazanie a nové pridanie je až ďalší krok.")]),
        make_article("Kam umiestniť robotický vysávač a kam ho schovať", "Robotický vysávač umiestnite tak, aby mal dokovacia stanica rovný povrch, dobrý prístup, stabilné napájanie a Wi-Fi signál. Schovať ho môžete pod nábytok alebo do výklenku iba vtedy, ak má dostatok priestoru na výjazd, návrat, nabíjanie a vetranie.", "Robotický vysávač najlepšie funguje, keď má dokovacia stanica voľný priestor pred sebou, stabilnú zásuvku, dobrý Wi-Fi signál a nie je natlačená v úzkej skrinke. Schovať ho môžete esteticky, ale nie tak, aby sa zhoršil návrat do stanice, nabíjanie alebo prístup k nádobe a mopu.", "umiestnenie stanice", [("Stanica v rohu", "môže byť málo priestoru na navádzanie", "nechať voľný priestor pred stanicou"), ("Stanica pod skrinkou", "robot sa môže zmestiť, ale zle sa čistí", "overiť výšku a prístup"), ("Slabý Wi-Fi signál", "aplikácia ukazuje offline stav", "neukrývať stanicu za hrubé prekážky"), ("Mopovací robot", "potrebuje manipuláciu s nádržkou", "nechať priestor na vyberanie mopu")], ["Vyberte rovné miesto pri stene a pri zásuvke.", "Nechajte robotu priestor na priamy výjazd zo stanice.", "Skontrolujte Wi-Fi signál v mieste stanice.", "Neumiestňujte stanicu do mokrého kúta ani za ťažký nábytok.", "Ak chcete robot schovať, otestujte aspoň niekoľko návratov do stanice.", "Myslite na vyberanie nádoby, filtra, nádržky a mopovacej handričky."], ["Schovať stanicu tak, že robot nemá priestor na návrat.", "Dať stanicu na voľný koberec, ktorý sa hýbe.", "Umiestniť robot ďaleko od Wi-Fi a potom riešiť offline stav.", "Zabudnúť na prístup k nádobe a mopu.", "Schovať mokrý mopovací robot do uzavretej skrinky bez vysušenia."], [("Koľko priestoru potrebuje stanica", "Presné odporúčanie sa líši podľa výrobcu, ale princíp je rovnaký: robot potrebuje jasný výjazd, stabilnú stenu za stanicou a miesto na navedenie. Príliš úzky výklenok zvyšuje počet neúspešných návratov."), ("Kam schovať robotický vysávač", "Esteticky ho môžete dať pod lavicu, do otvoreného výklenku alebo pod nábytok, ak sa tam bez problémov vracia. Uzavretá skrinka je riziko pre Wi-Fi, vetranie a manipuláciu s nádržkou."), ("Robot pod sedačkou", "Ak je robot nízky a sedačka má dostatočnú výšku, môže pod ňu chodiť upratovať. Dokovaciu stanicu však nedávajte tam, kde sa k nej zle dostanete."), ("Mopovací robot a vlhkosť", f"Pri mopovacom robote myslite na nádržku a handričku. Po použití ich nenechávajte mokré v uzavretom priestore a používajte vhodný <a href=\"{REQUIRED_CATEGORY}\">čistič do robotického vysávača</a>."), ("Káble a zásuvka", "Kábel od stanice upevnite tak, aby ho robot neťahal. Zásuvka má byť stabilná a nie za nábytkom, ktorý sa často posúva.")], ["Stanica je navigačný aj nabíjací bod. Ak je zle umiestnená, problémy sa prejavia ako zlé dokovanie, offline stav, neúplné mapovanie alebo nedokončené upratovanie.", "Výrobcovia pri mapovaní a návrate do stanice predpokladajú, že robot má okolo stanice rozumný priestor. Keď ho skryjete do úzkeho priestoru, znižujete spoľahlivosť celého systému.", "Pri Wi-Fi robotoch je poloha stanice aj sieťová otázka. Slabý signál môže spôsobiť, že robot fyzicky funguje, ale aplikácia ho vidí ako offline."], [("Xiaomi: Robot Vacuum offline", "https://www.mi.com/global/support/faq/details/KA-617950/"), ("iRobot: How does my Robot Navigate?", "https://homesupport.irobot.com/s/article/31056/")], [("Môžem schovať robotický vysávač do skrinky?", "Iba ak je skrinka otvorená, vetraná, má priestor na výjazd a robot sa opakovane spoľahlivo vráti do stanice."), ("Môže byť stanica pod sedačkou?", "Skôr nie, ak sa k nej zle dostanete. Robot môže upratovať pod sedačkou, ale stanica má byť prakticky dostupná."), ("Prečo sa robot nevracia do stanice?", "Stanica môže byť v úzkom mieste, na pohyblivom koberci, bez dobrého kontaktu alebo so slabým signálom.")]),
    ]
    ARTICLES.extend(articles)


def make_article(title, short, answer, theme, decision_rows, steps, mistakes, sections, expert, sources, faq_items):
    intro = [
        f"Téma „{title.lower()}“ vyzerá jednoducho, ale v praxi sa pri nej mieša technika, podlaha, aplikácia, voda, prach a každodenné návyky. Robotický vysávač nie je iba malý vysávač na kolieskach; je to zariadenie, ktoré potrebuje pripravený priestor, čisté senzory a pravidelnú údržbu.",
        f"Pri oblasti {theme} sa oplatí začať od najjednoduchšej kontroly. Veľa problémov nevzniká preto, že je robot pokazený, ale preto, že má slabý signál, špinavú kefu, plnú nádobu, mokrý mop alebo zle umiestnenú stanicu.",
        f"Ak robotický vysávač aj mopuje, pribúda ďalšia vrstva: nádržka, handrička a čistiaci roztok. Preto v každom článku počítame aj s tým, že správna údržba podlahy znamená vhodný prípravok, nie náhodnú chémiu naliatu do nádržky.",
        "Najlepší postup je pokojný a postupný. Najprv skontrolovať fyzický stav robota, potom aplikáciu a sieť, následne nastavenia mapy a až na konci robiť tvrdšie zásahy, ktoré môžu vymazať nastavenia alebo mapu.",
    ]
    check_rows = [
        ("Dokovacia stanica", "je základ návratu, nabíjania a mapy", "musí byť stabilná a dostupná"),
        ("Kefa a filter", "ovplyvňujú výkon aj hluk", "pri vlasoch a chlpoch kontrolovať častejšie"),
        ("Senzory", "pomáhajú orientácii", "utierať jemne a bez agresívnej chémie"),
        ("Mop a nádržka", "môžu tvoriť zápach alebo šmuhy", "po použití vyprázdniť a vysušiť"),
        ("Aplikácia a Wi-Fi", "ovplyvňujú mapu, plán a diaľkové ovládanie", "pri probléme overiť signál a účet"),
    ]
    rule = [
        "Pri robotickom vysávači vždy oddeľte technický problém, sieťový problém a bežnú údržbu.",
        "Ak model mopuje, čistá handrička a vhodný prípravok sú rovnako dôležité ako samotné sanie.",
    ]
    related = [
        ("Ako vyčistiť pelech pre psa alebo mačku, aby nezapáchal", "/n/ako-vycistit-pelech-pre-psa-alebo-macku-aby-nezapachal"),
    ]
    return {
        "title": title,
        "short": short,
        "answer": answer,
        "quick": [
            "<strong>Začnite stavom robota:</strong> batéria, nádoba, filter, kefa a senzory sú prvá kontrola.",
            "<strong>Potom riešte priestor:</strong> káble, prahy, stanica, Wi-Fi a prekážky často vysvetlia problém.",
            "<strong>Mopovanie berte ako samostatný režim:</strong> nádržka, handrička a čistič potrebujú vlastnú údržbu.",
            "<strong>Nerobte tvrdý reset bez dôvodu:</strong> najprv skúste jednoduchšie kroky.",
            "<strong>Pri čistiacom roztoku buďte opatrní:</strong> používajte iba vhodné produkty a neprelievajte nádržku.",
        ],
        "intro": intro,
        "decision_heading": f"Rozhodovanie pri téme: {theme}",
        "decision_rows": [(esc(a), esc(b), c) for a, b, c in decision_rows],
        "steps": steps,
        "mistakes": mistakes,
        "check_rows": [(esc(a), esc(b), esc(c)) for a, b, c in check_rows],
        "sections": sections,
        "rule": rule,
        "expert": expert,
        "sources": sources,
        "related": related,
        "faq": faq_items,
    }


def add_remaining_articles():
    add_more_articles()


def link_status(url):
    try:
        response = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0 Codex VEVO content preflight"})
        return {"url": url, "status": response.status_code, "final_url": response.url, "ok": 200 <= response.status_code < 400}
    except Exception as exc:  # pragma: no cover
        return {"url": url, "status": None, "final_url": None, "ok": False, "error": f"{type(exc).__name__}: {exc}"}


def visible_word_count(markup):
    text = re.sub(r"<[^>]+>", " ", markup)
    text = html.unescape(re.sub(r"\s+", " ", text)).strip()
    return len(re.findall(r"[A-Za-zÀ-ž0-9]+(?:[-'][A-Za-zÀ-ž0-9]+)?", text))


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
    OUT_XLS.parent.mkdir(parents=True, exist_ok=True)
    workbook.save(str(OUT_XLS))
    return str(OUT_XLS)


def main():
    add_remaining_articles()
    articles = []
    for index, article in enumerate(ARTICLES):
        long_html = render_article(article)
        public_blob = "\n".join([article["title"], article["short"], long_html])
        if FORBIDDEN_PUBLIC_RE.search(public_blob):
            raise SystemExit(f"Forbidden public wording in article: {article['title']}")
        if "Cena:" in public_blob or "€" in public_blob or "&euro;" in public_blob:
            raise SystemExit(f"Fixed price found in article: {article['title']}")
        if REQUIRED_CATEGORY not in long_html or REQUIRED_PRODUCT not in long_html:
            raise SystemExit(f"Required robot-vacuum links missing: {article['title']}")
        articles.append(
            {
                "title": article["title"],
                "short": article["short"],
                "long": long_html,
                "date_posted": BATCH_DATE,
                "time_posted": f"{8 + (index * 6) // 60:02d}:{(index * 6) % 60:02d}:00",
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
    bad_links = [item for item in checks if not item["ok"]]

    preflight = {
        "batch": BATCH,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "article_count": len(articles),
        "date_posted": BATCH_DATE,
        "required_category": REQUIRED_CATEGORY,
        "required_product": REQUIRED_PRODUCT,
        "all_links_ok": not bad_links,
        "bad_links": bad_links,
        "link_checks": checks,
        "articles": [
            {
                "title": item["title"],
                "slug": item["link"],
                "words": visible_word_count(item["long"]),
                "h2_count": item["long"].count("<h2"),
                "table_count": item["long"].count("<table"),
                "styled_block_count": item["long"].count("border-radius"),
                "required_category_present": REQUIRED_CATEGORY in item["long"],
                "required_product_present": REQUIRED_PRODUCT in item["long"],
                "product_link_count": item["long"].count("/p-") + item["long"].count("https://www.vevo.sk/p-"),
                "category_link_count": item["long"].count("/c/") + item["long"].count("https://www.vevo.sk/c/"),
            }
            for item in articles
        ],
    }
    if bad_links:
        OUT_PREFLIGHT.parent.mkdir(parents=True, exist_ok=True)
        OUT_PREFLIGHT.write_text(json.dumps(preflight, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        raise SystemExit(json.dumps({"bad_links": bad_links}, ensure_ascii=False, indent=2))

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_PREFLIGHT.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(articles, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    OUT_PREFLIGHT.write_text(json.dumps(preflight, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    xls_path = write_xls(articles)
    print(json.dumps({"json": str(OUT_JSON), "preflight": str(OUT_PREFLIGHT), "xls": xls_path, "article_count": len(articles)}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
