import json
import re
import unicodedata
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from urllib.parse import urljoin

import requests
import xlwt


BASE = "https://www.vevo.sk"
BATCH = "batch-28"
BATCH_DATE = "2025-09-24"
OUT_JSON = Path("content/VEVO_CONTENT/imports/batch-28-2026-06-17-articles.json")
OUT_PREFLIGHT = Path("content/VEVO_CONTENT/exports/batch-28-2026-06-17-preflight.json")
OUT_XLS = Path.home() / "AppData/Local/Temp/vevo-batch-28-fragrance-laundry-clean-urls.xls"

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
<h2 style="margin-top: 0;">{title}</h2>
<ul>{items}</ul>
</div>
""".strip()


def practical_box(items):
    rows = "".join(f"<li>{item}</li>" for item in items)
    return f"""
<div style="border: 1px solid #d7e2ec; border-radius: 8px; padding: 18px; margin: 22px 0; background: #f7fbff;">
<h2 style="margin-top: 0;">Riešite jednu z týchto situácií?</h2>
<p>Nižšie nájdete praktické odpovede pre bežné domáce prípady, kde vôňa súvisí s praním, sušením, materiálom alebo citlivosťou pokožky.</p>
<ul>{rows}</ul>
</div>
""".strip()


def source_box(items):
    rows = "".join(f'<li><a rel="noopener" href="{href}" target="_blank">{escape(label)}</a></li>' for label, href in items)
    return f"""
<div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; margin: 22px 0; background: #fbfbfb;">
<h2 style="margin-top: 0;">Zdroje a odborný kontext</h2>
<p>Odkazy nižšie používame ako širší odborný rámec. Pri citlivej pokožke alebo výrazných ťažkostiach nenahrádzajú individuálne odporúčanie lekára.</p>
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


ARTICLES = [
    {
        "title": "Ako vybrať vôňu do prania na zimu: deky, svetre, šály a sezónne textílie",
        "short": "Zimnú vôňu do prania vyberajte jemnejšie než vôňu do bytu. Deka, sveter, šál alebo termo vrstva musia byť najprv dobre vyprané, opláchnuté a vysušené. Parfum do prania má pridať čistý sezónny dojem, nie prekryť zatuchnutie po skladovaní alebo vlhkosť zo sušenia v byte.",
        "situations": [
            "deka po zime vonia zatuchnuto, hoci bola uložená v skrini",
            "sveter je čistý, ale po vysušení mu chýba svieži dojem",
            "šály a čiapky cítia parfum príliš silno pri tvári",
            "termo oblečenie po lyžovaní potrebuje najprv odstrániť pot",
            "posteľné prádlo v zime schne v byte a vôňa sa rýchlo stratí",
            "sezónne textílie idú na návštevu, chatu alebo do hosťovskej izby",
            "chcete vybrať vôňu bez slepého nákupu celej fľaše",
            "potrebujete rozlíšiť útulnú vôňu od ťažkej parfumácie",
        ],
        "quick": [
            "<strong>Na zimu voľte skôr mäkké a čisté vône.</strong> Pri dekách, svetroch a šáloch je dôležitejšia príjemnosť pri dlhom kontakte než prvý silný dojem.",
            "<strong>Zatuchnutie riešte praním a sušením.</strong> Ak textil schne pomaly alebo bol uložený vlhký, parfum do prania problém nezachráni.",
            "<strong>Pri šáloch a čiapkach dávkujte opatrne.</strong> Textil je blízko nosa a tváre, preto sa intenzita vníma silnejšie.",
            "<strong>Pri funkčných zimných vrstvách rešpektujte štítok.</strong> Niektoré materiály nemajú rady aviváž ani zbytočné nánosy.",
            "<strong>Ak si nie ste istí vôňou, začnite vzorkou.</strong> Zimné textílie často nosíte dlhšie, takže príliš výrazná vôňa môže rušiť.",
        ],
        "intro": [
            "Zimné pranie má inú logiku než letné tričká. Deka, sveter, šál, termo tričko, fleecová mikina alebo obliečky do chladných večerov sú textílie, ktoré sú pri tele dlhšie, často schnú v interiéri a ľahko držia pach z vlhkosti, skrine alebo potu. Preto pri nich vôňa do prania funguje najlepšie až ako posledný krok po správnom praní.",
            "Najčastejšia chyba je vybrať si vôňu podľa toho, čo krásne vonia z fľaše, a potom ju použiť rovnako na všetko. Zimná bielizeň potrebuje pokojnejšiu stratégiu. Pri dekách a posteľnej bielizni môže byť vôňa útulnejšia, pri šáloch a čiapkach skôr jemná, pri športových zimných vrstvách veľmi opatrná.",
            "Ak riešite otázku ako prevoňať deku, ako vyprať šál, ako prať sveter, ako dosiahnuť vôňu zimnej bielizne alebo prečo bielizeň v zime zatuchne, začnite príčinou. V zime je problémom najmä pomalé sušenie, slabé vetranie, preplnený bubon a uloženie textilu skôr, než je úplne suchý.",
            "Dobrá zimná vôňa má robiť dojem čistoty a tepla, nie ťažkého parfumu. Pri návšteve, chate alebo hosťovskej posteli je lepšie, keď vôňu človek jemne zaregistruje, ale po pár minútach ho neruší.",
        ],
        "why": [
            "Zimné textílie sú často objemné a savé. Deka alebo hrubší sveter držia viac vody než tenké tričko, takže po praní potrebujú dlhší čas na sušenie. Ak ostanú vlhké v strede, vôňa sa môže zmeniť na zatuchnutý dojem, hoci ste použili kvalitný produkt.",
            "Ďalší rozdiel je vzdialenosť od nosa. Šál, nákrčník, čiapka alebo rolák sú priamo pri tvári. Vôňa, ktorá je na obliečkach príjemná, môže byť na šále príliš intenzívna. Preto dávkovanie neprenášajte automaticky z jednej kategórie textilu na druhú.",
            "Funkčné zimné vrstvy majú špecifické vlákna a úpravy. Pri termo oblečení, softshelle, membránach alebo elastických športových kusoch je dôležité nepoškodiť priedušnosť a pružnosť. Vôňa je až doplnok po tom, čo je textil správne vypraný podľa štítku.",
            "Pri sezónnom skladovaní sa pridáva ešte jeden problém: textil môže nasať pach skrine, pivnice, plastového boxu alebo vlhkosti. Vtedy nepomáha pridať viac parfumácie. Najprv treba textil prevetrať, vyprať, úplne vysušiť a až potom uložiť do čistého priestoru.",
        ],
        "rows": [
            ("Deka a pléd", "mäkká, útulná vôňa v nízkej až strednej intenzite", "Dôležité je úplné preschnutie, inak sa vôňa zmení na zatuchnutý dojem."),
            ("Vlnený alebo kašmírový sveter", "veľmi jemná vôňa alebo radšej bez parfumácie", "Rešpektujte štítok a pri citlivých vláknach neexperimentujte agresívne."),
            ("Šál, čiapka, nákrčník", "jemná čistá vôňa v malej dávke", "Textil je pri tvári, takže intenzita pôsobí silnejšie."),
            ("Termo tričko a lyžiarske vrstvy", "najprv odstrániť pot, vôňu používať opatrne", "Funkčné materiály potrebujú šetrný prací režim bez zbytočných nánosov."),
            ("Zimné obliečky", "čistá a pokojná vôňa", "Pozor na pomalé sušenie v byte a preplnený bubon."),
        ],
        "steps": [
            "Rozdeľte zimné textílie podľa materiálu: deky, svetre, šály, termo vrstvy a posteľná bielizeň nepatria vždy do jednej dávky.",
            "Skontrolujte štítok. Pri vlne, kašmíre, membráne alebo elastických športových kusoch je štítok dôležitejší než bežná domáca rutina.",
            "Nepreplňte bubon. Objemná deka potrebuje vodu, pohyb a oplach. Ak je natlačená v práčke, vôňa sa neroznesie rovnomerne.",
            "Použite primeranú dávku pracieho prostriedku a dôkladný oplach. Zvyšky gélu môžu spôsobiť ťažký pocit aj horšie držanie vône.",
            "Parfum do prania dávkujte nižšie pri textíliách pri tvári a vyššie iba tam, kde to neprekáža kontaktu ani materiálu.",
            "Sušte s priestorom a vetrajte. V zime radšej predĺžte sušenie než uložiť textil napoly vlhký.",
            "Pred uložením do skrine nechajte textil úplne vychladnúť a preschnúť. Zatvorený box alebo bielizník zvýrazní každú zvyškovú vlhkosť.",
        ],
        "decision_rows": [
            ("Vôňa je po vysušení slabá", "skontrolovať sušenie a nepreplnený bubon", "Pri pomalom sušení vôňa často ustúpi zatuchnutému dojmu."),
            ("Vôňa je pri šále príliš silná", "znížiť dávku alebo vynechať parfumáciu", "Textil je blízko nosa a pokožky tváre."),
            ("Deka vonia zatuchnuto", "vyprať znovu v menšej dávke a sušiť dlhšie", "Problém je vlhkosť alebo skladovanie, nie nedostatok vône."),
            ("Sveter stratil tvar", "ďalšie pranie nerobiť naslepo", "Pri citlivých vláknach treba rešpektovať materiál a prípadne zvoliť ručný režim."),
            ("Termo oblečenie zapácha potom", "riešiť pot a prací režim, nie silnú vôňu", "Parfumácia nemá zakryť zvyšky potu vo vláknach."),
        ],
        "mistakes": [
            "Použiť rovnakú intenzitu vône na deku aj šál pri tvári.",
            "Prevoňať textil, ktorý je zatuchnutý zo skrine alebo nedosušený.",
            "Prať objemné zimné kusy v preplnenom bubne.",
            "Ignorovať štítok pri vlne, kašmíre, membráne alebo elastických termo vrstvách.",
            "Uložiť zimnú bielizeň do plastového boxu, kým ešte nie je úplne suchá.",
            "Miešať parfum do prania s agresívnou rutinou iba preto, aby bola vôňa silnejšia.",
        ],
        "detail_sections": [
            ("Ako prevoňať deku na zimu", "Deku perte samostatne alebo v malej dávke, aby mala priestor na pohyb. Po praní ju dôkladne pretrepte a sušte tak, aby vzduch prúdil aj cez hrubšie miesta. Ak ju dávate na posteľ alebo gauč, vôňa má byť mäkká a pokojná. Pri deke sa oplatí skôr nižšia intenzita, pretože sa s ňou človek dotýka dlhšie než s bežným oblečením."),
            ("Ako prať šál, čiapku a nákrčník", "Tieto textílie sú blízko tváre, vlasov a krku, takže zachytávajú parfum, pot, kožný maz aj kozmetiku. Pri praní ich otočte naruby, perte s podobne jemnými kusmi a vôňu dávkujte opatrne. Ak má niekto citlivú pokožku, je rozumnejšie zvoliť jemný prací základ a vôňu výrazne obmedziť."),
            ("Ako narábať so svetrami a citlivými vláknami", "Vlna, kašmír, mohér alebo zmesi s elastanom potrebujú opatrnosť. Silná parfumácia nie je náhradou za šetrný program. Pri svetri je kľúčové nízke trenie, vhodná teplota, jemné odstreďovanie a sušenie v tvare. Ak sveter zapácha po nosení, často pomôže vyvetranie a až potom pranie podľa štítku."),
            ("Ako skladovať sezónne textílie", "Pred uložením musí byť textil čistý a úplne suchý. Bielizník, box alebo skriňa musia byť suché a vetrané. Ak sa do uzavretého priestoru vloží textil s vlhkosťou, vôňa sa rýchlo stratí a nahradí ju zatuchnutý pach. Skladovanie je preto súčasť výsledku, nie až posledný detail."),
        ],
        "caution": [
            "Parfum do prania nepoužívajte ako prvé riešenie pri zatuchnutí, plesnivom pachu alebo vlhkom textile. Najprv odstráňte príčinu: nedosušenie, špinavú práčku, zanesený zásobník, preplnený bubon alebo nevetranú skriňu.",
            "Pri deťoch, alergikoch, astmatikoch alebo ľuďoch s podráždenou pokožkou začnite veľmi mierne. Ak sa objaví svrbenie, začervenanie alebo nepríjemný pocit, parfumáciu vynechajte a sledujte aj prací prostriedok, oplach a materiál.",
        ],
        "expert": [
            "Z odborného pohľadu je zimná vôňa bielizne výsledkom procesu, nie iba produktu. Pranie odstráni časť nečistôt, oplach zníži zvyšky a sušenie rozhodne, či sa čistý dojem udrží. Pri objemných textíliách je práve sušenie najčastejší slabý bod.",
            "Vonné látky sú prchavé, preto ich vnímame vo vzduchu. Pri textíliách pri tvári sa vôňa dostáva k nosu veľmi blízko, a preto sa aj nižšia dávka môže javiť ako silná. To je dôvod, prečo šál alebo čiapka potrebujú opatrnejší prístup než pléd na gauči.",
            "Pri citlivých materiáloch treba myslieť aj na mechaniku. Jemné vlákno môže viac poškodiť nevhodný program, trenie alebo sušenie než samotná vôňa. Preto je správny prací režim vždy nadradený parfumácii.",
        ],
        "sources": [
            ("Energy.gov: Laundry tips", "https://www.energy.gov/energysaver/laundry"),
            ("DermNet: Fragrance allergy", "https://dermnetnz.org/topics/fragrance-allergy"),
            ("US EPA: A Brief Guide to Mold, Moisture and Your Home", "https://www.epa.gov/mold/brief-guide-mold-moisture-and-your-home"),
        ],
        "sales": {
            "heading": "Riešenie pre výber zimnej vône bez slepého nákupu",
            "intro": "Pri zimných textíliách je rozumné najprv otestovať, ako vôňa pôsobí na deke, obliečkach alebo šále po reálnom vysušení. Vôňa z fľaše a vôňa na textile po dni používania nemusia byť rovnaké.",
            "product_name": "Sada vzoriek najpredávanejších vôní VEVO 3 x 10ml",
            "product_href": "/p-1574/sada-vzoriek-najpredavanejsich-voni-vevo-3-x-10ml",
            "fit": "keď chcete vybrať zimnú vôňu na deky, obliečky alebo sezónne textílie bez toho, aby ste hneď kupovali veľké balenie.",
            "boundary": "ak textil zapácha po vlhkosti, najprv riešte pranie, oplach, sušenie a skladovanie. Vzorka má pomôcť vybrať vôňu, nie prekryť zatuchnutie.",
            "product_button": "Vyskúšať vzorky vôní",
            "category_title": "Vyberte vôňu podľa textilu a intenzity",
            "category_intro": "V kategórii parfumov do prania porovnávajte vône podľa toho, či ich chcete na deky, obliečky, bežné oblečenie alebo jemnejšie textílie pri tvári.",
            "category_bullets": [
                ("Deka a obliečky", "môžu zniesť mäkký sezónny tón, ak sú dobre vysušené."),
                ("Šál a čiapka", "potrebujú nižšiu intenzitu, pretože sú pri tvári."),
                ("Funkčné oblečenie", "najprv riešte pot a štítok, vôňu dávkujte opatrne."),
            ],
            "category_href": "/c/vevo-fragrance/parfum-do-prania",
            "category_button": "Pozrieť parfumy do prania",
        },
        "related": [
            ("Ako prať vlnený sveter, keď zapácha po nosení", "/n/ako-prat-vlneny-sveter-ked-zapacha-po-noseni"),
            ("Ako prať kašmírový sveter doma bez zrazenia a žmolkov", "/n/ako-prat-kasmirovy-sveter-doma-bez-zrazenia-a-zmolkov"),
            ("Ako prať kuklu, nákrčník a termo čiapku po lyžovaní", "/n/ako-prat-kuklu-nakrcnik-a-termo-ciapku-po-lyzovani"),
            ("Ako odstrániť soľné mapy z nohavíc a kabáta po zime", "/n/ako-odstranit-solne-mapy-z-nohavic-a-kabata-po-zime"),
            ("Parfum do prania: čo to je a ako funguje", "/n/parfum-do-prania-co-to-je-a-ako-funguje"),
        ],
        "faq": [
            ("Aká vôňa do prania je najlepšia na zimu?", "Najčastejšie fungujú mäkké, čisté a menej ostré vône. Pri zimných textíliách je dôležité, aby vôňa nerušila pri dlhom kontakte."),
            ("Ako prevoňať deku, aby nezapáchala zatuchnuto?", "Najprv ju vyperte v dostatočne voľnej dávke a úplne vysušte. Vôňu pridávajte až vtedy, keď je problémom iba slabý čistý dojem, nie vlhkosť."),
            ("Môžem parfum do prania použiť na sveter?", "Iba ak to materiál a štítok dovoľujú. Pri vlne a kašmíre postupujte veľmi opatrne a často stačí jemné pranie bez výraznej parfumácie."),
            ("Prečo šál vonia silnejšie než obliečky?", "Je priamo pri nose a tvári, preto rovnaká dávka pôsobí intenzívnejšie. Pri šáloch dávku znížte."),
            ("Ako skladovať zimné textílie, aby voňali?", "Ukladajte ich čisté, úplne suché a do suchého priestoru. Ak je bielizník vlhký, vôňa sa rýchlo zmení na zatuchnutý pach."),
        ],
    },
    {
        "title": "Ako prevoňať bielizeň v malej kúpeľni: vlhkosť, sušenie a jemná vôňa bez zatuchnutia",
        "short": "V malej kúpeľni je základom najprv zvládnuť vlhkosť. Bielizeň potrebuje priestor, vetranie a rýchle preschnutie; až potom má zmysel jemná vôňa do prania. Ak textil schne pomaly, uteráky ostávajú vlhké alebo kúpeľňa nevetrá, parfumácia bude skôr maskovať problém než vytvárať čistý dojem.",
        "situations": [
            "bielizeň schne v malej kúpeľni a po dni vonia zatuchnuto",
            "uteráky sú čisté, ale po vysušení majú vlhký pach",
            "sušiak stojí pri vani, sprche alebo radiátore bez prúdenia vzduchu",
            "vôňa z prania je prvé hodiny príjemná, potom zmizne",
            "kúpeľňa nemá okno alebo sa v nej zráža para",
            "oblečenie sa odkladá do skrine ešte mierne vlhké",
            "chcete použiť vôňu jemne, aby malý priestor nepôsobil ťažko",
            "hľadáte rozdiel medzi čistým textilom a prevoňaním vzduchu",
        ],
        "quick": [
            "<strong>Najprv riešte vlhkosť.</strong> Malá kúpeľňa potrebuje vetranie, prúdenie vzduchu a rozostupy medzi kusmi bielizne.",
            "<strong>Nevešajte textil nahusto.</strong> Kusy, ktoré sa dotýkajú, schnú pomaly a vôňa sa môže zmeniť na zatuchnutý dojem.",
            "<strong>Uteráky perte a sušte samostatne.</strong> Froté drží vlhkosť aj zvyšky pracieho prostriedku viac než tričká.",
            "<strong>Vôňu dávkujte nižšie.</strong> Malý priestor zosilní parfumáciu, najmä ak je kúpeľňa nevetraná.",
            "<strong>Do skrine ukladajte iba úplne suchú bielizeň.</strong> Aj mierna vlhkosť vie pokaziť výsledok celého prania.",
        ],
        "intro": [
            "Malá kúpeľňa je náročné miesto na sušenie bielizne. Para zo sprchy, slabé vetranie, malý sušiak, mokré uteráky a málo priestoru spôsobia, že textil schne pomaly. Vtedy ani dobrý prací prostriedok a pekná vôňa nemusia vydržať. Bielizeň sa síce vyperie, ale počas sušenia získa vlhký alebo zatuchnutý tón.",
            "Pri otázke ako prevoňať bielizeň v malej kúpeľni je dôležité rozlíšiť dve veci: vôňu textilu a vôňu priestoru. Parfum do prania sa viaže na bielizeň, interiérová vôňa pracuje s miestnosťou. Ak je však kúpeľňa vlhká, ani jedno nerieši príčinu samo osebe.",
            "Najväčší rozdiel robí rýchlosť preschnutia. Tričko zavesené voľne pri prúdení vzduchu vonia inak než rovnaké tričko natlačené medzi uterákmi v zavretej kúpeľni. Preto treba vôňu chápať ako záverečný detail po dobrom praní, oplachu a sušení.",
            "Tento postup je vhodný pre byty bez balkóna, malé kúpeľne bez okna, domácnosti so sušiakom v kúpeľni aj pre situácie, keď nechcete silnú parfumáciu, ale čistý a svieži dojem z bielizne.",
        ],
        "why": [
            "Vlhkosť spomaľuje odparovanie vody z textilu. Ak je vzduch v kúpeľni už nasýtený parou, bielizeň schne pomalšie a dlhšie ostáva vo fáze, kde sa pachy ľahko rozvíjajú. Zatuchnutie potom nevzniká v práčke, ale až po vypraní.",
            "Uteráky sú v malej kúpeľni najčastejší problém. Sú hrubé, savé a často sa používajú opakovane. Ak sa sušia prehodené cez háčik alebo natlačené pri stene, ich vnútorné vrstvy ostávajú vlhké dlhšie než povrch.",
            "Preplnený sušiak funguje podobne ako preplnená práčka. Textil sa síce zmestí, ale nemá priestor. Vzduch sa nedostane medzi kusy, voda sa odparuje pomaly a vôňa z prania nemá šancu pôsobiť čisto.",
            "Malý priestor tiež zosilňuje intenzitu vôní. To, čo je v otvorenej izbe jemné, môže byť v nevetranej kúpeľni ťažké. Preto je lepšia nižšia dávka a pravidelné vetranie než snaha prevoňať kúpeľňu silnejšie.",
        ],
        "rows": [
            ("Tričká a ľahké kúsky", "vešať s rozostupom, nechať prúdiť vzduch", "Schnú rýchlo, ak sa nedotýkajú."),
            ("Uteráky", "prať v menšej dávke a sušiť rozložené", "Froté drží vodu a potrebuje priestor."),
            ("Spodná bielizeň", "sušiť v čistej zóne mimo mokrých uterákov", "Malé kusy ľahko nasajú vlhký pach z okolia."),
            ("Obliečky", "ak sa zmestia, sušiť mimo najvlhkejšej časti kúpeľne", "Veľké plochy sa pri zložení prekrývajú a schnú nerovnomerne."),
            ("Športové oblečenie", "najprv odstrániť pot, sušiť rýchlo", "Syntetika vie držať pach, keď schne pomaly."),
        ],
        "steps": [
            "Po praní bielizeň hneď vyberte z práčky. Nenechávajte ju stáť v zatvorenom bubne, najmä ak je kúpeľňa vlhká.",
            "Sušiak nepreplňte. Medzi kusmi nechajte medzeru a hrubé kusy nedávajte cez seba.",
            "Uteráky pretrepte a rozložte. Ak ich zavesíte zložené, vnútorná časť bude schnúť príliš dlho.",
            "Po sprchovaní krátko vetrajte alebo zapnite ventilátor. Sušenie bielizne tesne po horúcej sprche je slabý začiatok.",
            "Ak používate parfum do prania, začnite nižšou dávkou. Malá kúpeľňa vôňu zosilní.",
            "Pred uložením do skrine skontrolujte hrubšie miesta: pásy, lemy, kapucne, rohy uterákov a švy.",
            "Ak sa zatuchnutie opakuje, vyčistite práčku a skontrolujte kúpeľňu ako priestor, nie iba prací produkt.",
        ],
        "decision_rows": [
            ("Bielizeň vonia po praní, ale po dni zatuchne", "zrýchliť sušenie a vetrať", "Problém vzniká po praní, nie v dávke vône."),
            ("Uteráky sú tvrdé a vlhko páchnu", "menej pracieho prostriedku, viac oplachu, lepšie sušenie", "Froté drží zvyšky aj vodu."),
            ("Kúpeľňa nemá okno", "použiť ventilátor, otvorené dvere alebo presun sušiaka", "Bez výmeny vzduchu sa vlhkosť drží dlho."),
            ("Vôňa je v kúpeľni ťažká", "znížiť dávku a vetrať", "Malý priestor znásobí intenzitu."),
            ("Bielizeň v skrini zatuchne", "ukladať iba úplne suchú a riešiť bielizník", "Zvyšková vlhkosť sa v uzavretom priestore zvýrazní."),
        ],
        "mistakes": [
            "Použiť viac vône, keď je skutočný problém pomalé sušenie.",
            "Vešať mokré uteráky cez seba alebo na háčik bez rozloženia.",
            "Sušiť bielizeň hneď po sprchovaní v zavretej kúpeľni.",
            "Ukladať bielizeň do skrine, keď lemy alebo hrubé švy ešte nie sú suché.",
            "Prať uteráky s tenkými tričkami a očakávať rovnaké sušenie.",
            "Zamieňať vôňu do prania za riešenie vlhkosti v miestnosti.",
        ],
        "detail_sections": [
            ("Ako sušiť bielizeň v malej kúpeľni", "Najdôležitejšie je vytvoriť medzi textíliami priestor. Ak máte skladací sušiak, nerozťahujte naň celú dávku z veľkej práčky. Radšej sušte menej kusov naraz alebo časť presuňte do chodby. Hrubé kusy dávajte na vonkajšie priečky, aby okolo nich prúdil vzduch. Po hodine ich otočte alebo preložte, ak sú na jednej strane stále mokré."),
            ("Ako prevoňať uteráky bez zatuchnutia", "Uteráky perte v menšej dávke a nepoužívajte zbytočne veľa pracieho prostriedku. Po praní ich pretrepte, rozložte a nenechávajte ich schúlené. Parfum do prania má zmysel až vtedy, keď uterák dobre schne a nezostáva vlhký v strede. Ak uterák po jednom použití zapácha, riešte aj spôsob vešania po sprche."),
            ("Ako rozlíšiť pach z práčky a pach z kúpeľne", "Ak bielizeň zapácha hneď po otvorení práčky, problém môže byť v bubne, tesnení, filtri alebo dávkovaní. Ak vonia čisto a zatuchne až počas sušenia, hľadajte príčinu v kúpeľni, sušiaku a vetraní. Toto rozlíšenie šetrí čas, pretože nebudete pridávať vôňu tam, kde treba riešiť vlhký vzduch."),
            ("Kedy pomôže jemná vôňa", "Jemná vôňa pomôže vtedy, keď je bielizeň skutočne čistá a suchá, ale chcete príjemnejší pocit pri obliekaní alebo ukladaní do skrine. V malej kúpeľni začnite nižšou dávkou a sledujte výsledok po vysušení, nie iba po vybratí z práčky."),
        ],
        "caution": [
            "Ak kúpeľňa dlhodobo zapácha po vlhkosti, vidíte mapy na stenách alebo sa tvorí pleseň, vôňa do prania ani interiérová vôňa nie sú riešenie. Najprv treba odstrániť zdroj vlhkosti a zlepšiť vetranie.",
            "Pri alergikoch, astmatikoch, malých deťoch alebo citlivej pokožke používajte v malom priestore nižšiu intenzitu. Ak vôňa spôsobuje bolesť hlavy, škrabanie v nose alebo podráždenie, znížte dávku alebo ju vynechajte.",
        ],
        "expert": [
            "EPA pri téme vlhkosti v domácnosti zdôrazňuje kontrolu zdroja vlhkosti a sušenie mokrých materiálov. Pri bielizni to znamená jednoduchú zásadu: textil nemá zostať dlho vlhký a miestnosť musí mať výmenu vzduchu.",
            "Energetické odporúčania k praniu a sušeniu pripomínajú, že ťažšie bavlnené textílie, napríklad uteráky, je vhodné sušiť oddelene od ľahších kusov. V malej kúpeľni je tento rozdiel ešte výraznejší, pretože froté spomaľuje celú dávku.",
            "Vôňa je prchavý signál čistoty, ale nie dezinfekcia ani odvlhčovač. Ak sa vzduch nehýbe, vlhkosť zostáva a pach sa vráti. Praktická rutina má preto poradie: čistá práčka, správna dávka, dobrý oplach, rýchle sušenie, až potom jemná vôňa.",
        ],
        "sources": [
            ("US EPA: A Brief Guide to Mold, Moisture and Your Home", "https://www.epa.gov/mold/brief-guide-mold-moisture-and-your-home"),
            ("Energy.gov: Laundry tips", "https://www.energy.gov/energysaver/laundry"),
            ("DermNet: Fragrance allergy", "https://dermnetnz.org/topics/fragrance-allergy"),
        ],
        "sales": {
            "heading": "Riešenie pre jemnú vôňu po zvládnutí vlhkosti",
            "intro": "Keď bielizeň dobre schne a nezapácha po vlhkosti, môžete pridať jemnú vôňu. V malej kúpeľni je lepšie testovať nižšiu intenzitu a vyberať vôňu podľa výsledku po vysušení.",
            "product_name": "VEVO Essence Sample Set",
            "product_href": "/p-1621/vevo-essence-sample-set",
            "fit": "keď chcete otestovať, ktorá vôňa ostane na bielizni príjemná aj po sušení v menšom byte alebo kúpeľni.",
            "boundary": "ak bielizeň zatuchne počas sušenia, najprv vyriešte vlhkosť, rozostupy na sušiaku a vetranie. Vzorky používajte až na čistú a suchú bielizeň.",
            "product_button": "Vyskúšať sadu vzoriek",
            "category_title": "Vyberte vôňu až po vyriešení sušenia",
            "category_intro": "Parfumy do prania dávajú najväčší zmysel vtedy, keď textil nie je vlhký a práčka je čistá. Potom môžete riešiť štýl vône a intenzitu.",
            "category_bullets": [
                ("Malá kúpeľňa", "nižšia dávka, dobré vetranie a kontrola suchosti pred uložením."),
                ("Uteráky", "najprv savosť a preschnutie, až potom jemná vôňa."),
                ("Bežná bielizeň", "vôňa má dopĺňať čistotu, nie zakrývať vlhkosť."),
            ],
            "category_href": "/c/vevo-fragrance/parfum-do-prania",
            "category_button": "Pozrieť parfumy do prania",
        },
        "related": [
            ("Ako sušiť bielizeň v malom byte bez zatuchnutia", "/n/ako-susit-bielizen-v-malom-byte-bez-zatuchnutia"),
            ("Prečo moje oblečenie zapácha po praní", "/n/preco-moje-oblecenie-zapacha-po-prani-priciny-a-riesenia"),
            ("Prečo vôňa z prania rýchlo vyprchá a ako ju udržať dlhšie", "/n/preco-vona-z-prania-rychlo-vyprcha-a-ako-ju-udrzat-dlhsie"),
            ("Smrdí práčka? Ako vyčistiť bubon, gumu, zásuvku aj filter", "/n/smrdi-pracka-ako-vycistit-bubon-gumu-zasuvku-aj-filter"),
            ("Prečo uteráky zapáchajú aj po praní", "/n/preco-uteraky-zapachaju-aj-po-prani-zatuchnuty-pach-tvrdost-a-strata-savosti"),
        ],
        "faq": [
            ("Ako prevoňať bielizeň, keď suším v kúpeľni?", "Najprv zlepšite sušenie: rozostupy, vetranie, rýchle vybratie z práčky a kontrola suchosti. Vôňu pridajte až potom v nižšej dávke."),
            ("Prečo bielizeň v malej kúpeľni zatuchne?", "Najčastejšie preto, že schne príliš dlho vo vlhkom vzduchu alebo je na sušiaku natlačená bez prúdenia vzduchu."),
            ("Pomôže interiérový sprej v kúpeľni?", "Môže krátko prevoňať priestor, ale nerieši mokré uteráky, slabé vetranie ani zatuchnutú bielizeň."),
            ("Mám použiť viac parfumu do prania?", "Nie ako prvý krok. Ak textil zatuchne počas sušenia, viac vône problém často iba prekryje na krátky čas."),
            ("Kedy bielizeň uložiť do skrine?", "Až keď je úplne suchá aj v lemoch, švoch, rohoch uterákov a hrubších častiach."),
        ],
    },
    {
        "title": "Parfum do prania pri citlivej pokožke: kedy voliť jemnú vôňu a kedy radšej bez parfumácie",
        "short": "Pri citlivej pokožke používajte parfum do prania opatrne. Ak máte ekzém, svrbenie, podráždenie, malé dieťa alebo podozrenie na reakciu po praní, začnite radšej jemným pracím prostriedkom, nižšou dávkou a dôkladným oplachom. Vôňa je voliteľný doplnok, nie základ starostlivosti o citlivý textil.",
        "situations": [
            "oblečenie po praní svrbí alebo dráždi pokožku",
            "chcete jemnú vôňu, ale doma je človek s ekzémom",
            "periete detské oblečenie a neviete, či pridať parfumáciu",
            "bielizeň vonia príliš silno a pokožka je po nej nepokojná",
            "riešite rozdiel medzi hypoalergénnym pracím gélom a parfumom do prania",
            "potrebujete vedieť, kedy zvoliť extra oplach",
            "chcete testovať vôňu bez veľkej dávky naraz",
            "hľadáte bezpečnejšiu rutinu pre uteráky, pyžamo a posteľné prádlo",
        ],
        "quick": [
            "<strong>Pri podráždení vôňu najprv vynechajte.</strong> Ak pokožka svrbí, páli alebo je začervenaná, zjednodušte pranie a sledujte reakciu.",
            "<strong>Základ je jemný prací prostriedok a dobrý oplach.</strong> Parfum do prania pridávajte iba vtedy, keď je rutina stabilná.",
            "<strong>Pri deťoch a ekzéme postupujte opatrne.</strong> Vôňa môže byť príjemná, ale citlivá pokožka nemusí reagovať rovnako ako zdravá pokožka dospelého.",
            "<strong>Silná vôňa nie je dôkaz čistoty.</strong> Čistotu robí pranie, dávkovanie, oplach a sušenie.",
            "<strong>Ak chcete vôňu skúsiť, začnite veľmi nízko.</strong> Najprv na menej citlivom textile, nie na pyžame alebo spodnej bielizni.",
        ],
        "intro": [
            "Parfum do prania je obľúbený preto, že vie dať bielizni príjemný a osobný dojem. Pri citlivej pokožke však treba zmeniť poradie priorít. Najprv má byť textil čistý, dobre opláchnutý a bez zbytočných zvyškov. Až potom má zmysel riešiť vôňu.",
            "Citlivá pokožka neznamená automaticky, že každý vonný produkt bude problém. Znamená však, že chyba v dávkovaní, slabý oplach, nevhodný materiál alebo silná parfumácia sa môžu prejaviť rýchlejšie. Preto je lepšie postupovať pomaly a nehodnotiť výsledok iba podľa vône po otvorení práčky.",
            "Pri otázkach ako parfum do prania pri ekzéme, vôňa pri citlivej pokožke, hypoalergénny prací gél, detské oblečenie bez parfumácie alebo extra oplach po praní je dôležitá opatrná odpoveď: pri aktívnom podráždení a kožných ťažkostiach parfumáciu radšej vynechať a pri pretrvávajúcich prejavoch sa poradiť s lekárom.",
            "Tento článok pomáha rozlíšiť, kedy môže jemná vôňa dávať zmysel, kedy je rozumnejšia bielizeň bez parfumácie a ako nastaviť pranie tak, aby predaj vône neprebil zdravý úsudok.",
        ],
        "why": [
            "Vonné látky môžu u niektorých ľudí vyvolať kontaktnú alergiu alebo podráždenie. DermNet uvádza, že parfumované zložky sa nachádzajú nielen v kozmetike, ale aj v domácich produktoch vrátane pracích prostriedkov, aviváží a osviežovačov.",
            "Pri textile je kontakt dlhý. Pyžamo, spodná bielizeň, legíny, uterák alebo obliečka sa dotýkajú pokožky hodiny. Ak v nich zostanú zvyšky pracieho prostriedku alebo výrazná parfumácia, citlivejší človek si problém všimne skôr než pri krátkom kontakte s vôňou v priestore.",
            "Textilná dermatitída môže súvisieť aj s farbivami, povrchovými úpravami, materiálom alebo trením, nielen s vôňou. Preto netreba všetko zvaľovať na jeden produkt. Správny postup je zjednodušiť rutinu, meniť jednu vec naraz a sledovať, čo sa zlepší.",
            "Extra oplach je často praktickejší než silnejšia vôňa. Ak je problém v zvyškoch, menej produktu a lepší oplach majú väčší význam než pridanie ďalšej parfumácie.",
        ],
        "rows": [
            ("Aktívne svrbenie alebo začervenanie", "vôňu vynechať a zjednodušiť pranie", "Najprv znížiť počet možných dráždivých faktorov."),
            ("Detské oblečenie a pyžamo", "jemný prací základ, dôkladný oplach, bez zbytočnej vône", "Dlhý kontakt s pokožkou počas spánku."),
            ("Uteráky", "nepreháňať dávku, sledovať oplach a savosť", "Zvyšky produktu môžu znižovať komfort aj savosť."),
            ("Bežné tričká dospelého", "jemnú vôňu skúsiť v nízkej dávke", "Vhodné iba ak pokožka nereaguje a bielizeň je dobre opláchnutá."),
            ("Posteľné prádlo", "opatrne, najmä pri alergikoch a citlivej pokožke", "Dlhý nočný kontakt s tvárou a telom."),
        ],
        "steps": [
            "Ak sa objaví podráždenie, na niekoľko praní vynechajte parfumáciu aj aviváž a používajte jednoduchšiu rutinu.",
            "Skontrolujte dávku pracieho gélu. Priveľa produktu môže v textile zostať a dráždiť aj bez výraznej vône.",
            "Pridajte extra oplach pri pyžame, spodnej bielizni, uterákoch a detských veciach, ak máte podozrenie na zvyšky.",
            "Bielizeň sušte dôkladne. Vlhký textil môže sám o sebe pôsobiť nepríjemne a zhoršiť pocit na pokožke.",
            "Ak chcete vôňu vrátiť, začnite na menej citlivom textile, napríklad na dekoratívnom pléde alebo bežnom tričku dospelého.",
            "Použite veľmi nízku dávku a sledujte reakciu po celom dni nosenia, nie iba vôňu po praní.",
            "Pri ekzéme, alergii alebo opakovaných prejavoch sa poraďte s dermatológom a nerobte z parfumácie nutnú súčasť prania.",
        ],
        "decision_rows": [
            ("Svrbí pokožka po oblečení", "vynechať vôňu, znížiť dávku pracieho prostriedku, pridať oplach", "Najprv treba znížiť zvyšky a možné dráždivé faktory."),
            ("Bielizeň je príliš voňavá", "prať znova bez parfumácie alebo s extra oplachom", "Silná vôňa môže znamenať vysokú dávku alebo slabý oplach."),
            ("Doma je malé dieťa", "začať bez parfumácie alebo veľmi opatrne", "Detská pokožka a nočný kontakt vyžadujú konzervatívny prístup."),
            ("Chcete príjemnú vôňu pre návštevu", "použiť vôňu na deku alebo uterák pre hostí, nie na citlivé osobné kusy", "Oddelíte estetický dojem od každodenného kontaktu s citlivou pokožkou."),
            ("Reakcia sa opakuje", "zastaviť experimenty a riešiť to s odborníkom", "Môže ísť o alergiu, materiál, farbivo, prací prostriedok alebo kombináciu."),
        ],
        "mistakes": [
            "Použiť parfum do prania na detské pyžamo ako prvý test vône.",
            "Myslieť si, že hypoalergénny prací gél a parfum do prania majú rovnaký účel.",
            "Pridávať vôňu, keď je bielizeň zle opláchnutá alebo lepkavá.",
            "Hodnotiť citlivosť podľa toho, či vôňa príjemne vonia v dávkovači.",
            "Meniť naraz prací gél, aviváž, parfum, program aj materiál a potom nevedieť, čo spôsobilo reakciu.",
            "Používať silnú vôňu na posteľné prádlo človeka, ktorý má bolesti hlavy z vôní alebo podráždené dýchacie cesty.",
        ],
        "detail_sections": [
            ("Kedy radšej prať bez parfumácie", "Bez parfumácie perte vtedy, keď je pokožka práve podráždená, keď riešite ekzém, keď periete oblečenie pre bábätko alebo keď sa po praní objavuje svrbenie. Bez vône má zmysel prať aj spodnú bielizeň, pyžamo a posteľné prádlo osoby, ktorá na vône reaguje bolesťou hlavy, kašľom alebo nepríjemným pocitom."),
            ("Kedy môže jemná vôňa dávať zmysel", "Jemná vôňa môže dávať zmysel pri bežnom oblečení dospelého, dekoratívnej deke, uterákoch pre hostí alebo sezónnych textíliách, ak nikto v domácnosti nereaguje podráždene. Stále platí, že začínate nízko a najprv máte stabilnú praciu rutinu."),
            ("Ako testovať vôňu rozumne", "Nerobte prvý test na veľkej dávke osobnej bielizne. Vyberte jeden menej citlivý kus, použite nižšiu dávku a sledujte výsledok po nosení alebo používaní. Ak je všetko v poriadku, môžete vôňu použiť aj inde, ale stále rozlišujte medzi uterákmi, obliečkami, pyžamom a bežným tričkom."),
            ("Prečo je extra oplach dôležitý", "Extra oplach nepôsobí luxusne, ale pri citlivej pokožke je často praktickejší než ďalší produkt. Pomáha znížiť zvyšky pracieho prostriedku a parfumácie vo vláknach. Najmä pri tvrdej vode, uterákoch, posteľnej bielizni a preplnenom bubne môže byť rozdiel citeľný."),
        ],
        "caution": [
            "Tento článok nie je lekárska diagnóza. Ak máte ekzém, alergiu, opakované svrbenie, vyrážku alebo podozrenie na kontaktnú reakciu, riešte príčinu s dermatológom. Pri výrazných prejavoch nie je cieľom nájsť silnejšiu vôňu, ale bezpečnejšiu rutinu.",
            "Produkty označené ako jemné alebo vhodné pre citlivú pokožku nemusia vyhovovať každému človeku. Aj prírodná alebo príjemná vôňa môže niekomu prekážať. Sledujte vlastnú reakciu a nepoužívajte parfumáciu ako povinnú súčasť prania.",
        ],
        "expert": [
            "DermNet opisuje fragrance allergy ako alergickú kontaktnú dermatitídu na vonnú chemickú látku a upozorňuje, že parfumované zložky sa môžu nachádzať aj v pracích a domácich produktoch. Pre domáce pranie z toho vyplýva praktická opatrnosť: ak pokožka reaguje, vôňu treba brať ako možný faktor, nie ako samozrejmosť.",
            "Textile contact dermatitis môže súvisieť s viacerými faktormi: materiálom, farbivami, úpravami textilu, potom, trením alebo zvyškami produktov. Preto je najrozumnejšie meniť rutinu postupne. Najprv jednoduchší prací základ, správne dávkovanie a oplach; až potom test vône.",
            "Pri citlivej pokožke nie je cieľom sterilne bezvonná domácnosť za každú cenu, ale kontrola rizika. Niektorým ľuďom jemná vôňa neprekáža, iným áno. Rozdiel zistíte iba opatrným používaním a ochotou vôňu vynechať tam, kde textil trávi veľa času na pokožke.",
        ],
        "sources": [
            ("DermNet: Fragrance allergy", "https://dermnetnz.org/topics/fragrance-allergy"),
            ("DermNet: Textile contact dermatitis", "https://dermnetnz.org/topics/textile-contact-dermatitis"),
            ("Energy.gov: Laundry tips", "https://www.energy.gov/energysaver/laundry"),
        ],
        "sales": {
            "heading": "Riešenie pre citlivejšiu praciu rutinu",
            "intro": "Pri citlivej pokožke má prednosť šetrný prací základ, dávkovanie a oplach. Vôňa má byť až voliteľný doplnok, nie prvý krok.",
            "product_name": "Prací gél hypoalergénny z Marseillského mydla 1L",
            "product_href": "/p-1551/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l",
            "fit": "keď chcete začať pri citlivejšej bielizni jednoduchším pracím základom a až potom riešiť, či vôňu vôbec pridať.",
            "boundary": "ak sa objavuje svrbenie, vyrážka alebo pálenie, parfumáciu dočasne vynechajte a riešte oplach, dávku, materiál a prípadne odborné odporúčanie.",
            "product_button": "Pozrieť hypoalergénny prací gél",
            "category_title": "Vyberte pranie pre citlivejšiu pokožku",
            "category_intro": "Pri citlivej pokožke je vhodné porovnávať produkty podľa jednoduchosti rutiny, dávkovania a toho, či dokážete bielizeň dôkladne opláchnuť.",
            "category_bullets": [
                ("Pyžamo a spodná bielizeň", "najprv bez zbytočnej parfumácie a s dôkladným oplachom."),
                ("Detské oblečenie", "konzervatívny postup, malé dávky a jasná rutina."),
                ("Bežné oblečenie dospelého", "jemnú vôňu skúšať až po stabilnom praní bez reakcie."),
            ],
            "category_href": "/c/vevo-home-care/pranie/hypoalergenne-pracie-prostriedky",
            "category_button": "Pozrieť hypoalergénne pranie",
        },
        "related": [
            ("Extra oplach v práčke: kedy pomôže", "/n/extra-oplach-v-pracke-kedy-pomoze-pri-zapachu-tvrdej-bielizni-a-citlivej-pokozke"),
            ("Ako prať detské oblečenie a oblečenie pre bábätko bez podráždenia pokožky", "/n/ako-prat-detske-oblecenie-a-oblecenie-pre-babaetko-bez-podrazdenia-pokozky"),
            ("Bambusové vlákno vs bavlna pri citlivej pokožke", "/n/bambusove-vlakno-vs-bavlna-vyhody-nevyhody-a-pranie-pri-citlivej-pokozke"),
            ("Ako vybrať prací gél podľa typu bielizne", "/n/ako-vybrat-praci-gel-podla-typu-bielizne"),
            ("Parfum do prania: čo to je a ako funguje", "/n/parfum-do-prania-co-to-je-a-ako-funguje"),
        ],
        "faq": [
            ("Môžem používať parfum do prania pri citlivej pokožke?", "Niekedy áno, ale opatrne a v nízkej dávke. Pri aktívnom podráždení, ekzéme alebo opakovanej reakcii je rozumnejšie parfumáciu vynechať."),
            ("Je hypoalergénny prací gél to isté ako parfum do prania?", "Nie. Prací gél je základ na pranie, parfum do prania je doplnok vône. Pri citlivej pokožke riešte najprv prací základ a oplach."),
            ("Pomôže extra oplach?", "Často áno, najmä ak je problém v zvyškoch pracieho prostriedku alebo príliš silnej vôni v textile."),
            ("Môžem parfumovať detské oblečenie?", "Pri detskom oblečení je bezpečnejší konzervatívny postup: jemné pranie, dobrý oplach a bez zbytočnej parfumácie, najmä pri najmenších deťoch alebo citlivej pokožke."),
            ("Čo robiť, ak bielizeň po praní dráždi pokožku?", "Zjednodušte rutinu, vynechajte vôňu a aviváž, znížte dávku pracieho prostriedku, pridajte oplach a pri pretrvávaní sa poraďte s odborníkom."),
        ],
    },
]


def build_long(article):
    html = [
        f"<p><strong>Rýchla odpoveď:</strong> {article['short']}</p>",
        practical_box(article["situations"]),
        callout("Rýchly praktický výber", article["quick"]),
    ]
    html.extend(f"<p>{paragraph}</p>" for paragraph in article["intro"])
    html.append("<h2>Prečo pri tejto téme nestačí len pridať vôňu</h2>")
    html.extend(f"<p>{paragraph}</p>" for paragraph in article["why"])
    html.append("<h2>Rozhodovanie podľa situácie</h2>")
    html.append(table(["Situácia", "Odporúčaný postup", "Prečo"], article["rows"]))
    html.append("<h2>Postup krok za krokom</h2>")
    html.append("<ol>" + "".join(f"<li>{step}</li>" for step in article["steps"]) + "</ol>")
    html.append("<h2>Diagnostická tabuľka</h2>")
    html.append(table(["Príznak", "Prvý krok", "Dôvod"], article["decision_rows"]))
    html.append("<h2>Čomu sa vyhnúť</h2>")
    html.append("<ul>" + "".join(f"<li>{item}</li>" for item in article["mistakes"]) + "</ul>")
    for heading, text in article["detail_sections"]:
        html.append(f"<h2>{heading}</h2>")
        html.append(f"<p>{text}</p>")
    html.append("<h2>Kedy byť opatrný</h2>")
    html.extend(f"<p>{paragraph}</p>" for paragraph in article["caution"])
    html.append("<h2>Odbornejší pohľad</h2>")
    html.extend(f"<p>{paragraph}</p>" for paragraph in article["expert"])
    html.append(source_box(article["sources"]))
    html.append(sales_block(article["sales"]))
    html.append(related(article["related"]))
    html.append("<h2>FAQ</h2>")
    for question, answer in article["faq"]:
        html.append(f"<h3>{question}</h3><p>{answer}</p>")
    return "\n".join(html)


def collect_links(articles):
    hrefs = sorted({href for row in articles for href in re.findall(r'href="([^"]+)"', row["long"])})
    links = []
    for href in hrefs:
        if href.startswith("/"):
            url = urljoin(BASE, href)
        elif href.startswith("http"):
            url = href
        else:
            continue
        response = requests.get(
            url,
            headers={"User-Agent": f"Codex VEVO {BATCH} link preflight"},
            timeout=45,
            allow_redirects=True,
        )
        links.append({"href": href, "url": url, "status": response.status_code, "final_url": response.url, "ok": response.status_code == 200})
    return links


def validate_article(article):
    public_text = "\n".join([article["title"], article["short"], article["long"]])
    forbidden = sorted(set(match.group(0) for match in FORBIDDEN_PUBLIC_RE.finditer(public_text)), key=str.lower)
    if forbidden:
        raise SystemExit(f"Forbidden public wording in {article['title']}: {forbidden}")
    if "Cena:" in public_text or re.search(r"\d+(?:[,.]\d{1,2})?\s*€", public_text):
        raise SystemExit(f"Fixed price marker in {article['title']}")
    if len(article["long"]) > 32700:
        raise SystemExit(f"XLS cell too long for {article['title']}: {len(article['long'])}")


def main():
    times = ["08:00:00", "08:12:00", "08:24:00"]
    articles = []
    for index, article in enumerate(ARTICLES):
        row = {
            "title": article["title"],
            "short": article["short"],
            "long": build_long(article),
            "date_posted": BATCH_DATE,
            "time_posted": times[index],
            "active": 1,
            "link": slugify(article["title"]),
            "commenting": "none",
        }
        validate_article(row)
        articles.append(row)

    link_checks = collect_links(articles)
    failed_links = [item for item in link_checks if not item["ok"]]
    if failed_links:
        raise SystemExit(f"Link preflight failed: {failed_links[:3]}")

    OUT_JSON.write_text(json.dumps(articles, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    book = xlwt.Workbook(encoding="utf-8")
    sheet = book.add_sheet("news")
    headers = ["title", "short", "long", "date_posted", "time_posted", "active", "link", "commenting"]
    for col, header in enumerate(headers):
        sheet.write(0, col, header)
    for row_index, article in enumerate(articles, start=1):
        for col, header in enumerate(headers):
            sheet.write(row_index, col, article[header])
    OUT_XLS.parent.mkdir(parents=True, exist_ok=True)
    book.save(str(OUT_XLS))

    preflight = {
        "batch": BATCH,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "article_count": len(articles),
        "json": str(OUT_JSON),
        "xls": str(OUT_XLS),
        "date_posted": BATCH_DATE,
        "slugs": [article["link"] for article in articles],
        "lengths": {article["title"]: len(article["long"]) for article in articles},
        "link_count": len(link_checks),
        "links": link_checks,
    }
    OUT_PREFLIGHT.write_text(json.dumps(preflight, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "batch": BATCH,
                "article_count": len(articles),
                "json": str(OUT_JSON),
                "xls": str(OUT_XLS),
                "preflight": str(OUT_PREFLIGHT),
                "links_checked": len(link_checks),
                "slugs": preflight["slugs"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
