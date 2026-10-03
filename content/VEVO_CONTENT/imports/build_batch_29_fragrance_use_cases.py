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
BATCH = "batch-29"
BATCH_DATE = "2025-09-23"
OUT_JSON = Path("content/VEVO_CONTENT/imports/batch-29-2026-06-17-articles.json")
OUT_PREFLIGHT = Path("content/VEVO_CONTENT/exports/batch-29-2026-06-17-preflight.json")
OUT_XLS = Path.home() / "AppData/Local/Temp/vevo-batch-29-fragrance-use-cases-clean-urls.xls"

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
<p>Nižšie nájdete praktické odpovede pre bežné domáce prípady, kde vôňa bielizne závisí od čistoty, oplachu, sušenia a spôsobu používania textilu.</p>
<ul>{rows}</ul>
</div>
""".strip()


def source_box(items):
    rows = "".join(f'<li><a rel="noopener" href="{href}" target="_blank">{escape(label)}</a></li>' for label, href in items)
    return f"""
<div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; margin: 22px 0; background: #fbfbfb;">
<h2 style="margin-top: 0;">Zdroje a odborný kontext</h2>
<p>Odkazy nižšie používame ako širší odborný rámec k praniu, vlhkosti, vnútornému vzduchu a citlivosti na vône. Nenahrádzajú individuálne odporúčanie lekára ani pokyny výrobcu textilu.</p>
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
        "title": "Najčastejšie chyby pri parfumoch do prania: priveľa vône, zlý oplach a miešanie s avivážou",
        "short": "Parfum do prania najlepšie funguje na čistej, dobre opláchnutej a úplne suchej bielizni. Najčastejšie chyby sú priveľká dávka, používanie vône na prekrytie pachu, preplnená práčka, slabý oplach, pomalé sušenie a automatické miešanie s avivážou. Vôňa má čistý výsledok doplniť, nie nahradiť pranie.",
        "situations": [
            "bielizeň vonia príliš silno hneď po praní, ale po dni je ťažká alebo zatuchnutá",
            "tričká po zahriatí tela miešajú pot s parfumom",
            "uteráky voňajú, ale sú tvrdšie alebo horšie sajú",
            "oblečenie je lepkavé, akoby v ňom ostal prací prostriedok",
            "parfum do prania po vysušení necítiť tak, ako ste čakali",
            "vôňa sa bije s avivážou, deodorantom alebo parfumom na tele",
            "pri citlivej pokožke neviete, či prekáža prací gél, vôňa alebo zvyšky v textile",
            "chcete voňať čisto, ale nie tak, aby bielizeň pôsobila presýtene",
        ],
        "quick": [
            "<strong>Začnite čistým praním.</strong> Ak textil zapácha potom, vlhkosťou alebo práčkou, parfum ho nemá prekryť.",
            "<strong>Dávku zvyšujte až po teste.</strong> Silnejšia dávka nie je automaticky lepšia výdrž vône.",
            "<strong>Oplach je rovnako dôležitý ako vôňa.</strong> Zvyšky gélu, aviváže alebo parfumu môžu robiť textil ťažkým.",
            "<strong>Pri uterákoch a športových veciach buďte opatrnejší.</strong> Satie, funkčnosť a odstránenie potu sú pred vôňou.",
            "<strong>Nemixujte všetko naraz.</strong> Ak zmeníte prací gél, aviváž, parfum aj program, neviete, čo spôsobilo výsledok.",
        ],
        "intro": [
            "Parfum do prania vie urobiť z bežnej bielizne príjemný detail domácnosti. Problém nastáva vtedy, keď sa používa ako skratka namiesto správneho prania. Bielizeň môže voňať krásne po otvorení práčky, ale po usušení, nosení alebo uložení začne pôsobiť ťažko. Vtedy nie je vždy chybou samotná vôňa, ale celý postup okolo nej.",
            "Najčastejšie otázky sú veľmi praktické: koľko parfumu do prania použiť, či sa môže miešať s avivážou, prečo bielizeň po chvíli nevonia, prečo je vôňa príliš silná alebo prečo tričko smrdí pod pazuchami aj napriek príjemnej aróme. Odpoveď je skoro vždy v kombinácii dávkovania, oplachu, materiálu, vlhkosti a zdroja pôvodného pachu.",
            "Dobrý výsledok nevzniká tým, že do práčky nalejete viac voňavého produktu. Vzniká tým, že textil je rozdelený podľa typu, práčka nie je preplnená, prací prostriedok je dávkovaný rozumne, bielizeň sa dobre opláchne a rýchlo vysuší. Parfum do prania potom pridá podpis vône, nie masku.",
            "Tento návod je praktický kontrolný zoznam chýb, ktoré sa dajú opraviť bez toho, aby ste menili všetko naraz. Pri každej chybe je dôležité pýtať sa: je problém v vôni, alebo v tom, že textil ešte nie je na vôňu pripravený?",
        ],
        "why": [
            "Vôňa v bielizni je citlivá na zvyšky. Ak v textilii ostane priveľa pracieho gélu, aviváže alebo parfumovaného doplnku, výsledok môže byť opak sviežosti. Textil pôsobí hutne, pri dotyku nie je príjemný a pri zahriatí tela sa pachy ľahšie miešajú.",
            "Ďalším faktorom je materiál. Bavlna drží vlhkosť inak než polyester, uterák potrebuje savosť, športové oblečenie potrebuje odstrániť pot a posteľná bielizeň je v kontakte s pokožkou veľa hodín. Jedna dávka vône preto nemôže fungovať rovnako na všetko.",
            "Veľmi častý problém je sušenie. Ak bielizeň schne pomaly, visí natesno alebo sa uloží ešte mierne vlhká, vôňa sa stratí v zatuchnutom dojme. Pridať viac parfumu do ďalšieho prania potom nepomôže, pretože príčina zostala rovnaká.",
            "Pri citlivejších ľuďoch treba rozlišovať medzi príjemnou arómou a toleranciou pokožky alebo nosa. Vôňa, ktorá je príjemná na pléde, môže byť na tričku pri krku alebo na pyžame príliš intenzívna. Preto sa oplatí testovať pomaly a zvlášť pri textíliách pri tvári.",
        ],
        "rows": [
            ("Priveľa parfumu do prania", "textil je presýtený, vôňa pôsobí ťažko", "znížiť dávku a testovať na menšej dávke bielizne"),
            ("Vôňa na zapáchajúci textil", "pot, vlhkosť alebo práčka sa mieša s parfumom", "najprv odstrániť zdroj pachu"),
            ("Slabý oplach", "bielizeň je lepkavá alebo tuhá", "nepreplniť bubon, znížiť dávku gélu, pridať oplach"),
            ("Miešanie s avivážou", "vône sa bijú alebo vzniká nános", "použiť jednoduchšiu rutinu a meniť jednu vec naraz"),
            ("Pomalé sušenie", "vôňa po vysušení ustúpi zatuchnutiu", "sušiť s rozostupom, vetrať a neukladať vlhké kusy"),
        ],
        "steps": [
            "Vyberte jeden typ bielizne, napríklad tričká alebo obliečky, a netestujte novú vôňu hneď na všetkom naraz.",
            "Skontrolujte, či bielizeň pred praním nezapácha z konkrétnej príčiny: pot, vlhkosť, práčka, uteráky, športová taška alebo skriňa.",
            "Dávku pracieho gélu držte pri odporúčaní výrobcu a pri mäkkej vode ani malej náplni ju zbytočne nezvyšujte.",
            "Parfum do prania pridajte podľa návodu a začnite skôr nižšie, hlavne pri pyžame, šatkách, uterákoch a detských veciach.",
            "Práčku nepreplňte. Bielizeň potrebuje priestor, aby sa prací prostriedok aj vôňa rozložili a aby sa všetko opláchlo.",
            "Po praní bielizeň vyberte čo najskôr a sušte tak, aby cez textil prúdil vzduch.",
            "Výsledok hodnotte až po úplnom vysušení a po dni používania. Vôňa po otvorení práčky nie je rovnaká ako vôňa pri nosení.",
        ],
        "decision_rows": [
            ("Bielizeň vonia krásne iba z práčky", "skontrolovať sušenie a uloženie", "vlhkosť vie zmeniť čistú vôňu na zatuchnutý dojem"),
            ("Vôňa je silná pri krku", "znížiť dávku alebo vynechať pri šatkách a košeliach", "textil je blízko nosa a pokožky"),
            ("Uteráky sú tvrdšie", "riešiť dávku, oplach a aviváž", "nánosy môžu ovplyvniť pocit aj savosť"),
            ("Športové tričko zapácha po zahriatí", "prať skôr, v menšej dávke a s lepším oplachom", "parfum nemá nahradiť odstránenie potu"),
            ("Pokožka reaguje svrbením", "zjednodušiť rutinu a vôňu dočasne vynechať", "treba znížiť počet možných dráždivých faktorov"),
        ],
        "mistakes": [
            "Zvyšovať dávku vône skôr, než vyriešite zdroj pachu.",
            "Používať parfum do prania, aviváž a silne parfumovaný prací prostriedok naraz.",
            "Prať uteráky, športové veci a pyžamo rovnakou dávkou vône.",
            "Nechať bielizeň po praní dlho v zatvorenej práčke.",
            "Testovať novú vôňu na celej rodinnej várke bielizne.",
            "Hodnotiť výsledok podľa vône mokrej bielizne, nie podľa suchého textilu pri používaní.",
        ],
        "detail_sections": [
            ("Priveľa vône neznamená dlhšiu výdrž", "Silná dávka môže prevoňať miestnosť pri sušení, ale na textile nemusí pôsobiť lepšie. Pri oblečení pri tele sa intenzita zvyšuje teplom, pohybom a blízkosťou nosa. Ak sa vám vôňa zdá po hodine nosenia ťažká, pravdepodobne nejde o slabú kvalitu, ale o príliš vysokú dávku alebo nevhodný typ textilu."),
            ("Prečo sa vôňa mieša s potom", "Pot a kožný maz sa najviac držia v podpazuší, golieri, manžetách a syntetických vláknach. Ak tieto miesta nie sú dobre vyprané, parfum sa pridá na vrch existujúceho pachu. Výsledok môže byť horší než neutrálne čistý textil. Pri športových veciach a košeliach preto riešte najprv pranie a až potom vôňu."),
            ("Kedy miešanie s avivážou nedáva zmysel", "Aviváž aj parfum do prania pracujú s pocitom a vôňou textilu, ale neriešia rovnaký problém ako prací gél. Pri uterákoch, funkčnom oblečení alebo citlivej pokožke môže byť jednoduchšia rutina lepšia. Ak chcete zistiť, čo vám vyhovuje, nepoužívajte naraz tri voňavé produkty."),
            ("Ako testovať novú vôňu bezpečnejšie", "Vyberte jednu menšiu dávku bielizne, ideálne bežné tričká alebo uteráky pre hostí, nie detské pyžamo ani oblečenie alergika. Použite nižšiu dávku a sledujte výsledok po vysušení. Až keď viete, že vôňa sedí, preneste ju na väčší objem prania."),
        ],
        "caution": [
            "Pri bábätkách, deťoch, alergikoch, astmatikoch, ľuďoch s ekzémom alebo pri opakovanej bolesti hlavy z vôní začnite bez parfumácie alebo s veľmi nízkou intenzitou. Pri reakcii vôňu vynechajte a riešte pranie, oplach a materiál.",
            "Ak textil zapácha plesnivo alebo zatuchnuto, nepoužívajte vôňu ako prvý krok. Najprv skontrolujte práčku, sušenie, skladovanie, vlhkosť v miestnosti a to, či oblečenie neležalo mokré v koši alebo športovej taške.",
        ],
        "expert": [
            "Z praktického hľadiska je vôňa výsledkom celého pracieho procesu. Prací prostriedok má odstrániť nečistoty, oplach má znížiť zvyšky, odstreďovanie a sušenie majú dostať vodu z textilu. Parfum do prania vstupuje až do prostredia, kde už textil nepáchne z inej príčiny.",
            "Odborné zdroje k vnútornému vzduchu a vonným produktom pripomínajú, že vône sú vnímané cez prchavé látky. V bežnej domácnosti z toho vyplýva jednoduché pravidlo: používať ich rozumne, vetrať a rešpektovať citlivosť ľudí v priestore.",
            "Pri pokožke je dôležité, že textil zostáva v kontakte s telom dlhšie než vôňa v miestnosti. Preto sú pyžamá, spodná bielizeň, tričká pri krku a posteľná bielizeň citlivejšie testovacie plochy než dekoratívna deka alebo uterák pre hostí.",
        ],
        "sources": [
            ("Energy.gov: Laundry tips", "https://www.energy.gov/energysaver/laundry"),
            ("DermNet: Fragrance allergy", "https://dermnetnz.org/topics/fragrance-allergy"),
            ("US EPA: Volatile Organic Compounds and Indoor Air Quality", "https://www.epa.gov/indoor-air-quality-iaq/volatile-organic-compounds-impact-indoor-air-quality"),
        ],
        "sales": {
            "heading": "Začnite testom namiesto veľkej dávky",
            "intro": "Ak si nie ste istí intenzitou, najpraktickejšie je otestovať vôňu na menšej várke bielizne. Tak zistíte, či vám sedí po vysušení aj pri nosení.",
            "product_name": "Sada vzoriek najpredávanejších vôní VEVO 3 x 10ml",
            "product_href": "/p-1574/sada-vzoriek-najpredavanejsich-voni-vevo-3-x-10ml",
            "fit": "keď chcete zistiť, ktorá vôňa sedí vašej bielizni bez toho, aby ste hneď kupovali veľké balenie.",
            "boundary": "ak bielizeň zapácha potom, vlhkosťou alebo práčkou, najprv opravte pranie, oplach a sušenie.",
            "product_button": "Vyskúšať vzorky vôní",
            "category_title": "Vyberte parfum do prania podľa používania textilu",
            "category_intro": "Pri výbere vône porovnávajte nielen prvý dojem, ale aj to, či ju chcete na obliečky, bežné tričká, uteráky alebo sezónne textílie.",
            "category_bullets": [
                ("Bežné oblečenie", "znesie jemnú až strednú intenzitu, ak je dobre opláchnuté."),
                ("Textil pri tvári", "potrebuje nižšiu intenzitu a opatrnejšie testovanie."),
                ("Uteráky a šport", "najprv riešte savosť, pot a sušenie."),
            ],
            "category_href": "/c/vevo-fragrance/parfum-do-prania",
            "category_button": "Pozrieť parfumy do prania",
        },
        "related": [
            ("Parfum do prania: čo to je a ako funguje", "/n/parfum-do-prania-co-to-je-a-ako-funguje"),
            ("Ako dávkovať parfum do prania podľa množstva bielizne", "/n/ako-davkovat-parfum-do-prania-podla-mnozstva-bielizne"),
            ("Prečo vôňa z prania rýchlo vyprchá a ako ju udržať dlhšie", "/n/preco-vona-z-prania-rychlo-vyprcha-a-ako-ju-udrzat-dlhsie"),
            ("Ako prejsť z aviváže na parfum do prania bez sklamania", "/n/ako-prejst-z-avivaze-na-parfum-do-prania-bez-sklamania"),
            ("Parfum do prania pri citlivej pokožke", "/n/parfum-do-prania-pri-citlivej-pokozke-kedy-volit-jemnu-vonu-a-kedy-radsej-bez-parfumacie"),
        ],
        "faq": [
            ("Koľko parfumu do prania použiť?", "Začnite nižšie než je vaša predstava o ideálnej intenzite a výsledok hodnotte až po vysušení. Pri textíliách pri tvári dávkujte ešte opatrnejšie."),
            ("Môžem miešať parfum do prania s avivážou?", "Technicky to niekedy ľudia robia, ale často je lepšie rutinu zjednodušiť. Pri uterákoch, športe a citlivej pokožke najprv testujte bez aviváže."),
            ("Prečo bielizeň po parfume zapácha ťažko?", "Najčastejšie ide o zvyšky, slabý oplach, pomalé sušenie alebo pôvodný pach, ktorý nebol odstránený."),
            ("Je lepšie pridať viac vône, keď ju necítim?", "Nie hneď. Skontrolujte sušenie, množstvo bielizne v práčke, dávku pracieho gélu a to, či si nos na vôňu nezvykol."),
            ("Môžem parfum do prania používať na všetko?", "Nie rovnako. Inak dávkujte pri obliečkach, uterákoch, športových veciach, detskej bielizni a šáloch pri tvári."),
        ],
    },
    {
        "title": "Ako prevoňať oblečenie do kancelárie: jemná vôňa, košeľa pri krku a prádlo bez ťažkej parfumácie",
        "short": "Oblečenie do kancelárie má voňať čisto a nenápadne. Pri košeliach, blúzkach, sakách a tričkách pod sveter je dôležité odstrániť pot z goliera a podpazušia, dobre opláchnuť prací prostriedok a použiť vôňu skôr jemne. V práci je lepšia čistá bielizeň s ľahkým tónom než silná parfumácia, ktorá sa mieša s deodorantom alebo osobným parfumom.",
        "situations": [
            "košeľa pri krku vonia príliš silno alebo sa mieša s parfumom na tele",
            "blúzka je čistá, ale pri sedení v kancelárii rýchlo stratí sviežosť",
            "golier drží deodorant, krém, make-up alebo vlasové produkty",
            "tričko pod sako zapácha pri zahriatí tela, aj keď po praní voňalo",
            "chcete jemnú vôňu na pracovné oblečenie bez toho, aby rušila kolegov",
            "neviete, či použiť parfum do prania na košele, šatky a blúzky pri krku",
            "obliekate sa do malej kancelárie, auta alebo zasadačky, kde je vôňa cítiť viac",
            "chcete zladiť vôňu bielizne s deodorantom a osobným parfumom bez presýtenia",
        ],
        "quick": [
            "<strong>Pri kancelárii voľte nízku intenzitu.</strong> Oblečenie je v uzavretom priestore a pri krku, preto pôsobí vôňa silnejšie.",
            "<strong>Najprv riešte golier a podpazušie.</strong> Ak tam ostane pot, kožný maz alebo deodorant, parfum sa bude miešať s pachom.",
            "<strong>Sako a jemné kusy neperte naslepo.</strong> Pri štruktúrovaných odevoch rešpektujte štítok alebo čistiareň.",
            "<strong>Vôňu netestujte na celej pracovnej garderóbe.</strong> Začnite jednou košeľou alebo tričkom, ktoré nenosíte na dôležité stretnutie.",
            "<strong>Menej je v práci často lepšie.</strong> Cieľom je čistý dojem, nie vôňa, ktorú cíti celá miestnosť.",
        ],
        "intro": [
            "Oblečenie do kancelárie má iné pravidlá než domáca deka alebo športové tričko. Sedíte v ňom dlhšie, často v uzavretom priestore, pri kolegoch, v aute, výťahu alebo zasadačke. Aj jemná vôňa sa v takom prostredí vníma intenzívnejšie, najmä keď je pri krku a mieša sa s deodorantom, vlasovým sprejom alebo osobným parfumom.",
            "Najväčší rozdiel robia miesta kontaktu s telom: golier, podpazušie, manžety, vnútorná strana šatky a tričko pod sakom. Ak tieto miesta nie sú dobre vyprané, parfum do prania vytvorí iba príjemný prvý dojem, ktorý sa počas dňa môže zmeniť na ťažkú kombináciu pachu a vône.",
            "Otázky ako ako prevoňať košeľu, ako prať blúzku do kancelárie, ako odstrániť pach z goliera, aká vôňa do prania je vhodná do práce alebo ako neprehnať parfumáciu pri pracovnom oblečení majú spoločnú odpoveď: čistota, oplach, sušenie a nízka intenzita.",
            "Pri kancelárskom oblečení nejde o to, aby textil voňal najviac. Ide o to, aby pôsobil upravene, čisto a príjemne aj po niekoľkých hodinách nosenia. To sa dá dosiahnuť lepšou rutinou, nie iba silnejšou dávkou vône.",
        ],
        "why": [
            "Košeľa, blúzka a tričko pod sako sú v blízkosti nosa. Golier sa dotýka krku, zachytáva pot, kožný maz, make-up, krém, parfum aj zvyšky vlasových produktov. Ak sa tieto zvyšky nevymyjú, vôňa do prania sa s nimi počas dňa spojí.",
            "Kancelárske oblečenie býva často zmesové: bavlna s elastanom, viskóza, polyesterová podšívka, jemné blúzkové materiály alebo formálne textílie, ktoré neznesú hrubé pranie. Preto je dôležité pozerať na štítok a riešiť špecifické miesta, nie automaticky zvyšovať teplotu alebo vôňu.",
            "V práci vstupuje do hry aj ohľaduplnosť. Nie každý znáša vône rovnako. Človek, ktorý používa vôňu denne, si na ňu rýchlo zvykne, ale kolega pri stole ju môže vnímať silnejšie. To neznamená, že vôňu nemôžete používať; znamená to, že nízka intenzita je praktickejšia.",
            "Ak sa pracovné oblečenie suší v malej kúpeľni alebo v zle vetranej miestnosti, môže získať zatuchnutý podtón ešte pred tým, ako si ho oblečiete. Potom sa počas dňa zohreje a problém sa zvýrazní. Aj pri kancelárskom oblečení teda platí, že sušenie je súčasť vône.",
        ],
        "rows": [
            ("Košeľa alebo blúzka pri krku", "nízka intenzita vône, dôkladný oplach", "golier je blízko nosa a zachytáva kozmetiku"),
            ("Tričko pod sako", "riešiť podpazušie a pot, neprekrývať pach", "pri zahriatí tela sa pach rýchlo vráti"),
            ("Šatka a šál do práce", "veľmi jemná vôňa alebo bez parfumácie", "textil je pri tvári celý deň"),
            ("Sako a štruktúrované kusy", "riadiť sa štítkom, často lokálne alebo čistiareň", "výstuže a podšívka sa môžu poškodiť"),
            ("Pracovné oblečenie v malej kancelárii", "menej výrazná vôňa", "uzavretý priestor zvyšuje vnímanie intenzity"),
        ],
        "steps": [
            "Rozdeľte kancelárske oblečenie podľa materiálu: košele, blúzky, tričká, šatky a saká neperte automaticky jedným programom.",
            "Pred praním skontrolujte golier, podpazušie a manžety. Ak sú mastnejšie alebo zažltnuté, potrebujú jemné predbežné ošetrenie.",
            "Použite primeranú dávku pracieho gélu a nepreplňte bubon, aby sa zvyšky z goliera a podpazušia dobre vypláchli.",
            "Parfum do prania dávkujte nižšie než pri posteľnej bielizni alebo dekách. Pri pracovnom oblečení je dôležitá nevtieravosť.",
            "Pri blúzkach, viskóze, elastane a jemných materiáloch znížte trenie, otáčky a teplo podľa štítku.",
            "Sušte vo vzdušnom priestore. Košele a blúzky zaveste tak, aby sa nekrčili a aby golier nezostal vlhký.",
            "Pred väčším používaním otestujte jednu vôňu na jednom pracovnom kuse a sledujte, ako pôsobí po celom dni.",
        ],
        "decision_rows": [
            ("Vôňa je v práci príliš výrazná", "znížiť dávku alebo používať vôňu iba na menej kontaktné kusy", "v uzavretom priestore sa aróma vníma silnejšie"),
            ("Golier po dni zapácha", "riešiť predpranie goliera a oplach", "pach je v kontakte s pokožkou, nie v nedostatku vône"),
            ("Tričko pod sakom smrdí", "prať skôr, menšia dávka a bez preplnenia bubna", "pot sa pri vrstvení zahrieva"),
            ("Blúzka stratila tvar", "ďalšie pranie nerobiť naslepo", "materiál môže byť citlivý na teplo a trenie"),
            ("Kolegovia vôňu vnímajú", "prechod na jemnejšiu dávku", "cieľ je čistý dojem, nie dominancia vône"),
        ],
        "mistakes": [
            "Použiť rovnakú dávku vône na košeľu pri krku ako na posteľnú bielizeň.",
            "Prekryť pach z podpazušia parfumom namiesto lepšieho prania.",
            "Miešať vôňu bielizne so silným deodorantom, osobným parfumom a avivážou.",
            "Prať sako doma bez kontroly štítku a konštrukcie odevu.",
            "Nechať košele schnúť natesno v malej kúpeľni.",
            "Testovať novú vôňu prvýkrát na oblečení na dôležité rokovanie.",
        ],
        "detail_sections": [
            ("Ako prať košeľu, aby golier nepôsobil ťažko", "Golier najprv skontrolujte na mastnejšie miesta od krku, vlasových produktov a parfumu. Pri bežnej bavlnenej košeli pomôže jemné ošetrenie pred praním a dostatočný oplach. Pri farebných alebo jemných košeliach netrite agresívne, aby ste nepoškodili látku alebo farbu. Vôňu pridajte až po tom, čo je golier skutočne čistý."),
            ("Ako prevoňať blúzku bez presýtenia", "Blúzky bývajú z viskózy, polyesteru, saténových zmesí alebo elastických materiálov. To znamená, že treba šetriť teplo aj mechaniku. Vôňu voľte skôr ľahkú a dávkujte opatrne, pretože blúzka je často pri krku a v uzavretej kancelárii pôsobí intenzívnejšie."),
            ("Ako zladiť vôňu bielizne s deodorantom a parfumom", "Ak používate osobný parfum, vôňa bielizne by mala byť pozadie. Silný parfum do prania, výrazný deodorant a osobná vôňa môžu spolu pôsobiť chaoticky. Praktické riešenie je vybrať jednu dominantnú vôňu a ostatné držať jemné."),
            ("Ako myslieť na citlivosť v kancelárii", "Niektorí ľudia sú na vône citlivejší. V práci preto dávajte prednosť čistému a jemnému výsledku. Ak sedíte v open space, cestujete autom s kolegami alebo často chodíte na stretnutia, menej intenzívna vôňa je profesionálnejšia aj praktickejšia."),
        ],
        "caution": [
            "Pri ľuďoch, ktorí reagujú na vône bolesťou hlavy, podráždením dýchacích ciest alebo kožnou reakciou, je lepšie prať osobné pracovné oblečenie neutrálnejšie. Vôňa nemá byť povinná súčasť upraveného oblečenia.",
            "Pri sakách, kabátoch, podšívkach a odevoch s výstužou vždy sledujte štítok. Domáce pranie môže deformovať tvar. Vôňu pridávajte iba tam, kde je textil bezpečne prateľný a úplne suchý.",
        ],
        "expert": [
            "Pri pracovnom oblečení je dôležité, že textil zostáva celý deň v mikrovetranom priestore medzi telom a vrstvami. Košeľa pod svetrom alebo tričko pod sakom sa zahrieva, a preto sa zvyšky potu, deodorantu a vône uvoľňujú inak než na čerstvo zloženej bielizni.",
            "Dermatologické zdroje upozorňujú, že vonné látky môžu u citlivých ľudí vyvolať kontaktnú reakciu. Prakticky to neznamená zákaz vôní pre každého, ale opatrnosť pri textíliách, ktoré sa dotýkajú krku, podpazušia a tváre.",
            "Odborná literatúra k prchavým látkam z vonných produktov pripomína aj význam dávkovania a vetrania. Pri kancelárskom oblečení je preto najlepší výsledok taký, ktorý je príjemný na blízko, ale neplní celú miestnosť.",
        ],
        "sources": [
            ("DermNet: Fragrance allergy", "https://dermnetnz.org/topics/fragrance-allergy"),
            ("DermNet: Textile contact dermatitis", "https://dermnetnz.org/topics/textile-contact-dermatitis"),
            ("PubMed: Scented products and volatile compounds", "https://pubmed.ncbi.nlm.nih.gov/21245015/"),
        ],
        "sales": {
            "heading": "Jemná vôňa pre pracovné oblečenie",
            "intro": "Pri košeliach a blúzkach je rozumné začať malým testom. V práci má vôňa pôsobiť čisto a nerušivo, preto je výber intenzity dôležitejší než sila prvého dojmu.",
            "product_name": "Sada vzoriek najpredávanejších vôní VEVO 3 x 10ml",
            "product_href": "/p-1574/sada-vzoriek-najpredavanejsich-voni-vevo-3-x-10ml",
            "fit": "keď chcete zistiť, ktorá vôňa je na košeľu, blúzku alebo pracovné tričko dostatočne jemná po celom dni nosenia.",
            "boundary": "ak oblečenie zapácha v podpazuší alebo pri golieri, najprv riešte prací postup a odstránenie potu.",
            "product_button": "Vyskúšať vzorky vôní",
            "category_title": "Vyberte vôňu na oblečenie, nie iba podľa fľaše",
            "category_intro": "Vôňa na pracovnom oblečení sa má hodnotiť po vysušení a nosení. Pri výbere preto myslite na materiál, kontakt s krkom a uzavretý priestor.",
            "category_bullets": [
                ("Košeľa a blúzka", "jemná intenzita, aby vôňa nerušila pri krku."),
                ("Tričko pod sako", "najprv odstrániť pot, potom pridať ľahký tón."),
                ("Šatka a golier", "testovať veľmi opatrne, pretože sú pri tvári."),
            ],
            "category_href": "/c/vevo-fragrance/parfum-do-prania",
            "category_button": "Pozrieť parfumy do prania",
        },
        "related": [
            ("Ako prať sako doma a kedy ho radšej dať do čistiarne", "/n/ako-prat-sako-doma-a-kedy-ho-radsej-dat-do-cistiarne"),
            ("Ako odstrániť podkladový krém z goliera blúzky a košele", "/n/ako-odstranit-podkladovy-krem-z-goliera-bluzky-a-kosele"),
            ("Ako odstrániť parfumový fľak z oblečenia a jemných látok", "/n/ako-odstranit-parfumovy-flak-z-oblecenia-a-jemnych-latok"),
            ("Ako vybrať vôňu do prania na zimu", "/n/ako-vybrat-vonu-do-prania-na-zimu-deky-svetre-saly-a-sezonne-textilie"),
            ("Prečo bolí hlava z vône a ako používať vône doma jemnejšie", "/n/preco-boli-hlava-z-vone-a-ako-pouzivat-vone-doma-jemnejsie"),
        ],
        "faq": [
            ("Aká vôňa do prania je vhodná do kancelárie?", "Skôr jemná, čistá a menej sladká. V práci je dôležité, aby vôňa nerušila v uzavretom priestore."),
            ("Môžem používať parfum do prania na košele?", "Áno, ak košeľa nie je z materiálu, ktorý vyžaduje špeciálnu starostlivosť, a ak dávkujete opatrne."),
            ("Prečo košeľa pri krku zapácha aj po praní?", "Často ide o kožný maz, pot, make-up, krém alebo zvyšky parfumu v golieri. Treba riešiť konkrétne miesto, nie iba pridať vôňu."),
            ("Ako sa vyhnúť tomu, že vôňa bude v práci príliš silná?", "Začnite nižšou dávkou, netestujte na celej várke a vôňu kombinujte opatrne s deodorantom a osobným parfumom."),
            ("Čo ak kolegom vôňa prekáža?", "Znížte intenzitu alebo perte pracovné oblečenie neutrálnejšie. Čistý dojem sa dá dosiahnuť aj bez výraznej parfumácie."),
        ],
    },
    {
        "title": "Vôňa oblečenia v kufri: ako baliť čistú bielizeň na cestu, aby nezatuchla",
        "short": "Oblečenie v kufri vydrží svieže najmä vtedy, keď je pred balením úplne suché, dobre opláchnuté a oddelené od topánok, kozmetiky, vlhkých uterákov a už nosenej bielizne. Parfum do prania môže pomôcť s príjemným dojmom po otvorení batožiny, ale nezachráni vlhký kufor, napoly suché oblečenie ani zmiešané čisté a použité veci.",
        "situations": [
            "čisté tričká po príchode z kufra pôsobia zatuchnuto",
            "bielizeň bola zabalená ešte mierne vlhká alebo teplá po sušení",
            "topánky, kozmetika a čisté oblečenie sú v jednej časti batožiny",
            "po dovolenke sa pach použitej bielizne prenesie na čisté kusy",
            "hotelová izba je vlhká a oblečenie v skrini rýchlo stratí sviežosť",
            "chcete, aby posteľné veci, pyžamo alebo tričká na cestu jemne voňali",
            "neviete, či pred cestou použiť viac parfumu do prania alebo radšej riešiť balenie",
            "balíte sa na chatu, služobnú cestu, internát alebo dlhší pobyt v aute",
        ],
        "quick": [
            "<strong>Do kufra patrí iba úplne suché oblečenie.</strong> Zvyšková vlhkosť je najrýchlejšia cesta k zatuchnutiu.",
            "<strong>Čisté a použité veci držte oddelene.</strong> Jedno vlhké tričko alebo uterák môže pokaziť celú batožinu.",
            "<strong>Topánky a kozmetiku izolujte.</strong> Pachy a mastné zvyšky sa ľahko prenášajú na textil.",
            "<strong>Vôňu používajte ako doplnok pred cestou.</strong> Najprv pranie, oplach, sušenie a balenie; až potom jemná aróma.",
            "<strong>Po návrate kufor vyvetrajte.</strong> Ak ho zatvoríte s vlhkosťou a pachmi, pri ďalšej ceste sa problém vráti.",
        ],
        "intro": [
            "Kufor je malý uzavretý priestor. Oblečenie v ňom nemá vzduch ako v skrini a dotýka sa topánok, kozmetiky, tašiek, vlhkých uterákov, cestovného vankúša alebo použitej bielizne. Preto sa môže stať, že veci vypraté deň pred cestou po príchode nepôsobia tak sviežo, ako keď ste ich balili.",
            "Najčastejšia chyba je hľadať riešenie iba vo vôni. Pridať viac parfumu do prania môže zlepšiť prvý dojem po otvorení batožiny, ale ak balíte oblečenie mierne vlhké alebo ho miešate s nosenými vecami, zatuchnutie sa aj tak objaví. Vôňa v kufri začína pri vlhkosti.",
            "Pri otázkach ako ako baliť čistú bielizeň do kufra, ako zabrániť zatuchnutiu oblečenia na cestách, ako prevoňať kufor, prečo oblečenie v batožine zapácha alebo ako oddeliť použitú bielizeň od čistej je odpoveď praktická: suché textílie, oddelené zóny, priedušné vrecká a rozumná vôňa.",
            "Tento článok je pre cestovanie, chatu, služobné cesty, internát, dovolenku aj víkend mimo domu. Cieľom nie je prevoňať kufor ako difuzér, ale udržať oblečenie čisté a príjemné až do chvíle, keď ho vytiahnete.",
        ],
        "why": [
            "V uzavretom kufri sa pachy koncentrujú. Čisté oblečenie môže nasať vôňu obuvi, kozmetiky, plastového obalu, hotelovej skrine alebo použitej bielizne. Ak je v kufri vlhko, pach sa drží intenzívnejšie a textil pôsobí zatuchnuto.",
            "Dôležité je aj to, kedy balíte. Oblečenie môže byť na dotyk suché na povrchu, ale hrubší lem, golier, pás legín alebo uterák môže byť ešte mierne vlhký. V zatvorenej batožine sa takáto vlhkosť prejaví rýchlejšie než v otvorenej skrini.",
            "Vôňa do prania pred cestou funguje najlepšie na textil, ktorý je dobre vypraný a uložený oddelene. Ak ju použijete na veci, ktoré sa potom zmiešajú s topánkami alebo vlhkým uterákom, výsledok sa zmení. Nie je to chyba vône, ale balenia.",
            "Počas cesty sa mení teplota. Kufor v aute, autobuse alebo lietadle môže prejsť teplom, chladom aj kondenzáciou. Textil preto potrebuje jednoduchú ochranu: suchosť, oddelenie a po príchode rozbalenie, ak pobyt trvá dlhšie.",
        ],
        "rows": [
            ("Tričká a spodné vrstvy", "baliť úplne suché a radšej rolovať voľnejšie", "tesné stlačenie zvyšuje prenos pachov"),
            ("Pyžamo a oblečenie pri tvári", "jemná vôňa alebo neutrálne pranie", "dlhý kontakt s pokožkou a nosom"),
            ("Topánky", "samostatné vrecko mimo čistej bielizne", "pach obuvi sa ľahko prenáša"),
            ("Použitá bielizeň", "oddelené priedušné alebo prateľné vrecko", "vlhkosť a pot nesmú ísť k čistým kusom"),
            ("Vlhký uterák alebo plavky", "nebaliť s čistým oblečením", "vlhkosť je hlavný spúšťač zatuchnutia"),
        ],
        "steps": [
            "Pred balením perte s dostatočným predstihom, aby oblečenie stihlo úplne preschnúť aj v švoch a lemoch.",
            "Nepoužívajte viac pracieho gélu len preto, že idete cestovať. Zvyšky v textile môžu v kufri pôsobiť ťažko.",
            "Ak chcete jemnú vôňu, pridajte ju pri praní, nie až sprejovaním vlhkého alebo zbaleného textilu v kufri.",
            "Rozdeľte batožinu na čisté oblečenie, topánky, kozmetiku, použité veci a vlhké veci. Každá skupina potrebuje vlastné miesto.",
            "Pri dlhšom pobyte po príchode časť vecí vyberte a nechajte ich dýchať v skrini alebo na vešiaku.",
            "Použité oblečenie po nosení nenechávajte voľne medzi čistými kusmi. Oddelenie je dôležitejšie než silnejšia vôňa.",
            "Po návrate vyprázdnite kufor, vyvetrajte ho a prípadne utrite vnútro, ak sa preniesol pach topánok alebo kozmetiky.",
        ],
        "decision_rows": [
            ("Oblečenie zatuchlo po príchode", "skontrolovať vlhkosť pred balením a oddelenie topánok", "kufor zvýrazní aj malé zvyšky vlhkosti"),
            ("Vôňa sa stratila", "riešiť sušenie a nepreplnenie batožiny", "uzavretý priestor mení prvý dojem z bielizne"),
            ("Čisté kusy voňajú po topánkach", "topánky baliť samostatne", "pach sa prenáša kontaktom aj vzduchom v kufri"),
            ("Po dovolenke páchne všetko", "použitú bielizeň oddeliť hneď po nosení", "pot a vlhkosť sa nesmú miešať s čistými vecami"),
            ("Hotelová skriňa je zatuchnutá", "nechať oblečenie v kufri s otvorením alebo použiť vlastné vrecká", "skladovací priestor môže byť zdroj pachu"),
        ],
        "mistakes": [
            "Zabaliť oblečenie hneď po sušení, kým je ešte teplé alebo mierne vlhké.",
            "Pridať priveľa vône namiesto riešenia vlhkosti a oddelenia použitých vecí.",
            "Baliť topánky priamo k tričkám a spodnej bielizni.",
            "Nechať použité športové veci voľne medzi čistou bielizňou.",
            "Sprejovať vôňu do zatvoreného kufra na textil, ktorý už páchne.",
            "Po návrate zavrieť kufor s pachom dovolenky až do ďalšej cesty.",
        ],
        "detail_sections": [
            ("Ako baliť čisté tričká a spodnú bielizeň", "Tričká a spodnú bielizeň balte až vtedy, keď sú úplne suché. Ak ich rolujete, nerobte príliš tesné balíky z vlhkého textilu. Pri jemne prevoňanej bielizni je lepšie uložiť veci voľnejšie a oddeliť ich od kozmetiky, aby sa vôňa nemiešala s krémami alebo šampónom."),
            ("Ako udržať pyžamo a oblečenie pri tvári svieže", "Pyžamo, šál, tričko na spanie alebo cestovné oblečenie pri tvári potrebuje jemnejšiu vôňu než bežné tričko. Ak ste citlivejší na vône, perte tieto kusy neutrálnejšie a vôňu nechajte skôr na oblečenie, ktoré nie je celý čas pri nose."),
            ("Ako oddeliť použité oblečenie počas pobytu", "Použité oblečenie nenechávajte voľne v kufri. Vlhké športové veci, ponožky a spodná bielizeň patria do samostatného vrecka. Ak sú mokré, najprv ich nechajte preschnúť. Zatvorený sáčok s vlhkým textilom síce ochráni čisté veci, ale pach vnútri ešte zhorší."),
            ("Ako sa starať o kufor po návrate", "Po návrate kufor vyprázdnite a nechajte otvorený. Vyberte omrvinky, vlasy, prach a prípadné zvyšky kozmetiky. Ak vnútro páchne, vyvetrajte ho skôr, než doň nabudúce vložíte čistú bielizeň. Čisté oblečenie v špinavom kufri nemá šancu zostať svieže."),
        ],
        "caution": [
            "Ak kufor alebo textil zapácha po plesni, vlhkosti alebo dlhom skladovaní, vôňu nepoužívajte ako prvý krok. Najprv riešte sucho, vetranie a čistenie batožiny.",
            "Pri deťoch, citlivej pokožke alebo ľuďoch, ktorým prekážajú vône v malom priestore, dávkujte parfum do prania opatrne. V hotelovej izbe, aute alebo malej kajute sa vôňa môže zdať intenzívnejšia než doma.",
        ],
        "expert": [
            "Z pohľadu domácnosti je zatuchnutie v kufri najmä otázka vlhkosti a prenosu pachov. Uzavretý objem batožiny neodpúšťa napoly suché textílie ani kontakt čistých a použitých vecí. Preto je najlepšou prevenciou suché balenie a oddelené zóny.",
            "Odporúčania k vlhkosti v interiéri zdôrazňujú, že kontrola zdroja vlhkosti je základom prevencie zatuchnutia a plesní. Pri kufri ide o ten istý princíp v malom: vlhký uterák alebo tričko vytvorí lokálny problém, ktorý sa prenesie na ostatný textil.",
            "Vône sú prchavé a v uzavretom priestore batožiny sa môžu miešať s kozmetikou, plastmi alebo obuvou. Preto je lepšie mať jednu jemnú vôňu z prania než viac zdrojov arómy priamo v kufri.",
        ],
        "sources": [
            ("US EPA: A Brief Guide to Mold, Moisture and Your Home", "https://www.epa.gov/mold/brief-guide-mold-moisture-and-your-home"),
            ("Energy.gov: Laundry tips", "https://www.energy.gov/energysaver/laundry"),
            ("US EPA: Volatile Organic Compounds and Indoor Air Quality", "https://www.epa.gov/indoor-air-quality-iaq/volatile-organic-compounds-impact-indoor-air-quality"),
        ],
        "sales": {
            "heading": "Vôňa na cestu začína pri praní pred balením",
            "intro": "Ak chcete, aby kufor po otvorení pôsobil sviežo, vyberte jemnú vôňu pri praní a potom oblečenie dobre vysušte a oddeľte od zdrojov pachu.",
            "product_name": "Sada vzoriek najpredávanejších vôní VEVO 3 x 10ml",
            "product_href": "/p-1574/sada-vzoriek-najpredavanejsich-voni-vevo-3-x-10ml",
            "fit": "keď chcete pred cestou otestovať vôňu na tričkách, pyžame alebo posteľnej bielizni bez veľkého balenia.",
            "boundary": "ak je kufor vlhký alebo páchne po topánkach, najprv ho vyvetrajte a vyčistite. Vôňa nemá zakryť špinavú batožinu.",
            "product_button": "Vyskúšať vzorky vôní",
            "category_title": "Vyberte si vôňu pred cestou",
            "category_intro": "Pri cestovaní sa oplatí voliť vôňu, ktorá je príjemná aj v malom priestore kufra, auta alebo hotelovej izby.",
            "category_bullets": [
                ("Tričká a pyžamo", "jemná vôňa, ktorá neprekáža pri dlhšom kontakte."),
                ("Cestovanie autom alebo lietadlom", "nižšia intenzita, pretože priestor je uzavretý."),
                ("Dlhší pobyt", "oddelené čisté a použité veci sú dôležitejšie než silná aróma."),
            ],
            "category_href": "/c/vzorky/parfum-do-prania-vzorky",
            "category_button": "Pozrieť vzorky parfumov do prania",
        },
        "related": [
            ("Ako prať cestovné oblečenie po dlhom lete alebo vlaku", "/n/ako-prat-cestovne-oblecenie-po-dlhom-lete-alebo-vlaku"),
            ("Ako odstrániť zápach z cestovného vankúša po lietadle", "/n/ako-odstranit-zapach-z-cestovneho-vankusa-po-lietadle"),
            ("Ako prevoňať bielizeň v malej kúpeľni", "/n/ako-prevonat-bielizen-v-malej-kupelni-vlhkost-susenie-a-jemna-vona-bez-zatuchnutia"),
            ("Ako vybrať vôňu do prania na zimu", "/n/ako-vybrat-vonu-do-prania-na-zimu-deky-svetre-saly-a-sezonne-textilie"),
            ("Ako vybrať prací gél podľa typu bielizne", "/n/ako-vybrat-praci-gel-podla-typu-bielizne"),
        ],
        "faq": [
            ("Prečo čisté oblečenie v kufri zatuchne?", "Najčastejšie preto, že nebolo úplne suché, bolo príliš natlačené alebo sa miešalo s topánkami, kozmetikou či použitou bielizňou."),
            ("Mám dať do prania pred cestou viac vône?", "Nie automaticky. Dôležitejšie je dobré pranie, oplach, sušenie a oddelenie vecí v kufri."),
            ("Ako baliť topánky, aby neprevoňali oblečenie?", "Do samostatného vrecka a mimo spodnej bielizne, pyžama a tričiek. Ak sú topánky vlhké, nechajte ich najprv vyschnúť."),
            ("Čo robiť s použitou bielizňou na dovolenke?", "Držte ju oddelene a vlhké veci najprv presušte. Nepoužívajte jeden uzavretý priestor pre čisté aj spotené textílie."),
            ("Je vhodný interiérový sprej do kufra?", "Pri kufri je lepšie riešiť čistotu a suchosť batožiny. Ak chcete vôňu, bezpečnejšie je použiť jemnú vôňu už pri praní textilu."),
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
    html.append("<h2>Prečo nestačí len pridať viac vône</h2>")
    html.extend(f"<p>{paragraph}</p>" for paragraph in article["why"])
    html.append("<h2>Rozhodovanie podľa situácie</h2>")
    html.append(table(["Situácia", "Čo sa deje", "Lepší postup"], article["rows"]))
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
        raise SystemExit(f"Link preflight failed: {failed_links[:5]}")

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_PREFLIGHT.parent.mkdir(parents=True, exist_ok=True)
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
