import json
import re
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

import requests
import xlwt


BASE = "https://www.vevo.sk"
BATCH_DATE = "2025-10-01"
OUT_JSON = Path("content/VEVO_CONTENT/imports/batch-21-2026-06-10-articles.json")
OUT_XLS = Path.home() / "AppData/Local/Temp/vevo-batch-21-material-guides-clean-urls.xls"


def slugify(value):
    value = unicodedata.normalize("NFKD", value)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = re.sub(r"[^a-zA-Z0-9]+", "-", value.lower())
    return re.sub(r"-+", "-", value).strip("-")


def table(headers, rows):
    head = "".join(
        f'<th style="border: 1px solid #e5e5e5; padding: 10px; text-align: left;">{h}</th>'
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


def quick_box(title, bullets):
    items = "".join(f"<li>{item}</li>" for item in bullets)
    return (
        '<div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">\n'
        f'<h2 style="margin-top: 0;">{title}</h2>\n<ul>{items}</ul>\n</div>'
    )


def recommendation(category_href="/c/vevo-home-care/pranie/praci-gel", category_label="Pozrieť kategóriu pracie gély"):
    return """
<div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 24px 0; background: #f7fbf8;">
<h2 style="margin-top: 0;">Odporúčané riešenie z VEVO</h2>
<p>Pri materiálových článkoch je základom správny program, primerané dávkovanie a dôkladné sušenie. Produkt má pomáhať čistote textilu, nie prekrývať pot, zatuchnutie alebo zvyšky pracieho prostriedku.</p>
<div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff; margin: 14px 0;">
<h3 style="margin-top: 0;">Prací gél hypoalergénny z Marseillského mydla 1L</h3>
<p>Šetrný základ na bežné pranie mnohých textílií. Pri špeciálnych funkčných materiáloch, membránach, vlne alebo veľmi jemných kusoch vždy rešpektujte štítok výrobcu.</p>
<p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="/p-1551/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l">Pozrieť produkt</a></p>
</div>
<p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="{category_href}">{category_label}</a></p>
</div>
""".strip().format(category_href=category_href, category_label=category_label)


def sources(items):
    rows = "".join(f'<li><a rel="noopener" href="{href}" target="_blank">{label}</a></li>' for label, href in items)
    return (
        '<div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; margin: 22px 0; background: #fbfbfb;">\n'
        '<h2 style="margin-top: 0;">Zdroje a odborný kontext</h2>\n'
        f"<ul>{rows}</ul>\n</div>"
    )


def related(items):
    links = "".join(f'<li><a href="{href}">{label}</a></li>' for label, href in items)
    return f"<h2>Súvisiace návody na VEVO</h2>\n<ul>{links}</ul>"


ARTICLES = [
    {
        "title": "Recyklovaný polyester: čo znamená, aké má výhody a ako sa oň starať",
        "short": "Recyklovaný polyester je polyester vyrobený z už existujúceho plastového alebo textilného materiálu. Pri praní sa správa podobne ako bežný polyester.",
        "keywords": "recyklovaný polyester, rPET, ako prať recyklovaný polyester, recyklovaný polyester výhody, recyklovaný polyester nevýhody",
        "quick_title": "Rýchly praktický záver",
        "quick": [
            "<strong>Recyklovaný polyester nie je jemnejší automaticky.</strong> Stále ide o polyesterové vlákno a treba ho prať podľa štítku.",
            "<strong>Pri športe riešte pot a pach.</strong> Materiál môže rýchlo schnúť, ale spotené tričko nenechávajte zavreté v taške.",
            "<strong>Nepreháňajte teplotu.</strong> Vyššia teplota nemusí lepšie odstrániť zápach a môže poškodiť potlač alebo elastan v zmesi.",
            "<strong>Pri nákupe sledujte celé zloženie.</strong> rPET na štítku nevypovedá o všetkom: dôležitá je aj gramáž, väzba a povrchová úprava.",
        ],
        "intro": [
            "Recyklovaný polyester sa často označuje aj ako rPET. V oblečení, dekách, výplniach a športových doplnkoch zvyčajne znamená, že časť polyesterového vlákna vznikla z už existujúceho plastového alebo textilného vstupu. Z pohľadu prania je však najdôležitejšie to, že výsledný materiál sa stále správa ako polyester.",
            "Veľa ľudí očakáva, že recyklovaný materiál bude automaticky citlivejší, ekologickejší alebo úplne iný na dotyk. V praxi záleží na kvalite vlákna, konštrukcii látky, zmesi s elastanom alebo bavlnou a na tom, ako sa textil používa. Recyklovaný polyester môže byť výborný na šport, cestovanie a outdoor, no pri potení vyžaduje rovnakú disciplínu ako bežná syntetika.",
        ],
        "property_rows": [
            ("Pôvod", "vyrobený z existujúceho polyesterového alebo plastového zdroja", "stále prať ako polyester, nie ako bavlnu"),
            ("Schnutie", "zvyčajne rýchle", "nenechať vlhké v práčke, taške alebo koši"),
            ("Zápach", "pri pote sa môže držať podobne ako bežný polyester", "menšia dávka gélu, viac oplachu, rýchle sušenie"),
            ("Tvar", "často dobre drží tvar", "pozor na elastan, potlače a horúcu sušičku"),
        ],
        "care_rows": [
            ("Športové tričko z rPET", "Prať naruby čo najskôr po spotení.", "Pach vzniká hlavne z potu, mazu a vlhkosti."),
            ("Bunda alebo mikina", "Zapnúť zipsy, prať podľa štítku, sušiť vzdušne.", "Konštrukcia výrobku je dôležitejšia než samotné slovo recyklovaný."),
            ("Deky a doplnky", "Nepreplniť bubon a úplne vysušiť.", "Zatuchnutie často vzniká až pri skladovaní vlhkého textilu."),
        ],
        "mistakes": [
            "Nebrať recyklovaný polyester ako dôvod ignorovať štítok.",
            "Neprať spotené oblečenie až po niekoľkých dňoch v uzavretej športovej taške.",
            "Nepoužívať aviváž pri funkčných kusoch, ak ju výrobca neodporúča.",
            "Nespoliehať sa na silnú vôňu, ak textil stále drží pot.",
        ],
        "expert": "Recyklovaný polyester sa v textilnom priemysle rieši hlavne ako materiálová a environmentálna téma. Pri bežnej domácnosti je však najpraktickejší záver jednoduchý: spôsob prania sa nemení len preto, že je polyester recyklovaný. Typ vlákna, povrch látky a zmes s inými vláknami stále ovplyvňujú pach, sušenie a životnosť.",
        "sources": [
            ("Britannica: Polyester", "https://www.britannica.com/science/polyester"),
            ("Textile Exchange: Materials Market Report", "https://textileexchange.org/knowledge-center/reports/materials-market-report-2024/"),
            ("Microbial odor profile of polyester and cotton clothes", "https://pmc.ncbi.nlm.nih.gov/articles/PMC4249026/"),
        ],
        "related": [
            ("Čo je polyester a ako ho prať, aby nezapáchal", "/n/co-je-polyester-a-ako-ho-prat-aby-nezapachal"),
            ("Polyester vs bavlna: rozdiely pri nosení, praní a vôni", "/n/polyester-vs-bavlna-rozdiely-pri-noseni-prani-a-voni"),
            ("Ako prať syntetiku, polyester a elastan", "/n/ako-prat-syntetiku-polyester-a-elastan-aby-nezapachali-a-drzali-tvar"),
        ],
        "faq": [
            ("Perie sa recyklovaný polyester inak ako bežný polyester?", "Väčšinou nie. Rozhoduje štítok, zmes materiálu, potlač a účel oblečenia."),
            ("Je recyklovaný polyester vhodný na šport?", "Áno, často sa používa v športových materiáloch, ale po spotení ho treba rýchlo vyprať alebo aspoň presušiť."),
            ("Môžem použiť parfum do prania?", "Áno, ale až na čistý textil. Pri športovej syntetike dávkujte opatrne a neprekrývajte pot vôňou."),
        ],
    },
    {
        "title": "Čo je polyamid alebo nylon: vlastnosti, odolnosť a pranie",
        "short": "Polyamid, známy aj ako nylon, je pevné syntetické vlákno používané v pančuchách, športových látkach, bundách, batohoch a zmesových materiáloch.",
        "keywords": "čo je polyamid, čo je nylon, polyamid vlastnosti, ako prať polyamid, nylon pranie, polyamid športové oblečenie",
        "quick_title": "Rýchly praktický záver",
        "quick": [
            "<strong>Polyamid je odolné syntetické vlákno.</strong> Často ho nájdete v pančuchách, spodnej bielizni, outdoorových doplnkoch a športových zmesiach.",
            "<strong>Pri praní chráňte tvar.</strong> Jemnejšie kusy perte vo vrecku a naruby.",
            "<strong>Teplo používajte opatrne.</strong> Pri zmesi s elastanom a potlačami sa vyhnite horúcej sušičke.",
            "<strong>Zápach riešte rýchlo.</strong> Spotené polyamidové oblečenie nenechávajte dlho vlhké.",
        ],
        "intro": [
            "Polyamid je skupina syntetických vlákien, z ktorých najznámejší je nylon. V praxi ho poznáte z pančúch, športového oblečenia, spodnej bielizne, podšívok, batohov, vetroviek a rôznych elastických zmesí. Je obľúbený preto, že je pevný, ľahký a pomerne odolný voči oderu.",
            "Pri praní polyamidu je podstatné rozlišovať, či ide o samostatné vlákno alebo zmes s elastanom, bavlnou, vlnou či membránou. Jemné pančuchy potrebujú iný postup než batohový textil alebo športová vrstva. Základné pravidlo je prať šetrne, nepreplniť bubon a nepoužívať zbytočné teplo.",
        ],
        "property_rows": [
            ("Pevnosť", "dobrá odolnosť pri nízkej hmotnosti", "vhodný na šport a technické textílie"),
            ("Oder", "znesie trenie lepšie než mnohé jemné vlákna", "stále chrániť zipsy, háčiky a suché zipsy"),
            ("Schnutie", "zvyčajne rýchle", "sušiť voľne a neskladať vlhké"),
            ("Citlivosť zmesí", "často sa mieša s elastanom", "nízka teplota a šetrné otáčky chránia pružnosť"),
        ],
        "care_rows": [
            ("Nylonové pančuchy", "Prať vo vrecku, jemný program alebo ručne podľa štítku.", "Chrániť pred zipsami a háčikmi."),
            ("Polyamidové športové tričko", "Prať naruby, nepreplniť bubon, dôkladne opláchnuť.", "Pot sa drží hlavne pri pokožke a švoch."),
            ("Outdoorová zmes", "Postupovať podľa najcitlivejšej časti výrobku.", "Membrána alebo povrchová úprava mení pravidlá."),
        ],
        "mistakes": [
            "Nepridávať aviváž automaticky ku každému športovému polyamidu.",
            "Nesušiť horúco jemné a pružné zmesi.",
            "Neprať pančuchy spolu so zipsami, suchými zipsami alebo háčikmi.",
            "Nezamieňať odolnosť vlákna s nezničiteľnosťou hotového odevu.",
        ],
        "expert": "Nylon bol jedným z prvých široko používaných syntetických vlákien a jeho popularita súvisí s pevnosťou a odolnosťou. V domácnosti sa to prejaví tak, že polyamidové výrobky často vydržia časté používanie, ale pri zmesiach treba chrániť elastan, potlače a povrchové úpravy.",
        "sources": [
            ("Britannica: Nylon", "https://www.britannica.com/science/nylon"),
            ("Microbial odor profile of polyester and cotton clothes", "https://pmc.ncbi.nlm.nih.gov/articles/PMC4249026/"),
        ],
        "related": [
            ("Ako prať syntetiku, polyester a elastan", "/n/ako-prat-syntetiku-polyester-a-elastan-aby-nezapachali-a-drzali-tvar"),
            ("Čo je elastan", "/n/co-je-elastan-preco-je-v-leginach-spodnej-bielizni-a-sportovom-obleceni"),
            ("Kedy nepoužívať aviváž", "/n/kedy-nepouzivat-avivaz-uteraky-sportove-oblecenie-softshell-aj-detska-bielizen"),
        ],
        "faq": [
            ("Je polyamid to isté ako nylon?", "Nylon je najznámejší typ polyamidu. V bežnom oblečení sa tieto slová často používajú veľmi podobne."),
            ("Ako prať nylonové pančuchy?", "Najbezpečnejšie vo vrecku na jemnú bielizeň, na šetrnom programe alebo ručne podľa štítku."),
            ("Môže ísť polyamid do sušičky?", "Iba ak to povoľuje štítok. Pri zmesiach s elastanom je bezpečnejšie sušenie na vzduchu."),
        ],
    },
    {
        "title": "Polyamid vs polyester: ktorý materiál lepšie znáša pot, šport a časté pranie",
        "short": "Polyamid aj polyester sú syntetické vlákna, ale líšia sa pocitom, odolnosťou, schnutím a správaním pri pote. Pri praní rozhoduje konkrétna zmes.",
        "keywords": "polyamid vs polyester, nylon vs polyester, polyamid alebo polyester, športové oblečenie materiál, ktorý materiál zapácha menej",
        "quick_title": "Rýchle porovnanie",
        "quick": [
            "<strong>Polyester</strong> často rýchlo schne, dobre drží tvar a používa sa v športových tričkách.",
            "<strong>Polyamid</strong> býva veľmi pevný a odolný voči oderu, často v pančuchách, spodnej bielizni a technických textíliách.",
            "<strong>Pri pachu nerozhoduje iba názov vlákna.</strong> Dôležité je potenie, zmes, strih, prací postup a sušenie.",
            "<strong>Pri zmesiach s elastanom</strong> chráňte pružnosť: nižšia teplota, menej aviváže, opatrné sušenie.",
        ],
        "intro": [
            "Otázka polyamid vs polyester sa najčastejšie rieši pri športovom oblečení, legínach, spodnej bielizni, bundách a outdoorových doplnkoch. Oba materiály patria medzi syntetické vlákna, no v praxi sa používajú trochu inak. Polyester je veľmi rozšírený v tričkách a rýchloschnúcich vrstvách, polyamid často tam, kde sa očakáva pevnosť, oder a jemnejší technický pocit.",
            "Nie je fér povedať, že jeden materiál je vždy lepší. Na beh, turistiku, fitko alebo cestovanie rozhoduje kombinácia vlákna, väzby, gramáže, elastanu a strihu. A pri praní rozhoduje aj to, ako rýchlo textil vyperiete po spotení a či ho nenecháte vlhký v taške.",
        ],
        "property_rows": [
            ("Schnutie", "polyester často veľmi rýchle", "polyamid tiež rýchle, podľa väzby"),
            ("Oder", "dobrý pri mnohých športových látkach", "často veľmi dobrá odolnosť"),
            ("Pach", "pri pote môže držať pach výrazne", "záleží od konštrukcie a zmesi"),
            ("Použitie", "tričká, dresy, mikiny, výplne", "pančuchy, spodná bielizeň, outdoor, zmesi"),
        ],
        "care_rows": [
            ("Spotene športové tričko", "Vyprať čo najskôr, naruby, bez preplnenia bubna.", "Najväčší problém je vlhkosť a zvyšky potu."),
            ("Legíny alebo spodná bielizeň", "Nízka teplota, šetrné otáčky, opatrne s avivážou.", "Elastan v zmesi potrebuje ochranu."),
            ("Outdoorový kus", "Riadiť sa štítkom a povrchovou úpravou.", "Membrána alebo impregnácia mení postup."),
        ],
        "mistakes": [
            "Nechať spotenú syntetiku zavretú v taške cez noc.",
            "Pridať dvojnásobok gélu namiesto lepšieho oplachu.",
            "Použiť aviváž na funkčné kúsky, ktoré ju nemajú odporúčanú.",
            "Sušiť horúco elastické športové zmesi.",
        ],
        "expert": "Odborné texty o zápachu športového oblečenia ukazujú, že materiál vlákna môže ovplyvniť mikrobiálny profil a vnímanie pachu po aktivite. Pre domácu prax je dôležitý záver: syntetiku treba prať včas, dávkovať rozumne a sušiť úplne. Silná vôňa nemá nahradiť odstránenie potu.",
        "sources": [
            ("Britannica: Polyester", "https://www.britannica.com/science/polyester"),
            ("Britannica: Nylon", "https://www.britannica.com/science/nylon"),
            ("Microbial odor profile of polyester and cotton clothes", "https://pmc.ncbi.nlm.nih.gov/articles/PMC4249026/"),
        ],
        "related": [
            ("Čo je polyester a ako ho prať, aby nezapáchal", "/n/co-je-polyester-a-ako-ho-prat-aby-nezapachal"),
            ("Ako prať syntetiku, polyester a elastan", "/n/ako-prat-syntetiku-polyester-a-elastan-aby-nezapachali-a-drzali-tvar"),
            ("Ako používať parfum do prania pri športovom oblečení", "/n/ako-pouzivat-parfum-do-prania-pri-sportovom-obleceni"),
        ],
        "faq": [
            ("Je na šport lepší polyamid alebo polyester?", "Záleží od konkrétnej látky. Polyester je bežný pri tričkách, polyamid pri odolných a elastických technických zmesiach."),
            ("Ktorý materiál viac zapácha?", "Nedá sa rozhodnúť iba podľa názvu vlákna. Dôležitý je pot, zmes, oplach, sušenie a to, či textil nezostal vlhký."),
            ("Ako prať športovú syntetiku?", "Naruby, bez preplnenia bubna, s primeranou dávkou gélu a dôkladným sušením."),
        ],
    },
    {
        "title": "Modal v oblečení: čo znamená, prečo je mäkký a ako ho prať",
        "short": "Modal je mäkké regenerované celulózové vlákno. V oblečení pôsobí príjemne a splývavo, ale pri praní treba chrániť tvar, farbu a zmesi.",
        "keywords": "čo je modal, modal v oblečení, modal pranie, ako prať modal, modal spodná bielizeň, modal vlastnosti",
        "quick_title": "Rýchly praktický záver",
        "quick": [
            "<strong>Modal je regenerované celulózové vlákno.</strong> Na dotyk býva mäkký, hladký a príjemný pri pokožke.",
            "<strong>Často je v spodnej bielizni, tričkách a pyžamách.</strong> Preto riešite najmä pot, jemnosť a časté pranie.",
            "<strong>Perte skôr šetrne.</strong> Naruby, podľa štítku, s primeranou dávkou gélu.",
            "<strong>Sušte opatrne.</strong> Horúca sušička môže zhoršiť tvar, najmä pri zmesi s elastanom.",
        ],
        "intro": [
            "Modal sa často objavuje v spodnej bielizni, tričkách, pyžamách, domácich šatách a jemných úpletoch. Je obľúbený pre mäkký pocit na pokožke a pekný splývavý vzhľad. Mnohí ho vnímajú ako komfortnejšiu alternatívu k bežnej viskóze alebo bavlneným zmesiam.",
            "Z materiálového pohľadu patrí modal medzi regenerované celulózové vlákna podobne ako viskóza a lyocell. To znamená, že nevzniká ako klasická bavlna priamo z rastlinného vlákna, ale ani to nie je čistá syntetika typu polyester. Pri praní treba myslieť najmä na jemnosť, zmes s elastanom a to, že mokrý úplet môže meniť tvar.",
        ],
        "property_rows": [
            ("Pocit", "mäkký, hladký, príjemný na telo", "vhodný na bielizeň, pyžamá a tričká"),
            ("Savosť", "často príjemne pracuje s vlhkosťou", "po praní dôkladne vysušiť pred uložením"),
            ("Tvar", "závisí od úpletu a zmesi", "nevešať ťažké mokré kusy za tenké ramienka"),
            ("Zmesi", "často s elastanom", "chrániť pred teplom a agresívnym žmýkaním"),
        ],
        "care_rows": [
            ("Modalové tričko", "Prať naruby, triediť podľa farby, nepreplniť bubon.", "Chrániť povrch a farbu."),
            ("Modalová spodná bielizeň", "Použiť vrecko na jemnú bielizeň, šetrný program.", "Gumičky a elastan nemajú rady teplo."),
            ("Modalové pyžamo", "Sušiť voľne, pred uložením úplne dosušiť.", "Zatuchnutie vzniká pri vlhkom skladovaní."),
        ],
        "mistakes": [
            "Prať modal automaticky na rovnakom programe ako uteráky.",
            "Vešať mokré šaty alebo dlhý úplet tak, že sa vytiahnu vlastnou váhou.",
            "Použiť príliš veľa gélu a potom nedostatočne opláchnuť.",
            "Sušiť horúco zmes modalu s elastanom.",
        ],
        "expert": "Modal patrí do širšej skupiny regenerovaných celulózových vlákien. Pre domácu starostlivosť je praktickejšie než chemická definícia sledovať, ako sa správa konkrétny výrobok: či je úplet tenký, či obsahuje elastan, či púšťa farbu a ako výrobca obmedzuje teplotu.",
        "sources": [
            ("Britannica: Rayon textile fiber", "https://www.britannica.com/technology/rayon-textile-fiber"),
        ],
        "related": [
            ("Čo je viskóza: vlastnosti, krčivosť, zrážanie a starostlivosť", "/n/co-je-viskoza-vlastnosti-krcivost-zrazanie-a-starostlivost"),
            ("Čo je elastan", "/n/co-je-elastan-preco-je-v-leginach-spodnej-bielizni-a-sportovom-obleceni"),
            ("Ako vybrať prací gél podľa typu bielizne", "/n/ako-vybrat-praci-gel-podla-typu-bielizne"),
        ],
        "faq": [
            ("Je modal prírodný materiál?", "Modal vychádza z celulózy, ale je regenerované vlákno. Nie je to klasická bavlna ani polyester."),
            ("Zráža sa modal?", "Môže meniť tvar podľa úpletu, zmesi a prania. Vždy sledujte štítok a sušte opatrne."),
            ("Je modal vhodný na citlivú pokožku?", "Mnoho ľudí ho vníma ako príjemný, ale pri citlivej pokožke rozhoduje aj prací prostriedok, farbivá a konkrétna úprava látky."),
        ],
    },
    {
        "title": "Čo je lyocell alebo Tencel: priedušnosť, jemnosť a starostlivosť",
        "short": "Lyocell je regenerované celulózové vlákno známe jemnosťou, splývavosťou a príjemným pocitom pri nosení. Tencel je známa značka lyocellu.",
        "keywords": "čo je lyocell, čo je Tencel, lyocell pranie, ako prať Tencel, lyocell vlastnosti, lyocell vs viskóza",
        "quick_title": "Rýchly praktický záver",
        "quick": [
            "<strong>Lyocell je regenerované celulózové vlákno.</strong> V oblečení pôsobí jemne, hladko a splývavo.",
            "<strong>Tencel je značka, nie úplne iný typ domáceho prania.</strong> Vždy sa riaďte štítkom konkrétneho kusu.",
            "<strong>Pri praní chráňte povrch.</strong> Perte naruby, šetrne a bez preplneného bubna.",
            "<strong>Sušenie rozhoduje o tvare.</strong> Jemné kusy sušte voľne a neukladajte ich vlhké.",
        ],
        "intro": [
            "Lyocell poznáte z tričiek, šiat, košieľ, pyžám, spodnej bielizne a niekedy aj z posteľnej bielizne. Na dotyk býva hladký, chladivý a mäkký. Tencel je známa obchodná značka lyocellových vlákien, preto sa v obchodoch často stretnete s označením lyocell/Tencel.",
            "Pri praní lyocellu je dôležité nepovažovať ho za obyčajný polyester ani za hrubú bavlnu. Je to regenerované celulózové vlákno a pri jemných úpletoch alebo zmesiach potrebuje šetrnejší postup. Najviac škody zvyčajne narobí preplnená práčka, vysoké otáčky, horúce sušenie alebo zavesenie ťažkého mokrého kusu.",
        ],
        "property_rows": [
            ("Pocit", "jemný, hladký, často chladivý", "vhodný na tričká, šaty a bielizeň"),
            ("Splývavosť", "látka pekne padá", "pri mokrom stave chrániť tvar"),
            ("Povrch", "môže byť citlivý na trenie", "prať naruby a s podobne jemnými vecami"),
            ("Zmesi", "často s elastanom alebo bavlnou", "postupovať podľa najcitlivejšej zložky"),
        ],
        "care_rows": [
            ("Lyocellové tričko", "Prať naruby, šetrný program, primeraná dávka gélu.", "Chrániť hladký povrch."),
            ("Lyocellové šaty", "Nízke otáčky, sušiť upravené do tvaru.", "Mokrý kus môže byť ťažší a vytiahnuť sa."),
            ("Posteľná bielizeň s lyocellom", "Nepreplniť bubon a dobre vysušiť.", "Vlhké skladovanie zhorší sviežosť."),
        ],
        "mistakes": [
            "Zamieňať lyocell s polyesterom a prať ho príliš mechanicky.",
            "Nechať mokré šaty visieť na úzkych ramienkach.",
            "Použiť veľa gélu a nedostatočný oplach.",
            "Sušiť horúco bez kontroly štítku.",
        ],
        "expert": "Lyocell patrí medzi regenerované celulózové vlákna podobne ako viskóza a modal. V praxi ho ľudia oceňujú pre jemnosť a komfort, no starostlivosť musí rešpektovať hotový výrobok. Rozdiel medzi vláknom a hotovou látkou je podstatný: inak sa správa hladká košeľa, inak elastický úplet a inak posteľná bielizeň.",
        "sources": [
            ("Britannica: Rayon textile fiber", "https://www.britannica.com/technology/rayon-textile-fiber"),
        ],
        "related": [
            ("Čo je viskóza: vlastnosti, krčivosť, zrážanie a starostlivosť", "/n/co-je-viskoza-vlastnosti-krcivost-zrazanie-a-starostlivost"),
            ("Ako správne prať obliečky", "/n/ako-spravne-prat-obliecky-kompletny-sprievodca-starostlivostou"),
            ("Ako často prať posteľné prádlo", "/n/ako-casto-prat-postelne-pradlo"),
        ],
        "faq": [
            ("Je Tencel to isté ako lyocell?", "Tencel je známa značka lyocellových vlákien. Pri praní sa riaďte štítkom konkrétneho výrobku."),
            ("Ako prať lyocellové šaty?", "Naruby, šetrne, s nízkymi otáčkami a sušením upraveným do tvaru."),
            ("Krčí sa lyocell?", "Môže sa krčiť podľa väzby a zmesi. Pomáha jemný program, správne sušenie a žehlenie podľa štítku."),
        ],
    },
]


def build_long(article):
    html = [
        f"<p><strong>Rýchla odpoveď:</strong> {article['short']}</p>",
        f"<p>Okrem základného vysvetlenia sa venujeme aj praktickým situáciám z domácnosti: <strong>{article['keywords']}</strong>. Nejde iba o definíciu materiálu. Dôležité je, ako sa správa pri potení, praní, sušení a bežnom používaní v domácnosti.</p>",
        quick_box(article["quick_title"], article["quick"]),
    ]
    html.extend(f"<p>{paragraph}</p>" for paragraph in article["intro"])
    html.append("<h2>Vlastnosti materiálu v praxi</h2>")
    html.append(table(["Vlastnosť", "Ako sa prejavuje", "Čo to znamená pri praní"], article["property_rows"]))
    html.append("<h2>Ako prať a sušiť tento materiál</h2>")
    html.append("<p>Pri materiálových článkoch je najbezpečnejšie začať štítkom. Potom riešte farbu, mieru potu, konštrukciu odevu a to, či textil obsahuje elastan, membránu alebo jemnú povrchovú úpravu.</p>")
    html.append(table(["Textil", "Postup", "Prečo"], article["care_rows"]))
    html.append("<h2>Najčastejšie chyby</h2>")
    html.append("<ul>" + "".join(f"<li>{item}</li>" for item in article["mistakes"]) + "</ul>")
    html.append("<h2>Odbornejší pohľad</h2>")
    html.append(f"<p>{article['expert']}</p>")
    html.append(sources(article["sources"]))
    html.append(recommendation())
    html.append(related(article["related"]))
    html.append("<h2>FAQ</h2>")
    for question, answer in article["faq"]:
        html.append(f"<h3>{question}</h3><p>{answer}</p>")
    return "\n".join(html)


def main():
    articles = []
    times = ["08:00:00", "08:12:00", "08:24:00", "08:36:00", "08:48:00"]
    for index, article in enumerate(ARTICLES):
        long = build_long(article)
        if "CTA" in long.upper():
            raise SystemExit(f"Forbidden CTA wording in {article['title']}")
        if "Cena:" in long:
            raise SystemExit(f"Fixed price wording in {article['title']}")
        if len(long) > 32700:
            raise SystemExit(f"XLS cell too long for {article['title']}: {len(long)}")
        articles.append(
            {
                "title": article["title"],
                "short": article["short"],
                "long": long,
                "date_posted": BATCH_DATE,
                "time_posted": times[index],
                "active": 1,
                "link": slugify(article["title"]),
                "commenting": "none",
            }
        )

    hrefs = sorted({href for row in articles for href in re.findall(r'href="([^"]+)"', row["long"])})
    checks = []
    for href in hrefs:
        if href.startswith("/"):
            url = urljoin(BASE, href)
        elif href.startswith("http"):
            url = href
        else:
            continue
        response = requests.get(url, timeout=30, allow_redirects=True)
        checks.append((href, response.status_code, response.url))
        if response.status_code != 200:
            raise SystemExit(f"Link preflight failed: {href} -> {response.status_code} {response.url}")

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

    print(
        json.dumps(
            {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "article_count": len(articles),
                "json": str(OUT_JSON),
                "xls": str(OUT_XLS),
                "links_checked": len(checks),
                "slugs": [article["link"] for article in articles],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
