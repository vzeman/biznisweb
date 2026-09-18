import html
import json
import re
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests


BASE = "https://www.vevo.sk"
PUBLISH_DATE = "2026-07-22"
CANDIDATES = Path("content/VEVO_CONTENT/batches/batch-42-candidates-2026-07-22.txt")
OUT_JSON = Path("content/VEVO_CONTENT/imports/batch-42-2026-07-22-articles.json")
OUT_PREFLIGHT = Path("content/VEVO_CONTENT/exports/batch-42-2026-07-22-link-preflight.json")

PRODUCT_NAME = "Prací gél hypoalergénny Vevo Ylang Absolute 1L"
PRODUCT_URL = "/p-1627/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l"
CATEGORY_NAME = "Pracie gély"
CATEGORY_URL = "/c/vevo-home-care/pranie/praci-gel"

ISO_COLOR_WASH = "https://www.iso.org/standard/51276.html"
ISO_COLOR_RUB = "https://www.iso.org/standard/65207.html"
ISO_COLOR_LIGHT = "https://www.iso.org/standard/65209.html"
AATCC_COLOR = "https://www.aatcc.org/learn/online-test-method-training/colorfastness-module"
ISO_SEAM_FORCE = "https://www.iso.org/standard/88344.html"
ISO_SEAM_SLIP = "https://www.iso.org/standard/36416.html"
ISO_NEEDLE_CLAMP = "https://www.iso.org/standard/37219.html"
ASTM_SEAM_SLIP = "https://store.astm.org/d4034_d4034m-26.html"
ASTM_SNAG_MACE = "https://store.astm.org/d3939_d3939m-26.html"
ASTM_SNAG_BAG = "https://store.astm.org/d5362-13.html"
ISO_PILLING = "https://www.iso.org/standard/75376.html"
ISO_THREAD_COUNT = "https://www.iso.org/standard/86700.html"
ASTM_THREAD_COUNT = "https://store.astm.org/d3775-17.html"
ISO_YARN_DENSITY = "https://www.iso.org/standard/74893.html"
COTTON_SHEETS = "https://cottonworks.com/wp-content/uploads/2021/03/Cotton-Sheets-Buying-Guide_Spreads.pdf"
GINETEX = "https://www.ginetex.net/share/article/4201/care-symbols"

ARTICLE_COLOR_FIRST_WASH = "/n/ako-zabranit-pustaniu-farby-pri-prani-noveho-oblecenia"
ARTICLE_BLACK = "/n/ako-prat-cierne-oblecenie-aby-nevybledlo"
ARTICLE_DENIM_COLOR = "/n/ako-prat-riflovu-bundu-a-tmave-dzinsy-aby-nepustali-farbu"
ARTICLE_COLOR_TRANSFER = "/n/pustila-farba-v-pracke-co-urobit-s-bielym-trickom-a-ruzovou-bieliznou"
ARTICLE_HOLES = "/n/ako-predist-dierkam-v-trickach-po-prani-a-suseni"
ARTICLE_ZIPS = "/n/ako-prat-oblecenie-so-zipsami-a-suchym-zipsom-bez-zatrhnutia"
ARTICLE_COTTON_ELASTANE = "/n/bavlna-a-elastan-starostlivost-o-tricka-rifle-a-spodnu-bielizen"
ARTICLE_MARTINDALE = "/n/odolnost-textilu-proti-oderu-co-znamena-martindale-pri-obleceni-a-bytovych-latkach"
ARTICLE_PILLING = "/n/preco-sa-oblecenie-zmolkuje-vlakna-trenie-pranie-a-susenie"
ARTICLE_POLYAMIDE = "/n/co-je-polyamid-alebo-nylon-vlastnosti-odolnost-a-pranie"
ARTICLE_UNDERWEAR = "/n/ako-prat-podprsenku-a-jemnu-spodnu-bielizen-bez-deformacie"
ARTICLE_BEDDING_CHOICE = "/n/bavlna-lan-saten-alebo-flanel-ake-obliecky-vybrat-podla-sezony-a-potenia"
ARTICLE_GSM = "/n/gramaz-latky-co-znamena-gsm-pri-uterakoch-oblieckach-a-trickach"
ARTICLE_COTTON = "/n/co-je-bavlna-vlastnosti-vyhody-nevyhody-a-starostlivost"
ARTICLE_SATIN = "/n/co-je-saten-nie-je-to-vzdy-hodvab-a-ako-ho-spravne-prat"
ARTICLE_BEDDING_WASH = "/n/ako-spravne-prat-obliecky-kompletny-sprievodca-starostlivostou"
ARTICLE_BEDDING_FREQUENCY = "/n/ako-casto-prat-postelne-pradlo"

ARTICLE_COLORFASTNESS = "/n/stalofarebnost-textilu-preco-farby-blednu-pri-prani-svetle-a-treni"
ARTICLE_SEAMS = "/n/pevnost-sva-a-posun-niti-preco-oblecenie-praska-pri-svoch"
ARTICLE_SNAGGING = "/n/zatrhavanie-textilu-preco-vznikaju-vytiahnute-ocka-a-ako-im-predchadzat"
ARTICLE_THREAD_COUNT = "/n/pocet-niti-pri-oblieckach-co-znamena-thread-count-a-co-o-kvalite-nehovori"

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
<h2 style="margin-top: 0;">Prací prostriedok vyberte podľa textilu a štítku</h2>
<p>{article['product_intro']}</p>
<div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff; margin: 14px 0;">
<h3 style="margin-top: 0;">{PRODUCT_NAME}</h3>
<p>{article['product_text']}</p>
<p><strong>Dôležitá hranica:</strong> {article['product_limit']}</p>
<p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="{PRODUCT_URL}">Pozrieť produkt</a></p>
</div>
</div>
<div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 24px 0; background: #fffaf5;">
<h2 style="margin-top: 0;">Porovnajte pracie gély pre bežnú domácu bielizeň</h2>
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
            parts.append(callout(note[0], note[1], background=note[2], border=note[3]))
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
        "title": "Stálofarebnosť textilu: prečo farby blednú pri praní, svetle a trení",
        "link": "stalofarebnost-textilu-preco-farby-blednu-pri-prani-svetle-a-treni",
        "meta": "Čo je stálofarebnosť textilu, prečo farby blednú alebo púšťajú a ako oblečenie chrániť pri praní, sušení, nosení aj skladovaní.",
        "short": "Farba môže strácať sýtosť praním, svetlom aj trením, pričom každý mechanizmus je odlišný. Praktický sprievodca vysvetľuje stálofarebnosť, prenos farbiva a šetrnú starostlivosť o farebný textil.",
        "answer": "Stálofarebnosť vyjadruje, ako dobre si textil zachová farbu a ako málo farbiva prenesie na iný materiál pri konkrétnom pôsobení, napríklad pri praní, trení alebo svetle. Nie je to jedna univerzálna vlastnosť. Látka môže dobre znášať pranie, ale slabšie odolávať slnku či mokrému treniu. Farby najlepšie chránite triedením, rešpektovaním štítku, praním naruby, primeraným dávkovaním a obmedzením zbytočného tepla, oderu a dlhého sušenia na ostrom slnku.",
        "intro": "Keď čierne tričko zosivie, nové rifle zafarbia svetlú sedačku alebo červená ponožka zmení farbu celej náplne, nejde vždy o ten istý problém. Raz sa mení odtieň pôvodného výrobku, inokedy sa uvoľnené farbivo usadí na susednej textílii. Výsledok ovplyvňuje druh farbiva, vlákno, výrobný proces, voda, prací prostriedok, teplota, trenie, pot aj svetlo. Správna diagnóza je dôležitá, pretože ďalšia dávka gélu nevyrieši poškodenie spôsobené slnkom a vyššia teplota môže prenos farby ešte zhoršiť.",
        "quick": [
            "<strong>Jedno číslo nestačí:</strong> odolnosť pri praní, svetle, suchom trení a mokrom trení sa posudzuje oddelene.",
            "<strong>Vyblednutie a zafarbenie nie sú to isté:</strong> prvé opisuje zmenu pôvodného kusa, druhé prenos farbiva na inú textíliu.",
            "<strong>Nový tmavý výrobok si zaslúži opatrnosť:</strong> prvé cykly perte samostatne alebo s veľmi podobnými farbami.",
            "<strong>Mechanika je rovnako dôležitá ako chémia:</strong> trenie v bubne, tesné nosenie a drsné povrchy môžu meniť vzhľad farby.",
            "<strong>Štítok je základ rozhodovania:</strong> maximálna povolená teplota nie je odporúčanie používať ju pri každom praní.",
        ],
        "overview_heading": "Čo presne znamená stálofarebnosť",
        "overview": [
            "Stálofarebnosť je odolnosť zafarbenia voči presne určenému vplyvu. Pri laboratórnom hodnotení sa sleduje zmena farby skúšanej textílie a pri niektorých metódach aj zafarbenie priložených materiálov. Tieto dve pozorovania odpovedajú na rozdielne otázky: či výrobok bledne a či môže znečistiť niečo vedľa seba. Preto môže byť tmavá látka stále sýta, no napriek tomu pri mokrom trení zanechávať stopu.",
            "Výsledok nie je vlastnosťou samotného názvu farby. Rovnaký čierny odtieň možno vytvoriť na bavlne, polyamide alebo polyesteri odlišnými farbivami a postupmi. Úlohu má príprava vlákna, fixácia, následné pranie pri výrobe aj povrchová úprava. Dve na pohľad podobné tričká sa preto môžu po desiatich cykloch správať rozdielne, hoci na etikete uvádzajú rovnaké materiálové zloženie.",
            "Domáca starostlivosť výrobnú kvalitu nezmení, môže však spomaliť zbytočnú stratu vzhľadu. Najväčší prínos má zníženie nepotrebného zaťaženia: prať iba znečistené kusy, zvoliť primeraný program, nepreplniť bubon, správne dávkovať a nenechať mokrú náplň dlho stáť. Pri čiernom oblečení pomôže aj podrobný návod <a href=\"/n/ako-prat-cierne-oblecenie-aby-nevybledlo\">ako prať čierne veci bez zbytočného blednutia</a>.",
        ],
        "table1_heading": "Rozdielne druhy farebnej stálosti",
        "table1_intro": "Nasledujúce situácie sa v bežnej reči často zlievajú do jedného pojmu, ale ich príčina aj prevencia sa líšia. Hodnotenie z jedného typu skúšky nemožno automaticky preniesť na ostatné.",
        "table1_headers": ["Pôsobenie", "Čo sa môže stať", "Typický príklad", "Čo má zmysel kontrolovať"],
        "table1_rows": [
            ("Pranie", "Odtieň sa zmení alebo farbivo zafarbí susedný materiál.", "Nové červené alebo tmavomodré oblečenie pustí do vody.", "Štítok, triedenie, teplotu, dávku, dĺžku cyklu a veľkosť náplne."),
            ("Suché trenie", "Povrchová farba sa prenesie bez prítomnosti vody.", "Tmavý denim zanechá stopu na svetlom čalúnení.", "Povrch výrobku, intenzitu trenia a odporúčanie výrobcu pred prvým nosením."),
            ("Mokré trenie", "Vlhkosť uľahčí uvoľnenie a prenos farbiva.", "Vlhké rifle alebo spotené tmavé tričko zafarbia svetlý materiál.", "Kontakt za mokra, pot, dážď a úplné vysušenie pred uložením."),
            ("Svetlo", "Odtieň sa mení pôsobením žiarenia a času.", "Časť závesu pri okne alebo rameno trička vybledne nerovnomerne.", "Dĺžku expozície, intenzitu slnka a polohu pri sušení či skladovaní."),
            ("Pot a ďalšie vplyvy", "Farba reaguje na vlhkosť, soli, kyslé alebo zásadité prostredie.", "Oblasť podpazušia alebo goliera zmení odtieň skôr než zvyšok odevu.", "Rýchlosť vyprania po nosení, dezodorant, lokálne čistenie a štítok."),
        ],
        "sections": [
            (
                "Vyblednutie výrobku verzus zafarbenie inej bielizne",
                [
                    "Vyblednutie znamená, že pôvodný kus stratil sýtosť alebo zmenil odtieň. Môže byť rovnomerné, napríklad po mnohých praniach, alebo lokálne na miestach najväčšieho oderu. Zmena nemusí znamenať iba odchod farbiva. Svetlo môže chemicky meniť farebný systém a opotrebované vlákna rozptyľujú svetlo inak, takže povrch pôsobí sivšie či matnejšie.",
                    "Zafarbenie susednej bielizne vzniká vtedy, keď sa uvoľnené farbivo alebo farebné častice prenesú na iný materiál. Najväčšie riziko je pri kombinácii nového sýteho kusa, svetlej savej bielizne, vlhkosti, tepla a dlhého kontaktu. Ak už k nehode došlo, postupujte rýchlo a zafarbený kus nesušte ani nežehlite, kým nevyskúšate vhodnú nápravu podľa návodu <a href=\"/n/pustila-farba-v-pracke-co-urobit-s-bielym-trickom-a-ruzovou-bieliznou\">čo robiť po prenose farby v práčke</a>.",
                ],
            ),
            (
                "Prečo farby blednú pri praní",
                [
                    "Prací cyklus kombinuje vodu, chémiu, čas, teplotu a mechanický pohyb. Každá zložka môže ovplyvniť farbu. Vyššia teplota a dlhší program môžu urýchliť uvoľnenie niektorých nefixovaných alebo slabšie viazaných farbív, zatiaľ čo silné trenie postupne narúša povrch. Nevhodný bieliaci prostriedok môže farbivo chemicky poškodiť aj bez viditeľného prenosu do vody.",
                    "To neznamená, že najkratší studený cyklus je správny pre všetko. Bielizeň treba zároveň hygienicky a účinne vyprať. Rozumným cieľom je najmiernejší proces, ktorý zodpovedá štítku, miere znečistenia a materiálu. Pri bežne nosenom farebnom oblečení často netreba voliť maximálnu povolenú teplotu. Silne znečistený pracovný textil však môže vyžadovať iný postup než krátko nosené tričko.",
                ],
            ),
            (
                "Suché a mokré trenie: prečo tmavý denim farbí",
                [
                    "Trenie môže preniesť farbu z povrchu textilu na inú plochu. Pri denime sa navyše vzhľad zámerne mení opotrebovaním vystupujúcich miest. Koleno, stehno, okraj vrecka a šev sa trú intenzívnejšie, preto svetlejú skôr. Ide o inú situáciu než rovnomerné vypranie celej plochy. Praktické kroky pre tento materiál zhŕňa článok <a href=\"/n/ako-prat-riflovu-bundu-a-tmave-dzinsy-aby-nepustali-farbu\">ako prať rifľovú bundu a tmavé džínsy</a>.",
                    "Mokré trenie býva náročnejšie, pretože voda alebo pot môžu uľahčiť pohyb farbiva. Nové tmavé rifle preto nemusia zafarbiť iba bielizeň v práčke; stopu môžu zanechať aj na svetlej obuvi, taške či sedačke. Domáci test skrytej plochy môže upozorniť na zjavný prenos, no nenahrádza štandardizované meranie a nesmie sa robiť agresívnym drhnutím, ktoré by výrobok poškodilo.",
                ],
            ),
            (
                "Svetlo, slnko a nerovnomerné blednutie",
                [
                    "Svetlostálosť opisuje správanie zafarbenia pri vystavení svetlu. V praxi rozhoduje intenzita, spektrum, dĺžka pôsobenia aj materiál. Odev ponechaný na parapete môže mať po čase inú farbu na osvetlenej a zatienenej časti. Podobne záves pri južnom okne dostáva podstatne väčšiu dávku svetla než textília uložená v zatvorenej skrini.",
                    "Sušenie na vzduchu je pre mnohé výrobky vhodné, no sýte farby netreba nechávať dlhšie, než je potrebné, na prudkom priamom slnku. Kus obráťte naruby a po vysušení ho odložte. Tienisté, vzdušné miesto obmedzí svetelnú záťaž bez toho, aby sa mokrý textil uzavrel v nevetranom priestore. Už vzniknuté fotochemické blednutie sa praním nevráti.",
                ],
            ),
            (
                "Pot, kozmetika a lokálna zmena odtieňa",
                [
                    "Podpazušie, golier, čelo čiapky a pás športového oblečenia sú vystavené potu, kožnému mazu, dezodorantu aj opakovanému treniu. Zmes týchto vplyvov môže vytvoriť mapy, zmeniť odtieň alebo zvýrazniť zvyšky produktu. Ak sa zmena objavuje iba v kontakte s telom, nemusí ísť o všeobecnú nestálosť pri praní.",
                    "Nosený kus nenechávajte dlho vlhký v uzavretej taške. Predpieranie riešte podľa štítku a typu škvrny; koncentrovaný prostriedok najprv skúste na skrytom mieste a nenechajte ho na látke zaschnúť. Agresívne trenie jednej škvrny môže vytvoriť svetlejší kruh aj vtedy, keď samotný prací cyklus prebehne správne.",
                ],
            ),
            (
                "Prvé pranie nového farebného oblečenia",
                [
                    "Nový sýtofarebný výrobok perte prvýkrát samostatne alebo s veľmi podobnými tmavými farbami, pokiaľ výrobca neuvádza inak. Skontrolujte vrecká, zapnite prvky, ktoré by mohli poškodzovať povrch, a odev obráťte naruby. Návod <a href=\"/n/ako-zabranit-pustaniu-farby-pri-prani-noveho-oblecenia\">ako obmedziť púšťanie farby pri prvom praní</a> vysvetľuje aj triedenie a situácie, pri ktorých treba zvýšiť opatrnosť.",
                    "Domáce pridávanie soli alebo octu nemožno považovať za univerzálnu fixáciu farbiva. Farbenie prebieha rôznymi chemickými systémami a proces, ktorý sa používa pri výrobe jednej vlákniny, nemusí fungovať na inom hotovom odeve. Navyše môže odporovať pokynom výrobcu alebo ovplyvniť inú súčasť výrobku. Bezpečnejšie je riadiť sa štítkom a kontrolovať prenos počas prvých cyklov.",
                ],
            ),
            (
                "Ako rozpoznať farbivo, zvyšok gélu a mechanické ošúchanie",
                [
                    "Farebná škvrna na pôvodne svetlom kuse naznačuje prenos, zatiaľ čo bledý povlak na viacerých tmavých kusoch môže byť nerozpustený alebo zle vypláchnutý prostriedok. Ostré svetlejšie línie na záhyboch a hranách častejšie súvisia s mechanickým oderom. Rovnomerne vyblednutá plocha po opakovaných cykloch zasa ukazuje na kumulované pôsobenie prania a sušenia.",
                    "Pred zásahom si textil prezrite za denného neutrálneho svetla a porovnajte rub s lícom alebo zakrytú časť s exponovanou. Ak povrch po opätovnom oplachu vyzerá normálne, pravdepodobne išlo o zvyšok produktu. Ak sú vlákna na svetlom mieste zdrsnené, samotné opláchnutie nepomôže. Táto krátka diagnostika bráni tomu, aby sa na už ošúchanú farbu použilo ďalšie zbytočné čistenie.",
                ],
            ),
            (
                "Ako chrániť farebný textil bez nedostatočného prania",
                [
                    "Triedenie podľa farby doplňte triedením podľa hmotnosti a povrchu. Jemné tmavé tričko nemá v bubne rovnaké potreby ako hrubé rifle so zipsami. Oblečenie obráťte naruby, neprepĺňajte bubon a zvoľte dávku podľa tvrdosti vody, veľkosti náplne a pokynov produktu. Priveľa gélu farbu neochráni; môže zhoršiť oplach a zanechať matný film.",
                    "Po skončení cyklu náplň vyberte, rozložte a sušte podľa štítku. Mokré kusy rôznych farieb nenechávajte dlho pritlačené na sebe. Pri odstraňovaní škvŕn postupujte lokálne a šetrne, ale bez dlhého drhnutia. Cieľom nie je vyhnúť sa praniu, ale nastaviť ho tak, aby účinne odstránilo nečistoty bez nepotrebného tepla, trenia a chemického zaťaženia.",
                ],
            ),
        ],
        "notes": {
            3: (
                "Rýchle rozlíšenie stopy po farbe",
                [
                    "Farebná stopa na inom povrchu ukazuje na prenos; svetlejšie miesto na pôvodnom kuse ukazuje na stratu alebo mechanickú zmenu.",
                    "Mokrý kontakt posudzujte oddelene od suchého kontaktu, pretože výsledok môže byť výrazne odlišný.",
                    "Viditeľný domáci pokus je iba orientačný a nedáva porovnateľnú laboratórnu známku.",
                ],
                "#f7fbf8",
                "#dbe5de",
            ),
            7: (
                "Keď farba po praní vyzerá inak",
                [
                    "Najprv odlíšte prenos farby, biely povlak, ošúchanie a svetelné blednutie.",
                    "Kým neviete, čo sa stalo, nepoužívajte sušičku, žehličku ani náhodne zvolený bieliaci prípravok.",
                    "Pri hodnotnom alebo viacfarebnom kuse je bezpečnejšia profesionálna čistiareň než opakované domáce experimenty.",
                ],
                "#fffaf5",
                "#e6ded2",
            ),
        },
        "table2_heading": "Diagnostika zmeny farby po praní alebo nosení",
        "table2_intro": "Vzhľad poškodenia často napovie viac než samotná informácia, že sa objavilo po praní. Tabuľka pomáha určiť prvý bezpečný krok, nie definitívny laboratórny záver.",
        "table2_headers": ["Pozorovanie", "Pravdepodobné vysvetlenie", "Čo skontrolovať", "Prvý rozumný krok"],
        "table2_rows": [
            ("Svetlá bielizeň má nový farebný nádych.", "Prenos farbiva z iného kusa.", "Zloženie náplne, nový tmavý kus, teplotu a čas státia za mokra.", "Oddeliť, nesušiť teplom a postupovať podľa materiálu a štítku."),
            ("Tmavé kusy majú kriedový alebo lepkavý povlak.", "Zvyšok prostriedku alebo slabý oplach.", "Dávku, tvrdosť vody, preplnenie a zvolený cyklus.", "Spustiť vhodné opláchnutie bez pridania ďalšieho gélu."),
            ("Farba je svetlejšia na hranách a záhyboch.", "Mechanický oder povrchu.", "Kontakt so zipsami, preplnenie, vysoké otáčky a drsné kusy.", "Zmeniť triedenie a mechaniku ďalších cyklov; poškodenie už nedrhnúť."),
            ("Jedna strana je bledšia než druhá.", "Dlhodobé pôsobenie svetla.", "Miesto skladovania alebo sušenia a polohu pri okne.", "Obmedziť ďalšiu expozíciu; existujúci rozdiel sa praním neobnoví."),
            ("Zmena je najmä v podpazuší alebo pri golieri.", "Pot, kozmetika, maz a trenie.", "Spôsob nosenia, lokálne prípravky a čas pred vypraním.", "Čistiť včas a podľa štítku, bez koncentrovaného drhnutia."),
        ],
        "steps": [
            "Prečítajte materiálové zloženie aj symboly ošetrovania; rozhoduje hotový výrobok so všetkými doplnkami.",
            "Nový sýty kus oddeľte od svetlej bielizne a prvé prania sledujte opatrnejšie.",
            "Triedite nielen podľa farby, ale aj podľa hmotnosti, drsnosti povrchu a kovových či háčikových prvkov.",
            "Odev obráťte naruby, zapnite bezpečné zapínanie a bubon naplňte tak, aby sa textil mohol pohybovať a opláchnuť.",
            "Zvoľte teplotu a program podľa štítku a miery znečistenia, nie automaticky podľa zvyku.",
            "Dávkujte prací prostriedok podľa návodu, tvrdosti vody a veľkosti náplne; vyššia dávka nie je ochranný náter na farbu.",
            "Po cykle bielizeň hneď vyberte, oddeľte a vysušte bez nepotrebného presúšania alebo dlhého priameho slnka.",
        ],
        "remember": [
            "Púšťanie pri praní, prenos trením a blednutie svetlom sú tri samostatné otázky.",
            "Mokrý a suchý kontakt môžu dať odlišný výsledok.",
            "Maximálna teplota na štítku nie je povinná teplota každého cyklu.",
            "Povlak po nedostatočnom oplachu môže vyzerať ako vyblednutie.",
            "Nezvratné ošúchanie alebo svetelné blednutie sa ďalším praním neopraví.",
        ],
        "mistakes": [
            "Predpokladať, že výrobok odolný pri praní musí rovnako dobre znášať mokré trenie a slnko.",
            "Prať nový tmavý denim spolu s bielymi uterákmi iba preto, že oba kusy povoľujú rovnakú teplotu.",
            "Pridávať viac pracieho gélu v nádeji, že vytvorí ochranu proti blednutiu.",
            "Drhnúť svetlejšie miesto, hoci ide o mechanicky ošúchané vlákna, nie o odstrániteľný povlak.",
            "Nechať mokré farebné kusy dlho spolu v bubne alebo koši po skončení programu.",
            "Považovať soľ alebo ocot za univerzálnu domácu fixáciu každého farbiva a každého vlákna.",
        ],
        "expert_heading": "Odbornejší pohľad: prečo sa skúšky nedajú nahradiť jedným domácim testom",
        "expert": [
            "Norma ISO 105-C06 opisuje skúšky farebnej stálosti pri domácom a komerčnom praní s definovanými podmienkami, referenčnými detergentmi a priloženými materiálmi. Zmyslom je porovnateľný postup, nie presná simulácia každej práčky a každého výrobku v domácnosti. Výsledok preto treba čítať spolu s použitou metódou a podmienkami, nie ako neobmedzený prísľub, že farba nikdy nepustí.",
            "ISO 105-X12 sa venuje prenosu farby trením a rozlišuje suché a mokré podmienky. ISO 105-B02 používa umelé svetlo reprezentujúce denné svetlo na hodnotenie svetlostálosti. Už samotné oddelenie týchto noriem ukazuje, prečo údaj o odolnosti pri jednom pôsobení nevypovedá automaticky o inom. AATCC podobne združuje samostatné postupy pre pranie, trenie, svetlo, pot a ďalšie vplyvy.",
            "Pre zákazníka z toho vyplýva praktická zásada: porovnávajte iba výsledky získané rovnakou metódou a pýtajte sa, aké pôsobenie sa skúšalo. V domácnosti pracujte s rizikom podľa situácie. Nový sýty kus izolujte, svetlý povrch chráňte pred tmavým mokrým textilom a dlhú expozíciu slnku obmedzte. Ani kvalitná skúška však nenahrádza ošetrovací štítok konkrétneho výrobku.",
        ],
        "source_intro": "Nižšie uvedené zdroje opisujú rozdielne skúšobné postupy. Ich výsledky sú porovnateľné iba pri rovnakej metóde, podmienkach a spôsobe hodnotenia; domáci postup nimi nemožno certifikovať.",
        "sources": [
            ("ISO 105-C06: farebná stálosť pri domácom a komerčnom praní", ISO_COLOR_WASH),
            ("ISO 105-X12: farebná stálosť pri trení", ISO_COLOR_RUB),
            ("ISO 105-B02: farebná stálosť pri umelom svetle", ISO_COLOR_LIGHT),
            ("AATCC: prehľad metód hodnotenia farebnej stálosti", AATCC_COLOR),
            ("GINETEX: význam symbolov ošetrovania textilu", GINETEX),
        ],
        "product_intro": "Pri bežnom praní farebného textilu je dôležité skombinovať vhodný prostriedok s teplotou, programom a dávkou podľa štítku. Samotný názov gélu neprekoná nedostatočnú stálofarebnosť výrobku ani poškodenie slnkom.",
        "product_text": "Koncentrovaný prací gél z marseillského mydla na bežnú domácu bielizeň. Pred použitím si overte pokyny na obale aj ošetrovací štítok textilu a dávku prispôsobte tvrdosti vody, znečisteniu a veľkosti náplne.",
        "product_limit": "Prací gél nie je fixátor farbiva. Nové sýte kusy stále treba triediť a pri nestálofarebnom výrobku nemôže žiadny bežný prostriedok zaručiť nulový prenos.",
        "category_intro": "V kategórii môžete porovnať pracie gély podľa typu bielizne a spôsobu použitia. Pri výbere sledujte dávkovanie a vhodnosť pre konkrétny textil, nie iba vôňu alebo veľkosť balenia.",
        "category_text": "Vyberte prostriedok pre svoju bežnú náplň a používajte ho v dávke odporúčanej výrobcom. Farebné, jemné, funkčné alebo výrobcom osobitne ošetrené textílie môžu mať ďalšie obmedzenia uvedené na štítku.",
        "related": [
            ("Ako prať čierne oblečenie, aby zbytočne nevybledlo", ARTICLE_BLACK),
            ("Ako zabrániť púšťaniu farby pri praní nového oblečenia", ARTICLE_COLOR_FIRST_WASH),
            ("Ako prať rifľovú bundu a tmavé džínsy", ARTICLE_DENIM_COLOR),
            ("Čo robiť, keď bielizeň v práčke pustila farbu", ARTICLE_COLOR_TRANSFER),
            ("Pevnosť šva a posun nití", ARTICLE_SEAMS),
            ("Zatrhávanie textilu a vytiahnuté očká", ARTICLE_SNAGGING),
        ],
        "faq_title": "stálofarebnosť a blednutie textilu",
        "faq": [
            ("Čo je stálofarebnosť textilu?", "Je to odolnosť zafarbenia voči konkrétnemu pôsobeniu, napríklad praniu, treniu alebo svetlu. Treba vždy vedieť, voči čomu a akou metódou bola hodnotená."),
            ("Prečo nové oblečenie púšťa farbu?", "Na povrchu môže zostať prebytočné alebo slabšie fixované farbivo. Miera závisí od vlákna, farbiaceho procesu a úpravy, preto nový sýty kus prvýkrát oddeľte od svetlej bielizne."),
            ("Znamená studená voda, že farba určite nepustí?", "Nie. Nižšia teplota môže v niektorých situáciách riziko znížiť, ale nestálofarebný výrobok môže farbiť aj v studenej vode alebo pri mokrom trení."),
            ("Pomôže ocot alebo soľ zafixovať každú farbu?", "Nie je to univerzálne riešenie. Rôzne vlákna a farbivá sa fixujú odlišnými priemyselnými procesmi a domáci zásah môže byť neúčinný alebo nevhodný pre hotový výrobok."),
            ("Prečo čierne tričko vyzerá po praní sivé?", "Príčinou môže byť skutočné blednutie, mechanicky zdrsnený povrch, zachytené svetlé vlákna alebo zvyšok pracieho prostriedku. Najprv porovnajte rub a líce a skontrolujte povrch."),
            ("Dá sa vyblednutá farba obnoviť ďalším praním?", "Ak ide o povlak, vhodné opláchnutie môže vzhľad zlepšiť. Stratené farbivo, ošúchaný povrch alebo poškodenie svetlom však obyčajné pranie nevráti."),
        ],
    },
    {
        "title": "Pevnosť šva a posun nití: prečo oblečenie praská pri švoch",
        "link": "pevnost-sva-a-posun-niti-preco-oblecenie-praska-pri-svoch",
        "meta": "Prečo oblečenie praská pri švoch, ako odlíšiť pretrhnutú niť od posunu priadzí a čo ovplyvňuje pevnosť šva pri nosení a praní.",
        "short": "Otvorený šev nemusí znamenať iba pretrhnutú šijaciu niť. Sprievodca rozlišuje zlyhanie stehu, roztrhnutie látky a posun nití a ukazuje, ako poškodenie bezpečne posúdiť.",
        "answer": "Keď oblečenie praskne pri šve, najprv zistite, či sa pretrhla šijacia niť, roztrhla samotná látka, uvoľnil steh alebo sa priadze tkaniny od seba odsunuli. Každý typ poškodenia má inú príčinu aj opravu. Riziko zvyšuje tesný strih, malý prídavok na šev, nevhodná ihla či niť, riedka alebo klzká tkanina, poškodenie pri výrobe a opakované ťahové zaťaženie. Pranie môže slabé miesto odhaliť alebo zhoršiť, ale nebýva automaticky jediným vinníkom.",
        "intro": "Bočný šev sa otvorí, no šijacia niť zostane celá. Pri rozkroku riflí sa látka roztrhne tesne vedľa stehov. Na obliečke sa priadze rozostúpia do úzkej medzery bez jasného pretrhnutia. Navonok všetky situácie vyzerajú ako prasknutý šev, konštrukčne však ide o odlišné poruchy. Ak ich nerozlíšite, môžete pri oprave zachytiť iba okraj poškodenia, ešte viac oslabiť tkaninu alebo pripísať práčke problém, ktorý vznikal už pri strihu a nosení.",
        "quick": [
            "<strong>Najprv prezrite niť aj látku:</strong> otvor v línii stehu má inú príčinu než roztrhnutie niekoľko milimetrov vedľa nej.",
            "<strong>Posun nití nie je čisté roztrhnutie:</strong> priadze tkaniny sa od seba odsunú a vytvoria medzeru pri šve.",
            "<strong>Veľkosť a pohyb rozhodujú:</strong> opakované napätie pri sedení, drepe či obliekaní môže prekročiť rezervu konštrukcie.",
            "<strong>Viac stehov nemusí byť vždy lepšie:</strong> príliš husté prepichovanie môže citlivú látku perforovať.",
            "<strong>Včasná oprava býva menšia:</strong> uvoľnený úsek šitia sa rieši ľahšie než rozstrapkaná a vytrhnutá plocha.",
        ],
        "overview_heading": "Šev je systém, nie iba viditeľná čiara stehov",
        "overview": [
            "Funkčný šev spája vrstvy materiálu pomocou zvoleného typu stehu, šijacej nite a geometrie spoja. Jeho výkon ovplyvňuje smer osnovy a útku, pružnosť, hrúbka, hustota látky, prídavok na šev, počet vrstiev, ihla, napätie nití aj ukončenie okrajov. Ak je čo len jedna súčasť nevhodná, zaťaženie sa môže sústrediť do úzkeho pásu a poškodenie sa objaví práve pri stehoch.",
            "Pevnosť samotnej látky a pevnosť zošitého spoja nie sú rovnaký údaj. Materiál môže odolávať ťahu na voľnej ploche, ale pri šve sa jeho priadze rozostúpia. Naopak pevná tkanina môže zostať neporušená, zatiaľ čo sa pretrhne slabá šijacia niť. Aj skúšobné metódy preto presne určujú vzorku, orientáciu a spôsob zaťaženia.",
            "Pri domácom hodnotení nepotrebujete merať silu v newtonoch. Potrebujete však pozorne určiť miesto a tvar poruchy. Rozstrapkané okraje, neporušené stehy, vytiahnuté slučky alebo pravidelné dierky po ihle poskytujú odlišné stopy. Ak látka redne aj mimo šva, pomôže rozlíšiť celkové opotrebovanie článok o <a href=\"/n/odolnost-textilu-proti-oderu-co-znamena-martindale-pri-obleceni-a-bytovych-latkach\">odolnosti textilu proti oderu a skúške Martindale</a>.",
        ],
        "table1_heading": "Štyri časté spôsoby zlyhania pri šve",
        "table1_intro": "Pred šitím alebo reklamáciou skúste poškodenie zaradiť podľa toho, ktorá časť systému povolila. Jedna vec môže mať aj kombinovanú poruchu, najmä ak sa používala ďalej po prvom otvorení.",
        "table1_headers": ["Typ poruchy", "Ako vyzerá", "Častá príčina", "Čo oprava musí riešiť"],
        "table1_rows": [
            ("Pretrhnutá šijacia niť", "Látka pri línii šva ostáva prevažne celá, stehy sú prerušené alebo vypárané.", "Nevhodná niť, poškodenie nite, slabé zapošitie alebo lokálne preťaženie.", "Obnoviť správny steh a bezpečne nadviazať na pevný úsek."),
            ("Roztrhnutá látka", "Vlákna sú pretrhnuté, často tesne vedľa šva; okraj sa strapká.", "Oder, malý prídavok, perforácia ihlou, oslabenie materiálu alebo prudký ťah.", "Spevniť dostatočne veľkú zdravú plochu, nie iba prešiť poškodený okraj."),
            ("Posun priadzí", "Osnovné alebo útkové priadze sa rozostúpia a vznikne medzera bez čistého rezu.", "Riedka či klzká tkanina, nevhodný šev, malý prídavok alebo sústredené zaťaženie.", "Rozložiť silu a stabilizovať konštrukciu; jednoduché zatlačenie nití nemusí vydržať."),
            ("Uvoľnený alebo preskočený steh", "Na povrchu sú voľné slučky, chýbajúce stehy alebo sa spoj postupne pára.", "Nesprávne napätie, ihla, navlečenie, poškodenie pri výrobe alebo zachytenie slučky.", "Odstrániť príčinu, zaistiť konce a obnoviť kompatibilný typ stehu."),
            ("Kombinované zlyhanie", "Niť aj látka sú poškodené a otvor sa šíri do okolia.", "Dlhé používanie po prvom poškodení, výrazné preťaženie alebo opotrebovanie.", "Najprv posúdiť rozsah; často treba záplatu, výmenu dielu alebo odbornú opravu."),
        ],
        "sections": [
            (
                "Ako funguje šev pri ťahu a pohybe",
                [
                    "Pri nosení nepôsobí sila rovnomerne na celý odev. Rozkrok, podpazušie, zadný sed, bočné švy a rohy obliečok znášajú cyklické napínanie v určitých smeroch. Strih má vytvoriť priestor na pohyb a šev má preniesť zaťaženie do širšej plochy. Ak je výrobok tesný alebo sa látka po praní rozmerovo zmení, rezerva sa zmenší a šev dostáva väčšiu časť sily.",
                    "Pružnosť materiálu musí zodpovedať pružnosti stehu. Pevný nepružný steh na veľmi elastickom úplete môže pri natiahnutí prasknúť, aj keď samotný úplet zostane celý. Naopak voľný alebo nevhodne nastavený steh nemusí vrstvy stabilne držať. Pri výrobkoch z bavlny s elastanom je užitočné poznať aj <a href=\"/n/bavlna-a-elastan-starostlivost-o-tricka-rifle-a-spodnu-bielizen\">rozdiel medzi rozmerovou zmenou bavlny a únavou pružného vlákna</a>.",
                ],
            ),
            (
                "Posun nití pri tkanine: medzera bez jasného roztrhnutia",
                [
                    "Tkanina vzniká previazaním osnovných a útkových priadzí. Pri vhodnej kombinácii klzkého povrchu, nižšej hustoty a sústredeného ťahu sa priadze môžu pri šve odsunúť. Viditeľná medzera potom vyzerá, akoby sa šev otvoril, ale stehy aj jednotlivé priadze môžu byť spočiatku celé. Tento jav sa týka najmä tkanín; pri úplete sa štruktúra a spôsob šírenia poškodenia líšia.",
                    "Posun môže byť vratný iba zdanlivo. Keď priadze prstami vrátite na miesto, ich usporiadanie a povrchové trenie už nemusia rovnomerne prenášať ďalšie zaťaženie. Opakované odsúvanie navyše poškodzuje okolie ihlových vpichov. Na namáhanom mieste preto treba riešiť aj príčinu: šírku prídavku, tesnosť, smer zaťaženia a vhodné spevnenie.",
                ],
            ),
            (
                "Hustota stehov, prídavok na šev a ukončenie okraja",
                [
                    "Príliš riedke stehy môžu zvyšovať lokálny pohyb a pri vypáraní rýchlo otvoriť dlhý úsek. Príliš husté stehy však vytvoria veľa vpichov tesne vedľa seba a pri jemnej alebo poškodenej látke môžu pôsobiť ako perforácia. Správna hustota preto závisí od materiálu, typu stehu, nite a účelu výrobku; nejde o pravidlo, že vyšší počet je vždy kvalitnejší.",
                    "Prídavok na šev je pás materiálu za líniou šitia. Ak je príliš úzky alebo sa okraj silno strapká, šev nemá dostatok zdravej tkaniny, do ktorej by rozložil silu. Začistenie obmedzuje strapkanie, no samo nenahrádza primeranú šírku a vhodnú konštrukciu. Pri oprave treba šiť do stabilného materiálu, nie tesne pozdĺž už rozpadnutého okraja.",
                ],
            ),
            (
                "Ihla a šijacia niť môžu materiál chrániť aj poškodiť",
                [
                    "Veľkosť a hrot ihly sa volia podľa konštrukcie a hrúbky materiálu. Nevhodná alebo tupá ihla môže prerezať či vytlačiť vlákna, zanechať zväčšené otvory a vytvoriť oslabenú líniu. Na úplete sa poškodená slučka môže neskôr párať, kým na jemnej tkanine sa okolie vpichu môže pri zaťažení trhať alebo posúvať.",
                    "Šijacia niť musí mať primeranú pevnosť, pružnosť, hrúbku a odolnosť voči použitiu aj ošetrovaniu výrobku. Extrémne silná niť nie je automaticky bezpečnejšia: ak pri preťažení neustúpi, môže preniesť poruchu do drahšej a ťažšie opraviteľnej látky. Dôležité je vyváženie celého spoja, nie maximalizácia jednej súčasti.",
                ],
            ),
            (
                "Veľkosť odevu, strih a opakované preťaženie",
                [
                    "Ak šev praská pri každom drepe, nemusí byť jediným problémom jeho vyhotovenie. Tesný strih, nedostatočná pohybová voľnosť alebo zmena rozmerov po praní sústavne sústreďujú ťah na rovnaké miesto. Typické sú vodorovné napäťové vrásky, roztiahnuté stehy a medzery pri šve ešte pred úplným zlyhaním.",
                    "Jednorazový prudký pohyb môže poškodiť aj primerane navrhnutý výrobok, no opakované otváranie rovnakého miesta po oprave naznačuje nevyriešenú príčinu. Krajčír môže posúdiť, či je možné šev uvoľniť, pridať klin alebo rozložiť zaťaženie. Opakované prešitie tej istej oslabenej línie bez zmeny konštrukcie môže vytvoriť ďalší rad dierok.",
                ],
            ),
            (
                "Čo môže urobiť pranie a čo vzniklo už pred ním",
                [
                    "V bubne sa textil ohýba, nasiakne vodou a naráža do ostatných kusov. Otvorený zips, háčik alebo suchý zips môže zachytiť niť či okraj. Preplnenie zhoršuje voľný pohyb a oplach, kým vysoké otáčky zvyšujú sily pri nerovnomerne rozloženej ťažkej náplni. Praktické triedenie opisuje návod <a href=\"/n/ako-prat-oblecenie-so-zipsami-a-suchym-zipsom-bez-zatrhnutia\">ako prať oblečenie so zipsami a suchým zipsom</a>.",
                    "Pranie však často iba odhalí miesto oslabené pri výrobe, nosení alebo predchádzajúcom odere. Ak sú okraje starej trhliny hladko opotrebované a látka v okolí stenčená, porucha sa vyvíjala dlhšie. Ak sú vpichy prázdne a niť chýba v pravidelnom úseku, pravdepodobnejšie zlyhalo šitie. Dobrá diagnóza sa opiera o vzhľad celého okolia, nie iba o okamih, keď ste otvor objavili.",
                ],
            ),
            (
                "Kontrola švov pred praním a po ňom",
                [
                    "Pred praním skontrolujte vysoko namáhané miesta, vrecká, rohy obliečok a spojenie ramienok. Voľnú slučku neťahajte a dlhý koniec nite neodstrihnite tesne pri látke bez zaistenia; môže ísť o koniec stehu, ktorý sa začne párať. Malé otvorenie vyfoťte a označte jeho hranice, ak chcete sledovať, či sa zväčšuje.",
                    "Po praní textil rozložte bez prudkého trasenia a šev prezrite z líca aj rubu. Sledujte, či sa oddelili vrstvy, posunuli priadze, rozstrapkal okraj alebo pretrhla niť. Pri podozrení na výrobnú chybu výrobok pred zásadnou opravou zdokumentujte a overte podmienky reklamácie. Neodborný zásah môže sťažiť posúdenie pôvodnej poruchy.",
                ],
            ),
            (
                "Kedy opraviť doma a kedy zvoliť krajčíra",
                [
                    "Krátky vypáraný úsek na pevnej zdravej látke možno často obnoviť vhodným stehom so zaistenými koncami. Oprava musí nadviazať na stabilnú časť a zachovať potrebnú pružnosť. Pri roztrhnutej tkanine, posune priadzí, podšívke, nepremokavom šve alebo výrazne namáhanom rozkroku je bezpečnejšie odborné posúdenie.",
                    "Ak chýba materiál alebo je okolie stenčené, samotné zošitie okrajov zmenší výrobok a presunie ťah vedľa opravy. Potrebné môže byť podlepenie, záplata, výmena dielu alebo nový konštrukčný prvok. Lepidlo zvolené bez znalosti materiálu môže stvrdnúť, presiaknuť, obmedziť pružnosť a skomplikovať neskoršie šitie.",
                ],
            ),
        ],
        "notes": {
            3: (
                "Čo si všimnúť pri otvorenom šve",
                [
                    "Sú šijacie nite pretrhnuté, vypárané alebo stále celé?",
                    "Sú priadze látky od seba odsunuté, alebo majú rozstrapkané pretrhnuté konce?",
                    "Je materiál v okolí zdravý, alebo tenký, ošúchaný a perforovaný?",
                    "Objavujú sa napäťové vrásky či roztiahnutie aj na nepoškodenej časti?",
                ],
                "#f7fbf8",
                "#dbe5de",
            ),
            7: (
                "Pred domácou opravou",
                [
                    "Výrobok najprv vyperte alebo vyčistite iba vtedy, ak ďalší cyklus poškodenie nezväčší; otvorený šev dočasne chráňte.",
                    "Pri reklamovateľnom kuse urobte fotografie pred zásahom a overte postup predajcu.",
                    "Oprava má zasahovať do zdravej plochy a zachovať pohyb, nie iba kozmeticky uzavrieť medzeru.",
                ],
                "#fffaf5",
                "#e6ded2",
            ),
        },
        "table2_heading": "Kde švy najčastejšie zlyhávajú a čo tým telo výrobku naznačuje",
        "table2_intro": "Miesto poruchy pomáha odhadnúť smer a opakovanie zaťaženia. Nie je to automatický dôkaz výrobnej chyby ani nesprávneho používania, ale užitočný podklad pre kontrolu.",
        "table2_headers": ["Miesto", "Typické zaťaženie", "Skontrolujte", "Prevencia ďalšej škody"],
        "table2_rows": [
            ("Rozkrok a sed nohavíc", "Ťah pri sedení, chôdzi a drepe plus trenie medzi vrstvami.", "Tesnosť, stenčenie látky, posun priadzí a stav dvojitého šitia.", "Nenosiť po otvorení, opraviť do zdravej plochy a posúdiť pohybovú voľnosť."),
            ("Podpazušie a rukáv", "Viacsmerné napínanie pri zdvíhaní rúk a pot.", "Pružnosť stehu, strih, poškodenie látky a lokálnu zmenu rozmerov.", "Zvoliť vhodnú veľkosť a opravu, ktorá zachová pružnosť."),
            ("Bočný šev", "Napätie pri obliekaní a pohybe, prípadne krútenie výrobku.", "Rovnosť šva, prídavok, strapkanie a napäťové vrásky.", "Nezaťažovať otvorený úsek a pri oprave nadviazať na stabilné stehy."),
            ("Roh obliečky alebo obliečky na vankúš", "Ťah pri navliekaní, rotácia v bubne a tlak výplne.", "Prídavok, začistenie, rozmer výplne a oslabenie rohu.", "Použiť správny rozmer a malú poruchu opraviť pred ďalším praním."),
            ("Vrecko, pútko a ramienko", "Sila sústredená do malého kotviaceho bodu.", "Zaisťovacie stehy, podloženie a trhlinu v okolí.", "Vrecká nepreťažovať a kotvenie spevniť na širšej zdravej ploche."),
        ],
        "steps": [
            "Výrobok prestaňte na poškodenom mieste zaťažovať a otvor pred praním dočasne chráňte pred zachytením.",
            "Prezrite šev z líca aj rubu pri dobrom svetle a oddeľte poruchu šijacej nite od poruchy látky.",
            "Skontrolujte okolie: stenčenie, strapkanie, posun priadzí, pravidelné dierky po ihle a napäťové vrásky.",
            "Zvážte, či tesnosť, zmena rozmerov, obsah vrecka alebo konkrétny pohyb nezaťažujú miesto nad jeho rezervu.",
            "Ak môže ísť o výrobnú chybu, stav zdokumentujte a pred opravou overte reklamačný postup.",
            "Pri jednoduchej oprave nadviažte na pevný úsek, zaistite konce a použite niť aj steh vhodné pre pružnosť materiálu.",
            "Po oprave miesto najprv zaťažte mierne; ak sa okolie deformuje alebo znovu otvára, vyhľadajte odbornú úpravu konštrukcie.",
        ],
        "remember": [
            "Otvorený šev môže byť poruchou nite, stehu, látky alebo usporiadania priadzí.",
            "Posun nití vyzerá ako medzera pri šve, hoci jednotlivé priadze nemusia byť pretrhnuté.",
            "Príliš husté prepichovanie môže jemnú látku oslabiť.",
            "Opakované zlyhanie po oprave zvyčajne znamená, že pôvodná príčina zostala.",
            "Pri hodnote, funkčnej vrstve alebo rozsiahlej trhline je krajčír bezpečnejší než improvizácia.",
        ],
        "mistakes": [
            "Označiť každú medzeru pri šve za pretrhnutú niť bez kontroly samotnej tkaniny.",
            "Prešiť rozstrapkaný okraj tesne vedľa poškodenia, kde už nie je dosť zdravej plochy.",
            "Použiť čo najhrubšiu niť bez ohľadu na pružnosť a jemnosť materiálu.",
            "Odstrihnúť voľnú slučku pri povrchu a nechať steh pokračovať v páraní.",
            "Prať poškodený kus so zipsami a ťažkými textíliami, hoci sa otvor môže ďalej zachytiť.",
            "Viniť posledný prací cyklus bez posúdenia dlhodobého oderu, tesnosti a výrobných stôp.",
        ],
        "expert_heading": "Odbornejší pohľad: pevnosť šva a odolnosť proti posunu priadzí",
        "expert": [
            "ISO 13935-2 opisuje stanovenie maximálnej sily do pretrhnutia šva metódou grab. Skúšobná vzorka obsahuje priamy šev a zaťaženie pôsobí kolmo naň. Norma zároveň vymedzuje použitie a uvádza, že nie je určená pre všetky typy textílií a švov, napríklad niektoré geotextílie, netkané materiály, povrstvené textílie či švy zo sklenených vlákien. Číslo preto dáva zmysel iba spolu s typom vzorky a metódou.",
            "Séria ISO 13936 sa zameriava na odolnosť priadzí tkaniny proti posunu pri šve. Časť 1 používa pevnú veľkosť otvorenia a ISO 13936-3 metódu ihlovej svorky. ASTM D4034/D4034M rieši odolnosť čalúnnických tkanín proti posunu pri šve. Rozdielne oblasti použitia a postupy znamenajú, že výsledky nemožno bez ďalších údajov miešať do jedného rebríčka kvality.",
            "Pri praktickom porovnaní výrobkov sa pýtajte, či údaj opisuje silu potrebnú na porušenie zošitého spoja, alebo tendenciu priadzí odsunúť sa pri šve. Záleží aj na orientácii osnovy a útku, type šva, stehu a použitej niti. Laboratórna skúška pomáha porovnávať definované vzorky; nevysvetľuje sama osebe veľkosť odevu, jeho opotrebovanie ani konkrétny pohyb používateľa.",
        ],
        "source_intro": "Zdroje používajú presne určené vzorky, smery zaťaženia a oblasti použitia. Výsledky z odlišných metód alebo konštrukcií nemožno bez prepočtu a kontextu priamo porovnávať.",
        "sources": [
            ("ISO 13935-2: maximálna sila do pretrhnutia šva metódou grab", ISO_SEAM_FORCE),
            ("ISO 13936-1: posun priadzí pri šve metódou pevného otvorenia", ISO_SEAM_SLIP),
            ("ISO 13936-3: posun priadzí metódou ihlovej svorky", ISO_NEEDLE_CLAMP),
            ("ASTM D4034/D4034M: posun pri šve pri čalúnnických tkaninách", ASTM_SEAM_SLIP),
            ("GINETEX: význam symbolov ošetrovania textilu", GINETEX),
        ],
        "product_intro": "Správne pranie pomáha obmedziť zbytočné mechanické namáhanie, ale prací prostriedok nevie napraviť nevhodnú konštrukciu šva, tesný strih ani už roztrhnutú látku.",
        "product_text": "Prací gél je určený na bežnú domácu bielizeň. Používajte ho podľa etikety produktu a ošetrovacieho štítku, pričom dávku prispôsobte vode, znečisteniu a náplni a poškodené kusy pred cyklom zabezpečte.",
        "product_limit": "Gél nespevňuje šijaciu niť ani posunuté priadze. Otvorený šev treba pred ďalším mechanickým zaťažením skontrolovať a podľa rozsahu opraviť.",
        "category_intro": "Pri výbere pracieho gélu sledujte vhodnosť pre materiál a pokyny dávkovania. O životnosti šva však rozhoduje aj triedenie náplne, zapnutie kovania, veľkosť bubna a spôsob sušenia.",
        "category_text": "Kategória umožňuje porovnať pracie gély na bežné domáce použitie bez uvádzania fixnej ceny v návode. Pred praním oddeľte jemné a poškodené kusy od ťažkých textílií a zachytávajúcich prvkov.",
        "related": [
            ("Ako predísť dierkam v tričkách po praní a sušení", ARTICLE_HOLES),
            ("Ako prať oblečenie so zipsami a suchým zipsom", ARTICLE_ZIPS),
            ("Bavlna a elastan v tričkách, rifliach a spodnej bielizni", ARTICLE_COTTON_ELASTANE),
            ("Odolnosť textilu proti oderu a Martindale", ARTICLE_MARTINDALE),
            ("Zatrhávanie textilu a vytiahnuté očká", ARTICLE_SNAGGING),
            ("Stálofarebnosť textilu pri praní, svetle a trení", ARTICLE_COLORFASTNESS),
        ],
        "faq_title": "pevnosť švov a posun nití",
        "faq": [
            ("Prečo sa šev otvoril, hoci niť nie je pretrhnutá?", "Priadze tkaniny sa mohli pri šve odsunúť alebo sa steh mohol vypárať z konca. Prezrite rub, líniu vpichov a stav jednotlivých priadzí."),
            ("Čo je posun nití pri šve?", "Je to rozostúpenie osnovných alebo útkových priadzí v tkanine vplyvom zaťaženia pri šve. Môže vytvoriť viditeľnú medzeru bez okamžitého pretrhnutia každej priadze."),
            ("Môže šev poškodiť práčka?", "Mechanický pohyb, zachytenie o kovanie alebo nevhodná náplň môžu poškodenie zhoršiť. Pranie však často iba odhalí už oslabený materiál, nevhodný steh alebo dlhodobé preťaženie."),
            ("Stačí prasknutý šev jednoducho prešiť?", "Iba ak je látka zdravá a príčina bola v krátkom úseku šitia. Pri roztrhnutí, posune priadzí alebo tesnom strihu treba spevniť väčšiu plochu alebo upraviť konštrukciu."),
            ("Je hustejší steh vždy pevnejší?", "Nie. Príliš husté vpichy môžu citlivý materiál perforovať. Vhodná hustota závisí od látky, typu stehu, nite a zaťaženia."),
            ("Kedy odniesť výrobok ku krajčírovi?", "Pri rozsiahlej trhline, posune priadzí na namáhanom mieste, funkčnej membráne, podšívke, opakovanom zlyhaní alebo potrebe zmeniť veľkosť a rozloženie zaťaženia."),
        ],
    },
    {
        "title": "Zatrhávanie textilu: prečo vznikajú vytiahnuté očká a ako im predchádzať",
        "link": "zatrhavanie-textilu-preco-vznikaju-vytiahnute-ocka-a-ako-im-predchadzat",
        "meta": "Prečo sa na textile vyťahujú očká, ako odlíšiť zatrhnutie od žmolkov a diery a ako chrániť úplety a hladké látky pri nosení aj praní.",
        "short": "Vytiahnuté očko vzniká zachytením a posunutím nite alebo slučky v štruktúre textilu. Naučte sa rozlíšiť zatrhnutie od žmolkov, diery a poškodeného šva a predchádzať ďalšej škode.",
        "answer": "Zatrhnutie vzniká, keď háčik, drsná hrana alebo iný výstupok zachytí niť či slučku a vytiahne ju nad povrch textilu. Neodstrihujte ju automaticky: pri úplete môže ísť o súčasť spojitej slučkovej štruktúry a odstrihnutie môže vytvoriť dieru alebo spustiť páranie. Výrobok najprv prestaňte namáhať, prezrite z líca aj rubu a podľa materiálu nechajte očko jemne vtiahnuť na rub alebo opraviť odborníkovi. Prevenciu tvorí triedenie, zatvorenie suchých zipsov, ochranné vrecko a odstránenie drsných kontaktov.",
        "intro": "Dlhá slučka na svetri, tenká vytiahnutá čiara na hladkej blúzke a uvoľnené očko na športovom tričku môžu vznikať podobným zachytením, no nemusia sa opravovať rovnako. Konštrukcia úpletu, tkaniny, filamentovej priadze alebo čipky určuje, či sa iba presunula dĺžka nite, deformovalo okolie alebo sa vlákna už pretrhli. Zatrhnutie navyše nie je žmolok: žmolkovanie vzniká postupným zaplietaním voľných vlákien do uzlíkov, kým pri zatrhnutí je viditeľne vytiahnutý konštrukčný prvok.",
        "quick": [
            "<strong>Vytiahnuté očko nestrihajte naslepo:</strong> mohli by ste prerušiť niť, ktorá stále drží okolité slučky.",
            "<strong>Najväčšie riziko predstavujú háčiky:</strong> suchý zips, poškodený zips, prsteň, necht, drsný nábytok aj zvieracie pazúry.",
            "<strong>Štruktúra rozhoduje:</strong> voľný úplet, hladká filamentová tkanina a jemná čipka reagujú na zachytenie odlišne.",
            "<strong>Práčka nie je jediný zdroj:</strong> stopa môže vzniknúť pri nosení, obliekaní, sedení alebo ukladaní.",
            "<strong>Malé poškodenie izolujte včas:</strong> ďalšie trenie môže zväčšiť slučku, stiahnuť okolie alebo vytvoriť dieru.",
        ],
        "overview_heading": "Čo je zatrhnutie a čo sa deje v štruktúre látky",
        "overview": [
            "Textilný povrch tvorí sústava priadzí. V tkanine sa osnova a útok navzájom preväzujú, v úplete jedna alebo viaceré priadze vytvárajú prepojené slučky. Keď výstupok zachytí časť priadze, vytiahne ju z pôvodnej polohy a napätie sa prerozdelí do okolia. Môže zostať samostatná slučka, dlhá línia, stiahnutá plocha alebo kombinácia s pretrhnutými vláknami.",
            "Citlivosť nevyplýva iba z názvu vlákna. Polyamid môže byť spracovaný ako hustá pevná tkanina aj ako jemný hladký úplet; oba povrchy sa správajú inak. Dlhé filamenty môžu po zachytení vytvoriť nápadnú súvislú stopu, kým chlpatá priadza môže drobnú deformáciu čiastočne zakryť. Viac o vlastnostiach vlákna vysvetľuje článok <a href=\"/n/co-je-polyamid-alebo-nylon-vlastnosti-odolnost-a-pranie\">čo je polyamid alebo nylon</a>.",
            "Odolnosť voči oderu, žmolkovaniu a zatrhávaniu sú príbuzné, ale nie totožné vlastnosti. Materiál môže dobre znášať plošné trenie a napriek tomu sa zachytiť o jeden ostrý bod. Rovnako nízka tvorba žmolkov neznamená, že hladká priadza nevytvorí dlhú vytiahnutú slučku. Pri porovnávaní výrobkov preto treba vedieť, aký typ poškodenia skúška alebo tvrdenie opisuje.",
        ],
        "table1_heading": "Zatrhnutie, žmolok, diera alebo otvorený šev",
        "table1_intro": "Správne pomenovanie poškodenia pomáha zvoliť zásah, ktorý stav nezhorší. Pri pochybnosti výrobok ďalej nenoste ani neperte s drsnými kusmi, kým ho neprezriete.",
        "table1_headers": ["Jav", "Typický vzhľad", "Čo sa stalo", "Čomu sa vyhnúť"],
        "table1_rows": [
            ("Zatrhnutie", "Jedna slučka, vytiahnutá niť, čiara alebo stiahnuté okolie.", "Priadza alebo slučka sa zachytila a posunula z pôvodnej polohy.", "Neodstrihovať bez posúdenia konštrukcie."),
            ("Žmolkovanie", "Drobné uzlíky z vlákien na povrchu, často vo väčšej ploche.", "Uvoľnené vlákna sa trením zaplietli a zostali prichytené.", "Nezamieňať s jednou konštrukčnou slučkou a nevytrhávať hrubo."),
            ("Diera", "Otvor s pretrhnutými alebo unikajúcimi slučkami a vláknami.", "Materiál sa prerušil, prerezel, prepálil alebo sa poškodenie rozšírilo.", "Nenaťahovať a neprať bez dočasného zaistenia."),
            ("Posun priadzí", "Priadze tkaniny sa rozostúpia, často pri šve, bez jasného rezu.", "Zaťaženie odsunulo priadze v riedkej alebo klzkej štruktúre.", "Nespoliehať sa iba na zatlačenie medzery prstami."),
            ("Otvorený šev", "Vrstvy sa oddelia v línii stehov; vidno prerušenú alebo vypáranú niť.", "Zlyhal steh, šijacia niť, látka alebo ich kombinácia.", "Nezaťažovať a najprv určiť typ poruchy."),
        ],
        "sections": [
            (
                "Prečo sú niektoré úplety a hladké látky citlivejšie",
                [
                    "Voľnejšia konštrukcia poskytuje háčiku viac priestoru zachytiť slučku alebo priadzu. Pri pletenine môže vytiahnutie jednej slučky odobrať dĺžku susedným očkám, takže okolie sa zvlni alebo stiahne. Jemný sveter, sieťovina či športový úplet preto potrebuje oddelenie od predmetov, ktoré by hustej pevnej bavlnenej tkanine nemuseli uškodiť.",
                    "Hladký lesklý povrch môže byť vytvorený dlhými filamentmi alebo väzbou, ktorá odhaľuje dlhšie úseky priadze. Zachytenie potom vytvorí súvislú viditeľnú líniu. Satén navyše nie je názov jedného vlákna, ale typ väzby či vzhľadu, preto sa riaďte konkrétnym zložením a štítkom. Základ objasňuje sprievodca <a href=\"/n/co-je-saten-nie-je-to-vzdy-hodvab-a-ako-ho-spravne-prat\">čo je satén a ako ho prať</a>.",
                ],
            ),
            (
                "Suchý zips, zips a kovanie v jednej náplni",
                [
                    "Háčiková strana suchého zipsu je navrhnutá tak, aby zachytávala slučky. Ak zostane otvorená, môže sa prichytiť na úplet, čipku, froté aj elastický lem. Zips s poškodeným zubom, ostrý jazdec, uvoľnený háčik podprsenky alebo kovová pracka vytvárajú podobné bodové riziko. Každý prvok pred praním skontrolujte, bezpečne uzavrite a podľa potreby celý kus vložte do vrecka.",
                    "Samotné zapnutie nestačí, ak je kovanie ostré alebo sa môže počas cyklu znovu otvoriť. Poškodený diel radšej opravte pred praním. Jemnú bielizeň oddeľte od riflí, búnd a uterákov, aj keď majú podobnú farbu. Podrobný postup nájdete v článku <a href=\"/n/ako-prat-oblecenie-so-zipsami-a-suchym-zipsom-bez-zatrhnutia\">ako prať oblečenie so zipsami a suchým zipsom</a>.",
                ],
            ),
            (
                "Šperky, nechty, tašky a drsné povrchy pri nosení",
                [
                    "Zatrhnutie často vznikne mimo práčky. Prsteň, náramok, poškodený necht alebo hrana kabelky sa pohybujú po rovnakom mieste pri každom obliekaní. Popruh batoha môže tlačiť povrch o hrubý zips a drsná hrana stola či lavice zachytiť hladkú tkaninu. Čerstvú stopu preto hľadajte aj podľa toho, kde sa na odeve nachádza.",
                    "Opakované línie na rukáve môžu súvisieť s pracovnou doskou alebo šperkom, poškodenie na boku s taškou a očká na nohaviciach s hranou sedadla. Odstránenie zdroja je rovnako dôležité ako oprava. Ak sa nový kus zatrhne vždy na tom istom mieste, skontrolujte aj okolie domácnosti, auta a pracoviska.",
                ],
            ),
            (
                "Ako pripraviť citlivý textil do práčky",
                [
                    "Najprv si prečítajte štítok a vyprázdnite vrecká. Zapnite zipsy podľa konštrukcie, prekryte suché zipsy, zaistite háčiky a odstráňte odnímateľné kovové doplnky. Jemný kus obráťte naruby, aby bolo líce menej vystavené priamemu kontaktu, a vložte ho do dostatočne veľkého pracieho vrecka. Vrecko nemá byť napchaté; textil sa musí preprať a opláchnuť.",
                    "Zvoľte náplň s podobnou hmotnosťou a povrchom. Ťažké mokré rifle, froté uteráky a drobná čipka nie sú vhodní partneri v jednom bubne. Nepreplňujte práčku a použite mechaniku povolenú štítkom. Pri podprsenkách a konštrukčne jemných kusoch pomôže samostatný návod <a href=\"/n/ako-prat-podprsenku-a-jemnu-spodnu-bielizen-bez-deformacie\">ako prať podprsenku a jemnú spodnú bielizeň</a>.",
                ],
            ),
            (
                "Preplnenie bubna a nesprávne pracie vrecko",
                [
                    "Preplnený bubon obmedzuje voľný pohyb, zvyšuje stláčanie a sťažuje oplach. Textil sa môže pevnejšie tlačiť o kovanie iného kusa. Na druhej strane malé voľne pohodené jemné veci medzi ťažkými výrobkami tiež nemajú ochranu. Primerané triedenie a náplň sú preto dôležitejšie než snaha vyprať čo najviac naraz.",
                    "Pracie vrecko funguje ako fyzická bariéra, nie ako povolenie miešať všetky materiály. Príliš malé vrecko výrobok stlačí, poškodená sieťovina môže sama zachytávať a otvorený zips vrecka predstavuje ďalší tvrdý prvok. Po každom použití ho prezrite, zatvorte a vyberte veľkosť, v ktorej sa kus môže pohybovať bez vytŕčania.",
                ],
            ),
            (
                "Ako postupovať pri čerstvo vytiahnutom očku",
                [
                    "Výrobok položte na rovnú plochu a odstráňte napätie. Očko neťahajte, nestrihajte a nepokúšajte sa ho zatlačiť ostrým predmetom cez líce. Prezrite rub a sledujte, či ide o jednu vychýlenú priadzu, viac stiahnutých očiek alebo začínajúcu dieru. Pred zásahom si poškodenie odfoťte, najmä pri novom hodnotnom výrobku.",
                    "Pri niektorých úpletoch možno dĺžku veľmi jemne rozložiť do susedných slučiek a zvyšok vytiahnuť na rub tupou opravárskou pomôckou. Taký postup však nie je univerzálny. Jemný hodvábny vzhľad, viacfarebný vzor, funkčná pletenina alebo dlhá deformácia si zaslúžia krajčíra či opravára, ktorý pozná konštrukciu. Uzol na líci zanechá hrčku a môže sústrediť ďalšie napätie.",
                ],
            ),
            (
                "Kedy sa zo zatrhnutia stáva diera",
                [
                    "Ak sa priadza pretrhne, úplet môže začať púšťať očká a tkanina sa môže strapkať. Riziko rastie pri ďalšom naťahovaní, nosení a praní. Diera má viditeľný otvor a prerušené časti štruktúry; samotné vtiahnutie voľného konca na rub už neobnoví nosnosť. Malé poškodenie treba zaistiť skôr, než sa rozšíri.",
                    "Dierky na tričku môžu mať aj iné príčiny: trenie pri páse, kovanie, mole, chemické poškodenie alebo oslabené vlákna. Vizuálne podobné otvory preto neposudzujte iba podľa toho, že sa objavili po praní. Pomôže návod <a href=\"/n/ako-predist-dierkam-v-trickach-po-prani-a-suseni\">ako predísť dierkam v tričkách po praní a sušení</a>.",
                ],
            ),
            (
                "Dlhodobá prevencia v šatníku a domácnosti",
                [
                    "Prejdite rukou po vnútorných hranách koša na bielizeň, zásuviek a políc. Odštiepené drevo, ostrý plast, poškodený kovový kôš alebo vyčnievajúci drôt môžu vytvárať opakované škody. Jemné úplety ukladajte zložené, ak by ramienko na malom bode materiál naťahovalo, a držte ich oddelene od opaskov, šperkov a otvorených suchých zipsov.",
                    "Pred oblečením jemnej látky skontrolujte šperky a nechty. Pri zvieratách obmedzte priamy kontakt pazúrov s voľným úpletom. Povrch nábytku opravte alebo prekryte, ak sa na tom istom mieste zachytáva viac výrobkov. Prevencia je najúčinnejšia vtedy, keď odstráni konkrétny háčik, nie keď iba zvolí jemnejší program v práčke.",
                ],
            ),
        ],
        "notes": {
            3: (
                "Najčastejšie skryté zdroje zachytenia",
                [
                    "Otvorená háčiková strana suchého zipsu alebo deformovaný zub zipsu.",
                    "Prasknutý necht, prsteň, náramok, zips tašky alebo kovová pracka.",
                    "Odštiepená hrana stola, drsná sedačka, poškodený kôš na bielizeň či drôt police.",
                    "Háčik podprsenky, ktorý sa uvoľnil mimo pracieho vrecka.",
                ],
                "#f7fbf8",
                "#dbe5de",
            ),
            7: (
                "Zastavte sa pred odstrihnutím",
                [
                    "Jedna slučka môže byť stále súčasťou súvislej priadze, ktorá drží väčšiu oblasť úpletu.",
                    "Odstrihnutím získate dva voľné konce a môžete vytvoriť otvor, ktorý sa bude ďalej párať.",
                    "Ak nepoznáte konštrukciu alebo je výrobok hodnotný, nechajte poškodenie posúdiť odborníkovi.",
                ],
                "#fffaf5",
                "#e6ded2",
            ),
        },
        "table2_heading": "Ochrana podľa typu výrobku a rizikového kontaktu",
        "table2_intro": "Neexistuje jeden ochranný postup pre všetky textílie. Nasledujúci prehľad spája typické výrobky s miestami, na ktorých sa oplatí urobiť kontrolu ešte pred praním alebo nosením.",
        "table2_headers": ["Výrobok", "Typické riziko", "Príprava", "Poškodenie riešte"],
        "table2_rows": [
            ("Jemný sveter", "Šperky, nechty, zips bundy a drsné sedadlo.", "Prať podľa štítku naruby a oddelene od háčikov; skladovať bez bodového ťahu.", "Bez strihania slučky, jemným rozložením alebo odbornou opravou."),
            ("Športový úplet", "Suchý zips výstroja, popruh batoha a hrubá sieťovina.", "Uzavrieť kovanie, použiť vrecko a nemiešať s výstrojom.", "Po posúdení pružnosti; opravou, ktorá neobmedzí funkčný pohyb."),
            ("Saténová alebo hladká blúzka", "Prsteň, kabelka, ostrá hrana a dlhý vytiahnutý filament.", "Minimalizovať trenie a prať s hladkými ľahkými kusmi podľa štítku.", "Bez uzla na líci; pri dlhej línii profesionálne."),
            ("Čipka a spodná bielizeň", "Háčiky, kostice, zipsy a preplnené vrecko.", "Zaistiť háčiky a použiť nepoškodené vrecko vhodnej veľkosti.", "Skôr ručne alebo u opravára, ak je poškodený nosný lem."),
            ("Froté uterák", "Otvorený suchý zips a ostré kovanie zachytí slučku.", "Prať bez háčikových prvkov a s podobne ťažkými textíliami.", "Voľnú slučku neposudzovať ako žmolok; zabezpečiť podľa konštrukcie."),
        ],
        "steps": [
            "Prestaňte miesto naťahovať a položte výrobok voľne na rovnú dobre osvetlenú plochu.",
            "Prezrite líce aj rub a určte, či ide o jednu slučku, stiahnutú líniu, pretrhnutú priadzu, dieru alebo otvorený šev.",
            "Odfoťte stav a pri novom výrobku pred zásahom zvážte reklamačné podmienky.",
            "Neodstrihujte a neviažte slučku na líci; odstráňte predmet, ktorý mohol poškodenie spôsobiť.",
            "Ak konštrukciu poznáte, dĺžku rozkladajte iba jemne a bez ostrého prepichovania viditeľnej plochy.",
            "Poškodenie s pretrhnutím, dlhou čiarou, funkčnou vrstvou alebo veľkou deformáciou odovzdajte krajčírovi.",
            "Pred ďalším praním kus obráťte naruby, ochráňte vhodným vreckom a oddeľte od všetkých háčikov a ťažkých textílií.",
        ],
        "remember": [
            "Zatrhnutie je posun alebo vytiahnutie priadze; žmolok je uzlík zo zapletených voľných vlákien.",
            "Suchý zips je zámerne navrhnutý na zachytávanie slučiek a musí byť prekrytý.",
            "Ochranné vrecko musí byť nepoškodené a primerane veľké.",
            "Odstrihnutie očká môže zmeniť kozmetickú chybu na konštrukčnú dieru.",
            "Opakované poškodenie na rovnakom mieste ukazuje na konkrétny kontakt, ktorý treba odstrániť.",
        ],
        "mistakes": [
            "Odstrihnúť vytiahnutú slučku tesne pri povrchu bez kontroly rubu a konštrukcie.",
            "Prať jemný úplet spolu s otvoreným suchým zipsom, podprsenkovými háčikmi alebo poškodeným zipsom.",
            "Napchať viac kusov do malého pracieho vrecka a očakávať dostatočný pohyb a oplach.",
            "Zamieňať jednu vytiahnutú niť za žmolok a pokúšať sa ju vytrhnúť odžmolkovačom.",
            "Urobiť pevný uzol na líci, ktorý zostane viditeľný a sústredí napätie.",
            "Hľadať príčinu iba v práčke, hoci sa očká pravidelne objavujú pri šperku, taške alebo nábytku.",
        ],
        "expert_heading": "Odbornejší pohľad: ako sa odolnosť proti zatrhávaniu skúša",
        "expert": [
            "ASTM D3939/D3939M opisuje skúšanie odolnosti textílií proti zatrhávaniu pomocou zariadenia s ostnatými prvkami, ktoré pri kontrolovanom pohybe vytvára náhodné zachytenia povrchu. Výsledný vzhľad sa hodnotí podľa postupu uvedeného v metóde. Skúška vytvára reprodukovateľnejšie podmienky než domáce šúchanie látky o náhodný predmet, no nevie napodobniť každý šperk, zips ani pohyb pri nosení.",
            "ASTM D5362 používa odlišný princíp so skúšobnými vreckami a samostatne vymedzuje oblasť použitia. Rozdiel v zariadení, príprave vzorky a hodnotení je dôvod, prečo sa výsledky dvoch metód nemajú bez kontextu postaviť do jednej stupnice. Pri niektorých materiáloch sa sleduje aj to, či pranie alebo iná predpríprava zmenila náchylnosť povrchu.",
            "Norma ISO 12945-4 sa venuje hodnoteniu žmolkovania, plstnateniu a matovaniu voľným pohybom, nie tomu istému javu ako vytiahnutá slučka pri zatrhnutí. Táto hranica je dôležitá aj pri nákupe: tvrdenie o odolnosti proti žmolkom automaticky neznamená odolnosť proti bodovému zachyteniu. Pýtajte sa na názov metódy, skúšaný stav výrobku a spôsob hodnotenia.",
        ],
        "source_intro": "Metódy používajú rozdielne zariadenia, prípravu a rozsah použitia. Výsledok má význam iba s názvom konkrétnej metódy a nemožno ho bez ďalších údajov zameniť za hodnotenie žmolkovania či oderu.",
        "sources": [
            ("ASTM D3939/D3939M: odolnosť textílií proti zatrhávaniu metódou mace", ASTM_SNAG_MACE),
            ("ASTM D5362: odolnosť textílií proti zatrhávaniu metódou bean bag", ASTM_SNAG_BAG),
            ("ISO 12945-4: hodnotenie žmolkovania, plstnatenia a matovania", ISO_PILLING),
            ("GINETEX: význam symbolov ošetrovania textilu", GINETEX),
        ],
        "product_intro": "Pri citlivom textile je primeraná mechanika, triedenie a ochrana pred háčikmi dôležitejšia než vysoká dávka prostriedku. Gél vyberajte podľa štítku a náplne, nie ako riešenie už vytiahnutého očka.",
        "product_text": "Prací gél na bežnú domácu bielizeň použite v množstve uvedenom na etikete s ohľadom na tvrdosť vody a znečistenie. Jemný kus pred vložením do bubna zabezpečte a overte, či jeho štítok povoľuje zvolený cyklus.",
        "product_limit": "Žiadny prací gél nezabráni mechanickému zachyteniu o otvorený suchý zips či ostrú hranu. Ochranu tvorí najmä fyzické oddelenie a správne pripravená náplň.",
        "category_intro": "Pri porovnaní pracích gélov zohľadnite materiál, spôsob dávkovania a celý prací postup. Citlivé úplety môžu vyžadovať osobitný výrobok alebo ručné ošetrenie podľa štítku.",
        "category_text": "V kategórii nájdete gély pre rôzne bežné pracie potreby. Pred výberom skontrolujte etiketu a pri jemných či funkčných výrobkoch rešpektujte všetky obmedzenia ich výrobcu.",
        "related": [
            ("Ako prať oblečenie so zipsami a suchým zipsom", ARTICLE_ZIPS),
            ("Prečo sa oblečenie žmolkuje", ARTICLE_PILLING),
            ("Ako predísť dierkam v tričkách", ARTICLE_HOLES),
            ("Ako prať podprsenku a jemnú spodnú bielizeň", ARTICLE_UNDERWEAR),
            ("Pevnosť šva a posun nití", ARTICLE_SEAMS),
            ("Stálofarebnosť textilu pri praní, svetle a trení", ARTICLE_COLORFASTNESS),
        ],
        "faq_title": "zatrhávanie textilu a vytiahnuté očká",
        "faq": [
            ("Mám vytiahnuté očko odstrihnúť?", "Nie automaticky. Pri úplete alebo dlhej filamentovej priadzi môže odstrihnutie vytvoriť dva voľné konce a následnú dieru. Najprv prezrite konštrukciu z rubu."),
            ("Aký je rozdiel medzi zatrhnutím a žmolkom?", "Zatrhnutie je vytiahnutá alebo posunutá priadza či slučka. Žmolok je uzlík vytvorený zapletením voľných vlákien na povrchu počas trenia."),
            ("Prečo sa oblečenie zatrhne v práčke?", "Najčastejšie sa zachytí o suchý zips, poškodený zips, háčik, kovanie alebo iný drsný kus. Riziko zvyšuje nevhodné triedenie, preplnenie a chýbajúce ochranné vrecko."),
            ("Pomôže pranie naruby?", "Znižuje priamy kontakt lícnej plochy s inými kusmi, ale nie je úplnou ochranou. Háčiky treba uzavrieť a jemný výrobok podľa potreby vložiť do vrecka."),
            ("Dá sa zatrhnutie úplne opraviť?", "Malé očko možno pri niektorých konštrukciách jemne rozložiť alebo presunúť na rub. Dlhá deformácia, pretrhnutá priadza či diera často vyžadujú viditeľnú alebo odbornú opravu."),
            ("Ktoré textílie sa zatrhávajú najľahšie?", "Vyššie riziko majú voľné úplety, jemné sieťoviny, čipky a hladké povrchy s dlhšími odkrytými úsekmi priadze. Rozhoduje však konkrétna konštrukcia, nie iba názov vlákna."),
        ],
    },
    {
        "title": "Počet nití pri obliečkach: čo znamená thread count a čo o kvalite nehovorí",
        "link": "pocet-niti-pri-oblieckach-co-znamena-thread-count-a-co-o-kvalite-nehovori",
        "meta": "Čo znamená thread count pri obliečkach, ako sa počíta osnova a útok a prečo o pohodlí rozhoduje aj priadza, väzba, vlákno a gramáž.",
        "short": "Počet nití opisuje hustotu osnovných a útkových priadzí, nie celú kvalitu obliečok. Sprievodca vysvetľuje thread count, jemnosť priadze, väzbu, gramáž aj praktický výber posteľnej bielizne.",
        "answer": "Thread count je počet osnovných a útkových nití v určenej ploche alebo prepočte na jednotku dĺžky; najčastejšie sa uvádza na štvorcový palec, no skúšobné normy môžu pracovať s počtom priadzí na centimeter. Vyššie číslo samo osebe nezaručuje mäkšie, pevnejšie ani priedušnejšie obliečky. Výsledný pocit a životnosť ovplyvňuje aj druh a kvalita vlákna, jemnosť a zloženie priadze, väzba, gramáž, povrchová úprava, šitie a rozmerová stabilita. Porovnávajte iba údaje s jasnou jednotkou a rovnakým spôsobom počítania.",
        "intro": "Na obale jedných obliečok je číslo 200, na druhých 400 a tretie údaj vôbec neuvádzajú. Bez jednotky a metodiky však samotné číslo nepovie, koľko priadzí sa skutočne nachádza v jednom smere, či boli do údajov započítané jednotlivé zložky viacnásobnej priadze ani aká je hrúbka a kvalita použitého vlákna. Hustá tkanina z veľmi jemnej priadze môže pôsobiť ľahko, zatiaľ čo nižší počet hrubších priadzí vytvorí vyššiu plošnú hmotnosť. Rozumný výber preto nestavia jedno číslo nad dotyk, konštrukciu a použitie.",
        "quick": [
            "<strong>Osnova a útok sa počítajú spolu:</strong> údaj vychádza z hustoty priadzí v dvoch kolmých smeroch tkaniny.",
            "<strong>Jednotka je nevyhnutná:</strong> počet na palec a počet na centimeter nie sú rovnaké čísla.",
            "<strong>Jemnosť priadze mení význam výsledku:</strong> veľa jemných priadzí môže vytvoriť inú hmotnosť a pocit než menej hrubých.",
            "<strong>Väzba ovplyvňuje povrch:</strong> perkál a saténová väzba môžu mať pri podobnej hustote rozdielny dotyk, lesk a správanie.",
            "<strong>O kvalite rozhoduje súbor vlastností:</strong> vlákno, priadza, väzba, gramáž, švy, úprava a stálosť rozmerov treba čítať spolu.",
        ],
        "overview_heading": "Čo sa pri počte nití skutočne počíta",
        "overview": [
            "Tkanina má dve základné sústavy priadzí. Osnova vedie v smere výroby tkaniny a útok ju priečne preväzuje. Hustota sa zisťuje spočítaním počtu priadzí na definovanej dĺžke v každom smere. Spotrebiteľské označenie thread count potom spravidla vyjadruje súčet oboch smerov pre určenú plochu, často jeden štvorcový palec. Bez uvedenej jednotky a pravidla započítania však údaj nie je úplný.",
            "Prepočet medzi palcom a centimetrom nie je iba premenovanie. Jeden palec má 2,54 centimetra, preto číslo pri počte na centimeter vyzerá menšie než číslo pre rovnakú hustotu na palec. Pri porovnaní dvoch obalov si najprv overte, či oba výrobcovia uvádzajú rovnakú jednotku a či ide o počet priadzí v jednom smere, súčet smerov alebo údaj pre plochu.",
            "Počet nití sa týka tkaných obliečok. Pletená posteľná bielizeň, napríklad džersej, má slučkovú konštrukciu a hodnotí sa inými parametrami. Ani pri tkanine nejde o samostatnú známku. Bavlna, ľan, polyester či zmes môžu mať rovnaký počet, no celkom inú savosť, povrch a údržbu. Pri výbere podľa klímy a pocitu pomôže porovnanie <a href=\"/n/bavlna-lan-saten-alebo-flanel-ake-obliecky-vybrat-podla-sezony-a-potenia\">bavlny, ľanu, saténu a flanelu pri obliečkach</a>.",
        ],
        "table1_heading": "Údaje na obale a otázky, ktoré sa oplatí položiť",
        "table1_intro": "Jeden údaj môže byť užitočný, ak poznáte jeho význam a hranice. Tabuľka ukazuje, čo jednotlivé parametre opisujú a čo z nich nemožno bezpečne vyvodiť.",
        "table1_headers": ["Údaj", "Čo opisuje", "Čo sám nepotvrdzuje", "Ako ho čítať"],
        "table1_rows": [
            ("Thread count", "Hustotu osnovných a útkových priadzí podľa uvedeného spôsobu počítania.", "Kvalitu vlákna, priedušnosť, mäkkosť, životnosť ani poctivosť šitia.", "Spolu s jednotkou, metodikou a ďalšími vlastnosťami."),
            ("Zloženie vlákien", "Percentuálny podiel bavlny, ľanu, polyesteru alebo iných vlákien.", "Jemnosť priadze, dĺžku vlákien, väzbu ani konečný dotyk.", "Ako materiálový základ, nie úplný opis hotovej látky."),
            ("Gramáž", "Hmotnosť materiálu na jednotku plochy, zvyčajne v g/m².", "Počet priadzí, väzbu, savosť ani automatickú odolnosť.", "Na porovnanie hmotnosti podobných konštrukcií a účelu."),
            ("Väzba", "Spôsob previazania osnovy a útku, napríklad plátnová alebo saténová.", "Druh vlákna ani sama osebe úroveň spracovania.", "Ako vysvetlenie povrchu, lesku, ohybnosti a časti správania."),
            ("Rozmer a tolerancia", "Veľkosť obliečky a spôsob, ako má pasovať na výplň.", "Stálosť po opakovanom praní, ak tá nie je osobitne doložená.", "Spolu s rozmerom paplóna, zapínaním a pokynmi údržby."),
        ],
        "sections": [
            (
                "Osnova, útok a hustota tkaniny bez zbytočných skratiek",
                [
                    "Osnovné priadze sú pri tkaní napnuté pozdĺžne a útková priadza sa medzi nimi ukladá priečne. Počet v každom smere môže byť odlišný podľa väzby a požadovaného povrchu. Ak sa uvádza celkový počet, vzniká súčtom hustoty osnovy a útku pre dohodnutú jednotku. Napríklad dve tkaniny s rovnakým súčtom môžu mať odlišný pomer smerov, a preto aj inú konštrukciu.",
                    "Hustota nie je to isté ako hrúbka priadze. Rovnaký priestor môže obsahovať viac veľmi jemných alebo menej hrubších priadzí. Jemnejšia priadza zasa vyžaduje vhodné vlákno, spriadanie a spracovanie, aby mala potrebnú rovnomernosť a pevnosť. Samotný súčet preto neodhaľuje, z čoho a ako boli priadze vyrobené.",
                ],
            ),
            (
                "Prečo je jednotka dôležitejšia než veľké číslo",
                [
                    "Údaj na palec a údaj na centimeter vyzerajú numericky veľmi odlišne. Ak výrobca uvádza iba číslo bez jednotky, spotrebiteľ nevie, na akej základni ho porovnáva. Pri technickom liste hľadajte tiež informáciu, či sa uvádza počet koncov osnovy a útkov samostatne, alebo ich súčet. Rovnaké označenie v marketingovom poli nemusí byť naprieč krajinami a značkami zapísané totožne.",
                    "Pri porovnaní nepoužívajte rýchly prepočet iba na základe celkového čísla, ak nepoznáte definíciu. Skúšobné normy ISO a ASTM opisujú počítanie priadzí na jednotku dĺžky v smere osnovy a útku. Spotrebiteľské thread count na štvorcový palec je odvodený spôsob komunikácie, pri ktorom treba poznať, ktoré časti priadze boli zahrnuté.",
                ],
            ),
            (
                "Jednoduchá a viacnásobná priadza: prečo záleží na pravidle počítania",
                [
                    "Priadza môže byť jednoduchá alebo zložená z viacerých spolu skrútených jednoduchých priadzí. V hotovej tkanine však skúšobná hustota spravidla sleduje konštrukčné priadze prechádzajúce danou dĺžkou. Ak sa pri spotrebiteľskom tvrdení jednotlivé zložky viacnásobnej priadze komunikujú osobitne, číslo môže rásť bez zodpovedajúceho zvýšenia počtu samostatných osnovných a útkových prvkov v látke.",
                    "Preto sa pri nápadne vysokom údaji pýtajte na spôsob výpočtu, nie automaticky na podvod alebo vyššiu kvalitu. Dôležité je, či výrobca vysvetľuje jednotku, počet zložiek priadze a konštrukciu. Transparentné technické údaje umožnia porovnať podobné výrobky; samotný veľký nápis na obale takú možnosť neposkytuje.",
                ],
            ),
            (
                "Perkál a saténová väzba nie sú názvy vlákien",
                [
                    "Perkál sa zvyčajne spája s jednoduchou plátnovou väzbou, v ktorej sa osnova a útok často striedajú. Výsledkom býva matnejší, svieži a pevnejšie pôsobiaci povrch. Saténová väzba necháva na povrchu dlhšie voľné úseky jednej sústavy priadzí, čo môže vytvoriť hladší dotyk a lesk, ale aj odlišnú náchylnosť na zachytenie a trenie.",
                    "Obe väzby možno vyrobiť z rôznych vlákien. Bavlnený satén je bavlnená tkanina so saténovou väzbou; nemusí obsahovať hodváb. Dve bavlnené obliečky s podobným počtom nití preto môžu pôsobiť úplne inak, ak jedna používa plátnovú a druhá saténovú väzbu. Viac o tomto rozdiele vysvetľuje článok <a href=\"/n/co-je-saten-nie-je-to-vzdy-hodvab-a-ako-ho-spravne-prat\">satén nie je vždy hodváb</a>.",
                ],
            ),
            (
                "Vlákno, jemnosť priadze a povrchová úprava",
                [
                    "Konečný dotyk začína vlastnosťami vlákna, ale pokračuje pri spriadaní, tkaní a úprave. Rovnomernejšia jemná priadza môže umožniť hustú a zároveň ľahkú konštrukciu. Krátke uvoľňujúce sa vlákna môžu meniť povrch a vytvárať žmolky, kým veľmi hladká povrchová úprava môže pri prvom dotyku zakryť rozdiely, ktoré sa ukážu až po niekoľkých praniach.",
                    "Pri bavlne nie je užitočné posudzovať iba percento na etikete. Druh priadze, dĺžka a rovnomernosť vlákien, zvyškové napätie a finálna úprava vplývajú na vzhľad aj stabilitu. Materiálový základ nájdete v sprievodcovi <a href=\"/n/co-je-bavlna-vlastnosti-vyhody-nevyhody-a-starostlivost\">čo je bavlna, jej vlastnosti a starostlivosť</a>.",
                ],
            ),
            (
                "Gramáž a počet nití odpovedajú na iné otázky",
                [
                    "Gramáž vyjadruje plošnú hmotnosť materiálu, zvyčajne v gramoch na štvorcový meter. Počet nití opisuje hustotu priadzí. Vysokú gramáž možno dosiahnuť hrubšou priadzou pri nižšej hustote a vysokú hustotu jemnou priadzou pri relatívne nízkej hmotnosti. Ani jeden údaj preto nemožno jednoducho odvodiť z druhého.",
                    "Pri uteráku je gramáž často prakticky významná pre množstvo materiálu, no stále sama neurčuje savosť a dobu schnutia. Pri obliečke pomáha chápať hmotnosť a pocit látky, ale treba ju čítať spolu s väzbou a zložením. Rozdiel podrobnejšie rozoberá článok <a href=\"/n/gramaz-latky-co-znamena-gsm-pri-uterakoch-oblieckach-a-trickach\">čo znamená gramáž GSM pri textile</a>.",
                ],
            ),
            (
                "Ako vybrať obliečky bez slepého porovnávania jedného čísla",
                [
                    "Začnite pocitom, ktorý chcete: svieži matný povrch, hladšie splývanie, hrejivosť alebo jednoduchú údržbu. Potom skontrolujte zloženie, väzbu, hmotnosť, priehľadnosť, pevnosť švov, zapínanie a rozmery. Pri uvedenom počte nití hľadajte jednotku a vysvetlenie. Veľmi vysoké číslo bez ďalších údajov nie je dôvod automaticky zaplatiť viac.",
                    "Látku prezrite proti svetlu. Nepravidelné medzery, výrazné uzlíky, pokrivená väzba či nedokončené švy môžu byť praktickejšie varovania než rozdiel medzi dvoma blízkymi číslami. Overte možnosť vrátenia, pokyny pri prvom praní a toleranciu rozmerov. Obliečka musí pasovať na výplň bez trvalého napínania rohov a zapínania.",
                ],
            ),
            (
                "Prvé pranie a dlhodobá starostlivosť o obliečky",
                [
                    "Nové obliečky pred prvým použitím vyperte podľa štítku, najmä ak to výrobca odporúča. Zapnite gombíky alebo zips tak, aby sa otvor nezachytával, výrobok obráťte naruby podľa pokynov a perte s podobnými farbami a hmotnosťou. Bubon neprepĺňajte: veľké kusy potrebujú priestor na rozloženie, prepieranie a dôkladný oplach.",
                    "Dávku pracieho prostriedku prispôsobte tvrdosti vody, znečisteniu a náplni. Nadbytok môže zostať v záhyboch a vytvoriť tuhší či klzký pocit, ktorý sa mylne pripíše kvalite priadze. Po cykle obliečky hneď vyberte, rozložte a vysušte podľa štítku. Celý postup zhŕňa návod <a href=\"/n/ako-spravne-prat-obliecky-kompletny-sprievodca-starostlivostou\">ako správne prať obliečky</a>.",
                ],
            ),
        ],
        "notes": {
            3: (
                "Štyri otázky pri údaji thread count",
                [
                    "Je uvedená jednotka a ide o palec, centimeter alebo plochu?",
                    "Sú osnova a útok zapísané samostatne alebo iba ako jeden súčet?",
                    "Ako výrobca počíta zložky viacnásobnej priadze?",
                    "Aké sú vlákno, väzba, jemnosť priadze, gramáž a povrchová úprava?",
                ],
                "#f7fbf8",
                "#dbe5de",
            ),
            7: (
                "Čo si všimnúť priamo na obliečke",
                [
                    "Rovnomernosť tkania a to, či pri presvietení nevidno náhodné riedke pásy.",
                    "Pevnosť a rovnosť švov, ukončenie rohov a spracovanie zapínania.",
                    "Dotyk bez nánosu, zápachu alebo výrazne nerovnomernej povrchovej úpravy.",
                    "Rozmerovú rezervu pre konkrétnu výplň a jasné pokyny ošetrovania.",
                ],
                "#fffaf5",
                "#e6ded2",
            ),
        },
        "table2_heading": "Ako kombinovať parametre pri praktickom výbere",
        "table2_intro": "Nasledujúce príklady nie sú rebríčkom väzieb ani materiálov. Ukazujú, ktoré údaje treba čítať spolu podľa toho, čo od obliečok očakávate.",
        "table2_headers": ["Priorita", "Sledujte najmä", "Doplňujúca kontrola", "Častý chybný záver"],
        "table2_rows": [
            ("Svieži, matný pocit", "Plátnovú alebo perkálovú väzbu, jemnosť priadze a zloženie.", "Hmotnosť, priedušný pocit, rovnomernosť a štítok.", "Čím vyšší počet nití, tým bude látka automaticky sviežejšia."),
            ("Hladký, splývavý povrch", "Saténovú väzbu, kvalitu priadze a povrchové spracovanie.", "Citlivosť na zatrhnutie, lesk po praní a pokyny sušenia.", "Každý satén je hodváb alebo má rovnakú údržbu."),
            ("Odolnosť pri častom praní", "Konštrukciu, švy, stálosť rozmerov a odporúčanú údržbu.", "Rotáciu súprav, správnu veľkosť náplne a sušenie.", "Jedno vysoké číslo zaručuje neobmedzenú životnosť."),
            ("Nižšia hmotnosť", "Gramáž, jemnosť priadze a väzbu.", "Nepriehľadnosť, pevnosť a správanie po prvom praní.", "Nižšia gramáž vždy znamená riedku alebo nekvalitnú látku."),
            ("Jednoduché porovnanie", "Rovnakú jednotku, metódu, vlákno a typ väzby.", "Technický list a jasnosť tvrdení výrobcu.", "Čísla z rôznych systémov možno zoradiť bez ďalšieho kontextu."),
        ],
        "steps": [
            "Určte požadovaný pocit, teplotný komfort a nároky na údržbu namiesto výberu najvyššieho čísla.",
            "Skontrolujte, či ide o tkané obliečky a akú väzbu výrobca uvádza.",
            "Pri thread count vyhľadajte jednotku, osobitné hustoty osnovy a útku alebo vysvetlenie súčtu.",
            "Prečítajte zloženie vlákien, informáciu o priadzi, gramáži a povrchovej úprave, ak sú dostupné.",
            "Prezrite rovnomernosť plochy, švy, rohy, zapínanie a správnosť rozmeru pre svoju výplň.",
            "Porovnávajte podobné konštrukcie a rovnaké jednotky; pri nejasnom extrémnom údaji si vyžiadajte vysvetlenie.",
            "Po kúpe dodržte prvé pranie a ďalšiu starostlivosť podľa štítku a nenechávajte veľké kusy preplniť bubon.",
        ],
        "remember": [
            "Thread count je údaj o hustote priadzí, nie samostatná známka kvality.",
            "Počet na palec a počet na centimeter treba previesť a poznať presnú definíciu.",
            "Rovnaký súčet môže skrývať odlišný pomer osnovy a útku aj inú hrúbku priadze.",
            "Perkál a satén opisujú väzbu, nie automaticky druh vlákna.",
            "Dotyk po niekoľkých praniach môže byť výpovednejší než silná dočasná povrchová úprava.",
        ],
        "mistakes": [
            "Vybrať obliečky iba podľa najvyššieho čísla bez jednotky, väzby a materiálového kontextu.",
            "Porovnať počet na centimeter priamo s počtom na palec ako dve rovnako definované hodnoty.",
            "Predpokladať, že saténová väzba automaticky znamená hodvábne vlákno.",
            "Zamieňať gramáž za počet nití alebo z jedného údaja odhadovať druhý.",
            "Prehliadnuť nekvalitné švy, nevhodný rozmer a nejasný ošetrovací štítok pre vysoké číslo na obale.",
            "Preplniť bubon veľkými obliečkami a tuhý pocit po slabom oplachu pripísať výhradne hustote látky.",
        ],
        "expert_heading": "Odbornejší pohľad: meranie hustoty a jemnosti priadze",
        "expert": [
            "ISO 7211-2:2024 opisuje metódy na stanovenie počtu priadzí na jednotku dĺžky v tkaninách. ASTM D3775 podobne rieši počet osnovných a útkových priadzí v tkaných materiáloch a vymedzuje postup aj použitie. Tieto metódy pracujú s konkrétnou dĺžkou a smerom, preto technicky presný výsledok uvádza hustotu osnovy a útku s jednotkou, nie iba izolované veľké číslo.",
            "ISO 7211-5:2020 sa venuje lineárnej hustote priadze odstránenej z tkaniny, teda vzťahu hmotnosti priadze k jej dĺžke. Tento parameter dopĺňa počet priadzí: dve tkaniny môžu mať podobnú hustotu, no pri rozdielnej jemnosti priadze inú hmotnosť, ohybnosť a pokrytie plochy. Údaje treba vyhodnocovať spoločne a s vedomím použitých postupov.",
            "CottonWorks vo svojom sprievodcovi nákupom bavlnených plachiet uvádza thread count v širšom kontexte pokrytia tkaniny, spriadania, jemnosti priadze, hmotnosti a konštrukcie. Praktický záver nie je nájsť jedno ideálne číslo pre každého, ale porovnať transparentne opísané výrobky s rovnakou jednotkou a podobným účelom. Pohodlie navyše zostáva čiastočne subjektívne a mení sa s klímou, matracom aj spôsobom prania.",
        ],
        "source_intro": "Technické zdroje rozlišujú počet priadzí na jednotku dĺžky od lineárnej hustoty priadze a plošnej hmotnosti. Pri porovnaní treba poznať jednotku, smer a použitú metódu.",
        "sources": [
            ("ISO 7211-2:2024: počet priadzí na jednotku dĺžky", ISO_THREAD_COUNT),
            ("ASTM D3775: počet osnovných a útkových priadzí v tkanine", ASTM_THREAD_COUNT),
            ("ISO 7211-5:2020: lineárna hustota priadze odstránenej z tkaniny", ISO_YARN_DENSITY),
            ("CottonWorks: sprievodca výberom bavlnených plachiet", COTTON_SHEETS),
            ("GINETEX: význam symbolov ošetrovania textilu", GINETEX),
        ],
        "product_intro": "Správna dávka a dostatok priestoru v bubne pomáhajú obliečky vyprať a opláchnuť bez zvyškov. Prací prostriedok však nemení počet nití ani nekvalitné šitie a treba ho zvoliť podľa štítku.",
        "product_text": "Prací gél z marseillského mydla je určený na bežnú domácu bielizeň. Pri obliečkach postupujte podľa etikety výrobku, materiálového zloženia a symbolov a zohľadnite tvrdosť vody aj veľkú savú náplň.",
        "product_limit": "Gél nezvyšuje hustotu ani pevnosť tkaniny. Preplnený bubon a nadmerná dávka môžu zhoršiť oplach bez ohľadu na údaj thread count.",
        "category_intro": "Prací gél vyberajte podľa materiálu obliečok, farby a povolenej údržby. Kvalitu prania určuje aj dávkovanie, program, náplň a sušenie, nie samotná značka prostriedku.",
        "category_text": "V kategórii môžete porovnať pracie gély na bežnú bielizeň. Pred použitím skontrolujte etiketu produktu a pri citlivej povrchovej úprave alebo osobitnom vlákne rešpektujte pokyny výrobcu obliečok.",
        "related": [
            ("Bavlna, ľan, satén alebo flanel: aké obliečky vybrať", ARTICLE_BEDDING_CHOICE),
            ("Čo znamená gramáž GSM pri textile", ARTICLE_GSM),
            ("Čo je bavlna a ako sa o ňu starať", ARTICLE_COTTON),
            ("Čo je satén a ako ho správne prať", ARTICLE_SATIN),
            ("Ako správne prať obliečky", ARTICLE_BEDDING_WASH),
            ("Ako často prať posteľnú bielizeň", ARTICLE_BEDDING_FREQUENCY),
        ],
        "faq_title": "thread count a počet nití pri obliečkach",
        "faq": [
            ("Čo znamená thread count?", "Vyjadruje hustotu osnovných a útkových priadzí podľa uvedeného systému, často ako ich súčet na štvorcový palec. Presný údaj potrebuje jednotku a spôsob počítania."),
            ("Je vyšší počet nití vždy lepší?", "Nie. Bez kvality vlákna, jemnosti priadze, vhodnej väzby, gramáže a dobrého šitia môže byť vysoké číslo málo výpovedné."),
            ("Aký počet nití je najlepší na obliečky?", "Jedno univerzálne optimum neexistuje. Výber závisí od väzby, materiálu, požadovaného pocitu, klímy a transparentnosti údajov výrobcu."),
            ("Je perkál to isté ako bavlna?", "Nie. Perkál opisuje typ tkaniny alebo väzby, zatiaľ čo bavlna je druh vlákna. Perkál môže byť bavlnený alebo zo zmesi podľa etikety."),
            ("Je bavlnený satén skutočný satén?", "Áno, ak je utkaný saténovou väzbou; slovo satén neznamená automaticky hodváb. Bavlnený satén pomenúva bavlnené vlákno a spôsob väzby."),
            ("Prečo sú obliečky po praní tvrdšie, hoci majú vysoký počet nití?", "Príčinou môže byť tvrdá voda, nadmerná dávka, slabý oplach, presušenie alebo vlastnosť povrchovej úpravy. Thread count sám tento pocit neurčuje."),
        ],
    },
]


def article_hrefs(markup):
    return re.findall(r'href="([^"]+)"', markup)


def preflight_links(articles):
    rows = []
    seen = set()
    batch_paths = {f"/n/{article['link']}" for article in articles}
    headers = {"User-Agent": "Codex VEVO batch 42 link preflight"}

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
        raise SystemExit("Batch 42 titles do not exactly match the duplicate-guard candidate file")

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
        raise SystemExit("Batch 42 link preflight failed")


if __name__ == "__main__":
    main()
