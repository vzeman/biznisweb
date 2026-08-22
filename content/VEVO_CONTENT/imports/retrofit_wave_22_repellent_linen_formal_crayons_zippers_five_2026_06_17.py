import argparse
import json
import re
import textwrap
import time
from datetime import datetime, timezone
from pathlib import Path

import requests


ROOT = Path(__file__).resolve().parents[3]
CONFIG_PATH = Path.home() / ".codex" / "config.toml"
OUT_JSON = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-22-repellent-linen-formal-crayons-zippers-five-2026-06-17.json"
MCP_RESULTS = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-22-repellent-linen-formal-crayons-zippers-five-2026-06-17-mcp-results.json"


ARTICLES = [
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-18-2026-06-10-articles.json",
        "slug": "ako-odstranit-repelent-z-outdoorovej-ciapky-a-navlekov-na-ruky",
        "post_id": "2199",
        "url": "https://www.vevo.sk/n/ako-odstranit-repelent-z-outdoorovej-ciapky-a-navlekov-na-ruky",
        "topic": "repellent",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-16-2026-06-10-articles.json",
        "slug": "ako-prat-lanovu-koselu-aby-nezostala-tvrda-a-pokrcena",
        "post_id": "2154",
        "url": "https://www.vevo.sk/n/ako-prat-lanovu-koselu-aby-nezostala-tvrda-a-pokrcena",
        "topic": "linen_shirt",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-18-2026-06-10-articles.json",
        "slug": "ako-prat-spolocenske-saty-doma-a-kedy-zvolit-cistiaren",
        "post_id": "2191",
        "url": "https://www.vevo.sk/n/ako-prat-spolocenske-saty-doma-a-kedy-zvolit-cistiaren",
        "topic": "formal_dress",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-17-2026-06-10-articles.json",
        "slug": "ako-vyprat-voskovky-z-peracnika-a-textilneho-obalu",
        "post_id": "2171",
        "url": "https://www.vevo.sk/n/ako-vyprat-voskovky-z-peracnika-a-textilneho-obalu",
        "topic": "crayons",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-18-2026-06-10-articles.json",
        "slug": "ako-prat-oblecenie-so-zipsami-a-suchym-zipsom-bez-zatrhnutia",
        "post_id": "2188",
        "url": "https://www.vevo.sk/n/ako-prat-oblecenie-so-zipsami-a-suchym-zipsom-bez-zatrhnutia",
        "topic": "zippers_velcro",
    },
]


def clean(markup):
    return textwrap.dedent(markup).strip()


def table(headers, rows):
    head = "".join(f'<th style="border: 1px solid #e5e5e5; padding: 10px; text-align: left;">{h}</th>' for h in headers)
    body = "\n".join(
        "<tr>" + "".join(f'<td style="border: 1px solid #e5e5e5; padding: 10px;">{c}</td>' for c in row) + "</tr>"
        for row in rows
    )
    return (
        '<table style="width: 100%; border-collapse: collapse; margin: 20px 0;">\n'
        f"<thead><tr>{head}</tr></thead>\n<tbody>\n{body}\n</tbody>\n</table>"
    )


def note_card(title, bullets):
    items = "".join(f"<li>{item}</li>" for item in bullets)
    return clean(
        f"""
        <div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">
        <h2 style="margin-top: 0;">{title}</h2>
        <ul>{items}</ul>
        </div>
        """
    )


def recommendation_card(config):
    return clean(
        f"""
        <div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 24px 0; background: #f7fbf8;">
        <h2 style="margin-top: 0;">Odporúčané riešenie pre šetrné pranie</h2>
        <p>{config["recommendation_intro"]}</p>
        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 14px; margin: 14px 0;">
        <div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff;">
        <h3 style="margin-top: 0;">Prací gél hypoalergénny z Marseillského mydla 1L</h3>
        <p>{config["product_text"]}</p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="/p-1551/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l">Pozrieť produkt</a></p>
        </div>
        <div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff;">
        <h3 style="margin-top: 0;">Kategória pracie gély</h3>
        <p>Pri citlivých materiáloch, farebných škvrnách a kusoch s kovom, zipsom alebo aplikáciou pomáha mierne dávkovanie, dobrý oplach a rešpektovanie štítku.</p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="/c/vevo-home-care/pranie/praci-gel">Pozrieť pracie gély</a></p>
        </div>
        </div>
        </div>
        """
    )


def build_related_links(links):
    items = "\n".join(f'<li><a href="{href}">{label}</a></li>' for href, label in links)
    items += '\n<li><a href="/n/ako-vybrat-praci-gel-podla-typu-bielizne">Ako vybrať prací gél podľa typu bielizne</a></li>'
    items += '\n<li><a href="/n/casto-kladene-otazky-skvrny-na-obleceni-po-prani">Škvrny na oblečení po praní</a></li>'
    return f"<h2>Súvisiace návody na VEVO</h2>\n<ul>\n{items}\n</ul>"


TOPICS = {
    "repellent": {
        "marker": "Detailnejší postup na repelent z outdoorovej čiapky a návlekov",
        "problem": "repelent na textile často zanechá kombináciu vône, mastného alebo lepkavého filmu a zvyškov účinnej látky, ktoré sa môžu držať najmä na syntetických outdoorových doplnkoch",
        "scope": "outdoorovej čiapke, rukávnikoch, návlekoch na ruky, šiltovke, šatke, softshellovej vrstve a turistických doplnkoch",
        "avoid": "horúcu vodu ako prvý krok, sušičku, agresívne drhnutie funkčnej vrstvy a pranie spolu s uterákmi alebo detskou bielizňou",
        "diagnosis": [
            "<strong>Repelent nie je len vôňa:</strong> na látke môže zostať film, ktorý sa pri teple zvýrazní.",
            "<strong>Najprv vetrať:</strong> silný pach po aplikácii nechajte vyprchať skôr, než doplnok zavriete do koša.",
            "<strong>Syntetika drží zvyšky inak:</strong> čiapka, návlek a elastická vrstva môžu potrebovať dobrý oplach.",
            "<strong>Pri membráne opatrne:</strong> ak je výrobok súčasťou funkčného oblečenia, riaďte sa štítkom a nepoškodzujte úpravu.",
        ],
        "state_rows": [
            ("čerstvý postrek", "nechať vyvetrať a odobrať prebytok", "nešúchať horúco"),
            ("lepkavý film", "lokálne predprať a dôkladne opláchnuť", "zvyšky produktu"),
            ("silný pach", "prať oddelene a sušiť úplne", "neprekrývať vôňou"),
            ("funkčný doplnok", "podľa štítku, nízka mechanika", "membrána alebo úprava"),
        ],
        "textile_rows": [
            ("outdoorová čiapka", "ručné predčistenie okraja", "pot a repelent pri čele"),
            ("návleky na ruky", "prať naruby a dobre opláchnuť", "kontakt s pokožkou"),
            ("šiltovka", "neprelomiť šilt", "tvar je dôležitý"),
            ("softshellový doplnok", "bez agresívnej aviváže", "funkčná úprava"),
        ],
        "sections": [
            ("Ako odstrániť repelent z outdoorovej čiapky", "Čiapku najprv vyvetrajte a skontrolujte štítok. Najviac zvyškov býva na okraji pri čele, kde sa mieša repelent, pot a kožný maz. Miesto predčistite jemne, bez hrubého drhnutia a bez premočenia pevného šiltu.", "Ak má čiapka tvarovaný šilt alebo výstuž, nekrúťte ju. Po praní ju vytvarujte ešte za vlhka a sušte voľne."),
            ("Ako vyprať repelent z návlekov na ruky", "Návleky sú často elastické a tesne pri pokožke, preto je dôležitý dobrý oplach. Otočte ich naruby, použite primerané množstvo pracieho gélu a neperte ich v preplnenom bubne. Zvyšky repelentu aj pot sa držia v elastických vláknach.", "Ak sú návleky určené na šport, vyhnite sa aviváži, ak ju výrobca neodporúča."),
            ("Repelent na softshelli alebo funkčnej vrstve", "Pri funkčných vrstvách je rizikom poškodenie úpravy alebo membrány. Nevyberajte silný univerzálny zásah len preto, že pach je výrazný. Najprv si pozrite štítok, odoberte prebytok a perte podľa odporúčaného režimu.", "K softshellu nadväzuje návod <a href=\"/n/ako-prat-softshell-bundu-a-nohavice-bez-poskodenia-membrany\">ako prať softshell bundu</a>."),
            ("Prečo repelent po praní stále cítiť", "Repelent môže zostať v švoch, v elastických častiach alebo v mieste opakovaného postreku. Ak pach cítiť až po zahriatí pri nosení, v textile zostal zvyšok produktu alebo potu. Pomáha menšia dávka v práčke, dobrý oplach a úplné vysušenie.", "Silnejšia vôňa do prania problém nevyrieši, ak je látka stále zanesená zvyškom repelentu."),
            ("Kedy neprať outdoorový doplnok s bežnou bielizňou", "Repelentom zasiahnuté doplnky perte radšej oddelene od uterákov, detskej bielizne, posteľnej bielizne a jemných kúskov. Znížite riziko prenosu vône a zvyškov produktu na textílie, ktoré sú v blízkom kontakte s pokožkou.", "Pri veľmi silnom znečistení najprv riešte lokálne miesto, až potom bežné pranie."),
        ],
        "depth": [
            ("Repelent, pot a opaľovací krém", "V lete sa repelent často mieša s potom a opaľovacím produktom. Výsledkom nie je jedna jednoduchá škvrna, ale kombinovaný film. Preto môže byť textil po praní čistý na pohľad, ale stále cítiť alebo pôsobiť lepkavo.", "Ak bol na látke aj opaľovací olej, pomôže súvisiaci postup <a href=\"/n/ako-vyprat-opalovaci-olej-z-plazovej-tuniky-a-uteraka\">ako vyprať opaľovací olej</a>."),
            ("Bezpečnosť pri textile pri pokožke", "Čiapky, šatky a návleky sú v priamom kontakte s pokožkou. Preto nestačí odstrániť len viditeľný fľak. Dôležité je aj to, aby v látke nezostalo veľa pracieho gélu alebo pôvodného produktu. Oplach je pri tejto téme prakticky rovnako dôležitý ako pranie.", "Ak textil po praní dráždi pokožku, znížte dávkovanie a pridajte oplach podľa možností práčky."),
        ],
        "expert_title": "Odbornejší pohľad: zvyškový film na syntetike a funkčných doplnkoch",
        "expert_p1": "Repelenty sú navrhnuté tak, aby zostali určitý čas na povrchu. Keď sa dostanú na textil, môžu sa zachytiť na syntetických vláknach, švoch a elastických častiach. Preto sa pri praní nepozerajte len na viditeľný fľak, ale aj na pach, lepkavosť a pocit pri nosení.",
        "expert_p2": "Pri outdoorových doplnkoch treba súčasne chrániť funkciu výrobku. Príliš agresívne čistenie môže poškodiť povrchovú úpravu, zatiaľ čo slabý oplach nechá zvyšky produktu pri pokožke.",
        "checklist": "Pred praním zistite, kde bol repelent aplikovaný, či je textil elastický alebo funkčný, či má šilt alebo výstuž, či je prateľný a či pach pochádza z repelentu, potu alebo opaľovacieho produktu.",
        "rule": "Pri repelente najprv vetrať a odobrať prebytok, potom šetrne predčistiť, prať oddelene a dôkladne opláchnuť.",
        "recommendation_intro": "Pri repelente je prací gél súčasťou riešenia, ale rozhoduje aj oplach a oddelené pranie. Cieľom je odstrániť zvyškový film, nie ho prekryť vôňou.",
        "product_text": "Vhodný na šetrné pranie prateľných outdoorových doplnkov podľa štítku, keď potrebujete odstrániť pot, pach a bežné zvyšky z textilu.",
        "links": [
            ("/n/ako-prat-softshell-bundu-a-nohavice-bez-poskodenia-membrany", "Ako prať softshell bundu"),
            ("/n/ako-obnovit-impregnaciu-softshellu-po-prani-a-kedy-ju-neriesit", "Ako obnoviť impregnáciu softshellu"),
            ("/n/ako-vyprat-opalovaci-olej-z-plazovej-tuniky-a-uteraka", "Ako vyprať opaľovací olej"),
        ],
        "faq": [
            ("Ako odstrániť pach repelentu z čiapky?", "Najprv vetrať, potom jemne predčistiť okraj a vyprať podľa štítku s dobrým oplachom."),
            ("Môžem použiť aviváž?", "Pri funkčných outdoorových doplnkoch radšej nie, ak ju výrobca neodporúča. Môže zhoršiť vlastnosti textilu."),
            ("Prečo je návlek po praní stále lepkavý?", "V elastických častiach mohli zostať zvyšky repelentu, potu alebo pracieho prostriedku. Pomáha menšia dávka a lepší oplach."),
        ],
    },
    "linen_shirt": {
        "marker": "Detailnejší postup na ľanovú košeľu, tvrdosť a krčenie",
        "problem": "ľanová košeľa je prirodzene pevná, savá a krčivá, preto po zlom praní alebo sušení môže pôsobiť tvrdšie, ostro pokrčená alebo vytiahnutá v švoch",
        "scope": "ľanovej košeli, ľanovej blúzke, letných šatách, ľanovom obruse, zmesiach ľanu s bavlnou a svetlom letnom oblečení",
        "avoid": "preplnený bubon, silné odstreďovanie, presušenie na radiátore, veľa pracieho gélu a nechávanie košele pokrčenej v práčke",
        "diagnosis": [
            "<strong>Krčenie je vlastnosť ľanu:</strong> cieľom nie je úplne ho odstrániť, ale zjemniť výsledok.",
            "<strong>Tvrdosť často vzniká sušením:</strong> presušená košeľa pôsobí ostrejšie a horšie sa žehlí.",
            "<strong>Priestor v bubne pomáha:</strong> ľan potrebuje vodu a pohyb, nie natlačenú dávku.",
            "<strong>Tvarujte za vlhka:</strong> vyrovnanie po praní rozhoduje viac než agresívne žehlenie.",
        ],
        "state_rows": [
            ("tvrdá košeľa", "skontrolovať dávkovanie a sušenie", "nie vždy chyba materiálu"),
            ("silné krčenie", "vyrovnať za vlhka", "nechať doschnúť v tvare"),
            ("ľan s bavlnou", "postup podľa citlivejšej zložky", "zmes sa môže správať inak"),
            ("škvrna na ľane", "lokálne jemne predčistiť", "nešúchať agresívne"),
        ],
        "textile_rows": [
            ("ľanová košeľa", "jemné pranie a nižšie otáčky", "tvar a pokrčenie"),
            ("ľanová blúzka", "prať s podobne jemnými kusmi", "švy a gombíky"),
            ("ľanový obrus", "kontrola škvŕn pred žehlením", "teplo fixuje zvyšky"),
            ("ľan-bavlna", "nepreplniť bubon", "rovnomerný oplach"),
        ],
        "sections": [
            ("Ako prať ľanovú košeľu v práčke", "Ľanovú košeľu otočte naruby, zapnite gombíky len tak, aby sa nezachytávali, a perte ju s podobne ľahkými kusmi. Nepatrí k uterákom, rifliam ani zipsom. Použite primerané množstvo pracieho gélu a nepreplňte bubon.", "Najväčší rozdiel robí priestor v práčke a rýchle vybratie po praní. Ak košeľa zostane pokrčená v bubne, záhyby sa zvýraznia."),
            ("Ako zjemniť ľan bez zbytočnej aviváže", "Ľan sa nosením a praním prirodzene zjemňuje. Ak je po praní tvrdý, najprv skontrolujte dávkovanie, oplach a sušenie. Priveľa pracieho produktu alebo presušenie môže vytvoriť tvrdší pocit.", "Aviváž nepoužívajte automaticky ako opravu. Pri letnej košeli je často lepší dobrý oplach a sušenie v prúdení vzduchu."),
            ("Ako sušiť ľanovú košeľu, aby nebola tvrdá", "Košeľu po praní vytraste, vyrovnajte švy, golier a manžety a nechajte ju schnúť voľne. Nenechávajte ju preschnúť do úplne tvrdej dosky, ak ju chcete ľahšie žehliť. Mierne vlhký ľan sa tvaruje lepšie.", "Priame horúce teplo môže zvýrazniť tvrdosť aj záhyby."),
            ("Ako žehliť ľanovú košeľu", "Ľan sa žehlí najlepšie mierne vlhký alebo s naparovaním podľa štítku. Začnite golierom a manžetami, potom prejdite rukávy a telo košele. Netlačte zbytočne cez švy, ak nechcete lesklé hrany.", "Ak bola na košeli škvrna, žehlite až po kontrole, že je úplne preč."),
            ("Ľanová košeľa a škvrny od jedla", "Ľan je savý, preto škvrny riešte rýchlo. Pri oleji, víne alebo balzamikovom octe najprv odoberte prebytok a predčistite lokálne. Celú košeľu neperte agresívne len kvôli jednému miestu.", "K mastným škvrnám nadväzuje článok <a href=\"/n/ako-odstranit-olivovy-olej-z-lanovej-kosele-bez-mastnej-mapy\">ako odstrániť olivový olej z ľanovej košele</a>."),
        ],
        "depth": [
            ("Prečo sa ľan krčí", "Ľanové vlákno má prirodzene pevnú štruktúru a látka sa pri nosení aj praní ľahko láme do záhybov. Krčivosť preto nie je chyba, ale vlastnosť. Rozumná starostlivosť ju zmierni, no neurobí z ľanu nekrčivú syntetiku.", "Ak chcete hladší výsledok, pomáha menšia dávka v práčke, vyrovnanie za vlhka a žehlenie v správnom momente."),
            ("Tvrdosť po praní: voda, dávkovanie a presušenie", "Tvrdá ľanová košeľa môže byť výsledkom tvrdej vody, zvyškov pracieho prostriedku, presušenia alebo preplneného bubna. Preto sa oplatí upraviť rutinu, nie hneď meniť celý šatník.", "Ak sa tvrdosť opakuje, vyskúšajte menšiu dávku gélu, lepší oplach a sušenie mimo radiátora."),
        ],
        "expert_title": "Odbornejší pohľad: pevné ľanové vlákno, voda a mechanika",
        "expert_p1": "Ľan je pevné prírodné vlákno s vysokou savosťou a typickou krčivosťou. Pri praní sa správa inak než mäkká viskóza alebo pružná syntetika. Voda ho uvoľní, mechanika vytvorí záhyby a sušenie rozhodne, či bude pôsobiť príjemne alebo tvrdo.",
        "expert_p2": "Pri ľane je dôležité pracovať s vlastnosťou materiálu, nie proti nej. Najlepší výsledok vzniká kombináciou primeraného prania, dobrého oplachu, vyrovnania za vlhka a opatrného žehlenia.",
        "checklist": "Pred praním skontrolujte štítok, farbu, škvrny, gombíky, golier, zmes materiálu, veľkosť dávky a to, či budete mať čas košeľu po praní hneď vybrať a vytvarovať.",
        "rule": "Pri ľane nepreplniť bubon, dávkovať mierne, vybrať hneď po praní, vyrovnať za vlhka a žehliť až po kontrole škvŕn.",
        "recommendation_intro": "Pri ľanovej košeli má prací gél pomôcť čistote, nie zanechať zvyšky vo vlákne. Používajte primerané množstvo a dobrý oplach.",
        "product_text": "Vhodný na šetrné pranie ľanových a bavlnených textílií podľa štítku, najmä keď chcete dobrý oplach bez zbytočného presýtenia látky.",
        "links": [
            ("/n/co-je-lan-preco-sa-krci-ako-ho-prat-a-ako-ho-zjemnit", "Čo je ľan"),
            ("/n/lan-vs-bavlna-rozdiely-v-savosti-krcivosti-a-starostlivosti", "Ľan vs bavlna"),
            ("/n/ako-odstranit-olivovy-olej-z-lanovej-kosele-bez-mastnej-mapy", "Olivový olej z ľanovej košele"),
        ],
        "faq": [
            ("Prečo je ľanová košeľa po praní tvrdá?", "Často za to môže presušenie, priveľa pracieho prostriedku, tvrdá voda alebo preplnený bubon."),
            ("Ako prať ľan, aby sa menej krčil?", "Perte v menšej dávke, košeľu hneď vyberte, vyrovnajte za vlhka a sušte mimo horúceho zdroja."),
            ("Môžem ľanovú košeľu žehliť?", "Áno, podľa štítku. Najlepšie sa žehlí mierne vlhká, ale až po kontrole škvŕn."),
        ],
    },
    "formal_dress": {
        "marker": "Detailnejší postup na spoločenské šaty, podšívku a čistiareň",
        "problem": "spoločenské šaty sú často kombináciou jemnej látky, podšívky, výstuže, tylu, flitrov, zipsu alebo lepených aplikácií, takže ich nemožno posudzovať ako bežné tričko",
        "scope": "spoločenských šatách, koktejlových šatách, šatách s podšívkou, tylom, korálkami, flitrami, čipkou a jemným zipsom",
        "avoid": "pranie naslepo v práčke, sušičku, vysoké otáčky, silné trenie podšívky, žehlenie cez ozdoby a domáce experimenty pri drahých šatách",
        "diagnosis": [
            "<strong>Najprv konštrukcia:</strong> vrchná látka, podšívka a ozdoby môžu mať rozdielne limity.",
            "<strong>Čistiareň je často správna:</strong> najmä pri hodnote, čipke, flitroch alebo nejasnom štítku.",
            "<strong>Lokálne miesto riešte lokálne:</strong> pri malej škvrne nemusí byť bezpečné prať celé šaty.",
            "<strong>Sušenie nesmie zdeformovať tvar:</strong> mokré šaty môžu vlastnou váhou vytiahnuť ramienka alebo podšívku.",
        ],
        "state_rows": [
            ("pach po oslave", "vetrať a skontrolovať podpazušie", "nie hneď silné pranie"),
            ("škvrna pri leme", "lokálne podľa materiálu", "prach a špina"),
            ("flitre alebo korálky", "čistiareň alebo veľmi jemne", "mechanické riziko"),
            ("podšívka", "pozor na zrazenie", "môže reagovať inak"),
        ],
        "textile_rows": [
            ("šaty s tylom", "minimum trenia", "sieťka sa trhá"),
            ("šaty s flitrami", "chrániť ozdoby", "zachytenie a teplo"),
            ("jednoduché polyesterové šaty", "možno jemne podľa štítku", "stále kontrola zipsu"),
            ("čipkované šaty", "skôr ručne alebo čistiareň", "jemný povrch"),
        ],
        "sections": [
            ("Ako rozhodnúť, či prať spoločenské šaty doma", "Začnite štítkom, ale pozrite sa aj na konštrukciu. Má šaty podšívku, výstuž, tyl, korálky, flitre, čipku alebo lepené aplikácie? Ak áno, samotný materiál vrchnej látky nestačí na rozhodnutie.", "Ak sú šaty drahé, požičané, svadobné alebo majú citlivú výzdobu, čistiareň je rozumnejšia než domáci test."),
            ("Lokálne čistenie po oslave", "Po oslave skontrolujte podpazušie, lem, prednú časť pri stole a miesto pri zipsovom zapínaní. Malé lokálne znečistenie riešte jemne a bez premočenia celej konštrukcie. Používajte bielu handričku a testujte na skrytom mieste.", "Pri make-upe a parfume postupujte opatrne, aby ste nevytvorili väčšiu mapu."),
            ("Ako prať jednoduché spoločenské šaty", "Ak štítok povoľuje domáce pranie a šaty nemajú rizikové aplikácie, perte ich naruby, v ochrannom vrecku alebo ručne. Použite nízku mechaniku, primerané množstvo pracieho gélu a dôkladný oplach.", "Bubon nesmie byť plný. Šaty potrebujú priestor, aby sa nezamotali a nepokrčili do ostrých záhybov."),
            ("Tyl, čipka, flitre a korálky", "Ozdobené šaty sú rizikové najmä mechanicky. Tyl sa zachytí, čipka sa vytiahne, flitre sa môžu zdeformovať a korálky uvoľniť. Takýto kus neperte s bežnou dávkou bielizne.", "Súvisiace postupy nájdete v článkoch <a href=\"/n/ako-prat-tylovu-suknu-zavoj-a-jemny-tyl-bez-potrhania\">ako prať tyl</a> a <a href=\"/n/ako-prat-oblecenie-s-flitrami-koralkami-a-aplikaciami\">ako prať flitre a korálky</a>."),
            ("Sušenie spoločenských šiat", "Mokré šaty môžu byť ťažké. Ak ich zavesíte za tenké ramienka, môžu sa vytiahnuť. Sušte podľa materiálu: niekedy na širokom vešiaku, inokedy rozložené. Sušička je pri spoločenských šatách väčšinou zbytočné riziko.", "Žehlenie alebo naparovanie robte až po kontrole škvŕn a podľa štítku."),
        ],
        "depth": [
            ("Prečo podšívka mení pravidlá", "Podšívka môže byť z iného materiálu než vrchná látka. Ak sa zrazí alebo zdeformuje inak, šaty budú ťahať, krútiť sa alebo robiť vlny. Preto je pri šatách dôležité posudzovať celý výrobok, nie len vrchný materiál.", "Pri nejasnom zložení je čistiareň bezpečnejšia najmä pri tmavých, lesklých alebo vrstvených šatách."),
            ("Pach po oslave bez prevoňania problému", "Šaty po oslave môžu cítiť parfum, pot, jedlo alebo dym. Vôňa do prania má zmysel až vtedy, keď je textil čistý a suchý. Ak pach vychádza z podpazušia alebo podšívky, treba riešiť príčinu, nie iba prekrytie.", "Pomáha vetranie, lokálne ošetrenie a pri citlivých šatách profesionálne čistenie."),
        ],
        "expert_title": "Odbornejší pohľad: vrstvený odev a rozdielne reakcie materiálov",
        "expert_p1": "Spoločenské šaty sú často vrstvený odev. Vrchná látka, podšívka, tyl, výstuž, zips a ozdoby môžu reagovať na vodu odlišne. Práve rozdielna reakcia vrstiev spôsobuje zvlnenie, skrútenie alebo zmenu tvaru.",
        "expert_p2": "Pri takomto odeve je bezpečná stratégia konzervatívna: najprv zistiť limity, potom lokálne riešiť konkrétny problém a celé pranie voliť len pri jasne prateľných šatách.",
        "checklist": "Pred praním skontrolujte štítok, podšívku, výstuž, ozdoby, zips, čipku, farbu, škvrny, hodnotu šiat a to, či ide o lokálny problém alebo celkové znečistenie.",
        "rule": "Pri spoločenských šatách chráňte konštrukciu: najprv štítok a lokálne čistenie, pri ozdobách alebo hodnote radšej čistiareň.",
        "recommendation_intro": "Prací gél používajte iba pri šatách, ktoré štítok povoľuje prať vo vode. Pri ozdobách, čipke, tyle alebo podšívke je často bezpečnejší profesionálny postup.",
        "product_text": "Vhodný na šetrné pranie jednoduchších prateľných šiat podľa štítku. Pri spoločenských šatách s aplikáciami zvážte čistiareň.",
        "links": [
            ("/n/ako-prat-oblecenie-s-flitrami-koralkami-a-aplikaciami", "Ako prať flitre, korálky a aplikácie"),
            ("/n/ako-prat-tylovu-suknu-zavoj-a-jemny-tyl-bez-potrhania", "Ako prať tylovú sukňu a závoj"),
            ("/n/ako-prat-sako-doma-a-kedy-ho-radsej-dat-do-cistiarne", "Ako prať sako doma"),
        ],
        "faq": [
            ("Môžem prať spoločenské šaty v práčke?", "Len ak to povoľuje štítok a šaty nemajú citlivé ozdoby, výstuž alebo problematickú podšívku."),
            ("Kedy zvoliť čistiareň?", "Pri drahých šatách, čipke, flitroch, tyle, neznámej škvrne alebo zákaze vodného prania."),
            ("Ako odstrániť pach zo spoločenských šiat?", "Najprv vetrať a identifikovať zdroj. Pri podpazuší alebo podšívke pomôže lokálne ošetrenie alebo čistiareň, nie len vôňa."),
        ],
    },
    "crayons": {
        "marker": "Detailnejší postup na voskovky z peračníka a textilného obalu",
        "problem": "voskovka je kombinácia farebného pigmentu a voskovej zložky, preto sa pri nesprávnom teple môže rozmazať alebo preniesť hlbšie do textilu",
        "scope": "textilnom peračníku, školskom obale, detskej taške, bavlnenej látke, podšívke, vrecku a detskom oblečení po škole",
        "avoid": "horúcu vodu na začiatku, sušičku pred kontrolou, žehlenie cez škvrnu a pranie peračníka spolu s jemnou bielizňou",
        "diagnosis": [
            "<strong>Najprv odstrániť hrubý vosk:</strong> čo zostane na povrchu, môže sa v práčke rozmazať.",
            "<strong>Pigment a vosk sú dve veci:</strong> farba môže zostať aj po odstránení mastného pocitu.",
            "<strong>Podšívka drží zvyšky:</strong> peračník má švy, rohy a vrstvy, kde sa vosk usadí.",
            "<strong>Teplo až po kontrole:</strong> sušička alebo žehlenie môže zvyšok zafixovať.",
        ],
        "state_rows": [
            ("hrubý nános", "opatrne zoškrabnúť tupou hranou", "bez roztierania"),
            ("farebný tieň", "lokálne predčistiť", "pigment"),
            ("mastný pocit", "riešiť voskovú zložku", "nie iba farbu"),
            ("peračník s výstužou", "neprelomiť a nepremočiť naslepo", "tvar"),
        ],
        "textile_rows": [
            ("textilný peračník", "čistiť rohy a švy", "zvyšky sa držia v spojoch"),
            ("látkový obal", "lokálne a potom prať podľa štítku", "farba aj vosk"),
            ("detské tričko", "podložiť škvrnu", "pigment sa prenáša"),
            ("školská taška", "skôr lokálne", "výstuž a zipsy"),
        ],
        "sections": [
            ("Ako dostať voskovku z textilného peračníka", "Najprv vyberte všetko z peračníka a odstráňte drobky voskovky. Hrubý nános opatrne zoškrabte tupou hranou, nie nechtom do strán. Cieľ je zobrať vosk z povrchu, nie ho zatlačiť do tkaniny.", "Potom riešte farebný tieň a mastný pocit lokálne. Až následne má zmysel prať celý peračník, ak to materiál dovolí."),
            ("Voskovka v rohoch a pri zipse", "Peračník má švy, zips a rohy, kde sa vosk drží viac než na rovnej látke. Pred praním tieto miesta vyčistite osobitne. Ak zostane hrudka vosku pri zipse, môže sa pri praní rozmazať na podšívku.", "Zips pred praním zatvorte a skontrolujte, či sa vo vnútri nezachytili kúsky farby."),
            ("Ako riešiť farebný tieň po voskovke", "Po odstránení vosku môže zostať pigment. Podložte miesto bielou savou handrou a pracujte jemne z opačnej strany podľa materiálu. Nepoužívajte silné trenie, ktoré farbu roznesie do väčšej plochy.", "Ak je látka farebná alebo potlačená, najprv testujte na nenápadnom mieste."),
            ("Kedy neprať celý peračník", "Ak má peračník kartónovú výstuž, pevný tvar, koženku, lepené časti alebo elektronický štítok, celé pranie môže narobiť viac škody než voskovka. Vtedy čistite lokálne a nechajte dôkladne vyschnúť.", "Pri jednoduchom textilnom obale bez výstuže je pranie podľa štítku bezpečnejšie."),
            ("Voskovky na detskom oblečení", "Na tričku alebo mikine najprv odstráňte povrchový vosk. Potom riešte pigment a mastnú zložku. Oblečenie nedávajte hneď do sušičky, kým si nie ste istí, že škvrna zmizla.", "Pri výtvarných škvrnách pomôže aj návod <a href=\"/n/ako-odstranit-vodove-farby-z-detskej-zastery-a-rukavov-mikiny\">ako odstrániť vodové farby</a>."),
        ],
        "depth": [
            ("Prečo voskovka potrebuje iný postup než fixka", "Fixka je najmä farbivo alebo atrament, voskovka obsahuje aj voskovú zložku. Preto nestačí riešiť len farbu. Ak vosk zostane v látke, môže vytvoriť mastný pocit a pri teple sa znovu zviditeľniť.", "Pri voskovke je poradie krokov dôležité: najprv povrch, potom lokálne predčistenie, potom pranie a až nakoniec teplo."),
            ("Školské obaly a viac vrstiev", "Peračníky a textilné obaly často nie sú jednoduchá látka. Majú podšívku, výstuž, zips, potlač a švy. Vosk sa môže dostať medzi vrstvy alebo do rohov. Preto je kontrola pred praním dôležitejšia než pri hladkom tričku.", "Ak sa voskovka dostala hlboko do výstuže, úplné odstránenie doma nemusí byť realistické bez poškodenia tvaru."),
        ],
        "expert_title": "Odbornejší pohľad: vosk, pigment a riziko fixácie teplom",
        "expert_p1": "Voskovka kombinuje pigment s voskovou alebo mastnou nosnou zložkou. Pri teple sa vosk môže zmäkčiť, rozšíriť a preniesť do väčšej plochy. Preto sa pri tejto škvrne nezačína sušičkou, horúcou vodou ani žehlením.",
        "expert_p2": "Úspech závisí od toho, koľko vosku odstránite ešte pred praním. Prací cyklus potom rieši zvyšok, nie veľkú hrudku vosku ukrytú v rohu peračníka.",
        "checklist": "Pred praním skontrolujte rohy, zips, výstuž, podšívku, množstvo vosku, farbu látky a to, či je obal vôbec prateľný. Hrubé kúsky odstráňte ešte nasucho.",
        "rule": "Pri voskovke najprv mechanicky odobrať povrch, potom riešiť pigment a mastnotu, prať podľa štítku a teplo použiť až po kontrole.",
        "recommendation_intro": "Prací gél používajte až po tom, čo odstránite čo najviac vosku z povrchu. Samotný prací cyklus nemá v bubne rozpúšťať hrudky voskovky.",
        "product_text": "Vhodný na následné šetrné pranie textilných obalov a detského oblečenia podľa štítku po lokálnom predčistení škvrny.",
        "links": [
            ("/n/ako-odstranit-vodove-farby-z-detskej-zastery-a-rukavov-mikiny", "Ako odstrániť vodové farby"),
            ("/n/ako-odstranit-zvyraznovac-z-rukava-mikiny-a-skolskeho-tricka", "Ako odstrániť zvýrazňovač"),
            ("/n/ako-odstranit-plastelinu-z-teplakov-koberca-a-potahu", "Ako odstrániť plastelínu"),
        ],
        "faq": [
            ("Môžem dať peračník s voskovkou hneď do práčky?", "Nie je to ideálne. Najprv odstráňte hrubé kúsky vosku a predčistite miesto, aby sa vosk nerozmazal."),
            ("Prečo zostal mastný fľak?", "Vosková zložka sa neodstránila úplne. Treba riešiť nielen pigment, ale aj mastný nosič."),
            ("Môžem použiť žehličku?", "Až po veľkej opatrnosti a podľa materiálu. Pri nesprávnom použití môže teplo škvrnu rozšíriť alebo zafixovať."),
        ],
    },
    "zippers_velcro": {
        "marker": "Detailnejší postup na zipsy, suchý zips a ochranu textilu pri praní",
        "problem": "zipsy, suchý zips, háčiky a kovové časti môžu pri praní zatrhnúť jemné vlákna, poškodiť úplet, vytvoriť žmolky alebo zachytiť čipku a funkčné materiály",
        "scope": "mikine so zipsom, bunde, športovej vrstve, detskom oblečení, oblečení so suchým zipsom, podprsenkách, čipke a jemných úpletoch",
        "avoid": "otvorené zipsy, voľný suchý zips, pranie s čipkou a pančuchami, preplnený bubon a vysoké otáčky pri jemných textíliách",
        "diagnosis": [
            "<strong>Zips pred praním zapnúť:</strong> voľné zúbky a jazdec menej narážajú do ostatných vecí.",
            "<strong>Suchý zips oddeliť:</strong> zachytáva vlákna, chlpy a jemné textílie veľmi rýchlo.",
            "<strong>Jemné veci patria do vrecka:</strong> čipka, pančuchy a spodná bielizeň potrebujú ochranu.",
            "<strong>Triedenie je prevencia:</strong> drsné zapínanie nepatrí k svetrom, tylu ani funkčnej spodnej vrstve.",
        ],
        "state_rows": [
            ("mikina so zipsom", "zapnúť a otočiť naruby", "menej nárazov"),
            ("suchý zips", "spojiť protikusy alebo prať oddelene", "chytá vlákna"),
            ("čipka a pančuchy", "ochranné vrecko", "zatrhnutie"),
            ("zips s kovom", "kontrola hrdze a ostrých hrán", "poškodenie textilu"),
        ],
        "textile_rows": [
            ("fleece mikina", "zapnúť zips a prať naruby", "žmolkovanie"),
            ("softshell", "zapnúť zipsy a suché zipsy", "ochrana membrány a povrchu"),
            ("spodná bielizeň", "samostatné vrecko", "háčiky a čipka"),
            ("detské oblečenie", "skontrolovať suchý zips", "zachytáva vlákna"),
        ],
        "sections": [
            ("Ako prať mikinu alebo bundu so zipsom", "Pred praním zapnite hlavný zips aj vrecká, otočte kus naruby a skontrolujte, či jazdec nemá ostrú hranu. Zapnutý zips menej naráža do ostatných textílií a menej poškodzuje bubon aj oblečenie.", "Pri bunde skontrolujte aj šnúrky, kovové koncovky a podšívku. Voľné časti môžu pri praní ťahať švy."),
            ("Ako prať oblečenie so suchým zipsom", "Suchý zips pred praním spojte s protikusom alebo perte kus oddelene. Otvorený suchý zips zachytáva čipku, vlákna, vlasy aj úplety. Po praní sa môže zaplniť vláknami a prestane dobre držať.", "Pri detskom a športovom oblečení skontrolujte suchý zips pred každým praním, nie až po poškodení látky."),
            ("Jemné textílie a ochranné vrecko", "Pančuchy, čipkovaná bielizeň, tyl, jemné tričká a ľahké úplety perte vo vrecku, ak sú v rovnakej várke s akýmkoľvek zapínaním. Ešte lepšie je oddeliť ich úplne od zipsov a suchého zipsu.", "Ochranné vrecko znižuje riziko, ale nenahrádza triedenie."),
            ("Zipsy, žmolky a dierky po praní", "Ak sa na oblečení objavujú nové žmolky alebo malé zatrhnutia, skontrolujte, čo bolo v bubne spolu s ním. Zips, suchý zips, háčik alebo kovový prvok môže byť príčina, aj keď samotný prací program bol správny.", "K dierkam nadväzuje článok <a href=\"/n/ako-predist-dierkam-v-trickach-po-prani-a-suseni\">ako predísť dierkam v tričkách</a>."),
            ("Ako čistiť suchý zips, ktorý už nedrží", "Ak je suchý zips plný vlákien, odstráňte ich jemne pinzetou alebo kefkou. Neťahajte okolité vlákna z textilu. Ak je suchý zips zanesený pracím prostriedkom alebo špinou, vyčistite ho lokálne a nechajte dobre vyschnúť.", "Prevencia je jednoduchšia: pred praním suchý zips vždy zatvoriť."),
        ],
        "depth": [
            ("Prečo zips poškodí látku aj pri šetrnom programe", "Šetrný program znižuje mechaniku, ale neodstráni ostré hrany v bubne. Ak sa jemná látka zachytí o jazdec alebo suchý zips, poškodenie vznikne aj pri nízkej teplote. Preto je príprava pred praním kľúčová.", "Najviac trpia čipky, pančuchy, tyl, jemné úplety, fleece a funkčné hladké materiály."),
            ("Triedenie podľa rizika, nielen podľa farby", "Domáce pranie sa často triedi podľa farby, no pri zipsovom oblečení je rovnako dôležité triediť podľa mechanického rizika. Hrubá mikina so zipsom, uteráky a jemná blúzka môžu mať podobnú farbu, ale nepatria do rovnakej dávky.", "Triedenie podľa rizika znižuje žmolky, zatrhnutia aj dierky."),
        ],
        "expert_title": "Odbornejší pohľad: mechanické poškodenie nevyrieši prací prostriedok",
        "expert_p1": "Zipsy a suchý zips spôsobujú najmä mechanické poškodenie. Prací gél môže vyčistiť textil, ale neochráni jemnú látku pred zachytením o ostrý jazdec alebo háčiky suchého zipsu. Preto je prevencia pred vložením do bubna zásadná.",
        "expert_p2": "Pri praní sledujte nielen chemickú stránku, ale aj pohyb v bubne. Textílie sa o seba trú, narážajú a zachytávajú. Čím plnší bubon a čím viac tvrdých prvkov, tým vyššie riziko poškodenia.",
        "checklist": "Pred praním zapnite zipsy, spojte suchý zips, skontrolujte háčiky, otočte rizikové kusy naruby, jemné veci vložte do vrecka a oddeľte čipku, pančuchy, tyl a jemné úplety.",
        "rule": "Pri zipsoch a suchom zipse je základ prevencia: zapnúť, otočiť naruby, oddeliť jemné veci a nepreplniť bubon.",
        "recommendation_intro": "Prací gél pomôže čistote, ale pred zatrhnutím chráni hlavne príprava dávky. Pri týchto kusoch kombinujte primerané dávkovanie s mechanickou prevenciou.",
        "product_text": "Vhodný na bežné pranie oblečenia so zipsami a športových vrstiev podľa štítku, keď je dávka správne pripravená a roztriedená.",
        "links": [
            ("/n/ako-predist-dierkam-v-trickach-po-prani-a-suseni", "Ako predísť dierkam v tričkách"),
            ("/n/ako-prat-oblecenie-s-flitrami-koralkami-a-aplikaciami", "Ako prať oblečenie s aplikáciami"),
            ("/n/ako-prat-softshell-bundu-a-nohavice-bez-poskodenia-membrany", "Ako prať softshell bundu"),
        ],
        "faq": [
            ("Mám pred praním zapínať zipsy?", "Áno. Zapnutý zips menej poškodzuje ostatné veci a menej naráža v bubne."),
            ("Ako prať suchý zips?", "Spojte ho s protikusom alebo perte kus oddelene. Otvorený suchý zips zachytáva vlákna a jemné textílie."),
            ("Pomôže ochranné vrecko?", "Áno pri jemných veciach, ale stále je lepšie oddeliť ich od zipsov, háčikov a suchého zipsu."),
        ],
    },
}


def build_expansion(topic):
    config = TOPICS[topic]
    sections = "\n".join(f"<h2>{title}</h2>\n<p>{first}</p>\n<p>{second}</p>" for title, first, second in config["sections"])
    depth = "\n".join(f"<h2>{title}</h2>\n<p>{first}</p>\n<p>{second}</p>" for title, first, second in config["depth"])
    faq = "\n".join(f"<h3>{question}</h3>\n<p>{answer}</p>" for question, answer in config["faq"])
    return clean(
        f"""
        <h2>{config["marker"]}</h2>
        <p>{config["problem"].capitalize()}. Preto sa oplatí najprv rozlíšiť materiál, konštrukciu, typ znečistenia a to, či je bezpečné prať celý kus alebo iba lokálne miesto.</p>
        <p>Pri textile ako {config["scope"]} rozhoduje štítok, mechanická záťaž, množstvo vody, teplota a spôsob sušenia. Najväčšie riziko je {config["avoid"]}.</p>
        {note_card("Rýchla praktická diagnostika", config["diagnosis"])}
        <h2>Postup podľa stavu textilu alebo škvrny</h2>
        {table(["Stav", "Čo urobiť", "Poznámka"], config["state_rows"])}
        <h2>Postup podľa typu textilu</h2>
        {table(["Textil", "Postup", "Prečo"], config["textile_rows"])}
        {sections}
        <h2>{config["expert_title"]}</h2>
        <p>{config["expert_p1"]}</p>
        <p>{config["expert_p2"]}</p>
        <p>Pri škvrnách a citlivých odevoch je užitočné postupovať pomaly: najprv odstrániť prebytok alebo rizikový detail, potom zvoliť mierny postup, následne skontrolovať výsledok a až potom sušiť alebo žehliť. Tento princíp chráni textil pred zbytočným poškodením.</p>
        {depth}
        <h2>Kontrolný checklist pred praním</h2>
        <p>{config["checklist"]}</p>
        <p>Do jednej dávky nedávajte textílie s protichodnými potrebami. Outdoorový doplnok s repelentom, ľanová košeľa, spoločenské šaty, peračník s voskovkou a mikina so suchým zipsom potrebujú rozdielny režim. Triedenie je súčasť výsledku, nie detail navyše.</p>
        <h2>Kedy postup zopakovať</h2>
        <p>Ak po prvom praní zostal tieň, pach, lepkavý film, tvrdosť, voskový pocit alebo mechanické zachytenie, nesušte textil horúco. Najprv rozlíšte, či ide ešte o nečistotu alebo už o zmenu materiálu. Opakovaný mierny postup je bezpečnejší než jeden agresívny zásah.</p>
        <p>Pri hodnotných kusoch zastavte domáce experimentovanie skôr. Spoločenské šaty, funkčný outdoorový doplnok, tvarovaný peračník alebo jemná ľanová košeľa sa môžu poškodiť viac samotným zásahom než pôvodným problémom.</p>
        <h2>Ako predísť poškodeniu pri sušení</h2>
        <p>Sušenie často rozhodne o výsledku. Repelentový film sa môže po teple znovu ozvať, ľan stvrdnúť, šaty sa vytiahnuť, voskovka zafixovať a zipsy poškodiť jemné kusy v ďalšej dávke. Sušičku, radiátor a žehličku používajte iba vtedy, keď to štítok povoľuje a keď je textil po praní skontrolovaný.</p>
        <p>Pri škvrnách najprv overte, že miesto je čisté. Pri tvarovaných kusoch najprv obnovte tvar za vlhka. Pri mechanických detailoch pripravte dávku tak, aby sa problém neopakoval pri ďalšom praní.</p>
        <h2>Domáca rutina pre náročnejšie kusy</h2>
        <p>Ak sa podobné problémy opakujú, nastavte si jednoduchú rutinu: kontrola pred košom na bielizeň, oddelenie citlivých kusov, lokálne predčistenie, zapnutie zipsov, primeraná dávka pracieho gélu, nepreplnený bubon a kontrola pred sušením.</p>
        <p>Pri repelente sledujte zvyškový film, pri ľane tvrdosť a krčenie, pri šatách konštrukciu, pri voskovke vosk aj pigment a pri zipsoch mechanické poškodenie. Práve konkrétna príčina rozhoduje o ďalšom praní.</p>
        <div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">
        <h2 style="margin-top: 0;">Rýchla zásada</h2>
        <p>{config["rule"]}</p>
        </div>
        {recommendation_card(config)}
        {build_related_links(config["links"])}
        <h2>FAQ: praktické otázky</h2>
        {faq}
        """
    )


MARKERS = {key: value["marker"] for key, value in TOPICS.items()}
EXPANSIONS = {key: build_expansion(key) for key in TOPICS}


def article_slug(article):
    if article.get("link"):
        return article["link"]
    if article.get("slug"):
        return article["slug"]
    if article.get("url"):
        return article["url"].rstrip("/").split("/")[-1]
    return ""


def load_source(path):
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, list):
        return data, data
    if isinstance(data, dict) and isinstance(data.get("updates"), list):
        return data, data["updates"]
    raise SystemExit(f"Unsupported source format: {path}")


PUBLIC_REPLACEMENTS = [
    (
        re.compile(r"<p>\s*V článku pokrývame aj hľadané výrazy ako\s*<strong>(.*?)</strong>\.\s*(.*?)</p>", re.IGNORECASE | re.DOTALL),
        r"<p>V texte nájdete aj praktické situácie, ktoré sa pri tejto téme často riešia: <strong>\1</strong>. \2</p>",
    ),
    (
        re.compile(r"<p>\s*Pokryté výrazy:\s*(.*?)\.\s*</p>", re.IGNORECASE | re.DOTALL),
        r"<p>V texte nájdete aj praktické situácie, ktoré sa pri tejto téme často riešia: \1.</p>",
    ),
    (
        re.compile(r"<p>\s*Článok cieli výrazy ako\s+(.*?)\.\s*</p>", re.IGNORECASE | re.DOTALL),
        r"<p>V texte nájdete aj praktické situácie, ktoré ľudia pri tejto téme často riešia: \1.</p>",
    ),
]


def public_cleanup(long):
    cleaned = long
    for pattern, replacement in PUBLIC_REPLACEMENTS:
        cleaned = pattern.sub(replacement, cleaned)
    return cleaned


def insertion_index(long):
    candidates = [
        long.find('<div style="border: 1px solid #dbe5de'),
        long.find("\n<h2>Súvisiace"),
        long.find("\n<h2>FAQ"),
    ]
    candidates = [index for index in candidates if index != -1]
    if not candidates:
        raise ValueError("Could not find safe insertion point")
    return min(candidates)


def insert_expansion(long, key):
    long = public_cleanup(long)
    if MARKERS[key] in long:
        start = long.find(f"<h2>{MARKERS[key]}</h2>")
        faq_start = long.find("<h2>FAQ: praktick", start)
        search_from = faq_start if faq_start != -1 else start + len(MARKERS[key])
        candidates = [
            long.find('<div style="border: 1px solid #dbe5de', search_from),
            long.find("\n<h2>Súvisiace", search_from),
            long.find("\n<h2>FAQ", search_from + 1),
        ]
        candidates = [index for index in candidates if index != -1]
        if not candidates:
            raise ValueError("Could not find safe replacement end point")
        end = min(candidates)
        return long[:start].rstrip() + "\n" + EXPANSIONS[key] + "\n" + long[end:].lstrip()
    index = insertion_index(long)
    return long[:index].rstrip() + "\n" + EXPANSIONS[key] + "\n" + long[index:].lstrip()


def mcp_url():
    config = CONFIG_PATH.read_text(encoding="utf-8")
    match = re.search(r'(?s)\[mcp_servers\.biznisweb-vevo\]\s*url\s*=\s*"([^"]+)"', config)
    if not match:
        raise SystemExit("biznisweb-vevo MCP URL not found in ~/.codex/config.toml")
    return match.group(1)


def parse_sse_json(text):
    for line in text.splitlines():
        if line.startswith("data: "):
            return json.loads(line[6:])
    raise ValueError(f"No JSON data line in MCP response: {text[:500]}")


def call_update(endpoint, payload, request_id):
    body = {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "tools/call",
        "params": {"name": "biznisweb-update_news_post", "arguments": payload},
    }
    response = requests.post(
        endpoint,
        json=body,
        headers={"Accept": "application/json, text/event-stream"},
        timeout=120,
    )
    response.raise_for_status()
    parsed = parse_sse_json(response.text)
    if "error" in parsed:
        raise RuntimeError(json.dumps(parsed["error"], ensure_ascii=False))
    result = parsed.get("result", {})
    for item in result.get("content", []):
        if item.get("type") != "text":
            continue
        try:
            inner = json.loads(item.get("text", ""))
        except json.JSONDecodeError:
            continue
        if inner.get("error"):
            raise RuntimeError(inner["error"])
    return parsed


def main():
    parser = argparse.ArgumentParser(description="Conservatively expand VEVO retrofit wave 22 repellent/linen/formal/crayons/zippers articles.")
    parser.add_argument("--update-live", action="store_true")
    parser.add_argument("--sleep", type=float, default=1.5)
    args = parser.parse_args()

    loaded = {}
    updates = []

    for config in ARTICLES:
        source = config["source"]
        if source not in loaded:
            loaded[source] = load_source(source)
        data, rows = loaded[source]

        for article in rows:
            if article_slug(article) != config["slug"]:
                continue
            original = {
                "title": article.get("title"),
                "short": article.get("short", ""),
                "slug": article_slug(article),
                "date_posted": article.get("date_posted"),
                "time_posted": article.get("time_posted"),
                "active": article.get("active"),
                "link": article.get("link"),
                "url": article.get("url"),
            }
            original_long = article["long"]
            article["long"] = insert_expansion(article["long"], config["topic"])
            if (
                article.get("title") != original["title"]
                or article_slug(article) != original["slug"]
                or article.get("short", "") != original["short"]
                or article.get("date_posted") != original["date_posted"]
                or article.get("time_posted") != original["time_posted"]
                or article.get("active") != original["active"]
                or article.get("link") != original["link"]
            ):
                raise SystemExit(f"Retrofit attempted to change protected metadata for {config['slug']}")
            if original["url"] and article.get("url") != original["url"]:
                raise SystemExit(f"Retrofit attempted to change URL for {config['slug']}")
            updates.append(
                {
                    "post_id": config["post_id"],
                    "slug": config["slug"],
                    "url": config["url"],
                    "title": article["title"],
                    "short": article["short"],
                    "long": article["long"],
                    "source_file": str(source.relative_to(ROOT)),
                    "original_length": len(original_long),
                    "new_length": len(article["long"]),
                    "title_preserved": True,
                    "slug_preserved": True,
                    "url_preserved": True,
                    "short_preserved": True,
                    "date_preserved": True,
                    "visibility_preserved": True,
                }
            )
            break
        else:
            raise SystemExit(f"Article not found: {config['slug']}")

    for source, (data, _) in loaded.items():
        source.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    OUT_JSON.write_text(
        json.dumps(
            {
                "project": "VEVO_CONTENT",
                "wave": "retrofit-wave-22-repellent-linen-formal-crayons-zippers-five",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "purpose": "Conservative additive expansion. Titles, slugs, URLs, dates, visibility, and short descriptions are preserved.",
                "updates": updates,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    mcp_updates = []
    if args.update_live:
        endpoint = mcp_url()
        for index, item in enumerate(updates, start=1):
            result = call_update(
                endpoint,
                {
                    "post_id": item["post_id"],
                    "title": item["title"],
                    "short": item["short"],
                    "long": item["long"],
                    "visible": True,
                },
                index,
            )
            mcp_updates.append({"post_id": item["post_id"], "slug": item["slug"], "url": item["url"], "mcp_result": result.get("result", result)})
            time.sleep(args.sleep)

    MCP_RESULTS.write_text(
        json.dumps(
            {
                "project": "VEVO_CONTENT",
                "wave": "retrofit-wave-22-repellent-linen-formal-crayons-zippers-five",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "live_updated": args.update_live,
                "updated_count": len(mcp_updates),
                "updates": mcp_updates,
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
                "source_updates": len(updates),
                "live_updated": args.update_live,
                "mcp_updates": len(mcp_updates),
                "out": str(OUT_JSON),
                "mcp_results": str(MCP_RESULTS),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
