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
OUT_JSON = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-28-ski-socks-thermal-rain-mat-five-2026-06-17.json"
MCP_RESULTS = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-28-ski-socks-thermal-rain-mat-five-2026-06-17-mcp-results.json"


ARTICLES = [
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-19-2026-06-10-articles.json",
        "slug": "ako-odstranit-sol-a-mokry-sneh-z-lyziarskych-rukavic-s-membranou",
        "post_id": "2207",
        "url": "https://www.vevo.sk/n/ako-odstranit-sol-a-mokry-sneh-z-lyziarskych-rukavic-s-membranou",
        "topic": "ski_gloves",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-15-2026-06-09-articles.json",
        "slug": "ako-prat-biele-ponozky-aby-nezosedli-a-nezostali-tvrde",
        "post_id": "2132",
        "url": "https://www.vevo.sk/n/ako-prat-biele-ponozky-aby-nezosedli-a-nezostali-tvrde",
        "topic": "white_socks",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-19-2026-06-10-articles.json",
        "slug": "ako-prat-kuklu-nakrcnik-a-termo-ciapku-po-lyzovani",
        "post_id": "2208",
        "url": "https://www.vevo.sk/n/ako-prat-kuklu-nakrcnik-a-termo-ciapku-po-lyzovani",
        "topic": "thermal_headwear",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-15-2026-06-09-articles.json",
        "slug": "ako-prat-prsiplast-a-reflexne-nepremokave-nohavice-po-dazdi",
        "post_id": "2127",
        "url": "https://www.vevo.sk/n/ako-prat-prsiplast-a-reflexne-nepremokave-nohavice-po-dazdi",
        "topic": "rainwear",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-19-2026-06-10-articles.json",
        "slug": "ako-vycistit-rohozku-a-textilie-v-predsieni-od-posypovej-soli",
        "post_id": "2206",
        "url": "https://www.vevo.sk/n/ako-vycistit-rohozku-a-textilie-v-predsieni-od-posypovej-soli",
        "topic": "entry_mat",
    },
]


def clean(markup):
    return textwrap.dedent(markup).strip()


def table(headers, rows):
    head = "".join(f'<th style="border: 1px solid #e5e5e5; padding: 10px; text-align: left;">{item}</th>' for item in headers)
    body = "\n".join(
        "<tr>" + "".join(f'<td style="border: 1px solid #e5e5e5; padding: 10px;">{cell}</td>' for cell in row) + "</tr>"
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


def product_category_card(config):
    return clean(
        f"""
        <div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 24px 0; background: #f7fbf8;">
        <h2 style="margin-top: 0;">Odporúčané riešenie z VEVO</h2>
        <p>{config["recommendation_intro"]}</p>
        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 14px; margin: 14px 0;">
        <div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff;">
        <h3 style="margin-top: 0;">Prací gél hypoalergénny z Marseillského mydla 1L</h3>
        <p>{config["product_text"]}</p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="/p-1551/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l">Pozrieť produkt</a></p>
        </div>
        <div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff;">
        <h3 style="margin-top: 0;">Kategória pracie gély</h3>
        <p>{config["category_text"]}</p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="/c/vevo-home-care/pranie/praci-gel">Pozrieť pracie gély</a></p>
        </div>
        </div>
        </div>
        """
    )


def related_links(links):
    items = "\n".join(f'<li><a href="{href}">{label}</a></li>' for href, label in links)
    items += '\n<li><a href="/n/ako-vybrat-praci-gel-podla-typu-bielizne">Ako vybrať prací gél podľa typu bielizne</a></li>'
    items += '\n<li><a href="/n/kedy-nepouzivat-avivaz-uteraky-sportove-oblecenie-softshell-aj-detska-bielizen">Kedy nepoužívať aviváž</a></li>'
    return f"<h2>Súvisiace návody na VEVO</h2>\n<ul>\n{items}\n</ul>"


TOPICS = {
    "ski_gloves": {
        "marker": "Detailnejší postup na lyžiarske rukavice so soľou a mokrým snehom",
        "problem": "Lyžiarske rukavice s membránou sú kombinácia vrchného materiálu, výplne, podšívky, švov a často aj kožených alebo pogumovaných častí. Soľ a mokrý sneh sa držia najmä na prstoch, dlani a manžete, ale najväčšie riziko je premočenie vnútra a horúce sušenie.",
        "scope": "membránové rukavice, zateplené lyžiarske rukavice, kožené časti, manžety, podšívku, soľné mapy, mokrý sneh, pach po lyžovaní, sušenie pri izbovej teplote a pranie iba podľa štítku",
        "avoid": "radiátor, fén, sušičku bez povolenia výrobcu, aviváž, silné odmasťovače, krútenie rukavíc, dlhé namáčanie a pranie kožených častí bez návodu",
        "diagnosis": [
            "<strong>Soľ riešte povrchovo:</strong> vlhká handrička často stačí viac než celé pranie.",
            "<strong>Vnútri ide hlavne o vlhkosť:</strong> rukavica potrebuje vyschnúť pomaly a úplne.",
            "<strong>Membrána nemá rada zvyšky prostriedkov:</strong> dávkujte opatrne a oplachujte dôkladne.",
            "<strong>Kožené časti majú vlastný režim:</strong> riaďte sa výrobcom, nie univerzálnym návodom.",
        ],
        "state_rows": [
            ("mokrá manžeta", "odsávať uterákom a sušiť otvorené", "bez radiátora"),
            ("soľná mapa na povrchu", "pretrieť čistou vlhkou handričkou", "nepremáčať"),
            ("zápach zvnútra", "vyvetrať, vysušiť, prať iba podľa štítku", "výplň schne dlho"),
            ("kožená dlaň", "ošetriť podľa výrobcu", "iné ako textil"),
        ],
        "textile_rows": [
            ("rukavice s membránou", "jemné lokálne čistenie", "chráni priedušnosť"),
            ("zateplená výplň", "pomalé sušenie", "neuzavrieť vlhkosť"),
            ("kožené prvky", "bez bežného prania naslepo", "riziko stvrdnutia"),
            ("manžeta", "oplach soli a úplné sušenie", "častý zdroj máp"),
        ],
        "sections": [
            ("Ako odstrániť soľ z lyžiarskych rukavíc", "Po návrate z lyžovania najprv vytraste sneh a nechajte rukavice povoliť pri izbovej teplote. Soľné mapy pretrite čistou vlhkou handričkou, nie agresívnou kefou. Cieľom je rozpustiť a odobrať soľ z povrchu, nie premočiť celú výplň.", "Ak sa mapa drží na manžete, pracujte po menších úsekoch a vlhkosť priebežne odsávajte suchým uterákom."),
            ("Mokré lyžiarske rukavice: ako ich sušiť", "Rukavice sušte otvorené, voľne a mimo priameho tepla. Horúci radiátor môže poškodiť lepené vrstvy, zmeniť tvar alebo stvrdnúť kožené časti. Ak sú mokré zvnútra, krátko vložte suchý uterák a potom ho vyberte.", "Nenechávajte ich zatvorené v taške. Vlhká výplň a pot sú rýchla cesta k zápachu."),
            ("Kedy rukavice prať", "Celé pranie zvoľte iba vtedy, keď to povoľuje štítok alebo výrobca. Ak problémom je len soľ na povrchu, lokálne čistenie je bezpečnejšie. Pri praní použite malé množstvo gélu, nízku mechaniku a dôkladné opláchnutie.", "Súvisiaci postup pre funkčné vrstvy je <a href=\"/n/ako-prat-softshell-bundu-a-nohavice-bez-poskodenia-membrany\">ako prať softshell bez poškodenia membrány</a>."),
            ("Soľ, membrána a aviváž", "Aviváž pri membránových a funkčných materiáloch nepoužívajte. Môže zanechať povlak, ktorý zhorší priedušnosť alebo pocit sucha. Pri rukaviciach navyše nechcete zvyšky v podšívke, ktorá je pri pokožke.", "Ak rukavice po praní pôsobia ťažko alebo lepkavo, problémom môže byť priveľa prostriedku alebo slabý oplach."),
            ("Ako rozlíšiť špinu, soľ a poškodenie", "Soľná mapa sa často zmenší po vlhkom pretretí. Špina potrebuje jemné čistenie. Poškodenie povrchu alebo membrány sa však praním nevyrieši. Ak sa materiál olupuje, praská alebo prepúšťa vodu, ide skôr o opotrebenie.", "Vtedy má väčší zmysel servis alebo výmena než opakované pranie."),
        ],
        "expert_title": "Odbornejší pohľad: membrána, soľ a zvyšky pracieho prostriedku",
        "expert_p1": "Membránové textílie fungujú vďaka vrstveniu a schopnosti odvádzať vlhkosť. Zvyšky soli, potu aj pracieho prostriedku môžu meniť pocit sucha a spôsob, akým sa rukavice správajú pri ďalšom nosení.",
        "expert_p2": "Oficiálne odporúčania pre GORE-TEX oblečenie zdôrazňujú zapnutie uzáverov, malé množstvo tekutého pracieho prostriedku, šetrný cyklus, dvojité opláchnutie a vynechanie aviváže, bielidiel a odstraňovačov škvŕn. Pri rukaviciach treba tieto princípy ešte viac prispôsobiť štítku konkrétneho výrobcu.",
        "source_html": '<p>Princípy starostlivosti o membránové oblečenie uvádza aj <a rel="noopener" href="https://www.gore-tex.com/blog/wash-your-gore-tex-jacket-regularly" target="_blank">GORE-TEX návod na pranie</a>.</p>',
        "checklist": "Pred praním skontrolujte štítok, membránu, kožené časti, soľné mapy, vnútornú vlhkosť, zápach, manžety, možnosť lokálneho čistenia, odporúčaný spôsob sušenia a zákaz aviváže.",
        "rule": "Pri lyžiarskych rukaviciach najprv odobrať soľ a vlhkosť lokálne, potom sušiť pomaly a celé pranie voliť iba podľa štítku.",
        "recommendation_intro": "Pri prateľných rukaviciach používajte jemný gél len v malom množstve a až vtedy, keď nestačí lokálne čistenie.",
        "product_text": "Vhodný na šetrné pranie prateľných textilných častí po kontrole štítku, bez potreby aviváže a s dôkladným oplachom.",
        "category_text": "Pri membránových a zateplených rukaviciach vyberajte prací gél podľa štítku, oplachu a citlivosti funkčných vrstiev.",
        "links": [
            ("/n/ako-odstranit-vosk-na-lyze-z-lyziarskej-bundy-a-rukavic", "Ako odstrániť vosk na lyže z bundy a rukavíc"),
            ("/n/ako-obnovit-impregnaciu-softshellu-po-prani-a-kedy-ju-neriesit", "Ako obnoviť impregnáciu softshellu"),
            ("/n/ako-prat-softshell-bundu-a-nohavice-bez-poskodenia-membrany", "Ako prať softshell bez poškodenia membrány"),
        ],
        "faq": [
            ("Môžem lyžiarske rukavice sušiť na radiátore?", "Radšej nie. Priame teplo môže poškodiť membránu, kožu, lepené švy alebo výplň."),
            ("Ako odstrániť soľ z rukavíc?", "Väčšinou stačí čistá vlhká handrička, práca po malých plochách a následné pomalé sušenie."),
            ("Kedy ich dať do práčky?", "Iba ak to povoľuje štítok alebo výrobca a lokálne čistenie nestačí."),
        ],
    },
    "white_socks": {
        "marker": "Detailnejší postup na biele ponožky, ktoré šednú alebo tvrdnú",
        "problem": "Biele ponožky šednú najčastejšie preto, že sa perú s tmavými textíliami, v preplnenom bubne alebo bez predprania špinavých podrážok. Tvrdosť po praní zase často súvisí s dávkovaním, oplachom, tvrdou vodou a pomalým sušením.",
        "scope": "biele športové ponožky, bavlnené ponožky, ponožky s elastanom, špinavé chodidlá, sivý povlak, tvrdosť po praní, zápach z obuvi, predpranie, triedenie bielej bielizne a sušenie",
        "avoid": "miešanie s tmavou bielizňou, preplnený bubon, priveľa gélu, agresívne bielenie elastanu, odkladanie vlhkých ponožiek a sušenie v hrubej kope",
        "diagnosis": [
            "<strong>Šednutie je často prenos špiny:</strong> bubon musí mať priestor, aby sa špina odplavila.",
            "<strong>Tvrdosť býva oplach:</strong> priveľa gélu alebo tvrdá voda zanechajú pocit drsnosti.",
            "<strong>Podrážky riešte pred praním:</strong> špina z topánok sa inak presúva do celej dávky.",
            "<strong>Elastan nemá rád agresivitu:</strong> bielenie a teplo používajte opatrne.",
        ],
        "state_rows": [
            ("sivé ponožky", "prať oddelene so svetlou bielizňou", "menej prenosu farby"),
            ("špinavé podrážky", "predprať alebo krátko namočiť", "pred hlavným praním"),
            ("tvrdé po praní", "znížiť dávku gélu a zlepšiť oplach", "častý zvyšok"),
            ("zápach", "neskladovať vlhké a prať po nosení", "najmä šport"),
        ],
        "textile_rows": [
            ("bavlnené biele ponožky", "predpranie + bežné pranie so svetlou dávkou", "dobrá savosť"),
            ("ponožky s elastanom", "bez agresívneho bielenia", "chráni pružnosť"),
            ("športové ponožky", "rýchle sušenie a oddelenie od uterákov", "pot a obuv"),
            ("detské ponožky", "predpranie podrážok", "piesok a špina"),
        ],
        "sections": [
            ("Ako prať biele ponožky, aby nezošedli", "Najväčší rozdiel robí triedenie. Biele ponožky neperte s tmavým športovým oblečením, rifľami ani farebnými uterákmi. Ak sú podrážky výrazne špinavé, preperte ich pred hlavným praním.", "Pri silne špinavých ponožkách je lepšie menšie množstvo bielizne v bubne, aby voda a prací roztok mali priestor špinu odplaviť."),
            ("Prečo sú ponožky po praní tvrdé", "Tvrdosť často nevzniká tým, že je pracieho gélu málo, ale tým, že ho je priveľa alebo sa zle vypláchol. Pri tvrdej vode sa k tomu pridávajú minerály. Výsledkom je drsnejší dotyk a sivší vzhľad.", "Pomôže primerané dávkovanie, nepreplnený bubon a pri citlivých dávkach aj extra oplach."),
            ("Ako predprať špinavé podrážky", "Ponožky otočte naruby alebo aspoň skontrolujte chodidlovú časť. Najviac špiny býva na päte, špičke a spodnej ploche. Krátke ručné predpranie často urobí viac než silnejší program.", "Nepotrebujete tvrdé drhnutie. Stačí voda, malé množstvo gélu a prepracovanie najšpinavších miest."),
            ("Bielenie bielych ponožiek bez zničenia elastanu", "Pri ponožkách s elastanom buďte opatrní s vysokou teplotou a silným bielením. Ak chcete zosvetliť sivý povlak, postupujte radšej opakovane miernejšie než jedným tvrdým zásahom.", "Ak je ponožka už mechanicky opotrebovaná, bielenie jej nevráti pôvodnú pružnosť ani hrúbku."),
            ("Ponožky a zápach z topánok", "Ak ponožky zapáchajú aj po praní, problém môže byť aj v obuvi. Vlhké ponožky v uzavretých topánkach prenášajú pach späť na ďalšie pranie. Súvisiaci postup je <a href=\"/n/ako-odstranit-zapach-z-ponoziek-a-sportovej-obuvi-po-treningu\">ako odstrániť zápach z ponožiek a športovej obuvi</a>.", "Pred odložením musia byť ponožky úplne suché, najmä hrubšie športové páry."),
        ],
        "expert_title": "Odbornejší pohľad: šednutie, zvyšky a opätovné usádzanie špiny",
        "expert_p1": "Pri praní bielej bielizne nejde len o teplotu. Ak je bubon preplnený, špina sa nemá kam odplaviť a môže sa znovu usádzať na svetlých vláknach. Podobne priveľa prostriedku môže zhoršiť oplach.",
        "expert_p2": "American Cleaning Institute pri základnej starostlivosti o bielizeň zdôrazňuje triedenie podľa farby a štítkov, ošetrenie škvŕn pred praním a používanie primeraného množstva pracieho prostriedku. Pri ponožkách je táto základná disciplína často dôležitejšia než silné bielenie.",
        "source_html": '<p>Praktické základy prania zhrňuje <a rel="noopener" href="https://www.cleaninginstitute.org/cleaning-tips/clothes/laundry-basics" target="_blank">American Cleaning Institute: Laundry Basics</a>.</p>',
        "checklist": "Pred praním skontrolujte farbu dávky, špinavé podrážky, zápach, elastan, tvrdosť vody, dávku gélu, veľkosť bubna, možnosť extra oplachu a spôsob sušenia.",
        "rule": "Biele ponožky nešednú menej vďaka sile programu, ale vďaka triedeniu, predpraniu podrážok, primeranej dávke gélu a dobrému oplachu.",
        "recommendation_intro": "Pri bielych ponožkách je dôležitý dobre vypláchnuteľný prací gél a rozumné dávkovanie, aby nezostali tvrdé.",
        "product_text": "Vhodný na pravidelné pranie bielych ponožiek a svetlej bielizne, najmä keď nechcete zbytočne zvyšovať dávku pracieho prostriedku.",
        "category_text": "Pri bielych a svetlých dávkach vyberajte prací gél podľa tvrdosti vody, zašpinenia a potreby dôkladného oplachu.",
        "links": [
            ("/n/ako-prat-bielu-bielizen-aby-nezosedla-a-nezapachala", "Ako prať bielu bielizeň"),
            ("/n/ako-odstranit-zapach-z-ponoziek-a-sportovej-obuvi-po-treningu", "Ako odstrániť zápach z ponožiek a športovej obuvi"),
            ("/n/preco-je-bielizen-po-prani-tvrda-alebo-lepkava-zvysky-gelu-davkovanie-a-oplach", "Prečo je bielizeň po praní tvrdá alebo lepkavá"),
        ],
        "faq": [
            ("Prečo biele ponožky šednú?", "Najčastejšie pre miešanie s tmavým oblečením, preplnený bubon alebo špinu na podrážkach, ktorá sa pred praním nerieši."),
            ("Prečo sú tvrdé po praní?", "Často pre zvyšky pracieho prostriedku, tvrdú vodu, slabý oplach alebo pomalé sušenie."),
            ("Môžem ich bieliť?", "Opatrne, najmä ak obsahujú elastan. Najprv skúste triedenie, predpranie a dobrý oplach."),
        ],
    },
    "thermal_headwear": {
        "marker": "Detailnejší postup na kuklu, nákrčník a termo čiapku po lyžovaní",
        "problem": "Kukla, nákrčník a termo čiapka sú priamo pri tvári, krku, vlasoch a prilbe. Zachytávajú pot, kožný maz, krém, make-up, zvyšky jedla, vlhkosť zo snehu a pach z prilby. Preto sa perú častejšie než bunda, ale jemnejšie než uteráky.",
        "scope": "kuklu pod prilbu, nákrčník, termo čiapku, fleecové okraje, merino zmes, polyester, elastan, make-up, krém, pot, zápach, sušenie mimo radiátora a oddelenie od suchých zipsov",
        "avoid": "aviváž, pranie so suchým zipsom, vysokú teplotu, silné odstreďovanie, zatvorenú tašku po lyžovaní, odloženie vlhkého nákrčníka do skrine a prevoňanie namiesto prania",
        "diagnosis": [
            "<strong>Je to textil pri tvári:</strong> riešte pot, krém aj citlivosť pokožky.",
            "<strong>Aviváž vynechajte:</strong> pri funkčných vláknach môže zhoršiť odvod vlhkosti.",
            "<strong>Suché zipsy sú nepriateľ:</strong> kuklu a nákrčník perte oddelene alebo vo vrecku.",
            "<strong>Sušenie musí byť rýchle, nie horúce:</strong> vlhký záhyb vráti zápach.",
        ],
        "state_rows": [
            ("pot po lyžovaní", "prať naruby na jemnom programe", "kontakt s pokožkou"),
            ("make-up alebo krém", "lokálne predčistiť okraj", "tvár a krk"),
            ("zápach z prilby", "prať častejšie a sušiť otvorene", "neodkladať vlhké"),
            ("merino zmes", "postup podľa štítku", "nižšia mechanika"),
        ],
        "textile_rows": [
            ("polyesterová kukla", "jemný program, bez aviváže", "odvod vlhkosti"),
            ("merino nákrčník", "vlna program alebo ručne", "riziko zrazenia"),
            ("fleece čiapka", "naruby a oddeliť od zipsov", "žmolky"),
            ("elastická kukla", "nízke otáčky", "chráni tvar"),
        ],
        "sections": [
            ("Ako prať kuklu po lyžovaní", "Kuklu otočte naruby, aby sa prali miesta pri čele, nose, ústach a krku. Ak je na okraji krém alebo make-up, pred praním ho jemne prepracujte kvapkou pracieho gélu.", "Perte s podobnými funkčnými textíliami, nie s uterákmi, rifľami alebo vecami so suchým zipsom."),
            ("Ako prať nákrčník bez zápachu", "Nákrčník často vlhne od dychu a potu. Ak ho po lyžovaní necháte v batohu, zápach sa pri ďalšom nosení rýchlo vráti. Po príchode ho vyberte, presušte a podľa potreby vyperte.", "Pri merino alebo vlnených zmesiach použite postup podľa štítku a nízku mechaniku."),
            ("Termo čiapka a prilba", "Čiapka pod prilbou zachytáva pot aj pach z výstelky prilby. Neperte ju automaticky na vysokú teplotu, najmä ak obsahuje elastan, fleece alebo funkčné vlákna.", "Ak čiapka zapácha hneď po nasadení, problém môže byť aj v prilbe alebo v tom, že textil nebol úplne suchý pred odložením."),
            ("Make-up, krém a opaľovací prípravok", "Okraje kukly a nákrčníka pri tvári môžu mať mastnejší film z krému. Ten riešte lokálne pred praním, inak sa v nízkej teplote nemusí dobre uvoľniť.", "Pri svetlých kusoch kontrolujte okraje pred sušením. Teplo môže mastný tieň zafixovať."),
            ("Prečo nepoužívať aviváž", "Aviváž môže zanechať povlak, ktorý pri funkčných textíliách znižuje schopnosť odvádzať vlhkosť. Pri textile pri tvári môže byť problémom aj silná parfumácia.", "Ak chcete vôňu, dávkujte ju mierne a až na textil, ktorý je naozaj čistý a dobre vypláchnutý."),
        ],
        "expert_title": "Odbornejší pohľad: pot, kožný maz a funkčné vlákna pri tvári",
        "expert_p1": "Textílie pri tvári a krku sa špinia inak než bunda. Kombinujú pot, maz, kozmetiku a vlhkosť z dychu. Preto sa oplatí prať ich častejšie, ale s menšou mechanickou záťažou.",
        "expert_p2": "Pri funkčných vláknach je cieľom odstrániť zvyšky z pokožky a zachovať schopnosť odvádzať vlhkosť. To znamená jemný cyklus, primerané dávkovanie, dôkladný oplach a žiadnu aviváž.",
        "source_html": '<p>Vlhké textílie treba sušiť dôkladne. CDC pri plesniach vo vnútornom prostredí zdôrazňuje odstránenie vlhkosti ako základ prevencie problémov s plesňou; pri športových textíliách je preto dôležité neodkladať ich vlhké. Viac: <a rel="noopener" href="https://www.cdc.gov/mold-health/about/index.html" target="_blank">CDC Mold</a>.</p>',
        "checklist": "Pred praním skontrolujte materiál, merino alebo elastan, make-up, krém, pach, kontakt s prilbou, suché zipsy v dávke, dávku gélu, oplach a spôsob sušenia.",
        "rule": "Kuklu, nákrčník a termo čiapku perte častejšie než bundu, ale jemne: naruby, bez aviváže a s úplným vysušením.",
        "recommendation_intro": "Pri textíliách pri tvári je dôležitý šetrný prací gél, dobrý oplach a mierne dávkovanie.",
        "product_text": "Vhodný na pranie kukiel, nákrčníkov a termo čiapok podľa štítku, keď potrebujete odstrániť pot, krém a zápach bez aviváže.",
        "category_text": "Pri funkčnom prádle pri pokožke vyberajte prací gél podľa materiálu, citlivosti pokožky a potreby dobrého oplachu.",
        "links": [
            ("/n/ako-prat-termo-oblecenie-po-zime", "Ako prať termo oblečenie po zime"),
            ("/n/ako-prat-termo-bielizen-a-funkcnu-spodnu-vrstvu-bez-zapachu", "Ako prať termo bielizeň bez zápachu"),
            ("/n/ako-odstranit-parfumovy-flak-z-oblecenia-a-jemnych-latok", "Ako odstrániť parfumový fľak z oblečenia"),
        ],
        "faq": [
            ("Ako často prať kuklu po lyžovaní?", "Ak bola pri tvári celý deň alebo je spotená, perte ju po použití. Pri krátkom nosení ju aspoň vysušte a vyvetrajte."),
            ("Môžem použiť aviváž?", "Pri funkčných vláknach radšej nie. Môže zhoršiť odvod vlhkosti a zvýšiť ťažký pocit pri tvári."),
            ("Ako odstrániť make-up z okraja kukly?", "Pred praním okraj jemne prepracujte malým množstvom gélu a skontrolujte pred sušením."),
        ],
    },
    "rainwear": {
        "marker": "Detailnejší postup na pršiplášť a reflexné nepremokavé nohavice",
        "problem": "Pršiplášť a reflexné nepremokavé nohavice po daždi často nepotrebujú celé pranie. Potrebujú najprv vyschnúť, odvetrať, utrieť blato a až potom rozhodnúť, či je pranie vôbec potrebné. Pri reflexných prvkoch a vodoodpudivej úprave je zbytočná mechanika riziko.",
        "scope": "pršiplášť, nepremokavé nohavice, reflexné pásy, lepené švy, kapucňu, zipsy, manžety, blato, zatuchnutie, pot, vodoodpudivú úpravu a sušenie po daždi",
        "avoid": "zbalenie mokrého pršiplášťa do tašky, aviváž, vysokú teplotu, drhnutie reflexných prvkov, pranie po každom daždi bez dôvodu a sušenie v pokrčenom stave",
        "diagnosis": [
            "<strong>Najprv vysušiť a vyvetrať:</strong> mokrý pršiplášť v taške rýchlo zatuchne.",
            "<strong>Blato utrieť lokálne:</strong> celé pranie nie je vždy potrebné.",
            "<strong>Reflexné prvky nešúchať:</strong> povrch sa môže poškodiť.",
            "<strong>Aviváž vynechať:</strong> pri nepremokavých materiáloch je zbytočné riziko.",
        ],
        "state_rows": [
            ("čistý dážď", "vysušiť a vyvetrať", "bez prania"),
            ("blato na nohaviciach", "utrieť vlhkou handričkou", "pred praním"),
            ("zatuchnutý pach", "jemné pranie podľa štítku", "po vysušení"),
            ("slabšia vodoodpudivosť", "čistenie a až potom reaktivácia/obnova", "podľa výrobcu"),
        ],
        "textile_rows": [
            ("tenký pršiplášť", "nízka mechanika", "lepené švy"),
            ("reflexné nohavice", "naruby a bez abrazívneho trenia", "chráni pásy"),
            ("membránová vrstva", "tekutý prostriedok a oplach", "bez aviváže"),
            ("kapucňa a zipsy", "zapnúť a vysušiť otvorené", "menej deformácie"),
        ],
        "sections": [
            ("Čo urobiť hneď po daždi", "Pršiplášť zaveste na vešiak a nechajte ho odkvapkať. Neprekladajte ho mokrý do tašky ani do skrine. Blato utrite mäkkou handričkou, kým je ešte ľahko odstrániteľné.", "Ak textil nezapácha a nie je špinavý, často stačí vysušenie a vyvetranie."),
            ("Ako prať pršiplášť", "Pranie zvoľte podľa štítku. Zapnite zipsy, uvoľnite sťahovacie prvky a použite malé množstvo tekutého pracieho prostriedku. Nepoužívajte aviváž ani agresívne odstraňovače škvŕn.", "Po praní skontrolujte, či sa voda stále drží na povrchu alebo sa vpíja do materiálu."),
            ("Reflexné nepremokavé nohavice", "Reflexné pásy nešúchajte hrubou kefou. Otočenie naruby a nízka mechanika pomôžu znížiť odieranie. Najviac špiny býva pri spodnom leme a kolenách, preto tieto miesta očistite pred praním.", "Ak je reflexný povrch popraskaný alebo odretý, pranie ho neopraví."),
            ("Zatuchnutý pršiplášť", "Zatuchnutie vzniká z vlhkosti a uzavretia, nie z nedostatku vône. Najprv textil úplne vysušte, potom vyperte podľa štítku a nechajte opäť dôkladne preschnúť.", "Vlhký záhyb v kapucni alebo rukáve môže zapáchať aj vtedy, keď povrch pôsobí suchý."),
            ("Vodoodpudivá úprava po praní", "Ak sa voda po praní vpíja, môže byť potrebná obnova vodoodpudivej úpravy. Najprv však textil vyčistite. Nanášať impregnáciu na špinavý alebo mastný povrch nedáva dobrý výsledok.", "Súvisiaci postup je <a href=\"/n/ako-obnovit-impregnaciu-softshellu-po-prani-a-kedy-ju-neriesit\">ako obnoviť impregnáciu softshellu po praní</a>."),
        ],
        "expert_title": "Odbornejší pohľad: nepremokavé vrstvy a pranie bez aviváže",
        "expert_p1": "Nepremokavé a vodoodpudivé textílie majú povrchovú úpravu, membránu alebo záter. Cieľom prania je odstrániť pot, blato a zvyšky, ale nezaniesť povrch ďalším povlakom.",
        "expert_p2": "GORE-TEX pri praní technického oblečenia odporúča zapnúť uzávery, použiť malé množstvo tekutého prostriedku, šetrný cyklus a vynechať práškové detergenty, aviváž, bielidlá a odstraňovače škvŕn. Pri pršiplášťoch a reflexných nohaviciach je potrebné vždy rešpektovať aj konkrétny štítok.",
        "source_html": '<p>K starostlivosti o nepremokavé oblečenie pozri aj <a rel="noopener" href="https://www.gore-tex.com/blog/wash-your-gore-tex-jacket-regularly" target="_blank">GORE-TEX: Wash your jacket regularly</a>.</p>',
        "checklist": "Pred praním skontrolujte štítok, zipsy, lepené švy, reflexné prvky, blato na lemoch, pach zatuchnutia, vodoodpudivosť, zákaz aviváže a spôsob sušenia.",
        "rule": "Pršiplášť po daždi najprv vysušiť a očistiť lokálne; prať až vtedy, keď je špina alebo pach naozaj dôvod.",
        "recommendation_intro": "Pri prateľných nepremokavých veciach používajte malé množstvo gélu a dôkladný oplach bez aviváže.",
        "product_text": "Vhodný na šetrné pranie prateľného pršiplášťa alebo reflexných nohavíc podľa štítku, keď lokálne utretie nestačí.",
        "category_text": "Pri nepremokavých materiáloch vyberajte prací gél s ohľadom na štítok, oplach a citlivosť povrchovej úpravy.",
        "links": [
            ("/n/ako-prat-gore-tex", "Ako prať Gore-Tex"),
            ("/n/ako-obnovit-impregnaciu-softshellu-po-prani-a-kedy-ju-neriesit", "Ako obnoviť impregnáciu softshellu"),
            ("/n/ako-prat-oblecenie-po-rybacke-alebo-turistike", "Ako prať oblečenie po rybačke alebo turistike"),
        ],
        "faq": [
            ("Treba prať pršiplášť po každom daždi?", "Nie. Ak nie je špinavý ani zatuchnutý, často stačí vysušenie a vyvetranie."),
            ("Môžem použiť aviváž?", "Pri nepremokavých a funkčných materiáloch radšej nie."),
            ("Ako chrániť reflexné prvky?", "Nedrhnúť ich hrubou kefou, prať naruby a s nízkou mechanikou podľa štítku."),
        ],
    },
    "entry_mat": {
        "marker": "Detailnejší postup na rohožku a predsieňové textílie od posypovej soli",
        "problem": "Rohožka po zime zachytí soľ, piesok, blato, vodu z topánok a pach z predsiene. Ak sa iba prestrieka vôňou, príčina zostáva v textílii alebo podklade. Najprv treba odstrániť suché častice, potom riešiť soľ a až po úplnom vysušení vôňu.",
        "scope": "textilnú rohožku, gumový spodok, predsieňový koberček, behúň, textílie pri dverách, posypovú soľ, piesok, vlhkosť, zápach, podlahu pod rohožkou a sušenie mimo radiátora",
        "avoid": "mokré drhnutie bez vysatia, premočenie gumového podkladu, sušenie na horúcom radiátore, vôňu namiesto čistenia, odloženie vlhkej rohožky späť na podlahu a pranie s bežnou bielizňou",
        "diagnosis": [
            "<strong>Najprv nasucho:</strong> vyklepať, vysať a až potom vlhčiť.",
            "<strong>Soľ sa rozpúšťa vo vode:</strong> pracujte po menších plochách a vlhkosť odsávajte.",
            "<strong>Gumový spodok chráňte pred teplom:</strong> môže sa zdeformovať alebo popraskať.",
            "<strong>Vôňa až po vysušení:</strong> inak prekryje vlhkosť iba krátko.",
        ],
        "state_rows": [
            ("suchá soľ a piesok", "vyklepať a vysať", "pred vodou"),
            ("soľné mapy", "vlhká kefa po malých plochách", "nepremáčať"),
            ("zatuchnutý pach", "vyčistiť podklad a vysušiť", "nielen prevoňať"),
            ("gumový spodok", "sušiť voľne", "nie horúco"),
        ],
        "textile_rows": [
            ("textilná rohožka", "samostatné pranie alebo lokálne čistenie", "podľa štítku"),
            ("gumová rohožka", "oplach a vysušenie", "bez pracieho bubna"),
            ("predsieňový behúň", "vysávanie + lokálne čistenie", "veľká plocha"),
            ("podlaha pod rohožkou", "umyť a vysušiť", "zdroj zápachu"),
        ],
        "sections": [
            ("Ako vyčistiť rohožku od posypovej soli", "Rohožku najprv odneste von, vyklepte a povysávajte. Ak začnete vodou, soľ a piesok sa rozpustia a zatlačia hlbšie do vlákien. Suchá fáza je preto najdôležitejšia.", "Až potom pracujte vlhkou kefou alebo krátkym oplachom podľa typu rohožky."),
            ("Textilná rohožka verzus gumový spodok", "Textilná vrchná časť môže byť prateľná, ale gumový spodok nemusí znášať bubon, teplo ani silné odstreďovanie. Skontrolujte štítok a pri neistote zvoľte lokálne čistenie.", "Ak sa gumový spodok začne drobiť alebo deformovať, pranie ho neopraví."),
            ("Predsieň po zime zapácha", "Zápach v predsieni často vzniká z vlhkej rohožky, topánok a špiny pod rohožkou. Preto vyčistite aj podlahu pod ňou a nechajte všetko vyschnúť, kým rohožku vrátite späť.", "Až potom má zmysel jemná vôňa do priestoru alebo textílií."),
            ("Ako sušiť rohožku", "Rohožku sušte tak, aby vzduch prúdil aj k spodnej strane. Nepoložte ju hneď mokrú späť na podlahu. Vlhký gumový spodok môže držať pach aj vtedy, keď vrch pôsobí suchý.", "Vyhnite sa horúcemu radiátoru, najmä pri gume a lepidlách."),
            ("Kedy rohožku neprať v práčke", "Do práčky nepatrí príliš ťažká, drobiaca sa, gumová alebo neoznačená rohožka. Môže poškodiť bubon, filter alebo sama seba. Pri takejto rohožke je bezpečnejšie ručné čistenie vonku.", "Súvisiaci postup pre špinavšie dávky je <a href=\"/n/ako-vycistit-bubon-pracky-po-prani-pelechu-topanok-alebo-pracovnych-veci\">ako vyčistiť bubon práčky po špinavších veciach</a>."),
        ],
        "expert_title": "Odbornejší pohľad: soľ, vlhkosť a pach v predsieni",
        "expert_p1": "Posypová soľ je hygroskopická a v kombinácii s pieskom drží vlhkosť v textílii. Preto sa pach v predsieni môže vracať aj po povrchovom prevoňaní, ak zostala vlhkosť a špina v rohožke alebo pod ňou.",
        "expert_p2": "CDC pri plesniach zdôrazňuje, že pri problémoch s plesňou a zápachom treba riešiť vlhkosť ako príčinu. Pri rohožke je praktická verzia rovnaká: vyčistiť, vysušiť a až potom prevoňať.",
        "source_html": '<p>K vlhkosti a plesniam v domácnosti pozri <a rel="noopener" href="https://www.cdc.gov/mold-health/about/index.html" target="_blank">CDC Mold</a>.</p>',
        "checklist": "Pred čistením skontrolujte suchú soľ, piesok, štítok rohožky, gumový spodok, podlahu pod rohožkou, zápach, možnosť samostatného prania, hmotnosť mokrej rohožky a spôsob sušenia.",
        "rule": "Rohožku od soli najprv vyklepať a vysať, potom čistiť vlhko po malých plochách a prevoňať až po úplnom vysušení.",
        "recommendation_intro": "Pri prateľných predsieňových textíliách použite prací gél až po vyklepaní soli a piesku.",
        "product_text": "Vhodný na samostatné šetrné pranie prateľných textilných rohožiek alebo behúňov podľa štítku, keď lokálne čistenie nestačí.",
        "category_text": "Pri predsieňových textíliách vyberajte prací gél podľa materiálu, zašpinenia a potreby dobrého oplachu.",
        "links": [
            ("/n/ako-odstranit-solne-mapy-z-nohavic-a-kabata-po-zime", "Ako odstrániť soľné mapy z nohavíc a kabáta"),
            ("/n/ako-odstranit-zapach-z-topanok-a-textilii-v-predsieni", "Ako odstrániť zápach z topánok a textílií v predsieni"),
            ("/n/ako-vycistit-navlek-na-autosedacku-po-zime-a-posypovej-soli", "Ako vyčistiť návlek na autosedačku po zime"),
        ],
        "faq": [
            ("Môžem rohožku prať v práčke?", "Iba ak to povoľuje štítok a rohožka nie je príliš ťažká, gumová alebo poškodená."),
            ("Prečo predsieň zapácha aj po vôni?", "Zostala vlhkosť, soľ alebo špina v rohožke, topánkach alebo podklade pod rohožkou."),
            ("Ako odstrániť soľné mapy?", "Najprv suché zvyšky vysať, potom pracovať vlhko po menších plochách a dobre vysušiť."),
        ],
    },
}


def build_expansion(topic):
    config = TOPICS[topic]
    sections = "\n".join(f"<h2>{title}</h2>\n<p>{first}</p>\n<p>{second}</p>" for title, first, second in config["sections"])
    faq = "\n".join(f"<h3>{question}</h3>\n<p>{answer}</p>" for question, answer in config["faq"])
    return clean(
        f"""
        <h2>{config["marker"]}</h2>
        <p>{config["problem"]}</p>
        <p>Pri tejto téme sa oplatí pozerať na celý kontext: {config["scope"]}. Najväčšie riziko je {config["avoid"]}. Preto najprv rozlíšte, či riešite soľ, blato, pot, vlhkosť, šednutie, zápach alebo poškodenie materiálu.</p>
        {note_card("Rýchla praktická diagnostika", config["diagnosis"])}
        <h2>Postup podľa stavu problému</h2>
        {table(["Stav", "Čo urobiť", "Poznámka"], config["state_rows"])}
        <h2>Postup podľa typu textilu alebo časti</h2>
        {table(["Textil alebo časť", "Postup", "Prečo"], config["textile_rows"])}
        {sections}
        <h2>{config["expert_title"]}</h2>
        <p>{config["expert_p1"]}</p>
        <p>{config["expert_p2"]}</p>
        {config["source_html"]}
        <h2>Kontrolný checklist pred praním</h2>
        <p>{config["checklist"]}</p>
        <p>Ak je problém lokálny, nezačínajte automaticky celým praním. Najprv odstráňte hrubú špinu, soľ, blato alebo povrchový film. Celé pranie má dokončiť pripravené čistenie, nie zachraňovať neodstránený nános.</p>
        <h2>Malý test pred väčším zásahom</h2>
        <p>Pred silnejším čistením si vyberte nenápadné miesto: vnútorný lem, rubovú stranu, spodný okraj alebo menšiu plochu pri šve. Otestujte vodu, prací roztok, kefu alebo trenie tam, kde prípadná zmena nebude viditeľná.</p>
        <p>Ak sa farba púšťa, reflexný prvok matnie, guma sa deformuje, membrána pôsobí lepkavo alebo textil tvrdne, nepokračujte rovnakou silou na viditeľnej ploche. Pri funkčných a zimných veciach je zachovanie materiálu rovnako dôležité ako odstránenie špiny.</p>
        <h2>Kedy textil nesušiť a neodkladať</h2>
        <p>Textil nesušte horúco ani neodkladajte, ak v ňom zostala vlhkosť, soľ, blato, zápach alebo zvyšky pracieho prostriedku. Teplo a uzavretý priestor môžu zvýrazniť pach, zdeformovať podklad alebo zafixovať mapu.</p>
        <p>Pred odložením skontrolujte švy, lemy, manžety, gumový spodok, kapucňu, prsty rukavíc a hrubšie vrstvy. Práve tam sa vlhkosť drží najdlhšie.</p>
        <h2>Domáca rutina pri opakovanom probléme</h2>
        <p>Nastavte si jednoduchú rutinu: suché odstránenie nečistôt, lokálne predčistenie, primeraná dávka gélu, nepreplnený bubon, dôkladný oplach a úplné vysušenie. Pri bielych ponožkách pridajte triedenie, pri membránach zákaz aviváže a pri rohožke kontrolu podkladu.</p>
        <p>Takto sa z prania nestane náhodný pokus. Pri zimných rukaviciach, bielych ponožkách, termo doplnkoch, pršiplášti aj rohožke rozhoduje najmä to, čo urobíte pred zapnutím práčky.</p>
        <h2>Ako si nastaviť pravidlo pre ďalšie použitie</h2>
        <p>Po každom praní si všimnite, čo sa zlepšilo a čo zostalo: či zmizla soľná mapa, či sa vrátil zápach, či textil stvrdol, či sa špina drží v švoch alebo či sa voda začala vpíjať do povrchu. Táto spätná kontrola je praktickejšia než stále meniť celý prací postup.</p>
        <p>Ak sa rovnaký problém opakuje, nehľadajte silnejší prostriedok ako prvú možnosť. Najprv upravte triedenie, množstvo bielizne v bubne, predčistenie, dávku gélu, oplach a sušenie. Pri väčšine domácich textílií práve tieto kroky rozhodujú o výsledku viac než samotná teplota.</p>
        <p>Pri zimnej a nepremokavej výbave si nechajte malú rezervu: radšej dlhšie sušenie, menej mechaniky a kontrola štítku než rýchly zásah, ktorý zmení tvar, pružnosť alebo ochrannú úpravu.</p>
        <h2>Kedy stačí lokálne čistenie</h2>
        <p>Lokálne čistenie stačí vtedy, keď je problém na malej ploche: soľná mapa na manžete, blato na leme, sivá podrážka ponožky, mastný okraj pri tvári alebo špinavý roh rohožky. Vtedy netreba zaťažovať celý kus, ak materiál inak nezapácha a nie je prepotený.</p>
        <p>Celé pranie zvoľte až vtedy, keď je znečistenie plošné, textil bol pri pokožke dlhší čas, cítiť zápach alebo lokálne čistenie zanechalo zvyšky, ktoré treba vypláchnuť.</p>
        <div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">
        <h2 style="margin-top: 0;">Rýchla zásada</h2>
        <p>{config["rule"]}</p>
        </div>
        {product_category_card(config)}
        {related_links(config["links"])}
        <h2>FAQ: praktické otázky</h2>
        {faq}
        """
    )


MARKERS = {key: value["marker"] for key, value in TOPICS.items()}
EXPANSIONS = {key: build_expansion(key) for key in TOPICS}


PUBLIC_REPLACEMENTS = [
    (
        re.compile(r"<p>\s*V článku pokrývame aj hľadané výrazy ako\s*(.*?)\.\s*(.*?)</p>", re.IGNORECASE | re.DOTALL),
        r"<p>V texte nájdete aj praktické situácie, ktoré sa pri tejto téme často riešia: \1. \2</p>",
    ),
    (
        re.compile(r"<p>\s*Tento článok pokrýva výrazy ako\s*(.*?)\.\s*(.*?)</p>", re.IGNORECASE | re.DOTALL),
        r"<p>V texte nájdete aj praktické situácie, ktoré sa pri tejto téme často riešia: \1. \2</p>",
    ),
    (
        re.compile(r"<p>\s*Článok cieli\s*(.*?)\.\s*(.*?)</p>", re.IGNORECASE | re.DOTALL),
        r"<p>V texte nájdete aj praktické situácie, ktoré sa pri tejto téme často riešia: \1. \2</p>",
    ),
    (
        re.compile(r"<p>\s*Článok pokrýva\s*(.*?)\.\s*(.*?)</p>", re.IGNORECASE | re.DOTALL),
        r"<p>V texte nájdete aj praktické situácie, ktoré sa pri tejto téme často riešia: \1. \2</p>",
    ),
]


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
    if isinstance(data, dict) and isinstance(data.get("articles"), list):
        return data, data["articles"]
    raise SystemExit(f"Unsupported source format: {path}")


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


def insert_expansion(long, topic):
    cleaned = public_cleanup(long)
    marker = MARKERS[topic]
    if marker in cleaned:
        if "Kedy stačí lokálne čistenie" not in cleaned:
            addition = clean(
                """
                <h2>Kedy stačí lokálne čistenie</h2>
                <p>Lokálne čistenie stačí vtedy, keď je problém na malej ploche: soľná mapa na manžete, blato na leme, sivá podrážka ponožky, mastný okraj pri tvári alebo špinavý roh rohožky. Vtedy netreba zaťažovať celý kus, ak materiál inak nezapácha a nie je prepotený.</p>
                <p>Celé pranie zvoľte až vtedy, keď je znečistenie plošné, textil bol pri pokožke dlhší čas, cítiť zápach alebo lokálne čistenie zanechalo zvyšky, ktoré treba vypláchnuť.</p>
                """
            )
            quick_rule = '<h2 style="margin-top: 0;">Rýchla zásada</h2>'
            quick_pos = cleaned.find(quick_rule)
            div_pos = cleaned.rfind("<div", 0, quick_pos) if quick_pos != -1 else -1
            if div_pos != -1:
                return cleaned[:div_pos].rstrip() + "\n" + addition + "\n" + cleaned[div_pos:].lstrip()
            idx = insertion_index(cleaned)
            return cleaned[:idx].rstrip() + "\n" + addition + "\n" + cleaned[idx:].lstrip()
        if "Ako si nastaviť pravidlo pre ďalšie použitie" not in cleaned:
            addition = clean(
                """
                <h2>Ako si nastaviť pravidlo pre ďalšie použitie</h2>
                <p>Po každom praní si všimnite, čo sa zlepšilo a čo zostalo: či zmizla soľná mapa, či sa vrátil zápach, či textil stvrdol, či sa špina drží v švoch alebo či sa voda začala vpíjať do povrchu. Táto spätná kontrola je praktickejšia než stále meniť celý prací postup.</p>
                <p>Ak sa rovnaký problém opakuje, nehľadajte silnejší prostriedok ako prvú možnosť. Najprv upravte triedenie, množstvo bielizne v bubne, predčistenie, dávku gélu, oplach a sušenie. Pri väčšine domácich textílií práve tieto kroky rozhodujú o výsledku viac než samotná teplota.</p>
                <p>Pri zimnej a nepremokavej výbave si nechajte malú rezervu: radšej dlhšie sušenie, menej mechaniky a kontrola štítku než rýchly zásah, ktorý zmení tvar, pružnosť alebo ochrannú úpravu.</p>
                """
            )
            quick_rule = '<h2 style="margin-top: 0;">Rýchla zásada</h2>'
            quick_pos = cleaned.find(quick_rule)
            div_pos = cleaned.rfind("<div", 0, quick_pos) if quick_pos != -1 else -1
            if div_pos != -1:
                return cleaned[:div_pos].rstrip() + "\n" + addition + "\n" + cleaned[div_pos:].lstrip()
            idx = insertion_index(cleaned)
            return cleaned[:idx].rstrip() + "\n" + addition + "\n" + cleaned[idx:].lstrip()
        return cleaned
    idx = insertion_index(cleaned)
    return cleaned[:idx].rstrip() + "\n" + EXPANSIONS[topic] + "\n" + cleaned[idx:].lstrip()


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
    response = requests.post(endpoint, json=body, headers={"Accept": "application/json, text/event-stream"}, timeout=120)
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
    parser = argparse.ArgumentParser(description="Conservatively expand VEVO retrofit wave 28 ski/socks/thermal/rain/mat articles.")
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
                "wave": "retrofit-wave-28-ski-socks-thermal-rain-mat-five",
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
                "wave": "retrofit-wave-28-ski-socks-thermal-rain-mat-five",
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
    print(json.dumps({"source_updates": len(updates), "live_updated": args.update_live, "mcp_updates": len(mcp_updates), "out": str(OUT_JSON), "mcp_results": str(MCP_RESULTS)}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
