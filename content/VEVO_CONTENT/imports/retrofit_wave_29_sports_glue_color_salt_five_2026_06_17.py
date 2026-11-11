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
OUT_JSON = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-29-sports-glue-color-salt-five-2026-06-17.json"
MCP_RESULTS = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-29-sports-glue-color-salt-five-2026-06-17-mcp-results.json"


ARTICLES = [
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-15-2026-06-09-articles.json",
        "slug": "ako-prat-futbalovy-dres-stucne-a-treningove-veci-po-zapase",
        "post_id": "2129",
        "url": "https://www.vevo.sk/n/ako-prat-futbalovy-dres-stucne-a-treningove-veci-po-zapase",
        "topic": "football_kit",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-15-2026-06-09-articles.json",
        "slug": "ako-prat-hokejovy-dres-a-textilne-vrstvy-z-vystroja",
        "post_id": "2130",
        "url": "https://www.vevo.sk/n/ako-prat-hokejovy-dres-a-textilne-vrstvy-z-vystroja",
        "topic": "hockey_layers",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-17-2026-06-10-articles.json",
        "slug": "ako-odstranit-lepidlo-z-oblecenia-po-tvoreni-s-detmi",
        "post_id": "2165",
        "url": "https://www.vevo.sk/n/ako-odstranit-lepidlo-z-oblecenia-po-tvoreni-s-detmi",
        "topic": "kids_glue",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-15-2026-06-09-articles.json",
        "slug": "ako-zabranit-pustaniu-farby-pri-prani-noveho-oblecenia",
        "post_id": "2133",
        "url": "https://www.vevo.sk/n/ako-zabranit-pustaniu-farby-pri-prani-noveho-oblecenia",
        "topic": "color_bleeding",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-19-2026-06-10-articles.json",
        "slug": "ako-odstranit-solne-mapy-z-nohavic-a-kabata-po-zime",
        "post_id": "2205",
        "url": "https://www.vevo.sk/n/ako-odstranit-solne-mapy-z-nohavic-a-kabata-po-zime",
        "topic": "salt_maps",
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
    items += '\n<li><a href="/n/casto-kladene-otazky-skvrny-na-obleceni-po-prani">Škvrny na oblečení po praní</a></li>'
    return f"<h2>Súvisiace návody na VEVO</h2>\n<ul>\n{items}\n</ul>"


TOPICS = {
    "football_kit": {
        "marker": "Detailnejší postup na futbalový dres, štucne a tréningové veci",
        "problem": "Futbalové veci po zápase kombinujú pot, blato, trávu, gumené častice z ihriska, zápach z tašky a citlivú potlač dresu. Ak sa všetko hodí mokré do práčky bez prípravy, špina sa roznesie a potlač dostane zbytočnú mechanickú záťaž.",
        "scope": "futbalový dres, čísla a logá, štucne, tréningové tričko, šortky, teplákovú bundu, blato, trávu, gumové granuly, pot, zápach a sušenie po zápase",
        "avoid": "aviváž, vysokú teplotu, sušenie na radiátore, pranie dresu naruby nezapnuté, miešanie so suchým zipsom, pranie veľkého blata bez vytrasenia a silné drhnutie potlače",
        "diagnosis": [
            "<strong>Dres otočte naruby:</strong> chráni čísla, logo aj potlač.",
            "<strong>Štucne riešte pred práčkou:</strong> tráva a blato sa majú vytriasť alebo predprať.",
            "<strong>Aviváž vynechajte:</strong> pri funkčných športových vláknach je zbytočné riziko.",
            "<strong>Taška nie je sklad:</strong> mokré veci vyberte čo najskôr po zápase.",
        ],
        "state_rows": [
            ("dres s potlačou", "naruby a nízka teplota", "chráni čísla"),
            ("štucne s blatom", "vytriasť a predprať chodidlovú časť", "pred bubnom"),
            ("zápach po zápase", "prať menšiu dávku a dobre vysušiť", "neprekrývať vôňou"),
            ("tréningová mikina", "zapnúť zipsy a skontrolovať suché zipsy", "menej zatrhnutí"),
        ],
        "textile_rows": [
            ("polyesterový dres", "naruby, bez aviváže", "potlač a odvod vlhkosti"),
            ("štucne", "samostatne alebo vo vrecku", "blato a trenie"),
            ("šortky", "predprať škvrny od trávy", "lokálne ošetrenie"),
            ("tréningová bunda", "zapnúť zipsy", "ochrana dresu"),
        ],
        "sections": [
            ("Ako prať futbalový dres po zápase", "Dres otočte naruby a skontrolujte potlač, čísla a logá. Ak sú na ňom trávové alebo blatové miesta, riešte ich lokálne pred praním. Nepoužívajte vysokú teplotu len preto, že zápas bol náročný.", "Pri dresoch je dôležité zachovať povrch. Čistota nemá prísť za cenu popraskaného čísla alebo odlupujúceho sa loga."),
            ("Ako prať štucne", "Štucne najprv vytraste, odstráňte trávu a gumené častice a špinavé chodidlá krátko preperte. Ak sú veľmi zablatené, nedávajte ich do rovnakej dávky ako dres.", "Pracie vrecko pomôže, keď sa štucne zachytávajú o iné veci alebo majú hrubšie zvyšky trávy."),
            ("Zápach futbalových vecí po praní", "Ak dres alebo štucne zapáchajú aj po praní, problém býva v preplnenom bubne, slabom oplachu alebo mokrej taške. Silná vôňa zápach len prekryje, ale neodstráni pot a zvyšky z ihriska.", "Pomôže menšia dávka, rýchle sušenie a oddelenie najšpinavších vecí od dresu."),
            ("Tráva a blato na futbalovom oblečení", "Blato nechajte podľa situácie preschnúť a vytraste ho. Trávu riešte lokálne pred praním, najmä na kolenách, rukávoch a lemoch. Ak škvrna zostane pred sušením, opakujte mierny postup.", "Súvisiace športové pranie rieši návod <a href=\"/n/ako-odstranit-zapach-z-bezeckych-legin-po-treningu\">ako odstrániť zápach z bežeckých legín po tréningu</a>."),
            ("Ako sušiť dres a štucne", "Dres nesušte na radiátore ani na priamom slnku. Potlač a elastické vlákna lepšie znášajú voľné sušenie. Štucne rozložte tak, aby nezostali vlhké v hrubom záhybe.", "Pred odložením do športovej tašky musí byť všetko úplne suché."),
        ],
        "expert_title": "Odbornejší pohľad: športový polyester, pot a potlač",
        "expert_p1": "Športové dresy sú často z polyesteru alebo zmesových funkčných materiálov. Ich výhodou je rýchle schnutie, ale pot a zápach sa môžu držať v syntetických vláknach, ak sa veci nechajú vlhké v taške.",
        "expert_p2": "American Cleaning Institute pri praní odporúča triediť podľa farieb a štítkov, ošetriť škvrny pred praním a používať primerané množstvo pracieho prostriedku. Pri futbalových veciach to znamená najprv blato a trávu, potom dres naruby a až nakoniec vôňu.",
        "source_html": '<p>Praktické základy prania uvádza <a rel="noopener" href="https://www.cleaninginstitute.org/cleaning-tips/clothes/laundry-basics" target="_blank">American Cleaning Institute: Laundry Basics</a>.</p>',
        "checklist": "Pred praním skontrolujte potlač, čísla, štucne, blato, trávu, zipsy, suché zipsy, zápach z tašky, veľkosť dávky, dávku gélu a spôsob sušenia.",
        "rule": "Futbalové veci po zápase najprv vytriasť a rozdeliť, dres prať naruby a štucne predčistiť podľa miery blata.",
        "recommendation_intro": "Pri futbalovej výstroji používajte prací gél až po odstránení hrubej špiny a s miernym dávkovaním.",
        "product_text": "Vhodný na pranie prateľných športových textílií, keď chcete odstrániť pot a zvyšky po zápase bez aviváže.",
        "category_text": "Pri športovej výbave vyberajte prací gél podľa materiálu, zápachu, miery zašpinenia a potreby dobrého oplachu.",
        "links": [
            ("/n/ako-pouzivat-parfum-do-prania-pri-sportovom-obleceni", "Ako používať parfum do prania pri športovom oblečení"),
            ("/n/ako-prat-cyklisticky-dres-a-elasticke-sportove-oblecenie", "Ako prať cyklistický dres"),
            ("/n/ako-odstranit-zapach-z-bezeckych-legin-po-treningu", "Ako odstrániť zápach z bežeckých legín"),
        ],
        "faq": [
            ("Môžem prať dres a štucne spolu?", "Áno, ak štucne nie sú plné blata a dres je otočený naruby. Pri silnej špine ich radšej oddeľte."),
            ("Prečo futbalový dres stále zapácha?", "Často pre mokrú tašku, preplnený bubon, slabý oplach alebo zvyšky potu vo vláknach."),
            ("Môžem použiť aviváž?", "Pri funkčných športových materiáloch radšej nie."),
        ],
    },
    "hockey_layers": {
        "marker": "Detailnejší postup na hokejový dres a textilné vrstvy z výstroja",
        "problem": "Hokejové textílie sú špecifické tým, že po tréningu zostávajú vlhké, teplé a zavreté v taške s výstrojom. Dres, termo vrstvy, ponožky a podvýstrojové tričko potrebujú rýchle vybratie, oddelenie od tvrdých chráničov a úplné vysušenie.",
        "scope": "hokejový dres, spodné termo vrstvy, ponožky, podvýstrojové tričko, textilné návleky, športovú tašku, pot, vlhkosť, pach, suché zipsy, chrániče a sušenie medzi tréningmi",
        "avoid": "zavretú tašku cez noc, pranie tvrdých chráničov s dresom, aviváž, preplnený bubon, horúce sušenie potlače, parfumovanie špinavej výstroje a odkladanie vlhkých vrstiev",
        "diagnosis": [
            "<strong>Prvá hodina rozhoduje:</strong> vyberte textílie z tašky čo najskôr.",
            "<strong>Dres a chrániče oddeľte:</strong> tvrdé časti môžu poškodiť textil.",
            "<strong>Spodné vrstvy perte pravidelne:</strong> sú priamo pri potu.",
            "<strong>Zápach neprekrývajte:</strong> najprv vysušiť, potom prať.",
        ],
        "state_rows": [
            ("mokré spodné vrstvy", "hneď vybrať a prať alebo sušiť", "kontakt s pokožkou"),
            ("dres s potlačou", "naruby, jemne", "čísla a logo"),
            ("výstroj v taške", "vetrať oddelene", "tvrdé časti"),
            ("silný zápach", "menšia dávka a dôkladný oplach", "pot a vlhkosť"),
        ],
        "textile_rows": [
            ("hokejový dres", "naruby a bez tvrdých chráničov", "potlač"),
            ("termo vrstva", "prať po tréningu", "pot pri pokožke"),
            ("ponožky", "samostatne pri silnom zápachu", "vlhkosť"),
            ("taška", "vetrať a utierať", "zdroj pachu"),
        ],
        "sections": [
            ("Čo urobiť hneď po tréningu", "Po príchode domov otvorte tašku a vyberte textilné vrstvy. Dres, termo tričko, ponožky a návleky nedržte zavreté s mokrými chráničmi. V tejto fáze ešte nejde o pranie, ale o zastavenie zápachu.", "Ak textil nemôžete hneď prať, aspoň ho rozložte na vzduch a nenechajte ho v hrubej mokrej kope."),
            ("Ako prať hokejový dres", "Dres otočte naruby, skontrolujte potlač a perte oddelene od tvrdých častí výstroja. Nepoužívajte vysokú teplotu ani sušenie na radiátore. Pri logách a číslach je šetrnosť dôležitá.", "Ak je dres len spotený, nepotrebuje agresívny program. Potrebuje dobrý oplach a úplné vysušenie."),
            ("Ako prať termo vrstvy pod výstroj", "Spodné vrstvy sú pri pokožke, preto ich perte pravidelnejšie než samotný dres. Otočte ich naruby, nepoužívajte aviváž a nepreplňte bubon. Pot a kožný maz sa musia vypláchnuť.", "Súvisiaci postup je <a href=\"/n/ako-prat-termo-bielizen-a-funkcnu-spodnu-vrstvu-bez-zapachu\">ako prať termo bielizeň bez zápachu</a>."),
            ("Zápach z hokejovej tašky", "Ak textil zapácha aj po praní, skontrolujte tašku. Vlhká taška vie preniesť pach späť na čisté vrstvy. Vetrať treba nielen oblečenie, ale aj vnútro tašky.", "Viac k tejto téme je v návode <a href=\"/n/ako-odstranit-zapach-z-tasky-na-sport-a-posilnovnu\">ako odstrániť zápach z tašky na šport</a>."),
            ("Kedy neprať všetko naraz", "Dres, spodné vrstvy a ponožky môžu ísť spolu iba vtedy, keď nemajú tvrdé zipsy, suché zipsy alebo hrubú špinu. Chrániče a tvrdé časti patria mimo bežný prací cyklus.", "Menšia dávka sa lepšie opláchne a rýchlejšie vysuší."),
        ],
        "expert_title": "Odbornejší pohľad: vlhkosť, športová taška a zápach",
        "expert_p1": "Zápach hokejových textílií vzniká hlavne kombináciou potu, vlhkosti a času v uzavretej taške. Pranie pomôže až vtedy, keď textil po tréningu nezostane dlho zavretý a mokrý.",
        "expert_p2": "CDC pri plesniach v domácnosti zdôrazňuje potrebu riešiť vlhkosť ako príčinu. Pri športovej výstroji je praktický princíp rovnaký: vybrať, vetrať, prať textilné vrstvy a ukladať až suché.",
        "source_html": '<p>K významu kontroly vlhkosti pozri <a rel="noopener" href="https://www.cdc.gov/mold-health/about/index.html" target="_blank">CDC Mold</a>.</p>',
        "checklist": "Pred praním skontrolujte dres naruby, potlač, termo vrstvy, ponožky, suché zipsy, tvrdé chrániče, zápach z tašky, veľkosť dávky, dávku gélu a miesto na sušenie.",
        "rule": "Hokejové textílie najprv vybrať z tašky a oddeliť od tvrdých chráničov; pranie má nasledovať až po rozdelení vrstiev.",
        "recommendation_intro": "Pri hokejových textíliách má prací gél pomôcť s potom a zápachom, ale základ je rýchle vybratie z tašky.",
        "product_text": "Vhodný na pravidelné pranie dresov a spodných vrstiev podľa štítku, bez aviváže a s dôkladným oplachom.",
        "category_text": "Pri hokejových veciach vyberajte prací gél podľa miery potu, syntetiky a potreby rýchleho oplachu.",
        "links": [
            ("/n/ako-prat-termo-bielizen-a-funkcnu-spodnu-vrstvu-bez-zapachu", "Ako prať termo bielizeň bez zápachu"),
            ("/n/ako-odstranit-zapach-z-tasky-na-sport-a-posilnovnu", "Ako odstrániť zápach zo športovej tašky"),
            ("/n/ako-odstranit-zapach-potu-z-polyesteroveho-tricka", "Ako odstrániť zápach potu z polyesterového trička"),
        ],
        "faq": [
            ("Môžem prať hokejový dres s chráničmi?", "Nie ako bežnú dávku. Tvrdé časti môžu poškodiť textil a často potrebujú iný spôsob čistenia."),
            ("Prečo výstroj zapácha aj po praní?", "Textil možno zostal dlho vlhký v taške alebo zapácha samotná taška a chrániče."),
            ("Môžem použiť parfum do prania?", "Až na čisté textílie, nie ako náhradu prania a sušenia."),
        ],
    },
    "kids_glue": {
        "marker": "Detailnejší postup na lepidlo z oblečenia po tvorení s deťmi",
        "problem": "Lepidlo po tvorení s deťmi môže byť školské, disperzné, tyčinkové, trblietkové alebo iný typ. Každé sa správa trochu inak. Najhoršie je mokré lepidlo rozotrieť do väčšej plochy alebo textil vysušiť teplom, kým miesto ešte lepí.",
        "scope": "detské tričko, školskú zásteru, tepláky, rukávy mikiny, tyčinkové lepidlo, biele školské lepidlo, trblietkové lepidlo, zaschnutý film, farbu textilu a bezpečné pranie po predčistení",
        "avoid": "rozotieranie mokrého lepidla, horúcu sušičku pred kontrolou, acetón bez testu, miešanie lepidla s jemnou bielizňou, drhnutie potlače a pranie bez odstránenia prebytku",
        "diagnosis": [
            "<strong>Zistite typ lepidla:</strong> školské lepidlo nie je sekundové lepidlo.",
            "<strong>Prebytok odoberte:</strong> práčka nemá rozpustiť hrubú vrstvu.",
            "<strong>Testujte farbu:</strong> detské oblečenie má často potlače a sýte farby.",
            "<strong>Nesušte teplom:</strong> lepkavý film sa môže zafixovať.",
        ],
        "state_rows": [
            ("mokré lepidlo", "odobrať prebytok, nerozotierať", "zmenšiť plochu"),
            ("zaschnuté lepidlo", "uvolňovať po malých častiach", "bez trhania vlákien"),
            ("trblietkové lepidlo", "najprv mechanicky", "častice sa šíria"),
            ("potlačené tričko", "test na rubovej strane", "riziko farby"),
        ],
        "textile_rows": [
            ("bavlnené tričko", "predprať lokálne a prať podľa štítku", "bežný prípad"),
            ("školská zástera", "odobrať hrubý film", "opakovane špinená"),
            ("mikina", "skontrolovať rukávy a manžety", "lepidlo v švoch"),
            ("jemný úplet", "bez tvrdého drhnutia", "riziko vyťahania"),
        ],
        "sections": [
            ("Ako odstrániť školské lepidlo z oblečenia", "Najprv odoberte prebytok. Ak je lepidlo mokré, nesnažte sa ho rozotrieť do strán. Ak je zaschnuté, uvoľňujte ho tupou hranou po malých častiach. Až potom má zmysel voda a prací gél.", "Pri bielom školskom lepidle často pomáha vlažná voda a lokálne predpranie, ale vždy sledujte štítok a farbu textilu."),
            ("Lepidlo na detskom tričku", "Detské tričká majú často potlač, preto nečistite agresívne priamo cez obrázok. Otestujte rub alebo vnútorný šev a pracujte jemne. Po lokálnom ošetrení perte s podobnými farbami.", "Ak zostane tvrdý okraj, textil nesušte horúco. Najprv zopakujte mierny postup."),
            ("Trblietkové lepidlo a tvorenie", "Pri trblietkovom lepidle riešite lepidlo aj malé častice. Najprv odstráňte, čo ide nasucho, aby sa trblietky nerozšírili do celej dávky. Potom lokálne preperte lepkavý film.", "Podobný mechanický problém opisuje článok <a href=\"/n/ako-odstranit-trblietky-z-siat-saka-a-kabata-po-oslave\">ako odstrániť trblietky z oblečenia</a>."),
            ("Kedy nejde o bežné školské lepidlo", "Ak ide o sekundové lepidlo, tavné lepidlo alebo špeciálne lepidlo na modelovanie, postup je rizikovejší. Vtedy nepoužívajte univerzálny postup pre školské lepidlo.", "Súvisiaci návod je <a href=\"/n/ako-odstranit-sekundove-lepidlo-z-textilu-a-kedy-to-nerobit-doma\">ako odstrániť sekundové lepidlo z textilu</a>."),
            ("Pranie po tvorení s deťmi", "Po predčistení perte oblečenie v menšej dávke. Detské veci často kombinujú lepidlo, farby, plastelínu a jedlo, preto sa oplatí najskôr rozlíšiť jednotlivé škvrny.", "Pri školských a tvorivých aktivitách pomáha mať samostatnú zásteru, ktorú neperiete s jemnou bielizňou."),
        ],
        "expert_title": "Odbornejší pohľad: lepidlo ako film medzi vláknami",
        "expert_p1": "Lepidlo nie je bežná rozpustená škvrna. Po zaschnutí vytvára film medzi vláknami a pranie ho môže iba zmäkčiť alebo rozšíriť, ak neodstránite prebytok.",
        "expert_p2": "Pri odstraňovaní škvŕn odborné návody odporúčajú konať čo najskôr a ošetrovať škvrnu pred praním. Pri lepidle to znamená najprv znížiť množstvo lepidla a až potom prať.",
        "source_html": '<p>Všeobecné princípy predbežného ošetrenia škvŕn uvádza <a rel="noopener" href="https://www.cleaninginstitute.org/cleaning-tips/clothes/stain-removal-guide" target="_blank">American Cleaning Institute: Stain Removal Guide</a>.</p>',
        "checklist": "Pred praním skontrolujte typ lepidla, potlač, farbu textilu, zaschnutý film, trblietky, rukávy, manžety, štítok, možnosť testu a to, či textil nepatrí radšej do samostatnej dávky.",
        "rule": "Lepidlo po tvorení najprv odobrať a určiť typ, až potom lokálne predprať a prať podľa štítku.",
        "recommendation_intro": "Pri detskom oblečení použite prací gél až po odstránení prebytku lepidla, aby sa zvyšky dobre vypláchli.",
        "product_text": "Vhodný na následné pranie prateľných detských tričiek, teplákov a záster po lokálnom predčistení lepidla.",
        "category_text": "Pri detskom oblečení vyberajte prací gél podľa citlivosti textilu, farieb a potreby dôkladného oplachu.",
        "links": [
            ("/n/ako-odstranit-sekundove-lepidlo-z-textilu-a-kedy-to-nerobit-doma", "Ako odstrániť sekundové lepidlo z textilu"),
            ("/n/ako-vyprat-voskovky-z-peracnika-a-textilneho-obalu", "Ako vyprať voskovky z peračníka"),
            ("/n/ako-prat-skolske-oblecenie-a-teplaky-deti", "Ako prať školské oblečenie detí"),
        ],
        "faq": [
            ("Môžem dať oblečenie s lepidlom hneď do práčky?", "Radšej nie. Najprv odstráňte prebytok a zistite typ lepidla."),
            ("Čo ak je lepidlo už zaschnuté?", "Uvoľňujte ho po malých častiach tupou hranou a až potom predperte."),
            ("Je sekundové lepidlo rovnaký prípad?", "Nie. Sekundové lepidlo je rizikovejšie a má samostatný postup."),
        ],
    },
    "color_bleeding": {
        "marker": "Detailnejší postup pri púšťaní farby z nového oblečenia",
        "problem": "Nové oblečenie môže púšťať farbu najmä pri sýtych odtieňoch, tmavých rifliach, červených šatách, čiernych tričkách a lacnejších potlačiach. Prevencia je jednoduchšia než zachraňovanie zafarbenej bielizne: test farby, prvé pranie oddelene a nízka teplota.",
        "scope": "nové čierne tričko, červené šaty, tmavé rifle, sýte uteráky, potlač, prvé pranie, test vlhkou bielou handričkou, farbolapky, triedenie a ochranu bielej bielizne",
        "avoid": "prvé pranie s bielou bielizňou, horúcu vodu pri rizikovej farbe, preplnený bubon, dlhé namáčanie sýtych farieb, sušenie zafarbenej bielizne pred kontrolou a spoliehanie sa iba na vôňu",
        "diagnosis": [
            "<strong>Test bielou handričkou:</strong> rýchlo ukáže riziko púšťania farby.",
            "<strong>Prvé pranie oddelene:</strong> hlavne červená, tmavomodrá a čierna.",
            "<strong>Nízka teplota pomáha:</strong> znižuje riziko uvoľnenia farby.",
            "<strong>Zafarbenej bielizne sa nedotýka sušička:</strong> teplo môže problém zafixovať.",
        ],
        "state_rows": [
            ("handrička sa zafarbí", "prať samostatne", "vysoké riziko"),
            ("nový čierny kus", "naruby a s tmavými farbami", "ochrana povrchu"),
            ("červené oblečenie", "prvé prania oddelene", "časté púšťanie"),
            ("zafarbená bielizeň", "nesušiť, riešiť hneď", "nezafixovať"),
        ],
        "textile_rows": [
            ("tmavé rifle", "samostatne alebo s tmavými", "indigo"),
            ("červené šaty", "test a nízka teplota", "sýta farba"),
            ("čierne tričko", "naruby", "blednutie"),
            ("biela bielizeň", "nikdy s rizikovým kusom", "ochrana"),
        ],
        "sections": [
            ("Ako otestovať, či nové oblečenie púšťa farbu", "Navlhčite bielu handričku a pretrite vnútorný šev alebo skrytú časť. Ak sa handrička zafarbí, prvé pranie urobte samostatne alebo len s veľmi podobnými farbami.", "Test nie je dokonalý, ale rýchlo odhalí najrizikovejšie kusy."),
            ("Prvé pranie nového čierneho trička", "Čierne tričko otočte naruby, perte s tmavými vecami a nepoužívajte zbytočne vysokú teplotu. Pri potlači sledujte aj odporúčanie na štítku.", "Súvisiaci návod je <a href=\"/n/ako-prat-cierne-oblecenie-aby-nebledlo-a-nebolo-flakate\">ako prať čierne oblečenie, aby nebledlo</a>."),
            ("Ako prať červené šaty prvýkrát", "Červené a sýto ružové odtiene perte prvýkrát samostatne alebo s podobnými farbami. Ak máte pochybnosti, pridajte farbolapku, ale nespoliehajte sa na ňu ako na úplnú ochranu bielej bielizne.", "Ak voda výrazne zafarbí, zopakujte oddelené pranie aj ďalší raz."),
            ("Čo robiť, keď farba už pustila", "Zafarbenú bielizeň nesušte. Kým je mokrá, máte väčšiu šancu problém riešiť. Oddeľte zdroj farby a postupujte podľa materiálu a štítku.", "Súvisiaci text je <a href=\"/n/pustila-farba-v-pracke-co-urobit-s-bielym-trickom-a-ruzovou-bieliznou\">pustila farba v práčke</a>."),
            ("Prečo nepomôže len silnejší program", "Púšťanie farby nie je bežná špina. Silnejší program alebo vyššia teplota môžu riziko zvýšiť. Dôležitejšie je triedenie, test, nízka teplota a prvé prania bez bielej bielizne.", "Až keď farba prestane púšťať, môžete kus zaradiť do bežnejšieho režimu podľa štítku."),
        ],
        "expert_title": "Odbornejší pohľad: farbivo, teplota a triedenie",
        "expert_p1": "Pri novom oblečení môže byť časť farbiva na povrchu vlákien alebo menej stabilná po prvých praniach. Teplota, mechanika a pH pracieho prostredia môžu ovplyvniť, koľko farby sa uvoľní.",
        "expert_p2": "Základné pravidlá prania podľa American Cleaning Institute začínajú triedením podľa farieb a pokynov na štítku. Pri novom oblečení je toto pravidlo kľúčové, pretože jeden rizikový kus môže poškodiť celú dávku.",
        "source_html": '<p>Základy triedenia a prania uvádza <a rel="noopener" href="https://www.cleaninginstitute.org/cleaning-tips/clothes/laundry-basics" target="_blank">American Cleaning Institute: Laundry Basics</a>.</p>',
        "checklist": "Pred prvým praním skontrolujte farbu, štítok, potlač, test bielou handričkou, podobné farby v dávke, teplotu, veľkosť bubna, farbolapku a to, či v dávke nie je biela bielizeň.",
        "rule": "Pri novom oblečení najprv test farby, potom prvé pranie samostatne alebo s podobnými odtieňmi a až neskôr bežné triedenie.",
        "recommendation_intro": "Pri prvom praní nových farebných vecí používajte primerané množstvo gélu a nepreplňte bubon.",
        "product_text": "Vhodný na šetrné pranie nového prateľného oblečenia podľa štítku, keď chcete znížiť zvyšky a dobre opláchnuť farbené textílie.",
        "category_text": "Pri farebných dávkach vyberajte prací gél podľa materiálu, teploty a potreby šetrného prania.",
        "links": [
            ("/n/ako-prat-nove-oblecenie-prvykrat-farby-chemicky-pach-zrazanie-a-stitok", "Ako prať nové oblečenie prvýkrát"),
            ("/n/pustila-farba-v-pracke-co-urobit-s-bielym-trickom-a-ruzovou-bieliznou", "Pustila farba v práčke"),
            ("/n/ako-prat-cierne-oblecenie-aby-nebledlo-a-nebolo-flakate", "Ako prať čierne oblečenie"),
        ],
        "faq": [
            ("Ako zistím, či nové oblečenie púšťa farbu?", "Navlhčite bielu handričku a pretrite skryté miesto. Ak sa zafarbí, perte samostatne."),
            ("Pomôže farbolapka?", "Môže pomôcť znížiť riziko, ale nenahrádza triedenie a prvé pranie oddelene."),
            ("Čo robiť so zafarbenou bielizňou?", "Nesušiť ju a riešiť ju hneď, kým je mokrá."),
        ],
    },
    "salt_maps": {
        "marker": "Detailnejší postup na soľné mapy na nohaviciach a kabáte",
        "problem": "Soľné mapy po zime vznikajú na lemoch nohavíc, kabátoch, rukávoch a spodných okrajoch, kde sa posypová soľ mieša s vodou a špinou. Na tmavom textile vyzerajú dramaticky, ale často sa dajú riešiť jemne: nasucho, vlhkou bielou handričkou a až potom praním podľa štítku.",
        "scope": "tmavé nohavice, kabát, spodné lemy, rukávy, vlnenú zmes, softshell, posypovú soľ, mokrý sneh, biele mapy, lokálne čistenie, čistiareň a sušenie bez tepla",
        "avoid": "silné drhnutie mapy, premočenie kabáta, pranie neprateľného kabáta, horúce sušenie, farebnú handričku, agresívne odsoľovanie a vôňu namiesto odstránenia soli",
        "diagnosis": [
            "<strong>Najprv nechať preschnúť:</strong> suché kryštáliky idú ľahšie von.",
            "<strong>Použiť bielu handričku:</strong> vidíte, či sa prenáša soľ alebo farba.",
            "<strong>Kabát kontrolovať podľa štítku:</strong> výstuž a vlna nemusia patriť do práčky.",
            "<strong>Mapa nie je vždy špina:</strong> často ide o minerálny zvyšok.",
        ],
        "state_rows": [
            ("biela mapa na nohaviciach", "vykefovať a pretrieť vlhkou handričkou", "lokálne"),
            ("kabát s výstužou", "čistiareň alebo lokálne čistenie", "tvar"),
            ("mokré lemy", "najprv vysušiť", "pred kefou"),
            ("opakovaná mapa", "skontrolovať topánky a rohožku", "zdroj soli"),
        ],
        "textile_rows": [
            ("tmavé nohavice", "lokálne + pranie podľa štítku", "viditeľné mapy"),
            ("vlnený kabát", "lokálne alebo čistiareň", "riziko tvaru"),
            ("softshell", "bez aviváže", "povrchová úprava"),
            ("podšívka", "nepremáčať", "dlhé schnutie"),
        ],
        "sections": [
            ("Ako odstrániť soľné mapy z nohavíc", "Nohavice nechajte preschnúť a mäkkou kefou odstráňte kryštáliky soli. Potom použite čistú vlhkú bielu handričku a mapu jemne vytláčajte smerom von z látky.", "Ak nohavice štítok povoľuje prať, vyperte ich až po lokálnom ošetrení. Pranie má odstrániť zvyšok, nie celý nános soli."),
            ("Soľné mapy na kabáte", "Kabát nepatrí automaticky do práčky. Vlnená zmes, výstuž, podšívka a tvar ramien môžu byť citlivé. Pri malej mape skúste lokálne čistenie bielou handričkou.", "Ak je mapa veľká, kabát drahší alebo sa mení povrch, zvoľte čistiareň."),
            ("Prečo nepremáčať celý kabát", "Soľ sa síce rozpúšťa vo vode, ale celý kabát môže schnúť veľmi dlho a stratiť tvar. Vlhkosť v podšívke navyše môže spôsobiť zatuchnutie.", "Pracujte po malých plochách a vlhkosť priebežne odsávajte suchým uterákom."),
            ("Soľné mapy sa vracajú", "Ak sa mapa po vysušení vráti, soľ zostala hlbšie vo vlákne alebo v leme. Zopakujte mierny postup, ale nezvyšujte hneď silu drhnutia.", "Skontrolujte aj rohožku a predsieň, pretože soľ sa často prenáša späť z topánok a podlahy."),
            ("Prevencia po zime", "Po zime vyčistite lemy nohavíc, kabáty, rohožku aj textílie v predsieni. Ak zostane soľ v rohožke, bude sa vracať na oblečenie aj pri ďalšom nosení.", "Súvisiaci postup je <a href=\"/n/ako-vycistit-rohozku-a-textilie-v-predsieni-od-posypovej-soli\">ako vyčistiť rohožku od posypovej soli</a>."),
        ],
        "expert_title": "Odbornejší pohľad: posypová soľ, minerálne zvyšky a vlhkosť",
        "expert_p1": "Soľné mapy sú často minerálny zvyšok, nie klasická organická škvrna. Preto sa najprv oplatí odstrániť suché kryštáliky a až potom zvyšok rozpustiť malým množstvom vody.",
        "expert_p2": "Pri kabátoch a nohaviciach rozhoduje konštrukcia. Tkanina môže byť prateľná, ale výstuž, podšívka alebo tvar nemusia znášať celé pranie. Preto má lokálne čistenie často nižšie riziko.",
        "source_html": '<p>Všeobecné princípy ošetrovania škvŕn pred praním uvádza <a rel="noopener" href="https://www.cleaninginstitute.org/cleaning-tips/clothes/stain-removal-guide" target="_blank">American Cleaning Institute: Stain Removal Guide</a>.</p>',
        "checklist": "Pred čistením skontrolujte štítok, typ kabáta, podšívku, výstuž, veľkosť mapy, suché kryštáliky, farbu handričky, možnosť prania, zdroj soli v predsieni a spôsob sušenia.",
        "rule": "Soľné mapy najprv nasucho, potom čistou vlhkou handričkou a pranie alebo čistiareň až podľa materiálu.",
        "recommendation_intro": "Pri prateľných nohaviciach a textilných lemoch použite prací gél až po lokálnom odstránení soli.",
        "product_text": "Vhodný na následné pranie prateľných nohavíc alebo textilných častí po tom, čo je soľná mapa lokálne ošetrená.",
        "category_text": "Pri zimných škvrnách vyberajte prací gél podľa materiálu, farby a potreby dobrého oplachu po soli.",
        "links": [
            ("/n/ako-vycistit-rohozku-a-textilie-v-predsieni-od-posypovej-soli", "Ako vyčistiť rohožku od posypovej soli"),
            ("/n/ako-vycistit-navlek-na-autosedacku-po-zime-a-posypovej-soli", "Ako vyčistiť návlek na autosedačku po zime"),
            ("/n/ako-prat-sako-doma-a-kedy-ho-radsej-dat-do-cistiarne", "Ako prať sako doma a kedy zvoliť čistiareň"),
        ],
        "faq": [
            ("Môžem dať kabát so soľou do práčky?", "Iba ak to povoľuje štítok a konštrukcia kabáta. Pri výstuži alebo vlne radšej lokálne alebo čistiareň."),
            ("Prečo sa mapa po vysušení vrátila?", "Časť soli zostala hlbšie vo vlákne alebo v leme a po vyschnutí sa znovu ukázala."),
            ("Pomôže vôňa do bytu alebo sprej?", "Nie ako riešenie. Najprv treba odstrániť soľ a vlhkosť."),
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
        <p>Pri tejto téme sa oplatí pozerať na celý kontext: {config["scope"]}. Najväčšie riziko je {config["avoid"]}. Preto najprv určite, či riešite pot, blato, lepidlo, farbu, soľ, vlhkosť alebo citlivú potlač.</p>
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
        <p>Pred praním si položte tri otázky: čo je zdroj problému, či je textil prateľný a čo sa môže poškodiť. Dres s potlačou, hokejová vrstva plná potu, detské lepidlo, nová farba a soľná mapa potrebujú iný prvý krok.</p>
        <h2>Malý test pred väčším zásahom</h2>
        <p>Pred silnejším čistením testujte nenápadné miesto: rubový šev, vnútorný lem, kúsok potlače mimo hlavnej plochy alebo spodný okraj. Sledujte, či sa púšťa farba, mení povrch, mäkne potlač alebo zostáva lepkavý film.</p>
        <p>Ak test ukáže zmenu, nezvyšujte silu na viditeľnej časti. Pri športových potlačiach, detskom oblečení, nových farbách a kabátoch je bezpečnejšie postup spomaliť než poškodiť celý kus.</p>
        <h2>Kedy textil nesušiť a neodkladať</h2>
        <p>Textil nesušte horúco ani neodkladajte, ak zostal zápach, lepidlo, zafarbenie, soľná mapa alebo vlhkosť. Teplo vie zafixovať škvrnu, potlač aj farbu; uzavretá taška alebo skriňa zase vráti zápach.</p>
        <p>Pred odložením skontrolujte švy, lemy, potlač, čísla, vnútorné vrstvy, vrecká a miesta, ktoré schnú pomalšie. Suchý povrch nemusí znamenať suchý textil.</p>
        <h2>Domáca rutina pri opakovanom probléme</h2>
        <p>Ak sa problém opakuje, nastavte si pravidlo: najprv oddeliť rizikový kus, potom odstrániť povrchovú špinu, lokálne ošetriť, prať v menšej dávke, dávkovať primerane a pred sušením skontrolovať výsledok.</p>
        <p>Takto sa pranie stane predvídateľné. Pri športových veciach rozhoduje rýchle sušenie, pri lepidle predčistenie, pri farbe triedenie a pri soli práca po malých vlhkých plochách.</p>
        <h2>Ako rozdeliť dávku, aby sa problém nešíril</h2>
        <p>Najrizikovejší kus nepatrí automaticky do spoločnej dávky. Zablatené štucne, vlhké hokejové vrstvy, tričko s lepidlom, nové červené šaty alebo nohavice so soľou môžu zašpiniť ostatné veci ešte skôr, než sa stihnú vyčistiť.</p>
        <p>Ak je problém výrazný, perte samostatne alebo v menšej dávke s podobnými materiálmi a farbami. Bubon nesmie byť plný, pretože špina, farba, zvyšky gélu alebo rozpustená soľ potrebujú priestor na odplavenie. Menšia dávka často vyčistí lepšie než silnejší program.</p>
        <p>Pri športových veciach oddeľte textil s potlačou od vecí so suchým zipsom. Pri detskom tvorení oddeľte lepidlo od jemnej bielizne. Pri nových farbách chráňte bielu bielizeň. Pri soli nedávajte do dávky čisté tmavé oblečenie, ktoré by mohlo chytiť mapy späť.</p>
        <h2>Kontrola po praní pred sušením</h2>
        <p>Po praní neskáčte rovno na sušenie. Skontrolujte, či nezostal pach v podpazuší alebo v hokejovej vrstve, či lepidlo netvrdne, či farba nezafarbila okolie a či soľná mapa nezostala na leme. Ak problém vidíte ešte mokrý, riešte ho hneď.</p>
        <p>Sušička, radiátor alebo priame slnko sú až posledný krok. Teplo môže zafixovať zvyšok škvrny, oslabiť potlač, zvýrazniť mapu alebo zmeniť pružnosť športového textilu. Pri pochybnostiach zvoľte voľné sušenie a opakujte mierny postup.</p>
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
        re.compile(r"<p>\s*Článok cieli výrazy ako\s*(.*?)\.\s*(.*?)</p>", re.IGNORECASE | re.DOTALL),
        r"<p>V texte nájdete aj praktické situácie, ktoré sa pri tejto téme často riešia: \1. \2</p>",
    ),
    (
        re.compile(r"<p>\s*Téma pokrýva\s*(.*?)\.\s*(.*?)</p>", re.IGNORECASE | re.DOTALL),
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
    cleaned = cleaned.replace(
        'Súvisiaci všeobecný návod je <a href="/n/ako-vyprat-travu-z-oblecenia-kompletny-sprievodca-pre-rodicov-aj-sportovcov">ako vyprať trávu z oblečenia</a>.',
        'Súvisiace športové pranie rieši návod <a href="/n/ako-odstranit-zapach-z-bezeckych-legin-po-treningu">ako odstrániť zápach z bežeckých legín po tréningu</a>.',
    )
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
        if "Ako rozdeliť dávku, aby sa problém nešíril" not in cleaned:
            addition = clean(
                """
                <h2>Ako rozdeliť dávku, aby sa problém nešíril</h2>
                <p>Najrizikovejší kus nepatrí automaticky do spoločnej dávky. Zablatené štucne, vlhké hokejové vrstvy, tričko s lepidlom, nové červené šaty alebo nohavice so soľou môžu zašpiniť ostatné veci ešte skôr, než sa stihnú vyčistiť.</p>
                <p>Ak je problém výrazný, perte samostatne alebo v menšej dávke s podobnými materiálmi a farbami. Bubon nesmie byť plný, pretože špina, farba, zvyšky gélu alebo rozpustená soľ potrebujú priestor na odplavenie. Menšia dávka často vyčistí lepšie než silnejší program.</p>
                <p>Pri športových veciach oddeľte textil s potlačou od vecí so suchým zipsom. Pri detskom tvorení oddeľte lepidlo od jemnej bielizne. Pri nových farbách chráňte bielu bielizeň. Pri soli nedávajte do dávky čisté tmavé oblečenie, ktoré by mohlo chytiť mapy späť.</p>
                <h2>Kontrola po praní pred sušením</h2>
                <p>Po praní neskáčte rovno na sušenie. Skontrolujte, či nezostal pach v podpazuší alebo v hokejovej vrstve, či lepidlo netvrdne, či farba nezafarbila okolie a či soľná mapa nezostala na leme. Ak problém vidíte ešte mokrý, riešte ho hneď.</p>
                <p>Sušička, radiátor alebo priame slnko sú až posledný krok. Teplo môže zafixovať zvyšok škvrny, oslabiť potlač, zvýrazniť mapu alebo zmeniť pružnosť športového textilu. Pri pochybnostiach zvoľte voľné sušenie a opakujte mierny postup.</p>
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
    parser = argparse.ArgumentParser(description="Conservatively expand VEVO retrofit wave 29 sports/glue/color/salt articles.")
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
                "wave": "retrofit-wave-29-sports-glue-color-salt-five",
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
                "wave": "retrofit-wave-29-sports-glue-color-salt-five",
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
