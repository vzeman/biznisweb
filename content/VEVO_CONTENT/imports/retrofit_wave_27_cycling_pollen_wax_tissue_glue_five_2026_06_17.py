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
OUT_JSON = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-27-cycling-pollen-wax-tissue-glue-five-2026-06-17.json"
MCP_RESULTS = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-27-cycling-pollen-wax-tissue-glue-five-2026-06-17-mcp-results.json"


ARTICLES = [
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-19-2026-06-10-articles.json",
        "slug": "ako-vycistit-cyklisticke-navleky-na-tretry-po-dazdi-a-blate",
        "post_id": "2211",
        "url": "https://www.vevo.sk/n/ako-vycistit-cyklisticke-navleky-na-tretry-po-dazdi-a-blate",
        "topic": "cycling_overshoes",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-19-2026-06-10-articles.json",
        "slug": "ako-prat-oblecenie-pri-pelovej-alergii-po-prichode-zvonka",
        "post_id": "2222",
        "url": "https://www.vevo.sk/n/ako-prat-oblecenie-pri-pelovej-alergii-po-prichode-zvonka",
        "topic": "pollen_allergy",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-19-2026-06-10-articles.json",
        "slug": "ako-odstranit-vosk-na-lyze-z-lyziarskej-bundy-a-rukavic",
        "post_id": "2209",
        "url": "https://www.vevo.sk/n/ako-odstranit-vosk-na-lyze-z-lyziarskej-bundy-a-rukavic",
        "topic": "ski_wax",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-18-2026-06-10-articles.json",
        "slug": "ako-dostat-kusky-papierovej-vreckovky-z-ciernych-nohavic-a-mikiny",
        "post_id": "2185",
        "url": "https://www.vevo.sk/n/ako-dostat-kusky-papierovej-vreckovky-z-ciernych-nohavic-a-mikiny",
        "topic": "paper_tissue",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-17-2026-06-10-articles.json",
        "slug": "ako-odstranit-sekundove-lepidlo-z-textilu-a-kedy-to-nerobit-doma",
        "post_id": "2166",
        "url": "https://www.vevo.sk/n/ako-odstranit-sekundove-lepidlo-z-textilu-a-kedy-to-nerobit-doma",
        "topic": "super_glue",
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
    items += '\n<li><a href="/n/preco-je-bielizen-po-prani-tvrda-alebo-lepkava-zvysky-gelu-davkovanie-a-oplach">Prečo je bielizeň po praní tvrdá alebo lepkavá</a></li>'
    return f"<h2>Súvisiace návody na VEVO</h2>\n<ul>\n{items}\n</ul>"


TOPICS = {
    "cycling_overshoes": {
        "marker": "Detailnejší postup na cyklistické návleky po daždi a blate",
        "problem": "Cyklistické návleky na tretry sú malý kus výbavy, ale dostávajú veľmi agresívnu kombináciu špiny: blato zo spodnej hrany, vodu z cesty, pot z chodidla, prach zo suchých zipsov a trenie o kľuku alebo tretru. Ak ich po jazde hodíte mokré do tašky, problém už nie je len blato, ale aj zápach a oslabenie pružných častí.",
        "scope": "neoprénové návleky, textilné návleky, zateplené zimné návleky, reflexné prvky, zipsy, suché zipsy, švy pri podrážke, spodnú hranu, blato, cestnú špinu, pot a sušenie po tréningu",
        "avoid": "pranie hneď v mokrom blate, drhnutie tvrdou kefou, vysoké otáčky, aviváž na funkčné materiály, sušenie na radiátore, miešanie so spodnou bielizňou alebo jemnými úpletmi",
        "diagnosis": [
            "<strong>Blato nechajte preschnúť:</strong> mokré blato sa pri vode roznesie do švov a zipsu.",
            "<strong>Zapnite suché zipsy:</strong> chránite návleky aj ostatné textílie v dávke.",
            "<strong>Neoprén potrebuje jemnosť:</strong> netlačte ho kefou a nesušte ho horúco.",
            "<strong>Zápach riešte včas:</strong> mokrá výbava v taške zapácha rýchlejšie než bežné tričko.",
        ],
        "state_rows": [
            ("zaschnuté blato", "vykefovať nasucho", "až potom voda"),
            ("mokré blato po jazde", "nechať odkvapkať a preschnúť", "nerozotierať"),
            ("zápach z návlekov", "predprať jemne a úplne vysušiť", "často vzniká v taške"),
            ("suchý zips plný špiny", "vybrať hrubé nečistoty", "chráni ďalšie prádlo"),
        ],
        "textile_rows": [
            ("neoprénové návleky", "ručné pranie alebo veľmi jemný režim", "chráni pružnosť"),
            ("zateplené návleky", "nesušiť na priamom teple", "výplň schne pomalšie"),
            ("reflexné prvky", "nízka mechanika a otočenie naruby", "menšie odieranie"),
            ("zipsy a suché zipsy", "zapnúť pred praním", "menej zatrhnutí"),
        ],
        "sections": [
            ("Ako vyčistiť cyklistické návleky na tretry po daždi", "Po daždi nechajte návleky najprv odkvapkať a odstráňte hrubú špinu zo spodnej hrany. Ak je blato ešte mokré, netlačte ho kefou do švov. Oplatí sa počkať, kým sa dá odlúpnuť alebo jemne vykefovať.", "Až potom má zmysel ručné prepranie alebo jemný program. Práčka nemá riešiť celý nános blata, ale už len zvyšky, pot a zápach."),
            ("Ako prať neoprénové návleky", "Neoprén sa nespráva ako bavlnené tričko. Pri tvrdšom drhnutí, horúcej vode alebo sušení na radiátore môže stratiť pružnosť a povrch sa môže poškodiť. Použite vlažnú vodu, malé množstvo jemného gélu a krátke prepranie.", "Po praní návleky nekrúťte do špirály. Vodu vytlačte cez uterák a sušte rozložené alebo zavesené tak, aby sa nevytvoril mokrý záhyb."),
            ("Blato pri švoch a spodnej hrane", "Najviac špiny zostáva pri spodnej hrane, kde sa návlek dotýka tretry a podrážky. Toto miesto čistite osobitne, nie až v spoločnej dávke. Pomôže mäkká kefka, stará zubná kefka na detail a oplach pod miernym prúdom vody.", "Ak sa špina nechá v švoch celé dni, ďalšie pranie ju uvoľňuje ťažšie a zápach sa vracia rýchlejšie."),
            ("Zipsy, suché zipsy a reflexné prvky", "Pred praním zapnite zipsy a suché zipsy. Voľný suchý zips sa zachytáva o dresy, ponožky a jemné športové textílie. Reflexné prvky chráňte otočením naruby alebo pracím vreckom.", "Ak je suchý zips plný trávy a blata, najprv ho očistite nasucho. Inak sa nečistoty môžu preniesť do celej dávky."),
            ("Kedy návleky neprať s dresom", "Návleky po blate nepatria do rovnakej dávky ako cyklistický dres, spodná vrstva alebo uterák. Majú hrubšiu špinu, suché zipsy a často aj výraznejší pach.", "Ak ich chcete prať v práčke, dajte ich do samostatnej malej dávky alebo do vrecka a zvoľte nízku mechaniku."),
            ("Ako predísť zápachu v taške", "Po návrate domov návleky vyberte z tašky ako prvé. Najhoršia kombinácia je mokrý neoprén, zvyšky blata a uzavretá športová taška cez noc.", "Súvisiaci problém rieši aj návod <a href=\"/n/ako-odstranit-zapach-z-bezeckych-legin-po-treningu\">ako odstrániť zápach z bežeckých legín po tréningu</a>. Pri športovej výbave rozhoduje rýchle sušenie."),
        ],
        "expert_title": "Odbornejší pohľad: blato, pot a pružné materiály",
        "expert_p1": "Blato je kombinácia minerálnych častíc, organických zvyškov a vody. Pri praní sa správa inak než pot: najprv ho treba znížiť mechanicky, potom až prať. Pot a pach sa naopak riešia vodou, pracím prostriedkom a dobrým vysušením.",
        "expert_p2": "Pri neoprénových a elastických návlekoch je dôležitá nízka mechanika. Čím viac sa materiál naťahuje, krúti a prehrieva, tým vyššie je riziko, že stratí tvar alebo sa začnú odierať okraje.",
        "source_html": '<p>Pri škvrnách a textilných nečistotách je užitočné odlišovať typ škvrny a materiál. Praktický prehľad ponúka <a rel="noopener" href="https://extension.illinois.edu/global/stain-solutions" target="_blank">Illinois Extension Stain Solutions</a>.</p>',
        "checklist": "Pred praním skontrolujte zaschnuté blato, švy pri podrážke, zipsy, suché zipsy, reflexné prvky, typ materiálu, zápach z tašky, odporúčanú teplotu, možnosť ručného prania a spôsob sušenia.",
        "rule": "Pri návlekoch na tretry platí: najprv blato nasucho, potom jemné pranie a úplné sušenie mimo priameho tepla.",
        "recommendation_intro": "Pri športových návlekoch má jemný prací gél zmysel až po odstránení blata a zapnutí suchých zipsov.",
        "product_text": "Vhodný na následné šetrné pranie prateľných športových návlekov, keď je hrubá špina už preč a chcete riešiť pot, zápach a zvyšky blata.",
        "category_text": "Pri športovej výbave vyberajte prací gél podľa materiálu, potreby dobrého oplachu a citlivosti funkčných vlákien.",
        "links": [
            ("/n/ako-odstranit-zapach-z-bezeckych-legin-po-treningu", "Ako odstrániť zápach z bežeckých legín po tréningu"),
            ("/n/ako-prat-softshell-bundu-a-nohavice-bez-poskodenia-membrany", "Ako prať softshell bez poškodenia membrány"),
            ("/n/ako-prat-oblecenie-po-rybacke-alebo-turistike", "Ako prať oblečenie po rybačke alebo turistike"),
        ],
        "faq": [
            ("Môžem cyklistické návleky prať v práčke?", "Áno, ak to povoľuje výrobca, ale až po odstránení hrubého blata a so zapnutými zipsami alebo suchými zipsami."),
            ("Ako vyčistiť neoprénové návleky?", "Použite vlažnú vodu, jemný gél, nízku mechaniku a sušenie mimo radiátora."),
            ("Prečo návleky stále zapáchajú?", "Najčastejšie zostali mokré v taške alebo sa blato drží v švoch a spodnej hrane."),
        ],
    },
    "pollen_allergy": {
        "marker": "Detailnejší postup pri praní oblečenia počas peľovej sezóny",
        "problem": "Pri peľovej alergii nejde iba o to, či je tričko špinavé. Oblečenie, vlasy, taška, mikina a bunda môžu preniesť peľ do spálne, na pohovku, uteráky alebo posteľnú bielizeň. Pranie má preto znížiť prenos peľu, nie prevoňať ho.",
        "scope": "vrchné vrstvy po príchode zvonka, tričká pri tele, mikiny, bundy, šály, čiapky, posteľnú bielizeň, uteráky, sušenie vonku, predsieň a kontakt so spálňou",
        "avoid": "odkladanie vonkajšieho oblečenia na posteľ, sušenie alergikovej bielizne vonku počas silného peľu, miešanie peľového oblečenia s obliečkami, silné vône pri citlivom nose a zbytočné denné pranie všetkých vrchných vrstiev",
        "diagnosis": [
            "<strong>Spálňa je najcitlivejšia zóna:</strong> peľ z oblečenia a vlasov tam nechcete preniesť.",
            "<strong>Vrchné vrstvy nemusia ísť vždy do práčky:</strong> často stačí vytriasť, vyvetrať a držať mimo postele.",
            "<strong>Vrstvy pri tele perte častejšie:</strong> najmä ak sa človek spotil.",
            "<strong>Vôňa nie je riešenie alergie:</strong> pri citlivom nose používajte mierne dávkovanie a dobrý oplach.",
        ],
        "state_rows": [
            ("tričko po prechádzke", "prať pri spotení alebo priamom kontakte", "peľ a pot"),
            ("bunda a mikina", "vytriasť, valčekovať alebo prať podľa potreby", "nemusí denne"),
            ("posteľná bielizeň", "meniť častejšie v sezóne", "kontakt celú noc"),
            ("uterák po sprche", "oddeliť od vonkajších vrstiev", "neprenášať peľ"),
        ],
        "textile_rows": [
            ("bavlnené tričko", "bežné pranie s dobrým oplachom", "kontakt s pokožkou"),
            ("mikina", "vytriasť a prať podľa nosenia", "zachytáva peľ na ramenách"),
            ("softshell alebo bunda", "čistiť podľa membrány", "nie každodenné pranie"),
            ("obliečky alergika", "prať pravidelnejšie", "najdlhší kontakt"),
        ],
        "sections": [
            ("Čo urobiť po príchode zvonka", "Po návrate domov nedávajte bundu, mikinu ani tašku na posteľ. Ideálne je vytvoriť peľovú hranicu v predsieni: vonkajšie vrstvy zostanú tam, vrstvy pri tele idú podľa potreby do koša na bielizeň.", "Ak je peľ viditeľný, najprv ho odstráňte nasucho. Mokré roztieranie môže častice vtlačiť do textilu."),
            ("Ako prať oblečenie pri peľovej alergii", "Perte hlavne textílie, ktoré boli pri tele, spotili sa alebo prichádzajú do kontaktu s tvárou a krkom. Tričká, pyžamo, šály a uteráky sú dôležitejšie než bunda, ktorú ste mali krátko vonku.", "Pri citlivej pokožke a nose nepoužívajte zbytočne veľa gélu ani silnú vôňu. Dôležitý je dobrý oplach."),
            ("Peľ v spálni: oblečenie, vlasy a posteľná bielizeň", "Najväčší rozdiel často neurobí pranie bundy, ale zákaz ukladať vonkajšie oblečenie na posteľ. Peľ sa vie preniesť z ramien, vlasov a rukávov na vankúš.", "Súvisiaci návod je <a href=\"/n/ako-prat-postel-alergika-obliecky-vankuse-a-matracovy-chranic\">ako prať posteľ alergika</a>. Počas sezóny má zmysel meniť obliečky pravidelnejšie."),
            ("Sušenie bielizne počas peľovej sezóny", "Ak má niekto doma silnú peľovú alergiu, sušenie obliečok a uterákov vonku počas vysokej peľovej záťaže môže problém zhoršiť. Bielizeň síce vonia sviežo, ale zároveň môže zachytiť nové častice.", "Praktickejšie je sušenie vnútri pri dobrom vetraní alebo v sušičke, ak to štítok povoľuje."),
            ("Ako často prať pri peľovej alergii", "Nie všetko treba prať denne. Zamerajte sa na textílie, s ktorými alergik spí, utiera si tvár alebo ich nosí priamo na tele. Vrchné vrstvy stačí čistiť podľa expozície.", "Ak sa príznaky zhoršujú večer alebo v noci, hľadajte peľ najmä v spálni, nie iba v skrini."),
            ("Ako prať bundu alebo softshell pri alergii", "Softshell a membránové bundy neperte len preto, že boli vonku. Najprv ich vytraste, prejdite valčekom a nechajte mimo spálne. Perte až pri špine, pachu alebo podľa potreby.", "Podrobnejší postup je v návode <a href=\"/n/ako-prat-softshell-bundu-a-nohavice-bez-poskodenia-membrany\">ako prať softshell bundu a nohavice</a>."),
        ],
        "expert_title": "Odbornejší pohľad: peľ, textil a domáce zóny",
        "expert_p1": "Peľové častice sa zachytávajú na povrchoch a textíliách. Pri alergikovi preto nerozhoduje iba pranie, ale aj to, kde sa oblečenie po príchode odloží a či sa peľ prenesie do spálne.",
        "expert_p2": "Americké CDC pri peli odporúča po pobyte vonku meniť oblečenie a sprchovať sa, aby sa peľ odstránil z pokožky a vlasov. To podporuje jednoduchú domácu rutinu: predsieň, prezlečenie, pranie vrstiev pri tele a ochrana postele.",
        "source_html": '<p>Viac k zdravotnému kontextu peľu uvádza <a rel="noopener" href="https://www.cdc.gov/climate-health/php/effects/pollen-health.html" target="_blank">CDC: Pollen and Your Health</a>.</p>',
        "checklist": "Pred praním skontrolujte, či oblečenie bolo v tráve alebo pri kvitnúcich stromoch, či je spotené, či prišlo do kontaktu s tvárou, či sa dostalo do spálne, či sa bude sušiť vonku a či má alergik citlivý nos na silné vône.",
        "rule": "Pri peľovej alergii perte hlavne vrstvy pri tele a chráňte spálňu; vrchné vrstvy najprv vytriasť a držať mimo postele.",
        "recommendation_intro": "Pri textíliách alergika má zmysel šetrný prací gél, dobrý oplach a rozumné dávkovanie bez prehnanej vône.",
        "product_text": "Vhodný na pravidelné pranie tričiek, pyžama, obliečok a ďalších textílií, ktoré sú pri peľovej alergii v dlhom kontakte s pokožkou.",
        "category_text": "Pri alergickej sezóne vyberajte pracie gély podľa citlivosti pokožky, oplachu a toho, ako často periete textílie pri tele.",
        "links": [
            ("/n/ako-striast-pel-z-bundy-a-mikiny-po-prechadzke-pred-pranim", "Ako striasť peľ z bundy a mikiny pred praním"),
            ("/n/ako-prat-postel-alergika-obliecky-vankuse-a-matracovy-chranic", "Ako prať posteľ alergika"),
            ("/n/ako-casto-prat-postelne-pradlo", "Ako často prať posteľné prádlo"),
        ],
        "faq": [
            ("Musím pri peľovej alergii prať oblečenie po každom pobyte vonku?", "Nie vždy. Pravidelnejšie perte hlavne spotené vrstvy pri tele a textílie, ktoré sa dostali do spálne."),
            ("Je vhodné sušiť obliečky alergika vonku?", "Počas silnej peľovej sezóny to môže preniesť peľ späť na čistú bielizeň."),
            ("Pomôže silná vôňa do prania?", "Nie. Pri alergii je dôležitejšie odstránenie peľu, dobrý oplach a mierne dávkovanie."),
        ],
    },
    "ski_wax": {
        "marker": "Detailnejší postup na lyžiarsky vosk na bunde a rukaviciach",
        "problem": "Lyžiarsky vosk na bunde alebo rukaviciach je mastná škvrna s pridaným rizikom funkčného textilu. Pri bežnom oblečení sa niekedy používa teplo, ale lyžiarske bundy, rukavice, membrány, laminácie a povrchové úpravy môžu teplo alebo rozpúšťadlá znášať zle.",
        "scope": "lyžiarsku bundu, rukavice s membránou, manžety, vrecká, lemy, softshellové časti, zateplenie, voskový film, mastný tieň a následné pranie bez poškodenia impregnácie",
        "avoid": "horúcu žehličku na membránu, agresívne rozpúšťadlá bez testu, pranie celej bundy kvôli malej škvrne, sušenie pred kontrolou mastnoty a drhnutie zateplených rukavíc",
        "diagnosis": [
            "<strong>Vosk najprv stuhnúť:</strong> prebytok ide dole lepšie tupou kartou.",
            "<strong>Teplo je riziko:</strong> pri membráne a laminácii nežehlite naslepo.",
            "<strong>Mastný tieň kontrolujte pred sušením:</strong> teplo ho môže zvýrazniť.",
            "<strong>Rukavice čistite tvarovo opatrne:</strong> výplň a membrána schnú pomaly.",
        ],
        "state_rows": [
            ("hrubá vrstva vosku", "nechať stuhnúť a odobrať kartou", "bez drhnutia"),
            ("mastný tieň", "lokálne prepracovať gélom", "test na skrytom mieste"),
            ("membránová bunda", "nežehliť horúco", "riziko laminácie"),
            ("rukavice", "lokálne čistenie a pomalé sušenie", "výplň schne dlho"),
        ],
        "textile_rows": [
            ("lyžiarska bunda", "lokálne predčistiť, prať podľa štítku", "membrána a DWR"),
            ("rukavice s membránou", "čistiť povrchovo a sušiť pomaly", "tvar a výplň"),
            ("softshell", "jemné pranie bez aviváže", "povrchová úprava"),
            ("podšívka", "nepremáčať zbytočne", "pomalé schnutie"),
        ],
        "sections": [
            ("Ako odstrániť vosk na lyže z bundy", "Vosk nechajte stuhnúť. Potom tupou kartou alebo hranou lyžičky odoberte iba vrchnú vrstvu. Netlačte tak silno, aby ste vosk zatlačili do tkaniny alebo poškodili povrch.", "Zvyšný mastný tieň jemne prepracujte kvapkou pracieho gélu. Ak ide o membránovú bundu, držte sa štítku a nepoužívajte univerzálne rozpúšťadlá bez testu."),
            ("Vosk na lyžiarskych rukaviciach", "Rukavice čistite ešte opatrnejšie než bundu. Majú výplň, podšívku, švy a často membránu. Premočenie celej rukavice kvôli malej škvrne môže spôsobiť dlhé schnutie a zápach.", "Lokálne miesto pretrite, odstráňte prebytok vosku a nechajte rukavice schnúť otvorené pri izbovej teplote."),
            ("Prečo nežehlíme funkčnú bundu naslepo", "Pri obyčajnom obruse môže papier a žehlička niekedy pomôcť vytiahnuť vosk. Pri lyžiarskej bunde je to rizikové, pretože teplo môže ovplyvniť lamináciu, potlač, zipsy, lepené švy alebo vodoodpudivú úpravu.", "Ak výrobca výslovne neuvádza tepelnú obnovu, neaplikujte horúcu žehličku priamo na škvrnu."),
            ("Ako prať po lokálnom odstránení vosku", "Keď je vosk mechanicky odstránený, perte iba vtedy, ak to štítok povoľuje a škvrna alebo pach to vyžaduje. Použite primerané množstvo gélu, nepreplňte bubon a zvoľte program pre funkčné textílie.", "Súvisiaci postup nájdete v článku <a href=\"/n/ako-prat-softshell-bundu-a-nohavice-bez-poskodenia-membrany\">ako prať softshell bundu a nohavice</a>."),
            ("Impregnácia po praní", "Ak sa po praní voda do bundy vpíja namiesto toho, aby sa perličkovala na povrchu, môže byť potrebné obnoviť vodoodpudivú úpravu. Nerobte to však automaticky pred odstránením mastnoty.", "Viac k téme je v návode <a href=\"/n/ako-obnovit-impregnaciu-softshellu-po-prani-a-kedy-ju-neriesit\">ako obnoviť impregnáciu softshellu po praní</a>."),
            ("Kedy zvoliť čistiareň alebo servis", "Ak je bunda drahá, má podlepené švy, bledú farbu alebo je škvrna veľká, domáce experimenty s rozpúšťadlami sú riziko. Profesionálne čistenie je rozumnejšie než poškodená membrána.", "Pri rukaviciach s kožou, membránou alebo kombinovanými materiálmi je servis často bezpečnejší než celkové pranie."),
        ],
        "expert_title": "Odbornejší pohľad: vosk, mastnota a funkčné vrstvy",
        "expert_p1": "Lyžiarsky vosk je hydrofóbny a na textile sa správa ako mastná vrstva. Prací program ho nemusí odstrániť, ak predtým neodoberiete prebytok a neošetríte mastný tieň lokálne.",
        "expert_p2": "Pri lyžiarskej výbave sa navyše rieši aj chemické zloženie voskov a ochrana pri práci s nimi. Odborné práce o voskoch upozorňujú najmä na expozíciu pri nanášaní a úprave voskov, čo je iná situácia než malá škvrna na oblečení, ale pripomína význam vetrania a opatrnosti pri teple.",
        "source_html": '<p>K širšiemu kontextu lyžiarskych voskov a fluorovaných látok pozri napríklad prehľad <a rel="noopener" href="https://pmc.ncbi.nlm.nih.gov/articles/PMC10907454/" target="_blank">PFAS exposure in ski waxing</a>.</p>',
        "checklist": "Pred praním skontrolujte typ bundy, membránu, štítok, veľkosť voskovej vrstvy, mastný tieň, zipsy, lepené švy, rukavice s výplňou, možnosť lokálneho čistenia a potrebu obnovy impregnácie.",
        "rule": "Pri lyžiarskom vosku najprv stuhnúť a odobrať prebytok, potom lokálne riešiť mastnotu a až nakoniec prať podľa štítku.",
        "recommendation_intro": "Pri prateľnej lyžiarskej výbave používajte prací gél až po mechanickom odstránení vosku a kontrole materiálu.",
        "product_text": "Vhodný na následné šetrné pranie prateľných textilných častí po lokálnom ošetrení mastného tieňa, ak to štítok výrobcu povoľuje.",
        "category_text": "Pri funkčných bundách a rukaviciach vyberajte prací gél podľa materiálu, oplachu a kompatibility so štítkom výrobcu.",
        "links": [
            ("/n/ako-prat-softshell-bundu-a-nohavice-bez-poskodenia-membrany", "Ako prať softshell bundu a nohavice"),
            ("/n/ako-odstranit-sol-a-mokry-sneh-z-lyziarskych-rukavic-s-membranou", "Ako odstrániť soľ a mokrý sneh z lyžiarskych rukavíc"),
            ("/n/ako-obnovit-impregnaciu-softshellu-po-prani-a-kedy-ju-neriesit", "Ako obnoviť impregnáciu softshellu po praní"),
        ],
        "faq": [
            ("Môžem použiť žehličku na vosk na lyžiarskej bunde?", "Bez pokynu výrobcu nie. Pri membráne a laminácii je horúca žehlička riziková."),
            ("Ako dostať vosk z rukavíc?", "Nechajte ho stuhnúť, odoberte prebytok a čistite lokálne bez premočenia výplne."),
            ("Treba po praní obnoviť impregnáciu?", "Iba ak povrch po praní prestal odpudzovať vodu a výrobca to povoľuje."),
        ],
    },
    "paper_tissue": {
        "marker": "Detailnejší postup po papierovej vreckovke v práčke",
        "problem": "Papierová vreckovka v práčke je mechanický problém, nie škvrna. Kúsky papiera sa zachytia na tmavom povrchu, vo švoch, vo vreckách, na mikine, v tesnení a niekedy aj vo filtri. Ak začnete ďalším plným praním bez prípravy, papier sa iba znovu roznesie.",
        "scope": "čierne nohavice, mikinu, tmavé tričká, vrecká, švy, kapucňu, bubon práčky, tesnenie, filter, valček na textil, opakované opláchnutie a prevenciu pred ďalším praním",
        "avoid": "trhanie mokrých kúskov prstami po celej látke, sušenie horúco bez kontroly, ďalšiu plnú dávku, zanedbanie tesnenia práčky a miešanie papierom obalených vecí s čistou bielizňou",
        "diagnosis": [
            "<strong>Najprv vysušiť alebo preschnúť:</strong> suchý papier ide z tmavého textilu ľahšie.",
            "<strong>Vytriasť mimo práčky:</strong> inak sa kúsky vrátia do bubna.",
            "<strong>Valček pred ďalším praním:</strong> práčka nemá odstraňovať celý nános papiera.",
            "<strong>Skontrolovať bubon a tesnenie:</strong> ďalšia dávka sa môže zašpiniť znova.",
        ],
        "state_rows": [
            ("mokré kúsky papiera", "nechať preschnúť", "menej rozmazania"),
            ("papier na čiernej mikine", "valček alebo jemná kefa", "po vytrasení"),
            ("papier vo vreckách", "vybrať ručne pred praním", "zdroj problému"),
            ("zvyšky v práčke", "utrieť bubon a tesnenie", "ochrana ďalšej dávky"),
        ],
        "textile_rows": [
            ("čierne nohavice", "vytriasť, valčekovať, krátko opláchnuť", "viditeľné zvyšky"),
            ("mikina s vlasom", "kefa jedným smerom", "papier sa drží v povrchu"),
            ("jemný úplet", "bez tvrdého drhnutia", "riziko žmolkov"),
            ("bubon práčky", "utrieť a skontrolovať filter", "zvyšky sa vracajú"),
        ],
        "sections": [
            ("Papierová vreckovka v práčke: prvá pomoc", "Keď otvoríte práčku a vidíte kúsky papiera, nevracajte hneď celú dávku na ďalší program. Vyberte veci, vytraste ich nad vaňou alebo vonku a nechajte ich preschnúť.", "Mokré kúsky sa trhajú a rozmazávajú. Suchšie zvyšky sa dajú odstrániť valčekom, kefou alebo jemným pretrepaním."),
            ("Ako dostať kúsky papiera z čiernych nohavíc", "Čierne nohavice prejdite valčekom na textil alebo lepiacou kefou. Začnite veľkými plochami a až potom riešte švy, vrecká a pás. Ak majú nohavice vlas alebo elastan, netlačte príliš silno.", "Po mechanickom odstránení môžete zvoliť krátke opláchnutie alebo jemné pranie menšej dávky."),
            ("Papier na mikine a v kapucni", "Mikina zachytáva papier v kapucni, manžetách a vnútornom vlase. Preto ju nestačí pretriasť raz. Otočte ju naruby, vyčistite vrecká a prejdite miesta s najväčším nánosom.", "Ak papier zostal v kapucni, pri ďalšom praní sa roznesie späť na celý kus."),
            ("Čo urobiť s práčkou po papierovej vreckovke", "Utrite bubon, tesnenie a dvierka. Skontrolujte, či kúsky papiera nezostali v záhybe gumy. Ak bola vreckovka veľká, po niekoľkých dávkach sledujte aj filter.", "Súvisiaci postup je <a href=\"/n/ako-vycistit-filter-pracky-ked-bielizen-zapacha-alebo-voda-odteka-pomaly\">ako vyčistiť filter práčky</a>."),
            ("Kedy spustiť ďalšie pranie", "Ďalšie pranie má zmysel až vtedy, keď ste odstránili väčšinu papiera nasucho. Inak len miešate papier s vodou a textilom. Perte menšiu dávku, aby mal papier kam odísť.", "Pri tmavej bielizni pomáha aj dobrý oplach a nepreplnený bubon, podobne ako pri bielych šmuhách od prášku."),
            ("Prevencia: kontrola vreciek bez nervov", "Najjednoduchšia prevencia je samostatný krok pred práčkou: vrecká, kapucne, rukávy, servítky a papieriky. Pri detskom oblečení a mikinách kontrolujte aj skryté vrecká.", "Ak sa to opakuje, dajte pri koši na bielizeň malú nádobu na nálezy z vreciek."),
        ],
        "expert_title": "Odbornejší pohľad: prečo voda papier nevyrieši",
        "expert_p1": "Papierová vreckovka sa vo vode rozpadá na vláknité kúsky. Tie sa potom zachytávajú na tmavých a vlasových povrchoch. Preto je mechanická fáza dôležitejšia než ďalšie náhodné pranie.",
        "expert_p2": "Pri čiernom oblečení navyše vidno každý svetlý zvyšok. Podobne ako pri pracom prášku na tmavom textile pomáha menšia dávka, viac priestoru v bubne a kontrola pred sušením.",
        "source_html": '<p>Pri odstraňovaní zvyškov z textilu je dôležité rozlišovať mechanické nečistoty a chemické škvrny. Praktický prehľad typov škvŕn ponúka <a rel="noopener" href="https://extension.illinois.edu/global/stain-solutions" target="_blank">Illinois Extension Stain Solutions</a>.</p>',
        "checklist": "Pred ďalším praním skontrolujte vrecká, kapucňu, manžety, švy, čierne plochy, bubon, tesnenie, filter, veľkosť dávky a to, či väčšina papiera už odišla nasucho.",
        "rule": "Po papierovej vreckovke najprv vytriasť a odstrániť zvyšky nasucho, potom prať menšiu dávku a vyčistiť práčku.",
        "recommendation_intro": "Prací gél má význam až pri opakovanom praní po mechanickom odstránení kúskov papiera.",
        "product_text": "Vhodný na následné pranie tmavých nohavíc, mikín a bežnej bielizne po tom, čo je väčšina papiera odstránená z povrchu.",
        "category_text": "Pri opakovanom praní po papierovej vreckovke vyberajte prací gél, ktorý sa dobre vyplachuje a nezanecháva ďalšie viditeľné zvyšky.",
        "links": [
            ("/n/ako-odstranit-biele-smuhy-od-pracieho-prasku-z-cierneho-oblecenia", "Ako odstrániť biele šmuhy z čierneho oblečenia"),
            ("/n/ako-vycistit-filter-pracky-ked-bielizen-zapacha-alebo-voda-odteka-pomaly", "Ako vyčistiť filter práčky"),
            ("/n/ako-prat-cierne-oblecenie-aby-nebledlo-a-nebolo-flakate", "Ako prať čierne oblečenie"),
        ],
        "faq": [
            ("Mám dať oblečenie s papierom hneď znova prať?", "Nie hneď. Najprv odstráňte čo najviac papiera vytrasením, valčekom alebo kefou."),
            ("Pomôže sušička?", "Len ak ju povoľuje štítok a najprv ste odstránili väčšinu papiera. Pri citlivých textíliách postupujte opatrne."),
            ("Treba čistiť práčku?", "Áno, aspoň bubon a tesnenie. Pri veľkom množstve papiera sledujte aj filter."),
        ],
    },
    "super_glue": {
        "marker": "Detailnejší postup na sekundové lepidlo na textile",
        "problem": "Sekundové lepidlo, teda kyanoakrylátové lepidlo, rýchlo tvrdne a dokáže spojiť vlákna do tvrdej plochy. Domáce čistenie je rizikové hlavne pri jemných látkach, farbených materiáloch, elastane, koži, saku alebo drahom oblečení.",
        "scope": "sekundové lepidlo na nohaviciach, tričku, saku, bavlne, polyesteri, elastane, jemnej blúzke, detskom oblečení, tvrdý film, acetón, test na skrytom mieste a rozhodnutie, kedy ísť do čistiarne",
        "avoid": "trhanie lepidla aj s vláknami, acetón bez testu, horúcu vodu pred diagnostikou, drhnutie jemných látok, sušenie a žehlenie tvrdého miesta a domáce pokusy na drahom kuse",
        "diagnosis": [
            "<strong>Nechajte lepidlo vytvrdnúť:</strong> rozmazané lepidlo zväčší plochu.",
            "<strong>Test je povinný:</strong> acetón môže poškodiť farbu, elastan aj povrch.",
            "<strong>Tvrdý film nie je bežná škvrna:</strong> pranie ho samo nevyrieši.",
            "<strong>Drahý kus riešte profesionálne:</strong> najmä sako, hodváb, vlna alebo zmes s elastanom.",
        ],
        "state_rows": [
            ("čerstvé lepidlo", "nechať stabilizovať, nerozotierať", "zmenšiť plochu"),
            ("tvrdý prebytok", "opatrne odobrať tupou hranou", "bez trhania vlákien"),
            ("farebný textil", "test na skrytom mieste", "riziko vyblednutia"),
            ("jemný alebo drahý kus", "čistiareň", "nižšie riziko škody"),
        ],
        "textile_rows": [
            ("bavlna", "opatrný test a lokálne čistenie", "znesie viac než jemné látky"),
            ("polyester alebo elastan", "veľmi opatrne s rozpúšťadlami", "riziko povrchu a pružnosti"),
            ("sako alebo vlna", "profesionálne čistenie", "tvar a výstuž"),
            ("detské oblečenie", "odstrániť chemické zvyšky pred praním", "kontakt s pokožkou"),
        ],
        "sections": [
            ("Ako odstrániť sekundové lepidlo z textilu", "Najprv zastavte zväčšovanie škvrny. Lepidlo nerozotierajte prstami ani mokrou handrou. Nechajte ho vytvrdnúť a potom skúste odstrániť len povrchový prebytok tupou hranou.", "Cieľom nie je vytrhnúť tvrdú časť za každú cenu, ale znížiť objem lepidla bez poškodenia vlákien."),
            ("Acetón na sekundové lepidlo: kedy áno a kedy nie", "Acetón môže kyanoakrylát oslabiť, ale zároveň môže poškodiť farbu, povrch, elastan, acetátové vlákna alebo potlač. Preto sa nesmie používať plošne bez testu.", "Ak test na skrytom mieste zmení farbu alebo povrch, doma nepokračujte. Pri drahšom kuse je lacnejšia čistiareň než zničený textil."),
            ("Sekundové lepidlo na nohaviciach", "Pri bežných nohaviciach najprv odoberte tvrdý prebytok a otestujte lokálny postup. Potom perte až vtedy, keď je miesto zmäkčené alebo stabilné. Práčka sama tvrdý film väčšinou neodstráni.", "Pred sušením skontrolujte, či miesto nie je stále tvrdé. Teplo môže zvyšok zafixovať alebo zmeniť povrch látky."),
            ("Sekundové lepidlo na saku alebo jemnej blúzke", "Sako, blúzka, vlna, hodváb, viskóza a zmesi s elastanom sú rizikové. Domáce rozpúšťadlo môže poškodiť viac než samotná škvrna. Pri takomto kúsku je rozumné zastaviť sa po odstránení povrchového prebytku.", "Súvisiaci text je <a href=\"/n/ako-prat-sako-doma-a-kedy-ho-radsej-dat-do-cistiarne\">ako prať sako doma a kedy zvoliť čistiareň</a>."),
            ("Pranie po odstránení lepidla", "Keď je tvrdý zvyšok preč alebo aspoň stabilizovaný, vyperte textil podľa štítku. Použite primerané množstvo gélu, dobrý oplach a menšiu dávku, aby sa zvyšky z lokálneho čistenia vyplavili.", "Ak ste použili akékoľvek rozpúšťadlo, textil pred praním nechajte vyvetrať podľa bezpečného postupu a neperte ho spolu s citlivou bielizňou."),
            ("Kedy to doma nerobiť", "Nepokračujte doma, ak sa mení farba, povrch lepkavo mäkne, vlákna sa trhajú alebo ide o drahý kus. To už nie je bežná škvrna, ale riziko poškodenia materiálu.", "Pri detskom oblečení sledujte aj to, aby na textile nezostali chemické zvyšky. Čistota nesmie byť na úkor bezpečnosti pri kontakte s pokožkou."),
        ],
        "expert_title": "Odbornejší pohľad: kyanoakrylát a rozpúšťadlá",
        "expert_p1": "Sekundové lepidlá sú založené na kyanoakrylátoch, ktoré rýchlo polymerizujú a vytvárajú tvrdý spoj. Na textile to znamená, že nejde o rozpustnú škvrnu ako nápoj alebo bežná špina, ale o tvrdý materiál medzi vláknami.",
        "expert_p2": "Zdravotnícke a toxikologické zdroje pri sekundovom lepidle upozorňujú na opatrnosť s pokožkou, očami a odstraňovaním. Pri textile je princíp podobný: neodtrhávať násilne a pri chemickom postupe počítať s rizikom poškodenia povrchu.",
        "source_html": '<p>Bezpečnostný kontext sekundového lepidla zhrňuje napríklad <a rel="noopener" href="https://www.poison.org/articles/super-glue" target="_blank">Poison Control: Super Glue</a>.</p>',
        "checklist": "Pred čistením skontrolujte typ textilu, farbu, elastan, potlač, tvrdosť lepidla, veľkosť plochy, možnosť testu, hodnotu oblečenia, kontakt s pokožkou a to, či je rozumnejšia čistiareň.",
        "rule": "Pri sekundovom lepidle netrhať, najprv nechať vytvrdnúť, odobrať prebytok, otestovať postup a pri rizikovom textile zastaviť.",
        "recommendation_intro": "Prací gél používajte až po lokálnom vyriešení tvrdého lepidla; pranie má odstrániť zvyšky čistenia, nie nahradiť opatrný prvý krok.",
        "product_text": "Vhodný na následné pranie bežných prateľných textílií po bezpečnom lokálnom ošetrení a kontrole štítku.",
        "category_text": "Pri škvrnách po lepidle vyberajte prací gél podľa materiálu a potreby dobrého oplachu po lokálnom čistení.",
        "links": [
            ("/n/ako-odstranit-lepidlo-z-oblecenia-po-tvoreni-s-detmi", "Ako odstrániť lepidlo z oblečenia po tvorení s deťmi"),
            ("/n/ako-prat-sako-doma-a-kedy-ho-radsej-dat-do-cistiarne", "Ako prať sako doma"),
            ("/n/ako-vyprat-stare-skvrny-kompletny-sprievodca-pre-ciste-oblecenie", "Ako vyprať staré škvrny"),
        ],
        "faq": [
            ("Môžem použiť acetón na sekundové lepidlo z oblečenia?", "Iba po teste na skrytom mieste. Acetón môže poškodiť farbu, elastan, potlač alebo jemný materiál."),
            ("Má ísť textil hneď do práčky?", "Nie. Najprv treba riešiť tvrdý prebytok lepidla. Práčka ho zvyčajne sama neodstráni."),
            ("Kedy ísť do čistiarne?", "Pri saku, vlne, hodvábe, drahom kuse, veľkej škvrne alebo ak test mení farbu či povrch."),
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
        <p>Pri tejto téme sa oplatí pozerať na celý systém starostlivosti: {config["scope"]}. Najväčšie riziko je {config["avoid"]}. Správny postup preto nezačína automaticky pracím programom, ale diagnostikou, oddelením rizikového kusu a kontrolou materiálu.</p>
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
        <p>Ak si nie ste istí, zvoľte menší zásah: mechanicky odstrániť prebytok, otestovať nenápadné miesto, nepremáčať zbytočne celý kus a až potom prať. Pri funkčnej výbave, alergickom režime, lepidle alebo mastnom vosku môže byť predpríprava dôležitejšia než samotný prací program.</p>
        <h2>Ako zvoliť pranie, lokálne čistenie alebo pauzu</h2>
        <p>Pranie je vhodné vtedy, keď je textil prateľný, problém nie je iba na povrchu a potrebujete odstrániť pot, pach alebo zvyšky čistenia. Lokálne čistenie je vhodnejšie pri malej škvrne, funkčnej bunde, rukaviciach, saku alebo materiáli, ktorý by celým praním zbytočne trpel.</p>
        <p>Pauza je tiež postup. Ak sa mení farba, povrch, pružnosť alebo tvar, ďalší tvrdší zásah môže poškodiť textil. Vtedy je lepšie skontrolovať štítok, návod výrobcu alebo zvoliť profesionálne čistenie.</p>
        <h2>Kedy textil nesušiť a neodkladať</h2>
        <p>Textil nesušte horúco, ak na ňom zostal mastný tieň, tvrdý zvyšok, papierový povlak, peľový nános alebo vlhké blato v švoch. Teplo vie problém zvýrazniť, zápach uzavrieť alebo škvrnu zafixovať.</p>
        <p>Pred odložením do skrine skontrolujte lemy, švy, vrecká, kapucňu, zipsy a vnútorné vrstvy. Povrch môže pôsobiť suchý, ale výplň, neoprén, rukavica alebo kapucňa môžu držať vlhkosť ešte dlho.</p>
        <h2>Domáca rutina pri opakovanom probléme</h2>
        <p>Ak sa podobná situácia opakuje, nastavte si jednoduchú rutinu: kontrola po príchode domov, odstránenie povrchovej špiny, oddelenie rizikového kusu, lokálne predčistenie, primeraná dávka gélu, nepreplnený bubon a kontrola pred sušením.</p>
        <p>Takto sa z prania nestane náhodný pokus. Pri športovej výbave, peľovej sezóne, lyžiarskom vosku, papierovej vreckovke aj sekundovom lepidle rozhoduje najmä to, čo urobíte pred zapnutím práčky.</p>
        <h2>Malý test pred väčším zásahom</h2>
        <p>Pred silnejším čistením si vyberte nenápadné miesto: vnútorný lem, rubovú stranu, šev alebo malú časť pri vrecku. Otestujte vodu, prací roztok alebo mechanické trenie tam, kde prípadná zmena nebude viditeľná.</p>
        <p>Ak sa farba púšťa, povrch sa leskne, materiál sa naťahuje alebo zostáva lepkavý či tvrdý, nepokračujte rovnakou silou na viditeľnej ploche. Tento krátky test šetrí textil aj čas pri ďalšom praní.</p>
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
        if "Malý test pred väčším zásahom" not in cleaned:
            addition = clean(
                """
                <h2>Malý test pred väčším zásahom</h2>
                <p>Pred silnejším čistením si vyberte nenápadné miesto: vnútorný lem, rubovú stranu, šev alebo malú časť pri vrecku. Otestujte vodu, prací roztok alebo mechanické trenie tam, kde prípadná zmena nebude viditeľná.</p>
                <p>Ak sa farba púšťa, povrch sa leskne, materiál sa naťahuje alebo zostáva lepkavý či tvrdý, nepokračujte rovnakou silou na viditeľnej ploche. Tento krátky test šetrí textil aj čas pri ďalšom praní.</p>
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
    parser = argparse.ArgumentParser(description="Conservatively expand VEVO retrofit wave 27 cycling/pollen/wax/tissue/glue articles.")
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
                "wave": "retrofit-wave-27-cycling-pollen-wax-tissue-glue-five",
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
                "wave": "retrofit-wave-27-cycling-pollen-wax-tissue-glue-five",
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
