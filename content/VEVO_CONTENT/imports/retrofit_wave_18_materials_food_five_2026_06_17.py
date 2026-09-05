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
OUT_JSON = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-18-materials-food-five-2026-06-17.json"
MCP_RESULTS = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-18-materials-food-five-2026-06-17-mcp-results.json"


ARTICLES = [
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-25-2026-06-16-articles.json",
        "slug": "preco-sa-oblecenie-zrazi-po-prani-teplota-vlakna-susicka-a-prevencia",
        "post_id": "2252",
        "url": "https://www.vevo.sk/n/preco-sa-oblecenie-zrazi-po-prani-teplota-vlakna-susicka-a-prevencia",
        "topic": "shrinkage",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-24-2026-06-16-articles.json",
        "slug": "preco-sa-oblecenie-zmolkuje-vlakna-trenie-pranie-a-susenie",
        "post_id": "2248",
        "url": "https://www.vevo.sk/n/preco-sa-oblecenie-zmolkuje-vlakna-trenie-pranie-a-susenie",
        "topic": "pilling",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-25-2026-06-16-articles.json",
        "slug": "certifikaty-na-textile-oeko-tex-gots-recyklovane-vlakna-a-co-znamenaju-pri-prani",
        "post_id": "2254",
        "url": "https://www.vevo.sk/n/certifikaty-na-textile-oeko-tex-gots-recyklovane-vlakna-a-co-znamenaju-pri-prani",
        "topic": "certificates",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-20-2026-06-10-articles.json",
        "slug": "co-je-mikrovlakno-vyhody-nevyhody-savost-a-pranie",
        "post_id": "2229",
        "url": "https://www.vevo.sk/n/co-je-mikrovlakno-vyhody-nevyhody-savost-a-pranie",
        "topic": "microfiber",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-18-2026-06-10-articles.json",
        "slug": "ako-odstranit-vajicko-z-oblecenia-obrusu-a-kuchynskej-utierky",
        "post_id": "2196",
        "url": "https://www.vevo.sk/n/ako-odstranit-vajicko-z-oblecenia-obrusu-a-kuchynskej-utierky",
        "topic": "egg_stain",
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
        <p>Pri materiáloch, ktoré menia tvar, zachytávajú pach alebo potrebujú jemnejší prístup, je výber pracieho gélu rovnako dôležitý ako teplota a veľkosť dávky.</p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="/c/vevo-home-care/pranie/praci-gel">Pozrieť pracie gély</a></p>
        </div>
        </div>
        </div>
        """
    )


def build_related_links(links):
    items = "\n".join(f'<li><a href="{href}">{label}</a></li>' for href, label in links)
    items += '\n<li><a href="/n/ako-vybrat-praci-gel-podla-typu-bielizne">Ako vybrať prací gél podľa typu bielizne</a></li>'
    return f"<h2>Súvisiace návody na VEVO</h2>\n<ul>\n{items}\n</ul>"


TOPICS = {
    "shrinkage": {
        "marker": "Detailnejší pohľad na zrážanie oblečenia, vlákna a sušičku",
        "problem": "zrážanie oblečenia po praní nie je jedna chyba, ale súhra vlákna, konštrukcie látky, teploty, mechaniky a sušenia",
        "scope": "bavlnenom tričku, vlnenom svetri, viskózovej blúzke, ľanovej košeli, elastických legínach a zmesových materiáloch",
        "avoid": "kombinovať horúcu vodu, vysoké otáčky, preplnený bubon a sušičku bez kontroly štítku",
        "diagnosis": [
            "<strong>Rozlíšte zrazenie a skrútenie:</strong> kus môže byť kratší, užší alebo iba zdeformovaný.",
            "<strong>Materiál rozhoduje:</strong> vlna, viskóza, bavlna a zmesi reagujú rozdielne.",
            "<strong>Sušička je rizikový krok:</strong> teplo a pohyb môžu dokončiť zmenu tvaru.",
            "<strong>Prevencia je lacnejšia:</strong> raz zrazený kus sa nemusí dať vrátiť úplne späť.",
        ],
        "state_rows": [
            ("kus je kratší", "porovnať so švami a pôvodným strihom", "často ide o skutočné zrazenie"),
            ("kus je skrútený", "tvarovať za vlhka", "môže ísť o väzbu látky"),
            ("sveter je menší", "bez horúcej vody a sušičky", "vlna plstnatie"),
            ("viskóza je deformovaná", "sušiť naplocho alebo na ramienku podľa štítku", "mokrá je citlivá"),
        ],
        "textile_rows": [
            ("bavlna", "nižšia teplota a kontrola sušičky", "môže sa zraziť najmä prvé prania"),
            ("vlna", "ručný alebo vlnený program", "teplo a trenie spôsobia plstnatenie"),
            ("viskóza", "jemný program a šetrné sušenie", "mokré vlákno slabne"),
            ("zmesový materiál", "riadiť sa najcitlivejšou zložkou", "každé vlákno reaguje inak"),
        ],
        "sections": [
            ("Ako zistiť, či sa oblečenie naozaj zrazilo", "Najprv si všimnite, či sa zmenila dĺžka, šírka alebo iba tvar. Skrútený bočný šev pri tričku nie je to isté ako zrazený sveter. Pri košeli porovnajte golier, rukávy a dĺžku trupu, pri nohaviciach pás a vnútornú dĺžku.", "Ak sa zmenil iba tvar, môže pomôcť tvarovanie za vlhka. Ak sa zmenila veľkosť a vlákno sa stiahlo, návrat býva obmedzený."),
            ("Prečo sa bavlnené tričko zrazí", "Bavlna môže reagovať na teplo, pohyb a sušenie. Riziko rastie pri vyššej teplote, sušičke a pri lacnejšom úplete, ktorý nebol dobre stabilizovaný. Preto je dôležité čítať štítok a nové kúsky neprať hneď agresívne.", "K bavlne nadväzuje článok <a href=\"/n/co-je-bavlna-vlastnosti-vyhody-nevyhody-a-starostlivost\">čo je bavlna a ako sa o ňu starať</a>."),
            ("Vlna, merino a zrážanie svetra", "Vlnený sveter sa často nezrazí klasicky, ale splstnatí: vlákna sa teplom, vlhkosťou a trením zachytia do seba. Výsledkom je menší, tuhší a menej pružný kus. Tu je prevencia zásadná.", "Pri vlne používajte program na vlnu, nízku mechaniku a sušenie naplocho. Užitočný je aj návod <a href=\"/n/ako-prat-vlneny-sveter-ked-zapacha-po-noseni\">ako prať vlnený sveter</a>."),
            ("Viskóza, ľan a zmena tvaru", "Viskóza je za mokra citlivejšia a ľan sa prirodzene krčí. Pri týchto materiáloch niekedy nejde o trvalé zrazenie, ale o kombináciu pokrčenia, vytiahnutia alebo zlého sušenia. Preto ich po praní jemne vyrovnajte a nenechávajte zdeformované v bubne.", "Pri ľane pomáha súvisiaci článok <a href=\"/n/ako-prat-lanovu-koselu-aby-nezostala-tvrda-a-pokrcena\">ako prať ľanovú košeľu</a>."),
            ("Ako nastaviť pranie, aby sa oblečenie nezrazilo", "Základ je nízka až primeraná teplota, nepreplnený bubon, šetrné otáčky a sušenie podľa štítku. Pri neistote sa riaďte najcitlivejšou zložkou materiálu, nie najodolnejšou. Nový kus perte prvýkrát konzervatívne.", "Ak máte tendenciu prať všetko rovnako, riziko zrážania je vyššie práve pri zmesiach a jemných materiáloch."),
        ],
        "depth": [
            ("Sušička a zrážanie oblečenia", "Sušička kombinuje teplo, prúdenie vzduchu a pohyb. Pri niektorých materiáloch je to pohodlné, pri iných rizikové. Najmä vlna, viskóza, elastické kúsky a lacnejšie úplety môžu zmeniť tvar výraznejšie než pri voľnom sušení.", "Ak štítok sušičku nepovoľuje alebo si nie ste istí, nechajte kus vyschnúť voľne. Pri obľúbenom oblečení je bezpečnejší pomalší postup než rýchle zmenšenie."),
            ("Zrážanie verzus opotrebovanie", "Niekedy sa oblečenie zdá menšie, pretože sa skrútilo, stvrdlo alebo stratilo pružnosť. To nie je vždy čisté zrazenie. Príčinou môže byť preplnená práčka, zvyšky pracieho prostriedku, tvrdá voda alebo zlé sušenie.", "Ak je látka tvrdá alebo lepkavá, nadväzuje téma <a href=\"/n/preco-je-bielizen-po-prani-tvrda-alebo-lepkava-zvysky-gelu-davkovanie-a-oplach\">prečo je bielizeň po praní tvrdá alebo lepkavá</a>."),
        ],
        "expert_title": "Odbornejší pohľad: relaxácia vlákien, plstnatenie a stabilita úpletu",
        "expert_p1": "Textil si po výrobe nesie napätie vo vláknach a väzbe. Pri praní sa časť napätia uvoľní, vlákna napučia a mechanika bubna ich môže preskladať. Pri vlne sa pridáva plstnatenie, pri úpletoch stabilita očiek a pri zmesiach rozdielne správanie jednotlivých vlákien.",
        "rule": "Ak neviete, ako materiál zareaguje, perte prvýkrát šetrnejšie: nižšia teplota, menej mechaniky, primeraná dávka a žiadna sušička bez povolenia na štítku.",
        "recommendation_intro": "Pri prevencii zrážania je dôležité nepreháňať teplotu ani dávkovanie. Šetrný prací gél pomáha prať pri nižšej záťaži, keď rešpektujete štítok a materiál.",
        "product_text": "Vhodný na bežné pranie materiálov, pri ktorých chcete znížiť zbytočnú mechanickú a chemickú záťaž. Pri vlne a veľmi jemných kusoch vždy rozhoduje štítok.",
        "links": [
            ("/n/preco-sa-oblecenie-zmolkuje-vlakna-trenie-pranie-a-susenie", "Prečo sa oblečenie žmolkuje"),
            ("/n/co-je-zmesovy-material-preco-sa-oblecenie-zraza-alebo-sprava-inak-nez-cakate", "Čo je zmesový materiál"),
            ("/n/otacky-pri-odstredovani-ako-ovplyvnuju-vlhkost-krcenie-a-opotrebovanie-oblecenia", "Otáčky pri odstreďovaní"),
        ],
        "faq": [
            ("Dá sa zrazené oblečenie vrátiť späť?", "Niekedy len čiastočne, najmä ak ide o deformáciu. Pri skutočnom zrazení alebo plstnatení nemusí byť návrat úplný."),
            ("Je najväčší problém teplota?", "Teplota je dôležitá, ale spolu s mechanikou, sušičkou, vláknom a konštrukciou látky."),
            ("Ktoré materiály sú rizikové?", "Vlna, viskóza, niektoré úplety, zmesi a kúsky so zakázanou sušičkou na štítku."),
        ],
    },
    "pilling": {
        "marker": "Detailnejší pohľad na žmolkovanie, trenie a pranie bez zbytočného opotrebovania",
        "problem": "žmolkovanie vzniká hlavne trením, uvoľnením krátkych vlákien a ich zachytením do malých uzlíkov na povrchu látky",
        "scope": "svetroch, mikinách, teplákoch, dekách, legínach, tričkách a fleece materiáloch",
        "avoid": "prať drsné a jemné veci spolu, preplniť bubon a sušiť vysokou teplotou bez kontroly štítku",
        "diagnosis": [
            "<strong>Miesto prezradí príčinu:</strong> boky, podpazušie a rukávy žmolkujú od trenia.",
            "<strong>Krátke vlákna sa uvoľňujú ľahšie:</strong> nie každý materiál starne rovnako.",
            "<strong>Pranie nie je jediný vinník:</strong> veľa žmolkov vzniká už pri nosení.",
            "<strong>Odžmolkovač používajte opatrne:</strong> agresívne holenie skracuje životnosť látky.",
        ],
        "state_rows": [
            ("jemné chĺpky", "prať naruby a oddeliť drsné kusy", "začiatok žmolkovania"),
            ("viditeľné žmolky", "opatrne odstrániť mechanicky", "neťahať rukou"),
            ("žmolky na bokoch", "sledovať trenie tašky alebo bundy", "vznikajú pri nosení"),
            ("fleece alebo akryl", "šetrný program a nižšie trenie", "materiál je náchylnejší"),
        ],
        "textile_rows": [
            ("sveter", "prať naruby v ochrannom vrecku", "nižšie trenie"),
            ("fleece", "oddeliť od suchých zipsov a uterákov", "povrch sa ľahko chytá"),
            ("legíny", "prať naruby a bez drsných kusov", "trenie v rozkroku a bokoch"),
            ("bavlnené tričko", "nepreplniť bubon", "úplet sa menej trie"),
        ],
        "sections": [
            ("Prečo sa oblečenie žmolkuje aj pri správnom praní", "Žmolky nevznikajú iba v práčke. Často sa začnú tvoriť pri nosení, keď sa látka trie o bundu, tašku, bezpečnostný pás, sedačku alebo samu o seba. Pranie potom uvoľnené vlákna len zvýrazní.", "Preto sledujte, kde sa žmolky objavujú. Ak sú na bokoch a v podpazuší, príčina je skôr trenie pri nosení než jeden zlý prací cyklus."),
            ("Ako prať oblečenie proti žmolkovaniu", "Oblečenie perte naruby, zipsy a suché zipsy zapnite a jemné kusy oddeľte od uterákov, riflí a drsných materiálov. Bubon nepreplňte, pretože látky sa potom trú intenzívnejšie a horšie sa oplachujú.", "Pri citlivých kúskoch použite ochranné vrecko a nižšie otáčky. Cieľom nie je len čistota, ale aj menšia mechanická záťaž."),
            ("Ktoré materiály žmolkujú viac", "Akryl, niektoré zmesi, fleece a mäkké úplety môžu žmolkovať viac než hladké a pevne tkané materiály. Neznamená to automaticky zlú kvalitu, ale materiál si pýta iný režim nosenia a prania.", "Súvisí s tým článok <a href=\"/n/akryl-vs-vlna-zmolkovanie-teplo-zapach-a-starostlivost\">akryl vs vlna: žmolkovanie, teplo a starostlivosť</a>."),
            ("Ako používať odžmolkovač", "Odžmolkovač používajte na rovnej ploche, bez tlaku a iba na suchom textile. Nevracajte sa stále na jedno miesto, aby ste neoslabili povrch. Pri jemných svetroch najprv testujte na menej viditeľnej časti.", "Žmolky neťahajte rukou. Môžete vytiahnuť ďalšie vlákna a vytvoriť ešte väčší problém."),
            ("Prevencia pri sušení a skladovaní", "Sušenie vysokou teplotou a trenie v sušičke môže žmolkovanie zvýrazniť. Citlivé úplety sušte voľne a skladajte ich tak, aby sa netreli o drsné povrchy. Pri svetroch je lepšie skladanie než dlhé vešanie.", "Ak sa žmolky opakujú na tom istom mieste, skúste zmeniť aj nosenie: tašku na druhé rameno, hladšiu vrchnú vrstvu alebo menej drsný opasok."),
        ],
        "depth": [
            ("Žmolky verzus chlpy a prach", "Nie všetko, čo vidíte na povrchu, je žmolok. Chlpy, prach a papierové vlákna sa dajú odstrániť valčekom, žmolok je pevnejší uzlík z vlastných vlákien látky. Preto potrebuje iný prístup.", "Ak máte doma zviera alebo sa v praní rozpadla papierová vreckovka, najprv riešte cudzie vlákna, až potom samotné žmolkovanie."),
            ("Kedy je žmolkovanie signál opotrebovania", "Ak sa žmolky vracajú hneď po odstránení, povrch látky je už oslabený alebo sa stále vystavuje rovnakému treniu. Ďalšie agresívne holenie môže problém skôr urýchliť.", "Vtedy má zmysel upraviť pranie a nosenie, nie iba častejšie používať odžmolkovač."),
        ],
        "expert_title": "Odbornejší pohľad: krátke vlákna, pevnosť priadze a povrchová mechanika",
        "expert_p1": "Žmolok vzniká, keď sa voľné konce vlákien dostanú na povrch, trením sa zamotajú a zostanú uchytené na látke. Dôležitá je dĺžka vlákna, typ priadze, hustota väzby aj to, koľko mechaniky dostane textil pri nosení a praní.",
        "rule": "Proti žmolkovaniu pomáha najmä menšie trenie: prať naruby, oddeliť drsné kusy, nepreplniť bubon a citlivé veci nesušiť zbytočne agresívne.",
        "recommendation_intro": "Pri žmolkovaní nie je cieľom silnejšie pranie, ale šetrnejší režim s dobrým oplachom. Prací gél používajte primerane, aby sa textil nemusel zbytočne trieť v preplnenom bubne.",
        "product_text": "Vhodný na šetrné pranie bežných úpletov, mikín, tričiek a textílií, kde chcete znížiť zbytočné trenie a zvyšky pracieho prostriedku.",
        "links": [
            ("/n/co-je-fleece-hrejivost-zmolkovanie-a-starostlivost-pri-prani", "Čo je fleece: hrejivosť, žmolkovanie a starostlivosť"),
            ("/n/akryl-vs-vlna-zmolkovanie-teplo-zapach-a-starostlivost", "Akryl vs vlna: žmolkovanie, teplo, zápach a starostlivosť"),
            ("/n/preco-sa-oblecenie-zrazi-po-prani-teplota-vlakna-susicka-a-prevencia", "Prečo sa oblečenie zrazí po praní"),
        ],
        "faq": [
            ("Je žmolkovanie chyba prania?", "Niekedy áno, ale často vzniká aj nosením a trením. Pranie ho môže iba zvýrazniť."),
            ("Pomôže vyššia teplota?", "Nie. Vyššia teplota žmolkovanie nerieši a pri citlivých materiáloch môže pridať ďalšie poškodenie."),
            ("Ako často používať odžmolkovač?", "Len podľa potreby a jemne. Príliš časté alebo silné holenie oslabuje povrch látky."),
        ],
    },
    "certificates": {
        "marker": "Detailnejší pohľad na textilné certifikáty a čo znamenajú pri praní",
        "problem": "certifikát na textile hovorí o splnení určitého štandardu, ale automaticky neznamená, že oblečenie môžete prať akokoľvek",
        "scope": "bavlne, organickej bavlne, recyklovanom polyesteri, detskom textile, uterákoch, posteľnej bielizni a zmesových materiáloch",
        "avoid": "zameniť certifikát za prací štítok alebo predpokladať, že udržateľnejší materiál znesie agresívnejšie pranie",
        "diagnosis": [
            "<strong>Certifikát nie je návod na pranie:</strong> vždy čítajte aj symboly na štítku.",
            "<strong>OEKO-TEX rieši bezpečnosť látok:</strong> nehovorí sám o sebe o zrážaní.",
            "<strong>GOTS súvisí s organickým textilom:</strong> stále však rozhoduje konštrukcia výrobku.",
            "<strong>Recyklované vlákno má svoje limity:</strong> perte podľa materiálu a výrobcu.",
        ],
        "state_rows": [
            ("OEKO-TEX", "vnímať ako informáciu o testovaní", "nie ako povolenie horúceho prania"),
            ("GOTS", "sledovať organický pôvod a pravidlá výroby", "štítok prania ostáva rozhodujúci"),
            ("recyklovaný polyester", "prať ako polyester podľa štítku", "stále ide o syntetiku"),
            ("detský textil", "voliť šetrný oplach a primerané dávkovanie", "citlivá pokožka potrebuje čistý textil"),
        ],
        "textile_rows": [
            ("organická bavlna", "prať šetrne podľa štítku", "pôvod vlákna nemení základnú starostlivosť"),
            ("recyklovaný polyester", "nižšia teplota a dobrý oplach", "drží pach podobne ako polyester"),
            ("posteľná bielizeň", "kombinovať hygienu a štítok", "certifikát nenahrádza program"),
            ("uteráky", "nepreplniť bubon", "dôležitý je oplach a sušenie"),
        ],
        "sections": [
            ("Čo znamená OEKO-TEX pri praní", "OEKO-TEX je pre zákazníka signál, že textil prešiel určitým typom testovania. Pri domácej starostlivosti však stále rozhoduje štítok konkrétneho výrobku: teplota, bielenie, sušenie a žehlenie.", "Ak má tričko certifikát, neznamená to, že znesie sušičku alebo vysokú teplotu. Certifikát a prací symbol riešia rozdielne veci."),
            ("GOTS a organická bavlna v bežnej domácnosti", "Pri organickej bavlne ľudia často čakajú úplne inú starostlivosť. V praxi však stále ide o bavlnu, úplet alebo tkaninu, ktorá môže reagovať na teplo, trenie a sušičku. Rozdiel je skôr v pôvode a pravidlách výroby.", "Nadväzuje článok <a href=\"/n/organicka-bavlna-co-znamena-a-ci-sa-perie-inak-ako-bezna-bavlna\">organická bavlna a pranie</a>."),
            ("Recyklované vlákna a starostlivosť", "Recyklovaný polyester perte podľa toho, že ide o polyester. Môže držať pach, rýchlo schnúť a byť citlivý na vysoké teploty podobne ako bežný polyester. Recyklovaný pôvod nemení základné limity vlákna.", "Praktické porovnanie nájdete v článku <a href=\"/n/recyklovany-polyester-co-znamena-ake-ma-vyhody-a-ako-sa-on-starat\">recyklovaný polyester a starostlivosť</a>."),
            ("Certifikáty pri detskom textile a posteľnej bielizni", "Pri detskom textile a posteľnej bielizni má zmysel sledovať nielen certifikát, ale aj oplach, dávkovanie a sušenie. Zvyšky pracieho prostriedku alebo vlhkosť v textile môžu byť praktickejší problém než samotný nápis na etikete.", "Pri posteľnej bielizni kombinujte hygienu s materiálom a symbolmi prania."),
            ("Ako nakupovať textil podľa certifikátov bez sklamania", "Certifikát berte ako jednu časť rozhodnutia. Pozrite aj zloženie, gramáž, typ väzby, odporúčané pranie a to, ako budete kus používať. Jemná organická bavlna nie je pracovná tkanina a recyklovaný polyester nie je automaticky bez zápachu.", "Najlepšia starostlivosť vzniká až kombináciou: rozumný nákup, správny prací štítok a šetrná domáca rutina."),
        ],
        "depth": [
            ("Certifikát verzus materiálové zloženie", "Dve tričká s podobným certifikátom sa môžu pri praní správať rozdielne, ak majú inú gramáž, väzbu alebo prímes elastanu. Preto pri starostlivosti najprv čítajte zloženie a pracie symboly.", "Certifikát pomáha pri dôvere k výrobku, ale konkrétny režim prania určuje výrobca na štítku."),
            ("Prečo certifikát nevyrieši zrážanie ani žmolkovanie", "Zrážanie a žmolkovanie súvisia s vláknom, priadzou, väzbou, nosením a praním. Certifikát môže súvisieť s chemickou bezpečnosťou alebo pôvodom, ale nezaručí nulové opotrebovanie.", "Preto je vhodné prepojiť túto tému s článkami o zrážaní a žmolkovaní oblečenia."),
        ],
        "expert_title": "Odbornejší pohľad: štandard, výrobok a domáca starostlivosť sú tri rôzne vrstvy",
        "expert_p1": "Pri textile treba oddeliť štandard, ktorý posudzuje určitú vlastnosť alebo proces, od fyzikálneho správania hotového výrobku. Oblečenie v práčke reaguje podľa vlákna, priadze, väzby, farbenia, šitia a domáceho režimu prania.",
        "rule": "Certifikát berte ako užitočnú informáciu pri výbere, nie ako náhradu pracieho štítku. Pri praní vždy rozhoduje konkrétny materiál a symboly na výrobku.",
        "recommendation_intro": "Pri certifikovanom textile má zmysel prať šetrne a dôsledne oplachovať. Prací gél vyberajte podľa materiálu a citlivosti domácnosti, nie iba podľa nápisu na etikete.",
        "product_text": "Vhodný na šetrné bežné pranie certifikovaných aj necertifikovaných textílií, ak rešpektujete štítok, triedenie a primerané dávkovanie.",
        "links": [
            ("/n/organicka-bavlna-co-znamena-a-ci-sa-perie-inak-ako-bezna-bavlna", "Organická bavlna: čo znamená a či sa perie inak"),
            ("/n/recyklovany-polyester-co-znamena-ake-ma-vyhody-a-ako-sa-on-starat", "Recyklovaný polyester: čo znamená a ako sa oň starať"),
            ("/n/ako-citat-stitok-na-obleceni-material-symboly-prania-a-spravny-program", "Ako čítať štítok na oblečení"),
        ],
        "faq": [
            ("Znamená OEKO-TEX, že môžem prať na vyššej teplote?", "Nie. Teplotu určuje prací štítok konkrétneho výrobku."),
            ("Perie sa organická bavlna inak?", "Základ je podobný ako pri bavlne, ale vždy rozhoduje gramáž, väzba, farba a štítok."),
            ("Je recyklovaný polyester náročnejší na pranie?", "Perte ho ako polyester podľa štítku. Sledujte najmä pach, teplotu a dobrý oplach."),
        ],
    },
    "microfiber": {
        "marker": "Detailnejší pohľad na mikrovlákno, savosť a správne pranie",
        "problem": "mikrovlákno má veľmi jemnú štruktúru, vďaka ktorej dobre zachytáva prach, mastnotu a vodu, ale pri nesprávnom praní môže stratiť savosť aj mäkkosť",
        "scope": "utierkach z mikrovlákna, dekách, športových uterákoch, handričkách na upratovanie, poťahoch a ľahkých textíliách",
        "avoid": "aviváž, vysokú teplotu bez štítku, pranie s chlpatými uterákmi a sušenie, ktoré spevní jemné vlákna",
        "diagnosis": [
            "<strong>Savosť je hlavný test:</strong> ak voda ostáva na povrchu, vlákno môže byť obalené zvyškami.",
            "<strong>Aviváž je riziko:</strong> môže znížiť schopnosť mikrovlákna zachytávať vodu a prach.",
            "<strong>Oddelené pranie pomáha:</strong> mikrovlákno priťahuje chlpy a vlákna z iných textílií.",
            "<strong>Sušenie nepreháňať:</strong> vysoké teplo môže zmeniť povrch.",
        ],
        "state_rows": [
            ("handrička nesaje", "vyprať bez aviváže a dobre opláchnuť", "zvyšky obalia vlákna"),
            ("lepí sa prach", "prať oddelene od uterákov", "priťahuje voľné vlákna"),
            ("páchne po použití", "nenechať vlhké v koši", "rýchlo schne, ale musí vetrať"),
            ("stvrdlo po sušení", "znížiť teplotu a mechaniku", "kontrola štítku"),
        ],
        "textile_rows": [
            ("upratovacia handrička", "prať oddelene podľa znečistenia", "nesmie prenášať mastnotu"),
            ("deka z mikrovlákna", "šetrný program a dobrý oplach", "má zostať mäkká"),
            ("športový uterák", "bez aviváže a rýchlo vysušiť", "pach vzniká vo vlhku"),
            ("poťah", "skontrolovať štítok a zipsy", "konštrukcia môže byť citlivá"),
        ],
        "sections": [
            ("Čo je mikrovlákno v praxi", "Mikrovlákno je textil z veľmi jemných vlákien, ktoré vytvárajú veľkú plochu na zachytávanie nečistôt a vody. Preto sa používa na handričky, deky, uteráky aj športové doplnky. Rovnaká výhoda však znamená, že ľahko zachytí aj zvyšky z prania.", "Ak mikrovlákno stráca savosť, problém nemusí byť vek, ale povlak z aviváže, pracieho prostriedku alebo mastnoty."),
            ("Ako prať handričky z mikrovlákna", "Handričky na upratovanie neperte s bežnou jemnou bielizňou. Môžu obsahovať mastnotu, prach a zvyšky čistiacich prostriedkov. Najprv ich vytrieďte podľa použitia: kuchyňa, kúpeľňa, sklo a prach.", "Perte bez aviváže a s dobrým oplachom. Ak handrička slúžila na mastnotu, nesmie preniesť film na iné textílie."),
            ("Mikrovlákno a aviváž", "Aviváž môže mikrovlákno obaliť a znížiť jeho schopnosť sať alebo čistiť. Pri dekách sa môže zdať príjemná, ale pri handričkách a športových uterákoch je často kontraproduktívna. Ak chcete zachovať funkciu, vynechajte ju.", "Podobná logika platí aj pri funkčných materiáloch, kde je dôležitý odvod vlhkosti."),
            ("Ako prať deku z mikrovlákna, aby zostala mäkká", "Deka z mikrovlákna potrebuje priestor v bubne, primeranú dávku gélu a vzdušné sušenie. Preplnená práčka ju zle opláchne a po vyschnutí môže pôsobiť tvrdšie. Ak je veľká, perte ju samostatne.", "K podobnej téme nadväzuje návod <a href=\"/n/ako-prat-deku-z-mikroplysu-aby-zostala-hebka\">ako prať deku z mikroplyšu, aby zostala hebká</a>."),
            ("Mikrovlákno, pach a rýchle sušenie", "Mikrovlákno často rýchlo schne, ale ak ho necháte vlhké zvinuté alebo zatvorené v taške, pach sa vytvorí rýchlo. Po použití ho rozprestrite, nechajte preschnúť a až potom dajte do koša.", "Pri športových uterákoch a cestovných textíliách je to často rozdiel medzi sviežim a zatuchnutým výsledkom."),
        ],
        "depth": [
            ("Prečo mikrovlákno zachytáva aj to, čo nechcete", "Jemná štruktúra mikrovlákna je výborná na prach a vodu, ale rovnako ľahko zachytí chlpy, zvyšky papiera, mastnotu a povlak z aviváže. Preto sa oplatí prať ho oddelene a bez produktov, ktoré obalia vlákno.", "Ak ho periete s uterákmi, môže byť po praní plné voľných vlákien a stratiť hladký povrch."),
            ("Kedy mikrovlákno vymeniť", "Ak handrička ani po praní bez aviváže nesaje, zostáva mastná alebo páchne, môže byť už opotrebovaná alebo zanesená. Pri upratovaní je dôležité, aby textil nepresúval nečistoty späť na povrch.", "Pri dekách a poťahoch sledujte skôr mäkkosť, tvar a pach než dokonalú savosť."),
        ],
        "expert_title": "Odbornejší pohľad: veľký povrch jemných vlákien a zvyšky z prania",
        "expert_p1": "Mikrovlákno funguje vďaka veľkej ploche jemných vlákien. Čím väčšia plocha, tým viac kontaktu s vodou, prachom a mastnotou. Rovnaký princíp však znamená, že zvyšky aviváže alebo gélu sa môžu prejaviť výraznejšie než pri hladkom textile.",
        "rule": "Mikrovlákno perte bez aviváže, nepreplňte bubon a sušte vzdušne. Pri handričkách ho držte oddelene od textílií, ktoré púšťajú vlákna.",
        "recommendation_intro": "Pri mikrovlákne je kľúčové neobaliť jemné vlákna zvyškami. Šetrný prací gél používajte v primeranej dávke a bez aviváže, najmä pri handričkách a športových uterákoch.",
        "product_text": "Vhodný na pranie mikrovláknových textílií v primeranej dávke a s dobrým oplachom. Pri handričkách a funkčných kusoch odporúčame nepoužívať aviváž.",
        "links": [
            ("/n/ako-prat-deku-z-mikroplysu-aby-zostala-hebka", "Ako prať deku z mikroplyšu, aby zostala hebká"),
            ("/n/polyester-vs-bavlna-rozdiely-pri-noseni-prani-a-voni", "Polyester vs bavlna: rozdiely pri nosení, praní a vôni"),
            ("/n/ako-vycistit-susiak-na-bielizen-aby-neprenasal-spinu-na-pradlo", "Ako vyčistiť sušiak na bielizeň"),
        ],
        "faq": [
            ("Môžem používať aviváž na mikrovlákno?", "Pri handričkách a funkčných kusoch radšej nie. Môže znížiť savosť a čistiacu schopnosť."),
            ("Prečo handrička z mikrovlákna nesaje?", "Môže byť obalená avivážou, mastnotou alebo zvyškami pracieho prostriedku."),
            ("Prať mikrovlákno s uterákmi?", "Radšej oddelene. Uteráky môžu púšťať vlákna, ktoré sa na mikrovlákno zachytia."),
        ],
    },
    "egg_stain": {
        "marker": "Detailnejší postup na vajíčko, bielkovinu a žltý fľak na textile",
        "problem": "vajíčko na textile kombinuje bielkovinu, tuk zo žĺtka a niekedy aj zvyšky jedla, preto ho netreba hneď zalievať horúcou vodou",
        "scope": "oblečení, obruse, kuchynskej utierke, detskom tričku, zástere a látkovej servítke",
        "avoid": "horúcu vodu na začiatku, silné trenie a sušičku pred kontrolou žltého alebo mastného tieňa",
        "diagnosis": [
            "<strong>Najprv odobrať zvyšok:</strong> vajíčko nevtláčajte do väzby látky.",
            "<strong>Bielkovina reaguje na teplo:</strong> horúca voda môže problém zhoršiť.",
            "<strong>Žĺtok je aj mastný:</strong> po praní môže ostať slabá mapa.",
            "<strong>Kuchynská utierka drží pach:</strong> sušenie musí byť rýchle a vzdušné.",
        ],
        "state_rows": [
            ("čerstvé vajíčko", "odobrať prebytok a opláchnuť chladnejšie", "bez trenia"),
            ("zaschnutý zvyšok", "jemne uvoľniť pred praním", "neškrabať vlákna"),
            ("žltý tieň", "predčistiť a skontrolovať", "žĺtok môže mastiť"),
            ("pach na utierke", "prať a dobre vysušiť", "vlhko pach zhorší"),
        ],
        "textile_rows": [
            ("bavlnené tričko", "odobrať zvyšok a predčistiť", "odolnejší materiál"),
            ("obrus", "pracovať od okraja ku stredu", "nezažehliť tieň"),
            ("kuchynská utierka", "prať oddelene pri väčšom znečistení", "drží pach jedla"),
            ("detské oblečenie", "jemný postup a dobrý oplach", "citlivá pokožka"),
        ],
        "sections": [
            ("Ako odstrániť vajíčko z oblečenia", "Prebytok vajíčka najprv opatrne odoberte lyžičkou alebo tupou hranou. Netrite ho do strán a nezačínajte horúcou vodou. Miesto opláchnite skôr chladnejšie a potom lokálne predčistite podľa materiálu.", "Tričko alebo zásteru perte podľa štítku a pred sušením skontrolujte, či nezostal žltý alebo mastný tieň."),
            ("Vajíčko na obruse a látkovej servítke", "Na obruse sa vajíčko často mieša s olejom, majonézou, pečivom alebo omáčkou. Preto najprv odoberte pevný zvyšok, potom riešte bielkovinovú a mastnú časť. Žehlenie odložte až po kontrole výsledku.", "Pri mastných kombináciách nadväzuje článok <a href=\"/n/ako-odstranit-majonezu-a-dressing-z-obrusu-bez-mastneho-flaku\">ako odstrániť majonézu a dressing z obrusu</a>."),
            ("Prečo vajíčko neriešiť horúcou vodou hneď", "Bielkoviny vo vajíčku môžu pri teple stuhnúť. Na textile to znamená, že sa zvyšok môže prichytiť pevnejšie. Preto je bezpečnejšie začať miernejšie a teplo použiť až podľa štítku pri samotnom praní.", "Toto pravidlo je dôležité najmä pri čerstvom vajíčku a detskom oblečení."),
            ("Vajíčko na kuchynskej utierke", "Kuchynská utierka môže po vajíčku držať aj pach jedla. Po odstránení zvyškov ju neodkladajte vlhkú do koša. Ak je znečistenie výrazné, perte ju oddelene od jemnej bielizne a dobre vysušte.", "Ak sa pach v kuchynských textíliách opakuje, skontrolujte aj sušenie a veľkosť dávky v práčke."),
            ("Ako riešiť zaschnuté vajíčko", "Zaschnuté vajíčko najprv jemne uvoľnite. Neškrabte ho ostrým predmetom, aby ste nepoškodili vlákna. Po uvoľnení zvyškov postupujte lokálne a až potom perte celý kus.", "Ak po praní ostane tieň, nesušte horúco. Opakovanie mierneho predčistenia je bezpečnejšie než agresívny zásah."),
        ],
        "depth": [
            ("Bielkovina, tuk a pach v jednej škvrne", "Vajíčko nie je len žltý fľak. Bielok prináša bielkovinovú časť, žĺtok mastnotu a kuchynské použitie často pridá soľ, olej alebo omáčku. Preto univerzálny rýchly trik nemusí stačiť.", "Najlepšie funguje poradie krokov: odobrať objem, začať mierne, predčistiť, prať a skontrolovať pred sušením."),
            ("Ako kontrolovať výsledok po praní", "Po praní sledujte farebný tieň, mastný kruh aj pach. Mokrá látka môže vyzerať čisto, ale po vyschnutí sa žĺtok alebo pach ukáže znova. Preto ju nedávajte hneď do sušičky.", "Pri kuchynských utierkach je dôležité aj rýchle vysušenie, aby sa zvyškový pach nerozvinul vo vlhku."),
        ],
        "expert_title": "Odbornejší pohľad: bielkovinová škvrna a prečo začínať mierne",
        "expert_p1": "Škvrny s obsahom bielkovín sa môžu pri teple správať inak než čistá mastnota alebo pigment. Ak sa začína príliš horúco, zvyšok sa môže pevnejšie naviazať na textil. Preto je pri vajíčku rozumné najprv mechanicky odobrať prebytok a postupovať od miernejšieho kroku.",
        "rule": "Pri vajíčku nezačínajte horúcou vodou. Najprv odoberte zvyšok, použite mierne predčistenie a až potom perte podľa štítku.",
        "recommendation_intro": "Pri vajíčku má prací gél zmysel po tom, čo z textilu odstránite pevný zvyšok a nezačnete horúcou vodou. Potom pomôže doprať mastný aj pachový zvyšok.",
        "product_text": "Vhodný na následné pranie tričiek, obrusov a kuchynských textílií po lokálnom predčistení vajíčka. Pri detskom oblečení sledujte dobrý oplach.",
        "links": [
            ("/n/ako-odstranit-mlieko-a-jogurt-z-textilu-bez-kysleho-zapachu", "Ako odstrániť mlieko a jogurt z textilu"),
            ("/n/ako-odstranit-majonezu-a-dressing-z-obrusu-bez-mastneho-flaku", "Ako odstrániť majonézu a dressing z obrusu"),
            ("/n/ako-prat-obrus-po-oslave-aby-nezostali-mastne-skvrny-a-pachy", "Ako prať obrus po oslave"),
        ],
        "faq": [
            ("Prečo nepoužiť horúcu vodu hneď?", "Vajíčko obsahuje bielkoviny, ktoré môžu teplom stuhnúť a pevnejšie sa držať v textile."),
            ("Čo ak ostal žltý tieň?", "Nesušte horúco. Miesto znovu lokálne predčistite a perte podľa štítku."),
            ("Ako prať kuchynskú utierku po vajíčku?", "Odstráňte zvyšky, predčistite, perte podľa znečistenia a hlavne dobre vysušte."),
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
        <p>{config["problem"].capitalize()}. Preto sa oplatí rozmýšľať najprv nad príčinou a materiálom, nie iba nad tým, ktorý program spustiť. Dobrý postup musí rešpektovať vlákno, konštrukciu, zvyšky nečistoty aj spôsob sušenia.</p>
        <p>Pri textile ako {config["scope"]} rozhoduje aj to, či ide o bežnú údržbu, lokálnu škvrnu, funkčný materiál alebo citlivý kus. Najväčšie riziko je {config["avoid"]}.</p>
        {note_card("Rýchla praktická diagnostika", config["diagnosis"])}
        <h2>Postup podľa stavu textilu alebo škvrny</h2>
        {table(["Stav", "Čo urobiť", "Poznámka"], config["state_rows"])}
        <h2>Postup podľa typu materiálu alebo textilu</h2>
        {table(["Textil", "Postup", "Prečo"], config["textile_rows"])}
        {sections}
        <h2>{config["expert_title"]}</h2>
        <p>{config["expert_p1"]}</p>
        <p>Pri starostlivosti o textil sa oplatí spájať praktické pozorovanie s údajmi zo štítku: zloženie, symboly prania, odporúčaná teplota, sušenie a zákaz bielenia. Univerzálna rada funguje len vtedy, keď neignoruje konkrétny materiál.</p>
        {depth}
        <h2>Kontrolný checklist pred praním</h2>
        <p>Pred praním skontrolujte materiálové zloženie, symboly na štítku, mieru znečistenia, rizikové miesta a to, či sa problém týka celého kusu alebo iba jednej lokálnej plochy. Pri úpletoch sledujte tvar, pri škvrnách zvyšky na povrchu a pri funkčných materiáloch savosť alebo pach.</p>
        <p>Do jednej dávky nedávajte textílie s úplne rozdielnou potrebou: vlnený sveter, mastnú kuchynskú utierku, mikrovláknovú handričku a tmavé legíny nepotrebujú rovnaký režim. Triedenie je často dôležitejšie než pridanie väčšieho množstva pracieho prostriedku.</p>
        <h2>Kedy postup zopakovať</h2>
        <p>Ak po praní zostal pach, mastný tieň, tvrdší povrch, zmena tvaru alebo zvyšky na látke, nesušte textil horúco. Najprv zistite, či ide o nečistotu, zvyšok pracieho prostriedku alebo zmenu materiálu. Až potom zvoľte opakovanie prania alebo lokálne predčistenie.</p>
        <p>Ak látka púšťa farbu, stráca tvar, žmolkuje sa alebo reaguje citlivo, znížte mechaniku a teplotu. Opakovaný mierny postup je pri väčšine textílií bezpečnejší než jeden agresívny zásah.</p>
        <h2>Ako predísť poškodeniu pri sušení</h2>
        <p>Sušenie je rovnako dôležité ako pranie. Mokrá látka môže vyzerať čisto, ale až po preschnutí sa ukáže mastná mapa, pach, tvrdosť alebo zmena tvaru. Sušičku používajte iba vtedy, keď ju štítok povoľuje a keď je výsledok po praní skontrolovaný.</p>
        <p>Pri úpletoch, vlne, viskóze a mikrovlákne často pomôže voľné sušenie, tvarovanie a nepreplnený priestor. Pri škvrnách najprv overte, že miesto je čisté, až potom pridajte teplo.</p>
        <h2>Domáca rutina pri opakovaných problémoch</h2>
        <p>Ak sa problém opakuje, nastavte si jednoduchý postup: kontrola pred košom na bielizeň, triedenie podľa materiálu, lokálne predčistenie, primeraná dávka pracieho gélu, nepreplnený bubon a kontrola pred sušením. Tento postup znižuje riziko zrážania, žmolkovania, pachu aj zvyškov škvŕn.</p>
        <p>Všímajte si, kedy problém vzniká. Iné riešenie potrebuje zrazený sveter, žmolkujúca mikina, mikrovláknová handrička bez savosti a obrus po jedle. Práve konkrétna príčina rozhoduje o ďalšom praní.</p>
        <h2>Čo sledovať po druhom praní</h2>
        <p>Ak ani druhé šetrné pranie nepomohlo, rozlíšte, či ide o špinu, pach, zvyšky prostriedku alebo už o zmenu materiálu. Zrazenie, plstnatenie, vydratý povrch alebo trvalé oslabenie vlákna sa nedajú vyprať ako obyčajná škvrna.</p>
        <p>Pri drahších alebo citlivých kusoch si poznačte, čo pomohlo a čomu sa vyhnúť. Pri ďalšom praní tak nastavíte program rýchlejšie a s menším rizikom poškodenia.</p>
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
        re.compile(r"<p>\s*Pokryté výrazy:\s*(.*?)\.\s*</p>", re.IGNORECASE | re.DOTALL),
        r"<p>V texte nájdete aj praktické situácie, ktoré sa pri tejto téme často riešia: \1.</p>",
    ),
    (
        re.compile(r"<p>\s*Článok cieli výrazy ako\s+(.*?)\.\s*</p>", re.IGNORECASE | re.DOTALL),
        r"<p>V texte nájdete aj praktické situácie, ktoré ľudia pri tejto téme často riešia: \1.</p>",
    ),
    (
        re.compile(r"<p>\s*V článku pokrývame aj praktické otázky z praxe:\s*<strong>(.*?)</strong>\.\s*(.*?)</p>", re.IGNORECASE | re.DOTALL),
        r"<p>V texte nájdete aj praktické otázky z praxe: <strong>\1</strong>. \2</p>",
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
    parser = argparse.ArgumentParser(description="Conservatively expand VEVO retrofit wave 18 material and food articles.")
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
                "wave": "retrofit-wave-18-materials-food-five",
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
                "wave": "retrofit-wave-18-materials-food-five",
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
