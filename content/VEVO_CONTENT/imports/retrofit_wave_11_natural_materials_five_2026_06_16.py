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
OUT_JSON = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-11-natural-materials-five-2026-06-16.json"
MCP_RESULTS = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-11-natural-materials-five-2026-06-16-mcp-results.json"


ARTICLES = [
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-22-2026-06-11-articles.json",
        "slug": "lan-vs-bavlna-rozdiely-v-savosti-krcivosti-a-starostlivosti",
        "post_id": "2238",
        "url": "https://www.vevo.sk/n/lan-vs-bavlna-rozdiely-v-savosti-krcivosti-a-starostlivosti",
        "topic": "linen_vs_cotton",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-23-2026-06-11-articles.json",
        "slug": "co-je-akryl-preco-pripomina-vlnu-a-ako-sa-perie",
        "post_id": "2242",
        "url": "https://www.vevo.sk/n/co-je-akryl-preco-pripomina-vlnu-a-ako-sa-perie",
        "topic": "acrylic_material",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-22-2026-06-11-articles.json",
        "slug": "modal-vs-lyocell-vs-viskoza-ako-sa-lisia-pri-prani-a-noseni",
        "post_id": "2239",
        "url": "https://www.vevo.sk/n/modal-vs-lyocell-vs-viskoza-ako-sa-lisia-pri-prani-a-noseni",
        "topic": "modal_lyocell_viscose",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-22-2026-06-11-articles.json",
        "slug": "organicka-bavlna-co-znamena-a-ci-sa-perie-inak-ako-bezna-bavlna",
        "post_id": "2236",
        "url": "https://www.vevo.sk/n/organicka-bavlna-co-znamena-a-ci-sa-perie-inak-ako-bezna-bavlna",
        "topic": "organic_cotton",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-22-2026-06-11-articles.json",
        "slug": "co-je-bavlna-vlastnosti-vyhody-nevyhody-a-starostlivost",
        "post_id": "2235",
        "url": "https://www.vevo.sk/n/co-je-bavlna-vlastnosti-vyhody-nevyhody-a-starostlivost",
        "topic": "cotton_material",
    },
]


def clean(markup):
    return textwrap.dedent(markup).strip()


def table(headers, rows):
    header_html = "".join(
        f'<th style="border: 1px solid #e5e5e5; padding: 10px; text-align: left;">{header}</th>'
        for header in headers
    )
    body_html = "\n".join(
        "<tr>"
        + "".join(f'<td style="border: 1px solid #e5e5e5; padding: 10px;">{cell}</td>' for cell in row)
        + "</tr>"
        for row in rows
    )
    return (
        '<table style="width: 100%; border-collapse: collapse; margin: 20px 0;">\n'
        f"<thead><tr>{header_html}</tr></thead>\n<tbody>\n{body_html}\n</tbody>\n</table>"
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


def product_card(kind):
    if kind == "samples":
        return clean(
            """
            <div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 24px 0; background: #f7fbf8;">
            <h2 style="margin-top: 0;">Odporúčané riešenie na jemné testovanie vône</h2>
            <p>Pri mäkkých materiáloch nosených pri pokožke je lepšie skúšať vôňu postupne. Najprv riešte čistotu, oplach a sušenie, až potom intenzitu vône.</p>
            <div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff; margin: 14px 0;">
            <h3 style="margin-top: 0;">Vevo Essence Sample Set 9x10ml</h3>
            <p>Vzorkový set pomôže porovnať viac vôní na menšom množstve bielizne bez veľkého balenia.</p>
            <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="/p-1621/vevo-essence-sample-set">Pozrieť vzorkový set</a></p>
            </div>
            <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="/c/vzorky/parfum-do-prania-vzorky">Pozrieť vzorky parfumov do prania</a></p>
            </div>
            """
        )
    return clean(
        """
        <div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 24px 0; background: #f7fbf8;">
        <h2 style="margin-top: 0;">Odporúčané riešenie na šetrné pranie materiálov</h2>
        <p>Pri bavlne, ľane, akryle aj zmesiach rozhoduje primerané dávkovanie, dobrý oplach a sušenie. Prací produkt má pomôcť čistote, nie zakryť zvyšky potu alebo zatuchnutie.</p>
        <div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff; margin: 14px 0;">
        <h3 style="margin-top: 0;">Prací gél hypoalergénny z Marseillského mydla 1L</h3>
        <p>Univerzálny základ na bežné pranie mnohých textílií. Pri jemných úpletoch, vlne, zmesiach a špeciálnych úpravách vždy rešpektujte štítok výrobcu.</p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="/p-1551/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l">Pozrieť prací gél</a></p>
        </div>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="/c/vevo-home-care/pranie/praci-gel">Pozrieť pracie gély</a></p>
        </div>
        """
    )


TOPICS = {
    "linen_vs_cotton": {
        "marker": "Detailnejšie porovnanie ľanu a bavlny pri praní, savosti a krčivosti",
        "product_kind": "laundry",
        "intro": [
            "Ľan a bavlna sú prírodné celulózové materiály, ale v domácnosti sa nesprávajú rovnako. Bavlna je univerzálna, známa a často tolerantná pri bežnom praní. Ľan pôsobí vzdušne, pevne a elegantne, ale výraznejšie sa krčí a pri sušení potrebuje viac pozornosti.",
            "Pri porovnaní nejde iba o otázku, čo je lepšie. Iné vlastnosti chcete pri letných šatách, iné pri obliečkach, uterákoch, obruse alebo košeli. Rozumná starostlivosť vychádza zo štítku, farby, hrúbky tkaniny a toho, či ide o čistý ľan, čistú bavlnu alebo zmes.",
        ],
        "bullets": [
            "<strong>Ľan:</strong> vzdušný, pevný, krčivý a citlivý na tvar pri sušení.",
            "<strong>Bavlna:</strong> univerzálna, savá a dobre známa v domácnosti.",
            "<strong>Zmesi:</strong> môžu krčivosť znížiť, ale nemenia potrebu čítať štítok.",
            "<strong>Sušenie:</strong> pri ľane často rozhoduje viac než samotné pranie.",
        ],
        "tables": [
            {
                "title": "Ľan a bavlna podľa použitia",
                "headers": ["Textil", "Lepšie vynikne", "Čo strážiť pri praní"],
                "rows": [
                    ("letné oblečenie", "ľan pre vzdušnosť, bavlna pre univerzálnosť", "nepreplniť bubon a vybrať hneď po praní"),
                    ("posteľná bielizeň", "bavlna pre jednoduchšiu rutinu, ľan pre vzdušný pocit", "dobre dosušiť veľké kusy"),
                    ("obrus a kuchynský textil", "ľan pre vzhľad, bavlna pre praktickosť", "škvrny riešiť pred praním"),
                    ("uteráky", "bavlna je bežnejšia", "sledovať savosť a zvyšky aviváže"),
                ],
            },
            {
                "title": "Kontrola po praní",
                "headers": ["Prejav", "Typická príčina", "Čo spraviť nabudúce"],
                "rows": [
                    ("ľan je silno pokrčený", "dlho stál v práčke alebo bol preplnený bubon", "vybrať hneď, vyhladiť a sušiť voľne"),
                    ("bavlna je tvrdá", "zvyšky produktu, tvrdá voda alebo presušenie", "upraviť dávkovanie a oplach"),
                    ("obrus má mapy", "škvrny neboli predčistené", "riešiť fľak pred hlavným praním"),
                    ("obliečky zatuchli", "pomalé sušenie v záhyboch", "rozložiť a vetrať počas sušenia"),
                ],
            },
        ],
        "sections": [
            ("Ako prať ľanové a bavlnené oblečenie", "Ľanové oblečenie perte s priestorom v bubne a vyberajte ho hneď po skončení programu. Ak zostane mokré pokrčené, záhyby sa zvýraznia a žehlenie bude náročnejšie. Bavlna je zvyčajne praktickejšia, ale aj pri nej platí, že farba, potlač a zmes môžu byť citlivejšie než samotné vlákno.", "Pri košeliach, šatách a nohaviciach je dôležité sušenie. Ľan vyhlaďte rukami, vytvarujte švy a nenechávajte ho visieť tak, aby sa deformoval. Bavlnené tričká perte naruby, najmä ak majú potlač. Súvisiaci detail nájdete v článku <a href=\"/n/co-je-lan-preco-sa-krci-ako-ho-prat-a-ako-ho-zjemnit\">čo je ľan a ako ho prať</a>."),
            ("Ako prať ľanové a bavlnené obliečky", "Pri obliečkach rozhoduje veľkosť dávky. Ak do bubna natlačíte priveľa veľkých kusov, textil sa síce namočí, ale horšie sa vypláchne a po vysušení môže pôsobiť tvrdšie. Ľanové obliečky potrebujú priestor a dobré rozloženie pri sušení. Bavlnené obliečky sú tolerantnejšie, ale aj pri nich je častý problém zatuchnutie v záhyboch.", "Ak riešite posteľnú bielizeň podrobnejšie, nadväzuje článok <a href=\"/n/ako-spravne-prat-obliecky-kompletny-sprievodca-starostlivostou\">ako správne prať obliečky</a>. Pri frekvencii prania pomôže aj návod <a href=\"/n/ako-casto-prat-postelne-pradlo\">ako často prať posteľné prádlo</a>."),
            ("Ako riešiť krčivosť bez zbytočného tepla", "Ľan sa krčí prirodzene. Cieľom nemusí byť úplne hladký vzhľad, ale kontrolované záhyby a čistý tvar. Ak chcete ľan žehliť, často sa lepšie pracuje s mierne vlhkým textilom a podľa štítku. Pri bavlne záleží na väzbe: hladká košeľovina sa krčí inak než hrubý uterák alebo džersejové tričko.", "Najlepšia prevencia krčivosti je nepreplniť bubon, nenechať veci stáť mokré a sušiť ich vystreté. Ak sa textil príliš presuší, žehlenie môže byť ťažšie a budete potrebovať viac tepla, čo nie vždy prospieva farbe alebo potlači."),
            ("Ako zachovať savosť a príjemný dotyk", "Bavlnené uteráky a ľanové kuchynské utierky potrebujú dobrý oplach. Priveľa pracieho produktu alebo časté používanie aviváže môže zhoršiť savosť. Textil potom vonia, ale horšie saje. Pri uterákoch je lepšie riešiť čistotu, oplach a úplné sušenie než pridávať stále viac vône.", "Ak sú uteráky tvrdé alebo zatuchnuté, pomôže článok <a href=\"/n/preco-uteraky-zapachaju-aj-po-prani-zatuchnuty-pach-tvrdost-a-strata-savosti\">prečo uteráky zapáchajú aj po praní</a>. Pri ľanových utierkach sledujte hlavne mastnotu z kuchyne a dôkladné preschnutie."),
            ("Ako si vybrať medzi ľanom a bavlnou do domácnosti", "Ak chcete jednoduchú údržbu, bavlna býva praktickejšia vo veľkom množstve bežnej bielizne. Ak chcete vzdušnosť, prirodzenú štruktúru a nevadí vám krčivosť, ľan môže byť výborný. Rozhoduje však aj konkrétny výrobok: hrubý ľanový obrus sa perie inak než ľanová košeľa a bavlnený uterák inak než jemné tričko.", "V domácnosti je najlepšie mať pre materiály samostatné rutiny. Ľanové kúsky vyberajte z práčky hneď, bavlnené uteráky perte s dôrazom na savosť a obliečky sušte rozložené. Tak si zachováte výhody oboch materiálov bez zbytočných kompromisov."),
        ],
        "box": ("Rýchla zásada", "Ľan sa oplatí prať s väčším priestorom a sušiť vystretý. Bavlna znesie viac, ale aj pri nej rozhoduje dávkovanie, oplach a úplné sušenie."),
        "faq": [
            ("Je lepší ľan alebo bavlna?", "Závisí od použitia. Ľan je vzdušný a prirodzene krčivý, bavlna univerzálna a jednoduchšia v bežnej rutine."),
            ("Prečo sa ľan tak krčí?", "Krčivosť je prirodzená vlastnosť ľanovej tkaniny. Pomáha menšia dávka, rýchle vybratie a sušenie v tvare."),
            ("Môžem prať ľan a bavlnu spolu?", "Áno, ak majú podobnú farbu, hrúbku a štítok. Jemný ľan však neperte s ťažkými uterákmi."),
        ],
    },
    "acrylic_material": {
        "marker": "Detailnejší postup pre akryl, svetre, deky a pletené doplnky",
        "product_kind": "laundry",
        "intro": [
            "Akryl je syntetické vlákno, ktoré sa v oblečení často používa tam, kde má textil pripomínať vlnu, ale byť ľahší, lacnejší alebo jednoduchší na údržbu. Nájdete ho v svetroch, čiapkach, šáloch, dekách, kardigánoch aj zmesových úpletoch.",
            "Pri praní akrylu bývajú najväčšie témy žmolkovanie, statika, strata tvaru a prehriatie. Akrylový sveter sa nemusí zraziť ako vlna, ale môže sa vyťahať, stratiť pekný povrch alebo začať elektrizovať. Preto sa oplatí prať ho šetrne a nehodnotiť ho ako obyčajnú odolnú syntetiku.",
        ],
        "bullets": [
            "<strong>Perte naruby:</strong> znížite trenie na viditeľnom povrchu.",
            "<strong>Chráňte úplet:</strong> akryl nemá rád zipsy, suchý zips a preplnený bubon.",
            "<strong>Teplo opatrne:</strong> horúce sušenie môže zhoršiť tvar a statiku.",
            "<strong>Zmesi čítajte celé:</strong> ak je v úplete vlna, rozhoduje najcitlivejšia zložka.",
        ],
        "tables": [
            {
                "title": "Akryl podľa typu výrobku",
                "headers": ["Výrobok", "Najčastejší problém", "Lepší postup"],
                "rows": [
                    ("akrylový sveter", "žmolky a vytiahnutý tvar", "prať naruby, nízke otáčky, sušiť s oporou"),
                    ("čapica a šál", "deformácia malého úpletu", "ochranné vrecko alebo ručné pranie podľa štítku"),
                    ("akrylová deka", "veľký objem a slabý oplach", "nepreplniť bubon, sušiť vzdušne"),
                    ("zmes s vlnou", "citlivosť vlnenej zložky", "postupovať ako pri jemnejšom materiáli"),
                ],
            },
            {
                "title": "Kontrola po praní",
                "headers": ["Prejav", "Príčina", "Čo upraviť"],
                "rows": [
                    ("sveter žmolkuje", "trenie pri nosení alebo praní", "prať naruby a oddeliť od drsných kusov"),
                    ("úplet elektrizuje", "presušenie alebo syntetická dávka", "sušiť miernejšie, neprehrievať"),
                    ("rukávy sú vytiahnuté", "mokré vešanie alebo ťažká voda v úplete", "sušiť s oporou, nie za ramená"),
                    ("deka pôsobí zatuchnuto", "pomalé sušenie veľkého objemu", "rozložiť a dosušiť úplne"),
                ],
            },
        ],
        "sections": [
            ("Ako prať akrylový sveter bez žmolkovania", "Akrylový sveter otočte naruby a perte ho s podobne jemnými kúskami. Nepatrí k uterákom, rifliam ani oblečeniu so suchým zipsom. Mechanické trenie je jeden z hlavných dôvodov, prečo akryl začne vyzerať opotrebovane skôr, než by musel.", "Ak má sveter voľnejší úplet, použite nižšie otáčky a po praní ho netrhajte za rukávy. Jemne ho vytvarujte a sušte s oporou. Pri podobných pleteninách pomôže aj článok <a href=\"/n/akryl-vs-vlna-zmolkovanie-teplo-zapach-a-starostlivost\">akryl vs vlna</a>."),
            ("Ako prať akrylovú deku", "Deka je objemná, takže najčastejšou chybou je preplnený bubon. Ak deka nemá dosť miesta, prací produkt sa horšie vypláchne a po vysušení môže pôsobiť tvrdšie alebo zatuchnuto. Pri veľkom kuse sledujte kapacitu práčky a radšej perte samostatne, ak to dáva zmysel.", "Sušenie deky musí byť úplné. Vlhkosť v hrubších častiach vytvorí zatuchnutý nádych, ktorý vôňa iba prekryje. Deku počas sušenia prekladajte alebo rozložte tak, aby prúdil vzduch aj cez hrubšie vrstvy."),
            ("Ako riešiť statiku a elektrizovanie", "Akryl môže elektrizovať, najmä ak je presušený alebo kombinovaný s ďalšími syntetickými materiálmi. Problém zhoršuje horúce sušenie a príliš suchý vzduch. Riešením nie je vždy pridať veľa aviváže; pri niektorých úpletoch môže zanechať film a zhoršiť dotyk.", "Lepšie je prať s menším trením, neprehrievať a sušiť mierne. Ak je akryl v zmesi s vlnou, aviváž a teplo posudzujte ešte opatrnejšie. Vždy rozhoduje štítok konkrétneho kusu."),
            ("Ako odstraňovať žmolky z akrylu", "Žmolky z akrylu odstraňujte jemne. Odžmolkovač alebo textilný hrebeň používajte s citom, aby ste nevytrhali povrchové vlákna a nevytvorili tenšie miesta. Ak žmolky vznikajú rýchlo po každom praní, skontrolujte triedenie bielizne a trenie v bubne.", "Prevencia je účinnejšia než opakované holenie svetra. Perte naruby, nepoužívajte príliš silné odstreďovanie a oddeľte akryl od drsných materiálov. Pri svetroch, ktoré zapáchajú po nosení, najprv vetrajte a perte až vtedy, keď je to skutočne potrebné."),
            ("Ako odlíšiť akryl od vlny v praxi", "Akryl môže vyzerať podobne ako vlna, ale pri teple, vlhkosti a zápachu sa správa inak. Vlna má špecifickú schopnosť odolávať pachom a často potrebuje menej časté pranie. Akryl je syntetika a pri nosení môže skôr elektrizovať alebo žmolkovať. Zmes oboch materiálov preto perte podľa citlivejšej zložky.", "Ak si nie ste istí, či je v svetri vlna, nehádajte podľa dotyku. Pozrite štítok a radšej zvoľte jemnejší postup. Pre vlnu nadväzuje článok <a href=\"/n/ako-prat-vlneny-sveter-ked-zapacha-po-noseni\">ako prať vlnený sveter</a>."),
        ],
        "box": ("Rýchla zásada", "Akryl perte ako pleteninu, nie ako obyčajnú syntetiku. Menej trenia, menej tepla a lepšie sušenie urobia viac než agresívny program."),
        "faq": [
            ("Je akryl vlna?", "Nie. Akryl je syntetické vlákno, ktoré môže vzhľadom a pocitom vlnu pripomínať."),
            ("Ako prať akrylový sveter?", "Naruby, šetrne, s nižšími otáčkami a sušením podľa štítku. Pri zmesi s vlnou ešte opatrnejšie."),
            ("Prečo akryl žmolkuje?", "Najčastejšie kvôli treniu pri nosení a praní. Pomáha triedenie, pranie naruby a jemnejší pohyb."),
        ],
    },
    "modal_lyocell_viscose": {
        "marker": "Detailnejšie porovnanie modalu, lyocellu a viskózy v domácej starostlivosti",
        "product_kind": "samples",
        "intro": [
            "Modal, lyocell a viskóza patria medzi regenerované celulózové materiály, ale zákazník ich vníma cez veľmi praktické rozdiely: mäkkosť, splývavosť, krčivosť, chladivý dotyk a správanie za mokra. Preto nestačí povedať, že sú si príbuzné. Pri praní rozhoduje hotový výrobok.",
            "Modal býva obľúbený v spodnej bielizni a pyžamách, lyocell v hladkých tričkách, šatách a posteľnej bielizni, viskóza v blúzkach a splývavých šatách. Každý z týchto materiálov môže byť v zmesi s elastanom, bavlnou alebo polyesterom, čo mení aj domácu rutinu.",
        ],
        "bullets": [
            "<strong>Modal:</strong> mäkký, príjemný pri pokožke, vhodný na pyžamá a bielizeň.",
            "<strong>Lyocell:</strong> hladký, často chladivý, vyžaduje dobré sušenie.",
            "<strong>Viskóza:</strong> splývavá, ale za mokra môže byť citlivejšia.",
            "<strong>Zmes rozhoduje:</strong> elastan a konštrukcia menia pranie viac než názov materiálu.",
        ],
        "tables": [
            {
                "title": "Porovnanie podľa použitia",
                "headers": ["Použitie", "Modal", "Lyocell", "Viskóza"],
                "rows": [
                    ("spodná bielizeň", "mäkký kontakt s pokožkou", "menej častý, ale hladký", "skôr v jemných zmesiach"),
                    ("šaty a blúzky", "mäkké a pohodlné", "hladké a elegantné", "výrazne splývavé"),
                    ("posteľná bielizeň", "mäkký pocit", "hladký a chladivý dotyk", "menej bežná"),
                    ("pyžamá", "veľmi častý komfortný materiál", "príjemný pri dobrom sušení", "citlivejší na tvar"),
                ],
            },
            {
                "title": "Kontrola pri praní",
                "headers": ["Problém", "Kde vzniká", "Ako ho riešiť"],
                "rows": [
                    ("pokrčenie", "viskóza a lyocell po státí v bubne", "vybrať hneď a vyhladiť"),
                    ("vytiahnutý tvar", "mokré vešanie jemných kusov", "sušiť s oporou, nie pod ťahom"),
                    ("zatuchnutie", "pomalé sušenie v záhyboch", "rozložiť a vetrať"),
                    ("ťažká vôňa", "materiál nosený pri pokožke", "testovať jemnejšiu intenzitu"),
                ],
            },
        ],
        "sections": [
            ("Ako prať modal, lyocell a viskózu bez zmeny tvaru", "Všetky tri materiály perte s podobne jemnými kúskami. Ťažké uteráky, zipsy a hrubé švy pridávajú mechanické trenie, ktoré jemný povrch nepotrebuje. Pri blúzkach, šatách, pyžamách a spodnej bielizni je lepšia menšia dávka a rýchle vybratie z práčky.", "Ak je kus s elastanom, nepreháňajte teplo. Ak je veľmi splývavý, nevešajte ho mokrý tak, aby ho vlastná hmotnosť ťahala nadol. Detail k jednotlivým materiálom nájdete aj v článkoch <a href=\"/n/modal-v-obleceni-co-znamena-preco-je-maekky-a-ako-ho-prat\">modal v oblečení</a> a <a href=\"/n/co-je-lyocell-alebo-tencel-priedusnost-jemnost-a-starostlivost\">čo je lyocell alebo Tencel</a>."),
            ("Ako vybrať vôňu pri mäkkých materiáloch", "Modal, lyocell a viskóza sa často nosia priamo na pokožke alebo používajú v posteľnej bielizni. Preto môže byť silná vôňa rušivejšia než pri uterákoch alebo hrubšom oblečení. Začnite jemnejšou intenzitou a sledujte, ako vôňa pôsobí po úplnom vysušení.", "Ak textil zapácha zatuchnuto, vôňa nie je prvé riešenie. Najprv skontrolujte, či neostal mokrý v práčke, či sa sušil dostatočne rozložený a či v ňom neostali zvyšky pracieho produktu. Až potom má zmysel dolaďovať vôňu."),
            ("Ako sušiť jemné celulózové materiály", "Najväčšia zmena tvaru často vzniká po praní, nie počas neho. Mokrý kus je ťažší a citlivejší. Preto ho netrhajte za rukávy, nekrúťte a nevešajte na úzky vešiak, ak by sa ramená vytiahli. Vyhlaďte švy, upravte lem a sušte s oporou.", "Pri posteľnej bielizni alebo väčších kusoch kontrolujte záhyby. Veľká vlhká plocha sa môže zvnútra sušiť pomalšie, než sa zdá. Ak ju zložíte príliš skoro, vznikne zatuchnutý pach."),
            ("Ako riešiť krčivosť a žehlenie", "Viskóza a lyocell sa môžu krčiť výraznejšie, najmä ak zostanú po praní stlačené. Najlepšou prevenciou je vybrať ich hneď a vyhladiť rukami. Žehlenie robte podľa štítku, často z rubu alebo cez tenkú látku. Pri jemných kusoch je bezpečnejšie nižšie teplo a viac trpezlivosti.", "Modal býva menej dramatický, ale aj pri ňom platí, že zlé sušenie pokazí mäkký dojem. Ak materiál pôsobí tvrdšie, skontrolujte dávkovanie a oplach, nie iba žehličku."),
            ("Ako si vytvoriť domácu rutinu", "Ak máte v šatníku viac modalu, lyocellu a viskózy, vytvorte im samostatnú jemnú dávku. Znížite trenie, lepšie odhadnete dávkovanie a po praní ich rýchlejšie vyberiete. Táto rutina je praktickejšia než riešiť každý kus úplne samostatne.", "Pri výbere pracieho produktu a vône postupujte od najcitlivejšieho kusu. Ak vôňa sedí pyžamu alebo blúzke, bude väčšinou bez problémov aj na menej citlivom textile. Opačne to platiť nemusí."),
        ],
        "box": ("Rýchla zásada", "Modal, lyocell a viskózu perte jemne, sušte bez ťahu a vôňu testujte mierne. Najväčšia chyba je nechať ich mokré pokrčené v bubne."),
        "faq": [
            ("Je modal lepší ako lyocell?", "Nie univerzálne. Modal býva veľmi mäkký, lyocell hladký a často chladivý. Rozhoduje použitie a hotový výrobok."),
            ("Prečo sa viskóza po praní vytiahla?", "Mokrý materiál mohol visieť pod vlastnou váhou alebo bol silno namáhaný. Pomáha sušenie bez ťahu."),
            ("Ako voňať tieto materiály?", "Najprv ich dobre vyperte a vysušte. Vôňu skúšajte jemne, najmä pri pyžamách, bielizni a posteľnej bielizni."),
        ],
    },
    "organic_cotton": {
        "marker": "Detailnejší pohľad na organickú bavlnu a domáce pranie",
        "product_kind": "laundry",
        "intro": [
            "Organická bavlna je najmä informácia o pestovaní a spracovaní v rámci konkrétnych štandardov. Pre domácu práčku však stále platí, že periete hotový výrobok: tričko, body, obliečky, uterák alebo spodnú bielizeň. Rozhoduje farba, potlač, úplet, švy a štítok.",
            "Častý omyl je myslieť si, že organická bavlna sa musí prať úplne inak než bežná bavlna, alebo naopak, že znesie všetko, lebo je kvalitnejšia. Ani jedno nie je presné. Organický pôvod nemení základné vlastnosti bavlneného vlákna pri vode, teple, sušení a trení.",
        ],
        "bullets": [
            "<strong>Organická neznamená nezničiteľná:</strong> stále sledujte štítok.",
            "<strong>Pri deťoch:</strong> dôležitý je dobrý oplach, nie iba pôvod vlákna.",
            "<strong>Pri potlači:</strong> perte naruby a nepreháňajte teplotu.",
            "<strong>Pri obliečkach:</strong> najväčšie riziko býva preplnený bubon a pomalé sušenie.",
        ],
        "tables": [
            {
                "title": "Organická bavlna podľa výrobku",
                "headers": ["Výrobok", "Čo rozhoduje", "Praktický postup"],
                "rows": [
                    ("detské body", "pokožka a zvyšky pracieho produktu", "jemné dávkovanie a dobrý oplach"),
                    ("tričko s potlačou", "farba a obrázok", "prať naruby podľa štítku"),
                    ("obliečky", "veľká plocha a sušenie", "nepreplniť bubon, rozložiť pri sušení"),
                    ("uterák", "savosť", "nezahltiť avivážou a dosušiť"),
                ],
            },
            {
                "title": "Čo organický pôvod nerieši",
                "headers": ["Téma", "Prečo nestačí označenie", "Čo sledovať"],
                "rows": [
                    ("zrážanie", "bavlna stále reaguje na teplo a sušenie", "štítok a prvé pranie"),
                    ("podráždenie pokožky", "môžu ho zhoršiť zvyšky produktu", "oplach a dávkovanie"),
                    ("blednutie", "farbivo a potlač sú samostatná téma", "prať naruby a triediť farby"),
                    ("tvrdosť", "závisí od vody, produktu a sušenia", "upraviť rutinu, nie iba materiál"),
                ],
            },
        ],
        "sections": [
            ("Ako prať organickú bavlnu v bežnej domácnosti", "Začnite štítkom a typom výrobku. Organické bavlnené tričko sa perie inak než organická bavlnená osuška alebo detské body. Pri tričku riešite farbu a potlač, pri uteráku savosť, pri detskom textile zvyšky pracieho produktu a pri obliečkach veľkosť dávky.", "Ak používate organickú bavlnu kvôli citlivej pokožke, venujte pozornosť oplachu. Zvyšky pracieho gélu alebo príliš intenzívnej vône môžu byť prakticky dôležitejšie než samotný pôvod vlákna."),
            ("Organická bavlna pri detskom textile", "Pri detských veciach ľudia často riešia materiál, ale podceňujú pranie. Detské body, pyžamá a obliečky by mali byť dobre vypláchnuté a úplne suché. Ak v nich zostane vlhkosť alebo zvyšky produktu, pokožka môže reagovať aj na veľmi kvalitný materiál.", "Nadväzuje aj článok <a href=\"/n/ako-prat-detsku-postelnu-bielizen-bez-drazdenia-pokozky\">ako prať detskú posteľnú bielizeň bez dráždenia pokožky</a>. Pri detskom textile je dôležité nepreplniť práčku, aby sa bielizeň skutočne opláchla."),
            ("Organická bavlna, certifikáty a štítok", "Certifikát je užitočný pri výbere produktu, ale nenahrádza prací symbol. GOTS alebo iné označenia hovoria o pôvode a spracovaní, no domáci program stále vyberáte podľa hotového výrobku. Ak má organické tričko potlač, potlač môže byť citlivejšia než samotná bavlna.", "Viac k tejto téme je v článku <a href=\"/n/certifikaty-na-textile-oeko-tex-gots-recyklovane-vlakna-a-co-znamenaju-pri-prani\">certifikáty na textile</a>. Prakticky platí: certifikát čítajte pri nákupe, štítok pri praní."),
            ("Ako zabrániť zrážaniu organickej bavlny", "Bavlna môže zmeniť rozmer, najmä pri vyššej teplote, horúcej sušičke alebo nesprávnom prvom praní. Organický pôvod túto fyziku neruší. Nové kúsky perte podľa štítku, triedte farby a pri neistote sa vyhnite horúcemu sušeniu.", "Ak sa bavlnené tričko zrazilo, často ide o kombináciu tepla, mechaniky a sušenia. Pri prevencii pomôže aj článok <a href=\"/n/preco-sa-oblecenie-zrazi-po-prani-teplota-vlakna-susicka-a-prevencia\">prečo sa oblečenie zrazí po praní</a>."),
            ("Ako vybrať produkt a vôňu pri organickej bavlne", "Ak kupujete organickú bavlnu kvôli jemnosti alebo pokožke, držte rovnakú logiku aj pri praní. Nepreháňajte dávkovanie, používajte primeraný program a vôňu dávkujte opatrne. Pri posteľnej bielizni a pyžamách má vôňa pôsobiť čistým dojmom, nie rušiť počas spánku.", "Prací produkt vyberajte podľa typu bielizne a miery znečistenia. Organická bavlna nepotrebuje marketingovo špeciálne zaobchádzanie, potrebuje presný a čistý proces prania."),
        ],
        "box": ("Rýchla zásada", "Organická bavlna sa doma perie podľa hotového výrobku. Pôvod vlákna je dôležitý pri výbere, ale práčku nastavujete podľa štítku, farby, potlače a použitia."),
        "faq": [
            ("Perie sa organická bavlna inak ako bežná?", "Nie automaticky. Rozhoduje konkrétny výrobok, farba, potlač a štítok."),
            ("Je organická bavlna lepšia pri citlivej pokožke?", "Môže byť dobrá voľba, ale dôležitý je aj prací produkt, dávkovanie, oplach a úplné sušenie."),
            ("Môže sa organická bavlna zraziť?", "Áno. Organický pôvod nezruší reakciu bavlny na teplo, sušičku alebo nesprávne prvé pranie."),
        ],
    },
    "cotton_material": {
        "marker": "Detailnejší pohľad na bavlnu, savosť, zrážanie a každodennú starostlivosť",
        "product_kind": "laundry",
        "intro": [
            "Bavlna je jeden z najbežnejších materiálov v domácnosti. Nájdete ju v tričkách, uterákoch, obliečkach, detskom oblečení, spodnej bielizni, utierkach aj dekoračných textíliách. Práve preto sa často berie ako jednoduchý materiál, ktorý znesie všetko. V praxi však rozhoduje typ výrobku.",
            "Bavlnené tričko, bavlnený uterák a bavlnená obliečka majú iné nároky. Tričko rieši potlač a tvar, uterák savosť a tvrdosť, obliečka veľkosť dávky a sušenie. Ak sa bavlna zráža, tvrdne alebo zapácha, problém často nie je v bavlne samotnej, ale v rutine prania.",
        ],
        "bullets": [
            "<strong>Tričká:</strong> prať naruby, triediť farby a chrániť potlač.",
            "<strong>Uteráky:</strong> nezahltiť avivážou, riešiť savosť a úplné sušenie.",
            "<strong>Obliečky:</strong> nepreplniť bubon a sušiť bez vlhkých záhybov.",
            "<strong>Detské veci:</strong> dávkovať jemne a dobre oplachovať.",
        ],
        "tables": [
            {
                "title": "Bavlna podľa typu textilu",
                "headers": ["Textil", "Najväčšie riziko", "Ako prať prakticky"],
                "rows": [
                    ("tričko", "zrážanie, potlač, strata tvaru", "naruby, podľa farby, šetrné sušenie"),
                    ("uterák", "tvrdosť a strata savosti", "primerané dávkovanie, dobrý oplach"),
                    ("obliečky", "preplnený bubon a zatuchnutie", "prať s priestorom, dosušiť"),
                    ("detské body", "zvyšky produktu pri pokožke", "jemné dávkovanie a oplach"),
                ],
            },
            {
                "title": "Kontrola problémov pri bavlne",
                "headers": ["Problém", "Častá príčina", "Riešenie"],
                "rows": [
                    ("bavlna sa zrazila", "teplo, sušička alebo prvé pranie", "dodržať štítok a sušiť miernejšie"),
                    ("uteráky sú tvrdé", "tvrdá voda, zvyšky produktu, presušenie", "upraviť dávku a oplach"),
                    ("obliečky zapáchajú", "pomalé sušenie alebo preplnený bubon", "rozložiť a vetrať"),
                    ("farba bledne", "trenie, vysoká teplota alebo zlé triedenie", "prať naruby a s podobnými farbami"),
                ],
            },
        ],
        "sections": [
            ("Ako prať bavlnené tričká a oblečenie", "Bavlnené tričká perte naruby, najmä ak majú potlač. Farby triedte a pri prvom praní buďte opatrnejší. Bavlna síce pôsobí odolne, ale potlač, elastan v lemoch alebo farbivo môžu byť citlivejšie než samotné vlákno. Preto sa neoplatí automaticky voliť najvyššiu teplotu.", "Ak sa tričko zráža, často je problém kombinácia tepla a sušenia. Pri nových kusoch pomôže šetrnejšie prvé pranie a vyhnutie sa horúcej sušičke, pokiaľ ju štítok jasne nepovoľuje."),
            ("Ako prať bavlnené uteráky", "Pri uterákoch je cieľ iný než pri tričkách. Potrebujete čistotu, savosť a úplné sušenie. Priveľa pracieho produktu alebo aviváže môže zhoršiť savosť. Uterák potom síce vonia, ale horšie saje a po čase môže tvrdnúť alebo zapáchať.", "Uteráky perte s priestorom v bubne a nechajte ich úplne vyschnúť. Ak sú tvrdé alebo zatuchnuté, pozrite si návod <a href=\"/n/ako-prat-uteraky-rady-a-tipy-na-ciste-a-maekke-uteraky\">ako prať uteráky</a>."),
            ("Ako prať bavlnené obliečky", "Bavlnené obliečky sú veľké kusy, ktoré sa v bubne ľahko zrolujú. Ak ich periete priveľa naraz, voda a prací produkt sa nemusia dostať rovnomerne všade. Výsledkom môže byť slabší oplach, zatuchnutie v záhyboch alebo tvrdší dotyk po vysušení.", "Obliečky po praní rozložte a sušte tak, aby nevznikli vlhké miesta v rohoch. Pri starostlivosti o posteľnú bielizeň nadväzuje článok <a href=\"/n/ako-spravne-prat-obliecky-kompletny-sprievodca-starostlivostou\">ako správne prať obliečky</a>."),
            ("Bavlna pri citlivej pokožke", "Bavlna je často prvá voľba pri detskom a citlivom textile, ale sama o sebe nerieši všetko. Pokožku môžu dráždiť zvyšky pracieho produktu, zle vypláchnutá aviváž, vlhkosť alebo farbivá. Pri citlivých členoch domácnosti preto dávkujte presne a zvážte extra oplach.", "Dôležité je aj úplné vysušenie. Mierne vlhká bavlna v skrini môže zatuchnúť a potom pôsobí nepríjemne aj vtedy, keď bola vypraná správne."),
            ("Ako bavlnu kombinovať s vôňou", "Bavlna dobre nesie čistý dojem, ale vôňa nemá nahradiť pranie. Ak bavlnené oblečenie alebo obliečky zapáchajú, najprv riešte príčinu: pot, preplnený bubon, slabý oplach alebo pomalé sušenie. Až potom dávajte vôňu podľa typu textilu.", "Pri posteľnej bielizni a detských veciach začnite jemnejšie. Pri uterákoch sledujte savosť. Ak vôňa alebo aviváž zhorší funkciu textilu, výsledok nebude praktický ani príjemný."),
        ],
        "box": ("Rýchla zásada", "Bavlna je univerzálna, ale nie všetka bavlna sa perie rovnako. Tričko, uterák, obliečka a detské body potrebujú odlišný dôraz."),
        "faq": [
            ("Môže sa bavlna zraziť?", "Áno. Najmä pri vyššej teplote, horúcej sušičke alebo nevhodnom prvom praní."),
            ("Prečo sú bavlnené uteráky tvrdé?", "Často pre zvyšky produktu, tvrdú vodu, presušenie alebo priveľa aviváže."),
            ("Ako prať bavlnené obliečky?", "S dostatočným priestorom v bubne, podľa štítku a s dôkladným sušením bez vlhkých záhybov."),
        ],
    },
}


DEPTH_SECTIONS = {
    "linen_vs_cotton": [
        (
            "Odbornejší pohľad: prečo sa ľan a bavlna správajú rozdielne",
            "Ľan aj bavlna patria medzi celulózové vlákna, preto dobre prijímajú vlhkosť a v domácnosti pôsobia prirodzene. Rozdiel je v stavbe vlákna a v tom, ako sa hotová tkanina správa pri ohybe, tlaku a sušení. Ľanové vlákno býva pevnejšie a menej pružné, preto záhyby drží viditeľnejšie. Bavlna pôsobí mäkšie a bežný spotrebiteľ ju často vníma ako jednoduchšiu na údržbu, ale pri uterákoch, posteľnej bielizni a tričkách stále rozhoduje kvalita väzby, hustota tkaniny a spôsob sušenia.",
            "Prakticky to znamená, že porovnanie ľan vs bavlna nemožno uzavrieť jednou vetou. Ak riešite letnú košeľu, výhodou ľanu je vzdušnosť a prirodzený vzhľad. Ak riešite každodenné obliečky pre viacčlennú domácnosť, bavlna môže byť jednoduchšia rutina. Ak riešite obrus, ľan pôsobí elegantne, ale škvrny a žehlenie treba plánovať dopredu.",
        ),
        (
            "Domáci test po praní: čo sledovať pri ďalšom cykle",
            "Po ďalšom praní si všimnite tri veci: dotyk, pach a tvar. Ak je ľan veľmi tvrdý, nemusí ísť o problém materiálu, ale o presušenie, zvyšky pracieho produktu alebo príliš plný bubon. Ak bavlnené obliečky zapáchajú aj po praní, častá príčina je vlhkosť ukrytá v zložených rohoch alebo slabé prúdenie vzduchu pri sušení.",
            "Dobrý postup je meniť vždy iba jednu premennú. Najprv znížte náplň bubna, potom upravte dávkovanie a až potom riešte program alebo vôňu. Tak zistíte, čo naozaj pomohlo. Pri prírodných tkaninách je pomalé a úplné sušenie často rovnako dôležité ako samotný prací cyklus.",
        ),
    ],
    "acrylic_material": [
        (
            "Odbornejší pohľad: prečo akryl nežmolkuje vždy rovnako",
            "Akryl je syntetické vlákno, ktoré sa pri oblečení používa najmä pre mäkký, hrejivý a vlnu pripomínajúci pocit. Žmolkovanie však nezávisí iba od názvu materiálu. Rozhoduje dĺžka vlákien, konštrukcia priadze, hustota úpletu, trenie pri nosení a spôsob prania. Lacnejší voľný úplet sa môže opotrebovať rýchlejšie než pevnejšie spracovaný akryl v zmesi.",
            "Pri praní preto neriešte akryl ako jeden univerzálny materiál. Inak sa správa hrubá deka, inak jemný kardigán a inak čiapka s elastickým lemom. Ak je v zložení aj vlna, alpaka, polyamid alebo elastan, postup vyberajte podľa najcitlivejšej zložky. Pri svetri býva šetrné mechanické zaobchádzanie dôležitejšie než samotná teplota uvedená v návode.",
        ),
        (
            "Domáci test po praní: tvar, statika a povrch",
            "Po vysušení akrylu skontrolujte ramená, rukávy, lemy a miesta trenia pod pazuchami alebo pri kabelke. Ak sa sveter vytiahol, pravdepodobne visel mokrý alebo bol po praní silno ťahaný. Ak elektrizuje, problém môže byť presušenie, suchý vzduch alebo kombinácia viacerých syntetických kusov v jednej dávke.",
            "Pri ďalšom praní skúste menšiu dávku, otočenie naruby a nižšie otáčky. Pri väčších dekách sledujte najmä oplach a sušenie. Ak deka zostane vlhká vo vnútri objemu, bude pôsobiť ťažko a zatuchnuto bez ohľadu na použitú vôňu.",
        ),
    ],
    "modal_lyocell_viscose": [
        (
            "Odbornejší pohľad: prečo sú celulózové materiály citlivé za mokra",
            "Modal, lyocell aj viskóza patria medzi regenerované celulózové vlákna. V praxi to znamená, že majú príjemný dotyk, dobrú priedušnosť a často pôsobia mäkko na pokožke, no pri vode a mechanickom namáhaní sa nesprávajú rovnako ako polyester alebo pevná bavlna. Mokrý kus môže byť citlivejší na ťah, skrútenie a deformáciu.",
            "Rozdiely medzi týmito materiálmi sú dôležité, ale ešte dôležitejší je hotový výrobok. Jemná viskózová blúzka potrebuje inú rutinu než lyocellové obliečky alebo modalové pyžamo. Pri oblečení so zmesou elastanu sledujte aj pružné časti, pretože nevhodné teplo môže zhoršiť tvar aj pri materiáli, ktorý sa inak perie dobre.",
        ),
        (
            "Domáci test po praní: mäkkosť, ťah a vôňa pri pokožke",
            "Po praní skontrolujte, či materiál nestvrdol, nevytiahol sa v ramenách a či nemá zatuchnutý tón. Tvrdší dotyk často nevzniká preto, že by bol modal alebo lyocell nekvalitný, ale pre zvyšky pracieho produktu, slabý oplach alebo pomalé sušenie. Pri pyžame a spodných vrstvách je dôležité aj to, aby vôňa nebola príliš intenzívna.",
            "Ak chcete zlepšiť výsledok, najprv perte menšiu dávku podobne jemných kusov. Potom znížte mechanické trenie, vyberte bielizeň hneď po programe a sušte bez ťahu. Až keď je materiál čistý, mäkký a suchý, má zmysel doladiť vôňu podľa osobnej preferencie.",
        ),
    ],
    "organic_cotton": [
        (
            "Odbornejší pohľad: čo znamená organická bavlna pri údržbe",
            "Organická bavlna je dôležitá informácia pri výbere textilu, ale v práčke sa stále správate k hotovému výrobku. Pestovanie a certifikácia hovoria o pôvode a pravidlách výroby, no domáce riziká sú veľmi podobné ako pri bežnej bavlne: zrážanie, blednutie, tvrdnutie, zvyšky pracieho produktu a vlhkosť po nedosušenom praní.",
            "Pri detskom oblečení alebo posteľnej bielizni preto nestačí sledovať iba slovo organická. Všímajte si aj farbivá, potlač, hrúbku úpletu, švy, zapínanie a odporúčanie výrobcu. Ak chcete šetrnejší výsledok, najväčší rozdiel často spraví presné dávkovanie a dôkladný oplach, nie dramaticky odlišný program.",
        ),
        (
            "Domáci test po praní: pokožka, dotyk a zvyšky produktu",
            "Pri organickej bavlne používanej na pyžamá, body alebo obliečky sledujte, či po praní nepôsobí drsne a či pri pokožke neostáva príliš výrazná vôňa. Ak je textil tvrdý, môže ísť o tvrdú vodu, preplnený bubon alebo priveľa pracieho produktu. Ak je cítiť zatuchnutie, najčastejšie chýbalo rýchle a úplné sušenie.",
            "Dobrý kontrolný postup je jednoduchý: perte menšiu dávku, nastavte primeraný oplach a bielizeň po dosušení ovoňajte až po niekoľkých hodinách v skrini. Ak sa nepríjemný pach vráti, problém pravdepodobne nie je v pôvode bavlny, ale v procese prania alebo skladovania.",
        ),
    ],
    "cotton_material": [
        (
            "Odbornejší pohľad: prečo bavlna nie je jeden typ prania",
            "Bavlna je materiál, ktorý v domácnosti pôsobí samozrejme, no jej správanie sa mení podľa väzby, gramáže, úpletu a úpravy. Froté uterák má iné očakávania než hladké obliečky, tričko s potlačou alebo detské body. Pri jednom kuse riešite savosť, pri inom tvar, pri ďalšom farbu alebo dotyk pri pokožke.",
            "Preto je chyba prať všetku bavlnu rovnako. Uteráky potrebujú dôkladný oplach a úplné sušenie, obliečky dostatok priestoru v bubne, tričká ochranu potlače a detské veci jemné dávkovanie. Ak bavlna zlyháva, často nejde o zlý materiál, ale o nesprávne zvolenú rutinu pre konkrétny typ textilu.",
        ),
        (
            "Domáci test po praní: čo vám povie bavlna po vysušení",
            "Po vysušení si všimnite štyri signály: tvrdosť, pach, rozmer a savosť. Tvrdosť môže znamenať zvyšky produktu alebo presušenie. Pach môže ukazovať na pomalé sušenie alebo preplnený bubon. Zmena rozmeru často súvisí s teplom a savosť uterákov vie zhoršiť nadmerná aviváž alebo film z prípravkov.",
            "Ak chcete výsledok zlepšiť, nerobte všetko naraz. Pri uterákoch najprv znížte aviváž a upravte oplach. Pri obliečkach zmenšite dávku v bubne. Pri tričkách perte naruby a šetrnejšie sušte. Tak zistíte, ktorá zmena mala reálny efekt a nebudete zbytočne meniť funkčnú časť rutiny.",
        ),
    ],
}


def build_expansion(topic):
    config = TOPICS[topic]
    tables_html = "\n".join(
        f"<h2>{tbl['title']}</h2>\n{table(tbl['headers'], tbl['rows'])}" for tbl in config["tables"]
    )
    sections_html = "\n".join(
        f"<h2>{title}</h2>\n<p>{first}</p>\n<p>{second}</p>"
        for title, first, second in config["sections"]
    )
    depth_html = "\n".join(
        f"<h2>{title}</h2>\n<p>{first}</p>\n<p>{second}</p>"
        for title, first, second in DEPTH_SECTIONS[topic]
    )
    faq_html = "\n".join(f"<h3>{question}</h3>\n<p>{answer}</p>" for question, answer in config["faq"])
    box_title, box_text = config["box"]
    return clean(
        f"""
        <h2>{config["marker"]}</h2>
        <p>{config["intro"][0]}</p>
        <p>{config["intro"][1]}</p>
        {note_card("Rýchla praktická diagnostika", config["bullets"])}
        {tables_html}
        {sections_html}
        {depth_html}
        <div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">
        <h2 style="margin-top: 0;">{box_title}</h2>
        <p>{box_text}</p>
        </div>
        {product_card(config["product_kind"])}
        <h2>FAQ: praktické otázky</h2>
        {faq_html}
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
    parser = argparse.ArgumentParser(description="Conservatively expand VEVO retrofit wave 11 natural materials articles.")
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
            original_title = article.get("title")
            original_short = article.get("short", "")
            original_url = article.get("url")
            original_long = article["long"]
            article["long"] = insert_expansion(article["long"], config["topic"])
            if article.get("title") != original_title or article_slug(article) != config["slug"] or article.get("short", "") != original_short:
                raise SystemExit(f"Retrofit attempted to change title, slug, or short field for {config['slug']}")
            if original_url and article.get("url") != original_url:
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
                "wave": "retrofit-wave-11-natural-materials-five",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "purpose": "Conservative additive expansion. Titles, slugs, URLs, and short descriptions are preserved.",
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
            mcp_updates.append(
                {
                    "post_id": item["post_id"],
                    "slug": item["slug"],
                    "url": item["url"],
                    "mcp_result": result.get("result", result),
                }
            )
            time.sleep(args.sleep)

    MCP_RESULTS.write_text(
        json.dumps(
            {
                "project": "VEVO_CONTENT",
                "wave": "retrofit-wave-11-natural-materials-five",
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
