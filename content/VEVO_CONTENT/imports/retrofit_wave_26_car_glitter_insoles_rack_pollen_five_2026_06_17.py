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
OUT_JSON = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-26-car-glitter-insoles-rack-pollen-five-2026-06-17.json"
MCP_RESULTS = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-26-car-glitter-insoles-rack-pollen-five-2026-06-17-mcp-results.json"


ARTICLES = [
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-19-2026-06-10-articles.json",
        "slug": "ako-vycistit-navlek-na-autosedacku-po-zime-a-posypovej-soli",
        "post_id": "2212",
        "url": "https://www.vevo.sk/n/ako-vycistit-navlek-na-autosedacku-po-zime-a-posypovej-soli",
        "topic": "car_seat_cover",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-17-2026-06-10-articles.json",
        "slug": "ako-odstranit-trblietky-z-siat-saka-a-kabata-po-oslave",
        "post_id": "2181",
        "url": "https://www.vevo.sk/n/ako-odstranit-trblietky-z-siat-saka-a-kabata-po-oslave",
        "topic": "glitter",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-19-2026-06-10-articles.json",
        "slug": "ako-vycistit-textilne-vlozky-do-topanok-po-zime",
        "post_id": "2210",
        "url": "https://www.vevo.sk/n/ako-vycistit-textilne-vlozky-do-topanok-po-zime",
        "topic": "shoe_insoles",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-19-2026-06-10-articles.json",
        "slug": "ako-vycistit-susiak-na-bielizen-aby-neprenasal-spinu-na-pradlo",
        "post_id": "2223",
        "url": "https://www.vevo.sk/n/ako-vycistit-susiak-na-bielizen-aby-neprenasal-spinu-na-pradlo",
        "topic": "drying_rack",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-19-2026-06-10-articles.json",
        "slug": "ako-striast-pel-z-bundy-a-mikiny-po-prechadzke-pred-pranim",
        "post_id": "2221",
        "url": "https://www.vevo.sk/n/ako-striast-pel-z-bundy-a-mikiny-po-prechadzke-pred-pranim",
        "topic": "pollen",
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
    items += '\n<li><a href="/n/preco-moje-oblecenie-zapacha-po-prani-priciny-a-riesenia">Prečo oblečenie zapácha po praní</a></li>'
    return f"<h2>Súvisiace návody na VEVO</h2>\n<ul>\n{items}\n</ul>"


TOPICS = {
    "car_seat_cover": {
        "marker": "Detailnejší postup na návlek autosedačky po zime",
        "problem": "Návlek na autosedačku po zime zachytí posypovú soľ, piesok, vlhkosť z búnd, omrvinky a pach z uzavretého auta. Zároveň však nejde len o obyčajný poťah: pri detskej autosedačke treba rešpektovať návod výrobcu a bezpečnostné časti.",
        "scope": "snímateľný návlek, poťah autosedačky, detskú vložku, popruhy, výplne, švy, plastové časti, mapy od soli, omrvinky a miesta pri topánkach",
        "avoid": "premáčanie výplní, pranie bezpečnostných popruhov bez súhlasu výrobcu, agresívne čistenie peny, horúcu sušičku, nasadenie vlhkého poťahu späť a silnú vôňu namiesto odstránenia soli",
        "diagnosis": [
            "<strong>Najprv vysávať:</strong> soľ, piesok a omrvinky musia ísť preč pred vodou.",
            "<strong>Návod výrobcu má prednosť:</strong> pri autosedačke nejde iba o vzhľad textilu.",
            "<strong>Soľné mapy čistite lokálne:</strong> zbytočne nepremáčajte celú výplň.",
            "<strong>Poťah vráťte až suchý:</strong> vlhkosť v aute rýchlo vytvára zatuchnutie.",
        ],
        "state_rows": [
            ("soľ a piesok", "povysávať nasucho", "pred vlhčením"),
            ("mapa od soli", "lokálne pretrieť a odsávať vlhkosť", "nepremáčať"),
            ("popruhy", "postup len podľa výrobcu", "bezpečnostný prvok"),
            ("snímateľný poťah", "prať iba ak je to povolené", "štítok a návod"),
        ],
        "textile_rows": [
            ("detská vložka", "jemne a úplne vysušiť", "kontakt s dieťaťom"),
            ("ochranný návlek", "lokálne alebo podľa štítku", "môže mať výstuž"),
            ("poťah sedadla", "nepremáčať penu", "dlhé schnutie"),
            ("popruhy a pracky", "neprať naslepo", "bezpečnostná funkcia"),
        ],
        "sections": [
            ("Návlek na autosedačku po zime: prvý krok", "Návlek najprv povysávajte. Suchá soľ, piesok a omrvinky sa pri vode rozpustia alebo roznesú do väčšej plochy. Vysávanie zníži množstvo špiny, ktorú by ste inak tlačili do textilu.", "Pri detskej autosedačke najprv pozrite návod výrobcu. Niektoré časti sú prateľné, iné sa majú čistiť iba povrchovo."),
            ("Ako odstrániť mapy od posypovej soli", "Soľné mapy čistite lokálne mierne vlhkou handričkou a malým množstvom jemného pracieho roztoku. Potom miesto pretrite čistou vlhkou handričkou, aby v látke nezostali zvyšky.", "Neaplikujte veľa vody naraz. Pena a výplň pod poťahom schnú pomaly a vlhkosť v aute sa ľahko zmení na zatuchnutie."),
            ("Čo nerobiť s popruhmi a bezpečnostnými časťami", "Popruhy, pracky a bezpečnostné prvky neperte ani nenamáčajte bez výslovného pokynu výrobcu. Čistiaci postup, ktorý vyzerá bezpečne pre tričko, nemusí byť vhodný pre autosedačku.", "Ak máte pochybnosť, držte sa návodu k modelu autosedačky. Cieľom je čistota bez zásahu do funkcie."),
            ("Kedy prať celý poťah", "Celý poťah perte iba vtedy, keď je snímateľný, štítok alebo návod to povoľuje a viete ho bezpečne vysušiť. Pred praním zapnite suché zipsy, odoberte tvrdé časti a skontrolujte švy.", "Ak je problém iba lokálna mapa od soli, celé pranie nemusí byť potrebné."),
            ("Sušenie pred nasadením späť do auta", "Poťah alebo návlek nasaďte späť až úplne suchý. V aute sa vlhkosť drží dlhšie než v miestnosti, najmä pri nízkych teplotách a zatvorených dverách.", "Skontrolujte švy, rohy a miesta pri výplni. Práve tam ostáva vlhkosť najdlhšie."),
            ("Prevencia na ďalšiu zimu", "V zime pomáha pravidelne vysávať sedadlá, používať ochranný návlek a nenechávať mokré textílie v aute celé dni. Soľ a vlhkosť riešte priebežne, nie až na jar.", "Súvisiaci návod je <a href=\"/n/ako-vycistit-rohozku-a-textilie-v-predsieni-od-posypovej-soli\">ako vyčistiť rohožku a textílie od posypovej soli</a>."),
        ],
        "expert_title": "Odbornejší pohľad: soľ, vlhkosť a bezpečnostná konštrukcia",
        "expert_p1": "Posypová soľ priťahuje vlhkosť a po vyschnutí zanechá mapy. V autosedačke sa navyše kombinuje s pieskom, prachom, jedlom a teplotnými zmenami. Preto je čistenie iba vodou často málo presné.",
        "expert_p2": "Pri autosedačke však treba myslieť na konštrukciu. Textil je len jedna časť. Výplne, popruhy a plastové prvky môžu mať vlastné limity čistenia.",
        "checklist": "Pred čistením skontrolujte návod výrobcu, snímateľnosť poťahu, popruhy, výplne, soľné mapy, omrvinky, piesok, švy, možnosť úplného sušenia a to, či ide o detskú bezpečnostnú autosedačku alebo len ochranný návlek.",
        "rule": "Pri návleku na autosedačku najprv vysať soľ a piesok, potom čistiť lokálne a celý poťah prať iba podľa návodu.",
        "recommendation_intro": "Pri prateľných textilných častiach autosedačky má jemný prací gél zmysel až po vysatí soli, piesku a omrviniek.",
        "product_text": "Vhodný na šetrné pranie snímateľných prateľných návlekov podľa štítku a návodu výrobcu. Bezpečnostné popruhy čistite iba podľa odporúčania výrobcu.",
        "category_text": "Pri poťahoch a návlekoch vyberajte prací gél podľa materiálu, potreby oplachu a toho, či textil prichádza do kontaktu s dieťaťom.",
        "links": [
            ("/n/ako-vycistit-rohozku-a-textilie-v-predsieni-od-posypovej-soli", "Ako vyčistiť textílie od posypovej soli"),
            ("/n/ako-prat-textilne-navleky-na-kocik-po-prechadzke-v-dazdi", "Ako prať textilné návleky na kočík po daždi"),
            ("/n/ako-vycistit-bubon-pracky-po-prani-pelechu-topanok-alebo-pracovnych-veci", "Ako vyčistiť bubon práčky po špinavších veciach"),
        ],
        "faq": [
            ("Môžem prať poťah detskej autosedačky v práčke?", "Iba ak to povoľuje návod výrobcu a štítok. Pri autosedačke vždy rešpektujte bezpečnostnú konštrukciu."),
            ("Ako dostať soľnú mapu z návleku?", "Najprv vysať suché zvyšky, potom lokálne pretrieť mierne vlhkou handričkou a dobre vysušiť."),
            ("Môžem čistiť aj popruhy?", "Len podľa návodu výrobcu. Popruhy sú bezpečnostný prvok a nemajú sa prať naslepo."),
        ],
    },
    "glitter": {
        "marker": "Detailnejší postup na trblietky zo šiat, saka a kabáta",
        "problem": "Trblietky po oslave sú mechanický problém. Ak ich namočíte alebo dáte rovno do práčky, roznesú sa po ďalšom oblečení, zachytia sa v švoch a môžu skončiť aj v práčke.",
        "scope": "šaty, sako, kabát, podšívku, golier, ramená, rukávy, kabelku, čalúnenie po oslave, flitre, glitre a jemné spoločenské látky",
        "avoid": "mokré rozotieranie glitrov, pranie saka alebo kabáta bez štítku, tvrdú kefu na jemnej látke, sušičku pred kontrolou a miešanie trblietkových vecí s tmavou bielizňou",
        "diagnosis": [
            "<strong>Najprv nasucho:</strong> valček, jemná páska alebo vytrasenie odstráni viac než voda.",
            "<strong>Sako a kabát nie sú tričko:</strong> často potrebujú lokálne čistenie alebo čistiareň.",
            "<strong>Trblietky sa šíria:</strong> pracujte nad vaňou, papierom alebo vonku.",
            "<strong>Vôňa až po čistení:</strong> parfum neodstráni mechanické častice.",
        ],
        "state_rows": [
            ("voľné trblietky", "valček alebo jemná páska", "nasucho"),
            ("trblietky v švoch", "opatrne po malých úsekoch", "nevtláčať"),
            ("sako alebo kabát", "podľa štítku, často neperieť", "tvar a podšívka"),
            ("šaty po oslave", "najprv mechanicky, potom prať", "ak to štítok dovolí"),
        ],
        "textile_rows": [
            ("spoločenské šaty", "valček a jemné pranie podľa štítku", "chráni povrch"),
            ("sako", "lokálne alebo čistiareň", "držanie tvaru"),
            ("kabát", "vytriasť a kefovať jemne", "nepremáčať"),
            ("podšívka", "nízka mechanika", "ľahko sa zatrhne"),
        ],
        "sections": [
            ("Ako odstrániť trblietky zo šiat", "Šaty najprv vytraste nad vaňou alebo vonku. Potom použite valček na textil a prechádzajte látku jedným smerom. Pri jemnej látke netlačte, aby sa povrch nezaleskol alebo nezatrhol.", "Ak štítok pranie povoľuje, perte až po mechanickom odstránení väčšiny trblietok."),
            ("Trblietky na saku", "Sako má tvar, výstuž a podšívku, preto nepatrí automaticky do práčky. Trblietky odstráňte valčekom alebo jemnou páskou a sledujte najmä ramená, chrbát a rukávy.", "Ak je sako zároveň cítiť potom alebo jedlom, riešte pach lokálne a podľa štítku. Pri drahšom kuse je bezpečnejšia čistiareň."),
            ("Glitre na kabáte", "Kabát vytraste vonku a jemne prejdite kefou alebo valčekom. Nepoužívajte mokrú handru ako prvý krok, pretože trblietky sa môžu prilepiť hlbšie k vláknam.", "Pri vlnenom alebo zmesovom kabáte netlačte pásku agresívne. Mohli by ste vytiahnuť vlas alebo zmeniť povrch."),
            ("Ako ochrániť práčku pred trblietkami", "Do práčky nedávajte oblečenie, z ktorého stále padajú trblietky. Mechanické častice sa môžu preniesť na ďalšiu dávku alebo zostať v tesnení.", "Po praní trblietkového kúsku skontrolujte bubon a tesnenie, podobne ako pri chlpových alebo papierových zvyškoch."),
            ("Trblietky verzus flitre a korálky", "Voľné trblietky riešite odstránením z povrchu. Flitre a korálky sú súčasťou odevu a potrebujú ochranu pred zatrhnutím. Súvisiaci detail je <a href=\"/n/ako-prat-oblecenie-s-flitrami-koralkami-a-aplikaciami\">ako prať oblečenie s flitrami a aplikáciami</a>.", "Ak sa dekorácia odlepuje, pranie môže situáciu zhoršiť."),
            ("Po oslave: pach, dym a parfum", "Ak odev po oslave zapácha, najprv ho vyvetrajte a odstráňte trblietky. Až potom riešte lokálne pot, dym alebo jedlo. Silná vôňa na glitre a pot vytvorí ťažší dojem.", "Textil má byť čistý, nie iba prevoňaný."),
        ],
        "expert_title": "Odbornejší pohľad: prečo trblietky nepatria hneď do práčky",
        "expert_p1": "Trblietky sú malé pevné častice. Pranie ich nerozpustí ako bežnú špinu, skôr ich rozptýli. Preto je najdôležitejšia mechanická fáza pred praním.",
        "expert_p2": "Pri spoločenskom oblečení sa navyše stretáva jemný povrch, podšívka, tvar a dekorácie. Šetrné odstraňovanie po malých úsekoch je bezpečnejšie než jeden agresívny zásah.",
        "checklist": "Pred praním skontrolujte množstvo trblietok, štítok, podšívku, ozdoby, typ látky, ramená, švy, vrecká, bubon práčky a to, či oblečenie vôbec patrí do práčky.",
        "rule": "Pri trblietkach najprv mechanicky nasucho, potom až pranie alebo lokálne čistenie podľa materiálu.",
        "recommendation_intro": "Ak je odev po odstránení trblietok prateľný, použite šetrné pranie podľa štítku a nízku mechaniku.",
        "product_text": "Vhodný na následné pranie prateľných šiat alebo textílií po tom, čo sú voľné trblietky odstránené z povrchu.",
        "category_text": "Pri spoločenských a jemných textíliách vyberajte prací gél až po kontrole štítku. Nie každý kus patrí do práčky.",
        "links": [
            ("/n/ako-prat-oblecenie-s-flitrami-koralkami-a-aplikaciami", "Ako prať oblečenie s flitrami a aplikáciami"),
            ("/n/ako-prat-spolocenske-saty-doma-a-kedy-zvolit-cistiaren", "Ako prať spoločenské šaty doma"),
            ("/n/ako-dostat-kusky-papierovej-vreckovky-z-ciernych-nohavic-a-mikiny", "Ako dostať papierovú vreckovku z oblečenia"),
        ],
        "faq": [
            ("Môžem dať oblečenie s trblietkami rovno do práčky?", "Radšej nie. Najprv odstráňte čo najviac trblietok nasucho."),
            ("Ako dostať trblietky zo saka?", "Použite valček alebo jemnú pásku a rešpektujte štítok. Sako často nepatrí do bežného prania."),
            ("Čo ak sú trblietky v práčke?", "Skontrolujte bubon a tesnenie, utrite ich a ďalšiu dávku perte až po kontrole."),
        ],
    },
    "shoe_insoles": {
        "marker": "Detailnejší postup na textilné vložky do topánok po zime",
        "problem": "Textilné vložky do topánok po zime držia pot, posypovú soľ, vlhkosť a pach z uzavretej obuvi. Sú malé, ale často rozhodujú o tom, či predsieň a šatník pôsobia čisto.",
        "scope": "textilné vložky, penové vrstvy, tvarované vložky, zimné topánky, soľ, prach, pot, pach, sušenie mimo radiátora a vrátenie vložiek späť do obuvi",
        "avoid": "vrátenie vlhkých vložiek do topánok, horúce sušenie na radiátore, pranie kožených alebo špeciálnych ortopedických vložiek bez návodu, silnú vôňu namiesto čistenia a drhnutie penovej vrstvy",
        "diagnosis": [
            "<strong>Vložky vždy vyberte:</strong> v topánke nevyschnú ani sa nevyčistia dobre.",
            "<strong>Soľ najprv vykefujte:</strong> suchý minerálny zvyšok nepatrí do vody ako prvý krok.",
            "<strong>Pach je vo vlhkosti:</strong> vrátenie nedosušených vložiek problém obnoví.",
            "<strong>Materiál rozhoduje:</strong> textil, pena, koža a ortopedické vložky nečistite rovnako.",
        ],
        "state_rows": [
            ("vlhké vložky", "vybrať a sušiť voľne", "nie v topánke"),
            ("soľný povlak", "vykefovať nasucho", "pred praním"),
            ("pach", "jemné čistenie a úplné sušenie", "zdroj je často vo vložke"),
            ("tvarovaná vložka", "podľa návodu", "nekrútiť"),
        ],
        "textile_rows": [
            ("textilná vložka", "ručné čistenie alebo jemné pranie podľa materiálu", "nižšie riziko deformácie"),
            ("penová vložka", "nežmýkať agresívne", "ľahko stratí tvar"),
            ("kožená vložka", "nenamáčať ako textil", "materiál môže stvrdnúť"),
            ("ortopedická vložka", "postup podľa výrobcu", "funkčný tvar"),
        ],
        "sections": [
            ("Ako vybrať a pripraviť vložky do topánok", "Vložky vyberte z topánok a nechajte ich preschnúť. Ak sú tvarované, odfoťte si ich uloženie, aby ste ich po vyčistení vrátili správne.", "Suchú soľ a prach odstráňte mäkkou kefkou. Až potom riešte vlhké čistenie."),
            ("Ako odstrániť soľ z textilných vložiek", "Soľné stopy čistite mierne vlhkou handričkou alebo ručne v malom množstve vody s jemným pracím roztokom. Cieľom nie je vložku premočiť, ale dostať z povrchu minerálne zvyšky.", "Po čistení zvyšky roztoku odstráňte čistou vlhkou handričkou a vložku nechajte voľne schnúť."),
            ("Textilné vložky zapáchajú: čo skontrolovať", "Pach najčastejšie vzniká z potu, vlhkosti a uzavretej topánky. Ak vložky vrátite späť mierne vlhké, zápach sa vráti veľmi rýchlo.", "Pomáha pravidelné vyberanie vložiek, striedanie topánok a úplné sušenie mimo priameho prehriatia."),
            ("Kedy vložky neprať", "Kožené, ortopedické alebo špeciálne tvarované vložky neperte ako obyčajný textil. Môžu stvrdnúť, zmeniť tvar alebo stratiť funkciu.", "Ak si nie ste istí materiálom, zvoľte povrchové čistenie a postup podľa výrobcu."),
            ("Sušenie bez deformácie", "Vložky nesušte priamo na horúcom radiátore. Teplo môže zdeformovať penu, lepidlá alebo tvarovanú časť. Lepšie je prúdenie vzduchu a čas.", "Do topánok ich vráťte až úplne suché. Inak bude pach aj vlhkosť späť v uzavretom priestore."),
            ("Ako vyčistiť topánky spolu s vložkami", "Ak zapáchajú vložky, často zapácha aj vnútro topánky. Vyvetrajte obuv, vyberte šnúrky, povysávajte prach a kontrolujte textilnú podšívku.", "Súvisiaci návod je <a href=\"/n/ako-odstranit-zapach-z-ponoziek-a-sportovej-obuvi-po-treningu\">ako odstrániť zápach z ponožiek a športovej obuvi</a>."),
        ],
        "expert_title": "Odbornejší pohľad: uzavretá obuv, vlhkosť a pach",
        "expert_p1": "Vo vložke sa stretáva pot, teplo, tlak chodidla a obmedzené prúdenie vzduchu. Preto malý textilný diel môže zapáchať výraznejšie než veľký kus oblečenia.",
        "expert_p2": "Soľ po zime navyše mení povrchový pocit a drží vlhkosť. Účinné čistenie je kombinácia suchého odstránenia soli, mierneho čistenia a úplného sušenia.",
        "checklist": "Pred čistením skontrolujte materiál vložky, tvarovanie, soľný povlak, pach, penovú vrstvu, kožené časti, návod výrobcu, možnosť úplného sušenia a stav samotnej topánky.",
        "rule": "Pri vložkách do topánok najprv vybrať, vysušiť, vykefovať soľ a až potom čistiť mierne podľa materiálu.",
        "recommendation_intro": "Pri textilných vložkách používajte malé množstvo pracieho roztoku a dôkladné sušenie. Vôňa nenahradí odstránenie vlhkosti.",
        "product_text": "Vhodný na jemné ručné čistenie textilnej vrstvy vložiek podľa materiálu. Pri kožených, penových a ortopedických vložkách rešpektujte návod výrobcu.",
        "category_text": "Pri malých textíliách v obuvi používajte prací gél úsporne. Príliš veľa prostriedku sa horšie odstraňuje a môže držať pach.",
        "links": [
            ("/n/ako-odstranit-zapach-z-ponoziek-a-sportovej-obuvi-po-treningu", "Ako odstrániť zápach z ponožiek a športovej obuvi"),
            ("/n/ako-vycistit-rohozku-a-textilie-v-predsieni-od-posypovej-soli", "Ako vyčistiť textílie od posypovej soli"),
            ("/n/ako-vycistit-bubon-pracky-po-prani-pelechu-topanok-alebo-pracovnych-veci", "Ako vyčistiť bubon po špinavších veciach"),
        ],
        "faq": [
            ("Môžem vložky do topánok vyprať v práčke?", "Iba ak to dovoľuje materiál a výrobca. Pri pene, koži a ortopedických vložkách je bezpečnejšie ručné čistenie."),
            ("Prečo vložky stále zapáchajú?", "Pravdepodobne nevyschli úplne alebo je pach aj v podšívke topánky."),
            ("Môžem ich sušiť na radiátore?", "Radšej nie priamo na horúcom zdroji. Teplo môže zdeformovať penu alebo lepené vrstvy."),
        ],
    },
    "drying_rack": {
        "marker": "Detailnejší postup na sušiak na bielizeň",
        "problem": "Sušiak na bielizeň sa dotýka čistého prádla, ale často stojí na podlahe, balkóne alebo v kúpeľni. Prach, hrdza, zvyšky pracieho prostriedku a špina z nožičiek sa potom prenesú na oblečenie až po praní.",
        "scope": "tyčky sušiaka, spoje, nožičky, plastové krytky, hrdzu, prach, balkónové sušenie, peľ, špinu z podlahy a čisté prádlo po praní",
        "avoid": "vešanie bieleho prádla na hrdzavé tyčky, používanie lepivého sušiaka, čistenie až po vzniku fľakov, odloženie mokrého sušiaka do kúta a sušenie počas silného peľu bez kontroly",
        "diagnosis": [
            "<strong>Tyčky musia byť čisté:</strong> prádlo sa ich dotýka celou váhou.",
            "<strong>Skontrolujte spoje:</strong> tam sa drží prach, hrdza aj zvyšky vody.",
            "<strong>Nožičky prenášajú špinu:</strong> podlaha a balkón sa dostanú na čisté prádlo nepriamo.",
            "<strong>Balkónový sušiak čistite častejšie:</strong> zachytáva peľ, prach a exteriérové nečistoty.",
        ],
        "state_rows": [
            ("prach na tyčkách", "utrieť vlhkou a suchou handrou", "pred vešaním"),
            ("hrdza", "nepoužiť na biele prádlo", "riziko fľakov"),
            ("lepivé miesto", "odstrániť zvyšky prostriedku", "prenáša sa na textil"),
            ("balkónový sušiak", "čistiť po peľovej a prašnej fáze", "vonkajší prach"),
        ],
        "textile_rows": [
            ("biele oblečenie", "vešať len na čisté tyčky", "hrdza je viditeľná"),
            ("uteráky", "sušiak musí byť stabilný a čistý", "vyššia váha"),
            ("jemná bielizeň", "bez drsných alebo hrdzavých miest", "riziko zatrhnutia"),
            ("posteľná bielizeň", "čisté dlhé plochy", "veľký kontakt so sušiakom"),
        ],
        "sections": [
            ("Ako vyčistiť sušiak pred veľkým praním", "Rozložte sušiak a utrite všetky tyčky vlhkou handričkou. Potom ich pretrite suchou handrou, aby na prádle nezostala voda ani zvyšky špiny.", "Nezabudnite na spodnú stranu tyčiek. Prádlo sa pri vešaní často posunie a dotkne sa aj miest, ktoré nevidíte zhora."),
            ("Hrdza na sušiaku a biele prádlo", "Ak má sušiak hrdzavé miesta, nevešajte na ne biele tričká, obliečky ani uteráky. Hrdzavý fľak vznikne až po praní a vyzerá, akoby sa oblečenie zašpinilo v práčke.", "Súvisiaci návod je <a href=\"/n/ako-odstranit-hrdzave-flaky-od-stipcov-a-snury-na-bielizen\">ako odstrániť hrdzavé fľaky od štipcov a šnúry</a>."),
            ("Nožičky, spoje a špina z podlahy", "Nožičky sušiaka stoja na podlahe, balkóne alebo v kúpeľni. Keď sušiak skladáte a rozkladáte, špina z nožičiek sa môže dostať na tyčky alebo na prádlo.", "Pri čistení preto prejdite aj nožičky, plastové krytky a spoje. Práve tam sa drží prach a vlhkosť."),
            ("Balkónový sušiak, peľ a prach", "Ak sušíte vonku, sušiak zachytáva peľ, prach a špinu zo vzduchu. Pred vešaním čistého prádla ho utrite, najmä počas jari, stavebných prác alebo po veternom dni.", "Pri alergikoch myslite aj na to, že vonku sušené prádlo môže niesť peľ späť do bytu."),
            ("Lepivé zvyšky aviváže alebo gélu", "Na sušiaku sa môžu časom objaviť lepivé miesta zo zvyškov pracích produktov alebo z nedosušeného textilu. Také miesto môže preniesť mapu na čisté prádlo.", "Utrite ho jemným roztokom, potom čistou vodou a nakoniec dosucha."),
            ("Ako často sušiak čistiť", "Pri bežnom používaní stačí rýchla kontrola pred väčším praním. Ak sušiak stojí na balkóne, v pivnici alebo pri domácich miláčikoch, čistite ho častejšie.", "Sušiak je súčasť prania. Ak je špinavý, čisté prádlo sa zašpiní až po vybratí z práčky."),
        ],
        "expert_title": "Odbornejší pohľad: čisté prádlo sa môže zašpiniť až pri sušení",
        "expert_p1": "Pranie sa nekončí vypnutím práčky. Textil je po praní vlhký, ťažší a viac sa dotýka tyčiek sušiaka. Ak je povrch prašný, hrdzavý alebo lepivý, ľahko prenesie stopu na vlákna.",
        "expert_p2": "Zdroj fľaku preto nemusí byť prací gél ani práčka. Niekedy je problém až v sušení: sušiak, štipce, šnúra, balkónový prach alebo hrdza.",
        "checklist": "Pred vešaním skontrolujte tyčky, spoje, nožičky, hrdzu, lepivé miesta, prach, balkónové nečistoty, peľ, stabilitu sušiaka a to, či sa čisté prádlo nebude dotýkať špinavých častí.",
        "rule": "Sušiak čistite ako súčasť prania. Na špinavý alebo hrdzavý povrch nepatrí čisté prádlo.",
        "recommendation_intro": "Pri fľakoch po praní treba skontrolovať aj sušenie. Prací produkt pomôže bielizni, ale špinavý sušiak ju môže zašpiniť znova.",
        "product_text": "Vhodný na bežné pranie textílií, pri ktorých potom kontrolujete aj čistý sušiak, šnúru a štipce, aby sa fľaky nevrátili po praní.",
        "category_text": "Pri opakovaných fľakoch sledujte celý proces: dávkovanie, oplach, práčku aj miesto sušenia.",
        "links": [
            ("/n/ako-odstranit-hrdzave-flaky-od-stipcov-a-snury-na-bielizen", "Ako odstrániť hrdzavé fľaky od štipcov"),
            ("/n/casto-kladene-otazky-skvrny-na-obleceni-po-prani", "Škvrny na oblečení po praní"),
            ("/n/preco-je-bielizen-po-prani-tvrda-alebo-lepkava-zvysky-gelu-davkovanie-a-oplach", "Zvyšky gélu, dávkovanie a oplach"),
        ],
        "faq": [
            ("Ako často čistiť sušiak?", "Pred väčším praním ho aspoň skontrolujte. Balkónový alebo prašný sušiak čistite častejšie."),
            ("Môže sušiak robiť fľaky na prádle?", "Áno. Hrdza, prach alebo lepivé miesta sa môžu preniesť na čistý vlhký textil."),
            ("Čo robiť s hrdzavým sušiakom?", "Hrdzavé miesta nepoužívajte na biele prádlo a zvážte výmenu alebo opravu poškodených častí."),
        ],
    },
    "pollen": {
        "marker": "Detailnejší postup na peľ z bundy a mikiny",
        "problem": "Peľ na bunde alebo mikine je jemný prášok. Keď ho hneď namočíte, môže sa rozmazať do vlákna a pri alergikoch sa zároveň prenesie z vonkajšieho prostredia do bytu.",
        "scope": "bundy, mikiny, softshell, kapucňu, rukávy, ramená, tmavý textil, peľ po prechádzke, oblečenie pri alergii a skladovanie v predsieni",
        "avoid": "mokré utieranie suchého peľu, striasanie v spálni, položenie oblečenia na posteľ, pranie funkčnej bundy bez potreby, sušenie vonku počas silného peľu a miešanie peľového oblečenia s posteľnou bielizňou",
        "diagnosis": [
            "<strong>Najprv vonku striasť:</strong> peľ nechcete preniesť do spálne ani na sedačku.",
            "<strong>Suchý peľ riešte nasucho:</strong> valček alebo jemná kefa sú bezpečnejší prvý krok než voda.",
            "<strong>Alergický režim je prísnejší:</strong> oblečenie nedávajte na posteľ a po návrate sa prezlečte.",
            "<strong>Pranie nie je vždy nutné:</strong> pri bunde často stačí odstránenie z povrchu.",
        ],
        "state_rows": [
            ("viditeľný peľ", "vytriasť vonku", "nie v spálni"),
            ("žltý prášok", "valček alebo jemná kefa", "nasucho"),
            ("alergická domácnosť", "prezliecť a oddeliť oblečenie", "menej peľu v byte"),
            ("funkčná bunda", "prať len podľa potreby", "chráni úpravu"),
        ],
        "textile_rows": [
            ("mikina", "vytriasť a podľa potreby prať", "drží prach v úplete"),
            ("softshell", "kefa a opatrné pranie", "funkčná úprava"),
            ("bunda s kapucňou", "kontrola ramien a kapucne", "peľ padá zhora"),
            ("tmavý textil", "valček jedným smerom", "peľ je viditeľný"),
        ],
        "sections": [
            ("Ako striasť peľ z bundy po prechádzke", "Bundu zložte ideálne v predsieni alebo vonku a jemne ju vytraste. Nerobte to v spálni ani nad posteľou. Peľ je jemný a ľahko sa usadí na textíliách v byte.", "Potom prejdite ramená, rukávy a kapucňu valčekom alebo jemnou kefou."),
            ("Peľ na mikine: prečo nezačať vodou", "Suchý peľ je lepšie odstrániť nasucho. Mokrá handrička ho môže rozmazať a vytvoriť žltkastý tieň, najmä na svetlejšom textile.", "Ak po nasucho odstránenom peli ostal tieň, až potom riešte lokálne predčistenie a pranie podľa štítku."),
            ("Oblečenie pri peľovej alergii", "Pri alergickej domácnosti má zmysel prísnejší režim: po návrate zvonka sa prezliecť, oblečenie nenechať na posteli a vonkajšiu vrstvu držať v predsieni.", "CDC pri peli odporúča po pobyte vonku sprchu, odstránenie peľu z pokožky a vlasov a prezlečenie oblečenia. Pri textile to znamená najmä nepreniesť vonkajšiu vrstvu do spálne."),
            ("Kedy prať a kedy stačí mechanické odstránenie", "Mikinu po prechádzke často vyperiete ľahko. Bundu alebo softshell však nemusíte prať po každom kontakte s peľom. Najprv odstráňte peľ z povrchu a perte až pri viditeľnej špine, pachu alebo alergickom režime.", "Pri softshelli rešpektujte štítok a funkčnú úpravu. Súvisiaci návod je <a href=\"/n/ako-prat-softshell-bundu-a-nohavice-bez-poskodenia-membrany\">ako prať softshell</a>."),
            ("Sušenie vonku počas peľovej sezóny", "Ak je peľová situácia silná, vonku sušené prádlo môže peľ znovu zachytiť. Pri alergikoch radšej sušte citlivé textílie vnútri alebo aspoň mimo najprašnejšieho času.", "Podobne kontrolujte aj sušiak na balkóne, ktorý zachytáva prach a peľ."),
            ("Peľ, prach a práčka", "Ak periete viac peľového oblečenia naraz, nepreplňte bubon a použite dobrý oplach. Peľ je jemná častica a pri zlej dávke alebo preplnení sa môže len rozptýliť.", "Pri posteľnej bielizni buďte prísnejší. Vonkajšie bundy a mikiny nepatria na posteľ ani do dávky s obliečkami."),
        ],
        "expert_title": "Odbornejší pohľad: peľ ako častica a alergén",
        "expert_p1": "Peľ sa správa ako jemná častica. Na textile sedí na povrchu, v úplete a v záhyboch. Mechanické odstránenie pred vodou znižuje riziko rozmazania aj prenosu do interiéru.",
        "expert_p2": "Odborný kontext k peľu a zdraviu nájdete v materiáli <a rel=\"noopener\" href=\"https://www.cdc.gov/climate-health/php/effects/pollen-health.html\" target=\"_blank\">CDC: Pollen and Your Health</a>, ktorý odporúča po pobyte vonku okrem iného sprchu a prezlečenie.",
        "checklist": "Pred praním skontrolujte množstvo peľu, miesto striasania, kapucňu, ramená, rukávy, štítok, funkčnú úpravu, alergický režim domácnosti, sušenie vonku a to, či textil nepatrí najprv vyvetrať a vyčistiť nasucho.",
        "rule": "Pri peli najprv striasť a odstrániť nasucho, až potom prať podľa potreby a alergického režimu.",
        "recommendation_intro": "Pri peľovom oblečení má prací gél pomôcť až po mechanickom odstránení častíc a správnom triedení dávky.",
        "product_text": "Vhodný na následné pranie mikín, tričiek a prateľných vonkajších textílií po odstránení peľu z povrchu podľa štítku.",
        "category_text": "Pri alergickej sezóne sledujte aj oplach, triedenie a to, či sa peľové oblečenie nemieša s posteľnou bielizňou.",
        "links": [
            ("/n/ako-prat-oblecenie-pri-pelovej-alergii-po-prichode-zvonka", "Ako prať oblečenie pri peľovej alergii"),
            ("/n/ako-prat-softshell-bundu-a-nohavice-bez-poskodenia-membrany", "Ako prať softshell bundu"),
            ("/n/ako-vycistit-susiak-na-bielizen-aby-neprenasal-spinu-na-pradlo", "Ako vyčistiť sušiak od prachu a peľu"),
        ],
        "faq": [
            ("Mám peľ z bundy hneď umyť vodou?", "Nie ako prvý krok. Najprv ho odstráňte nasucho, aby sa nerozmazal do vlákna."),
            ("Čo robiť pri alergii na peľ?", "Oblečenie z vonku nenechávajte v spálni, prezlečte sa a pri potrebe perte oddelene od posteľnej bielizne."),
            ("Musím bundu prať po každej prechádzke?", "Nie vždy. Často stačí vytriasť, vyvalčekovať a prať až pri špine, pachu alebo prísnejšom alergickom režime."),
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
        <p>V praxi sa oplatí pozerať na celý kontext: {config["scope"]}. Najväčšie riziko je {config["avoid"]}. Správny postup začína mimo práčky: mechanické odstránenie, lokálne čistenie, kontrola materiálu a až potom pranie alebo sušenie.</p>
        {note_card("Rýchla praktická diagnostika", config["diagnosis"])}
        <h2>Postup podľa stavu problému</h2>
        {table(["Stav", "Čo urobiť", "Poznámka"], config["state_rows"])}
        <h2>Postup podľa typu textilu alebo časti</h2>
        {table(["Textil alebo časť", "Postup", "Prečo"], config["textile_rows"])}
        {sections}
        <h2>{config["expert_title"]}</h2>
        <p>{config["expert_p1"]}</p>
        <p>{config["expert_p2"]}</p>
        <p>Pri týchto problémoch je dôležité rozlíšiť, či ide o mechanickú časticu, soľ, hrdzu, peľ, pach, vlhkosť alebo citlivú konštrukciu. Prací cyklus má dokončiť dobre pripravené čistenie, nie nahradiť prvý krok.</p>
        <h2>Kontrolný checklist pred praním</h2>
        <p>{config["checklist"]}</p>
        <p>Do jednej dávky nedávajte veci s protichodnými potrebami. Návlek z auta so soľou, kabát s trblietkami, vložky do topánok, čisté prádlo na sušiaku a mikina s peľom potrebujú iné miesto čistenia, inú mechaniku a iné sušenie.</p>
        <h2>Kedy postup zopakovať</h2>
        <p>Ak po prvom čistení zostal tieň, pach, soľná mapa, trblietky, hrdzavý bod alebo peľový povlak, nesušte textil horúco a neodkladajte ho do skrine. Najprv určte, čo presne zostalo.</p>
        <p>Opakovaný mierny postup býva bezpečnejší než jeden tvrdý zásah. Pri autosedačke, saku, kabáte, tvarovaných vložkách alebo funkčnej bunde má tvar a konštrukcia rovnakú váhu ako čistota.</p>
        <h2>Ako predísť zafixovaniu pri sušení</h2>
        <p>Sušenie je posledná kontrola. Teplo môže zvýrazniť mastné mapy, uzavrieť vlhkosť, rozšíriť pach alebo zafixovať škvrnu. Pri mechanických časticiach zase skontrolujte, či sa pred sušením nedostali na ďalší textil.</p>
        <p>Pred uložením skontrolujte švy, lemy, rohy, vnútorné strany a miesta, ktoré schnú pomalšie. Suchý povrch nemusí znamenať suchú výplň, penu alebo spodnú stranu poťahu.</p>
        <h2>Kedy nepokračovať agresívnejším čistením</h2>
        <p>Ak sa mení farba, tvar, povrch alebo funkčná časť, ďalšie silnejšie čistenie nemusí pomôcť. Môže ísť už o poškodenie materiálu, nie o zvyšnú špinu.</p>
        <p>Vtedy je bezpečnejšie zastaviť sa, pozrieť štítok alebo návod výrobcu a upraviť rutinu do budúcna. Pri drahších kusoch, bezpečnostných prvkoch a jemných materiáloch je zachovanie funkcie dôležitejšie než dokonalý domáci zásah.</p>
        <h2>Domáca rutina pri opakovanom probléme</h2>
        <p>Nastavte si jednoduchý postup: kontrola pri príchode domov, odstránenie povrchových častíc, lokálne predčistenie, primeraná dávka gélu, nepreplnený bubon a kontrola pred sušením.</p>
        <p>Takto sa z prania nestane náhodný pokus. Pri opakovaných situáciách rýchlo zistíte, či problém vzniká v aute, na oslave, v topánkach, pri sušení alebo počas peľovej sezóny.</p>
        <div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">
        <h2 style="margin-top: 0;">Rýchla zásada</h2>
        <p>{config["rule"]}</p>
        </div>
        <h2>Ako si z toho spravi&#357; opakovate&#318;n&#253; postup</h2>
        <p>Pri podobn&#253;ch probl&#233;moch pom&#225;ha ma&#357; doma jednoduch&#253; rozhodovac&#237; postup, nie iba zoznam n&#225;hodn&#253;ch trikov. Najprv odde&#318;te vec, ktor&#225; m&#244;&#382;e &#353;pini&#357; &#271;al&#353;&#237; textil, potom odstr&#225;&#328;te v&#353;etko, &#269;o ide pre&#269; nasucho, a a&#382; n&#225;sledne rie&#353;te vlhk&#233; &#269;istenie, prac&#237; cyklus alebo dosu&#353;enie.</p>
        <p>Ak sa probl&#233;m opakuje, zapisujte si, &#269;o pomohlo: &#269;i rozhodlo vys&#225;vanie, val&#269;ek, krat&#353;ie namo&#269;enie, lep&#353;&#237; oplach, ni&#382;&#353;ia d&#225;vka g&#233;lu alebo dlh&#353;ie su&#353;enie. Po nieko&#318;k&#253;ch praniach tak vid&#237;te, kde je skuto&#269;n&#225; pr&#237;&#269;ina, a nemus&#237;te pri ka&#382;dom &#271;al&#353;om kuse za&#269;&#237;na&#357; od nuly.</p>
        <p>D&#244;le&#382;it&#233; je aj oddelenie vec&#237; pod&#318;a rizika. Text&#237;lie so so&#318;ou, pe&#318;om, prachom, trblietkami alebo z&#225;pachom nepatria do rovnakej d&#225;vky ako uter&#225;ky, oblie&#269;ky alebo be&#382;n&#233; tri&#269;k&#225;. &#268;&#237;m presnej&#353;ie d&#225;vku priprav&#237;te, t&#253;m menej mus&#237; pr&#225;&#269;ka zachra&#328;ova&#357; chyby pred pran&#237;m.</p>
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


def insert_expansion(long, key):
    long = public_cleanup(long)
    marker = MARKERS[key]
    if marker in long:
        start = long.find(f"<h2>{marker}</h2>")
        search_from = long.find("<h2>FAQ: praktické otázky</h2>", start)
        if search_from == -1:
            search_from = start + len(marker)
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
    parser = argparse.ArgumentParser(description="Conservatively expand VEVO retrofit wave 26 car/glitter/insoles/rack/pollen articles.")
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
                "wave": "retrofit-wave-26-car-glitter-insoles-rack-pollen-five",
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
                "wave": "retrofit-wave-26-car-glitter-insoles-rack-pollen-five",
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
