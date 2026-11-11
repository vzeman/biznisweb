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
OUT_JSON = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-12-materials-outdoor-five-2026-06-16.json"
MCP_RESULTS = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-12-materials-outdoor-five-2026-06-16-mcp-results.json"


ARTICLES = [
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-23-2026-06-11-articles.json",
        "slug": "bambusove-vlakno-vs-bavlna-vyhody-nevyhody-a-pranie-pri-citlivej-pokozke",
        "post_id": "2241",
        "url": "https://www.vevo.sk/n/bambusove-vlakno-vs-bavlna-vyhody-nevyhody-a-pranie-pri-citlivej-pokozke",
        "topic": "bamboo_vs_cotton",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-23-2026-06-11-articles.json",
        "slug": "co-je-zmesovy-material-preco-sa-oblecenie-zraza-alebo-sprava-inak-nez-cakate",
        "post_id": "2244",
        "url": "https://www.vevo.sk/n/co-je-zmesovy-material-preco-sa-oblecenie-zraza-alebo-sprava-inak-nez-cakate",
        "topic": "mixed_fabric",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-21-2026-06-10-articles.json",
        "slug": "recyklovany-polyester-co-znamena-ake-ma-vyhody-a-ako-sa-on-starat",
        "post_id": "2230",
        "url": "https://www.vevo.sk/n/recyklovany-polyester-co-znamena-ake-ma-vyhody-a-ako-sa-on-starat",
        "topic": "recycled_polyester",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-24-2026-06-16-articles.json",
        "slug": "co-je-softshell-vrstvy-membrana-impregnacia-a-spravna-starostlivost",
        "post_id": "2246",
        "url": "https://www.vevo.sk/n/co-je-softshell-vrstvy-membrana-impregnacia-a-spravna-starostlivost",
        "topic": "softshell_material",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-24-2026-06-16-articles.json",
        "slug": "co-je-fleece-hrejivost-zmolkovanie-a-starostlivost-pri-prani",
        "post_id": "2245",
        "url": "https://www.vevo.sk/n/co-je-fleece-hrejivost-zmolkovanie-a-starostlivost-pri-prani",
        "topic": "fleece_material",
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
    if kind == "gentle":
        text = "Pri textile nosenom pri pokožke je dôležitý najmä dobrý oplach, primerané dávkovanie a úplné sušenie. Vôňa má byť jemný doplnok čistej bielizne, nie spôsob prekrytia zvyškov produktu."
    elif kind == "outdoor":
        text = "Pri funkčných vrstvách, fleeci a softshelli je kľúčové rešpektovať štítok, nepreťažovať textil trením a pri membránach nepoužívať aviváž, ak ju výrobca neodporúča."
    else:
        text = "Pri zmesiach a syntetike rozhoduje správny program, rozumné dávkovanie a rýchle sušenie. Produkt má pomôcť čistote textilu, nie zakrývať pot alebo zatuchnutie."
    return clean(
        f"""
        <div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 24px 0; background: #f7fbf8;">
        <h2 style="margin-top: 0;">Odporúčané riešenie pre šetrnú starostlivosť</h2>
        <p>{text}</p>
        <div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff; margin: 14px 0;">
        <h3 style="margin-top: 0;">Prací gél hypoalergénny z Marseillského mydla 1L</h3>
        <p>Vhodný základ na bežné pranie mnohých textílií, keď nechcete materiál zbytočne preťažovať agresívnym postupom. Pri funkčných materiáloch, membránach, vlne a veľmi jemných kusoch vždy rozhoduje štítok výrobcu.</p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="/p-1551/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l">Pozrieť produkt</a></p>
        </div>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="/c/vevo-home-care/pranie/praci-gel">Pozrieť kategóriu pracie gély</a></p>
        </div>
        """
    )


TOPICS = {
    "bamboo_vs_cotton": {
        "marker": "Detailnejšie porovnanie bambusového vlákna a bavlny pri citlivej pokožke",
        "product_kind": "gentle",
        "intro": [
            "Bambusové vlákno v oblečení alebo uterákoch je najčastejšie bambusová viskóza, teda regenerované celulózové vlákno. Na dotyk býva veľmi mäkké a hladké, preto sa často používa pri pyžamách, spodnej bielizni, detských veciach alebo uterákoch. Bavlna je prirodzená, známa a v domácnosti univerzálna, ale nie každý bavlnený výrobok je automaticky jemnejší než bambusová viskóza.",
            "Pri citlivej pokožke preto nerozhoduje iba názov materiálu. Veľký rozdiel spraví farbivo, povrchová úprava, zvyšky pracieho produktu, tvrdosť vody, spôsob sušenia a intenzita vône. Ak textil dráždi pokožku, problém môže byť v praní aj v konkrétnom výrobku, nie iba v tom, či je z bambusu alebo bavlny.",
        ],
        "bullets": [
            "<strong>Bambusová viskóza:</strong> mäkká a hladká, ale citlivá na ťah a nevhodné sušenie.",
            "<strong>Bavlna:</strong> univerzálna a savá, no môže tvrdnúť pri zlom oplachu alebo presušení.",
            "<strong>Citlivá pokožka:</strong> sledujte dávkovanie, oplach a vôňu, nie iba názov materiálu.",
            "<strong>Uteráky a obliečky:</strong> potrebujú priestor v bubne a úplné vysušenie.",
        ],
        "tables": [
            {
                "title": "Bambusové vlákno vs bavlna podľa použitia",
                "headers": ["Použitie", "Čo je dôležité", "Praktický postup"],
                "rows": [
                    ("spodná bielizeň a pyžamá", "kontakt s pokožkou a zvyšky produktu", "jemné dávkovanie, dobrý oplach, mierna vôňa"),
                    ("detské body", "citlivá pokožka, farbivá a sušenie", "menšia dávka, dôkladný oplach, úplné vysušenie"),
                    ("uteráky", "savosť a tvrdosť", "nepreplniť bubon, nepoužívať priveľa aviváže"),
                    ("obliečky", "veľká plocha pri pokožke", "prať s priestorom a sušiť bez vlhkých záhybov"),
                ],
            },
            {
                "title": "Keď materiál dráždi alebo škriabe",
                "headers": ["Prejav", "Častá príčina", "Čo skúsiť"],
                "rows": [
                    ("textil je tvrdý", "zvyšky produktu, tvrdá voda alebo presušenie", "znížiť dávku a pridať oplach"),
                    ("vôňa je príliš silná", "intenzívne dávkovanie pri textile pri pokožke", "testovať menšie množstvo vône"),
                    ("materiál sa vytiahol", "mokré vešanie alebo ťah za švy", "sušiť bez ťahu a vytvarovať"),
                    ("uterák horšie saje", "film z aviváže alebo produktu", "upraviť dávkovanie a oplach"),
                ],
            },
        ],
        "sections": [
            ("Ako prať bambusovú viskózu", "Bambusovú viskózu perte jemne, s podobnými materiálmi a bez zbytočného trenia. Mokrý materiál môže byť citlivejší na ťah, preto ho po praní nekrúťte a nevešajte tak, aby sa vytiahol vlastnou váhou. Pri pyžamách a spodných vrstvách vyberajte vôňu opatrne, pretože textil je dlho pri pokožke.", "Ak riešite samotný materiál podrobnejšie, nadväzuje článok <a href=\"/n/co-je-bambusova-viskoza-maekkost-marketingove-tvrdenia-a-realna-starostlivost\">čo je bambusová viskóza</a>. Pri porovnaní s bavlnou pomôže aj <a href=\"/n/co-je-bavlna-vlastnosti-vyhody-nevyhody-a-starostlivost\">sprievodca bavlnou</a>."),
            ("Ako prať bavlnu pri citlivej pokožke", "Bavlna je dobrá voľba pre veľa domácností, ale citlivá pokožka môže reagovať aj na bavlnený textil, ak v ňom zostanú zvyšky produktu alebo vlhkosť. Pri detskom textile, pyžamách a obliečkach je lepšie prať menšiu dávku, dávkovať presne a pri potrebe použiť extra oplach.", "Pri organickej bavlne platí rovnaká logika. Pôvod vlákna je dôležitý pri výbere, ale v práčke stále rozhoduje hotový výrobok, farba, potlač a štítok. K tomu patrí článok <a href=\"/n/organicka-bavlna-co-znamena-a-ci-sa-perie-inak-ako-bezna-bavlna\">organická bavlna a pranie</a>."),
            ("Ako vybrať materiál na uteráky a obliečky", "Pri uterákoch je dôležitá savosť. Mäkký materiál na dotyk nemusí byť automaticky najpraktickejší, ak po praní zle schne alebo horšie saje. Pri obliečkach zas rozhoduje plocha pri pokožke, schopnosť vetrať a to, či materiál po vysušení nepôsobí tvrdšie.", "Ak sa uteráky po praní menia na tvrdé alebo zatuchnuté, často je problém v oplachu a sušení, nie v samotnom názve materiálu. Pri obliečkach sledujte preplnený bubon a vlhké záhyby."),
            ("Odbornejší pohľad: pokožka rieši celý proces", "Pri citlivej pokožke je materiál iba jedna časť rovnice. Koža je v kontakte s vláknom, farbivom, povrchovou úpravou, zvyškami pracieho produktu aj vôňou. Preto môže jeden bambusový výrobok sedieť a iný nie, rovnako ako pri bavlne.", "Rozumný domáci test je vyprať pár kusov s nižším dávkovaním, dobrým oplachom a bez prehnanej vône. Ak je výsledok lepší, problém bol pravdepodobne v rutine. Ak nie, môže ísť o konkrétny textil alebo individuálnu citlivosť."),
        ],
        "box": ("Rýchla zásada", "Pri citlivej pokožke vyberajte materiál aj praciu rutinu spolu. Mäkkosť na dotyk nestačí, ak textil ostane zle vypláchnutý, vlhký alebo prevoňaný príliš intenzívne."),
        "faq": [
            ("Je bambusové vlákno lepšie ako bavlna?", "Nie univerzálne. Bambusová viskóza býva veľmi mäkká, bavlna je praktická a savá. Rozhoduje výrobok, pokožka a pranie."),
            ("Je bambusové vlákno vhodné pre citlivú pokožku?", "Môže byť, ale sledujte aj farbivá, prací produkt, oplach, sušenie a intenzitu vône."),
            ("Prečo bavlna po praní škriabe?", "Často pre zvyšky produktu, tvrdú vodu, presušenie alebo preplnený bubon."),
        ],
    },
    "mixed_fabric": {
        "marker": "Detailnejší pohľad na zmesové materiály, zrážanie a správanie pri praní",
        "product_kind": "mixed",
        "intro": [
            "Zmesový materiál znamená, že textil nie je z jedného vlákna, ale z kombinácie, napríklad bavlna s elastanom, polyester s viskózou, vlna s polyamidom alebo bavlna s polyesterom. Zmes môže zlepšiť pružnosť, tvar, rýchlosť schnutia alebo cenu, ale zároveň mení spôsob prania.",
            "Najväčšia chyba je hodnotiť zmes podľa najznámejšej zložky. Ak je tričko 95 % bavlna a 5 % elastan, elastan môže rozhodovať o teple a sušení. Ak je sveter zmes vlny a syntetiky, citlivejšia vlnená zložka stále určuje opatrnosť. Preto pri zmesiach platí pravidlo: riaďte sa najcitlivejšou časťou výrobku.",
        ],
        "bullets": [
            "<strong>Elastan:</strong> dáva pružnosť, ale nemá rád horúce sušenie.",
            "<strong>Viskóza v zmesi:</strong> môže meniť tvar, najmä za mokra.",
            "<strong>Vlna v zmesi:</strong> vyžaduje opatrnosť aj pri menšom podiele.",
            "<strong>Polyester v zmesi:</strong> zlepšuje schnutie, ale pri pote môže držať pach.",
        ],
        "tables": [
            {
                "title": "Zmesi podľa typu oblečenia",
                "headers": ["Zmes", "Typický dôvod použitia", "Na čo dať pozor"],
                "rows": [
                    ("bavlna + elastan", "pružnosť tričiek, legín a spodnej bielizne", "teplo a sušička môžu oslabiť pružnosť"),
                    ("polyester + bavlna", "tvar, rýchlejšie schnutie a odolnosť", "pot a pach riešiť včasným praním"),
                    ("vlna + polyamid", "pevnejší úplet", "prať ako jemnejší materiál"),
                    ("viskóza + elastan", "splývavosť a pružnosť", "nevešať mokré s ťahom"),
                ],
            },
            {
                "title": "Prečo sa zmes správa inak, než čakáte",
                "headers": ["Prejav", "Možný dôvod", "Bezpečnejšia rutina"],
                "rows": [
                    ("oblečenie sa zrazilo", "teplo, sušička alebo citlivá zložka", "nižšia teplota a šetrné sušenie"),
                    ("lem sa vyťahal", "elastan alebo guma reagovali na teplo", "neprehrievať a nekrútiť"),
                    ("tričko zapácha", "syntetická zložka drží pot", "prať včas a dobre sušiť"),
                    ("blúzka stratila tvar", "viskóza bola mokrá pod ťahom", "sušiť bez ťahu a vytvarovať"),
                ],
            },
        ],
        "sections": [
            ("Ako čítať zloženie zmesového materiálu", "Pri zmesi si nepozerajte iba prvé percento. Najcitlivejšia zložka môže tvoriť menší podiel, ale stále ovplyvňuje starostlivosť. Elastan, vlna, viskóza, membrána, potlač alebo výstuž môžu byť dôležitejšie než dominantná bavlna alebo polyester.", "Ak si nie ste istí, použite opatrnejší program a vyhnite sa horúcej sušičke. Pri novom kúsku je lepšie prvé pranie spraviť šetrnejšie a podľa výsledku upraviť ďalšiu rutinu."),
            ("Ako prať zmes bavlny a elastanu", "Bavlna s elastanom je bežná pri tričkách, legínach, spodnej bielizni a detskom oblečení. Práve elastan dáva pohodlie, ale teplo a agresívna mechanika ho môžu časom poškodiť. Perte naruby, nepoužívajte zbytočne vysokú teplotu a so sušičkou buďte opatrní.", "Ak pružné oblečenie stratilo tvar, problém často nebol v samotnej bavlne, ale v elastane, gume alebo lemoch. Pri podobných otázkach nadväzuje článok <a href=\"/n/co-je-elastan-preco-je-v-leginach-spodnej-bielizni-a-sportovom-obleceni\">čo je elastan</a>."),
            ("Ako prať zmes polyesteru a bavlny", "Polyester s bavlnou môže schnúť rýchlejšie a lepšie držať tvar, ale pri spotenom oblečení treba riešiť pach včas. Ak textil necháte vlhký v taške, zmes sa môže dostať do stavu, keď bežné krátke pranie nestačí.", "Pomáha nenechať spotené oblečenie zatuchnúť, nepreplniť bubon a dobre sušiť. K syntetickej časti nadväzuje článok <a href=\"/n/co-je-polyester-a-ako-ho-prat-aby-nezapachal\">čo je polyester a ako ho prať</a>."),
            ("Odbornejší pohľad: zmes je hotový systém, nie matematický priemer", "Zmesové oblečenie sa nespráva ako jednoduchý priemer dvoch vlákien. Výsledok závisí od priadze, väzby, úpletu, farbiva, elastických častí a spôsobu spracovania. Preto sa dve tričká s podobným percentom bavlny a elastanu môžu po praní správať odlišne.", "Domáca prax je jednoduchá: sledujte štítok, chráňte najcitlivejšiu zložku a po prvom praní si všimnite tvar, dotyk, pach a pružnosť. To je presnejšia spätná väzba než samotný názov zmesi."),
        ],
        "box": ("Rýchla zásada", "Pri zmesovom materiáli sa riaďte najcitlivejšou zložkou a hotovým výrobkom. Percentá na štítku sú dôležité, ale nehovoria celý príbeh."),
        "faq": [
            ("Prečo sa zmesové oblečenie zrazilo?", "Často pre teplo, sušičku alebo citlivú zložku, napríklad vlnu, viskózu alebo elastan."),
            ("Mám prať podľa väčšinového materiálu?", "Nie vždy. Pri zmesi rozhoduje najcitlivejšia zložka a konštrukcia výrobku."),
            ("Je polyester s bavlnou lepší než čistá bavlna?", "Záleží od účelu. Môže lepšie držať tvar a rýchlejšie schnúť, ale pri pote treba riešiť pach."),
        ],
    },
    "recycled_polyester": {
        "marker": "Detailnejší pohľad na recyklovaný polyester a domácu starostlivosť",
        "product_kind": "mixed",
        "intro": [
            "Recyklovaný polyester znamená, že polyesterové vlákno alebo materiál pochádza z recyklovaného zdroja, často z plastových fliaš alebo textilného odpadu podľa konkrétneho reťazca. Pre domáce pranie je však dôležité, že hotový výrobok sa stále správa ako polyester alebo polyesterová zmes.",
            "Recyklovaný pôvod teda automaticky neznamená, že textil znesie vyššiu teplotu, menej páchne alebo nepotrebuje opatrnosť. Pri športovej mikine, fleecovej vrstve, taške alebo tričku rozhoduje konštrukcia, farba, pot, povrchová úprava, elastan a štítok výrobcu.",
        ],
        "bullets": [
            "<strong>Pôvod vlákna:</strong> je dôležitý pri výbere, ale nemení základnú starostlivosť o polyester.",
            "<strong>Pach po športe:</strong> riešte včasným praním, oplachom a sušením.",
            "<strong>Fleece a česaný povrch:</strong> perte naruby a znižujte trenie.",
            "<strong>Funkčné vrstvy:</strong> aviváž používajte iba vtedy, ak ju štítok povoľuje.",
        ],
        "tables": [
            {
                "title": "Recyklovaný polyester podľa typu výrobku",
                "headers": ["Výrobok", "Praktické riziko", "Ako postupovať"],
                "rows": [
                    ("športové tričko", "pach potu a zvyšky produktu", "prať včas, nepreplniť bubon, dobre sušiť"),
                    ("fleece", "žmolkovanie a uvoľňovanie vlákien", "prať naruby, oddeliť od drsných vecí"),
                    ("mikina", "hrubší objem a pomalšie sušenie", "sušiť rozložené a úplne"),
                    ("taška alebo doplnok", "výstuže a povrchové úpravy", "čistiť podľa štítku, nie automaticky prať"),
                ],
            },
            {
                "title": "Čo recyklovaný pôvod nerieši",
                "headers": ["Téma", "Prečo to stále platí", "Domáce riešenie"],
                "rows": [
                    ("zápach", "polyesterová štruktúra môže držať pot", "prať včas a nenechať vlhké"),
                    ("teplo", "syntetika a elastan nemusia znášať horúce sušenie", "nižšie teploty a štítok"),
                    ("mikrovlákna", "opotrebovanie a trenie stále zohrávajú rolu", "menej zbytočného prania a dlhšia životnosť"),
                    ("farba a potlač", "hotový výrobok môže mať citlivé detaily", "prať naruby a triediť"),
                ],
            },
        ],
        "sections": [
            ("Ako prať recyklovaný polyester po športe", "Po spotení nenechávajte recyklovaný polyester zavretý vo vlhkej taške. Ak nemôžete prať hneď, textil aspoň presušte. Pach sa rieši ľahšie, keď sa pot a vlhkosť nezafixujú do materiálu a keď v bubne nie je priveľa vecí naraz.", "Pri športovej syntetike nepoužívajte aviváž automaticky. Môže zhoršiť funkčný pocit alebo priedušnosť niektorých vrstiev. K téme nadväzuje článok <a href=\"/n/polyamid-vs-polyester-ktory-material-lepsie-znasa-pot-sport-a-caste-pranie\">polyamid vs polyester pri športe</a>."),
            ("Ako prať recyklovaný polyesterový fleece", "Fleece z recyklovaného polyesteru perte naruby, s podobnými mäkkými kusmi a mimo zipsov, suchých zipsov a drsných tkanín. Česaný povrch je náchylný na trenie, ktoré môže zhoršiť žmolkovanie a vzhľad. Menej trenia znamená dlhšiu životnosť.", "Ak fleece po praní stvrdne alebo zapácha, skontrolujte oplach a sušenie. Hrubšia vrstva môže držať vlhkosť dlhšie, než sa zdá podľa povrchu."),
            ("Ako sa líši recyklovaný polyester od bežného pri praní", "Z pohľadu domácnosti sú pravidlá veľmi podobné. Rozdiel v pôvode vlákna je dôležitý pri nákupe a pri environmentálnom kontexte, ale v práčke stále pracujete s polyesterom, farbou, švami a hotovým výrobkom.", "Namiesto hľadania špeciálneho programu pre recyklovaný polyester je praktickejšie predĺžiť životnosť oblečenia: prať iba vtedy, keď treba, používať šetrný program, neprehrievať a dobre sušiť."),
            ("Odbornejší pohľad: životnosť je tiež súčasť udržateľnosti", "Pri recyklovaných materiáloch sa často rieši pôvod vlákna, ale pri používaní v domácnosti rozhoduje aj to, ako dlho výrobok vydrží. Oblečenie, ktoré sa rýchlo zničí nesprávnym praním, stráca veľkú časť praktického prínosu.", "Preto je pri recyklovanom polyesteri dobré myslieť na jednoduchú prevenciu: menej zbytočného trenia, správne triedenie, opatrné sušenie a riešenie zápachu skôr, než sa z neho stane trvalý problém."),
        ],
        "box": ("Rýchla zásada", "Recyklovaný polyester perte ako polyester podľa konkrétneho výrobku. Recyklovaný pôvod nemení potrebu riešiť pot, trenie, teplo, sušenie a štítok."),
        "faq": [
            ("Perie sa recyklovaný polyester inak než bežný polyester?", "V domácej praxi väčšinou nie. Rozhoduje konkrétny výrobok, zmes a štítok."),
            ("Môže recyklovaný polyester zapáchať po športe?", "Áno. Pach súvisí aj s potom, vlhkosťou, baktériami, zvyškami produktu a sušením."),
            ("Je recyklovaný polyester vhodný do sušičky?", "Iba ak to povoľuje štítok. Pri syntetike a elastane je horúce sušenie časté riziko."),
        ],
    },
    "softshell_material": {
        "marker": "Detailnejší pohľad na softshell, membránu, impregnáciu a pranie",
        "product_kind": "outdoor",
        "intro": [
            "Softshell nie je jedno vlákno, ale konštrukcia materiálu. Môže mať vonkajšiu odolnejšiu vrstvu, vnútornú mäkkú vrstvu, membránu alebo vodoodpudivú úpravu. Práve preto sa softshell neperie ako obyčajná mikina, hoci sa tak môže na prvý pohľad tváriť.",
            "Pri softshelli sú dôležité tri veci: čistota, funkcia a povrch. Pot, prach a mastnota môžu zhoršiť priedušnosť a komfort, ale nevhodná aviváž, agresívne pranie alebo zlé sušenie môžu zhoršiť aj vodoodpudivosť a membránu. Preto treba postupovať podľa štítku a typu konkrétneho výrobku.",
        ],
        "bullets": [
            "<strong>Bez aviváže:</strong> pri funkčných vrstvách ju nepoužívajte, ak ju výrobca neodporúča.",
            "<strong>Zipsy zapnúť:</strong> znížite trenie a riziko poškodenia povrchu.",
            "<strong>Impregnáciu riešte podľa správania vody:</strong> nie automaticky po každom praní.",
            "<strong>Sušenie:</strong> robte podľa štítku; pri niektorých úpravách rozhoduje aj obnova povrchu.",
        ],
        "tables": [
            {
                "title": "Softshellové vrstvy a čo znamenajú pri praní",
                "headers": ["Časť materiálu", "Úloha", "Riziko pri nesprávnej starostlivosti"],
                "rows": [
                    ("vonkajšia tkanina", "odolnosť voči vetru, oderu a vode", "strata vodoodpudivosti alebo vzhľadu"),
                    ("membrána", "priedušnosť a ochrana podľa typu", "zanesenie alebo zhoršenie funkcie"),
                    ("vnútorný fleece", "komfort a teplo", "žmolkovanie a zachytenie zápachu"),
                    ("DWR úprava", "kvapky vody sa majú odrážať od povrchu", "voda sa začne vpíjať do vrchnej vrstvy"),
                ],
            },
            {
                "title": "Softshell po praní: diagnostika",
                "headers": ["Prejav", "Čo môže znamenať", "Čo spraviť"],
                "rows": [
                    ("voda sa vpíja do povrchu", "oslabená alebo znečistená DWR úprava", "vyčistiť, vysušiť a zvážiť obnovu impregnácie"),
                    ("bunda zapácha", "pot a vlhkosť vnútri vrstiev", "prať včas a úplne vysušiť"),
                    ("materiál je tuhý", "zvyšky produktu alebo nevhodný prostriedok", "dobrý oplach a správne dávkovanie"),
                    ("povrch je ošúchaný", "trenie o drsné kusy", "prať samostatnejšie a zapínať zipsy"),
                ],
            },
        ],
        "sections": [
            ("Ako prať softshellovú bundu", "Softshellovú bundu pred praním vyprázdnite, zapnite zipsy a suché zipsy, otočte podľa potreby a perte podľa štítku. Nepatrí k uterákom ani rifliam. Cieľom je odstrániť pot a špinu bez zbytočného trenia a bez zanášania funkčných vrstiev.", "Podrobný praktický postup je v článku <a href=\"/n/ako-prat-softshell-bundu-a-nohavice-bez-poskodenia-membrany\">ako prať softshell bundu a nohavice</a>. Tento článok dopĺňa najmä vysvetlenie, prečo softshell nie je jeden univerzálny materiál."),
            ("Kedy obnovovať impregnáciu softshellu", "Impregnáciu neriešte automaticky po každom praní. Najprv sledujte, čo robí voda na povrchu. Ak sa kvapky držia a stekajú, úprava ešte funguje. Ak sa voda začne vpíjať do vrchnej vrstvy, môže pomôcť správna obnova podľa typu výrobku.", "Samostatný postup nájdete v článku <a href=\"/n/ako-obnovit-impregnaciu-softshellu-po-prani-a-kedy-ju-neriesit\">ako obnoviť impregnáciu softshellu</a>. Pri membráne alebo špeciálnej úprave sa vždy riaďte výrobcom."),
            ("Prečo softshellu škodí aviváž", "Aviváž môže na funkčných textíliách vytvoriť film, ktorý zhorší priedušnosť, savosť, transport vlhkosti alebo vodoodpudivý povrch. Pri softshelli preto nie je dobrý nápad používať aviváž len preto, aby bol materiál mäkší alebo viac voňal.", "Ak softshell zapácha, riešte pot, sušenie a čistotu. Vôňa nemá prekryť problém, ktorý vznikol vnútri vrstiev. Pri funkčných materiáloch je menej často lepšie."),
            ("Odbornejší pohľad: softshell je kompromis medzi komfortom a ochranou", "Softshell má fungovať ako praktická vrstva medzi mikinou a tvrdou nepremokavou bundou. Preto môže kombinovať pružnosť, vetruodolnosť, čiastočnú ochranu pred vodou a priedušnosť. Pranie má tento kompromis chrániť, nie posunúť materiál do stavu, keď bude síce voňať, ale horšie fungovať.", "Pri domácej starostlivosti je najlepšia prevencia: prať až pri reálnej potrebe, nenechať pot dlhodobo v materiáli, nepoužívať aviváž pri funkčných vrstvách a po praní skontrolovať povrch vodou."),
        ],
        "box": ("Rýchla zásada", "Softshell perte podľa štítku, bez aviváže pri funkčných vrstvách a s dôrazom na oplach. Impregnáciu obnovujte podľa správania vody, nie zo zvyku."),
        "faq": [
            ("Môžem prať softshell v bežnom pracom géli?", "Pri niektorých kusoch áno, ak to štítok umožňuje a použijete primerané dávkovanie. Funkčné špeciály však vždy posudzujte podľa výrobcu."),
            ("Prečo nepoužívať aviváž na softshell?", "Môže zhoršiť funkčné vlastnosti, priedušnosť alebo vodoodpudivú úpravu."),
            ("Kedy treba impregnáciu?", "Keď čistý a suchý povrch prestane odpudzovať vodu a kvapky sa začnú vpíjať."),
        ],
    },
    "fleece_material": {
        "marker": "Detailnejší pohľad na fleece, hrejivosť, žmolkovanie a zápach po praní",
        "product_kind": "outdoor",
        "intro": [
            "Fleece je najčastejšie česaný syntetický materiál, typicky polyesterový, ktorý hreje najmä vďaka objemu a vzduchu zachytenému v štruktúre. Je ľahký, rýchlo schne a príjemne sa nosí, ale jeho povrch je citlivý na trenie, žmolkovanie a zachytávanie drobných nečistôt.",
            "Pri praní fleecu preto nejde len o odstránenie špiny. Dôležité je znížiť trenie, chrániť česaný povrch, dobre vypláchnuť prací produkt a úplne vysušiť hrubšie kusy. Ak fleece po praní zapácha alebo stráca mäkkosť, problém býva v pote, preplnenom bubne, slabom oplachu alebo pomalom sušení.",
        ],
        "bullets": [
            "<strong>Perte naruby:</strong> povrch sa menej trie o bubon a ostatné veci.",
            "<strong>Oddelte od drsných kusov:</strong> zipsy, suchý zips a uteráky zhoršujú opotrebovanie.",
            "<strong>Nepreháňajte teplo:</strong> syntetika a povrchová úprava nemajú rady horúce sušenie.",
            "<strong>Riešte pach včas:</strong> vlhký fleece v taške zatuchne rýchlejšie.",
        ],
        "tables": [
            {
                "title": "Fleece podľa použitia",
                "headers": ["Typ fleecu", "Najčastejší problém", "Lepší postup"],
                "rows": [
                    ("tenká mikina", "pach po športe", "presušiť alebo vyprať včas"),
                    ("hrubá bunda", "pomalšie sušenie v objeme", "rozložiť a dosušiť úplne"),
                    ("detský fleece", "škvrny a časté pranie", "predčistiť lokálne, prať šetrne"),
                    ("outdoor vrstva", "trenie so zipsami a suchým zipsom", "zapnúť, prať naruby, oddeliť"),
                ],
            },
            {
                "title": "Fleece po praní: čo kontrolovať",
                "headers": ["Prejav", "Možná príčina", "Ako upraviť ďalšie pranie"],
                "rows": [
                    ("žmolky na povrchu", "trenie a drsné kusy v dávke", "prať naruby a oddeliť"),
                    ("zatuchnutý pach", "vlhkosť v hrubšej vrstve", "sušiť dlhšie a vzdušnejšie"),
                    ("tvrdší dotyk", "zvyšky produktu alebo presušenie", "upraviť dávku a oplach"),
                    ("strata hrejivosti", "zľahnutý objem alebo zanesenie", "nepreplniť bubon, jemnejšie sušiť"),
                ],
            },
        ],
        "sections": [
            ("Ako prať fleece mikinu", "Fleece mikinu perte naruby a so zapnutými zipsami. Nepatrí do dávky s uterákmi, rifľami, suchým zipsom alebo veľmi drsnými textíliami. Pri športovej mikine je dôležité nenechať pot zaschnúť a zatuchnúť v taške, lebo potom budete mať tendenciu voliť silnejší program, ktorý povrchu neprospieva.", "Pri bežnom nosení často stačí šetrnejší program podľa štítku a primeraná dávka pracieho gélu. Ak je fleece silno spotený, pomôže prať skôr než pridávať viac vône."),
            ("Ako sušiť fleece bez straty objemu", "Fleece hreje vďaka vzduchu v štruktúre. Ak ho preplníte v bubne alebo necháte schnúť stlačený, môže pôsobiť menej nadýchane. Po praní ho vytraste, upravte tvar a sušte tak, aby cez materiál prúdil vzduch.", "Horúcu sušičku používajte iba vtedy, ak to štítok povoľuje. Pri syntetike je opatrnejšie sušenie často lepšie pre povrch aj tvar."),
            ("Ako riešiť žmolkovanie fleecu", "Žmolkovanie vzniká najmä trením. Pomáha prať naruby, oddeliť fleece od drsných materiálov a nepoužívať príliš intenzívny program bez potreby. Ak už žmolky vznikli, odstraňujte ich opatrne, aby ste nepoškodili povrch.", "Pri lacnejšom alebo veľmi voľnom fleeci sa žmolky môžu objaviť rýchlejšie. Domáca starostlivosť ich vie spomaliť, ale nie vždy úplne zastaviť."),
            ("Odbornejší pohľad: hrejivosť fleecu je aj otázka štruktúry", "Fleece nehreje preto, že by bol ťažký, ale preto, že česaný povrch a objem zadržiavajú vzduch. Keď sa povrch zoderie, zlepí zvyškami produktu alebo zľahne nevhodným sušením, pocit tepla a mäkkosti sa môže zhoršiť.", "Preto je pri fleeci dôležitá nízka mechanická záťaž, dobrý oplach a úplné sušenie. Vôňa je príjemný doplnok, ale nesmie nahradiť čistotu a zachovanie štruktúry."),
        ],
        "box": ("Rýchla zásada", "Fleece perte naruby, mimo drsných kusov a bez zbytočného tepla. Najviac mu škodí trenie, zatuchnutie a pomalé sušenie v hrubej vrstve."),
        "faq": [
            ("Prečo fleece žmolkuje?", "Najčastejšie pre trenie pri nosení a praní. Pomáha prať naruby a oddeliť od drsných kusov."),
            ("Môže ísť fleece do sušičky?", "Iba ak to povoľuje štítok. Pri syntetike je horúce sušenie riziko pre povrch aj tvar."),
            ("Ako odstrániť zápach z fleecu?", "Nenechať ho vlhký, prať včas, nepreplniť bubon a dosušiť úplne. Vôňa nemá prekryť zatuchnutie."),
        ],
    },
}


DEPTH_SECTIONS = {
    "bamboo_vs_cotton": [
        (
            "Domáci test po praní pri citlivej pokožke",
            "Ak porovnávate bambusovú viskózu a bavlnu, nehodnoťte ich iba podľa dotyku pred prvým nosením. Urobte jednoduchý domáci test po dvoch alebo troch praniach: textil ovoňajte až po úplnom vysušení, skontrolujte tvrdosť v miestach švov a všimnite si, či pokožka reaguje po celodennom nosení. Práve po opakovanom praní sa ukáže, či materiál drží mäkkosť alebo potrebuje upraviť rutinu.",
            "Ak sa zhorší pocit na pokožke, nemeňte hneď všetky produkty naraz. Najprv znížte dávku pracieho gélu, potom pridajte lepší oplach a až nakoniec riešte vôňu. Tak zistíte, či problém robí materiál, prací postup alebo príliš intenzívne prevoňanie textilu.",
        ),
        (
            "Ako rozhodnúť pri nákupe a starostlivosti",
            "Bambusová viskóza môže byť príjemná pri pyžamách, spodnej bielizni a mäkkých uterákoch, no pri veľkých kusoch sledujte aj stabilitu tvaru. Bavlna je praktická tam, kde chcete jednoduchšiu údržbu, savosť a známe správanie v práčke. Pri citlivej pokožke sa oplatí uprednostniť menej komplikovaný materiál, svetlejšie farby a pranie s dôrazom na oplach.",
            "Ak máte doma viac ľudí s rôznymi preferenciami, netreba rozhodnúť ideologicky pre jeden materiál. Na obliečky môže vyhovovať bavlna, na pyžamo bambusová viskóza a na uteráky hustejšia bavlna s dobrou savosťou. Dôležité je nastaviť pranie podľa každého typu textilu.",
        ),
    ],
    "mixed_fabric": [
        (
            "Domáci test zmesového materiálu po prvom praní",
            "Pri zmesových materiáloch je prvé pranie diagnostické. Po vysušení skontrolujte dĺžku rukávov, pružnosť lemu, tvar ramien, dotyk a pach. Ak sa tričko skrátilo, problém mohol byť v teple. Ak sa vytiahol lem, mohlo ísť o elastan alebo gumu. Ak textil zapácha, syntetická zložka alebo slabé sušenie môžu byť dôležitejšie než bavlna v názve.",
            "Druhý cyklus potom upravujte cielene. Pri strate pružnosti znížte teplo a vyhnite sa sušičke. Pri zápachu perte skôr po spotení a lepšie sušte. Pri strate tvaru znížte odstreďovanie a nevešajte mokré kusy za najťažšie body.",
        ),
        (
            "Prečo sa dva podobné štítky nesprávajú rovnako",
            "Dve tričká s označením bavlna a elastan môžu mať iný úplet, inú hrúbku, iný typ elastickej priadze a inú potlač. Preto nestačí prečítať percentá a očakávať rovnaký výsledok. Rozdiel môže byť aj v tom, či je elastan v celej látke alebo hlavne v lemoch.",
            "Pri zmesiach je najlepšia konzervatívna rutina: prať naruby, triediť podľa farby aj citlivosti, neprehrievať a po praní sledovať, čo sa zmenilo. Ak jedna zmes zvládne vyššiu záťaž, neznamená to, že ju zvládne každá podobná vec v šatníku.",
        ),
    ],
    "recycled_polyester": [
        (
            "Domáci test životnosti recyklovaného polyesteru",
            "Pri recyklovanom polyesteri sledujte po praní najmä pach, povrch a tvar. Ak sa materiál rýchlo žmolkuje, môže ísť o trenie v bubne alebo konštrukciu priadze. Ak zapácha aj po praní, problém môže byť v pote, preplnenom bubne, slabom oplachu alebo v tom, že oblečenie ostalo pred praním dlho vlhké.",
            "Najpraktickejší test je zmeniť jednu vec: prať naruby, oddeliť od drsných kusov a nenechať spotené oblečenie čakať v taške. Ak sa výsledok zlepší, netreba hľadať špeciálny recyklovaný režim, stačí lepšia rutina pre polyesterovú syntetiku.",
        ),
        (
            "Recyklovaný polyester, mikrovlákna a rozumná domácnosť",
            "Pri syntetike sa často rieši uvoľňovanie drobných vlákien. V bežnej domácnosti dáva najväčší zmysel predĺžiť životnosť oblečenia: prať iba vtedy, keď treba, znížiť zbytočné trenie, nepreťažovať program a nekupovať veci, ktoré sa rýchlo zničia. Takýto prístup pomáha textilu aj praktickému používaniu.",
            "Recyklovaný pôvod je užitočná informácia pri nákupe, ale dennú starostlivosť robí kvalita výrobku a vaše pranie. Čím dlhšie mikina, tričko alebo fleece slúži bez zápachu a poškodenia, tým menej často riešite náhradu.",
        ),
    ],
    "softshell_material": [
        (
            "Domáci test softshellu po praní",
            "Po vysušení softshellu spravte jednoduchý test kvapkou vody na čistom povrchu. Ak voda perlí a steká, povrchová úprava ešte funguje. Ak sa voda rýchlo vpíja, môže byť povrch znečistený, zle vypláchnutý alebo potrebuje obnovu impregnácie. Test robte až po úplnom vysušení, nie na vlhkej bunde.",
            "Skontrolujte aj vnútro bundy. Ak zapácha, problém často nie je v DWR úprave, ale v pote, vlhkosti alebo slabom sušení. Vtedy nepomôže iba impregnácia. Najprv treba vyčistiť a vysušiť celý kus tak, aby neostala vlhkosť v švoch a vreckách.",
        ),
        (
            "Ako si nastaviť softshellovú rutinu počas sezóny",
            "Softshell nemusíte prať po každej prechádzke. Pri miernom nosení často stačí vysušiť, vyvetrať a lokálne utrieť blato. Pranie má zmysel pri pote, zápachu, mastnote alebo viditeľnom znečistení, ktoré zhoršuje komfort. Menej zbytočného prania znamená menej mechanickej záťaže pre povrch.",
            "Na konci sezóny bundu vyperte podľa štítku, úplne vysušte a skontrolujte vodoodpudivosť. Ak treba, obnovte impregnáciu pred odložením alebo pred ďalšou sezónou. Skladujte ju suchú, nie stlačenú vo vlhkej taške.",
        ),
    ],
    "fleece_material": [
        (
            "Domáci test fleecu po praní",
            "Po praní fleece prejdite rukou po povrchu a skontrolujte, či je stále nadýchaný, či nezapácha v hrubších miestach a či sa nezačali tvoriť žmolky. Pach vo vnútri goliera, manžiet alebo podpazušia často znamená, že pot a vlhkosť neboli úplne vyriešené. Tvrdší dotyk môže ukazovať na zvyšky produktu alebo presušenie.",
            "Pri ďalšom praní znížte trenie: otočte fleece naruby, zapnite zipsy a nedávajte ho k uterákom alebo suchým zipsom. Ak je kus hrubý, venujte viac pozornosti sušeniu než vôni. Vlhkosť v objeme vie vytvoriť zatuchnutý tón aj po správnom programe.",
        ),
        (
            "Ako predĺžiť životnosť fleecu počas sezóny",
            "Fleece často slúži ako druhá vrstva na turistiku, šport alebo bežné nosenie. Nemusí ísť do práčky po každom krátkom použití, ak nie je spotený. Vyvetranie medzi noseniami znižuje počet cyklov a tým aj trenie, ktoré poškodzuje česaný povrch.",
            "Keď fleece periete, berte ho ako mäkký technický úplet, nie ako uterák. Čím lepšie chránite povrch pred oderom, tým dlhšie zostane hrejivý a príjemný na dotyk. Pri silnom zápachu radšej riešte príčinu včas než pridávať intenzívnejšie prevoňanie.",
        ),
    ],
}


FOLLOWUP_SECTIONS = {
    "bamboo_vs_cotton": (
        "Ako upraviť ďalšie pranie podľa výsledku",
        "Ak je bambusová viskóza alebo bavlna po praní príjemná, nemeníte rutinu len preto, že existuje nový produkt alebo silnejšia vôňa. Ak je textil tvrdý, začnite oplachom. Ak je príliš výrazne prevoňaný, uberte vôňu. Ak sa vytiahol, upravte sušenie. Takto sa starostlivosť opiera o reálny výsledok, nie o dojem z etikety.",
    ),
    "mixed_fabric": (
        "Ako upraviť ďalšie pranie podľa výsledku",
        "Pri zmesiach si po prvom praní zapamätajte, čo sa zmenilo: tvar, pružnosť, pach alebo dotyk. Ak sa zhoršila pružnosť, problém bude skôr teplo než prací produkt. Ak zostal pach, riešte čas pred praním a sušenie. Ak sa zmenil tvar, dávajte pozor na odstreďovanie a vešanie mokrého textilu.",
    ),
    "recycled_polyester": (
        "Ako upraviť ďalšie pranie podľa výsledku",
        "Pri recyklovanom polyesteri je dobré sledovať hlavne športové a hrubšie kusy. Ak po praní stále cítiť pot, nestačí pridať vôňu. Skontrolujte, či oblečenie pred praním nevysychalo zavreté v taške, či bubon nebol preplnený a či textil po praní naozaj úplne preschol aj v hrubších miestach.",
    ),
    "softshell_material": (
        "Ako upraviť ďalšie pranie podľa výsledku",
        "Softshell po praní vyhodnocujte až suchý. Ak bunda vyzerá čisto, nezapácha a voda na povrchu stále perlí, nie je dôvod robiť ďalší zásah. Ak voda vsiakne, najprv skontrolujte čistotu a oplach, až potom riešte impregnáciu. Ak zapácha vnútro, priorita je pot a sušenie, nie vonkajší vodoodpudivý efekt.",
    ),
    "fleece_material": (
        "Ako upraviť ďalšie pranie podľa výsledku",
        "Pri fleeci si všímajte, či sa problém opakuje vždy na rovnakom mieste. Žmolky na rukávoch a bokoch môžu súvisieť s nosením batohu alebo bundy, nie iba s práčkou. Zatuchnutie v golieri a podpazuší zas ukazuje na pot a pomalé sušenie. Podľa toho upravte nosenie, vetranie aj pranie.",
    ),
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
    followup_title, followup_text = FOLLOWUP_SECTIONS[topic]
    return clean(
        f"""
        <h2>{config["marker"]}</h2>
        <p>{config["intro"][0]}</p>
        <p>{config["intro"][1]}</p>
        {note_card("Rýchla praktická diagnostika", config["bullets"])}
        {tables_html}
        {sections_html}
        {depth_html}
        <h2>{followup_title}</h2>
        <p>{followup_text}</p>
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
    parser = argparse.ArgumentParser(description="Conservatively expand VEVO retrofit wave 12 material/outdoor articles.")
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
                "wave": "retrofit-wave-12-materials-outdoor-five",
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
                "wave": "retrofit-wave-12-materials-outdoor-five",
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
