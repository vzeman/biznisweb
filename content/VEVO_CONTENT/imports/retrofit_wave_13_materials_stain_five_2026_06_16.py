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
OUT_JSON = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-13-materials-stain-five-2026-06-16.json"
MCP_RESULTS = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-13-materials-stain-five-2026-06-16-mcp-results.json"


ARTICLES = [
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-23-2026-06-11-articles.json",
        "slug": "co-je-bambusova-viskoza-maekkost-marketingove-tvrdenia-a-realna-starostlivost",
        "post_id": "2240",
        "url": "https://www.vevo.sk/n/co-je-bambusova-viskoza-maekkost-marketingove-tvrdenia-a-realna-starostlivost",
        "topic": "bamboo_viscose",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-20-2026-06-10-articles.json",
        "slug": "co-je-merino-vlna-vyhody-nevyhody-a-pranie-bez-zrazenia",
        "post_id": "2228",
        "url": "https://www.vevo.sk/n/co-je-merino-vlna-vyhody-nevyhody-a-pranie-bez-zrazenia",
        "topic": "merino_wool",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-20-2026-06-10-articles.json",
        "slug": "co-je-elastan-preco-je-v-leginach-spodnej-bielizni-a-sportovom-obleceni",
        "post_id": "2227",
        "url": "https://www.vevo.sk/n/co-je-elastan-preco-je-v-leginach-spodnej-bielizni-a-sportovom-obleceni",
        "topic": "elastane",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-20-2026-06-10-articles.json",
        "slug": "co-je-viskoza-vlastnosti-krcivost-zrazanie-a-starostlivost",
        "post_id": "2226",
        "url": "https://www.vevo.sk/n/co-je-viskoza-vlastnosti-krcivost-zrazanie-a-starostlivost",
        "topic": "viscose",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-18-2026-06-10-articles.json",
        "slug": "ako-odstranit-arasidove-maslo-z-tricka-obrusu-a-detskej-mikiny",
        "post_id": "2195",
        "url": "https://www.vevo.sk/n/ako-odstranit-arasidove-maslo-z-tricka-obrusu-a-detskej-mikiny",
        "topic": "peanut_butter",
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
    if kind == "stain":
        text = "Pri mastných škvrnách je dôležité najprv odstrániť prebytok, potom lokálne predčistiť a až následne prať. Produkt má pomôcť odmasťovaniu a čistote, nie prekryť zvyšok škvrny vôňou."
        product_text = "Vhodný základ na následné pranie po lokálnom predčistení mastnej škvrny. Pri jemných materiáloch najprv overte štítok a nepoužívajte zbytočne horúcu vodu."
    elif kind == "wool":
        text = "Pri vlne a merine je najdôležitejšie nepreháňať pranie. Najprv vetrajte, perte až pri reálnej potrebe a vždy rešpektujte štítok výrobcu."
        product_text = "Vhodný základ na bežné pranie mnohých textílií. Pri vlne, merine a kašmíre však vždy vyberajte postup podľa štítku a použite program alebo produkt určený pre jemnú starostlivosť, ak to výrobca vyžaduje."
    else:
        text = "Pri jemných a pružných materiáloch rozhoduje primerané dávkovanie, dobrý oplach a sušenie bez zbytočného tepla. Čistota je dôležitejšia než silné prevoňanie."
        product_text = "Vhodný základ na bežné pranie mnohých textílií, keď nechcete materiál zbytočne preťažovať agresívnym postupom. Pri veľmi jemných kusoch vždy rozhoduje štítok výrobcu."
    return clean(
        f"""
        <div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 24px 0; background: #f7fbf8;">
        <h2 style="margin-top: 0;">Odporúčané riešenie pre šetrnú starostlivosť</h2>
        <p>{text}</p>
        <div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff; margin: 14px 0;">
        <h3 style="margin-top: 0;">Prací gél hypoalergénny z Marseillského mydla 1L</h3>
        <p>{product_text}</p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="/p-1551/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l">Pozrieť produkt</a></p>
        </div>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="/c/vevo-home-care/pranie/praci-gel">Pozrieť kategóriu pracie gély</a></p>
        </div>
        """
    )


TOPICS = {
    "bamboo_viscose": {
        "marker": "Detailnejší pohľad na bambusovú viskózu, mäkkosť a reálnu starostlivosť",
        "product_kind": "gentle",
        "intro": [
            "Bambusová viskóza je mäkký regenerovaný celulózový materiál, ktorý sa často predáva cez pocit jemnosti a komfortu. Pri domácom praní je však dôležité vnímať ju skôr ako jemnú viskózu než ako odolný prírodný bambus. Mokrý úplet môže byť citlivejší na ťah, zle znáša drsné trenie a pri nevhodnom sušení vie stratiť tvar.",
            "Najviac otázok vzniká pri spodnej bielizni, pyžamách, tričkách, uterákoch a detských textíliách. V každej kategórii rozhoduje iný detail: elastan v bielizni, savosť pri uteráku, farbivo pri tričku a zvyšky pracieho produktu pri citlivej pokožke.",
        ],
        "bullets": [
            "<strong>Perte podľa hotového výrobku:</strong> samotné slovo bambus nestačí na výber programu.",
            "<strong>Chráňte mokrý tvar:</strong> jemné kusy nevešajte tak, aby sa vytiahli vlastnou váhou.",
            "<strong>Pri pokožke riešte oplach:</strong> zvyšky produktu môžu byť dôležitejšie než názov vlákna.",
            "<strong>Vôňu dávkujte mierne:</strong> pyžamá a bielizeň sú pri tele dlho.",
        ],
        "tables": [
            {
                "title": "Bambusová viskóza podľa typu textilu",
                "headers": ["Textil", "Riziko", "Lepší postup"],
                "rows": [
                    ("spodná bielizeň", "elastan, gumičky a kontakt s pokožkou", "ochranné vrecko, dobrý oplach, mierna vôňa"),
                    ("pyžamo", "dlhý kontakt s pokožkou", "neprevoňať príliš a úplne dosušiť"),
                    ("tričko", "tvar a švy za mokra", "prať naruby a sušiť bez ťahu"),
                    ("uterák alebo osuška", "savosť a zvyšky aviváže", "dávkovať striedmo a dobre opláchnuť"),
                ],
            },
            {
                "title": "Keď bambusová viskóza po praní nevyzerá dobre",
                "headers": ["Prejav", "Možná príčina", "Čo upraviť"],
                "rows": [
                    ("vytiahnuté ramená", "mokrý kus visel na úzkom vešiaku", "sušiť rozložené alebo s oporou"),
                    ("tvrdší dotyk", "zvyšky produktu alebo tvrdá voda", "znížiť dávku a pridať oplach"),
                    ("zatuchnutie", "pomalé sušenie v záhyboch", "rozložiť a dosušiť úplne"),
                    ("slabšia savosť", "film z aviváže", "obmedziť aviváž a kontrolovať oplach"),
                ],
            },
        ],
        "sections": [
            ("Ako prať bambusové tričko a pyžamo", "Bambusové tričko alebo pyžamo perte s podobne jemnými kúskami, naruby a bez preplnenia bubna. Ak je v zložení elastan, gumičky alebo jemné švy, vyberajte nižšiu mechanickú záťaž a opatrné sušenie. Pri pyžame je dôležitá aj intenzita vône, pretože textil je celú noc pri pokožke.", "Ak hľadáte porovnanie s bavlnou, nadväzuje článok <a href=\"/n/bambusove-vlakno-vs-bavlna-vyhody-nevyhody-a-pranie-pri-citlivej-pokozke\">bambusové vlákno vs bavlna</a>. Pri príbuzných materiáloch pomôže aj <a href=\"/n/modal-vs-lyocell-vs-viskoza-ako-sa-lisia-pri-prani-a-noseni\">modal vs lyocell vs viskóza</a>."),
            ("Ako sušiť bambusovú viskózu bez vyťahania", "Po praní textil nekrúťte a neťahajte za rukávy alebo ramienka. Jemný mokrý úplet je ťažší a citlivejší než suchý kus. Najbezpečnejšie je vytvarovať švy, vyhladiť látku rukami a sušiť tak, aby materiál neniesol vlastnú váhu na jednom bode.", "Ak sa textil už vytiahol, skúste ho po ďalšom praní vytvarovať za vlhka a sušiť s oporou. Pôvodný tvar sa nemusí vrátiť úplne, ale často sa dá zlepšiť, ak nebudete opakovať rovnaké mokré vešanie."),
            ("Bambusová viskóza a citlivá pokožka", "Pri citlivej pokožke nestačí vybrať mäkký materiál. Sledujte aj farbivá, zvyšky pracieho gélu, aviváž, intenzitu vône a úplné dosušenie. Textil môže byť na dotyk jemný, ale ak v ňom ostane prací film alebo vlhkosť, pokožka môže reagovať.", "Dobrý test je vyprať menšiu dávku s presným dávkovaním, dobrým oplachom a jemnou vôňou alebo bez nej. Ak sa komfort zlepší, problém bol skôr v rutine než v samotnom vlákne."),
            ("Odbornejší pohľad: názov materiálu nie je prací návod", "Bambusový zdroj celulózy je dôležitý pre označenie materiálu, ale pre práčku je rozhodujúci hotový výrobok. Viskózové vlákno sa správa inak než čistá bavlna, inak v úplete a inak v uterákovej väzbe. Preto rovnaké slovo na etikete nestačí na rovnaký program.", "Pri nákupe aj praní rozlišujte marketingové označenie, reálne zloženie a štítok starostlivosti. Najbezpečnejšia domáca rutina je šetrné pranie, dobrý oplach a sušenie bez ťahu."),
            ("Ako upraviť ďalšie pranie podľa výsledku", "Po ďalšom praní sledujte tvar, dotyk, pach a reakciu pokožky. Ak sa textil vyťahuje, upravte sušenie. Ak tvrdne, riešte dávkovanie a oplach. Ak je vôňa rušivá, uberte intenzitu. Tak sa starostlivosť prispôsobí skutočnému správaniu textilu.", "Bambusová viskóza môže byť veľmi príjemný materiál, ale najlepšie funguje vtedy, keď ju neperiete ako odolnú bavlnu ani ako športovú syntetiku."),
        ],
        "box": ("Rýchla zásada", "Bambusovú viskózu perte ako jemný hotový výrobok. Mäkkosť je výhoda, ale tvar, oplach a sušenie rozhodujú o tom, či zostane príjemná aj po viacerých cykloch."),
        "faq": [
            ("Je bambusová viskóza to isté ako bambusové vlákno?", "V bežnom textile ide často o regenerované celulózové vlákno z bambusového zdroja. Pri praní sa riaďte štítkom výrobku."),
            ("Prečo sa bambusové tričko vytiahlo?", "Mokrý jemný úplet mohol visieť pod vlastnou váhou alebo bol pri praní silno namáhaný."),
            ("Môžem používať aviváž?", "Pri niektorých kusoch áno, ale opatrne. Pri uterákoch a citlivej pokožke sledujte savosť, oplach a reakciu kože."),
        ],
    },
    "merino_wool": {
        "marker": "Detailnejší pohľad na merino vlnu, zápach, zrážanie a pranie bez poškodenia",
        "product_kind": "wool",
        "intro": [
            "Merino vlna je jemnejšia vlna, ktorá dobre pracuje s vlhkosťou a pachom. Preto sa používa v termo bielizni, ponožkách, svetroch a outdoor vrstvách. Jej výhoda však neznamená, že ju treba prať po každom nosení. Často pomôže vetranie, prestávka medzi noseniami a pranie až vtedy, keď je naozaj potrebné.",
            "Najväčšie riziká pri merine sú zrážanie, plstnatenie, strata tvaru, príliš silná mechanika a nevhodné teplo. Aj keď je merino praktické pri športe, stále ide o vlnu. Pri praní preto rozhoduje štítok, nízka teplota, jemný pohyb a sušenie bez ťahu.",
        ],
        "bullets": [
            "<strong>Najprv vetrajte:</strong> merino často nepotrebuje pranie po každom nosení.",
            "<strong>Chráňte pred teplom:</strong> horúca voda a sušička môžu spôsobiť zrazenie.",
            "<strong>Netrite a nekrúťte:</strong> mechanika je pri vlne rovnako dôležitá ako teplota.",
            "<strong>Sušte s oporou:</strong> mokrý sveter nevešajte za ramená.",
        ],
        "tables": [
            {
                "title": "Merino podľa typu výrobku",
                "headers": ["Výrobok", "Čo riešiť", "Lepšia rutina"],
                "rows": [
                    ("termo tričko", "pot a zápach po športe", "vetrať, prať až pri potrebe, sušiť voľne"),
                    ("merino ponožky", "vlhkosť a pach", "nechať preschnúť medzi noseniami"),
                    ("sveter", "tvar a plstnatenie", "jemné pranie, nízke otáčky, sušiť rozložené"),
                    ("detská vrstva", "pokožka a zvyšky produktu", "dobrý oplach a mierna vôňa"),
                ],
            },
            {
                "title": "Keď sa merino pokazí",
                "headers": ["Prejav", "Možná príčina", "Čo spraviť nabudúce"],
                "rows": [
                    ("sveter sa zrazil", "teplo alebo silná mechanika", "nižšia teplota a jemný program"),
                    ("povrch splstnatel", "trenie vlnených vlákien", "netrieť, nekrútiť, neprať s drsnými kusmi"),
                    ("rukávy sa vytiahli", "mokré vešanie", "sušiť rozložené a vytvarované"),
                    ("zápach ostal", "zlé vysušenie alebo príliš dlhé skladovanie vlhké", "vetrať a úplne dosušiť"),
                ],
            },
        ],
        "sections": [
            ("Ako prať merino termo oblečenie", "Merino termo oblečenie po nosení najprv vyvetrajte. Ak zápach po vyvetraní zmizne, nemusí ísť hneď do práčky. Keď už periete, vyberte postup podľa štítku a znížte mechanickú záťaž. Merino neperte s uterákmi, rifľami ani zipsami, ktoré by mohli poškodiť povrch.", "Ak ide o športový kus so zmesou syntetiky, stále sledujte najcitlivejšiu zložku. Viac k pachom pri vlnenom oblečení nadväzuje článok <a href=\"/n/ako-prat-vlneny-sveter-ked-zapacha-po-noseni\">ako prať vlnený sveter, keď zapácha</a>."),
            ("Ako prať merino sveter bez zrazenia", "Sveter perte iba vtedy, keď je to potrebné. Použite jemný režim podľa štítku, nízku teplotu a slabšiu mechaniku. Po praní ho nekrúťte, neťahajte za rukávy a nevešajte mokrý za ramená. Voda v úplete je ťažká a môže vytiahnuť tvar.", "Najbezpečnejšie je sveter jemne vytvarovať a sušiť rozložený. Ak je v zložení aj polyamid alebo elastan, neznamená to, že ho môžete prať ako bežnú syntetiku. Vlnená zložka stále určuje opatrnosť."),
            ("Merino, zápach a vetranie", "Merino vie zápach zvládať lepšie než mnohé syntetické vrstvy, ale nie je nezničiteľné. Ak ho necháte vlhké v taške, problém sa zhorší. Po nosení ho rozložte alebo zaveste tak, aby preschlo. Vetranie je šetrnejšie než zbytočné pranie.", "Pri silnom pote alebo špine pranie neodkladajte donekonečna. Rozdiel je medzi bežným prevetraním po nosení a skladovaním vlhkého kusu, ktorý začne zatuchnúť."),
            ("Odbornejší pohľad: prečo vlna reaguje na teplo a trenie", "Vlnené vlákna majú povrch, ktorý pri kombinácii vlhkosti, tepla a mechanického trenia môže zmeniť štruktúru. Preto sa často hovorí o plstnatení. Pri merine je vlákno jemnejšie, ale princíp opatrnosti zostáva rovnaký.", "Domáca prevencia je jednoduchá: menej zbytočného prania, stabilná nízka teplota, jemný pohyb, správny produkt podľa štítku a sušenie bez ťahu."),
            ("Ako upraviť ďalšie pranie podľa výsledku", "Ak merino po praní drží tvar, neexperimentujte zbytočne s teplejším programom. Ak sa zrazilo alebo splstnatelo, vrátiť pôvodný stav môže byť nemožné. Pri vytiahnutí skúste sušenie rozložené a tvarovanie za vlhka.", "Pri merine je prevencia lacnejšia než oprava. Jeden horúci alebo mechanicky silný cyklus môže poškodiť kus, ktorý predtým vydržal roky."),
        ],
        "box": ("Rýchla zásada", "Merino neperte zo zvyku. Najprv vetrajte, perte jemne podľa štítku a sušte rozložené bez ťahu."),
        "faq": [
            ("Treba merino prať po každom nosení?", "Nie. Často stačí vetranie, najmä ak nejde o silné spotenie alebo špinu."),
            ("Prečo sa merino zrazí?", "Najčastejšie pre teplo, mechanické trenie alebo nevhodné sušenie."),
            ("Môže ísť merino do sušičky?", "Iba ak to výslovne povoľuje štítok. Pri neistote sušte voľne rozložené."),
        ],
    },
    "elastane": {
        "marker": "Detailnejší pohľad na elastan, pružnosť, teplo a každodenné pranie",
        "product_kind": "gentle",
        "intro": [
            "Elastan je pružné vlákno, ktoré pomáha oblečeniu držať tvar a vrátiť sa späť po natiahnutí. Nájdete ho v legínach, spodnej bielizni, športových tričkách, ponožkách, detskom oblečení aj v zmesových tričkách. Práve malé percento elastanu často rozhoduje o tom, ako sa kus správa pri praní a sušení.",
            "Elastan nemá rád zbytočné teplo, agresívnu mechaniku, horúcu sušičku a niektoré nevhodné postupy. Ak legíny stratia pružnosť, guma sa vyťahá alebo spodná bielizeň nedrží tvar, problém často nie je v hlavnom materiáli, ale v pružnej zložke.",
        ],
        "bullets": [
            "<strong>Teplo je hlavné riziko:</strong> horúca voda a sušička môžu oslabiť pružnosť.",
            "<strong>Perte naruby:</strong> chránite povrch aj pružné vlákna v zmesi.",
            "<strong>Nepreplňte bubon:</strong> príliš veľa trenia zhoršuje tvar.",
            "<strong>Sušte mierne:</strong> pružné lemy a gumy nemajú rady prehriatie.",
        ],
        "tables": [
            {
                "title": "Elastan podľa typu oblečenia",
                "headers": ["Textil", "Čo sa môže pokaziť", "Lepší postup"],
                "rows": [
                    ("legíny", "strata pružnosti a presvitanie", "prať naruby, bez horúcej sušičky"),
                    ("spodná bielizeň", "gumičky a lemy sa vyťahajú", "ochranné vrecko a jemný program"),
                    ("športové tričko", "pach a deformácia", "prať včas a sušiť mierne"),
                    ("bavlnené tričko s elastanom", "zmena tvaru po teple", "nižšia teplota a šetrné sušenie"),
                ],
            },
            {
                "title": "Elastan po praní: diagnostika",
                "headers": ["Prejav", "Možná príčina", "Čo upraviť"],
                "rows": [
                    ("legíny nedržia", "teplo alebo opakované preťažovanie", "vynechať horúcu sušičku"),
                    ("lem sa vlní", "guma stratila pružnosť", "jemnejšia mechanika a nižšia teplota"),
                    ("oblečenie zapácha", "syntetická zložka a pot", "prať skôr, dobre sušiť"),
                    ("povrch sa žmolkuje", "trenie v bubne", "prať naruby a oddeliť drsné kusy"),
                ],
            },
        ],
        "sections": [
            ("Ako prať legíny s elastanom", "Legíny perte naruby, s podobnými materiálmi a bez uterákov alebo zipsov. Ak sú športové a spotené, nenechávajte ich dlho vo vlhkej taške. Pach sa potom rieši ťažšie a človek má tendenciu voliť silnejší program, ktorý pružnosti neprospieva.", "Pri športovej syntetike nadväzuje článok <a href=\"/n/ako-prat-syntetiku-polyester-a-elastan-aby-nezapachali-a-drzali-tvar\">ako prať syntetiku, polyester a elastan</a>. Pri zmesiach pomôže aj <a href=\"/n/co-je-zmesovy-material-preco-sa-oblecenie-zraza-alebo-sprava-inak-nez-cakate\">čo je zmesový materiál</a>."),
            ("Ako prať spodnú bielizeň s elastanom", "Spodná bielizeň má často gumičky, čipku, jemné švy a elastan. Použite ochranné vrecko, šetrný program a miernejšie sušenie. Príliš silné odstreďovanie alebo horúca sušička môže skrátiť životnosť pružných častí.", "Pri bielizni pri pokožke sledujte aj oplach a vôňu. Silná vôňa nie je náhrada za čistotu a pri citlivej pokožke môže byť rušivá."),
            ("Prečo elastan nemá rád sušičku", "Elastan je citlivý na teplo. Opakované horúce sušenie môže oslabiť pružnosť, najmä v lemoch, pásoch a úzkych častiach oblečenia. Nie vždy si to všimnete po jednom praní, často ide o postupné opotrebovanie.", "Ak štítok sušičku povoľuje, používajte šetrnejší režim. Ak si nie ste istí, sušenie na vzduchu je bezpečnejšie pre tvar aj pružnosť."),
            ("Odbornejší pohľad: malé percento môže rozhodovať", "Elastan tvorí často iba pár percent materiálu, ale bez neho by oblečenie nesedelo rovnako. Preto je chyba ignorovať ho len preto, že väčšinový materiál je bavlna, polyester alebo viskóza. Pri praní zmesového kusu môže práve elastan určovať limit teploty a sušenia.", "Praktická zásada je prať podľa najcitlivejšej zložky a hotového výrobku, nie podľa najvyššieho percenta na etikete."),
            ("Ako upraviť ďalšie pranie podľa výsledku", "Ak oblečenie s elastanom po praní drží tvar, rutinu nemeníte. Ak sa pás vyťahuje, znížte teplo. Ak legíny zapáchajú, riešte skladovanie po športe a sušenie. Ak sa povrch ničí, znížte trenie a perte naruby.", "Elastan vydrží najdlhšie, keď mu nedávate zbytočné teplo a nepoužívate agresívny cyklus ako univerzálne riešenie."),
        ],
        "box": ("Rýchla zásada", "Elastan je malá, ale citlivá zložka. Chráňte ho pred teplom, sušičkou a zbytočným trením."),
        "faq": [
            ("Prečo legíny stratili pružnosť?", "Často pre teplo, horúcu sušičku, opakované preťažovanie alebo nevhodné pranie."),
            ("Môže ísť elastan do sušičky?", "Len ak to povoľuje štítok. Pri neistote sušte na vzduchu."),
            ("Ako prať bielizeň s elastanom?", "V ochrannom vrecku, šetrne, s dobrým oplachom a bez zbytočného tepla."),
        ],
    },
    "viscose": {
        "marker": "Detailnejší pohľad na viskózu, mokrý tvar, krčivosť a zrážanie",
        "product_kind": "gentle",
        "intro": [
            "Viskóza je príjemné regenerované celulózové vlákno, ktoré krásne splýva, pôsobí chladivo a často sa používa v blúzkach, šatách, tričkách, pyžamách alebo podšívkach. Jej slabina sa ukáže pri vode: mokrý materiál môže byť citlivejší na ťah, krútenie a deformáciu.",
            "Pri viskóze sa najčastejšie rieši krčivosť, zrážanie, vytiahnutie ramien, tvrdší dotyk po praní a otázka, či ju možno žehliť. Odpoveď vždy začína štítkom, ale prax je podobná: menej mechaniky, rýchle vybratie z práčky, sušenie bez ťahu a opatrné žehlenie podľa výrobcu.",
        ],
        "bullets": [
            "<strong>Mokrý tvar je citlivý:</strong> viskózu nevešajte tak, aby sa vytiahla.",
            "<strong>Krčivosť riešte hneď:</strong> vybrať z práčky, vyhladiť a sušiť v tvare.",
            "<strong>Teplo opatrne:</strong> zrážanie často súvisí s praním aj sušením.",
            "<strong>Pri zmesiach čítajte celé zloženie:</strong> elastan, polyester alebo bavlna menia správanie.",
        ],
        "tables": [
            {
                "title": "Viskóza podľa typu oblečenia",
                "headers": ["Textil", "Riziko", "Lepší postup"],
                "rows": [
                    ("blúzka", "krčivosť a strata tvaru", "vybrať hneď, vyhladiť švy, sušiť bez ťahu"),
                    ("šaty", "mokrý materiál je ťažký", "nevešať na úzky vešiak za ramená"),
                    ("pyžamo", "kontakt s pokožkou a vôňa", "dobrý oplach, mierna intenzita vône"),
                    ("zmes s elastanom", "pružnosť a tvar", "nižšie teplo a šetrné sušenie"),
                ],
            },
            {
                "title": "Viskóza po praní: čo znamenajú prejavy",
                "headers": ["Prejav", "Možná príčina", "Čo skúsiť"],
                "rows": [
                    ("blúzka sa vytiahla", "mokré vešanie pod vlastnou váhou", "sušiť rozložené alebo s oporou"),
                    ("materiál sa zrazil", "teplo alebo nevhodný cyklus", "nižšia teplota a štítok"),
                    ("je veľmi pokrčená", "stála mokrá v bubne", "vybrať ihneď a vyhladiť"),
                    ("dotyk je tvrdší", "zvyšky produktu alebo slabý oplach", "upraviť dávkovanie"),
                ],
            },
        ],
        "sections": [
            ("Ako prať viskózovú blúzku", "Viskózovú blúzku perte podľa štítku a radšej v menšej dávke jemnejších kusov. Po skončení programu ju vyberte hneď, vyhlaďte švy a netrhajte za ramená. Ak zostane mokrá pokrčená v bubne, zhyby sa zvýraznia a materiál môže pôsobiť horšie.", "Pri podobných materiáloch nadväzuje článok <a href=\"/n/modal-vs-lyocell-vs-viskoza-ako-sa-lisia-pri-prani-a-noseni\">modal vs lyocell vs viskóza</a>. Pri bambusovej viskóze platí veľa podobných zásad, preto pomôže aj <a href=\"/n/co-je-bambusova-viskoza-maekkost-marketingove-tvrdenia-a-realna-starostlivost\">čo je bambusová viskóza</a>."),
            ("Ako sušiť viskózu bez vytiahnutia", "Najväčšia chyba je zavesiť mokré šaty alebo blúzku za úzke ramená. Mokrý materiál je ťažší a môže sa vytiahnuť. Lepšie je sušiť s oporou, vytvarovať švy a nechať materiál preschnúť bez bodového ťahu.", "Ak viskóza patrí do zmesi s elastanom, sledujte aj pružné časti. Teplo a ťah môžu zhoršiť tvar aj tam, kde samotná viskóza ešte vyzerá dobre."),
            ("Ako riešiť krčivosť viskózy", "Viskóza sa môže krčiť, najmä ak zostane po praní stlačená. Pomáha rýchle vybratie, jemné vyhladenie rukami a sušenie v tvare. Žehlenie robte podľa štítku, často z rubu alebo cez tenkú látku.", "Ak sa snažíte krčivosť riešiť veľmi vysokým teplom, môžete si poškodiť farbu, tvar alebo zmesové časti. Lepšia je prevencia hneď po praní."),
            ("Odbornejší pohľad: viskóza za mokra nie je rovnaká ako za sucha", "Viskóza pôsobí na tele príjemne práve pre svoj splývavý charakter, ale pri vode sa mení jej praktická odolnosť. Preto sa s mokrým kusom zaobchádza opatrnejšie než so suchým. Krútenie, ťahanie a vešanie sú časté dôvody deformácie.", "Domáca starostlivosť by mala chrániť tvar: menšia dávka, menej mechaniky, rýchle vybratie a sušenie bez ťahu."),
            ("Ako upraviť ďalšie pranie podľa výsledku", "Ak sa viskóza vytiahla, začnite sušením. Ak sa zrazila, riešte teplotu a štítok. Ak je tvrdá, znížte dávku produktu a skontrolujte oplach. Ak sa veľmi krčí, nenechávajte ju stáť mokrú v bubne.", "Viskóza nie je problematická, keď ju poznáte. Problémy vznikajú najmä vtedy, keď sa perie ako odolná bavlna alebo športová syntetika."),
        ],
        "box": ("Rýchla zásada", "Viskózu perte šetrne a po praní ju rýchlo vytvarujte. Najviac rozhoduje mokrý tvar, nie iba samotný prací program."),
        "faq": [
            ("Prečo sa viskóza zrazila?", "Často pre teplo, nevhodný cyklus alebo sušenie. Vždy sledujte štítok konkrétneho výrobku."),
            ("Môže sa viskóza žehliť?", "Ak to povoľuje štítok. Často je bezpečnejšie žehliť z rubu alebo cez tenkú látku."),
            ("Prečo sa viskózová blúzka vytiahla?", "Mokrý materiál mohol visieť pod vlastnou váhou alebo bol pri praní silno namáhaný."),
        ],
    },
    "peanut_butter": {
        "marker": "Detailnejší postup na arašidové maslo, mastný fľak a detské oblečenie",
        "product_kind": "stain",
        "intro": [
            "Arašidové maslo je nepríjemná škvrna preto, že kombinuje tuk, hustú pastu, bielkoviny a často aj cukor alebo soľ. Na tričku, obruse alebo detskej mikine sa nespráva ako obyčajné blato. Ak ho hneď zalejete horúcou vodou alebo silno rozotriete, môžete mastnú časť rozšíriť hlbšie do vlákien.",
            "Najbezpečnejší postup je najprv mechanicky odobrať prebytok tupou hranou, potom lokálne odmasťovať a až následne prať podľa štítku. Pri detskom oblečení, farebných tričkách a obrusoch rozhoduje aj materiál: bavlna znesie viac než viskóza, vlna alebo jemná zmes.",
        ],
        "bullets": [
            "<strong>Nešúchať nasucho:</strong> mastnú pastu môžete vtlačiť hlbšie do vlákien.",
            "<strong>Najprv odobrať prebytok:</strong> lyžičkou alebo tupou hranou smerom od okraja.",
            "<strong>Predčistiť lokálne:</strong> cieľ je rozpustiť mastnú časť pred hlavným praním.",
            "<strong>Sušičku až po kontrole:</strong> teplo môže zvyšok mastnoty zafixovať.",
        ],
        "tables": [
            {
                "title": "Arašidové maslo podľa textilu",
                "headers": ["Textil", "Riziko", "Postup"],
                "rows": [
                    ("bavlnené tričko", "mastná mapa po praní", "odobrať prebytok, predčistiť, prať podľa farby"),
                    ("obrus", "škvrna sa rozšíri do väzby", "pracovať od okraja ku stredu a neprehriať"),
                    ("detská mikina", "zvyšky v hrubšom úplete", "predčistiť z oboch strán, dobre opláchnuť"),
                    ("jemná viskóza", "tvar a citlivosť za mokra", "bez trenia, podľa štítku, opatrne"),
                ],
            },
            {
                "title": "Čo nerobiť pri arašidovom masle",
                "headers": ["Chyba", "Prečo škodí", "Lepšie riešenie"],
                "rows": [
                    ("horúca voda hneď na začiatku", "môže zhoršiť mastno-bielkovinový zvyšok", "najprv odobrať a odmasťovať lokálne"),
                    ("silné trenie", "rozotrie pastu do vlákien", "jemne prikladať a pracovať postupne"),
                    ("sušička bez kontroly", "teplo zafixuje mastný tieň", "skontrolovať po praní pri dennom svetle"),
                    ("priveľa produktu bez oplachu", "zvyšky môžu ostať v tkanine", "dávkovať primerane a dobre vypláchnuť"),
                ],
            },
        ],
        "sections": [
            ("Ako odstrániť arašidové maslo z trička", "Najprv zoškrabte prebytok tupou hranou. Neťahajte škvrnu po látke do strán. Potom naneste malé množstvo vhodného pracieho gélu alebo odmasťovacieho postupu podľa štítku a nechajte krátko pôsobiť. Textil jemne prepracujte z rubovej strany, aby sa mastnota uvoľňovala von z vlákna.", "Po lokálnom predčistení perte podľa farby a materiálu. Pred sušením skontrolujte, či nezostal mastný tieň. Ak áno, postup zopakujte pred sušičkou alebo žehlením."),
            ("Ako odstrániť arašidové maslo z obrusu", "Pri obruse býva problém väčšia plocha a väzba látky. Prebytok odoberajte od okrajov ku stredu, aby sa škvrna nerozšírila. Ak je obrus ľanový, bavlnený alebo zo zmesi, postupujte podľa najcitlivejšej zložky a nesnažte sa škvrnu vyvariť bez kontroly štítku.", "Pri mastných kuchynských škvrnách nadväzuje článok <a href=\"/n/ako-odstranit-olivovy-olej-z-lanovej-kosele-bez-mastnej-mapy\">ako odstrániť olivový olej</a> a pri podobnej mastnote aj <a href=\"/n/ako-odstranit-majonezu-a-dressing-z-obrusu-bez-mastneho-flaku\">majonéza a dressing z obrusu</a>."),
            ("Ako odstrániť arašidové maslo z detskej mikiny", "Detská mikina môže byť hrubšia, s rebrovanými lemami alebo potlačou. Arašidové maslo sa môže dostať do úpletu a zostať tam ako mastný tieň. Po odobratí prebytku predčistite škvrnu z líca aj rubu podľa toho, kde je viac zvyškov, a mikinu perte s podobnými farbami.", "Ak je na mikine potlač, nešúchajte ju agresívne. Pri detskom textile je dôležitý aj oplach, aby v látke nezostali zvyšky predčistenia."),
            ("Čo robiť, keď škvrna ostala po praní", "Ak po praní vidíte mastnú mapu, nedávajte textil do sušičky. Teplo môže zvyšok fixovať. Radšej škvrnu znovu lokálne odmastite, nechajte pôsobiť a opakujte pranie podľa štítku. Pri starej škvrne môže byť potrebných viac jemných krokov namiesto jedného agresívneho zásahu.", "Mastné škvrny sa často ukážu až po vysušení. Preto kontrolujte pri dennom svetle, najmä na farebných tričkách a obruse."),
            ("Odbornejší pohľad: prečo arašidové maslo drží v látke", "Arašidové maslo obsahuje olejovú časť, pevné čiastočky a bielkovinové zvyšky. Tuk sa viaže na vlákna inak než vodnatá škvrna a hustá pasta ho vie vtlačiť do štruktúry látky. Preto obyčajné opláchnutie vodou často nestačí.", "Domáci postup musí najprv znížiť množstvo pasty, potom riešiť tuk a až následne prať celý kus. Tento postup je šetrnejší aj účinnejší než silné trenie bez prípravy."),
            ("Ako upraviť ďalšie pranie podľa výsledku", "Ak po praní ostala iba svetlá mapa, zamerajte sa na mastnú časť. Ak ostal farebný alebo hnedý tieň, skontrolujte zloženie arašidového masla a materiál textilu. Ak ostal pach, problém môže byť v zvyškoch tuku v hrubšej vrstve, nie v nedostatku vône.", "Pri detskom oblečení je lepšie postup zopakovať jemne než použiť neprimerane silné prípravky na citlivý materiál alebo potlač."),
        ],
        "box": ("Rýchla zásada", "Arašidové maslo najprv odoberte, potom lokálne odmasťujte a až potom perte. Sušičku použite až po kontrole, že mastný tieň zmizol."),
        "faq": [
            ("Môžem použiť horúcu vodu?", "Nie ako prvý krok. Najprv odoberte prebytok a predčistite mastnú časť podľa štítku."),
            ("Čo ak škvrna ostala po praní?", "Nesušte horúco. Znovu lokálne predčistite a perte podľa materiálu."),
            ("Ako postupovať pri detskej mikine s potlačou?", "Predčisťujte jemne, netrite potlač agresívne a dobre opláchnite zvyšky prípravku."),
        ],
    },
}


DEPTH_SECTIONS = {
    "bamboo_viscose": [
        (
            "Domáci test po treťom praní",
            "Pri bambusovej viskóze sa oplatí nehodnotiť iba prvý dotyk v obchode. Po treťom praní skontrolujte švy, dĺžku rukávov, mäkkosť, pach a to, či materiál pri pokožke nepôsobí ťažko alebo vlhko. Ak sa zhoršil tvar, zamerajte sa na sušenie. Ak sa zhoršil dotyk, problém môže byť v oplachu alebo dávkovaní.",
            "Takýto test pomáha odlíšiť marketingový dojem od reálnej domácej starostlivosti. Bambusová viskóza môže byť veľmi príjemná, ale iba vtedy, keď ju nepreťažujete trením, teplom a mokrým vešaním za slabé body.",
        ),
        (
            "Ako kombinovať bambusovú viskózu s vôňou",
            "Pri bielizni, pyžame a detských veciach začnite s miernou vôňou. Materiál je často blízko pokožky celé hodiny a príliš intenzívna vôňa môže pôsobiť rušivo. Ak textil po praní zapácha, najprv riešte sušenie, preplnený bubon a zvyšky pracieho produktu.",
            "Vôňa má doplniť čistotu, nie zakryť problém. Pri bambusovej viskóze je praktickejšie dosiahnuť mäkký, dobre vypláchnutý a suchý textil než silný parfumovaný efekt.",
        ),
    ],
    "merino_wool": [
        (
            "Domáci test merina po sezóne",
            "Na konci sezóny skontrolujte, či merino nezmenilo rozmer, nepôsobí splstnatene a či v podpazuší alebo pri golieri nezostal pach. Ak je tvar stabilný a pach po vyvetraní mizne, rutina je pravdepodobne správna. Ak sa materiál zmenšil, hľadajte teplo alebo mechaniku.",
            "Pri merine je dobré zapisovať si, čo funguje pri konkrétnom kuse. Termo tričko, ponožky a sveter nemusia potrebovať rovnaký postup. Rozdiel je v hrúbke, zmesi aj spôsobe nosenia.",
        ),
        (
            "Ako skladovať merino medzi praniami",
            "Merino po nosení neskladajte vlhké do skrine. Nechajte ho preschnúť a až potom odložte. Pri dlhšom skladovaní musí byť čisté a suché, inak sa pach alebo zvyšky potu môžu vrátiť hneď po ďalšom oblečení.",
            "Skladovanie je súčasť starostlivosti. Ak sa merino dobre vyvetrá, často znížite počet praní a tým aj riziko zrážania alebo opotrebovania.",
        ),
    ],
    "elastane": [
        (
            "Domáci test pružnosti po praní",
            "Pri elastane sledujte návratnosť materiálu. Legíny, spodná bielizeň alebo športové tričko by sa po natiahnutí mali vrátiť späť bez zvlnených lemov. Ak sa pás alebo guma vyťahuje, problém môže byť v teple, sušičke alebo dlhodobom preťažovaní.",
            "Test robte po úplnom vysušení, nie hneď po vybratí z práčky. Vlhký textil sa správa inak a môže pôsobiť ťažší. Ak sa pružnosť zhoršuje postupne, upravte sušenie skôr, než sa guma zničí natrvalo.",
        ),
        (
            "Ako triediť oblečenie s elastanom",
            "Veci s elastanom nepatria automaticky do jednej dávky so všetkým športovým oblečením. Oddelte jemnú spodnú bielizeň, legíny s hladkým povrchom a hrubšie mikiny so zipsami. Znížite trenie a ochránite pružné časti.",
            "Pri zmesiach s bavlnou alebo viskózou sledujte aj hlavný materiál. Elastan určuje teplo a pružnosť, viskóza mokrý tvar, bavlna zrážanie a polyester zápach. Dobrý program je kompromis podľa najcitlivejšej časti.",
        ),
    ],
    "viscose": [
        (
            "Domáci test viskózy po praní",
            "Po praní viskózy si všimnite tri signály: dĺžku, krčivosť a dotyk. Ak sa blúzka predĺžila, pravdepodobne visela mokrá. Ak sa zrazila, skontrolujte teplotu a sušenie. Ak je tvrdšia, upravte dávkovanie a oplach.",
            "Viskóza často potrebuje rýchlu reakciu po skončení programu. Nenechávajte ju ležať mokrú v bubne, lebo krčivosť a deformácia sa zhoršia práve vtedy, keď je textil stlačený a vlhký.",
        ),
        (
            "Ako viskózu zaradiť do bežného prania",
            "Najlepšie funguje samostatná jemná dávka s podobnými materiálmi. Viskózová blúzka nepatrí k uterákom, rifliam ani ťažkým mikinám. Ak ju periete spolu s drsnými vecami, zvyšujete trenie aj riziko vytiahnutia.",
            "Pri zmiešanom šatníku si vytvorte jednoduché skupiny: pevná bavlna, uteráky, športová syntetika a jemné celulózové materiály. Viskóza patrí do poslednej skupiny spolu s modalom, lyocellom a bambusovou viskózou, ak to štítky dovoľujú.",
        ),
    ],
    "peanut_butter": [
        (
            "Čerstvá škvrna vs stará škvrna z arašidového masla",
            "Pri čerstvej škvrne máte výhodu, že prebytok ešte často sedí na povrchu. Odoberte ho tupou hranou a netlačte ho do látky. Potom riešte tuk lokálne a perte až po predčistení. Pri starej škvrne už môže byť mastnota hlbšie vo vlákne, preto postup opakujte trpezlivejšie a nesnažte sa všetko vyriešiť jedným silným zásahom.",
            "Starú škvrnu pred praním skontrolujte na svetle. Ak je okolo nej mastná mapa, pracujte najprv s ňou. Ak je viditeľný aj farebný zvyšok, postupujte podľa materiálu a farby textilu. Pri detskej mikine s potlačou je lepšie viac jemných krokov než agresívne trenie.",
        ),
        (
            "Postup krok za krokom pred praním",
            "Položte textil škvrnou nahor, podložte čistou handričkou a odoberte prebytok. Potom naneste malé množstvo pracieho gélu na mastnú časť, jemne ho zapracujte prstami alebo mäkkou kefkou podľa odolnosti látky a nechajte krátko pôsobiť. Pri jemných materiáloch vynechajte kefku a pracujte len dotykom.",
            "Následne opláchnite alebo vložte do prania podľa štítku. Po vypraní nekontrolujte škvrnu až po sušičke. Skontrolujte ju ešte vlhkú a potom znovu pri dennom svetle po voľnom vysušení. Ak zostal mastný tieň, opakujte predčistenie.",
        ),
        (
            "Ako postupovať podľa farby a materiálu",
            "Biele bavlnené tričko znesie iný postup než tmavá detská mikina s potlačou alebo viskózový obrus. Pri farebných materiáloch najprv testujte na menej viditeľnom mieste a vyhnite sa horúcej vode bez kontroly štítku. Pri jemných materiáloch je dôležitejšie chrániť tvar než škvrnu agresívne vydrhnúť.",
            "Ak je materiál vlna, viskóza alebo jemná zmes, zvážte šetrnejší postup alebo profesionálne čistenie. Arašidové maslo je síce bežná škvrna, ale nie každý textil znesie rovnaké odmasťovanie.",
        ),
        (
            "Kontrola pred sušením a žehlením",
            "Po praní škvrnu nehodnoťte iba podľa toho, či vonia čisto. Mastný zvyšok môže byť viditeľný ako tmavšia mapa, ktorá sa ukáže až pri svetle alebo po čiastočnom preschnutí. Textil preto pred sušičkou alebo žehlením skontrolujte z líca aj z rubu. Ak zostal tieň, znovu predčistite iba postihnuté miesto.",
            "Tento krok je dôležitý najmä pri tričku a detskej mikine. Teplo zo sušičky alebo žehličky môže zvyšok tuku zafixovať a ďalšie čistenie bude ťažšie. Pri obrusoch kontrolujte aj okolie pôvodnej škvrny, pretože mastnota sa vie rozšíriť za hranicu viditeľnej pasty.",
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
    parser = argparse.ArgumentParser(description="Conservatively expand VEVO retrofit wave 13 material/stain articles.")
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
                "wave": "retrofit-wave-13-materials-stain-five",
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
                "wave": "retrofit-wave-13-materials-stain-five",
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
