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
OUT_JSON = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-06-mixed-five-2026-06-16.json"
MCP_RESULTS = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-06-mixed-five-2026-06-16-mcp-results.json"


ARTICLES = [
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-26-2026-06-16-quality-update.json",
        "slug": "ako-funguje-praci-gel-tenzidy-enzymy-ph-a-davkovanie-pri-beznom-prani",
        "post_id": "2255",
        "url": "https://www.vevo.sk/n/ako-funguje-praci-gel-tenzidy-enzymy-ph-a-davkovanie-pri-beznom-prani",
        "title": "Ako funguje prací gél: tenzidy, enzýmy, pH a dávkovanie pri bežnom praní",
        "expansion": "gel_science",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-20-2026-06-10-articles.json",
        "slug": "polyester-vs-bavlna-rozdiely-pri-noseni-prani-a-voni",
        "post_id": "2225",
        "url": "https://www.vevo.sk/n/polyester-vs-bavlna-rozdiely-pri-noseni-prani-a-voni",
        "title": "Polyester vs bavlna: rozdiely pri nosení, praní a vôni",
        "expansion": "polyester_cotton",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-16-2026-06-10-articles.json",
        "slug": "ako-odstranit-pivo-z-tricka-obrusu-a-sedacky-bez-zapachu",
        "post_id": "2157",
        "url": "https://www.vevo.sk/n/ako-odstranit-pivo-z-tricka-obrusu-a-sedacky-bez-zapachu",
        "title": "Ako odstrániť pivo z trička, obrusu a sedačky bez zápachu",
        "expansion": "beer_stain",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-16-2026-06-10-articles.json",
        "slug": "ako-prat-vlneny-sveter-ked-zapacha-po-noseni",
        "post_id": "2152",
        "url": "https://www.vevo.sk/n/ako-prat-vlneny-sveter-ked-zapacha-po-noseni",
        "title": "Ako prať vlnený sveter, keď zapácha po nosení",
        "expansion": "wool_sweater_odor",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-16-2026-06-10-articles.json",
        "slug": "ako-odstranit-parfumovy-flak-z-oblecenia-a-jemnych-latok",
        "post_id": "2149",
        "url": "https://www.vevo.sk/n/ako-odstranit-parfumovy-flak-z-oblecenia-a-jemnych-latok",
        "title": "Ako odstrániť parfumový fľak z oblečenia a jemných látok",
        "expansion": "perfume_stain",
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


EXPANSIONS = {
    "gel_science": clean(
        f"""
        <h2>Ako prací gél reálne pracuje vo vode</h2>
        <p>Prací gél nefunguje tak, že špinu jednoducho prekryje vôňou. Základom sú povrchovo aktívne látky, ktoré pomáhajú uvoľniť mastnotu a nečistoty z vlákien do vody. Ďalšie zložky môžu stabilizovať pH, pomáhať pri konkrétnych typoch škvŕn alebo zlepšovať rozptýlenie nečistôt v pracom roztoku. Výsledok však vždy závisí od vody, teploty, času, pohybu bubna a oplachu.</p>
        <p>Pre bežnú domácnosť je dôležité hlavne to, že prací gél potrebuje priestor a správnu dávku. Ak ho nalejete priveľa, nemusí sa lepšie prať. Naopak, zvyšky môžu zostať vo vláknach, najmä pri krátkom programe, preplnenom bubne alebo slabo opláchnutej dávke.</p>
        {note_card("Čo rozhoduje o výsledku", [
            "<strong>Voda:</strong> tvrdosť vody mení dávkovanie aj pocit z bielizne po vysušení.",
            "<strong>Čas programu:</strong> krátky program má menej priestoru na rozptýlenie a oplach.",
            "<strong>Pohyb v bubne:</strong> textil sa musí prevracať, nie tlačiť v jednej hmote.",
            "<strong>Oplach:</strong> čistota nie je hotová, kým z textilu neodídu zvyšky prostriedku."
        ])}
        <h2>Čo znamenajú tenzidy, enzýmy a pH v praxi</h2>
        {table(["Zložka alebo faktor", "Praktický význam", "Na čo si dať pozor"], [
            ("Tenzidy", "pomáhajú uvoľňovať mastnotu a bežné nečistoty", "pri prebytku môžu zanechať film"),
            ("Enzýmy", "môžu cieliť na niektoré organické škvrny", "neznamenajú, že každý materiál znesie rovnaký program"),
            ("pH", "ovplyvňuje prostredie prania", "pri citlivých materiáloch rešpektovať štítok"),
            ("Teplota", "mení rýchlosť a účinnosť procesu", "vyššia teplota nie je vhodná pre každý textil"),
            ("Oplach", "odvádza zvyšky z vlákien", "slabý oplach sa prejaví tvrdosťou, lepkavosťou alebo pachom"),
        ])}
        <h2>Prečo viac gélu nemusí znamenať lepšie pranie</h2>
        <p>Ak je bielizeň veľmi špinavá, prirodzená reakcia je pridať viac gélu. Funguje to však iba do určitej miery. Pri prebytku produktu sa zvyšuje záťaž na oplach a výsledkom môže byť ťažší, lepkavý alebo príliš parfumovaný textil. Pri špine ako blato, mastnota alebo pot je často dôležitejší správny postup: predčistenie, menšia dávka v bubne, vhodný program a dôkladné sušenie.</p>
        <p>Pri citlivejšej pokožke je táto rovnováha ešte dôležitejšia. Jemnejší produkt pomôže, ale ak je dávka príliš vysoká alebo je bubon preplnený, zvyšky môžu zostať v textile bez ohľadu na to, aký produkt používate.</p>
        <h2>Kontrola po praní: čo vám povie textil</h2>
        <p>Po vysušení sledujte tri veci: vôňu, dotyk a návrat pachu pri nosení. Ak bielizeň príliš silno vonia alebo lepí, použili ste pravdepodobne priveľa produktu alebo bol slabý oplach. Ak sa športové oblečenie po zahriatí tela rýchlo rozvonia potom, problém môže byť v krátkom programe, syntetike alebo odkladaní vlhkých vecí.</p>
        <p>Ak je bielizeň tvrdá, riešte aj tvrdosť vody a spôsob sušenia. Prací gél je iba jedna časť procesu. Stabilný výsledok vznikne až vtedy, keď spolu sedia dávka, program, náplň, oplach a sušenie.</p>
        <div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 24px 0; background: #f7fbf8;">
        <h2 style="margin-top: 0;">Praktický základ pre bežné pranie</h2>
        <p>Pri každodennom praní začnite primeranou dávkou a nepreplneným bubnom. Produkt má pomôcť praniu, nie kompenzovať chyby v programe.</p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="/p-1551/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l">Pozrieť prací gél</a></p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="/c/vevo-home-care/pranie/praci-gel">Pozrieť pracie gély</a></p>
        </div>
        <h2>Jednoduché pravidlo pre ďalšie pranie</h2>
        <p>Ak je bielizeň čistá, príjemná na dotyk a po vysušení nezapácha, dávka je pravdepodobne správna. Ak lepí, tvrdne alebo príliš silno vonia, najprv upravte dávku, náplň a oplach. Až potom má zmysel meniť produkt alebo pridávať vôňu.</p>
        """
    ),
    "polyester_cotton": clean(
        f"""
        <h2>Polyester a bavlna sa pri pote správajú rozdielne</h2>
        <p>Polyester je syntetické vlákno, ktoré rýchlo schne a často drží tvar. Preto sa používa v športovom oblečení, funkčných tričkách, mikinách aj zmesových materiáloch. Bavlna je prírodné vlákno, ktoré dobre saje vlhkosť a býva príjemné na bežné nosenie, ale po nasiaknutí schne pomalšie. Rozdiel pri vôni vzniká najmä v tom, ako materiál pracuje s potom, vlhkosťou a zvyškami pracieho prostriedku.</p>
        <p>Polyester môže po tréningu zapáchať aj vtedy, keď na pohľad nie je špinavý. Bavlna zas môže dlhšie držať vlhkosť a zatuchnúť, ak sa suší pomaly alebo sa odloží vlhká. Preto nie je dobré prať oba materiály vždy rovnakou rutinou.</p>
        {note_card("Rýchle rozhodnutie podľa situácie", [
            "<strong>Šport a pot:</strong> polyester perte skoro, v menšej dávke a s dobrým oplachom.",
            "<strong>Bežné nosenie:</strong> bavlna je príjemná, ale po nasiaknutí potrebuje rýchle sušenie.",
            "<strong>Zmesové materiály:</strong> riaďte sa najcitlivejšou časťou zmesi a štítkom.",
            "<strong>Vôňa:</strong> pridávajte ju až po vyriešení čistoty, nie na prekrytie pachu."
        ])}
        <h2>Porovnanie pri nosení, praní a sušení</h2>
        {table(["Vlastnosť", "Polyester", "Bavlna"], [
            ("Schnutie", "zvyčajne rýchlejšie", "pomalšie, najmä pri hrubšej tkanine"),
            ("Pot a pach", "pach sa môže vracať pri zahriatí tela", "vlhkosť môže zatuchnúť pri pomalom sušení"),
            ("Tvar", "často drží tvar, závisí od zmesi", "môže sa zraziť alebo vytiahnuť podľa úpravy"),
            ("Pranie", "nepreplniť, nepoužívať zbytočne veľa gélu", "riešiť teplotu, triedenie a sušenie"),
            ("Aviváž", "pri športovej syntetike často opatrne", "podľa typu textilu a želaného výsledku"),
        ])}
        <h2>Ako prať polyesterové tričko po tréningu</h2>
        <p>Polyesterové športové tričko nenechávajte dlho zatvorené v taške. Pot a vlhkosť sa v textile držia a pach sa môže po ďalšom nosení rýchlo vrátiť. Pred praním nechajte veci preschnúť alebo ich perte čo najskôr. V práčke zvoľte menšiu dávku, vhodný program a nepreháňajte množstvo pracieho gélu.</p>
        <p>Pri syntetike býva dôležitý oplach. Ak v textile ostane veľa produktu alebo parfumácie, oblečenie môže po zahriatí tela pôsobiť ešte ťažšie. Pri opakovanom pachu skúste skôr menšiu náplň a lepší program než viac vône.</p>
        <h2>Ako prať bavlnu, aby nezatuchla</h2>
        <p>Bavlna dobre saje, čo je príjemné pri nosení, ale znamená to aj viac vlhkosti po praní. Hrubšie bavlnené tričká, uteráky alebo mikiny potrebujú priestor v bubne a rýchle sušenie. Ak sa sušia pomaly v nevetranej miestnosti, môžu zatuchnúť aj po správnom praní.</p>
        <p>Pri bielej bavlne sledujte teplotu a štítok. Pri farebnej bavlne riešte triedenie a púšťanie farieb. Pri zmesiach polyesteru a bavlny berte do úvahy obe vlastnosti: syntetika môže držať pach, bavlna môže držať vlhkosť.</p>
        <div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 24px 0; background: #f7fbf8;">
        <h2 style="margin-top: 0;">Univerzálny základ pre bežné zmesi</h2>
        <p>Pri bežných bavlnených a polyesterových zmesiach pomôže primeraná dávka pracieho gélu, nepreplnený bubon a rýchle sušenie po praní.</p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="/p-1551/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l">Pozrieť prací gél</a></p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="/c/vevo-home-care/pranie/praci-gel">Pozrieť pracie gély</a></p>
        </div>
        <h2>Jednoduché pravidlo pre polyester vs bavlnu</h2>
        <p>Polyester riešte hlavne cez pot, menšiu dávku v práčke a dôkladný oplach. Bavlnu riešte cez vlhkosť, správnu teplotu a rýchle sušenie. Pri zmesi materiálov sa riaďte štítkom a nečakajte, že jedna silná vôňa vyrieši rozdielne správanie vlákien.</p>
        """
    ),
    "beer_stain": clean(
        f"""
        <h2>Prečo pivo zanechá pach aj po vysušení</h2>
        <p>Pivo nie je len voda. Obsahuje zvyšky cukrov, kvasných látok a vône nápoja, ktoré sa môžu dostať do vlákna alebo čalúnenia. Keď sa miesto iba vysuší bez vyčistenia, viditeľný fľak sa môže stratiť, ale pach sa vráti pri vlhkosti alebo teple. Pri sedačke je riziko ešte vyššie, pretože tekutina môže prejsť pod povrch do výplne.</p>
        <p>Preto je pri pive dôležité rýchlo odsatie, studenšia voda, lokálne čistenie a dôkladné sušenie. Horúca voda, trenie a parfumovanie mokrého miesta môžu situáciu zhoršiť: fľak sa rozšíri, pach sa prekryje len dočasne a textil môže ostať vlhký vo vnútri.</p>
        {note_card("Rýchly postup podľa povrchu", [
            "<strong>Tričko:</strong> odsajte prebytok, prepláchnite zo zadnej strany a perte podľa štítku.",
            "<strong>Obrus:</strong> neodkladajte ho mokrý v koši, najprv odstráňte tekutinu.",
            "<strong>Sedačka:</strong> čistite lokálne, nenamáčajte výplň a sušte prúdením vzduchu.",
            "<strong>Zápach:</strong> riešte zdroj vlhkosti, nie iba prevoňanie."
        ])}
        <h2>Diagnostika škvrny od piva</h2>
        {table(["Situácia", "Čo urobiť ako prvé", "Čomu sa vyhnúť"], [
            ("Čerstvé pivo na tričku", "odsávať papierovou utierkou a prepláchnuť", "šúchať fľak do strán"),
            ("Pivo na obruse", "oddeliť od suchej bielizne a predprať", "nechať zaschnúť s cukrami vo vlákne"),
            ("Pivo na sedačke", "odsávať tlakovo, čistiť lokálne", "naliať veľa vody do výplne"),
            ("Zaschnutý pach", "navlhčiť len kontrolovane a znovu čistiť", "maskovať vôňou bez čistenia"),
            ("Jemný materiál", "testovať na skrytom mieste", "použiť agresívny postup bez skúšky"),
        ])}
        <h2>Ako postupovať pri tričku a obruse</h2>
        <p>Pri oblečení alebo obruse začnite tým, že tekutinu odsajete. Ak je fľak čerstvý, preplachujte ho zo zadnej strany, aby sa zvyšky dostávali von z vlákna, nie hlbšie do textilu. Potom použite primerané množstvo pracieho gélu a vhodný program podľa štítku. Pred sušením skontrolujte, či fľak a pach zmizli.</p>
        <p>Ak textil po praní stále cítiť pivom, nedávajte ho do sušičky ani na radiátor. Teplo môže zvyšky zafixovať alebo zvýrazniť pach. Radšej zopakujte lokálne ošetrenie a perte menšiu dávku, aby mal textil dobrý oplach.</p>
        <h2>Ako čistiť pivo zo sedačky</h2>
        <p>Pri sedačke je cieľom odstrániť tekutinu bez toho, aby ste ju zatlačili hlbšie. Prikladajte savú utierku, jemne tlačte a postup opakujte. Čistiaci roztok používajte v malom množstve a vždy najprv otestujte na skrytom mieste. Po čistení musí miesto preschnúť aj vo vnútri, inak sa pach vráti.</p>
        <p>Ak je škvrna veľká alebo ide o citlivé čalúnenie, neriskujte premočenie. Pri sedačke je niekedy bezpečnejšie použiť profesionálne čistenie než domáce opakované namáčanie.</p>
        <div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 24px 0; background: #f7fbf8;">
        <h2 style="margin-top: 0;">Pri textíliách začnite čistotou, nie vôňou</h2>
        <p>Na tričko alebo obrus je praktický jemný prací základ. Vôňu pridávajte až vtedy, keď je zdroj pachu odstránený.</p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="/p-1551/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l">Pozrieť prací gél</a></p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="/c/vevo-home-care/pranie/praci-gel">Pozrieť pracie gély</a></p>
        </div>
        <h2>Jednoduché pravidlo pri pive</h2>
        <p>Najprv odsajte, potom čistite, až potom sušte. Ak textil alebo sedačku iba prestriekate vôňou, pach sa vráti. Pri pive rozhoduje hlavne rýchlosť, kontrolované množstvo vody a dôkladné vysušenie.</p>
        """
    ),
    "wool_sweater_odor": clean(
        f"""
        <h2>Vlnený sveter netreba prať po každom nosení</h2>
        <p>Vlna sa správa inak než bavlna alebo polyester. Mnohé vlnené svetre stačí po nosení vyvetrať, nechať preschnúť a odložiť až úplne suché. Pranie má zmysel vtedy, keď je sveter spotený, zatuchnutý, znečistený alebo sa pach vracia aj po vetraní. Príliš časté pranie môže zbytočne namáhať vlákna a zvýšiť riziko splstenia.</p>
        <p>Najväčším rizikom pri vlne je kombinácia tepla, trenia a prudkej zmeny podmienok. Preto pri praní vlny nejde o silu, ale o kontrolu: vhodný program, nízke otáčky, správna teplota podľa štítku a sušenie naležato.</p>
        {note_card("Rýchle rozhodnutie pred praním", [
            "<strong>Len ľahký pach po nosení:</strong> najprv vetrať, neukladať vlhké.",
            "<strong>Zatuchnutie zo skrine:</strong> vetrať, skontrolovať skladovanie a až potom prať.",
            "<strong>Pot alebo škvrna:</strong> riešiť lokálne a šetrne podľa štítku.",
            "<strong>Neistý materiál:</strong> radšej ručné pranie alebo čistiareň podľa hodnoty svetra."
        ])}
        <h2>Postup podľa typu pachu</h2>
        {table(["Pach alebo problém", "Prvý krok", "Riziko"], [
            ("Pach po jednom nosení", "vyvetrať na vzduchu", "zbytočné pranie skracuje životnosť"),
            ("Zatuchnutý sveter zo skrine", "vetrať a skontrolovať vlhkosť v skrini", "pranie nevyrieši vlhké skladovanie"),
            ("Pot v podpazuší", "jemné lokálne ošetrenie a šetrné pranie", "trením sa vlna môže poškodiť"),
            ("Mokrý sveter po daždi", "najprv vysušiť naplocho", "odloženie vlhkého svetra zhorší pach"),
            ("Starší citlivý sveter", "voliť najšetrnejší postup", "riziko splstenia a deformácie"),
        ])}
        <h2>Ako prať vlnený sveter v práčke</h2>
        <p>Ak štítok povoľuje pranie v práčke, použite program na vlnu alebo jemný program, nízku teplotu a nízke otáčky. Sveter perte samostatne alebo s podobne jemnými kusmi, nie s rifľami, uterákmi alebo zipsami. Nepoužívajte veľa pracieho prostriedku a nepoužívajte postup, ktorý má iba prekryť pach parfumáciou.</p>
        <p>Po praní sveter nekrúťte. Jemne ho vytlačte do uteráka a sušte naležato. Vešanie mokrého svetra môže vytiahnuť ramená a dĺžku. Radiátor alebo horúci vzduch môže zmeniť tvar a pocit z vlákna.</p>
        <h2>Ako zabrániť návratu pachu</h2>
        <p>Vlnený sveter nikdy neodkladajte vlhký. Po nosení mu dajte čas preschnúť mimo skrine. Ak ho nosíte na tričko alebo tenkú spodnú vrstvu, pot sa dostane do svetra menej a pranie nebude potrebné tak často. Pri sezónnom skladovaní musí byť sveter čistý a suchý.</p>
        <p>Ak sa pach vracia po každom nosení, skontrolujte aj šatník. Vlhká skriňa, preplnené police alebo zatuchnuté sezónne oblečenie môžu preniesť pach späť aj na dobre vypraný sveter.</p>
        <div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 24px 0; background: #f7fbf8;">
        <h2 style="margin-top: 0;">Pri jemných materiáloch najprv riešte šetrnosť</h2>
        <p>Pri vlne sledujte štítok. Ak používate prací produkt na bežné jemné pranie, dávkujte opatrne a nepoužívajte ho ako náhradu vhodného programu.</p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="/p-1551/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l">Pozrieť prací gél</a></p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="/c/vevo-home-care/pranie/praci-gel">Pozrieť pracie gély</a></p>
        </div>
        <h2>Jednoduché pravidlo pre vlnený sveter</h2>
        <p>Najprv vetrať, potom šetrne prať a vždy sušiť naležato. Ak sveter nezapácha výrazne, pranie nie je automaticky lepšie než dobré vetranie. Ak už periete, chráňte tvar, vlákno a sušenie.</p>
        """
    ),
    "perfume_stain": clean(
        f"""
        <h2>Prečo môže parfum zanechať fľak</h2>
        <p>Parfum, parfum do prania alebo vonný olej môže obsahovať vonné zložky, rozpúšťadlá alebo olejové časti, ktoré sa na citlivej látke prejavia ako mapa. Niekedy nejde o klasickú špinu, ale o zmenu povrchu textilu, zvlhčenie vlákna alebo reakciu farby. Preto je pri jemných materiáloch dôležité neaplikovať vôňu priamo na oblečenie bez testu.</p>
        <p>Rizikové sú najmä hodvábne, viskózové, saténové, jemné syntetické a tmavé hladké látky. Fľak môže byť viditeľný až po zaschnutí alebo po vystavení svetlu. Ak ho hneď pretriete, môže sa zväčšiť.</p>
        {note_card("Čo urobiť hneď po vzniku fľaku", [
            "<strong>Netrieť do strán:</strong> fľak sa môže rozšíriť do väčšej mapy.",
            "<strong>Odsávať:</strong> jemne prikladať savú čistú textíliu.",
            "<strong>Testovať:</strong> pri jemnej látke skúsiť postup na skrytom mieste.",
            "<strong>Nesušiť teplom:</strong> pred kontrolou výsledku nepoužiť radiátor ani sušičku."
        ])}
        <h2>Postup podľa typu látky</h2>
        {table(["Materiál", "Prvý krok", "Kedy byť opatrný"], [
            ("Bavlna", "jemne predčistiť a vyprať podľa štítku", "pri farebnej bavlne testovať stálofarebnosť"),
            ("Viskóza", "minimálne trenie a studenší postup", "mokrá viskóza sa ľahko deformuje"),
            ("Hodváb alebo satén", "radšej odborné čistenie alebo veľmi opatrný test", "riziko mapy a straty lesku"),
            ("Tmavé tričko", "odsávať a nepoužiť veľa produktu", "môže zostať svetlá alebo mastná mapa"),
            ("Kabát alebo sako", "nepremáčať podšívku", "pri drahom kúsku zvážiť čistiareň"),
        ])}
        <h2>Ako odstrániť parfumový fľak krok za krokom</h2>
        <p>Najprv zistite, či ide o čerstvý alebo zaschnutý fľak. Pri čerstvom fľaku jemne odsajte prebytok. Pri zaschnutom fľaku nešúchajte suchú látku nasilu. Podľa materiálu skúste lokálne ošetrenie malým množstvom vody alebo vhodného pracieho roztoku, vždy po teste na menej viditeľnom mieste.</p>
        <p>Ak je materiál prateľný, perte podľa štítku a pred sušením skontrolujte výsledok. Ak mapa zostala, opakujte šetrné ošetrenie. Teplo môže niektoré stopy zafixovať alebo zvýrazniť. Pri neprateľných látkach, hodvábe, saku alebo drahých šatách je bezpečnejšia čistiareň.</p>
        <h2>Ako parfumy testovať bez rizika</h2>
        <p>Vôňu netestujte priamo na drahých alebo jemných šatách. Najprv ju skúste mimo textilu, na malej vzorke alebo na menej citlivom materiáli. Pri parfumoch do prania je rozumnejšie testovať nižšiu dávku v bežnej dávke bielizne, nie liať väčšie množstvo kvôli intenzite vône.</p>
        <p>Ak máte viac citlivých materiálov, sledujte, na ktorých sa mapy objavujú. Niektoré látky reagujú na olejové zložky alebo rozpúšťadlá citlivejšie než bavlna. Prevencia je jednoduchšia než odstraňovanie mapy z hotového outfitu.</p>
        <div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 24px 0; background: #f7fbf8;">
        <h2 style="margin-top: 0;">Vône testujte najprv v menšom</h2>
        <p>Vzorky sú praktické vtedy, keď chcete zistiť intenzitu a správanie vône bez veľkého balenia a bez zbytočného rizika pri citlivých textíliách.</p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="/p-1621/vevo-essence-sample-set">Pozrieť vzorkový set</a></p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="/c/vzorky/parfum-do-prania-vzorky">Pozrieť vzorky parfumov do prania</a></p>
        </div>
        <h2>Jednoduché pravidlo pri parfumovom fľaku</h2>
        <p>Najprv odsajte, potom testujte, až potom čistite. Jemnú látku nešúchajte a nesušte teplom, kým neviete, že fľak zmizol. Pri vôňach platí, že menej a otestované je bezpečnejšie než silná aplikácia priamo na citlivý textil.</p>
        """
    ),
}


TOP_UPS = {
    "polyester_cotton": clean(
        f"""
        <h2>Keď sa polyester a bavlna miešajú v jednom kuse</h2>
        <p>Veľa tričiek, mikín a legín nie je čisto bavlnených ani čisto polyesterových. Zmesový materiál môže byť pohodlný, pružnejší a rýchlejšie schnúť, ale pri praní sa správa kompromisne. Ak má textil vyšší podiel polyesteru, pach potu sa môže vracať rýchlejšie. Ak má vyšší podiel bavlny, dlhšie drží vlhkosť a potrebuje lepšie sušenie.</p>
        <p>Pri takomto kúsku sa neoplatí rozhodovať iba podľa názvu materiálu. Pozrite si štítok, zvážte spôsob nosenia a sledujte, čo sa deje po praní. Ak oblečenie vonia čisto po vysušení, ale pri nosení rýchlo zapácha, problém býva skôr v zvyškoch potu, krátkom programe alebo slabom oplachu. Ak je skôr zatuchnuté, hľadajte problém v sušení a skladovaní.</p>
        {note_card("Domáci test po troch praniach", [
            "<strong>Po prvom praní:</strong> skontrolujte, či textil nie je lepkavý alebo príliš parfumovaný.",
            "<strong>Po druhom praní:</strong> znížte náplň bubna a sledujte rozdiel v pachu.",
            "<strong>Po treťom praní:</strong> ak sa pach vracia, riešte program a oplach, nie iba vôňu."
        ])}
        <h2>Najčastejšie chyby pri zmesových materiáloch</h2>
        {table(["Chyba", "Ako sa prejaví", "Lepší postup"], [
            ("Veľa kusov v bubne", "oblečenie sa zle prepláchne", "prať menšiu dávku"),
            ("Krátky program po športe", "pach sa vráti pri nosení", "zvoliť program s lepším oplachom"),
            ("Priveľa vône", "textil je ťažký a pach sa mieša", "najprv odstrániť zdroj pachu"),
            ("Pomalé sušenie", "bavlnená časť zatuchne", "sušiť vo vzdušnom priestore"),
        ])}
        <div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">
        <h2 style="margin-top: 0;">Praktické zhrnutie pre šatník</h2>
        <p>Polyesterové športové veci perte skôr a v menšej dávke. Bavlnené kusy nenechávajte dlho vlhké. Zmesové materiály sledujte podľa toho, či sa problém prejaví skôr pachom pri nosení alebo zatuchnutím po sušení.</p>
        </div>
        """
    ),
    "beer_stain": clean(
        f"""
        <h2>Čo robiť, keď pivo zaschlo cez noc</h2>
        <p>Zaschnuté pivo už nečistíte ako čerstvú tekutinu. V textílii zostali zvyšky, ktoré sa pri kontakte s vlhkosťou môžu znovu rozvoňať. Najprv fľak jemne navlhčite studenšou vodou, aby sa povrch uvoľnil. Potom ho odsávajte čistou handričkou a až následne použite malé množstvo pracieho roztoku. Pri obrusoch a tričkách je cieľom dostať zvyšky von z vlákna, nie ich rozotrieť do väčšej plochy.</p>
        <p>Ak ide o sedačku, postupujte pomalšie. Zaschnutý okraj fľaku sa môže po premočení rozšíriť a vznikne mapa. Preto čistite od okrajov smerom do stredu, s minimom vody a s priebežným odsávaním. Miesto po čistení sušte prúdením vzduchu. Kým je vlhké, nepoužívajte výraznú vôňu, lebo iba skomplikuje kontrolu, či pach naozaj zmizol.</p>
        {note_card("Postup pri zaschnutom pive", [
            "<strong>Navlhčiť kontrolovane:</strong> nepremáčať celý kus, iba postihnuté miesto.",
            "<strong>Odsávať, nie trieť:</strong> tlak pomáha viac než šúchanie.",
            "<strong>Prať až po predčistení:</strong> samotný program nemusí zaschnuté zvyšky uvoľniť.",
            "<strong>Sušiť až po kontrole:</strong> teplo nepoužiť, kým fľak a pach nezmiznú."
        ])}
        <h2>Prečo sa pivo môže objaviť ako mapa</h2>
        <p>Mapa vzniká vtedy, keď sa tekutina rozleje do strán a pri schnutí nechá zvyšky na okraji. Na hladkom obruse je to viditeľné rýchlo, na sedačke až po vysušení. Pri tmavšom textile môže byť fľak nenápadný, ale pach zostane. Preto je dôležité pracovať s malým množstvom vody a postup opakovať radšej viackrát než raz premočiť veľkú plochu.</p>
        {table(["Povrch", "Riziko mapy", "Praktická kontrola"], [
            ("Biele tričko", "žltkastý okraj po zaschnutí", "pozrieť proti svetlu pred sušením"),
            ("Farebný obrus", "rozpitý okraj po premočení", "testovať stálosť farby"),
            ("Látková sedačka", "pach z výplne", "kontrolovať aj po úplnom vyschnutí"),
            ("Koberec", "vlhkosť v spodnej vrstve", "odsávať a sušiť dlhšie"),
            ("Jemná látka", "zmena štruktúry", "voliť veľmi mierny postup"),
        ])}
        <div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">
        <h2 style="margin-top: 0;">Kedy postup zopakovať</h2>
        <p>Ak po prvom praní ostal na tričku slabý pach, neznamená to automaticky, že treba viac prostriedku. Skôr zopakujte lokálne predčistenie, perte menšiu dávku a pridajte dôkladnejší oplach. Pri sedačke pokračujte až po úplnom vyschnutí, aby ste vedeli rozlíšiť zvyškovú vlhkosť od skutočného zápachu.</p>
        </div>
        <h2>Ako zabrániť tomu, aby obrus po oslave zatuchol</h2>
        <p>Po oslave nedávajte mokrý obrus rovno do koša s ostatnou bielizňou. Pivo, víno, jedlo a mastnota sa v uzavretom koši rýchlo zmiešajú a pach sa prenesie aj na suché veci. Ak nemôžete prať hneď, aspoň odsajte mokré miesta, nechajte obrus rozložený preschnúť a až potom ho pripravte na pranie. Pri veľkých obrusoch perte radšej samostatne, aby mali v bubne priestor.</p>
        <p>Pri tričkách platí podobné pravidlo. Ak je kus mokrý od piva a potu, nenechávajte ho zrolovaný v taške. Vlhkosť je hlavný dôvod, prečo sa zápach z jednoduchého fľaku zmení na problém celej dávky bielizne.</p>
        <div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 24px 0; background: #f7fbf8;">
        <h2 style="margin-top: 0;">Domáca kontrola pred odložením</h2>
        <p>Textil odložte až vtedy, keď je suchý a bez pachu. Ak miesto cítiť len pri priblížení nosa alebo pri miernom navlhčení, zvyšky ešte ostali vo vlákne a je lepšie ich riešiť hneď.</p>
        </div>
        """
    ),
    "wool_sweater_odor": clean(
        f"""
        <h2>Keď vlnený sveter páchne aj po vyvetraní</h2>
        <p>Ak sveter zapácha aj po niekoľkých hodinách vetrania, pravdepodobne nejde iba o bežný pach z nosenia. Môže byť nasiaknutý potom, dymom, kuchynskou arómou alebo zatuchnutím zo skrine. Vtedy má zmysel šetrné pranie alebo odborné čistenie podľa hodnoty a zloženia svetra. Stále však platí, že vlna neznáša unáhlené silové riešenia.</p>
        <p>Pred praním skontrolujte štítok, švy, žmolky a farbu. Starší sveter môže byť oslabený v lakťoch alebo pri manžetách. Ak sa ho pokúsite intenzívne drhnúť, poškodíte presne miesta, ktoré sú už namáhané. Pri zápachu je lepšie pracovať s časom namočenia, jemným pohybom a dôkladným sušením než s trením.</p>
        {note_card("Kontrola pred praním vlny", [
            "<strong>Štítok:</strong> rozhoduje, či je možná práčka, ručné pranie alebo čistiareň.",
            "<strong>Farba:</strong> tmavé a sýte farby najprv testujte na skrytom mieste.",
            "<strong>Tvar:</strong> zmerajte dĺžku a rukávy, aby ste po sušení videli zmenu.",
            "<strong>Zápach:</strong> odlíšte pot, zatuchnutie a pach zo skladovania."
        ])}
        <h2>Ručné pranie vlneného svetra bez splstenia</h2>
        <p>Pri ručnom praní nepoužívajte horúcu vodu a nemeňte prudko teplotu. Sveter ponorte, nechajte jemne nasiaknuť a hýbte ním opatrne. Nešúchajte rukáv o rukáv a nekrúťte ho pri vyberaní z vody. Po oplachu ho položte na uterák, zrolujte a vytlačte prebytočnú vodu tlakom.</p>
        <p>Sušenie je rovnako dôležité ako pranie. Sveter vytvarujte na uteráku do pôvodného tvaru, zarovnajte ramená a rukávy. Počas sušenia ho podľa potreby otočte alebo vymeňte mokrý uterák. Ak ho zavesíte mokrý, vlastná váha vody môže vytiahnuť pleteninu.</p>
        {table(["Krok", "Správne", "Nesprávne"], [
            ("Namáčanie", "krátko a v stabilnej teplote", "horúca voda a dlhé máčanie"),
            ("Pohyb", "jemné stláčanie", "drhnutie a krútenie"),
            ("Oplach", "postupný a šetrný", "prudké zmeny teploty"),
            ("Odvodnenie", "tlak do uteráka", "žmýkanie rukami"),
            ("Sušenie", "naležato vo vzduchu", "radiátor alebo vešiak"),
        ])}
        <div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">
        <h2 style="margin-top: 0;">Ako riešiť pach zo skrine</h2>
        <p>Ak sveter páchne skriňou, samotné pranie nemusí stačiť. Vyvetrajte policu, skontrolujte vlhkosť, neodkladajte do nej sezónne veci natlačené na seba a uistite sa, že všetko skladované oblečenie je úplne suché.</p>
        </div>
        <h2>Kedy vlnený sveter radšej neprať doma</h2>
        <p>Domáce pranie nie je najlepšie riešenie pri drahom kašmíre, saku s vlneným podielom, svetri s podšívkou, ozdobami alebo pri kúsku, ktorý už raz zmenil tvar. Ak neviete zloženie alebo má sveter vysokú hodnotu, čistiareň môže byť lacnejšia než poškodenie. Pri bežnom svetri však často stačí správne vetranie, šetrné pranie a sušenie naležato.</p>
        <p>Pri opakovanom zápachu sledujte aj to, čo nosíte pod svetrom. Tenké bavlnené tričko zachytí časť potu a vlna ostane dlhšie svieža. Znížite tým potrebu prania a predĺžite životnosť svetra.</p>
        <div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 24px 0; background: #f7fbf8;">
        <h2 style="margin-top: 0;">Praktická sezónna rutina</h2>
        <p>Na konci sezóny odkladajte iba čisté, suché a vyvetrané svetre. Ak ich skladujete v uzavretom priestore, nechajte medzi nimi priestor. Vlna potrebuje suché prostredie a jemné zaobchádzanie, nie časté silné pranie.</p>
        </div>
        """
    ),
    "perfume_stain": clean(
        f"""
        <h2>Rozdiel medzi vôňou na telo a vôňou na textil</h2>
        <p>Parfum na telo je navrhnutý na pokožku, nie automaticky na každý textil. Na látke sa môže správať inak: zanechať mokrú mapu, olejový tieň alebo zmenu lesku. Parfum do prania sa zase používa cez prací proces a riedi sa vo vode podľa dávkovania. Ani pri ňom však nie je rozumné liať koncentrovaný produkt priamo na jemnú látku.</p>
        <p>Ak chcete oblečenie príjemne prevoňať, bezpečnejšia cesta je správne pranie a primerané dávkovanie. Pri hotovom outfite je lepšie aplikovať vôňu tak, aby nedopadala priamo na citlivý textil. Pri šatách, blúzkach, šatkách a saténových povrchoch sa oplatí testovať ešte opatrnejšie.</p>
        {note_card("Kde parfum najčastejšie robí mapy", [
            "<strong>Golier a výstrih:</strong> kontakt s pokožkou, potom a kozmetikou.",
            "<strong>Saténové šaty:</strong> lesklý povrch ukáže aj malú zmenu.",
            "<strong>Tmavá viskóza:</strong> mapa môže byť viditeľná až po zaschnutí.",
            "<strong>Kabát alebo sako:</strong> problémom môže byť aj podšívka a nemožnosť prania doma."
        ])}
        <h2>Čo nerobiť pri parfumovom fľaku</h2>
        <p>Najhorší postup je fľak ihneď silno pretrieť vodou a potom ho vysušiť teplom. Mapa sa môže zväčšiť a pri citlivých vláknach sa zmení povrch. Rovnako nepomáha pridať ďalšiu vôňu. Fľak tým nezmizne, len sa ťažšie posúdi, či v látke zostal olejový alebo parfumový zvyšok.</p>
        <p>Pri drahšom kúsku si zapamätajte presný produkt, ktorý fľak spôsobil. Ak pôjdete do čistiarne, informácia o type látky a pôvode škvrny pomôže zvoliť opatrnejší postup.</p>
        {table(["Chyba", "Možný následok", "Lepšia voľba"], [
            ("Silné trenie", "väčšia mapa a poškodený povrch", "odsávať prikladaním"),
            ("Teplé sušenie", "zafixovanie stopy", "sušiť voľne po kontrole"),
            ("Veľa vody na viskózu", "deformácia látky", "test a minimálna vlhkosť"),
            ("Ďalšia vôňa", "prekrytie problému", "najprv čistiť zdroj"),
            ("Pranie bez kontroly štítku", "zmena tvaru alebo farby", "riadiť sa materiálom"),
        ])}
        <div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">
        <h2 style="margin-top: 0;">Bezpečnejší spôsob používania vôní</h2>
        <p>Pri jemnom oblečení aplikujte vôňu s odstupom a mimo najcitlivejších látok. Pri praní začnite nižšou dávkou vône a sledujte výsledok po vysušení. Intenzitu zvyšujte až vtedy, keď viete, že textil nereaguje mapou ani ťažkým pocitom.</p>
        </div>
        <h2>Ako skontrolovať výsledok po vyčistení</h2>
        <p>Po vyčistení nehodnoťte látku len mokrú. Niektoré mapy sa ukážu až po úplnom vyschnutí, iné sú viditeľné iba pri bočnom svetle. Položte kus na rovný povrch, pozrite sa naň z viacerých uhlov a skontrolujte aj dotyk. Ak je miesto mastnejšie, tvrdšie alebo inak lesklé, zvyšok ešte nemusí byť preč.</p>
        <p>Pri bežnej bavlne sa dá postup zopakovať. Pri hodvábe, saténe, viskóze alebo saku je opakovanie doma rizikovejšie. Vtedy je dôležité nepoškodiť látku ďalšími pokusmi.</p>
        <div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 24px 0; background: #f7fbf8;">
        <h2 style="margin-top: 0;">Pravidlo pre jemné látky</h2>
        <p>Ak si nie ste istí, či je látka prateľná a stálofarebná, nerobte veľký zásah na viditeľnom mieste. Test na skrytej časti je pri parfumových fľakoch praktickejší než rýchle pranie celého kúsku.</p>
        </div>
        """
    ),
}


DEEPENERS = {
    "beer_stain": clean(
        """
        <h2>Prečo po pive nestačí len bežné vypranie</h2>
        <p>Pri pive býva problém v tom, že škvrna môže byť po bežnom praní opticky slabšia, ale zvyšky vo vlákne zostanú. Pri ďalšom kontakte s vlhkosťou sa pach znovu objaví. Typické je to pri tričkách z večera, obrusoch po oslave alebo textíliách, ktoré sa pred praním nechali dlhšie v koši. Ak sa k pivu pridá pot, jedlo alebo mastnota, výsledok je ešte horší, pretože každý typ nečistoty potrebuje trochu inú časť postupu.</p>
        <p>Preto je pri domácich textíliách lepšie rozdeliť prácu na dve fázy. Prvá je odstránenie tekutiny a uvoľnenie zvyškov z konkrétneho miesta. Druhá je pranie celej veci s dostatočným pohybom a oplachom. Ak preskočíte prvú fázu, práčka musí riešiť zaschnutý lokálny problém v celej dávke bielizne, čo často nestačí.</p>
        <div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">
        <h2 style="margin-top: 0;">Kontrola pred sušením</h2>
        <p>Po praní miesto ovoňajte ešte vlhké aj po čiastočnom preschnutí. Ak je cítiť kyslastý alebo kvasný tón, textil nesušte teplom. Zopakujte lokálne ošetrenie a až potom ho nechajte úplne vyschnúť.</p>
        </div>
        <h2>Ako postupovať pri kombinovaných škvrnách z oslavy</h2>
        <p>Obrus po oslave málokedy obsahuje iba pivo. Často je tam aj víno, omáčka, olej alebo sladké nápoje. V takom prípade najprv riešte najmokrejšie miesta, potom mastné stopy a až nakoniec celkové pranie. Ak dáte celý obrus do práčky bez predbežnej kontroly, niektoré fľaky sa môžu po vysušení ukázať výraznejšie než pred praním.</p>
        <p>Pri sedačke je kombinovaná škvrna ešte citlivejšia. Ak sa pivo zmiešalo s jedlom, nepoužívajte veľa vody naraz. Zvyšky by ste zatlačili hlbšie do výplne. Pracujte po menších úsekoch, priebežne odsávajte vlhkosť a po čistení nechajte miesto vetrať dlhšie, než sa zdá potrebné na dotyk.</p>
        """
    ),
    "wool_sweater_odor": clean(
        """
        <h2>Prečo vlna niekedy vonia inak po daždi alebo pare</h2>
        <p>Vlnené vlákno reaguje na vlhkosť výraznejšie než mnohé bežné materiály. Sveter, ktorý v suchu pôsobí v poriadku, môže po daždi, pare z kuchyne alebo po nosení pod kabátom znovu ukázať pach. Neznamená to vždy, že je špinavý. Niekedy sa iba aktivuje pach zo skladovania alebo zvyšky potu, ktoré sa pri suchom stave neprejavovali.</p>
        <p>Ak sa to deje opakovane, pomôže viesť si jednoduchú kontrolu: kde bol sveter uložený, či bol pred uložením úplne suchý, čo ste mali pod ním a či sa pach objavuje v rovnakej časti. Pach pri golieri a podpazuší súvisí skôr s nosením. Pach celého svetra skôr so skladovaním, vlhkosťou alebo vzduchom v skrini.</p>
        <div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">
        <h2 style="margin-top: 0;">Malý test pred praním</h2>
        <p>Sveter vyvetrajte mimo skrine, nechajte ho úplne suchý a potom skontrolujte pach na golieri, v podpazuší a v strede chrbta. Ak zapácha iba lokálne, riešte lokálne nosenie. Ak zapácha celý, skontrolujte aj skriňu.</p>
        </div>
        <h2>Ako skladovať vlnený sveter medzi noseniami</h2>
        <p>Vlnený sveter po nosení nevracajte hneď medzi ostatné oblečenie. Nechajte ho aspoň krátko preschnúť a vydýchať, najmä ak ste ho mali celý deň pod kabátom. V skrini ho neskladujte natlačený medzi vlhkými alebo čerstvo vypranými vecami. Vlhkosť a slabé prúdenie vzduchu sú častým dôvodom, prečo sa pach vracia aj po šetrnom praní.</p>
        <p>Pri sezónnom odkladaní je dôležité, aby bol sveter čistý, suchý a voľne uložený. Ak ho zložíte ešte mierne vlhký, zatuchnutie sa môže prejaviť až o týždne neskôr. Potom vyzerá problém ako chyba prania, hoci vznikol pri skladovaní.</p>
        """
    ),
    "perfume_stain": clean(
        """
        <h2>Prečo sa parfumový fľak niekedy ukáže až neskôr</h2>
        <p>Niektoré parfumové stopy nie sú viditeľné okamžite. Látka môže najprv vyzerať iba mokrá a po vyschnutí sa objaví mastnejší okraj, svetlejšia mapa alebo zmena lesku. Pri jemných materiáloch sa to stáva najmä tam, kde je povrch hladký a odráža svetlo. Preto je dôležité nehodnotiť výsledok iba v kúpeľni pod jedným svetlom.</p>
        <p>Ak ste fľak čistili, nechajte kus vyschnúť prirodzene a skontrolujte ho pri dennom svetle aj pod uhlom. Prstami jemne porovnajte miesto so zvyškom látky. Ak je tvrdšie, mastnejšie alebo inak hladké, môže v ňom zostať zvyšok vonnej alebo olejovej zložky. Ďalší zásah robte až po tejto kontrole, nie automaticky hneď po prvom mokrom ošetrení.</p>
        <div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">
        <h2 style="margin-top: 0;">Kontrola pri spoločenskom oblečení</h2>
        <p>Pri šatách, blúzke, saku alebo saténe porovnajte fľak s rovnakou časťou na druhej strane oblečenia. Ak sa líši lesk, tvar alebo dotyk, nepokračujte silným domácim čistením bez testu.</p>
        </div>
        <h2>Ako predchádzať fľakom pri každodennom používaní vôní</h2>
        <p>Pri obliekaní nechajte parfum na pokožke najprv zaschnúť a až potom si oblečte citlivý kus. Ak používate vôňu pri vlasoch alebo krku, myslite na golier, šál a vrchné diely. Najviac rizikové sú hladké tmavé látky, lesklé materiály a kúsky, ktoré sa nedajú jednoducho vyprať.</p>
        <p>Pri vôňach do prania je prevencia podobná: držať sa primeraného dávkovania a sledovať výsledok na bežnej dávke bielizne. Ak chcete skúšať novú vôňu, nezačínajte na najcitlivejších šatách alebo obľúbenej blúzke. Najprv zistite, ako sa vôňa správa na bežných textíliách po praní aj po úplnom vysušení.</p>
        """
    ),
}


FINAL_TOUCHUPS = {
    "wool_sweater_odor": clean(
        """
        <h2>Malé pravidlo pre opakované nosenie</h2>
        <p>Ak sveter po jednom dni nie je spotený ani znečistený, nechajte ho oddýchnuť mimo skrine a noste ho znova až po vyvetraní. Vlna často potrebuje menej prania, ale viac priestoru, sucha a trpezlivého skladovania medzi noseniami.</p>
        """
    ),
    "perfume_stain": clean(
        """
        <h2>Malé pravidlo pred odchodom z domu</h2>
        <p>Pri citlivom oblečení naneste vôňu skôr, nechajte ju uschnúť na pokožke a až potom si oblečte vrchnú vrstvu. Tento jednoduchý návyk výrazne znižuje riziko máp na golieri, šatke, saku alebo jemnej blúzke.</p>
        """
    ),
}


MARKERS = {
    "gel_science": "Ako prací gél reálne pracuje vo vode",
    "polyester_cotton": "Polyester a bavlna sa pri pote správajú rozdielne",
    "beer_stain": "Prečo pivo zanechá pach aj po vysušení",
    "wool_sweater_odor": "Vlnený sveter netreba prať po každom nosení",
    "perfume_stain": "Prečo môže parfum zanechať fľak",
}


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
    updated = long
    if MARKERS[key] not in updated:
        index = insertion_index(updated)
        updated = updated[:index].rstrip() + "\n" + EXPANSIONS[key] + "\n" + updated[index:].lstrip()
    top_up = TOP_UPS.get(key)
    if top_up:
        top_marker = re.search(r"<h2>(.*?)</h2>", top_up).group(1)
        if top_marker not in updated:
            index = insertion_index(updated)
            updated = updated[:index].rstrip() + "\n" + top_up + "\n" + updated[index:].lstrip()
    deepener = DEEPENERS.get(key)
    if deepener:
        deepener_marker = re.search(r"<h2>(.*?)</h2>", deepener).group(1)
        if deepener_marker not in updated:
            index = insertion_index(updated)
            updated = updated[:index].rstrip() + "\n" + deepener + "\n" + updated[index:].lstrip()
    final_touchup = FINAL_TOUCHUPS.get(key)
    if final_touchup:
        final_marker = re.search(r"<h2>(.*?)</h2>", final_touchup).group(1)
        if final_marker not in updated:
            index = insertion_index(updated)
            updated = updated[:index].rstrip() + "\n" + final_touchup + "\n" + updated[index:].lstrip()
    return updated


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
    parser = argparse.ArgumentParser(description="Conservatively expand VEVO retrofit wave 06 with five articles.")
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
            if article.get("title") != config["title"]:
                raise SystemExit(f"Title changed unexpectedly for {config['slug']}: {article.get('title')}")
            original_long = article["long"]
            original_short = article.get("short", "")
            original_url = article.get("url")
            article["long"] = insert_expansion(article["long"], config["expansion"])
            if article.get("title") != config["title"] or article_slug(article) != config["slug"] or article.get("short", "") != original_short:
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
                "wave": "retrofit-wave-06-mixed-five",
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
                "wave": "retrofit-wave-06-mixed-five",
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
