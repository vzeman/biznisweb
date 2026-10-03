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
OUT_JSON = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-01-washer-care-2026-06-16.json"
MCP_RESULTS = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-01-washer-care-2026-06-16-mcp-results.json"


ARTICLES = [
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-15-2026-06-09-articles.json",
        "slug": "ako-vycistit-zasobnik-pracky-od-usadenin-pracieho-gelu-a-avivaze",
        "post_id": "2142",
        "url": "https://www.vevo.sk/n/ako-vycistit-zasobnik-pracky-od-usadenin-pracieho-gelu-a-avivaze",
        "expansion": "drawer",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-18-2026-06-10-articles.json",
        "slug": "ako-vycistit-tesnenie-pracky-po-prani-pelechu-plneho-chlpov",
        "post_id": "2202",
        "url": "https://www.vevo.sk/n/ako-vycistit-tesnenie-pracky-po-prani-pelechu-plneho-chlpov",
        "expansion": "seal",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-18-2026-06-10-articles.json",
        "slug": "ako-vycistit-bubon-pracky-po-prani-pelechu-topanok-alebo-pracovnych-veci",
        "post_id": "2201",
        "url": "https://www.vevo.sk/n/ako-vycistit-bubon-pracky-po-prani-pelechu-topanok-alebo-pracovnych-veci",
        "expansion": "drum",
    },
]


def clean_html(markup):
    return textwrap.dedent(markup).strip()


def table(headers, rows):
    head = "".join(
        f'<th style="border: 1px solid #e5e5e5; padding: 10px; text-align: left;">{header}</th>'
        for header in headers
    )
    body = "\n".join(
        "<tr>"
        + "".join(f'<td style="border: 1px solid #e5e5e5; padding: 10px;">{cell}</td>' for cell in row)
        + "</tr>"
        for row in rows
    )
    return (
        '<table style="width: 100%; border-collapse: collapse; margin: 20px 0;">\n'
        f"<thead><tr>{head}</tr></thead>\n<tbody>\n{body}\n</tbody>\n</table>"
    )


def quick_card(title, bullets):
    items = "".join(f"<li>{item}</li>" for item in bullets)
    return clean_html(
        f"""
        <div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">
        <h2 style="margin-top: 0;">{title}</h2>
        <ul>{items}</ul>
        </div>
        """
    )


def product_and_category_block(context):
    return clean_html(
        f"""
        <div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 24px 0; background: #f7fbf8;">
        <h2 style="margin-top: 0;">Odporúčané riešenie podľa príčiny</h2>
        <p>{context}</p>
        <div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff; margin: 14px 0;">
        <h3 style="margin-top: 0;">Vevo Shot koncentrát na čistenie práčky</h3>
        <p><strong>Kedy dáva zmysel:</strong> keď riešite zápach práčky, povlak v zásobníku, nečistoty po náročnej dávke alebo preventívnu údržbu práčky.</p>
        <p><strong>Kedy najprv riešiť mechanické čistenie:</strong> ak vidíte chlpy, piesok, hrubé usadeniny alebo kúsky textilu. Tie najprv vyberte ručne, až potom spúšťajte čistiaci cyklus.</p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="/p-1549/vevo-shot-koncentrat-na-cistenie-pracky">Pozrieť produkt</a></p>
        </div>
        </div>
        <div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 24px 0; background: #fffaf5;">
        <h2 style="margin-top: 0;">Vyberte údržbu práčky podľa problému</h2>
        <ul><li><strong>Zápach z práčky:</strong> skontrolujte tesnenie, filter, zásobník a potom použite čistenie práčky.</li><li><strong>Usadeniny z gélu a aviváže:</strong> znížte dávkovanie, vyčistite zásobník a nechajte diely preschnúť.</li><li><strong>Chlpy, piesok a hrubé zvyšky:</strong> najprv ich odstráňte ručne, aby sa neposunuli do filtra.</li></ul>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="/c/vevo-home-care/pranie/detox-pracky">Pozrieť kategóriu detox práčky</a></p>
        </div>
        """
    )


EXPANSIONS = {
    "drawer": clean_html(
        f"""
        <h2>Detailnejšia diagnostika zásobníka práčky</h2>
        <p>Zásobník práčky je malé miesto, ale pri bežnom praní cez neho prechádza prací gél, prášok, aviváž, voda aj časť nečistôt z okolitého prostredia. Ak sa nečistí pravidelne, zvyšky sa v ňom nevysušia rovnomerne a vytvoria povlak. Ten môže byť biely, mazľavý, sivý alebo miestami tmavý. Nie vždy ide hneď o pleseň, ale vždy ide o signál, že zásobník potrebuje vybrať, vyčistiť a nechať preschnúť.</p>
        <p>Najčastejšia chyba je riešiť zápach iba silnejšou vôňou do prania. Ak je zdroj v zásobníku, vôňa sa mieša so starými zvyškami prostriedkov a výsledok môže byť ešte ťažší. Preto je lepšie najprv odstrániť nános, skontrolovať priestor za zásuvkou a až potom upraviť dávkovanie pracieho prostriedku alebo aviváže.</p>
        {quick_card("Rýchla diagnostika zásobníka", [
            "<strong>Biely suchý povlak:</strong> často zvyšky prášku, minerály z vody alebo zaschnutý prací prostriedok.",
            "<strong>Lepkavý film:</strong> typicky priveľa gélu alebo aviváže, prípadne slabé vyplachovanie priehradky.",
            "<strong>Čierne bodky:</strong> riziko plesne alebo dlhodobej vlhkosti, skontrolujte aj priestor za zásobníkom.",
            "<strong>Zápach po otvorení zásuvky:</strong> väčšinou kombinácia vlhkosti, nánosov a zavretého priestoru."
        ])}
        <h2>Čo znamenajú jednotlivé typy nánosov</h2>
        {table(["Príznak", "Pravdepodobná príčina", "Najbezpečnejší prvý krok"], [
            ("Biely práškový povlak", "zaschnutý prací prostriedok alebo tvrdšia voda", "namočiť zásobník do teplej vody a vyčistiť mäkkou kefkou"),
            ("Lepkavá priehradka na aviváž", "priveľa aviváže alebo hustý zvyšok v úzkom kanáliku", "prepláchnuť teplou vodou a znížiť dávku pri ďalšom praní"),
            ("Tmavé bodky v rohoch", "vlhkosť a slabé presychanie", "vybrať zásobník, vyčistiť rohy, utrieť aj priestor v práčke"),
            ("Zápach po praní", "zvyšky v zásobníku, tesnení alebo filtri", "čistiť zásobník spolu s kontrolou tesnenia a filtra"),
        ])}
        <h2>Rozšírený postup bez zbytočného poškodenia plastu</h2>
        <p>Pri čistení zásobníka sa neoplatí používať ostrý nôž, drôtenku ani agresívne škrabanie. Plast sa môže poškrabať a v ryhách sa potom budú zvyšky zachytávať rýchlejšie. Lepší postup je nechať nános zmäknúť. Ak je povlak starší, pomôže dlhšie namočenie, opakované prepláchnutie a mäkká kefka na zuby, ktorou sa dostanete do rohov a kanálikov.</p>
        <p>Po vyčistení zásobníka nezabudnite na samotný otvor v práčke. Práve tam často ostáva vlhký film, ktorý človek nevidí, kým zásuvku úplne nevyberie. Priestor utrite vlhkou handričkou, potom suchou handrou a nechajte ho vetrať. Ak zásobník zasuniete späť mokrý a hneď zatvoríte dvierka práčky, problém sa môže rýchlo vrátiť.</p>
        <h2>Ako často čistiť zásobník podľa používania</h2>
        {table(["Domácnosť", "Odporúčaná frekvencia", "Prečo"], [
            ("Pranie 1-2x týždenne", "približne raz za 4 až 6 týždňov", "nánosy vznikajú pomalšie, ale vlhkosť stále ostáva"),
            ("Každodenné pranie", "raz za 2 až 4 týždne", "zásobník má málo času úplne preschnúť"),
            ("Časté používanie aviváže", "aspoň raz mesačne", "avivážový film sa drží v úzkych kanálikoch"),
            ("Zápach alebo viditeľné bodky", "hneď, nie až podľa kalendára", "ide už o prejav zanesenia alebo vlhkosti"),
        ])}
        <h2>Kedy nestačí vyčistiť iba zásobník</h2>
        <p>Ak bielizeň zapácha aj po vyčistení zásobníka, príčina môže byť v tesnení, filtri, bubne alebo v tom, že sa práčka po praní zatvára príliš skoro. Zásobník je dobrý začiatok, ale práčka funguje ako celý systém. Voda, zvyšky prostriedkov a vlákna sa môžu držať na viacerých miestach naraz.</p>
        <p>Ak sa zápach vracia rýchlo, skontrolujte dávkovanie. Príliš veľa pracieho gélu alebo aviváže nezlepší čistotu prania, ale môže zvyšovať množstvo zvyškov v zásobníku. Pri hustých produktoch je dôležité rešpektovať dávkovanie, tvrdosť vody a veľkosť náplne. Pri menšej dávke bielizne často stačí menej produktu, než človek od oka naleje.</p>
        <h2>Prevencia: ako udržať zásobník čistý dlhšie</h2>
        <ul><li>Po praní nechajte zásobník pootvorený, aby preschol.</li><li>Neprelievajte aviváž nad značku maxima.</li><li>Pri pracom géli sledujte dávkovanie podľa tvrdosti vody a veľkosti náplne.</li><li>Raz za čas zásobník úplne vyberte, nie iba utrite spredu.</li><li>Ak riešite zápach práčky, čistite naraz zásobník, tesnenie aj filter.</li></ul>
        """
    ),
    "seal": clean_html(
        f"""
        <h2>Prečo sa chlpy držia práve v tesnení</h2>
        <p>Gumové tesnenie práčky má záhyby, ktoré zachytávajú vodu aj drobné nečistoty. Pri bežnom oblečení si to často nevšimnete, ale po praní pelechu, deky alebo textílií od domácich zvierat sa v tesnení môžu objaviť chlpy, piesok, drobné vlákna a maz. Ak ostanú vlhké, rýchlo začnú zapáchať a pri ďalšom praní sa môžu dostať späť na bielizeň.</p>
        <p>Pri pelechoch je problém aj v tom, že chlpy nie sú jediná nečistota. Textil môže niesť prach, kožný maz, sliny, zvyšky krmiva alebo vonkajšiu špinu. Práve preto nestačí po praní iba zavrieť dvierka. Tesnenie treba prejsť ručne, utrieť a nechať preschnúť.</p>
        {quick_card("Kontrola tesnenia po pelechu", [
            "<strong>Spodný záhyb:</strong> tam sa najčastejšie drží voda, chlpy a drobné zvyšky.",
            "<strong>Bočné časti gumy:</strong> prejdite ich handrou, nie iba pohľadom.",
            "<strong>Dvierka a okraj bubna:</strong> odstráňte mokré vlákna pred ďalšou dávkou.",
            "<strong>Filter:</strong> skontrolujte ho pri veľkom množstve srsti alebo pomalom odtekaní."
        ])}
        <h2>Diagnostická tabuľka po praní pelechu</h2>
        {table(["Čo vidíte alebo cítite", "Čo to môže znamenať", "Čo urobiť najskôr"], [
            ("Chlpy v spodnej časti gumy", "srsť sa zachytila pri odstreďovaní", "vybrať ju vlhkou handrou a tesnenie utrieť dosucha"),
            ("Piesok alebo drobné zrnká", "pelech niesol vonkajšiu špinu", "neoplachovať len do bubna, najprv zvyšky zotrieť"),
            ("Zatuchnutý pach", "vlhké chlpy alebo starší nános v záhybe", "vyčistiť gumu, vetrať, skontrolovať filter"),
            ("Pomalšie odtekanie", "časť srsti alebo zvyškov môže byť vo filtri", "postupovať podľa návodu práčky a filter skontrolovať"),
        ])}
        <h2>Rozšírený bezpečný postup</h2>
        <p>Najprv nechajte bubon prázdny a pripravte si dve handry: jednu vlhkú na vybratie chlpov a jednu suchú na dosušenie. Prejdite tesnenie po celom obvode, najmä spodnú časť. Ak je v gume veľa srsti, handru priebežne oplachujte mimo práčky, aby ste chlpy neposúvali späť do bubna.</p>
        <p>Potom skontrolujte vnútorný okraj dvierok a miesto, kde sa guma stretáva s bubnom. Pri pelechoch sa tam často zachytia jemné vlákna, ktoré pri ďalšom praní skončia na uterákoch alebo čiernom oblečení. Až keď je tesnenie mechanicky čisté, má zmysel riešiť čistiaci program alebo prípravok na údržbu práčky.</p>
        <h2>Kedy spustiť čistiaci cyklus a kedy stačí ručné čistenie</h2>
        {table(["Situácia", "Stačí ručné čistenie?", "Kedy pridať čistenie práčky"], [
            ("Pár chlpov v tesnení", "áno", "ak sa zápach nevracia, ďalší zásah netreba"),
            ("Veľa srsti a mokrý zápach", "čiastočne", "po ručnom odstránení spustite údržbu podľa návodu"),
            ("Pomalé odtekanie", "nie vždy", "skontrolujte filter a až potom čistiaci cyklus"),
            ("Pelech bol veľmi špinavý", "nie ako jediný krok", "vyčistite gumu, bubon, dvierka a nechajte práčku vetrať"),
        ])}
        <h2>Čomu sa pri tesnení radšej vyhnúť</h2>
        <p>Nepoužívajte ostré predmety na vyťahovanie chlpov zo záhybov. Guma sa môže poškodiť a netesnosť je oveľa väčší problém než samotný zápach. Rovnako neprelievajte do tesnenia silné parfumované produkty. Ak je zdrojom zápachu vlhká srsť alebo nános, vôňa ho nevyrieši.</p>
        <p>Opatrní buďte aj s častým praním pelechov spolu s bežným oblečením. Pelechy perte oddelene, pred praním ich vytraste alebo povysávajte a po praní nechajte práčku otvorenú. Ak doma periete zvieracie textílie pravidelne, kontrola tesnenia by mala byť automatická rutina.</p>
        <h2>Prevencia pred ďalším praním pelechu</h2>
        <ul><li>Pelech pred praním vytraste alebo povysávajte.</li><li>Perte ho samostatne, nie s bežnou bielizňou.</li><li>Po praní vyberte chlpy z tesnenia hneď, kým sú mäkké a viditeľné.</li><li>Nechajte dvierka otvorené, aby guma preschla.</li><li>Pri opakovanom zápachu skontrolujte aj filter a zásobník.</li></ul>
        """
    ),
    "drum": clean_html(
        f"""
        <h2>Prečo práčka potrebuje kontrolu po náročnej dávke</h2>
        <p>Pelech, topánky alebo pracovné oblečenie nie sú pre práčku rovnaká situácia ako bežné tričká. Do bubna prinášajú chlpy, piesok, prach, zeminu, zvyšky trávy, pot, mastnotu alebo drobné kamienky. Časť nečistôt odíde s vodou, ale časť môže zostať v bubne, tesnení, pri dvierkach alebo vo filtri.</p>
        <p>Ak po takejto dávke hneď vyperiete biele obliečky alebo jemné oblečenie, riskujete prenos chlpov, šmúh alebo zápachu. Preto je dobré po náročnej dávke urobiť krátku kontrolu práčky. Nie je to zbytočná opatrnosť, ale ochrana ďalšieho prania.</p>
        {quick_card("Kontrola po pelechu, topánkach alebo pracovných veciach", [
            "<strong>Bubon:</strong> pozrite, či v ňom nie je piesok, chlpy alebo kúsky textilu.",
            "<strong>Tesnenie:</strong> utrite spodný záhyb, kde ostáva voda a nečistoty.",
            "<strong>Filter:</strong> kontrolujte najmä pri veľa srsti, piesku alebo pomalom odtekaní.",
            "<strong>Zásobník:</strong> nechajte ho pootvorený, aby po čistení práčka preschla."
        ])}
        <h2>Čo skontrolovať podľa typu prania</h2>
        {table(["Náročná dávka", "Čo môže ostať v práčke", "Prvý krok po praní"], [
            ("Pelech alebo deka od zvieraťa", "chlpy, maz, pach, drobné vlákna", "vybrať chlpy z bubna a tesnenia, nechať vetrať"),
            ("Textilné topánky", "piesok, kamienky, zvyšky blata", "utrieť bubon a skontrolovať spodnú časť tesnenia"),
            ("Pracovné oblečenie", "prach, mastnota, kovový alebo zemný zápach", "neprať hneď jemné veci, najprv skontrolovať bubon"),
            ("Koberčeky a hrubé textílie", "vlákna, žmolky, voda v záhyboch", "vyčistiť tesnenie a nechať dvierka otvorené"),
        ])}
        <h2>Rozšírený postup pred ďalším praním</h2>
        <p>Po skončení programu vyberte prané veci a nechajte bubon prázdny. Rukou alebo handrou prejdite vnútro bubna, potom spodnú časť tesnenia. Ak nájdete piesok alebo chlpy, neoplachujte ich len tak do práčky. Najprv ich zotrite, aby sa neposúvali do filtra alebo späť na ďalšiu bielizeň.</p>
        <p>Ak bol náklad veľmi špinavý, oplatí sa spustiť krátky oplach alebo čistiaci cyklus podľa návodu výrobcu. Pri zápachu však najprv odstráňte viditeľné zvyšky. Čistiaci program má pomôcť s filmom a pachom, ale nemá nahrádzať mechanické vybratie chlpov, piesku alebo kúskov textilu.</p>
        <h2>Kedy stačí kontrola a kedy riešiť údržbu práčky</h2>
        {table(["Stav po praní", "Stačí kontrola?", "Odporúčaný ďalší krok"], [
            ("Bubon je čistý, bez zápachu", "áno", "nechajte dvierka otvorené a pokračujte bežným praním"),
            ("Vidíte chlpy alebo piesok", "nie úplne", "zotrite ich ručne a skontrolujte tesnenie"),
            ("Práčka zapácha", "nie", "skontrolujte tesnenie, filter, zásobník a zvážte čistenie práčky"),
            ("Ďalšia dávka má byť biela alebo jemná", "len po kontrole", "najprv prejdite bubon a gumu handrou"),
        ])}
        <h2>Čomu sa vyhnúť po špinavej dávke</h2>
        <p>Nedávajte hneď po pelechu, topánkach alebo pracovných nohaviciach do práčky biele uteráky, obliečky alebo jemné kúsky bez kontroly bubna. Ak v práčke ostal piesok alebo srsť, ďalšia dávka to zachytí. Rovnako nepoužívajte silnú vôňu ako náhradu za čistenie. Ak je problém v zvyškoch v práčke, vôňa ho iba prekryje.</p>
        <p>Ak periete topánky, vždy rešpektujte štítok a odporúčanie výrobcu. Nie každá obuv patrí do práčky. Pri pracovných veciach zas dávajte pozor na oleje, chemikálie alebo kovové piliny. Niektoré znečistenie je lepšie predčistiť alebo riešiť oddelene, aby ste nepoškodili práčku ani ďalšiu bielizeň.</p>
        <h2>Prevencia pri náročných dávkach</h2>
        <ul><li>Pelechy pred praním vytraste alebo povysávajte.</li><li>Topánky pred praním zbavte blata a piesku.</li><li>Pracovné veci perte oddelene od bežnej bielizne.</li><li>Po náročnej dávke vždy utrite tesnenie a nechajte práčku otvorenú.</li><li>Ak sa zápach vracia, skontrolujte filter, zásobník a pravidelnú údržbu práčky.</li></ul>
        """
    ),
}


EXTRA_DEPTH = {
    "drawer": clean_html(
        """
        <h2>Kontrola po ďalších praniach: ako zistiť, či sa problém vracia</h2>
        <p>Po vyčistení zásobníka si všimnite najbližšie dve až tri prania. Ak sa priehradka znova rýchlo lepí, problém pravdepodobne nie je iba v tom, že zásobník bol dlho nečistený. Často ide o kombináciu dávkovania, hustého produktu, tvrdšej vody a toho, že zásuvka po praní nepreschne. Vtedy má zmysel zmeniť rutinu, nie iba čistiť ten istý nános každý týždeň.</p>
        <p>Praktický test je jednoduchý: po praní vytiahnite zásobník o pár centimetrov a pozrite sa, či v priehradke ostáva stojatá voda alebo viditeľný film. Ak áno, utrite ho a nechajte zásuvku pootvorenú. Pri ďalšom praní použite presnejšiu dávku gélu alebo aviváže. Ak sa stav zlepší, príčina bola pravdepodobne v prebytku produktu alebo v slabom vysychaní.</p>
        <p>Ak sa čierne bodky alebo zápach vrátia veľmi rýchlo, skontrolujte aj okolie zásobníka, tesnenie a filter. Zápach z práčky sa často presúva medzi viacerými miestami: zásobník zapácha, tesnenie drží vlhkosť a filter zachytáva zvyšky. Preto je pri opakovanom probléme lepšie spraviť jednu dôkladnejšiu údržbu celej práčky než stále čistiť iba zásuvku.</p>
        <h2>Rozdiel medzi prevenciou a riešením zápachu</h2>
        <p>Prevencia je jemná a pravidelná: menej prebytočného prostriedku, otvorená zásuvka, preschnutie a občasné vybratie zásobníka. Riešenie zápachu je dôkladnejšie: vyčistiť zásobník, priestor za ním, tesnenie, filter a podľa návodu použiť údržbu práčky. Ak tieto dve veci zamieňate, problém sa bude vracať. Bežné preventívne vetranie nevyrieši starý nános a silný čistiaci cyklus nenahradí každodenné presychanie.</p>
        <p>Pri domácnostiach, kde sa perie často, je dobré spojiť údržbu zásobníka s kontrolou dávkovania. Ak má bielizeň po praní príliš silnú vôňu, je lepkavá alebo uteráky horšie sajú, môže ísť o zvyšky pracieho prostriedku alebo aviváže. Vtedy je čistejší zásobník len časť riešenia. Druhá časť je menšia dávka, nepreplnený bubon a dostatočný oplach.</p>
        """
    ),
    "seal": clean_html(
        """
        <h2>Rutina pre domácnosť so psom alebo mačkou</h2>
        <p>Ak periete pelechy pravidelne, oplatí sa vytvoriť krátku rutinu po každom takomto praní. Nemusí byť zložitá. Stačí vybrať pelech, pozrieť do spodnej časti tesnenia, vlhkou handrou vytiahnuť chlpy, utrieť gumu dosucha a nechať dvierka otvorené. Celé to trvá pár minút, ale výrazne znižuje riziko, že ďalšia dávka bielizne bude zapáchať alebo bude plná srsti.</p>
        <p>Najväčší rozdiel robí príprava pelechu pred praním. Ak ho dáte do práčky plný srsti, trávy a prachu, práčka musí riešiť viac nečistôt, než je potrebné. Pred praním pelech vytraste vonku, povysávajte alebo prejdite valčekom na chlpy. Tým znížite množstvo srsti v bubne, tesnení aj filtri. Prací prostriedok potom rieši textil, nie hromadu mechanických zvyškov.</p>
        <p>Pri veľmi chlpatých pelechoch má zmysel prať ich samostatne a následne skontrolovať filter podľa návodu výrobcu. Filter neotvárajte naslepo počas programu ani hneď po praní, ak neviete, ako je riešený váš model práčky. Pri každom zásahu do filtra postupujte podľa manuálu, pripravte si handru alebo nízku nádobu na vodu a nepoužívajte silu.</p>
        <h2>Kedy riešiť aj pelech, nielen práčku</h2>
        <p>Ak práčka po pelechu zapácha opakovane, nemusí byť problém iba v spotrebiči. Pelech môže byť príliš dlho vlhký, zle vysušený alebo už nasiaknutý pachom, ktorý jedno pranie neodstráni. Vtedy pomáha prať pelech v menšej náplni, dobre ho presušiť a medzi praniami ho vetrať. Ak sa mokrý pelech hneď položí späť na podlahu alebo do uzavretého priestoru, zápach sa môže rýchlo vrátiť.</p>
        <p>Vôňa do prania má pri zvieracích textíliách zmysel len vtedy, keď je textil čistý a suchý. Ak zostali chlpy v tesnení alebo pelech nie je presušený, parfumácia problém nevyrieši. Naopak, môže vytvoriť ťažký mix vône a zatuchnutia. Preto je poradie vždy rovnaké: mechanicky odstrániť srsť, vyprať, skontrolovať práčku, vysušiť a až potom riešiť jemný voňavý dojem.</p>
        """
    ),
    "drum": clean_html(
        """
        <h2>Kontrola pred bielou alebo jemnou dávkou</h2>
        <p>Najväčšie riziko po pelechu, topánkach alebo pracovných veciach vzniká vtedy, keď do práčky hneď vložíte bielu alebo jemnú bielizeň. Biele uteráky zachytia chlpy, obliečky môžu chytiť zatuchnutý pach a jemné textílie môžu prísť do kontaktu s pieskom alebo hrubými zvyškami. Preto sa pred ďalšou dávkou oplatí spraviť rýchlu kontrolu bubna, tesnenia a dvierok.</p>
        <p>Ak plánujete prať biele veci, prejdite bubon bielou alebo svetlou handričkou. Keď na nej ostanú tmavé vlákna, piesok alebo mastný film, práčka ešte nie je pripravená. Najprv utrite bubon a tesnenie, prípadne spustite oplach alebo čistiaci cyklus podľa návodu. Až potom dávajte do práčky biele textílie, ktoré by zachytili zvyšky.</p>
        <p>Pri jemných veciach je dôležitá aj mechanika. Piesok, kamienky alebo tvrdé kúsky nečistôt môžu pri pohybe bubna pôsobiť ako abrazívum. Nemusia hneď zničiť práčku, ale môžu zhoršiť povrch jemného oblečenia. Ak ste prali topánky alebo pracovné veci, venujte tejto kontrole viac pozornosti než po bežnej dávke tričiek.</p>
        <h2>Ako si nastaviť bezpečné poradie prania</h2>
        <p>Praktické poradie je jednoduché: najprv najčistejšie a najjemnejšie dávky, potom bežné oblečenie a až nakoniec náročné veci ako pelechy, koberčeky, topánky alebo pracovné textílie. Ak to nejde a musíte prať náročnú dávku skôr, po nej si nechajte čas na kontrolu práčky. Tak sa znižuje riziko, že sa zvyšky prenesú do drahších alebo svetlejších kusov.</p>
        <p>Pri pravidelnej záťaži, napríklad v domácnosti so psom, v dielni alebo pri športe, je dobré mať údržbu práčky v kalendári. Nemusí ísť vždy o veľké čistenie. Niekedy stačí utrieť tesnenie, vybrať zvyšky z bubna, nechať práčku otvorenú a raz za čas použiť čistenie podľa odporúčania výrobcu. Dôležité je, aby sa z náročného prania nestal zdroj zápachu pre celé ďalšie pranie.</p>
        """
    ),
}


FINAL_DEPTH = {
    "drawer": clean_html(
        """
        <h2>Krátky kontrolný zoznam pred ďalšou dávkou</h2>
        <p>Pred ďalším praním skontrolujte tri veci: či je zásobník suchý, či v priehradke nezostal film a či pri otvorení necítite zatuchnutie. Ak je všetko v poriadku, môžete pokračovať bežným praním. Ak je zásobník znova mokrý alebo lepkavý, nechajte ho pootvorený dlhšie a pri najbližšej dávke znížte množstvo produktu.</p>
        <p>Pri bielej bielizni a uterákoch má čistý zásobník väčší význam, než sa zdá. Zvyšky aviváže alebo gélu sa môžu dostať do oplachu a ovplyvniť pocit z textilu. Ak uteráky horšie sajú alebo bielizeň po praní pôsobí ťažko, skontrolujte nielen program, ale aj to, či zásobník nepúšťa staré nánosy späť do prania.</p>
        <p>Ak chcete mať rutinu jednoduchú, spojte čistenie zásobníka s jedným pravidelným dňom v mesiaci. Vtedy utrite aj dvierka, tesnenie a viditeľný okraj bubna. Práčka tak nebude potrebovať nárazové zásahy až vo chvíli, keď začne zapáchať.</p>
        <p>Pri ďalšom nákupe alebo výbere pracieho produktu sledujte aj to, ako ľahko sa dávkuje. Presnejšie dávkovanie znamená menej zvyškov v zásobníku, menej filmu v práčke a stabilnejší výsledok prania. Čistota práčky teda nezačína až pri čistení, ale už pri tom, koľko produktu do nej pravidelne dávate.</p>
        """
    ),
    "seal": clean_html(
        """
        <h2>Krátky kontrolný zoznam pred ďalšou dávkou</h2>
        <p>Pred ďalším praním prejdite tesnenie suchou bielou handričkou. Ak na nej ostanú chlpy, sivý film alebo vlhký zápach, tesnenie ešte nie je čisté. Pri tmavom oblečení by sa chlpy ukázali okamžite, pri uterákoch by sa mohli zachytiť v slučkách a pri posteľnej bielizni by sa preniesol pach.</p>
        <p>Ak doma periete pelechy často, oddeľte zvieracie textílie od detskej bielizne, uterákov a obliečok. Nie je to otázka prehnanej opatrnosti, ale praktickej hygieny a pohodlia. Pelech môže byť čistý po praní, no práčka po ňom ešte potrebuje minútu kontroly.</p>
        <p>Keď sa zápach objavuje aj po čistení tesnenia, sledujte sušenie pelechu. Nedostatočne presušený textil sa môže stať zdrojom pachu znova a práčka potom vyzerá ako vinník, hoci problém vzniká až po praní. Suchý pelech a suché tesnenie sú najlepšia prevencia.</p>
        <p>Ak sa v domácnosti strieda viac pelechov, perte ich postupne a po každom praní urobte rovnakú krátku kontrolu. Jedna veľmi chlpatá dávka vie zanechať viac zvyškov než niekoľko bežných praní. Pravidelnosť je preto dôležitejšia než jednorazové silné čistenie po dlhom čase.</p>
        """
    ),
    "drum": clean_html(
        """
        <h2>Krátky kontrolný zoznam pred ďalšou dávkou</h2>
        <p>Pred ďalším praním sa pozrite na bubon pri dobrom svetle. Skontrolujte, či na dne nevidíte piesok, chlpy, kúsky trávy alebo drobné vlákna. Potom prejdite spodnú časť tesnenia handrou. Ak je handra čistá a práčka necítiť, môžete pokračovať. Ak nie, najprv urobte údržbu.</p>
        <p>Pri bielych a jemných veciach je lepšie venovať kontrole minútu navyše. Zvyšky z topánok alebo pracovných vecí môžu na bežnom tmavom oblečení zaniknúť, ale na bielych obliečkach alebo uterákoch sú viditeľné hneď. To isté platí pre pach po pelechu, ktorý sa v teplom a vlhkom textile rýchlo zvýrazní.</p>
        <p>Ak práčku používate aj na náročné dávky pravidelne, nastavte si jednoduché poradie: najprv jemné a svetlé pranie, potom bežné oblečenie, nakoniec pelechy, koberčeky, topánky alebo pracovné textílie. Po poslednej náročnej dávke nechajte práčku otvorenú a skontrolujte ju ešte v ten deň.</p>
        <p>Pri práčke, ktorá slúži celej domácnosti, pomáha aj jednoduchá dohoda: kto perie náročnú dávku, po praní skontroluje bubon a tesnenie. Zodpovednosť tak nezostane na ďalšom človeku, ktorý chce prať obliečky alebo detské oblečenie. Je to malý zvyk, ktorý predchádza zbytočným reklamáciám výsledku prania.</p>
        """
    ),
}


TOP_UP = {
    "drawer": clean_html(
        """
        <h2>Mesačný režim údržby zásobníka</h2>
        <p>Raz mesačne si spravte krátku kontrolu aj vtedy, keď zásobník na prvý pohľad nevyzerá špinavo. Vyberte ho, opláchnite teplou vodou, utrite priehradku na aviváž a pozrite sa do priestoru za zásuvkou. Práve tam sa často drží tenký film, ktorý nie je viditeľný spredu. Ak sa tento film nechá dlhodobo, môže sa miešať s novým pracím prostriedkom a ovplyvniť vôňu aj pocit z bielizne.</p>
        <p>Pri častom praní športu, uterákov alebo detských vecí má takáto údržba ešte väčší význam. Tieto dávky často používajú viac produktu, viac oplachu alebo aviváž, a tým sa zásobník zaťažuje viac než pri občasnom praní tričiek. Mesačný režim je jednoduchý kompromis medzi každodenným čistením a tým, že problém necháte narásť až do zápachu.</p>
        """
    ),
    "seal": clean_html(
        """
        <h2>Mesačný režim pri pelechoch a zvieracích textíliách</h2>
        <p>Ak periete pelechy, deky alebo uteráky pre zvieratá aspoň niekoľkokrát mesačne, nastavte si jednu dôkladnejšiu kontrolu práčky. Skontrolujte tesnenie, filter, zásobník aj vnútorný okraj dvierok. Tieto miesta spolu súvisia: chlpy sa zachytia v tesnení, jemné zvyšky môžu skončiť vo filtri a vlhkosť zhorší zápach v celej práčke.</p>
        <p>Takáto kontrola nie je náhradou za utretie tesnenia po každom praní pelechu. Je to druhá vrstva údržby, ktorá zachytí to, čo pri rýchlej kontrole nevidno. V domácnosti so zvieratami je to praktickejšie než čakať, kým začne zapáchať ďalšia čistá bielizeň. Pomáha aj pri čiernom oblečení, na ktorom je každá srsť okamžite viditeľná.</p>
        """
    ),
    "drum": clean_html(
        """
        <h2>Mesačný režim pri náročnom praní</h2>
        <p>Ak v práčke pravidelne končia pelechy, topánky, pracovné veci alebo koberčeky, nastavte si mesačný režim údržby. Skontrolujte bubon, tesnenie, filter aj zásobník. Pri náročných dávkach sa nečistoty neusádzajú iba na jednom mieste a práve kombinácia malých zvyškov spôsobuje, že práčka po čase začne zapáchať alebo prenášať nečistoty do ďalšieho prania.</p>
        <p>Najlepšie je plánovať náročné pranie pred údržbou, nie pred bielymi obliečkami. Po poslednej špinavej dávke vyčistite viditeľné zvyšky, nechajte práčku vetrať a podľa potreby použite čistiaci cyklus. Tak sa z náročného prania nestane problém pre zvyšok domácnosti. Tento zvyk je dôležitý najmä pri malých deťoch, alergikoch alebo svetlej bielizni.</p>
        """
    ),
}


FINAL_TOP_UP = {
    "drawer": "<h2>Doplnková kontrola</h2><p>Ak sa usadeniny vracajú aj po úprave dávkovania, pozrite sa aj na tvrdosť vody a na to, či zásobník po praní skutočne presychá. Kombinácia tvrdej vody a zatvorenej vlhkej zásuvky vie vytvoriť povlak veľmi rýchlo.</p>",
    "seal": "<h2>Doplnková kontrola</h2><p>Pri zvieracích textíliách sledujte aj to, či sa srsť nezachytáva na ďalšej dávke bielizne. Ak áno, tesnenie alebo filter ešte držia zvyšky a treba ich skontrolovať skôr, než budete prať uteráky alebo obliečky.</p>",
    "drum": "<h2>Doplnková kontrola</h2><p>Ak po náročnej dávke vidíte v bubne iba pár drobných zvyškov, aj tie odstráňte hneď. Pri ďalšom praní sa môžu nalepiť na svetlé textílie alebo skončiť v tesnení, kde sa budú horšie hľadať.</p>",
}


def naturalize_intro(long):
    long = re.sub(
        r"<p>Téma pokrýva (.*?)\. Čistý zásobník",
        r"<p>V článku riešime praktické situácie ako \1. Čistý zásobník",
        long,
    )
    long = re.sub(
        r"<p>V článku pokrývame aj hľadané výrazy ako <strong>(.*?)</strong>\. Základ je jednoduchý:",
        r"<p>V článku riešime aj praktické situácie: <strong>\1</strong>. Základ je jednoduchý:",
        long,
    )
    return long


def insert_expansion(long, expansion_key):
    extra_markers = {
        "drawer": "Kontrola po ďalších praniach",
        "seal": "Rutina pre domácnosť so psom alebo mačkou",
        "drum": "Kontrola pred bielou alebo jemnou dávkou",
    }
    if "Detailnejšia diagnostika zásobníka práčky" in long or "Prečo sa chlpy držia práve v tesnení" in long or "Prečo práčka potrebuje kontrolu po náročnej dávke" in long:
        if extra_markers[expansion_key] not in long:
            sales_start = long.find('<div style="border: 1px solid #dbe5de')
            if sales_start == -1:
                raise ValueError("Sales block start not found for extra depth")
            long = long[:sales_start].rstrip() + "\n" + EXTRA_DEPTH[expansion_key] + "\n" + long[sales_start:]
        if "Krátky kontrolný zoznam pred ďalšou dávkou" not in long:
            sales_start = long.find('<div style="border: 1px solid #dbe5de')
            if sales_start == -1:
                raise ValueError("Sales block start not found for final depth")
            long = long[:sales_start].rstrip() + "\n" + FINAL_DEPTH[expansion_key] + "\n" + long[sales_start:]
        if "Mesačný režim" not in long:
            sales_start = long.find('<div style="border: 1px solid #dbe5de')
            if sales_start == -1:
                raise ValueError("Sales block start not found for top-up depth")
            long = long[:sales_start].rstrip() + "\n" + TOP_UP[expansion_key] + "\n" + long[sales_start:]
        if "Doplnková kontrola" not in long:
            sales_start = long.find('<div style="border: 1px solid #dbe5de')
            if sales_start == -1:
                raise ValueError("Sales block start not found for final top-up")
            long = long[:sales_start].rstrip() + "\n" + FINAL_TOP_UP[expansion_key] + "\n" + long[sales_start:]
        return long
    marker = "\n<h2>Súvisiace návody na VEVO</h2>"
    related_index = long.find(marker)
    if related_index == -1:
        raise ValueError("Related guides marker not found")
    sales_start = long.rfind('<div style="border: 1px solid #dbe5de', 0, related_index)
    if sales_start == -1:
        raise ValueError("Sales block start not found")
    before_sales = long[:sales_start].rstrip()
    related_onward = long[related_index:].lstrip()
    context = {
        "drawer": "Pri zanesenom zásobníku má produkt zmysel až po ručnom vyčistení zásuvky a priestoru za ňou. Najprv odstráňte nánosy, potom riešte celkovú údržbu práčky.",
        "seal": "Pri chlpoch v tesnení najprv odstráňte srsť mechanicky. Produkt má zmysel ako následná údržba práčky, keď je tesnenie už zbavené hrubých zvyškov.",
        "drum": "Po pelechu, topánkach alebo pracovných veciach najprv vyberte viditeľné nečistoty. Produkt má zmysel ako následný krok pri zápachu alebo preventívnej údržbe.",
    }[expansion_key]
    expanded = "\n".join(
        [
            before_sales,
            EXPANSIONS[expansion_key],
            product_and_category_block(context),
            related_onward,
        ]
    )
    return expanded


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
    parser = argparse.ArgumentParser(description="Conservatively expand first VEVO washer-care retrofit wave.")
    parser.add_argument("--update-live", action="store_true")
    parser.add_argument("--sleep", type=float, default=1.5)
    args = parser.parse_args()

    by_source = {}
    updates = []
    for config in ARTICLES:
        by_source.setdefault(config["source"], json.loads(config["source"].read_text(encoding="utf-8")))
        rows = by_source[config["source"]]
        for article in rows:
            if article.get("link") != config["slug"]:
                continue
            original_long = article["long"]
            article["long"] = insert_expansion(naturalize_intro(article["long"]), config["expansion"])
            updates.append(
                {
                    "post_id": config["post_id"],
                    "slug": config["slug"],
                    "url": config["url"],
                    "title": article["title"],
                    "short": article["short"],
                    "long": article["long"],
                    "source_file": str(config["source"].relative_to(ROOT)),
                    "original_length": len(original_long),
                    "new_length": len(article["long"]),
                }
            )
            break
        else:
            raise SystemExit(f"Article not found: {config['slug']}")

    for source, rows in by_source.items():
        source.write_text(json.dumps(rows, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    OUT_JSON.write_text(
        json.dumps(
            {
                "project": "VEVO_CONTENT",
                "wave": "retrofit-wave-01-washer-care",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "purpose": "Conservative additive expansion of three washer-care articles.",
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
                "wave": "retrofit-wave-01-washer-care",
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
