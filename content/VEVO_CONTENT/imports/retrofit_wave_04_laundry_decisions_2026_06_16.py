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
OUT_JSON = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-04-laundry-decisions-2026-06-16.json"
MCP_RESULTS = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-04-laundry-decisions-2026-06-16-mcp-results.json"


ARTICLES = [
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-27-2026-06-16-articles.json",
        "slug": "praci-gel-alebo-praci-prasok-kedy-co-funguje-lepsie-a-preco",
        "post_id": "2261",
        "url": "https://www.vevo.sk/n/praci-gel-alebo-praci-prasok-kedy-co-funguje-lepsie-a-preco",
        "title": "Prací gél alebo prací prášok: kedy čo funguje lepšie a prečo",
        "expansion": "detergent_choice",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-26-2026-06-16-quality-update.json",
        "slug": "predpieranie-v-pracke-kedy-ma-zmysel-a-kedy-len-mina-vodu-cas-a-praci-prostriedok",
        "post_id": "2256",
        "url": "https://www.vevo.sk/n/predpieranie-v-pracke-kedy-ma-zmysel-a-kedy-len-mina-vodu-cas-a-praci-prostriedok",
        "title": "Predpieranie v práčke: kedy má zmysel a kedy len míňa vodu, čas a prací prostriedok",
        "expansion": "prewash_decision",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-26-2026-06-16-quality-update.json",
        "slug": "otacky-pri-odstredovani-ako-ovplyvnuju-vlhkost-krcenie-a-opotrebovanie-oblecenia",
        "post_id": "2257",
        "url": "https://www.vevo.sk/n/otacky-pri-odstredovani-ako-ovplyvnuju-vlhkost-krcenie-a-opotrebovanie-oblecenia",
        "title": "Otáčky pri odstreďovaní: ako ovplyvňujú vlhkosť, krčenie a opotrebovanie oblečenia",
        "expansion": "spin_speed",
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
    "detergent_choice": clean(
        f"""
        <h2>Ako si vybrať podľa materiálu, teploty a typu škvrny</h2>
        <p>Pri rozhodovaní medzi gélom a práškom začnite tým, čo je v bubne. Farebné tričká, tmavé nohavice, bežná spodná bielizeň a oblečenie prané pri nižšej teplote zvyčajne potrebujú hlavne rovnomerné rozptýlenie prostriedku a dobrý oplach. Tu býva tekutý prací gél praktický, pretože sa ľahko dávkuje aj pri menšej dávke.</p>
        <p>Biela bavlna, kuchynské utierky alebo textil so zreteľnejšími škvrnami môžu niekedy lepšie reagovať na prášok alebo špecializovaný postup. Stále však platí, že žiadna forma produktu nenahradí správny program, primeranú teplotu a nepreplnený bubon. Ak prášok použijete na krátky program, do tvrdej vody a na tmavé oblečenie, riziko šmúh bude vyššie.</p>
        {note_card("Rýchle rozhodnutie pred praním", [
            "<strong>Farebné a tmavé oblečenie:</strong> voľte skôr dobre dávkovateľný gél a nepreháňajte množstvo.",
            "<strong>Biela bavlna a utierky:</strong> zvážte prášok alebo cielenejší postup podľa štítku.",
            "<strong>Citlivejšia pokožka:</strong> sledujte najmä zvyšky v textile, parfumáciu a kvalitu oplachu.",
            "<strong>Krátky program:</strong> dávku znížte, pretože čas na rozptýlenie a vypláchnutie je obmedzený."
        ])}
        <h2>Porovnanie v bežných domácich situáciách</h2>
        {table(["Situácia", "Praktická voľba", "Čo kontrolovať"], [
            ("Čierne tričká a nohavice", "tekutý prací gél", "nízka dávka, dostatok oplachu, žiadne biele šmuhy"),
            ("Biele utierky a bavlna", "prášok alebo špecializovaný postup", "štítok, teplota, typ škvrny a tvrdosť vody"),
            ("Malá dávka na krátkom programe", "skôr gél v menšom množstve", "zvyšky produktu, lepkavosť a zápach po vysušení"),
            ("Veľmi špinavé pracovné veci", "najprv predčistenie, potom vhodný program", "nečakať, že forma produktu vyrieši hrubú špinu"),
            ("Bielizeň pre citlivejšiu pokožku", "jemnejší produkt a dôkladný oplach", "nepreplniť bubon a nedávkovať od oka"),
        ])}
        <h2>Prečo zvyšky produktu rozhodujú viac než forma</h2>
        <p>Veľa problémov, ktoré ľudia pripisujú praciemu gélu alebo prášku, v skutočnosti vzniká z kombinácie príliš veľkej dávky, krátkeho programu a slabého oplachu. Bielizeň môže pôsobiť čistá hneď po vybratí z práčky, no po vysušení je tvrdá, lepkavá alebo na tmavých kusoch vidno mapy. Vtedy treba upraviť dávkovanie a program skôr než meniť produkt naslepo.</p>
        <p>Pri géli sa problém často prejaví ako hladký film alebo príliš silná vôňa. Pri prášku môžu byť viditeľnejšie biele stopy, najmä na tmavých materiáloch. V oboch prípadoch pomôže menšia náplň, presnejšia dávka, dlhší program alebo extra oplach. Ak je bubon natlačený, prací roztok sa nedostane ku každému kusu rovnomerne a oplach nemá šancu odviesť zvyšky von.</p>
        <h2>Kedy má zmysel mať doma oba typy</h2>
        <p>Najpraktickejšia domácnosť často nepoužíva jeden univerzálny prostriedok na všetko. Jeden tekutý gél môže slúžiť ako základ na bežné farebné pranie a tmavšie oblečenie. Druhý, špecializovanejší produkt alebo prášok môže mať miesto pri bielej bavlne, odolnejších škvrnách alebo pracovných textíliách. Rozdiel nie je v tom, že jeden typ je vždy lepší, ale v tom, že každý má svoje vhodné použitie.</p>
        <p>Ak chcete rutinu zjednodušiť, začnite od toho, čo periete najčastejšie. Pri väčšine domácností sú to tričká, spodná bielizeň, pyžamá, uteráky a obliečky. Pri nich sa oplatí mať stabilný prací gél, správne dávkovanie a jasné pravidlo, kedy oddeliť uteráky, športové veci alebo bielu bavlnu do samostatnej dávky.</p>
        <div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 24px 0; background: #f7fbf8;">
        <h2 style="margin-top: 0;">Praktický základ pre bežné pranie</h2>
        <p>Ak doma najčastejšie periete farebné a bežné oblečenie, začnite pracím gélom a dávku upravujte podľa náplne, tvrdosti vody a programu.</p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="/p-1551/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l">Pozrieť prací gél</a></p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="/c/vevo-home-care/pranie/praci-gel">Pozrieť kategóriu pracích gélov</a></p>
        </div>
        <h2>Jednoduché pravidlo pre ďalšie pranie</h2>
        <p>Ak sa neviete rozhodnúť, vyberajte podľa rizika. Pri tmavom a farebnom oblečení je väčším rizikom viditeľný zvyšok a slabý oplach, preto je praktický dobre dávkovateľný gél. Pri bielej bavlne a odolnejšej špine riešte najmä typ škvrny, štítok a vhodný program. Vždy však platí: menej produktu v dobre zvolenom programe býva lepšie než veľa produktu v preplnenom bubne.</p>
        """
    ),
    "prewash_decision": clean(
        f"""
        <h2>Predpieranie nie je náhrada predčistenia</h2>
        <p>Predpieranie má najväčší zmysel pri špine, ktorú treba pred hlavným praním uvoľniť z povrchu textilu: blato, prach, pot, zvyšky trávy alebo pracovná špina. Neznamená to však, že všetko špinavé má ísť rovno do práčky. Hrubé blato, chlpy, piesok a zaschnuté kúsky špiny treba najprv vytriasť, zotrieť alebo jemne predčistiť mimo bubna.</p>
        <p>Ak sa hrubá špina dostane do práčky, predpieranie môže pomôcť bielizni, ale zároveň zaťaží spotrebič. Nečistoty sa môžu usádzať v tesnení, zásobníku a filtri. Preto pri pracovných veciach, záhradnom oblečení, pelechoch alebo veľmi špinavých detských veciach rozmýšľajte v dvoch krokoch: najprv odstrániť hrubú špinu, potom zvoliť program.</p>
        {note_card("Kedy predpieranie zvoliť a kedy nie", [
            "<strong>Blato a hlina:</strong> áno, ale až po vysušení a vytrasení hrubej vrstvy.",
            "<strong>Bežné nosenie:</strong> väčšinou nie, stačí vhodný hlavný program.",
            "<strong>Mastnota:</strong> často potrebuje cielenejšie predčistenie, nie iba dlhší cyklus.",
            "<strong>Zápach z práčky:</strong> predpieranie ho nevyrieši, treba skontrolovať spotrebič."
        ])}
        <h2>Rozhodovanie podľa typu znečistenia</h2>
        {table(["Typ znečistenia", "Predpieranie", "Lepší prvý krok"], [
            ("Suché blato na nohaviciach", "môže pomôcť", "nechať uschnúť, vykefovať, až potom prať"),
            ("Pot na tričku", "nie vždy nutné", "neodkladať vlhké veci do koša a zvoliť vhodný program"),
            ("Mastný fľak", "samotné predpieranie často nestačí", "predčistiť miesto podľa materiálu"),
            ("Pelech alebo textil s chlpmi", "opatrne", "najprv odstrániť chlpy a po praní vyčistiť práčku"),
            ("Bežné detské oblečenie", "iba pri silnej špine", "triediť podľa škvŕn a nepreplniť bubon"),
        ])}
        <h2>Koľko pracieho prostriedku pri predpierke</h2>
        <p>Pri predpierke je častá chyba pridať veľa prostriedku do predpierky aj do hlavného prania. Výsledkom nemusí byť čistejšia bielizeň, ale viac zvyškov v textile a v práčke. Predpierka má uvoľniť časť nečistôt, hlavný program má následne vyprať. Ak dáte priveľa gélu alebo prášku do oboch krokov, oplach bude mať zbytočne ťažkú prácu.</p>
        <p>Pri veľmi špinavom textile radšej znížte náplň bubna, zvoľte primeranú teplotu podľa štítku a nechajte textilu priestor. Ak je bubon plný pracovných nohavíc, uterákov a detských vecí naraz, predpieranie samo o sebe nepomôže. Voda a prací roztok sa musia dostať ku každému kusu.</p>
        <h2>Predpieranie a teplota vody</h2>
        <p>Pri predpierke nemusí byť vždy cieľom vysoká teplota. Niektoré škvrny sa pri príliš teplej vode môžu horšie uvoľňovať alebo sa zafixujú skôr, než ich hlavný program stihne vyprať. Preto sa oplatí pozrieť najprv na pôvod škvrny a materiál. Blato, prach a pot zvyčajne potrebujú hlavne pohyb, vodu a čas. Mastnota alebo kozmetika často potrebuje lokálne predčistenie.</p>
        <p>Ak neviete, akú teplotu zvoliť, držte sa štítku a nezačínajte najagresívnejším nastavením. Šetrnejší postup s menšou dávkou, dobrým mechanickým pohybom a správnym hlavným praním býva bezpečnejší než automaticky pridávať teplotu, predpierku aj veľa prostriedku naraz.</p>
        <h2>Kedy po predpierke vyčistiť práčku</h2>
        <p>Ak pravidelne periete blato, pelechy, textil po záhrade alebo pracovné oblečenie, samotná bielizeň nie je jediná vec, ktorá potrebuje pozornosť. Po takýchto dávkach skontrolujte tesnenie, zásobník a filter. Ak sa začne vracať zatuchnutý pach, môže ísť o usadeniny v práčke, nie o slabý prací prostriedok.</p>
        <p>Preventívne pomáha prať veľmi špinavé dávky oddelene, nenechávať mokré veci dlho v bubne a po praní nechať dvierka aj zásobník preschnúť. Pri opakovanom zápachu má zmysel zaradiť samostatné čistenie práčky bez bielizne.</p>
        <div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 24px 0; background: #f7fbf8;">
        <h2 style="margin-top: 0;">Po špinavých dávkach myslite aj na práčku</h2>
        <p>Predpieranie pomáha textilu, ale pri blate, chlpovej záťaži alebo pracovných veciach sa časť nečistôt môže usadiť aj v spotrebiči.</p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="/p-1549/vevo-shot-koncentrat-na-cistenie-pracky">Pozrieť čistič práčky</a></p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="/c/vevo-home-care/pranie/detox-pracky">Pozrieť detox práčky</a></p>
        </div>
        <h2>Jednoduché pravidlo pre predpieranie</h2>
        <p>Predpieranie používajte vtedy, keď pomáha uvoľniť skutočnú špinu pred hlavným praním. Nepoužívajte ho automaticky na každú dávku a nesnažte sa ním nahradiť vytrasenie blata, odstránenie chlpov alebo predčistenie mastného fľaku. Ak sa problém po praní opakuje, skontrolujte aj práčku, nie iba program.</p>
        """
    ),
    "spin_speed": clean(
        f"""
        <h2>Otáčky riešte spolu so sušením, nie oddelene</h2>
        <p>Vyššie otáčky dostanú z bielizne viac vody, ale nie vždy sú lepšou voľbou. Pri pevnej bavlne, uterákoch alebo obliečkach môžu skrátiť sušenie. Pri viskóze, elastických vláknach, jemných blúzkach alebo pleteninách môžu zvýšiť krčenie, namáhanie vlákien a riziko deformácie. Preto otáčky nevyberajte podľa najvyššieho čísla na práčke, ale podľa toho, čo sa bude diať po praní.</p>
        <p>Ak sušíte v byte, príliš nízke otáčky môžu nechať bielizeň veľmi mokrú a sušenie sa natiahne. Vlhký textil potom ľahšie zatuchne. Ak však nastavíte príliš vysoké otáčky na citlivý materiál, ušetríte trochu času pri sušení, ale môžete získať pokrčený alebo vytiahnutý kus oblečenia. Správne nastavenie je kompromis medzi vlhkosťou, materiálom a spôsobom sušenia.</p>
        {note_card("Rýchla voľba podľa textilu", [
            "<strong>Uteráky a pevná bavlna:</strong> zvyčajne znesú vyššie otáčky, ak to povoľuje štítok.",
            "<strong>Obliečky:</strong> vyššie otáčky môžu pomôcť, ale bubon nesmie byť preplnený.",
            "<strong>Viskóza a jemné materiály:</strong> voľte nižšie otáčky a šetrnejšie sušenie.",
            "<strong>Elastické športové oblečenie:</strong> príliš vysoké otáčky môžu zbytočne namáhať vlákna."
        ])}
        <h2>Odporúčané nastavenie v bežných situáciách</h2>
        {table(["Textil", "Praktické otáčky", "Poznámka"], [
            ("Uteráky", "vyššie, ak to štítok povoľuje", "rýchlejšie schnú, ale nepreplňte bubon"),
            ("Posteľná bielizeň", "stredné až vyššie", "veľké kusy sa musia vedieť rozložiť"),
            ("Viskózová blúzka", "nižšie", "materiál je citlivý na deformáciu a krčenie"),
            ("Legíny a elastické veci", "stredné alebo nižšie", "chrániť pružnosť a tvar"),
            ("Svetre a pleteniny", "nízke alebo veľmi šetrný režim", "riadiť sa štítkom a sušiť naplocho, ak treba"),
        ])}
        <h2>Prečo bielizeň po odstreďovaní zostane mokrá</h2>
        <p>Ak je bielizeň po programe stále veľmi mokrá, nemusí byť problém iba v nastavení otáčok. Často ide o preplnený bubon, zle rozložené veľké kusy, upchatý filter alebo textil, ktorý nasiakne veľa vody. Práčka sa pri nevyváženej náplni môže snažiť odstreďovanie obmedziť, aby znížila vibrácie. Výsledkom je mokrejšia bielizeň a dlhšie sušenie.</p>
        <p>Pri obliečkach pomôže zapnúť zipsy a neprepĺňať bubon. Pri uterákoch pomôže prať ich samostatne a nechať im priestor. Ak sa problém opakuje aj pri primeranej náplni, skontrolujte filter a odtok. Pridávať stále vyššie otáčky bez kontroly príčiny nemusí problém vyriešiť.</p>
        <h2>Ako znížiť krčenie po praní</h2>
        <p>Krčenie často vzniká kombináciou preplneného bubna, vysokých otáčok, nevhodného programu a pomalého vyberania bielizne po skončení prania. Ak chcete menej pokrčené oblečenie, znížte náplň, pri jemnejších veciach zvoľte nižšie otáčky a bielizeň vyberte hneď po programe. Potom ju pretrepte a zaveste s priestorom medzi kusmi.</p>
        <p>Pri materiáloch ako viskóza, ľan alebo jemné zmesi je dôležité nehodnotiť iba vlhkosť. Niekedy je lepšie nechať textil o niečo vlhší a sušiť ho správne, než ho silno vyžmýkať v bubne a potom bojovať s deformáciou alebo tvrdými záhybmi.</p>
        <div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 24px 0; background: #f7fbf8;">
        <h2 style="margin-top: 0;">Pri sušičke pomáha oddeliť veľké kusy textilu</h2>
        <p>Ak po odstreďovaní používate sušičku, sledujte štítok a veľkosť dávky. Pri vhodných textíliách môžu vlnené gule pomôcť rovnomernejšiemu pohybu v bubne.</p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="/p-1612/prirodne-vlnene-gule-do-susicky-3-ks">Pozrieť vlnené gule</a></p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="/c/vevo-home-care/pranie/gule-do-susicky">Pozrieť kategóriu gúľ do sušičky</a></p>
        </div>
        <h2>Jednoduché pravidlo pre odstreďovanie</h2>
        <p>Vyššie otáčky používajte pri textíliách, ktoré ich znesú a ktoré by inak dlho schli. Nižšie otáčky používajte pri materiáloch, kde je dôležitejší tvar, pružnosť a menšie krčenie. Ak je bielizeň opakovane mokrá, najprv skontrolujte náplň, rozloženie veľkých kusov a filter, až potom zvyšujte otáčky.</p>
        """
    ),
}


MARKERS = {
    "detergent_choice": "Ako si vybrať podľa materiálu, teploty a typu škvrny",
    "prewash_decision": "Predpieranie nie je náhrada predčistenia",
    "spin_speed": "Otáčky riešte spolu so sušením, nie oddelene",
}


PREWASH_TEMPERATURE_SECTION = clean(
    """
    <h2>Predpieranie a teplota vody</h2>
    <p>Pri predpierke nemusí byť vždy cieľom vysoká teplota. Niektoré škvrny sa pri príliš teplej vode môžu horšie uvoľňovať alebo sa zafixujú skôr, než ich hlavný program stihne vyprať. Preto sa oplatí pozrieť najprv na pôvod škvrny a materiál. Blato, prach a pot zvyčajne potrebujú hlavne pohyb, vodu a čas. Mastnota alebo kozmetika často potrebuje lokálne predčistenie.</p>
    <p>Ak neviete, akú teplotu zvoliť, držte sa štítku a nezačínajte najagresívnejším nastavením. Šetrnejší postup s menšou dávkou, dobrým mechanickým pohybom a správnym hlavným praním býva bezpečnejší než automaticky pridávať teplotu, predpierku aj veľa prostriedku naraz.</p>
    """
)


DETERGENT_WATER_SECTION = clean(
    """
    <h2>Tvrdosť vody a forma pracieho prostriedku</h2>
    <p>Tvrdosť vody mení to, ako sa prací prostriedok správa v bubne. V tvrdšej vode môže byť potrebné presnejšie dávkovanie a dôkladnejší oplach, pretože minerály vo vode ovplyvňujú pranie aj pocit z bielizne po vysušení. Ak sa pri rovnakej dávke objavujú šmuhy, tvrdosť alebo lepkavý pocit, neriešte to automaticky výmenou gélu za prášok. Najprv upravte dávku a veľkosť náplne.</p>
    <p>Pri mäkšej vode býva ľahšie naliať zbytočne veľa tekutého prostriedku, pretože pena a vôňa môžu vytvoriť dojem silnejšieho prania. Pri tvrdej vode zas môže človek dávku zvyšovať bez toho, aby vyriešil preplnený bubon alebo krátky program. V oboch prípadoch je najspoľahlivejšie dávkovať podľa obalu, vody, náplne a reálneho znečistenia.</p>
    """
)


SPIN_FINAL_CHECK_SECTION = clean(
    """
    <h2>Kontrola po vybratí z práčky</h2>
    <p>Správne otáčky spoznáte aj podľa toho, ako sa bielizeň správa po otvorení dvierok. Ak je pevná bavlna primerane vlhká a dá sa dobre pretrepať, nastavenie bolo pravdepodobne vhodné. Ak sú jemné kusy skrútené, tvrdé v záhyboch alebo výrazne natiahnuté, odstreďovanie bolo na daný materiál príliš agresívne.</p>
    <p>Pri ďalšom praní rovnakého typu textilu si nastavenie upravte o jeden krok, nie extrémne. Pri uterákoch môžete skúsiť vyššie otáčky, ak dlho schnú. Pri viskóze, elastane alebo pleteninách skôr znížte otáčky a po praní textil hneď vytvarujte. Tak sa rutina postupne prispôsobí konkrétnej domácnosti, nie iba číslam na displeji práčky.</p>
    """
)


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
    marker = MARKERS[key]
    if marker in long:
        if key == "detergent_choice" and "Tvrdosť vody a forma pracieho prostriedku" not in long:
            target = "<h2>Kedy má zmysel mať doma oba typy</h2>"
            if target not in long:
                raise ValueError("Could not find detergent water insertion point")
            return long.replace(target, DETERGENT_WATER_SECTION + "\n" + target, 1)
        if key == "prewash_decision" and "Predpieranie a teplota vody" not in long:
            target = "<h2>Kedy po predpierke vyčistiť práčku</h2>"
            if target not in long:
                raise ValueError("Could not find prewash temperature insertion point")
            return long.replace(target, PREWASH_TEMPERATURE_SECTION + "\n" + target, 1)
        if key == "spin_speed" and "Kontrola po vybratí z práčky" not in long:
            target = "<h2>Ako znížiť krčenie po praní</h2>"
            if target not in long:
                raise ValueError("Could not find spin final check insertion point")
            return long.replace(target, SPIN_FINAL_CHECK_SECTION + "\n" + target, 1)
        return long
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
    parser = argparse.ArgumentParser(description="Conservatively expand VEVO laundry decision wave 04.")
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
                "wave": "retrofit-wave-04-laundry-decisions",
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
                "wave": "retrofit-wave-04-laundry-decisions",
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
