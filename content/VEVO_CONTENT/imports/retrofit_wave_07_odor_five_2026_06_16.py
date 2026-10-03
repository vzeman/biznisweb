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
OUT_JSON = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-07-odor-five-2026-06-16.json"
MCP_RESULTS = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-07-odor-five-2026-06-16-mcp-results.json"


ARTICLES = [
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-19-2026-06-10-articles.json",
        "slug": "ako-odstranit-zapach-z-cestovneho-vankusa-po-lietadle",
        "post_id": "2215",
        "url": "https://www.vevo.sk/n/ako-odstranit-zapach-z-cestovneho-vankusa-po-lietadle",
        "expansion": "travel_pillow",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-15-2026-06-09-articles.json",
        "slug": "ako-odstranit-mlieko-a-jogurt-z-textilu-bez-kysleho-zapachu",
        "post_id": "2139",
        "url": "https://www.vevo.sk/n/ako-odstranit-mlieko-a-jogurt-z-textilu-bez-kysleho-zapachu",
        "expansion": "milk_yogurt",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-15-2026-06-09-articles.json",
        "slug": "ako-vyprat-kakao-z-pyzama-a-postelnej-bielizne-bez-mliecneho-zapachu",
        "post_id": "2138",
        "url": "https://www.vevo.sk/n/ako-vyprat-kakao-z-pyzama-a-postelnej-bielizne-bez-mliecneho-zapachu",
        "expansion": "cocoa_bedding",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-15-2026-06-09-articles.json",
        "slug": "ako-odstranit-zapach-z-ponoziek-a-sportovej-obuvi-po-treningu",
        "post_id": "2131",
        "url": "https://www.vevo.sk/n/ako-odstranit-zapach-z-ponoziek-a-sportovej-obuvi-po-treningu",
        "expansion": "socks_shoes",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-15-2026-06-09-articles.json",
        "slug": "ako-odstranit-zapach-z-bezeckych-legin-po-treningu",
        "post_id": "2128",
        "url": "https://www.vevo.sk/n/ako-odstranit-zapach-z-bezeckych-legin-po-treningu",
        "expansion": "running_leggings",
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
            <h2 style="margin-top: 0;">Odporúčané riešenie na opatrné testovanie vône</h2>
            <p>Pri textíliách pri tvári alebo citlivejšom nose je rozumnejšie začať menším množstvom a najprv zistiť, ktorá vôňa vám sedí.</p>
            <div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff; margin: 14px 0;">
            <h3 style="margin-top: 0;">Vevo Essence Sample Set 9x10ml</h3>
            <p>Vzorkový set pomôže otestovať intenzitu vôní postupne a bez veľkého balenia.</p>
            <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="/p-1621/vevo-essence-sample-set">Pozrieť vzorkový set</a></p>
            </div>
            <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="/c/vzorky/parfum-do-prania-vzorky">Pozrieť vzorky parfumov do prania</a></p>
            </div>
            """
        )
    return clean(
        """
        <div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 24px 0; background: #f7fbf8;">
        <h2 style="margin-top: 0;">Odporúčané riešenie na pranie textílií so zápachom</h2>
        <p>Pri pachu najprv riešte zdroj: pot, mastnotu, mliečne zvyšky alebo vlhkosť. Prací produkt má pomôcť čistiť, nie prekryť problém silnou vôňou.</p>
        <div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff; margin: 14px 0;">
        <h3 style="margin-top: 0;">Prací gél hypoalergénny z Marseillského mydla 1L</h3>
        <p>Univerzálny základ na bežné pranie textílií, pri ktorých chcete odstrániť nečistoty a zvyšky pachu pred pridaním vône.</p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="/p-1551/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l">Pozrieť prací gél</a></p>
        </div>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="/c/vevo-home-care/pranie/praci-gel">Pozrieť pracie gély</a></p>
        </div>
        """
    )


EXPANSIONS = {
    "travel_pillow": clean(
        f"""
        <h2>Prečo cestovný vankúš po lete zapácha</h2>
        <p>Cestovný vankúš je malý kus textilu, ale dostáva veľkú záťaž. Je priamo pri krku, vlasoch a tvári, počas letu sa dotýka oblečenia, sedadla, tašky aj rúk. Pach preto nevzniká iba z lietadla. Často ide o kombináciu potu, kožného mazu, krému, parfumu, vlasových produktov a vlhkosti, ktorá sa po návrate zatvorí v kufri alebo batohu.</p>
        <p>Najdôležitejšie je rozlíšiť poťah a výplň. Poťah býva prateľný, výplň často nie. Pamäťová pena, mikroguľôčky alebo nafukovací vankúš potrebujú iný postup. Ak sa výplň premočí a schne pomaly, pach sa môže zhoršiť a vankúš môže stratiť tvar. Preto treba čistiť presne, nie silovo.</p>
        {note_card("Rýchla diagnostika pred čistením", [
            "<strong>Snímateľný poťah:</strong> vyperte ho samostatne podľa štítku.",
            "<strong>Pamäťová pena:</strong> neperte ju v práčke bez odporúčania výrobcu.",
            "<strong>Pach pri krku:</strong> riešte pot, krém a kožný maz, nie iba prevoňanie.",
            "<strong>Uloženie po ceste:</strong> vankúš najprv vyvetrajte, až potom ho dajte do skrine."
        ])}
        <h2>Postup podľa typu cestovného vankúša</h2>
        {table(["Typ vankúša", "Bezpečný postup", "Čomu sa vyhnúť"], [
            ("Poťah so zipsom", "prať poťah samostatne a sušiť úplne dosucha", "odložiť vlhký poťah späť na výplň"),
            ("Pamäťová pena", "vetrať, lokálne čistiť iba povrch a nechať dlho schnúť", "práčka, žmýkanie a radiátor"),
            ("Mikroguľôčky", "riadiť sa štítkom, väčšinou skôr vetrať než prať", "premáčanie, ktoré sa zle suší"),
            ("Nafukovací vankúš", "umyť povrch, poťah vyprať zvlášť", "skladovať vlhký v obale"),
            ("Cestovný vankúš pre deti", "prať poťah častejšie a kontrolovať škvrny od jedla", "silná vôňa blízko tváre"),
        ])}
        <h2>Ako vyčistiť poťah bez návratu pachu</h2>
        <p>Poťah otočte naruby a skontrolujte miesta pri krku. Ak sú mastnejšie, pred praním ich jemne ošetrite malým množstvom pracieho roztoku. Nepoužívajte priveľa produktu, pretože hrubší alebo mäkčený poťah sa potom môže horšie vypláchnuť. Perte radšej s menšou dávkou podobných jemných textílií, aby mal poťah v bubne priestor.</p>
        <p>Pred navlečením späť na výplň musí byť poťah úplne suchý. Ak ostane len mierne vlhký, vo vnútri sa pach obnoví. Pri cestovaní je dobré mať samostatné prateľné vrecko alebo obal, aby vankúš po použití neležal voľne medzi topánkami, kozmetikou a oblečením z cesty.</p>
        <h2>Čo robiť s výplňou, ktorá sa nedá prať</h2>
        <p>Výplň najprv vyvetrajte mimo priameho slnka a tepla. Ak cítiť konkrétne miesto, čistite ho iba lokálne a veľmi mierne navlhčenou handričkou. Cieľom nie je dostať vodu dovnútra, ale odstrániť povrchový zdroj pachu. Pri pamäťovej pene je dôležité, aby po čistení preschla aj vo vnútri, čo môže trvať dlhšie než pri bežnom textile.</p>
        <p>Ak výplň zapácha aj po vyvetraní a poťah je čistý, môže byť problém v tom, že sa dlhodobo skladovala vlhká. Vtedy pomôže skôr opakované vetranie a suché skladovanie než ďalšie prevoňanie. Vôňu používajte až na čistý a suchý poťah, nie na vlhkú výplň.</p>
        <div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">
        <h2 style="margin-top: 0;">Pravidlo pri textíliách pri tvári</h2>
        <p>Pri vankúši, šatke, kapucni alebo golieri používajte vôňu opatrnejšie než pri uterákoch či bežnej bielizni. Textil je blízko nosa a pokožky, preto aj mierna intenzita pôsobí silnejšie.</p>
        </div>
        <h2>Ako zabrániť pachu pri ďalšej ceste</h2>
        <p>Po každej ceste nechajte vankúš vyvetrať ešte pred vybalením zvyšnej batožiny. Poťah perte podľa potreby a výplň skladujte suchú, nie stlačenú v nepriedušnom obale. Ak cestujete často, oplatí sa mať dva poťahy: jeden na cestu a jeden čistý po návrate.</p>
        <p>Ak chcete pridať vôňu, začnite testom na poťahu a nízkou intenzitou. Pri citlivejšom nose, deťoch alebo migréne je lepšie mať vankúš neutrálne čistý než výrazne parfumovaný.</p>
        {product_card("samples")}
        <h2>FAQ: cestovný vankúš a zápach</h2>
        <h3>Môžem cestovný vankúš vyprať celý?</h3>
        <p>Iba ak to povoľuje výrobca. Pri pamäťovej pene a výplniach, ktoré zle schnú, perte skôr poťah a výplň vetrajte.</p>
        <h3>Prečo vankúš zapácha aj po vypratí poťahu?</h3>
        <p>Pach môže byť vo výplni alebo v skladovaní. Ak poťah navlečiete späť vlhký, problém sa rýchlo vráti.</p>
        <h3>Je vhodné použiť parfum do prania?</h3>
        <p>Áno, ale jemne a až na čistý poťah. Keďže je vankúš pri tvári, intenzitu testujte opatrne.</p>
        """
    ),
    "milk_yogurt": clean(
        f"""
        <h2>Prečo mlieko a jogurt kysnú v textile</h2>
        <p>Mlieko a jogurt sú zradné tým, že na začiatku nemusia vyzerať ako vážna škvrna. Problém sa často ukáže až po pár hodinách, keď sa v textile začne objavovať kyslý zápach. Dôvodom je kombinácia bielkovín, tukov, cukrov a vlhkosti. Ak sa zvyšky nechajú zaschnúť v koši na bielizeň, pach sa môže preniesť aj na ďalšie kusy.</p>
        <p>Pri mliečnych škvrnách nie je najlepším prvým krokom horúca voda. Teplo môže bielkovinové zvyšky zafixovať a mastnejšia časť ostane vo vlákne. Lepšie funguje studenšie prepláchnutie, jemné predčistenie a až potom bežné pranie podľa štítku.</p>
        {note_card("Rýchly postup pri mlieku a jogurte", [
            "<strong>Čerstvá škvrna:</strong> najprv odstrániť prebytok a prepláchnuť studenšou vodou.",
            "<strong>Zaschnutá škvrna:</strong> jemne navlhčiť a uvoľniť, nešúchať nasucho.",
            "<strong>Kyslý pach:</strong> riešiť zdroj, nie prekrytie vôňou.",
            "<strong>Detské oblečenie:</strong> prať čím skôr, lebo body a pyžamá držia vlhkosť."
        ])}
        <h2>Postup podľa typu textilu</h2>
        {table(["Textil", "Čo urobiť najprv", "Pozor na"], [
            ("Detské body", "prepláchnuť zo zadnej strany a predprať", "zaschnutie v koši"),
            ("Tričko", "odstrániť jogurt lyžičkou a jemne ošetriť", "roztieranie do väčšej plochy"),
            ("Obrus", "oddeliť od ostatnej bielizne a predčistiť", "kombináciu mlieka, kávy a kakaa"),
            ("Poťah alebo deka", "čistiť lokálne a nepremačať výplň", "pomalé schnutie"),
            ("Jemná látka", "testovať na skrytom mieste", "silné trenie a horúcu vodu"),
        ])}
        <h2>Ako odstrániť kyslý zápach po praní</h2>
        <p>Ak textil po praní stále cítiť kyslo, nedávajte ho do sušičky ani na radiátor. Najprv skontrolujte miesto škvrny. Ak je mastnejšie, tvrdšie alebo inak vonia než zvyšok látky, zvyšky mlieka ešte ostali vo vlákne. V takom prípade zopakujte lokálne predčistenie a perte menšiu dávku, aby mal textil lepší oplach.</p>
        <p>Kyslý pach sa môže zhoršiť aj vtedy, keď oblečenie po praní pomaly schne. Detské pyžamá, deky a hrubšie bavlnené kusy nenechávajte visieť natlačené na sebe. Sušte ich vo vzdušnom priestore a do skrine ich ukladajte až úplne suché.</p>
        <h2>Čo robiť pri zaschnutom jogurte</h2>
        <p>Jogurt najprv mechanicky odstráňte, ale netlačte ho hlbšie do vlákna. Zaschnuté miesto jemne navlhčite studenšou vodou a nechajte chvíľu povoliť. Potom použite malé množstvo pracieho roztoku a opatrne ho zapracujte len v mieste škvrny. Pri farebnom textile najprv testujte stálosť farby.</p>
        <p>Pri veľkej škvrne na deke alebo poťahu je dôležité nepremačať vnútro. Ak sa mliečna zložka dostane do výplne, môže schnúť dlho a pach sa vráti pri každom zahriatí alebo vlhkosti.</p>
        <div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">
        <h2 style="margin-top: 0;">Domáca kontrola pred sušením</h2>
        <p>Pred sušením ovoňajte miesto škvrny ešte vlhké. Ak cítiť kyslý tón, nepoužívajte teplo. Teplo môže zvyšky zvýrazniť a ďalšie čistenie bude ťažšie.</p>
        </div>
        <h2>Ako predchádzať mliečnemu zápachu v koši</h2>
        <p>Textil s mliekom alebo jogurtom nepatrí zatvorený do koša na bielizeň na niekoľko dní. Ak nemôžete prať hneď, aspoň odstráňte prebytok, prepláchnite miesto a nechajte kus preschnúť oddelene. Pri detských veciach je praktické mať menšiu samostatnú dávku na rýchle pranie, aby sa mliečne zvyšky nemiešali s uterákmi alebo posteľnou bielizňou.</p>
        <p>Vôňu do prania používajte až vtedy, keď je kyslý zdroj odstránený. Ak ju pridáte predčasne, bude sa miešať s pachom a výsledok bude ťažší, nie čistejší.</p>
        {product_card("laundry")}
        <h2>FAQ: mlieko, jogurt a textil</h2>
        <h3>Prečo nepoužiť hneď horúcu vodu?</h3>
        <p>Mliečne zvyšky obsahujú bielkoviny a tuk. Horúci postup môže časť z nich zafixovať, preto je bezpečnejšie začať studenším prepláchnutím.</p>
        <h3>Čo ak oblečenie smrdí kyslo aj po praní?</h3>
        <p>Zopakujte lokálne predčistenie, perte menšiu dávku a sušte rýchlejšie. Pred sušičkou skontrolujte, či pach naozaj zmizol.</p>
        <h3>Dá sa škvrna len prevoňať?</h3>
        <p>Nie spoľahlivo. Vôňa môže doplniť čistý textil, ale kyslý mliečny zvyšok treba najprv odstrániť.</p>
        """
    ),
    "cocoa_bedding": clean(
        f"""
        <h2>Prečo je kakao na pyžame a obliečkach zložitejšie</h2>
        <p>Kakao nie je obyčajná hnedá voda. V typickom domácom kakau je mlieko, cukor a kakaový prášok. Preto riešite naraz mliečny zápach, sladký lepkavý zvyšok aj farebnú stopu. Na pyžame a posteľnej bielizni sa škvrna často rozotrie počas spánku, takže po ránu už nemusí byť presne tam, kde vznikla.</p>
        <p>Najväčšou chybou je hodiť pyžamo a obliečky rovno do koša a prať až o pár dní. Mliečna časť začne kysnúť, cukor lepí a kakaová stopa sa môže usadiť hlbšie vo vlákne. Preto má zmysel krátke predčistenie ešte pred bežným praním.</p>
        {note_card("Rýchla odpoveď podľa situácie", [
            "<strong>Čerstvé kakao:</strong> odsajte, prepláchnite studenšou vodou a predperte.",
            "<strong>Zaschnuté kakao:</strong> najprv navlhčiť a uvoľniť, nie šúchať nasucho.",
            "<strong>Posteľná bielizeň:</strong> prať samostatne alebo s podobnými kusmi, nie preplniť bubon.",
            "<strong>Matrac:</strong> chrániť pred premočením a riešiť iba povrch."
        ])}
        <h2>Postup pri pyžame, obliečkach a plachte</h2>
        {table(["Textil", "Prvý krok", "Dôležitý detail"], [
            ("Pyžamo", "prepláchnuť zo zadnej strany škvrny", "skontrolovať manžety a golier"),
            ("Obliečka na vankúš", "predčistiť miesto a prať podľa štítku", "pach pri tvári je cítiť silnejšie"),
            ("Plachta", "oddeliť od suchej bielizne", "škvrna sa môže roztiahnuť do mapy"),
            ("Prikrývka alebo výplň", "nepremačať, ak nie je prateľná", "dlhé schnutie zhorší pach"),
            ("Matracový chránič", "prať samostatne a dosušiť úplne", "vlhkosť uzavretá pod plachtou zapácha"),
        ])}
        <h2>Ako vyprať kakao bez hnedého tieňa</h2>
        <p>Najprv odstráňte prebytok kakaa savou utierkou. Ak je škvrna mokrá, netrite ju do strán. Preplachujte studenšou vodou zo zadnej strany, aby sa zvyšky dostávali von z textilu. Potom použite malé množstvo pracieho roztoku na miesto škvrny a nechajte ho chvíľu pôsobiť podľa citlivosti materiálu.</p>
        <p>Pri bielej posteľnej bielizni sa hnedý tieň ukáže rýchlo, pri farebnom pyžame môže byť menej viditeľný, ale pach zostane. Pred sušením preto kontrolujte aj vôňu. Ak je cítiť mliečny alebo kyslastý tón, škvrna ešte nie je vyriešená.</p>
        <h2>Čo ak kakao zaschlo cez noc</h2>
        <p>Zaschnuté kakao najprv jemne navlhčite studenšou vodou. Nezoškrabujte ho silno, aby ste nepoškodili vlákna alebo potlač. Po uvoľnení pokračujte lokálnym predčistením. Pri detskom pyžame s potlačou postupujte opatrne, pretože intenzívne trenie môže poškodiť obrázok skôr než škvrnu.</p>
        <p>Ak je zasiahnutá posteľná bielizeň, perte ju s dostatočným priestorom v bubne. Veľké kusy sa v práčke ľahko stočia a škvrna ostane vo vnútri balíka. Pomôže menšia dávka a dôkladný oplach.</p>
        <div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">
        <h2 style="margin-top: 0;">Kontrola pred uložením do skrine</h2>
        <p>Posteľnú bielizeň po kakaovej škvrne ukladajte až úplne suchú. Ak je látka v mieste škvrny čo i len mierne vlhká alebo kyslastá, v skrini sa pach zvýrazní.</p>
        </div>
        <h2>Ako chrániť posteľ pri deťoch</h2>
        <p>Ak dieťa často pije kakao v posteli, praktický je prateľný matracový chránič a jednoduché pravidlo: mokré pyžamo a obliečky nečakajú v koši. Čím kratšie ostane kakao v textile, tým menšia šanca na kyslý pach a hnedú mapu.</p>
        <p>Pri opakovaných škvrnách skontrolujte aj dávkovanie pracieho prostriedku. Priveľa gélu nemusí zlepšiť výsledok; môže iba sťažiť oplach a textil pôsobí po vysušení ťažšie.</p>
        {product_card("laundry")}
        <h2>FAQ: kakao na pyžame a obliečkach</h2>
        <h3>Mám použiť teplú vodu?</h3>
        <p>Začnite radšej studenšou vodou. Kakao obsahuje mliečnu časť, ktorú horúci postup môže zbytočne zafixovať.</p>
        <h3>Prečo obliečky po praní stále cítiť?</h3>
        <p>Pravdepodobne zostal mliečny zvyšok alebo textil pomaly schol. Skúste predčistenie, menšiu dávku v bubne a rýchle sušenie.</p>
        <h3>Dá sa kakao vyprať aj z bielej bielizne?</h3>
        <p>Áno, ale treba konať skôr, kontrolovať tieň pred sušením a neopierať sa iba o silnejšiu vôňu.</p>
        """
    ),
    "socks_shoes": clean(
        f"""
        <h2>Prečo ponožky a športová obuv zapáchajú inak než tričko</h2>
        <p>Chodidlá sa potia, ponožka je zatvorená v topánke a obuv často schne pomalšie než oblečenie. Vzniká kombinácia vlhkosti, tepla, potu, kožných zvyškov a nedostatočného vetrania. Ponožky síce môžete vyprať, ale ak ich po tréningu necháte vo vlhkej obuvi alebo v zatvorenej taške, pach sa vráti veľmi rýchlo.</p>
        <p>Pri obuvi je dôležité rozlíšiť, čo sa dá prať a čo sa má iba čistiť a sušiť. Nie každá športová topánka patrí do práčky. Lepší výsledok často prinesie vybratie vložiek, dôkladné vysušenie, čistenie povrchu a pranie ponožiek naruby.</p>
        {note_card("Rýchle rozhodnutie po tréningu", [
            "<strong>Ponožky:</strong> otočiť naruby, nenechať vlhké v taške a prať s dobrým oplachom.",
            "<strong>Vložky:</strong> vybrať a sušiť oddelene, ak to konštrukcia umožňuje.",
            "<strong>Topánky:</strong> najprv vetrať a sušiť, až potom riešiť vôňu.",
            "<strong>Opakovaný pach:</strong> skontrolovať aj športovú tašku a práčku."
        ])}
        <h2>Diagnostika zápachu podľa miesta</h2>
        {table(["Kde je pach", "Čo to zvyčajne znamená", "Praktický krok"], [
            ("Ponožky po praní", "zvyšky potu alebo slabý oplach", "prať naruby a nepreplniť bubon"),
            ("Vložky do topánok", "vlhkosť uzavretá vo vnútri", "vybrať, vyvetrať a sušiť samostatne"),
            ("Celé topánky", "pach v materiáli alebo výplni", "neprať naslepo, skontrolovať odporúčanie výrobcu"),
            ("Športová taška", "prenáša pach späť na čisté veci", "vetrať a čistiť pravidelne"),
            ("Kôš na bielizeň", "vlhké ponožky čakajú príliš dlho", "nechať preschnúť alebo prať skôr"),
        ])}
        <h2>Ako prať ponožky po tréningu</h2>
        <p>Ponožky otočte naruby, aby sa lepšie vyplavili zvyšky potu a kože z vnútornej strany. Ak sú veľmi mokré, nenechávajte ich zrolované. Pred praním ich nechajte preschnúť alebo ich perte čo najskôr. Pri hrubších športových ponožkách pomôže menšia dávka v bubne a primerané množstvo pracieho gélu.</p>
        <p>Ak sú ponožky po praní tvrdé, lepkavé alebo príliš parfumované, problém môže byť v dávkovaní. Viac produktu neznamená čistejšie ponožky. Pri pachu je často dôležitejší priestor v bubne, oplach a rýchle sušenie.</p>
        <h2>Ako odstrániť pach zo športovej obuvi</h2>
        <p>Topánky po tréningu otvorte, vyberte vložky a nechajte ich schnúť mimo tašky. Ak ich dáte rovno do skrine, vlhkosť ostane vo vnútri a pach sa zhorší. Povrch čistite podľa materiálu: textil, sieťovina, koža, syntetika a lepené časti nemajú rovnakú odolnosť.</p>
        <p>Pranie celej obuvi v práčke je rizikové, ak to výrobca neodporúča. Môže poškodiť lepenie, tvar alebo tlmenie. Pri opakovanom pachu je bezpečnejšie čistiť vložky, vetrať topánky a striedať páry, aby mali čas úplne preschnúť.</p>
        <div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">
        <h2 style="margin-top: 0;">Rutina pre športovú tašku</h2>
        <p>Taška, v ktorej ostávajú vlhké ponožky, legíny a uterák, sa sama stáva zdrojom pachu. Po tréningu ju otvorte, vyberte mokré veci a občas vyčistite aj vnútorné vrecká.</p>
        </div>
        <h2>Ako zabrániť návratu pachu</h2>
        <p>Najlepšia prevencia je jednoduchá: nenechať vlhké veci zatvorené. Ponožky perte naruby, topánky sušte otvorené a čisté ponožky neskladujte v zapáchajúcej obuvi. Pri častom tréningu pomáha mať viac párov topánok aj ponožiek, aby každé stihli preschnúť.</p>
        <p>Vôňa môže byť príjemný doplnok, ale až po odstránení vlhkosti a potu. Ak je topánka stále vlhká, vôňa sa zmieša so zdrojom pachu a výsledok bude ťažší.</p>
        {product_card("laundry")}
        <h2>FAQ: ponožky a športová obuv</h2>
        <h3>Prečo ponožky zapáchajú aj po praní?</h3>
        <p>Často ostali zvyšky potu vo vnútornej strane alebo bol slabý oplach. Perte naruby, v menšej dávke a nenechávajte ich dlho vlhké.</p>
        <h3>Môžem dať športové topánky do práčky?</h3>
        <p>Iba ak to povoľuje výrobca. Pri mnohých topánkach je bezpečnejšie čistiť povrch, vložky a sušiť ich oddelene.</p>
        <h3>Ako rýchlo odstrániť pach po tréningu?</h3>
        <p>Vyberte ponožky, otvorte topánky, vyberte vložky a nechajte všetko preschnúť. Až potom riešte vôňu.</p>
        """
    ),
    "running_leggings": clean(
        f"""
        <h2>Prečo bežecké legíny držia pach po tréningu</h2>
        <p>Bežecké legíny bývajú z pružnej syntetiky, často s elastanom. Sú tesne na tele, zachytávajú pot, kožný maz a zvyšky krémov, no zároveň sa často po tréningu zrolujú do tašky. Pach sa potom drží najmä v miestach, kde je látka najviac v kontakte s telom: pás, rozkrok, vnútorné stehná a zadná časť kolien.</p>
        <p>Problém nie je vždy v tom, že legíny sú nekvalitné. Často ide o kombináciu odkladaného prania, preplneného bubna, priveľa gélu, aviváže a pomalého sušenia. Pri syntetike sa pach môže vrátiť až pri ďalšom behu, keď sa látka zahreje na tele.</p>
        {note_card("Rýchly postup po behu", [
            "<strong>Hneď po tréningu:</strong> legíny nechať preschnúť alebo ich vyprať čo najskôr.",
            "<strong>Pred praním:</strong> otočiť naruby, aby sa čistila vnútorná strana.",
            "<strong>Pri praní:</strong> nepreplniť bubon a dávkovať primerane.",
            "<strong>Po praní:</strong> sušiť vzdušne, nie zrolované v kúpeľni."
        ])}
        <h2>Diagnostika zápachu pri legínach</h2>
        {table(["Prejav", "Pravdepodobná príčina", "Čo zmeniť"], [
            ("Pach sa vráti pri behu", "zvyšky potu v syntetike", "prať naruby a pridať lepší oplach"),
            ("Legíny sú lepkavé", "priveľa gélu alebo slabé vypláchnutie", "znížiť dávku a nepreplniť bubon"),
            ("Pach je zatuchnutý", "dlhé čakanie vo vlhku", "nechať preschnúť alebo prať skôr"),
            ("Látka stratila pružnosť", "teplo, sušička alebo nevhodná aviváž", "sušiť vzdušne a riadiť sa štítkom"),
            ("Vôňa je príliš ťažká", "prekrytie namiesto čistenia", "najprv odstrániť pot a zvyšky produktu"),
        ])}
        <h2>Ako prať bežecké legíny krok za krokom</h2>
        <p>Legíny perte naruby a oddelene od uterákov, riflí a vecí so zipsami. Elastan a jemnejšia syntetika nemajú rady trenie ani vysoké teplo. Použite primeranú dávku pracieho gélu a program, ktorý dá textilu dosť času na vypláchnutie. Krátky program po náročnom behu nemusí stačiť, najmä ak periete viac športových vecí naraz.</p>
        <p>Aviváž pri športovej syntetike radšej vynechajte, ak ju výrobca výslovne neodporúča. Môže zanechať film, zhoršiť pocit z materiálu a prispieť k návratu pachu. Vôňu pridávajte opatrne až vtedy, keď legíny po praní naozaj pôsobia čisto.</p>
        <h2>Čo robiť, keď legíny zapáchajú aj po praní</h2>
        <p>Najprv skontrolujte, či problém nie je v práčke. Zatuchnuté tesnenie, zásobník alebo bubon môže preniesť pach na športové veci. Potom znížte dávkovanie pracieho prostriedku a perte menšiu dávku. Ak sú legíny veľmi spotené, nenechávajte ich v koši viac dní.</p>
        <p>Pri opakovanom pachu je lepšie upraviť proces než pridávať viac vône. V praxi často pomôže kombinácia: naruby, menšia dávka v bubne, primeraný prací gél, žiadna aviváž, lepší oplach a rýchle sušenie.</p>
        <div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">
        <h2 style="margin-top: 0;">Kontrola pred ďalším behom</h2>
        <p>Legíny ovoňajte po úplnom vysušení aj po krátkom zahriatí v rukách. Ak sa pach vracia už vtedy, pri behu sa zvýrazní. V takom prípade riešte pranie, nie iba parfumáciu.</p>
        </div>
        <h2>Ako predĺžiť životnosť bežeckých legín</h2>
        <p>Legíny nesušte na radiátore a nedávajte ich do horúcej sušičky, ak to štítok neumožňuje. Elastan a lepené alebo potlačené časti môžu teplom trpieť. Po praní ich vyrovnajte a sušte voľne, aby nezostali vlhké v záhyboch.</p>
        <p>Ak beháte často, striedajte viac kusov. Jeden pár, ktorý sa stále perie, suší a znovu nosí bez oddychu, sa opotrebuje rýchlejšie. Správna rutina znižuje pach a zároveň chráni pružnosť.</p>
        {product_card("laundry")}
        <h2>FAQ: bežecké legíny a zápach</h2>
        <h3>Prečo legíny zapáchajú až pri nosení?</h3>
        <p>Teplo tela a nový pot môžu aktivovať zvyšky, ktoré po praní neboli úplne odstránené.</p>
        <h3>Môžem použiť aviváž?</h3>
        <p>Pri športovej syntetike ju radšej vynechajte, ak ju neodporúča výrobca. Môže zanechať film a zhoršiť funkčnosť materiálu.</p>
        <h3>Ako často prať bežecké legíny?</h3>
        <p>Po spotenom tréningu čo najskôr. Ak ich nemôžete prať hneď, nechajte ich aspoň preschnúť mimo tašky.</p>
        """
    ),
}


TOP_UPS = {
    "travel_pillow": clean(
        f"""
        <h2>Ako rozlíšiť pach z poťahu, výplne a batožiny</h2>
        <p>Pri cestovnom vankúši sa často rieši iba poťah, ale pach môže prichádzať z troch miest. Prvé je poťah, ktorý je v kontakte s pokožkou. Druhé je výplň, ktorá mohla nasiaknuť vlhkosť alebo vôňu kozmetiky. Tretie je batožina: kufor, vrecko alebo batoh, kde sa vankúš dotýkal topánok, jedla, použitého oblečenia alebo vlhkého uteráka.</p>
        <p>Jednoduchý test je nechať poťah a výplň oddelene vetrať. Ak poťah po praní vonia čisto, ale výplň je stále cítiť, ďalšie pranie poťahu nepomôže. Ak je cítiť hlavne obal alebo vrecko, treba vyčistiť aj to, v čom vankúš cestuje. Inak sa pach prenesie späť na čistý textil.</p>
        {table(["Zdroj pachu", "Ako ho spoznáte", "Čo spraviť"], [
            ("Poťah", "pach je najmä pri švoch, zipsovej časti a krku", "vyprať poťah a skontrolovať úplné vysušenie"),
            ("Výplň", "pach ostáva aj bez poťahu", "vetrať dlhšie a čistiť iba lokálne"),
            ("Cestovné vrecko", "čistý vankúš zapácha po uložení", "vyčistiť alebo vyprať obal"),
            ("Kufor", "pach je podobný ako v batožine", "vetrať kufor a oddeliť textílie"),
            ("Kozmetika", "vôňa je mastná, krémová alebo parfumová", "ošetriť miesto pri krku a vlasoch"),
        ])}
        <div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">
        <h2 style="margin-top: 0;">Kontrola pred ďalšou cestou</h2>
        <p>Vankúš pred zabalením ovoňajte samostatne, nie až v kufri. Ak je cítiť aj po vyvetraní, problém sa počas cesty zvýrazní, pretože bude znovu pri teple krku a tváre.</p>
        </div>
        <h2>Kedy vankúš radšej vymeniť</h2>
        <p>Ak je výplň deformovaná, dlhodobo zatuchnutá alebo sa po lokálnom čistení nevie úplne vysušiť, ďalšie domáce zásahy môžu byť horšie než výmena. Pri textíliách blízko tváre je dôležitá nielen vôňa, ale aj pocit čistoty, sucha a pohodlia.</p>
        """
    ),
    "milk_yogurt": clean(
        f"""
        <h2>Rozdiel medzi čerstvou, zaschnutou a opakovane pranou škvrnou</h2>
        <p>Čerstvé mlieko alebo jogurt sa rieši inak než škvrna, ktorá prešla práčkou a stále zapácha. Pri čerstvej škvrne ide hlavne o rýchle odstránenie zvyškov. Pri zaschnutej škvrne treba najprv zvyšky znovu uvoľniť. Pri opakovane pranej škvrne už môže byť problém v tom, že sa zvyšky spojili so zle vypláchnutým pracím prostriedkom alebo s pomalým sušením.</p>
        <p>Preto sa neoplatí robiť stále ten istý postup. Ak prvé pranie nepomohlo, ďalšia silnejšia dávka gélu nemusí byť riešenie. Skôr treba zmeniť predčistenie, veľkosť dávky v bubne a kontrolu pred sušením. Pri detskom oblečení je to dôležité aj preto, že škvrny od mlieka bývajú blízko krku, rukávov a zapínania, kde sa ľahko prehliadnu.</p>
        {table(["Stav škvrny", "Čo znamená", "Najlepší ďalší krok"], [
            ("Čerstvá", "zvyšky sú ešte voľné", "odsatie a studené prepláchnutie"),
            ("Zaschnutá", "bielkoviny a tuk sú vo vlákne", "navlhčiť, uvoľniť a až potom predčistiť"),
            ("Po praní stále cítiť", "zvyšok neodišiel alebo textil zle schol", "nepoužiť teplo a zopakovať lokálne ošetrenie"),
            ("Škvrna na deke", "riziko vlhkosti vo vnútri", "nepremačať a sušiť veľmi dôkladne"),
            ("Škvrna v koši", "pach sa môže preniesť", "oddeliť od ostatnej bielizne"),
        ])}
        <div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">
        <h2 style="margin-top: 0;">Praktická rutina pri deťoch</h2>
        <p>Ak sa mliečne škvrny opakujú denne, oplatí sa mať malé oplachové miesto alebo vedierko na okamžité predčistenie. Nejde o dokonalé pranie hneď, ale o to, aby zvyšky nekysli v uzavretom koši.</p>
        </div>
        <h2>Kedy skontrolovať aj práčku</h2>
        <p>Ak kyslo zapácha viac kusov naraz, problém nemusí byť iba v jednej škvrne. Skontrolujte tesnenie, zásobník a to, či bielizeň po praní nezostáva dlho v bubne. Mliečne zvyšky a vlhká práčka sú kombinácia, ktorá vie pokaziť aj inak správny postup.</p>
        """
    ),
    "cocoa_bedding": clean(
        f"""
        <h2>Kakao na detskej posteli: čo skontrolovať okrem obliečky</h2>
        <p>Keď sa kakao vyleje v posteli, viditeľná škvrna na obliečke je iba časť problému. Tekutina mohla prejsť cez obliečku na vankúš, plachtu, matracový chránič alebo pyžamo. Ak vyperiete iba vrchný kus, mliečny pach sa môže znovu objaviť z vrstvy pod ním. Preto najprv rozoberte posteľ a skontrolujte všetky textílie, ktoré boli pod miestom nehody.</p>
        <p>Pri detských posteliach sa škvrna často zmieša so slinami, potom alebo krémom. Výsledok môže byť kyslastý pach, hnedý tieň a lepkavý dotyk. Každý z týchto signálov znamená, že škvrna ešte nie je úplne odstránená. Pred sušením a uložením do skrine treba skontrolovať nielen farbu, ale aj dotyk a vôňu.</p>
        {table(["Vrstva postele", "Riziko po kakau", "Kontrola"], [
            ("Pyžamo", "mliečny pach pri krku a rukávoch", "prepláchnuť a prať naruby, ak treba"),
            ("Obliečka na vankúš", "pach blízko tváre", "kontrola pred sušením pri dennom svetle"),
            ("Plachta", "rozpitá mapa", "predčistenie väčšej plochy"),
            ("Matracový chránič", "vlhkosť pod plachtou", "prať samostatne a dosušiť úplne"),
            ("Matrac", "dlhé schnutie a kyslý pach", "čistiť iba povrch a nepremačať"),
        ])}
        <div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">
        <h2 style="margin-top: 0;">Večerné pravidlo</h2>
        <p>Ak sa kakao vyleje tesne pred spaním, aspoň rýchlo oddeľte mokré vrstvy a prepláchnite pyžamo alebo obliečku. Odloženie do rána často zmení jednoduchú škvrnu na mliečny zápach v celej posteľnej bielizni.</p>
        </div>
        <h2>Prečo nepomáha iba silnejšia vôňa</h2>
        <p>Kakao má mliečny základ, a preto sa pri zvyškovom pachu vôňa s problémom mieša. Výsledok môže pôsobiť sladko-kyslo a ťažko. Najprv odstráňte mliečny a kakaový zvyšok, potom perte normálne a až na čistý textil pridávajte vôňu v primeranej intenzite.</p>
        """
    ),
    "socks_shoes": clean(
        f"""
        <h2>Keď zapácha celý športový set, nielen ponožky</h2>
        <p>Po tréningu sa pach zvyčajne netýka iba ponožiek. V tej istej taške bývajú tričko, uterák, legíny, vložky do topánok a niekedy aj mokrá fľaša alebo sprchové veci. Ak jeden zdroj ostane vlhký, prenesie pach na čisté kusy. Preto má zmysel riešiť celý systém: nohy, ponožky, obuv, vložky a tašku.</p>
        <p>Pri opakovanom pachu si všimnite, kedy sa objaví. Ak ponožky zapáchajú hneď po praní, problém je v praní alebo sušení. Ak začnú zapáchať až po obutí, zdroj môže byť v topánke. Ak sú čisté veci cítiť už v taške, treba čistiť aj tašku a vrecká.</p>
        {table(["Kedy sa pach objaví", "Pravdepodobný zdroj", "Čo skontrolovať"], [
            ("Hneď po praní", "ponožky alebo práčka", "dávka gélu, oplach, tesnenie práčky"),
            ("Po obutí", "topánka alebo vložka", "vlhkosť vo vnútri a stav vložiek"),
            ("V športovej taške", "taška prenáša pach", "vnútorné vrecká a dno tašky"),
            ("Po sušení", "pomalé schnutie", "prúdenie vzduchu a rozloženie ponožiek"),
            ("Pri ďalšom tréningu", "zvyšky potu vo vlákne", "prať naruby a nepreplniť bubon"),
        ])}
        <div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">
        <h2 style="margin-top: 0;">Týždenná športová rutina</h2>
        <p>Raz týždenne vyberte z tašky všetko, nechajte ju otvorenú, vyvetrajte topánky a skontrolujte vložky. Ponožky perte naruby a neskladujte čisté páry v obuvi, ktorá ešte nie je suchá.</p>
        </div>
        <h2>Kedy riešiť aj materiál ponožiek</h2>
        <p>Hrubé bavlnené ponožky môžu držať vlhkosť, syntetické športové ponožky zas môžu držať pach potu, ak sa zle perú. Ak sa problém stále vracia, skúste porovnať rôzne materiály a sledujte, ktoré schnú rýchlejšie a ktoré po praní ostávajú sviežejšie.</p>
        """
    ),
    "running_leggings": clean(
        f"""
        <h2>Najrizikovejšie miesta na bežeckých legínach</h2>
        <p>Pri legínach sa pach nekoncentruje rovnomerne. Najviac sa drží tam, kde je látka tesne na tele, kde sa tvorí pot a kde sa textil počas behu ohýba. Preto pri kontrole nestačí ovoňať vonkajšiu stranu. Dôležitá je vnútorná strana pásu, rozkrok, vnútorné švy a zadná časť kolien.</p>
        <p>Ak tieto miesta po praní ostanú cítiť, ďalšia vôňa problém len prekryje. Lepšie je prať naruby, znížiť veľkosť dávky a skontrolovať, či prací prostriedok nezostáva v pružnom materiáli. Pri legínach s vysokým podielom elastanu je zároveň dôležité nepreháňať teplo pri sušení.</p>
        {table(["Miesto na legínach", "Prečo drží pach", "Čo pomáha"], [
            ("Pás", "pot a kontakt s pokožkou", "prať naruby a dobre opláchnuť"),
            ("Rozkrok", "teplo, vlhkosť a trenie", "neodkladať vlhké, sušiť rýchlo"),
            ("Vnútorné stehná", "trením sa držia zvyšky potu", "menšia dávka v bubne"),
            ("Zadná časť kolien", "záhyby schnú pomalšie", "vyrovnať pri sušení"),
            ("Vrecko na mobil", "pot a uzavretý šev", "skontrolovať pred praním aj po ňom"),
        ])}
        <div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">
        <h2 style="margin-top: 0;">Pranie po dlhom behu</h2>
        <p>Po dlhom alebo horúcom behu neberte legíny ako mierne nosené oblečenie. Potrebujú viac priestoru v bubne a dôkladnejší oplach než veci, ktoré ste mali na sebe len krátko.</p>
        </div>
        <h2>Ako kombinovať čistotu a príjemnú vôňu</h2>
        <p>Pri športových legínach má vôňa fungovať ako jemný záver, nie ako hlavný čistiaci nástroj. Ak textil po praní stále zapácha, najprv upravte dávkovanie, oplach a sušenie. Až keď legíny pôsobia neutrálne čisto, má zmysel pridať jemnú vôňu, ktorá nebude pri behu príliš ťažká.</p>
        """
    ),
}


FINAL_TOUCHUPS = {
    "travel_pillow": clean(
        """
        <h2>Malý cestovný režim po návrate domov</h2>
        <p>Najlepší čas na čistenie cestovného vankúša je hneď po návrate, nie pred ďalšou cestou. Vtedy ešte viete, či bol vankúš v lietadle, vlaku, aute alebo v hoteli vystavený potu, jedlu, parfumu či vlhkosti. Ak ho odložíte na mesiac do skrine, pach sa stabilizuje a bude ťažšie rozlíšiť, či pochádza z poťahu, výplne alebo batožiny.</p>
        <p>Praktická rutina je jednoduchá: vybrať vankúš z kufra, oddeliť poťah, poťah vyprať, výplň vetrať a obal nechať otvorený. Až keď je všetko suché, môžete vankúš uložiť. Pri častom cestovaní pomáha aj tenká prateľná návliečka navyše, ktorá zachytí najviac kontaktu s krkom a vlasmi.</p>
        <div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">
        <h2 style="margin-top: 0;">Jedna veta na zapamätanie</h2>
        <p>Cestovný vankúš najprv vyčistite a vysušte, až potom jemne prevoňajte. Pri texte pri tvári je čistota dôležitejšia než silná aróma.</p>
        </div>
        """
    ),
    "milk_yogurt": clean(
        """
        <h2>Kontrola po druhom praní</h2>
        <p>Ak textil po druhom praní stále zapácha, zmeňte pohľad z jednej škvrny na celý proces. Skontrolujte, či sa kus pral s preplnenou dávkou, či sa po praní rýchlo vybral z práčky a či schol na vzdušnom mieste. Kyslý pach sa často drží v kombinácii vlhkosti a zvyškov vo vlákne, nie iba v samotnej pôvodnej škvrne.</p>
        <p>Pri detskom oblečení si všímajte aj zapínanie, švy a vrstvené časti látky. Mlieko sa vie dostať do miest, ktoré sa pri rýchlej kontrole prehliadnu. Ak ide o body, podbradník alebo pyžamo, je lepšie predčistiť menšiu plochu navyše, než po vysušení zistiť, že pach ostal pri leme.</p>
        <div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">
        <h2 style="margin-top: 0;">Jedna veta na zapamätanie</h2>
        <p>Mliečne škvrny najprv uvoľnite studenším postupom, potom perte a až po odstránení kyslého zdroja riešte príjemnú vôňu.</p>
        </div>
        """
    ),
    "cocoa_bedding": clean(
        """
        <h2>Kontrola bielizne po úplnom vysušení</h2>
        <p>Kakao môže po praní vyzerať vyriešené, kým je látka ešte vlhká. Po úplnom vysušení sa však môže ukázať hnedý tieň, lepkavejší dotyk alebo slabý mliečny pach. Preto bielizeň po praní neukladajte automaticky do skrine. Skontrolujte ju na dennom svetle a ovoňajte miesto, kde bola škvrna najvýraznejšia.</p>
        <p>Ak ide o detskú posteľnú bielizeň, kontrolujte aj vankúš a matracový chránič. Deti sa počas spánku hýbu a škvrna sa často prenesie na viac vrstiev. Keď ostane vlhkosť alebo mliečny zvyšok v spodnej vrstve, čistá obliečka sa môže pri ďalšom použití znovu napáchnuť.</p>
        <div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">
        <h2 style="margin-top: 0;">Jedna veta na zapamätanie</h2>
        <p>Pri kakau riešte naraz mlieko, cukor aj farbu. Najprv studenšie predčistenie, potom pranie a až nakoniec vôňa.</p>
        </div>
        """
    ),
    "socks_shoes": clean(
        """
        <h2>Kontrola po tréningovom týždni</h2>
        <p>Ak trénujete viackrát týždenne, sledujte, či sa pach hromadí postupne. Jeden tréning nemusí byť problém, ale opakované vlhké ponožky v rovnakej taške, rovnakých topánkach a rovnakej skrini vytvoria uzavretý cyklus. Ponožky sa vyperú, ale topánky alebo taška im pach vrátia.</p>
        <p>Pomáha jednoduchá rotácia. Striedajte ponožky aj topánky, vyberajte vložky a po tréningu nechajte veci rozložené. Ak sa pach drží v topánkach, čisté ponožky ho nezachránia. Ak sa drží v ponožkách, čisté topánky budú po tréningu opäť cítiť.</p>
        <div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">
        <h2 style="margin-top: 0;">Jedna veta na zapamätanie</h2>
        <p>Ponožky perte, topánky sušte a tašku vetrajte. Zápach po tréningu je systémový problém, nie iba problém jedného páru ponožiek.</p>
        </div>
        """
    ),
    "running_leggings": clean(
        """
        <h2>Kontrola rutiny po troch behoch</h2>
        <p>Ak sa pach vracia, otestujte jednu zmenu naraz. Najprv perte legíny naruby a v menšej dávke. Potom skúste pridať dôkladnejší oplach. Nakoniec upravte sušenie tak, aby legíny neostali zložené alebo zavesené cez seba v nevetranej kúpeľni. Tak zistíte, ktorá časť rutiny robí najväčší rozdiel.</p>
        <p>Pri bežeckých legínach sa oplatí sledovať aj opotrebovanie. Ak látka stráca pružnosť, stenčuje sa alebo zostáva trvalo cítiť v konkrétnych miestach, môže byť za hranou bežnej domácej obnovy. Správne pranie predlžuje životnosť, ale nenahradí materiál, ktorý už dlhodobo drží pot a vlhkosť.</p>
        <div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">
        <h2 style="margin-top: 0;">Jedna veta na zapamätanie</h2>
        <p>Pri legínach riešte vnútornú stranu látky, nie iba celkovú vôňu. Pach sa vracia najmä tam, kde sa drží pot a kde textil schne najpomalšie.</p>
        </div>
        """
    ),
}


LAST_PASS = {
    "travel_pillow": clean(
        """
        <h2>Praktický signál, že je vankúš pripravený na uloženie</h2>
        <p>Vankúš je pripravený na uloženie až vtedy, keď poťah aj výplň pôsobia neutrálne aj po niekoľkých hodinách v uzavretejšej miestnosti. Ak je vôňa svieža len hneď po vetraní, ale po chvíli sa vracia zatuchnutie, vankúš ešte potrebuje viac sucha alebo je zdroj pachu hlbšie vo výplni.</p>
        <p>Pri ďalšej ceste ho nebaľte medzi použité oblečenie. Samostatné priedušné vrecko zníži kontakt s pachmi z kufra a po návrate vám uľahčí rozhodnutie, čo treba vyprať a čo stačí vyvetrať.</p>
        """
    ),
    "milk_yogurt": clean(
        """
        <h2>Praktický signál, že je škvrna naozaj preč</h2>
        <p>Textil nestačí hodnotiť podľa toho, či nevidíte mapu. Pri mlieku a jogurte rozhoduje aj dotyk a vôňa po úplnom vysušení. Ak je miesto tvrdšie, mastnejšie alebo sa pri zahriatí v ruke objaví kyslý tón, zvyšky ešte ostali vo vlákne.</p>
        <p>V takom prípade postup nekomplikujte silnejším parfumovaním. Vráťte sa k lokálnemu predčisteniu, vyperte menšiu dávku a nechajte kus schnúť voľne. Až neutrálny, čistý textil má zmysel jemne prevoňať.</p>
        """
    ),
    "cocoa_bedding": clean(
        """
        <h2>Praktický signál, že posteľná bielizeň je bezpečne čistá</h2>
        <p>Obliečky a pyžamo sú po kakau v poriadku vtedy, keď miesto škvrny nelepí, nemá hnedý tieň a necítiť mliečny pach ani po úplnom vysušení. Pri bielej bielizni kontrolujte aj svetlo pri okne, pri farebnej najmä dotyk a vôňu.</p>
        <p>Ak sa pach vracia až v skrini, problém môže byť v tom, že bielizeň bola uložená mierne vlhká. Po škvrnách s mliekom nechajte posteľné textílie schnúť radšej dlhšie a ukladajte ich až úplne suché.</p>
        """
    ),
    "socks_shoes": clean(
        """
        <h2>Praktický signál, že problém je v topánkach</h2>
        <p>Ak čisté ponožky začnú zapáchať krátko po obutí, zdroj je pravdepodobne v topánke alebo vo vložke. Vtedy ďalšie pranie ponožiek pomôže len krátkodobo. Topánky treba otvoriť, vetrať, vysušiť a podľa materiálu vyčistiť aj vnútro.</p>
        <p>Pri častom športe si všímajte aj to, či topánky stihnú preschnúť medzi tréningami. Jeden pár používaný každý deň môže zostať vlhký vnútri, aj keď povrch vyzerá suchý. Striedanie párov je často účinnejšie než ďalšie parfumovanie.</p>
        """
    ),
    "running_leggings": clean(
        """
        <h2>Praktický signál, že legíny potrebujú zmenu rutiny</h2>
        <p>Ak legíny po vypratí voňajú čisto, ale po desiatich minútach behu sa vráti starý pach, vo vlákne alebo v švoch ostali zvyšky potu. Vtedy nepomôže iba silnejšia vôňa. Potrebujete lepšie prepranie vnútornej strany a dôkladnejší oplach.</p>
        <p>Skúste jednu dávku oprať samostatnejšie, naruby a bez aviváže. Ak je rozdiel výrazný, problém bol v procese. Ak nie, môže ísť o dlhodobo zanesený alebo opotrebovaný materiál, ktorý už drží pach viac než nové legíny.</p>
        """
    ),
}


LAST_EXTRA = {
    "milk_yogurt": clean(
        """
        <h2>Krátky domáci test pri opakovanom zápachu</h2>
        <p>Navlhčite len malé miesto škvrny čistou vodou a po pár minútach ho ovoňajte. Ak sa kyslý tón vráti, problém je stále vo vlákne. Ak sa nevráti, sledujte skôr sušenie, kôš na bielizeň alebo samotnú práčku.</p>
        """
    ),
    "cocoa_bedding": clean(
        """
        <h2>Krátky domáci test pri kakaovej škvrne</h2>
        <p>Po vysušení prejdite miesto prstami. Ak je látka lepkavejšia alebo tuhšia než okolie, cukor alebo mliečna časť ešte úplne neodišli. Vtedy je lepšie zopakovať jemné predčistenie než uložiť bielizeň do skrine.</p>
        """
    ),
    "socks_shoes": clean(
        """
        <h2>Krátky domáci test pri športovej obuvi</h2>
        <p>Vložte do topánky čistú suchú ponožku na niekoľko hodín bez nosenia. Ak začne zapáchať, zdroj je v topánke alebo vložke. Ak ostane neutrálna, problém vzniká najmä pri potení a treba zlepšiť rutinu po tréningu.</p>
        """
    ),
}


LAST_TINY = {
    "cocoa_bedding": clean(
        """
        <h2>Mini kontrola na záver</h2>
        <p>Ak miesto po kakau vyzerá aj vonia rovnako ako zvyšok bielizne, môžete ho považovať za bezpečne vyriešené.</p>
        """
    ),
}


MARKERS = {
    "travel_pillow": "Prečo cestovný vankúš po lete zapácha",
    "milk_yogurt": "Prečo mlieko a jogurt kysnú v textile",
    "cocoa_bedding": "Prečo je kakao na pyžame a obliečkach zložitejšie",
    "socks_shoes": "Prečo ponožky a športová obuv zapáchajú inak než tričko",
    "running_leggings": "Prečo bežecké legíny držia pach po tréningu",
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
    final_touchup = FINAL_TOUCHUPS.get(key)
    if final_touchup:
        final_marker = re.search(r"<h2>(.*?)</h2>", final_touchup).group(1)
        if final_marker not in updated:
            index = insertion_index(updated)
            updated = updated[:index].rstrip() + "\n" + final_touchup + "\n" + updated[index:].lstrip()
    last_pass = LAST_PASS.get(key)
    if last_pass:
        last_marker = re.search(r"<h2>(.*?)</h2>", last_pass).group(1)
        if last_marker not in updated:
            index = insertion_index(updated)
            updated = updated[:index].rstrip() + "\n" + last_pass + "\n" + updated[index:].lstrip()
    last_extra = LAST_EXTRA.get(key)
    if last_extra:
        extra_marker = re.search(r"<h2>(.*?)</h2>", last_extra).group(1)
        if extra_marker not in updated:
            index = insertion_index(updated)
            updated = updated[:index].rstrip() + "\n" + last_extra + "\n" + updated[index:].lstrip()
    last_tiny = LAST_TINY.get(key)
    if last_tiny:
        tiny_marker = re.search(r"<h2>(.*?)</h2>", last_tiny).group(1)
        if tiny_marker not in updated:
            index = insertion_index(updated)
            updated = updated[:index].rstrip() + "\n" + last_tiny + "\n" + updated[index:].lstrip()
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
    parser = argparse.ArgumentParser(description="Conservatively expand VEVO retrofit wave 07 odor articles.")
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
            article["long"] = insert_expansion(article["long"], config["expansion"])
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
                "wave": "retrofit-wave-07-odor-five",
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
                "wave": "retrofit-wave-07-odor-five",
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
