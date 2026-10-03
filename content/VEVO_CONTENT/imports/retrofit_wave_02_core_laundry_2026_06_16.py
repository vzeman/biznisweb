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
OUT_JSON = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-02-core-laundry-2026-06-16.json"
MCP_RESULTS = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-02-core-laundry-2026-06-16-mcp-results.json"


ARTICLES = [
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-15-2026-06-09-articles.json",
        "slug": "ako-vycistit-filter-pracky-ked-bielizen-zapacha-alebo-voda-odteka-pomaly",
        "post_id": "2143",
        "url": "https://www.vevo.sk/n/ako-vycistit-filter-pracky-ked-bielizen-zapacha-alebo-voda-odteka-pomaly",
        "title": "Ako vyčistiť filter práčky, keď bielizeň zapácha alebo voda odteká pomaly",
        "expansion": "filter",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-15-2026-06-09-articles.json",
        "slug": "pustila-farba-v-pracke-co-urobit-s-bielym-trickom-a-ruzovou-bieliznou",
        "post_id": "2134",
        "url": "https://www.vevo.sk/n/pustila-farba-v-pracke-co-urobit-s-bielym-trickom-a-ruzovou-bieliznou",
        "title": "Pustila farba v práčke: čo urobiť s bielym tričkom a ružovou bielizňou",
        "expansion": "color_bleed",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-25-2026-06-16-articles.json",
        "slug": "ako-citat-stitok-na-obleceni-material-symboly-prania-a-spravny-program",
        "post_id": "2250",
        "url": "https://www.vevo.sk/n/ako-citat-stitok-na-obleceni-material-symboly-prania-a-spravny-program",
        "title": "Ako čítať štítok na oblečení: materiál, symboly prania a správny program",
        "expansion": "care_label",
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


def card(title, bullets):
    items = "".join(f"<li>{item}</li>" for item in bullets)
    return clean(
        f"""
        <div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">
        <h2 style="margin-top: 0;">{title}</h2>
        <ul>{items}</ul>
        </div>
        """
    )


def product_card(product_title, product_href, product_text, category_title, category_href, category_text):
    return clean(
        f"""
        <div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 24px 0; background: #f7fbf8;">
        <h2 style="margin-top: 0;">Praktické odporúčanie k téme</h2>
        <p>{category_text}</p>
        <div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff; margin: 14px 0;">
        <h3 style="margin-top: 0;">{product_title}</h3>
        <p>{product_text}</p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="{product_href}">Pozrieť produkt</a></p>
        </div>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="{category_href}">{category_title}</a></p>
        </div>
        """
    )


EXPANSIONS = {
    "filter": clean(
        f"""
        <h2>Prečo filter ovplyvňuje zápach aj odtok vody</h2>
        <p>Filter práčky zachytáva drobnosti, vlákna, vlasy, nite, mince, sponky a iné zvyšky, ktoré by sa nemali dostať ďalej do odtokového systému. Keď sa v ňom drží vlhký nános, voda odteká pomalšie a bielizeň môže po praní pôsobiť zatuchnuto. Nie je to problém, ktorý vyrieši silnejšia vôňa do prania. Najprv treba skontrolovať mechanickú príčinu.</p>
        <p>Pri zápachu po praní sa často rieši bubon, zásobník alebo tesnenie, ale filter je rovnako dôležitý. Ak je v ňom starý nános alebo zvyšky textilu, zápach sa môže vracať aj po tom, čo vyvetráte práčku. Preto dáva zmysel kontrolovať filter najmä po praní uterákov, pelechov, koberčekov, pracovných vecí alebo textílií, ktoré púšťajú veľa vlákien.</p>
        {card("Rýchla diagnostika filtra", [
            "<strong>Voda odteká pomaly:</strong> skontrolujte filter a odtok podľa návodu výrobcu.",
            "<strong>Bielizeň zapácha po praní:</strong> filter môže držať vlhkosť, vlákna alebo staré zvyšky.",
            "<strong>Po praní ostávajú v bubne drobnosti:</strong> pozrite tesnenie aj filter.",
            "<strong>Práčka hlási chybu odtoku:</strong> filter neotvárajte silou, postupujte podľa manuálu."
        ])}
        <h2>Čo nájdete vo filtri a čo to znamená</h2>
        {table(["Nález vo filtri", "Čo môže spôsobovať", "Prvý bezpečný krok"], [
            ("Vlákna, nite a chlpy", "pomalší odtok a zatuchnutý pach", "vybrať zvyšky, vyčistiť okolie filtra a nechať práčku vetrať"),
            ("Mince, sponky alebo drobné predmety", "hluk, blokovanie alebo riziko poškodenia", "opatrne odstrániť, skontrolovať tesnenie a vrecká pri ďalšom praní"),
            ("Mazľavý povlak", "zvyšky pracieho prostriedku, aviváže a vlhkosti", "filter očistiť a skontrolovať dávkovanie prostriedku"),
            ("Zápach po otvorení", "dlhodobo vlhké zvyšky", "vyčistiť filter, zásobník a tesnenie ako jeden systém"),
        ])}
        <h2>Ako otvárať filter bez zbytočného rizika</h2>
        <p>Filter neotvárajte narýchlo hneď po programe, ak neviete, koľko vody môže vytiecť. Pripravte si nízku nádobu, starý uterák a postupujte pomaly. Niektoré práčky majú pri filtri vypúšťaciu hadičku, iné nie. Preto je dôležité držať sa návodu konkrétneho modelu.</p>
        <p>Ak filter nejde povoliť, nepoužívajte hrubú silu. Dôvodom môže byť tlak vody, zaseknutý predmet alebo konštrukcia krytu. Vtedy je bezpečnejšie skontrolovať manuál alebo zavolať servis, než poškodiť závit alebo tesnenie. Zle zatvorený filter môže neskôr tiecť.</p>
        <h2>Kedy čistiť filter preventívne</h2>
        {table(["Domácnosť alebo pranie", "Odporúčaná kontrola", "Prečo"], [
            ("Bežné pranie párkrát týždenne", "približne raz mesačne", "vlhkosť a vlákna sa hromadia aj pri bežnom režime"),
            ("Domácnosť so zvieraťom", "po pelechoch alebo textíliách plných srsti", "chlpy a vlákna sa ľahko zachytia v tesnení aj filtri"),
            ("Časté pranie uterákov", "každé 2 až 4 týždne", "uteráky púšťajú vlákna a držia vlhkosť"),
            ("Pracovné veci, koberčeky, deky", "po náročnej dávke", "hrubšie nečistoty môžu zhoršiť odtok"),
        ])}
        <h2>Prečo nestačí pridať viac pracieho gélu alebo vône</h2>
        <p>Ak je problém vo filtri, väčšia dávka pracieho gélu alebo výraznejšia vôňa nepomôže. Naopak, priveľa prostriedku môže vytvárať ďalšie zvyšky, ktoré sa horšie vypláchnu. Pri zápachu po praní je lepšie postupovať odzadu: najprv skontrolovať odtok, filter, tesnenie a zásobník, až potom riešiť vôňu bielizne.</p>
        <p>Dôležitý je aj poriadok pred praním. Vyprázdnite vrecká, pelechy a deky pred praním vytraste, pracovné veci od blata najprv zbavte hrubej špiny. Filter je poistka, nie miesto, kam má pravidelne končiť všetko, čo sa dalo odstrániť pred vložením do práčky.</p>
        <h2>Kontrola po vyčistení filtra</h2>
        <p>Po vyčistení filter pevne vráťte späť a skontrolujte, či okolie netečie. Pri najbližšom praní sledujte, či voda odteká normálne a či bielizeň po programe nezapácha. Ak sa problém vráti, skontrolujte aj hadicu, zásobník a tesnenie. Filter je častá príčina, ale nie jediná.</p>
        <p>Ak sa v domácnosti často perú uteráky, šport, pracovné veci alebo pelechy, nastavte si jednoduchú rutinu: raz mesačne filter, zásobník a tesnenie. Tak sa zápach rieši skôr, než sa dostane do bielizne.</p>
        {product_card(
            "Vevo Shot - koncentrát na čistenie práčky 100ml",
            "/p-1549/vevo-shot-koncentrat-na-cistenie-pracky",
            "Hodí sa ako následná údržba práčky po ručnom vyčistení filtra, zásobníka a tesnenia, keď sa vracia zápach alebo povlak v práčke.",
            "Pozrieť kategóriu detox práčky",
            "/c/vevo-home-care/pranie/detox-pracky",
            "Ak filter ukazuje, že práčka drží vlhkosť a zvyšky, riešte údržbu celého spotrebiča, nielen vôňu bielizne."
        )}
        """
    ),
    "color_bleed": clean(
        f"""
        <h2>Prečo je dôležité konať, kým je bielizeň mokrá</h2>
        <p>Keď pustí farba v práčke, najhoršie je zafarbenú bielizeň usušiť. Teplo zo sušičky, radiátora alebo silného slnka môže pigment viac zafixovať do vlákien. Preto je prvý krok jednoduchý: bielizeň nechajte mokrú, nájdite kus, ktorý pustil farbu, a zvyšok rozdeľte podľa materiálu a farby.</p>
        <p>Inak sa rieši bavlnené biele tričko, inak jemná blúzka, uterák, detské body alebo zmesový materiál s elastanom. Agresívne odfarbovanie bez kontroly štítku môže poškodiť vlákna alebo zmeniť odtieň ešte viac. Cieľ nie je urobiť najtvrdší zásah, ale zvoliť postup, ktorý má šancu pomôcť bez ďalšej škody.</p>
        {card("Prvá pomoc pri zafarbenej bielizni", [
            "<strong>Nesušiť:</strong> zafarbené kúsky nechajte mokré, kým sa rozhodnete pre ďalší krok.",
            "<strong>Oddeliť zdroj:</strong> nájdite farebný kus, ktorý pustil pigment.",
            "<strong>Triediť podľa materiálu:</strong> bavlna znesie viac než vlna, viskóza alebo elastan.",
            "<strong>Čítať štítok:</strong> nie každý biely textil môže ísť do bielidla alebo horúcej vody."
        ])}
        <h2>Diagnostika podľa stavu bielizne</h2>
        {table(["Stav po praní", "Čo to znamená", "Najbezpečnejší prvý krok"], [
            ("Biele tričko je jemne ružové", "pravdepodobne slabý prenos pigmentu", "nechať mokré a skúsiť opakované pranie podľa štítku"),
            ("Bielizeň má mapy alebo fľaky", "farba sa nepreniesla rovnomerne", "neriešiť sušením, rozdeliť kúsky a postupovať po materiáloch"),
            ("Farebný kus stále púšťa", "pigment nie je stabilný", "prať ho samostatne a overiť púšťanie farby pred ďalším praním"),
            ("Jemná látka zmenila odtieň", "riziko poškodenia pri agresívnom zásahu", "zvoliť šetrný postup alebo profesionálne čistenie"),
        ])}
        <h2>Čo robiť pri bielom tričku a čo pri zmiešanej dávke</h2>
        <p>Pri bielom bavlnenom tričku máte zvyčajne väčší priestor na opakované pranie alebo vhodné odfarbenie podľa štítku. Pri zmesi materiálov, potlači, čipke alebo elastane postupujte opatrnejšie. Aj keď je textil biely, nemusí zniesť vysokú teplotu alebo silný prostriedok.</p>
        <p>Pri zmiešanej dávke nikdy neriešte všetky kusy rovnako. Najprv oddeľte biele, svetlé, farebné a jemné. Každá skupina môže potrebovať iný postup. Ak farebný kus pustil farbu, nevracajte ho späť k ostatnej bielizni, kým neoveríte, či pigment nepúšťa opakovane.</p>
        <h2>Čomu sa vyhnúť pri zafarbenej bielizni</h2>
        {table(["Chyba", "Prečo je riziková", "Lepšia voľba"], [
            ("Sušenie v sušičke", "teplo môže pigment zafixovať", "nechať textil mokrý a riešiť ho hneď"),
            ("Silné bielidlo na všetko", "môže poškodiť jemné alebo zmesové vlákna", "najprv skontrolovať štítok a materiál"),
            ("Pranie celej dávky znova spolu", "farba sa môže preniesť na ďalšie kusy", "rozdeliť bielizeň podľa farby a materiálu"),
            ("Ignorovanie zdroja farby", "problém sa zopakuje pri ďalšom praní", "nájsť kus, ktorý pustil farbu, a prať ho oddelene"),
        ])}
        <h2>Prevencia: ako znížiť riziko pustenia farby</h2>
        <p>Nové sýte oblečenie perte prvýkrát samostatne alebo s podobnými farbami. Rizikové sú najmä červené, tmavomodré, čierne a výrazne sýte kúsky. Pred prvým praním pomôže jednoduchý test: navlhčite nenápadné miesto bielou handričkou a pozrite sa, či sa farba prenáša.</p>
        <p>Dôležité je aj nepreplniť práčku. Keď sa bielizeň nemôže voľne pohybovať, farba a nečistoty sa horšie riedia vo vode a môžu sa zachytiť na iných kusoch. Pri nových farbách voľte primeranú dávku, vhodnú teplotu a neperte ich hneď s bielymi uterákmi alebo obliečkami.</p>
        <h2>Kedy už zafarbenie neriešiť doma</h2>
        <p>Ak ide o drahý, jemný alebo konštrukčne citlivý kus, domáce experimenty môžu narobiť viac škody. Pri hodvábe, vlne, viskóze, potlači, čipke alebo saku je lepšie postupovať konzervatívne. Ak si nie ste istí, či textil znesie ďalší zásah, overte štítok alebo zvážte profesionálne čistenie.</p>
        <p>Pri bielych uterákoch alebo bavlnených tričkách býva šanca na zlepšenie vyššia než pri jemnom oblečení. Aj tam však platí, že jeden silný zásah nie je vždy najlepší. Často je bezpečnejšie opakovať šetrnejší postup, než riskovať poškodenie vlákien.</p>
        {product_card(
            "Prací gél hypoalergénny z Marseillského mydla 1L",
            "/p-1551/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l",
            "Dáva zmysel pri následnom šetrnom praní podľa štítku, keď už je zafarbenie roztriedené podľa materiálu a nechcete textil zbytočne preťažiť.",
            "Pozrieť kategóriu pracie gély",
            "/c/vevo-home-care/pranie/praci-gel",
            "Pri zafarbenej bielizni je produkt až druhý krok. Najprv zastavte sušenie, rozdeľte materiály a overte štítok."
        )}
        <h2>Súvisiace témy k pustenej farbe</h2>
        <ul><li><a href="/n/ako-zabranit-pustaniu-farby-pri-prani-noveho-oblecenia">Ako zabrániť púšťaniu farby pri praní nového oblečenia</a></li><li><a href="/n/ako-prat-nove-oblecenie-pred-prvym-nosenim">Ako prať nové oblečenie pred prvým nosením</a></li><li><a href="/n/ako-prat-cierne-oblecenie-aby-nevybledlo">Ako prať čierne oblečenie, aby nevybledlo</a></li></ul>
        """
    ),
    "care_label": clean(
        f"""
        <h2>Štítok berte ako hranicu, nie ako slepý návod na každé pranie</h2>
        <p>Údaj na štítku často ukazuje maximálny bezpečný limit, nie povinnosť prať vždy na najvyššej povolenej teplote. Ak je tričko nosené krátko a nie je silno znečistené, často stačí šetrnejší program. Ak je textil spotený, mastný alebo znečistený, treba zohľadniť aj reálny stav oblečenia, nielen samotný symbol.</p>
        <p>Najspoľahlivejší postup je čítať štítok ako celok: materiál, symbol prania, symbol sušenia, žehlenie, zákaz bielenia a konštrukcia výrobku. Pri zmesových materiáloch sa riaďte najcitlivejšou zložkou, nie tou, ktorej je v látke najviac.</p>
        {card("Ako čítať štítok bez zbytočných omylov", [
            "<strong>Najprv materiál:</strong> bavlna, vlna, viskóza, polyester alebo elastan sa správajú rozdielne.",
            "<strong>Potom symboly:</strong> pranie, bielenie, sušenie a žehlenie spolu tvoria jeden návod.",
            "<strong>Potom stav textilu:</strong> pot, mastnota, blato alebo zápach môžu meniť voľbu programu.",
            "<strong>Nakoniec konštrukcia:</strong> potlač, guma, výstuž, zips alebo čipka môžu byť citlivejšie než látka."
        ])}
        <h2>Najčastejšie kombinácie materiálu a programu</h2>
        {table(["Materiál alebo detail", "Čo sledovať na štítku", "Praktická opatrnosť"], [
            ("Bavlna", "teplota, sušenie a zrážanie", "nové kusy perte opatrnejšie, najmä pri sýtej farbe"),
            ("Polyester a športové textílie", "nižšia teplota, zákaz aviváže pri funkčných úpravách", "riešte zápach dávkovaním, oplachom a sušením"),
            ("Vlna alebo jemná viskóza", "ručné alebo veľmi šetrné pranie", "nekrútiť, nesušiť horúcim vzduchom"),
            ("Elastan, guma, potlač", "nižšia teplota a jemnejšia mechanika", "teplo a agresívne prostriedky môžu urýchliť opotrebovanie"),
        ])}
        <h2>Čo robiť, keď je štítok odstrihnutý alebo nečitateľný</h2>
        <p>Ak štítok chýba, postupujte podľa najopatrnejšieho rozumného scenára. Pozrite sa na pružnosť, hrúbku, potlač, podšívku a švy. Ak si nie ste istí, perte na nižšej teplote, s menšími otáčkami a s podobnými farbami. Pri drahom alebo citlivom kuse je bezpečnejšie ručné či profesionálne čistenie.</p>
        <p>Pri odstrihnutom štítku nepoužívajte automaticky program podľa podobného kusu zo skrine. Dve čierne tričká môžu mať úplne iné zloženie: jedno bavlnené, druhé viskózové s elastanom. Rovnaká farba nie je rovnaký materiál.</p>
        <h2>Diagnostická tabuľka: keď si nie ste istí programom</h2>
        {table(["Otázka pred praním", "Ak je odpoveď áno", "Bezpečnejší postup"], [
            ("Je v textile vlna, viskóza alebo jemná podšívka?", "materiál môže meniť tvar", "nižšia teplota, jemný program, slabšie odstreďovanie"),
            ("Má oblečenie potlač, gumu alebo elastan?", "detaily môžu byť citlivejšie než látka", "nepoužiť horúcu vodu ani sušičku bez overenia"),
            ("Je kus nový a sýto farebný?", "môže púšťať farbu", "prať samostatne alebo s podobnými farbami"),
            ("Je oblečenie silno spotené alebo mastné?", "šetrný program nemusí stačiť", "najprv riešiť škvrnu a dávkovanie, nie iba teplotu"),
        ])}
        <h2>Štítok, prací prostriedok a aviváž</h2>
        <p>Štítok často priamo nehovorí, koľko pracieho gélu použiť, ale nepriamo určuje, aký šetrný má byť celý proces. Pri jemných materiáloch a funkčnom oblečení dávajte pozor aj na aviváž. Niektoré textílie môžu po aviváži stratiť savosť, priedušnosť alebo pôvodný pocit na dotyk.</p>
        <p>Ak neviete, či je aviváž vhodná, začnite bez nej a sledujte výsledok. Vôňa a mäkkosť nemajú byť dôležitejšie než funkcia materiálu. Pri uterákoch, športe, softshelli alebo detskej bielizni je opatrnosť často lepšia než automatické nalievanie aviváže do každej dávky.</p>
        <h2>Kedy sa riadiť symbolmi prania podrobnejšie</h2>
        <p>Pri drahších kusoch, oblečení s membránou, vlne, saku, kabáte, zmesových materiáloch a detskom oblečení sa oplatí ísť symbol po symbole. Ak si nie ste istí významom značiek, použite samostatný sprievodca <a href="/n/symboly-prania-kompletny-sprievodca-praciim-stitkom">symbolmi prania</a> a až potom vyberte program.</p>
        <p>Pri bežných tričkách stačí často rýchla kontrola materiálu, teploty a sušenia. Pri citlivých veciach je však práve štítok rozdiel medzi tým, či oblečenie vydrží sezóny, alebo sa zrazí, vyťahá či stratí povrch po prvom praní.</p>
        {product_card(
            "Prací gél hypoalergénny z Marseillského mydla 1L",
            "/p-1551/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l",
            "Univerzálnejší prací gél má zmysel pri bežnom praní podľa štítku, keď nechcete textil zbytočne preťažovať agresívnym postupom.",
            "Pozrieť kategóriu pracie gély",
            "/c/vevo-home-care/pranie/praci-gel",
            "Pri výbere programu podľa štítku myslite aj na vhodný prací prostriedok, dávkovanie a to, či je aviváž pre daný materiál vôbec vhodná."
        )}
        """
    ),
}


TOP_UP = {
    "filter": clean(
        """
        <h2>Keď sa zápach po vyčistení filtra vráti</h2>
        <p>Ak sa zápach vráti po jednom alebo dvoch praniach, filter bol pravdepodobne iba jedna časť problému. V takom prípade skontrolujte aj zásobník na prací prostriedok, gumové tesnenie pri dvierkach a spôsob sušenia bielizne. Vlhké zvyšky sa v práčke správajú ako systém: filter môže držať vlákna, zásobník nánosy gélu a tesnenie vodu so špinou.</p>
        <p>Pri opakovanom zápachu si všímajte aj to, čo periete najčastejšie. Uteráky, športové oblečenie, pelechy, koberčeky a pracovné textílie zaťažujú práčku viac než bežné tričká. Ak sa tieto dávky perú často, nestačí vyčistiť filter raz za niekoľko mesiacov. Lepšia je krátka pravidelná kontrola, ktorá zabráni tomu, aby sa problém dostal do každej ďalšej dávky.</p>
        <h2>Rozdiel medzi pomalým odtokom a zápachom</h2>
        <p>Pomalý odtok je často viditeľný hneď: program trvá dlhšie, v bubne ostáva voda alebo práčka ukáže chybu. Zápach je zradnejší, pretože sa môže prejaviť až po vybratí bielizne alebo po vysušení. Preto je dobré pri zápachu myslieť nielen na vôňu textilu, ale aj na cestu vody cez práčku.</p>
        <p>Ak voda neodchádza plynulo, bielizeň sa horšie oplachuje. Zvyšky pracieho prostriedku, vlákna a nečistoty potom môžu ostať v textile alebo v práčke. Výsledkom je bielizeň, ktorá síce prešla programom, ale nepôsobí sviežo. Filter je v takom prípade prvé miesto, ktoré má zmysel skontrolovať.</p>
        <h2>Praktický mesačný režim</h2>
        <ol><li>Vyberte a skontrolujte filter podľa návodu výrobcu.</li><li>Utrite okolie filtra a kryt, aby tam neostala vlhkosť.</li><li>Skontrolujte spodnú časť tesnenia pri dvierkach.</li><li>Vyberte zásobník a pozrite, či v ňom nie je povlak.</li><li>Po údržbe nechajte dvierka aj zásobník pootvorené.</li></ol>
        <p>Tento režim nie je potrebné robiť komplikovane. Dôležité je, aby sa práčka pravidelne vysušila a aby vlhké zvyšky nezostávali zatvorené v tmavých miestach. Tak sa znižuje riziko, že budete problém riešiť až vtedy, keď bielizeň po praní zapácha.</p>
        """
    ),
    "color_bleed": clean(
        """
        <h2>Keď je zafarbenie slabé, stredné alebo výrazné</h2>
        <p>Slabé zafarbenie býva najčastejšie ružový, sivý alebo modrastý nádych na bielej bielizni. Vtedy má zmysel konať rýchlo, ale šetrne. Kým je textil mokrý, opakované pranie podľa štítku môže časť pigmentu uvoľniť. Pri strednom zafarbení, teda pri viditeľných mapách alebo farebných šmuhách, už treba postupovať podľa materiálu a zvážiť, či textil znesie silnejší zásah.</p>
        <p>Výrazné zafarbenie je situácia, keď bielizeň zmenila farbu natoľko, že sa blíži odtieňu kusu, ktorý pustil pigment. Tu sú domáce možnosti obmedzené. Pri bavlne môže byť šanca vyššia, pri jemných látkach, potlači a elastane je riziko poškodenia väčšie. Preto sa neoplatí naslepo kombinovať horúcu vodu, bielidlo a dlhý program.</p>
        <h2>Ako postupovať pri rôznych typoch bielej bielizne</h2>
        <p>Biele tričko, uterák a blúzka nie sú rovnaký problém. Tričko môže mať potlač alebo elastan, uterák znesie iný režim a jemná blúzka sa môže pri agresívnom zásahu zdeformovať. Pred ďalším praním sa preto pozrite nielen na farbu, ale aj na zloženie a konštrukciu.</p>
        <p>Pri uterákoch býva dôležité aj to, či už neobsahujú veľa aviváže alebo zvyškov pracieho prostriedku. Tie môžu zhoršiť oplach a pocit z textilu. Pri tričkách sledujte švy, potlač a pružnosť. Pri obliečkach riešte veľkosť dávky, aby sa textil mohol voľne pohybovať a pigment sa ďalej neusádzal v záhyboch.</p>
        <h2>Kontrola pred ďalším praním</h2>
        <ol><li>Vyberte z práčky kus, ktorý pravdepodobne pustil farbu.</li><li>Ostatnú bielizeň rozdeľte na bielu bavlnu, jemné veci a farebné kusy.</li><li>Skontrolujte štítky a vylúčte textil, ktorý neznesie silnejší program.</li><li>Nesušte nič, čo chcete ešte zachraňovať.</li><li>Po opakovanom praní skontrolujte výsledok pri dennom svetle, nie iba v kúpeľni.</li></ol>
        <p>Prevencia je pri púšťaní farby jednoduchšia než oprava. Nový červený uterák, tmavé džínsy alebo sýta mikina by nemali ísť do prvej dávky s bielou bielizňou. Ak si nie ste istí, perte nový sýty kus samostatne a až potom ho zaraďte k podobným farbám.</p>
        """
    ),
    "care_label": clean(
        """
        <h2>Príklady rozhodovania podľa štítku v bežnej domácnosti</h2>
        <p>Ak máte bavlnené tričko s potlačou, nestačí pozrieť iba na bavlnu. Potlač môže byť citlivejšia než samotná látka, preto je vhodnejší nižší teplotný režim, pranie naruby a opatrné sušenie. Pri športovom tričku z polyesteru zas nevyberajte program len podľa toho, že je tmavé. Dôležitá je funkčná úprava, zápach, pot a to, či výrobca neodporúča vyhnúť sa aviváži.</p>
        <p>Pri posteľnej bielizni často rozhoduje kombinácia materiálu a rozmeru. Bavlnené obliečky znesú viac než jemná viskózová zmes, ale aj pri bavlne môže sušenie v príliš horúcej sušičke zhoršiť krčenie. Pri uterákoch zas symboly čítajte spolu so savosťou. Ak chcete, aby uterák dobre sal, aviváž nemusí byť najlepšia voľba, aj keď štítok priamo nerieši každý detail dávkovania.</p>
        <h2>Keď sa štítok bije s realitou</h2>
        <p>Niekedy štítok povoľuje teplotu, ktorá sa vám zdá príliš vysoká pre bežné nosenie. Vtedy môžete zvoliť nižšiu teplotu, ak nejde o hygienicky náročnú dávku alebo silné znečistenie. Inokedy je oblečenie zapáchajúce alebo mastné, ale štítok povoľuje iba šetrný režim. Vtedy pomáha predčistenie, menšia dávka, dostatočný oplach a dôkladné sušenie.</p>
        <p>Štítok teda nie je izolovaný príkaz. Je to bezpečnostný rámec, v ktorom hľadáte najlepší kompromis medzi čistotou, ochranou materiálu a reálnym stavom textilu. Pri neistote začnite jemnejšie. Pridať opatrný druhý krok je zvyčajne bezpečnejšie než poškodiť oblečenie hneď prvým praním.</p>
        <h2>Mini postup pred prvým praním nového kúsku</h2>
        <ol><li>Prečítajte zloženie materiálu a nájdite najcitlivejšiu zložku.</li><li>Skontrolujte symbol prania, sušenia a zákaz bielenia.</li><li>Pri sýtej farbe perte prvýkrát oddelene alebo s podobnými farbami.</li><li>Pri potlači perte naruby a nepoužívajte zbytočne vysokú teplotu.</li><li>Po praní skontrolujte tvar, farbu a povrch, aby ste vedeli upraviť ďalší režim.</li></ol>
        <p>Takýto postup predlžuje životnosť oblečenia a znižuje riziko, že sa budete spätne pýtať, prečo sa tričko zrazilo, prečo pustila farba alebo prečo športový materiál prestal fungovať tak ako na začiatku.</p>
        """
    ),
}


FINAL_TOP_UP = {
    "filter": clean(
        """
        <h2>Kedy filter nestačí riešiť samostatne</h2>
        <p>Ak práčka po vyčistení filtra stále zle odteká, nepokračujte ďalšími náhodnými zásahmi. Problém môže byť v odtokovej hadici, čerpadle, nesprávnom napojení odpadu alebo v predmete, ktorý sa nedá bezpečne vybrať cez filter. Vtedy je lepšie zastaviť sa a postupovať podľa návodu alebo servisu.</p>
        <p>Rovnako platí, že ak po zatvorení filtra uniká voda, nejde už o bežnú údržbu. Skontrolujte, či je filter správne dosadený, či na závite nie je nečistota a či tesnenie neostalo posunuté. Ak si nie ste istí, nespúšťajte ďalšie pranie s plnou dávkou bielizne. Najprv overte tesnosť na krátkom programe alebo podľa odporúčania výrobcu.</p>
        <p>Pri zápachu je dobré po vyčistení filtra vyprať jednu bežnú dávku a sledovať výsledok. Ak bielizeň vonia normálne a voda odteká plynulo, išlo pravdepodobne o zanesený filter. Ak sa zatuchnutie vráti, pokračujte kontrolou zásobníka, tesnenia a pravidelnej údržby práčky.</p>
        """
    ),
    "color_bleed": clean(
        """
        <h2>Ako hodnotiť výsledok po záchrannom praní</h2>
        <p>Výsledok nekontrolujte hneď v tmavej kúpeľni ani pod žltým svetlom. Zafarbenie je najlepšie vidieť pri dennom svetle, keď je textil vlhký aj po čiastočnom preschnutí. Ak sa odtieň výrazne zlepšil, pokračujte opatrne podľa štítku. Ak sa nezmenil vôbec, ďalší rovnaký cyklus pravdepodobne neprinesie veľký rozdiel.</p>
        <p>Pri bielych veciach si všímajte, či je zafarbenie plošné alebo mapovité. Plošný jemný nádych sa niekedy dá zmierniť ľahšie než ostré mapy pigmentu. Pri mapách je dôležité nešúchať jedno miesto príliš silno, najmä na úplete, viskóze alebo elastane. Mechanické poškodenie môže byť viditeľnejšie než samotný farebný nádych.</p>
        <p>Ak zafarbený kus po záchrannom praní necháte v koši mokrý niekoľko hodín, riskujete zatuchnutie. Po zvolenom postupe preto bielizeň buď ďalej riešte, alebo ju nechajte šetrne vyschnúť až vtedy, keď ste sa rozhodli, že ďalší zásah už robiť nebudete.</p>
        """
    ),
}


FINAL_EXTRA = {
    "filter": clean(
        """
        <h2>Kontrola po jednom skúšobnom programe</h2>
        <p>Po čistení filtra je rozumné neskočiť hneď na veľkú dávku posteľnej bielizne alebo uterákov. Najprv sledujte jeden kratší program s menšou dávkou alebo podľa možností práčky. Všímajte si, či voda odteká plynulo, či sa pri filtri neobjaví vlhkosť a či po otvorení dvierok necítiť zatuchnutie. Táto krátka kontrola vám ukáže, či bol problém naozaj vo filtri alebo treba pokračovať v širšej údržbe.</p>
        """
    ),
}


def naturalize_existing_public_text(long):
    long = re.sub(
        r"<p>Téma pokrýva (.*?)\. Najhoršie je",
        r"<p>V článku riešime praktické situácie ako \1. Najhoršie je",
        long,
    )
    return long


def insert_expansion(long, key):
    marker_by_key = {
        "filter": "Prečo filter ovplyvňuje zápach aj odtok vody",
        "color_bleed": "Prečo je dôležité konať, kým je bielizeň mokrá",
        "care_label": "Štítok berte ako hranicu, nie ako slepý návod na každé pranie",
    }
    top_marker_by_key = {
        "filter": "Praktický mesačný režim",
        "color_bleed": "Kontrola pred ďalším praním",
        "care_label": "Príklady rozhodovania podľa štítku",
    }
    final_marker_by_key = {
        "filter": "Kedy filter nestačí riešiť samostatne",
        "color_bleed": "Ako hodnotiť výsledok po záchrannom praní",
    }

    def insertion_index(start):
        sales_index = long.find('<div style="border: 1px solid #dbe5de', start)
        if sales_index != -1:
            return sales_index
        related_index = long.find("\n<h2>Súvisiace", start)
        if related_index != -1:
            return related_index
        faq_index = long.find("\n<h2>FAQ", start)
        if faq_index != -1:
            return faq_index
        return len(long)

    if marker_by_key[key] not in long:
        first_index = insertion_index(0)
        long = long[:first_index].rstrip() + "\n" + EXPANSIONS[key] + "\n" + long[first_index:].lstrip()

    start = long.find(marker_by_key[key])
    if top_marker_by_key[key] not in long:
        index = insertion_index(start)
        long = long[:index].rstrip() + "\n" + TOP_UP[key] + "\n" + long[index:].lstrip()

    if key in FINAL_TOP_UP and final_marker_by_key[key] not in long:
        start = long.find(top_marker_by_key[key]) if top_marker_by_key[key] in long else long.find(marker_by_key[key])
        index = insertion_index(start)
        long = long[:index].rstrip() + "\n" + FINAL_TOP_UP[key] + "\n" + long[index:].lstrip()

    if key in FINAL_EXTRA and "Kontrola po jednom skúšobnom programe" not in long:
        start = long.find(final_marker_by_key.get(key, top_marker_by_key[key]))
        index = insertion_index(start if start != -1 else long.find(marker_by_key[key]))
        long = long[:index].rstrip() + "\n" + FINAL_EXTRA[key] + "\n" + long[index:].lstrip()

    return long


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
    parser = argparse.ArgumentParser(description="Conservatively expand VEVO core laundry retrofit wave 02.")
    parser.add_argument("--update-live", action="store_true")
    parser.add_argument("--sleep", type=float, default=1.5)
    args = parser.parse_args()

    by_source = {}
    updates = []
    for config in ARTICLES:
        rows = by_source.setdefault(config["source"], json.loads(config["source"].read_text(encoding="utf-8")))
        for article in rows:
            if article.get("link") != config["slug"]:
                continue
            if article.get("title") != config["title"]:
                raise SystemExit(f"Title changed unexpectedly for {config['slug']}: {article.get('title')}")
            original_long = article["long"]
            original_short = article.get("short", "")
            article["long"] = insert_expansion(naturalize_existing_public_text(article["long"]), config["expansion"])
            if article.get("title") != config["title"] or article.get("link") != config["slug"] or article.get("short", "") != original_short:
                raise SystemExit(f"Retrofit attempted to change title, slug, or short field for {config['slug']}")
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
                    "title_preserved": True,
                    "slug_preserved": True,
                    "short_preserved": True,
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
                "wave": "retrofit-wave-02-core-laundry",
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
                "wave": "retrofit-wave-02-core-laundry",
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
