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
OUT_JSON = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-19-food-elastic-five-2026-06-17.json"
MCP_RESULTS = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-19-food-elastic-five-2026-06-17-mcp-results.json"


ARTICLES = [
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-16-2026-06-10-articles.json",
        "slug": "ako-vyprat-cierny-caj-z-bieleho-obrusu-bez-hnedych-map",
        "post_id": "2158",
        "url": "https://www.vevo.sk/n/ako-vyprat-cierny-caj-z-bieleho-obrusu-bez-hnedych-map",
        "topic": "black_tea",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-17-2026-06-10-articles.json",
        "slug": "ako-odstranit-sojovu-omacku-z-kosele-obrusu-a-prestierania",
        "post_id": "2174",
        "url": "https://www.vevo.sk/n/ako-odstranit-sojovu-omacku-z-kosele-obrusu-a-prestierania",
        "topic": "soy_sauce",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-17-2026-06-10-articles.json",
        "slug": "ako-odstranit-olivovy-olej-z-lanovej-kosele-bez-mastnej-mapy",
        "post_id": "2176",
        "url": "https://www.vevo.sk/n/ako-odstranit-olivovy-olej-z-lanovej-kosele-bez-mastnej-mapy",
        "topic": "olive_oil",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-15-2026-06-09-articles.json",
        "slug": "ako-vyprat-kari-a-kurkumu-z-bavlneneho-tricka-bez-zlteho-tiena",
        "post_id": "2135",
        "url": "https://www.vevo.sk/n/ako-vyprat-kari-a-kurkumu-z-bavlneneho-tricka-bez-zlteho-tiena",
        "topic": "curry_turmeric",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-18-2026-06-10-articles.json",
        "slug": "ako-prat-kompresne-pancuchy-a-elasticke-zdravotne-navleky",
        "post_id": "2203",
        "url": "https://www.vevo.sk/n/ako-prat-kompresne-pancuchy-a-elasticke-zdravotne-navleky",
        "topic": "compression",
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
        <p>Pri škvrnách aj citlivých elastických materiáloch je dôležité dávkovať rozumne, dobre oplachovať a nepreháňať mechaniku prania.</p>
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
    "black_tea": {
        "marker": "Detailnejší postup na čierny čaj, taníny a hnedé mapy na bielom obruse",
        "problem": "čierny čaj na bielom obruse zanecháva tanínovú škvrnu, ktorá môže po vyschnutí stmavnúť a pri teple sa zafixovať",
        "scope": "bielom obruse, látkovej servítke, prestieraní, bavlnenom tričku a kuchynskej utierke",
        "avoid": "sušičku, žehlenie a horúci zásah pred tým, než skontrolujete hnedý tieň pri dennom svetle",
        "diagnosis": [
            "<strong>Rýchlosť pomáha:</strong> čerstvý čaj sa rieši ľahšie než zaschnutá hnedá mapa.",
            "<strong>Najprv riediť, nie drhnúť:</strong> silné trenie rozšíri okraj škvrny.",
            "<strong>Mlieko mení postup:</strong> čaj s mliekom pridáva bielkovinu a tuk.",
            "<strong>Žehlenie až po kontrole:</strong> teplo zvyšok škvrny zafixuje.",
        ],
        "state_rows": [
            ("čerstvý čaj", "odsávať a opláchnuť chladnejšie", "bez trenia do strán"),
            ("zaschnutá mapa", "lokálne predčistiť a opakovať mierne", "nezažehliť"),
            ("čaj s mliekom", "riešiť aj bielkovinu a tuk", "nezačínať horúco"),
            ("biely obrus", "kontrola pred sušením", "tieň sa ukáže po preschnutí"),
        ],
        "textile_rows": [
            ("bavlnený obrus", "predčistiť lokálne a prať podľa štítku", "taníny sa držia vo vlákne"),
            ("ľanová servítka", "jemnejšia mechanika a tvarovanie", "ľan sa krčí a tvrdne"),
            ("kuchynská utierka", "prať oddelene pri výraznom znečistení", "drží pach a farbu"),
            ("farebné tričko", "testovať stálosť farby", "nevybieliť okolie škvrny"),
        ],
        "sections": [
            ("Ako vyprať čierny čaj z bieleho obrusu", "Čerstvú škvrnu najprv odsávajte čistou savou handričkou alebo papierom. Netrite ju do strán. Ak je obrus prateľný, miesto opláchnite skôr chladnejšou vodou z rubovej strany a potom lokálne predčistite podľa štítku.", "Pri bielom obruse je dôležitá kontrola pred žehlením. Mokrá látka môže vyzerať čisto, ale po preschnutí sa hnedý okraj môže vrátiť."),
            ("Čierny čaj s mliekom alebo cukrom", "Ak bol v čaji cukor, mlieko alebo citrón, škvrna nie je iba tanínová. Mlieko pridáva bielkovinu a tuk, cukor lepivosť a citrón môže ovplyvniť farbu textilu. Preto postupujte mierne a po vrstvách.", "Pri mliečnych zložkách pomáha nadviazať na článok <a href=\"/n/ako-odstranit-mlieko-a-jogurt-z-textilu-bez-kysleho-zapachu\">ako odstrániť mlieko a jogurt z textilu</a>."),
            ("Prečo sa hnedé mapy vracajú po praní", "Tanínové škvrny môžu po praní vyzerať slabšie, ale po vyschnutí alebo teple znovu vystúpia. Ak obrus vyžehlíte príliš skoro, zvyšok sa odstraňuje ťažšie. Preto kontrolujte pri dennom svetle a z viacerých uhlov.", "Ak tieň zostal, opakujte lokálne predčistenie skôr než zvýšite teplotu alebo použijete sušičku."),
            ("Čierny čaj na látkovej servítke a prestieraní", "Servítka má menšiu plochu, no škvrna býva koncentrovaná pri okraji alebo leme. Prestieranie zase často obsahuje švy, v ktorých sa čaj drží dlhšie. Podložte škvrnu a pracujte od okraja ku stredu.", "K starostlivosti o jedálenské textílie nadväzuje článok <a href=\"/n/ako-prat-latkove-obrusky-a-prestieranie\">ako prať látkové obrúsky a prestieranie</a>."),
            ("Čo urobiť po oslave alebo návšteve", "Po oslave obrus nenechávajte s čajom, vínom, mastnotou a omáčkami dlho v koši. Najprv označte škvrny, ktoré potrebujú predčistenie. Ak ich dáte do práčky bez kontroly, časť sa môže len rozšíriť alebo zoslabnúť bez úplného odstránenia.", "Pri viacerých škvrnách postupujte podľa najrizikovejšej, nie podľa najľahšej. Čaj a mastnota potrebujú rozdielny prístup."),
        ],
        "depth": [
            ("Taníny a prečo nestačí iba dlhší program", "Taníny sú prirodzene farbiace látky v čaji. Pri kontakte s textilom sa môžu viazať do vlákna a vytvoriť hnedastý tieň. Dlhší program bez predčistenia nemusí dostať farbivo z lokálneho miesta dostatočne rýchlo.", "Preto je účinný prvý krok: odsať, riediť a predčistiť skôr, než škvrna zaschne."),
            ("Biely obrus a optická kontrola", "Biela látka ukáže aj slabý zvyšok. Kontrolujte ju na dennom svetle, nie iba v kúpeľni pod teplým osvetlením. Hnedý okraj môže byť slabý, ale po žehlení bude viditeľnejší.", "Ak obrus používate na slávnostné stolovanie, oplatí sa byť konzervatívny a teplo pridať až po kontrole."),
        ],
        "expert_title": "Odbornejší pohľad: tanínová škvrna, teplo a čas",
        "expert_p1": "Tanínové škvrny patria medzi farebné škvrny, pri ktorých rozhoduje čas, riedenie a kontrola pred teplom. Čím dlhšie škvrna schne, tým viac sa farebný zvyšok prejaví vo vlákne. Preto má význam aj jednoduché včasné odsatie prebytku.",
        "rule": "Pri čiernom čaji najprv riediť a odsávať, potom lokálne predčistiť a až po kontrole sušiť alebo žehliť.",
        "recommendation_intro": "Pri čaji pomôže šetrný prací gél až po tom, čo škvrnu nezafixujete trením alebo teplom. Dôležité je najmä predčistenie a kontrola pred žehlením obrusu.",
        "product_text": "Vhodný na následné pranie obrusov, servítok a kuchynských textílií po lokálnom ošetrení čajovej škvrny.",
        "links": [
            ("/n/ako-prat-obrus-po-oslave-aby-nezostali-mastne-skvrny-a-pachy", "Ako prať obrus po oslave"),
            ("/n/ako-prat-latkove-obrusky-a-prestieranie", "Ako prať látkové obrúsky a prestieranie"),
            ("/n/ako-vyprat-stare-skvrny-kompletny-sprievodca-pre-ciste-oblecenie", "Ako vyprať staré škvrny"),
        ],
        "faq": [
            ("Prečo ostala po čaji hnedá mapa?", "Tanínový zvyšok sa neodstránil úplne alebo sa zafixoval teplom."),
            ("Môžem obrus hneď vyžehliť?", "Až po kontrole pri dennom svetle. Žehlenie môže zvyšok škvrny zafixovať."),
            ("Je čaj s mliekom iný problém?", "Áno. Okrem tanínu riešite aj bielkovinu a tuk z mlieka."),
        ],
    },
    "soy_sauce": {
        "marker": "Detailnejší postup na sójovú omáčku, soľ a tmavý pigment na textile",
        "problem": "sójová omáčka kombinuje tmavý pigment, soľ, fermentovaný pach a pri jedle často aj mastnotu z ďalších surovín",
        "scope": "košeli, obruse, prestieraní, tričku, kuchynskej utierke a látkovej servítke",
        "avoid": "nechať škvrnu zaschnúť v leme alebo ju pretrieť mastnou obrúskovou handričkou",
        "diagnosis": [
            "<strong>Najprv riediť:</strong> tmavý pigment sa musí dostať z vlákna skôr, než zaschne.",
            "<strong>Soľ drží vlhkosť:</strong> škvrna môže zanechať mapu aj po prvom praní.",
            "<strong>Jedlo pridáva mastnotu:</strong> sushi, omáčka alebo olej menia postup.",
            "<strong>Golier a manžeta sú riziko:</strong> košeľa má často viac vrstiev látky.",
        ],
        "state_rows": [
            ("čerstvá kvapka", "odsávať a oplachovať z rubu", "bez rozmazania"),
            ("tmavý okraj", "predčistiť lokálne", "kontrola pred sušením"),
            ("mastná kombinácia", "riešiť najprv mastnotu aj pigment", "neprať naslepo"),
            ("prestieranie so švami", "skontrolovať lem", "omáčka sa drží v okrajoch"),
        ],
        "textile_rows": [
            ("košeľa", "podložiť a pracovať od okraja ku stredu", "tenká látka sa prepije"),
            ("obrus", "predčistiť pred hlavným praním", "tmavý tieň sa ukáže po vyschnutí"),
            ("prestieranie", "prepláchnuť aj lem", "švy držia soľ a farbu"),
            ("kuchynská utierka", "prať podľa znečistenia oddelene", "pach jedla sa drží"),
        ],
        "sections": [
            ("Ako odstrániť sójovú omáčku z košele", "Košeľu najprv podložte savou vrstvou a prebytok odsajte. Ak je škvrna na hrudi, rukáve alebo manžete, nevtierajte ju hlbšie do vlákna. Oplachujte skôr z rubovej strany, aby sa pigment vytláčal von.", "Po lokálnom predčistení perte košeľu podľa štítku. Pred sušením skontrolujte, či nezostal tmavý okraj alebo slaný mapovitý tieň."),
            ("Sójová omáčka na obruse a prestieraní", "Na obruse sa sójová omáčka často stretne s olejom, ryžou, zeleninou alebo sladkou omáčkou. Najprv riešte tmavú tekutinu a potom prípadnú mastnú časť. Prestieranie skontrolujte aj v lemoch.", "K jedálenským textíliám nadväzuje článok <a href=\"/n/ako-prat-latkove-obrusky-a-prestieranie\">ako prať látkové obrúsky a prestieranie</a>."),
            ("Prečo nestačí škvrnu iba prevoňať alebo vyprať neskôr", "Sójová omáčka má vlastný pach a tmavú farbu. Ak zaschne, pigment aj soľ sa držia vo vlákne silnejšie. Prevoňanie problém len prekryje a oneskorené pranie môže nechať slabý hnedý tieň.", "Pri kuchynských škvrnách je rýchly prvý krok často dôležitejší než dlhý program neskôr."),
            ("Sójová omáčka a mastné jedlo", "Ak sa škvrna stala pri jedle s olejom alebo majonézou, nestačí riešiť len tmavú farbu. Mastnota môže po praní zostať ako priesvitná mapa. Vtedy kombinujte postup pre pigment aj tuk.", "Prakticky súvisí článok <a href=\"/n/ako-odstranit-majonezu-a-dressing-z-obrusu-bez-mastneho-flaku\">ako odstrániť majonézu a dressing z obrusu</a>."),
            ("Ako kontrolovať výsledok po praní", "Po praní nechajte miesto preschnúť alebo ho skontrolujte na svetle. Sójová omáčka môže zanechať slabší hnedý okraj, ktorý na mokrej látke nevidno. Ak je tam, nesušte horúco.", "Opakované mierne predčistenie je bezpečnejšie než vysoká teplota, najmä pri košeliach a obrusoch."),
        ],
        "depth": [
            ("Soľ, pigment a pach v jednej škvrne", "Sójová omáčka nie je len farebná voda. Soľ môže podporiť mapy, pigment zanechá tieň a fermentovaný charakter prinesie pach. Preto je dôležité rýchle riedenie a dobrý oplach.", "Ak sa škvrna kombinuje s olejom, postup sa musí prispôsobiť aj mastnote."),
            ("Prečo sa škvrna šíri do lemu", "Prestieranie, obrusy a košele majú lemy a švy, kde sa tekutina drží dlhšie. Ak ošetríte len stred škvrny, okraj môže po vyschnutí zostať tmavší.", "Pri oplachu preto myslite aj na rub a švy, nielen na viditeľnú plochu."),
        ],
        "expert_title": "Odbornejší pohľad: tmavé vodné škvrny s rozpustenými látkami",
        "expert_p1": "Pri tmavých omáčkach ide o farbivá a rozpustené látky vo vode, ktoré sa rýchlo dostanú do vlákna. Ak sa k nim pridá tuk z jedla, vzniká dvojitý problém: farebný tieň a mastná mapa. Pranie musí riešiť obe vrstvy.",
        "rule": "Pri sójovej omáčke najprv odsávať a riediť, potom skontrolovať mastnotu a až potom prať celý kus podľa štítku.",
        "recommendation_intro": "Pri sójovej omáčke má prací gél pomôcť až po tom, čo tmavú tekutinu nevtlačíte hlbšie do vlákna. Dôležitý je aj dobrý oplach švov a lemov.",
        "product_text": "Vhodný na následné pranie košieľ, obrusov a kuchynských textílií po lokálnom ošetrení sójovej omáčky.",
        "links": [
            ("/n/ako-odstranit-balzamikovy-ocot-z-bieleho-obrusu", "Ako odstrániť balzamikový ocot z bieleho obrusu"),
            ("/n/ako-odstranit-majonezu-a-dressing-z-obrusu-bez-mastneho-flaku", "Ako odstrániť majonézu a dressing z obrusu"),
            ("/n/ako-prat-obrus-po-oslave-aby-nezostali-mastne-skvrny-a-pachy", "Ako prať obrus po oslave"),
        ],
        "faq": [
            ("Ako rýchlo riešiť sójovú omáčku?", "Čím skôr. Odsajte prebytok, oplachujte z rubu a až potom predčistite."),
            ("Prečo ostal tmavý okraj?", "Pigment alebo soľ zostali v leme či väzbe látky. Pred sušením miesto znova ošetrite."),
            ("Čo ak bola škvrna aj mastná?", "Riešte aj mastnotu, nie iba tmavú farbu. Inak môže ostať priesvitná mapa."),
        ],
    },
    "olive_oil": {
        "marker": "Detailnejší postup na olivový olej, ľan a mastnú mapu na košeli",
        "problem": "olivový olej na ľanovej košeli vytvára mastnú mapu, ktorá môže na mokrej látke zmiznúť a po vyschnutí sa znovu ukázať",
        "scope": "ľanovej košeli, bavlnenom tričku, obruse, servítke, prestieraní a kuchynskej utierke",
        "avoid": "silné drhnutie ľanu, vysokú teplotu pred predčistením a sušenie pred kontrolou mastného tieňa",
        "diagnosis": [
            "<strong>Mastnota klame:</strong> mokrá látka môže vyzerať čisto, suchá ukáže mapu.",
            "<strong>Ľan je pevný, ale krčivý:</strong> neznáša zbytočné drhnutie a krútenie.",
            "<strong>Najprv odsať:</strong> čerstvý olej treba dostať z povrchu.",
            "<strong>Kontrola pred žehlením:</strong> teplo mastnotu zvýrazní alebo zafixuje.",
        ],
        "state_rows": [
            ("čerstvý olej", "odsávať papierom bez trenia", "nevtláčať do vlákna"),
            ("mastná mapa", "lokálne predčistiť gélom", "kontrola po preschnutí"),
            ("ľanová košeľa", "jemná mechanika a tvarovanie", "nekrútiť silou"),
            ("obrus po jedle", "oddeliť mastné miesta", "nežehliť pred kontrolou"),
        ],
        "textile_rows": [
            ("ľanová košeľa", "predčistiť lokálne a prať podľa štítku", "ľan drží tvar aj mapy"),
            ("bavlna", "riešiť mastnotu pred praním", "odolnejšia voľba"),
            ("obrus", "kontrola pred žehlením", "mastnota býva viditeľná po vysušení"),
            ("servítka", "podložiť a pracovať od okraja", "tuk sa šíri do väzby"),
        ],
        "sections": [
            ("Ako odstrániť olivový olej z ľanovej košele", "Čerstvý olej najprv odsajte papierovou utierkou alebo čistou savou handričkou. Netrite ho do strán. Ľan je síce pevný materiál, ale pri lokálnom drhnutí sa môže zmeniť povrch alebo vytvoriť svetlejší kruh.", "Po odsátí použite malé množstvo pracieho gélu na lokálne predčistenie a perte podľa štítku. Košeľu pred sušením skontrolujte pri dennom svetle."),
            ("Prečo olej po praní stále vidno", "Mastnota sa na mokrej látke často stratí z dohľadu. Po vyschnutí však svetlo ukáže priesvitnú mapu. Ak ju uvidíte, nežehlite a nedávajte košeľu do sušičky. Najprv zopakujte lokálne predčistenie.", "Pri obrusoch a servítkach platí rovnaké pravidlo: teplo až po kontrole."),
            ("Ľanová košeľa a šetrná mechanika", "Ľan sa prirodzene krčí a môže pôsobiť tvrdšie, ak sa zle vysuší alebo preperie v preplnenom bubne. Pri mastnej škvrne preto riešte len konkrétne miesto, nie celú košeľu agresívnejším režimom.", "K ľanu nadväzuje návod <a href=\"/n/ako-prat-lanovu-koselu-aby-nezostala-tvrda-a-pokrcena\">ako prať ľanovú košeľu</a>."),
            ("Olivový olej na obruse alebo servítke", "Pri stolovaní sa olivový olej často mieša s bylinkami, octom alebo omáčkou. Vtedy najprv odsajte mastnotu a potom skontrolujte, či nezostal aj farebný zvyšok. Lemy a švy servítok kontrolujte zvlášť.", "Ak bola na textile aj majonéza alebo dressing, pomôže článok <a href=\"/n/ako-odstranit-majonezu-a-dressing-z-obrusu-bez-mastneho-flaku\">ako odstrániť majonézu a dressing</a>."),
            ("Prevencia mastných máp pri varení", "Pri ľanových košeliach a zásterách pomáha rýchla kontrola po varení. Olej nenechávajte zaschnúť v koši na bielizeň. Čím skôr odoberiete prebytok, tým menšia je šanca na trvalú mapu.", "Ak sa mastné škvrny opakujú často, vytvorte si rutinu predprania kuchynských textílií a pracovných tričiek."),
        ],
        "depth": [
            ("Mastnota verzus farebná škvrna", "Olivový olej nemusí mať výraznú farbu, ale mení lom svetla v látke. Preto je mastná mapa viditeľná najmä po vyschnutí. Farebná škvrna potrebuje riešiť pigment, olej zase film na vlákne.", "Ak je v oleji paprika, pesto alebo bylinky, riešite aj farebnú časť."),
            ("Ako nepoškodiť ľan pri čistení", "Ľan znesie používanie, ale lokálne drhnutie jedného miesta môže zmeniť povrch. Pracujte skôr prikladaním, jemným zapracovaním gélu a dôkladným oplachom než tvrdou kefkou.", "Pri drahšej košeli je bezpečnejšie opakovať mierny postup než vytvoriť vydratý svetlý kruh."),
        ],
        "expert_title": "Odbornejší pohľad: olejový film a viditeľnosť mastnej mapy",
        "expert_p1": "Mastná škvrna funguje inak než pigmentová. Olej sa rozloží v štruktúre textilu a mení spôsob, akým látka odráža svetlo. Preto môže byť neviditeľný za mokra a výrazný po vyschnutí. Dobré predčistenie musí uvoľniť film, nie iba prevoňať textil.",
        "rule": "Pri olivovom oleji najprv odsať prebytok, potom lokálne uvoľniť mastnotu a až po kontrole sušiť alebo žehliť.",
        "recommendation_intro": "Pri mastných škvrnách je dôležité malé lokálne množstvo gélu a dobrý oplach. Viac produktu bez oplachu môže na ľane zanechať ďalší povlak.",
        "product_text": "Vhodný na následné pranie ľanových, bavlnených a kuchynských textílií po lokálnom predčistení olejovej škvrny.",
        "links": [
            ("/n/ako-odstranit-olejove-a-mastne-skvrny-z-oblecenia-po-prani", "Ako odstrániť olejové a mastné škvrny"),
            ("/n/ako-prat-lanovu-koselu-aby-nezostala-tvrda-a-pokrcena", "Ako prať ľanovú košeľu"),
            ("/n/ako-prat-obrus-po-oslave-aby-nezostali-mastne-skvrny-a-pachy", "Ako prať obrus po oslave"),
        ],
        "faq": [
            ("Prečo olej vidno až po vyschnutí?", "Mastnota mení vzhľad látky najmä za sucha. Mokrá látka vie problém skryť."),
            ("Môžem ľan drhnúť kefkou?", "Radšej jemne. Tvrdé drhnutie môže zmeniť povrch ľanu a vytvoriť svetlejší kruh."),
            ("Čo ak ostala mastná mapa?", "Nesušte horúco ani nežehlite. Zopakujte lokálne predčistenie a dobre opláchnite."),
        ],
    },
    "curry_turmeric": {
        "marker": "Detailnejší postup na kari, kurkumu a žltý tieň na bavlnenom tričku",
        "problem": "kari a kurkuma patria medzi výrazné pigmentové škvrny, ktoré sa pri teple, slnku alebo trení môžu správať inak než bežná omáčka",
        "scope": "bavlnenom tričku, detskej mikine, obruse, kuchynskej utierke, zástere a svetlej bavlne",
        "avoid": "drhnutie žltého pigmentu do strán, sušičku pred kontrolou a náhodné bielenie farebného textilu",
        "diagnosis": [
            "<strong>Pigment je silný:</strong> kurkuma farbí aj veľmi malé množstvo.",
            "<strong>Omáčka býva mastná:</strong> kari často rieši pigment aj tuk.",
            "<strong>Slnko môže meniť odtieň:</strong> nie je náhradou za predčistenie.",
            "<strong>Bavlna drží farbu:</strong> biely a svetlý úplet treba kontrolovať pred sušením.",
        ],
        "state_rows": [
            ("čerstvá omáčka", "odobrať prebytok bez trenia", "nevtláčať pigment"),
            ("žltý tieň", "opakovať mierne predčistenie", "nesušiť horúco"),
            ("mastná kari omáčka", "riešiť tuk aj farbu", "kontrola po praní"),
            ("farebné tričko", "testovať stálosť farby", "nevybieliť okolie"),
        ],
        "textile_rows": [
            ("bavlnené tričko", "pracovať od okraja ku stredu", "úplet drží pigment"),
            ("detská mikina", "jemné predčistenie a dobrý oplach", "často ide o väčšiu plochu"),
            ("obrus", "nežehliť pred kontrolou", "žltý tieň sa fixuje teplom"),
            ("zástera", "predprať lokálne", "kombinácia tuku a korenia"),
        ],
        "sections": [
            ("Ako vyprať kari z bavlneného trička", "Najprv odstráňte prebytok omáčky tupou hranou alebo papierovou utierkou. Netrite žltý pigment do strán. Miesto podložte savou vrstvou a predčistite lokálne, aby sa kari nerozšírilo do väčšej mapy.", "Tričko perte podľa štítku a pred sušením skontrolujte, či nezostal žltý tieň alebo mastný okraj."),
            ("Kurkuma na bielom a farebnom textile", "Kurkuma farbí intenzívne aj v malom množstve. Na bielom textile je tieň viditeľný hneď, na farebnom môže splynúť a ostať až po vyschnutí. Preto kontrolujte aj svetlo okolo škvrny a rub látky.", "Pri farebných kusoch si dávajte pozor na agresívne bielenie. Môže odstrániť farbu textilu skôr než škvrnu."),
            ("Kari ako kombinácia pigmentu a mastnoty", "Kari omáčka často obsahuje olej, kokosové mlieko, jogurt alebo maslo. To znamená, že po odstránení žltého pigmentu môže zostať mastná mapa. Riešte obe vrstvy postupne, nie jedným náhodným zásahom.", "Pri mastnej časti pomáha návod <a href=\"/n/ako-odstranit-olejove-a-mastne-skvrny-z-oblecenia-po-prani\">ako odstrániť olejové a mastné škvrny</a>."),
            ("Kari na obruse, zástere a kuchynskej utierke", "Kuchynské textílie majú často viac vrstiev škvŕn naraz. Kari môže byť v leme obrusu, na zástere pri varení alebo v utierke, ktorou sa škvrna utierala. Takéto kusy neperte s jemnou bielizňou bez predchádzajúcej kontroly.", "Ak je škvrna staršia, postupujte trpezlivo. Pri žltom pigmente je opakované mierne predčistenie bezpečnejšie než vysoká teplota."),
            ("Čo robiť, keď žltý tieň zostane", "Ak po praní vidíte žltý tieň, kus nesušte horúco. Skontrolujte, či je tieň pigmentový alebo mastný. Pigment potrebuje iný prístup než olejový film. Až keď je miesto čisté, môžete textil sušiť bežným spôsobom.", "Pri detskom oblečení myslite aj na dobrý oplach, aby na látke nezostali zvyšky predčistenia."),
        ],
        "depth": [
            ("Prečo je kurkuma taká viditeľná", "Kurkuma obsahuje intenzívne farbiace zložky, ktoré sa ľahko zachytia na svetlých vláknach. Preto sa škvrna správa skôr ako pigment než obyčajná omáčka. Čas a teplo hrajú proti vám.", "Čím skôr odstránite prebytok a začnete mierne predčistenie, tým menšie riziko trvalého tieňa."),
            ("Slnko a žlté škvrny", "Pri niektorých žltých pigmentoch ľudia skúšajú slnko. Nemá to byť prvý krok ani náhrada za čistenie. Najprv treba odstrániť omáčku, tuk a zvyšky z vlákna. Až potom má zmysel riešiť veľmi slabý tieň podľa materiálu.", "Pri farebných kusoch môže slnko ovplyvniť aj pôvodnú farbu textilu."),
        ],
        "expert_title": "Odbornejší pohľad: pigment, tuk a riziko fixácie",
        "expert_p1": "Kari a kurkuma sú náročné preto, že kombinujú farbiace látky s jedlom, ktoré môže obsahovať tuk. Pigment sa snažíte dostať z vlákna, mastnotu uvoľniť z povrchu a zároveň nechcete poškodiť farbu samotného textilu.",
        "rule": "Pri kari a kurkume najprv odstráňte objem, potom riešte pigment a mastnotu, a až po kontrole sušte.",
        "recommendation_intro": "Pri kari a kurkume prací gél pomôže hlavne v následnom praní po lokálnom predčistení. Dôležité je nevtlačiť pigment do väčšej plochy.",
        "product_text": "Vhodný na následné pranie bavlnených tričiek, záster a kuchynských textílií po lokálnom ošetrení kari alebo kurkumy.",
        "links": [
            ("/n/ako-odstranit-skvrny-od-horcice-z-tricka-obrusu-a-utierky", "Ako odstrániť škvrny od horčice"),
            ("/n/ako-odstranit-cervenu-papriku-z-tricka-a-kuchynskej-utierky", "Ako odstrániť červenú papriku"),
            ("/n/ako-vyprat-granatove-jablko-z-oblecenia-bez-ruzovych-map", "Ako vyprať granátové jablko z oblečenia"),
        ],
        "faq": [
            ("Prečo kari necháva žltý tieň?", "Kurkuma a koreniny obsahujú intenzívne pigmenty, ktoré sa držia vo vláknach."),
            ("Môžem škvrnu hneď sušiť?", "Nie, až po kontrole. Teplo môže zvyšok pigmentu alebo mastnoty zafixovať."),
            ("Čo ak je kari aj mastné?", "Riešte pigment aj tuk. Nestačí iba oplach vodou."),
        ],
    },
    "compression": {
        "marker": "Detailnejší postup na kompresné pančuchy, elastan a zdravotné návleky",
        "problem": "kompresné pančuchy a elastické zdravotné návleky potrebujú čistotu, ale zároveň nesmú stratiť pružnosť, tvar a funkčný tlak",
        "scope": "kompresných pančuchách, elastických návlekoch, zdravotných ponožkách, kolenných návlekoch a jemných elastických dieloch",
        "avoid": "aviváž, horúce sušenie, silné žmýkanie, radiátor a pranie spolu so zipsami alebo suchým zipsom",
        "diagnosis": [
            "<strong>Funkcia je dôležitejšia než vôňa:</strong> návlek má zostať pružný a priliehavý.",
            "<strong>Elastan neznáša teplo:</strong> horúca sušička alebo radiátor môžu skrátiť životnosť.",
            "<strong>Aviváž je riziko:</strong> môže ovplyvniť elastické vlákna a priľnavosť.",
            "<strong>Pranie po nosení:</strong> pot a kožný maz treba dostať preč šetrne.",
        ],
        "state_rows": [
            ("denné nosenie", "prať pravidelne jemne", "pot oslabuje komfort"),
            ("návlek stráca pružnosť", "skontrolovať teplotu a sušenie", "možné poškodenie elastanu"),
            ("zápach", "dobre opláchnuť a rýchlo sušiť", "neprevoňať bez vyprania"),
            ("jemná štruktúra", "ochranné vrecko a bez zipsov", "nižšie riziko zatrhnutia"),
        ],
        "textile_rows": [
            ("kompresné pančuchy", "ručné alebo jemné pranie podľa výrobcu", "funkčný tlak"),
            ("elastický návlek", "nižšia teplota a bez aviváže", "ochrana elastanu"),
            ("zdravotná ponožka", "dobrý oplach", "kontakt s pokožkou"),
            ("návlek s lemom", "nekrútiť a nesušiť na radiátore", "lem drží tvar"),
        ],
        "sections": [
            ("Ako prať kompresné pančuchy doma", "Najprv si pozrite štítok alebo odporúčanie výrobcu. Väčšina kompresných pančúch potrebuje jemné pranie, nízku teplotu, malé množstvo pracieho gélu a dôkladný oplach. Ak ich periete v práčke, použite ochranné vrecko a nedávajte ich k zipsom či suchým zipsom.", "Cieľom je odstrániť pot, maz a bežné znečistenie bez toho, aby sa poškodila pružnosť a kompresná funkcia."),
            ("Prečo nepoužívať aviváž na elastické návleky", "Aviváž môže zanechať film na vláknach a pri funkčných elastických textíliách je zbytočné riziko. Návlek má priliehať, odvádzať vlhkosť a držať tvar. Mäkkosť nie je dôležitejšia než funkcia.", "Podobné pravidlo platí pri športových a elastických materiáloch, pozri <a href=\"/n/kedy-nepouzivat-avivaz-uteraky-sportove-oblecenie-softshell-aj-detska-bielizen\">kedy nepoužívať aviváž</a>."),
            ("Ako sušiť kompresné pančuchy", "Po praní pančuchy jemne vytlačte do uteráka, nekrúťte ich silou a nesušte na radiátore. Vysoké teplo môže poškodiť elastické vlákna. Sušte ich voľne, mimo priameho horúceho zdroja.", "Ak sa lem alebo chodidlová časť deformuje, funkcia aj komfort môžu byť horšie pri ďalšom nosení."),
            ("Zápach a pot v zdravotných návlekoch", "Pri zdravotných návlekoch je dôležitý pravidelný oplach potu a kožného mazu. Ak návlek iba prevoňáte, problém zostane vo vlákne. Perte ho skôr jemne a pravidelne než zriedkavo a agresívne.", "Pri syntetike a elastane nadväzuje článok <a href=\"/n/ako-prat-syntetiku-polyester-a-elastan-aby-nezapachali-a-drzali-tvar\">ako prať syntetiku, polyester a elastan</a>."),
            ("Kedy návlek radšej vymeniť", "Ak návlek stratí pružnosť, zosúva sa, má poškodené vlákna alebo zmenený lem, pranie už nemusí funkciu vrátiť. Pri zdravotných pomôckach je dôležité sledovať aj odporúčanie výrobcu a zdravotníka.", "Čistota je dôležitá, ale kompresný výrobok musí hlavne plniť svoju funkciu."),
        ],
        "depth": [
            ("Elastan a teplo", "Elastické vlákna pomáhajú pančuchám držať tvar a tlak. Teplo, silné krútenie a agresívne pranie môžu pružnosť postupne oslabiť. Preto je sušenie na radiátore alebo v horúcej sušičke rizikové.", "Pri elastických kusoch sa oplatí voliť nižšiu záťaž a častejšie jemné pranie."),
            ("Hygiena pri kontakte s pokožkou", "Kompresné pančuchy sú v priamom kontakte s pokožkou a často sa nosia celé hodiny. Pot, krémy a kožný maz sa preto hromadia rýchlejšie než na voľnom oblečení. Dobrý oplach je dôležitý pre komfort aj citlivú pokožku.", "Ak používate krém na nohy, nechajte ho vstrebať skôr, než si pančuchy oblečiete."),
        ],
        "expert_title": "Odbornejší pohľad: pružnosť, kompresia a domáca starostlivosť",
        "expert_p1": "Kompresný textil nie je obyčajná ponožka. Jeho úlohou je vytvárať presne definovaný tlak a pritom zostať pohodlný. Domáca starostlivosť preto nesmie byť zameraná len na odstránenie pachu, ale aj na zachovanie pružnosti a tvaru.",
        "rule": "Pri kompresných pančuchách perte jemne, bez aviváže, bez horúceho sušenia a vždy podľa odporúčania výrobcu.",
        "recommendation_intro": "Pri elastických zdravotných textíliách používajte šetrný prací gél v malom množstve a dobre oplachujte. Aviváž a teplo sú väčšie riziko než miernejší prací režim.",
        "product_text": "Vhodný na šetrné pranie elastických textílií, ak výrobca povoľuje pranie s jemným pracím gélom. Pri zdravotných pomôckach vždy rešpektujte štítok a odporúčanie výrobcu.",
        "links": [
            ("/n/co-je-elastan-preco-je-v-leginach-spodnej-bielizni-a-sportovom-obleceni", "Čo je elastan"),
            ("/n/ako-prat-syntetiku-polyester-a-elastan-aby-nezapachali-a-drzali-tvar", "Ako prať syntetiku, polyester a elastan"),
            ("/n/kedy-nepouzivat-avivaz-uteraky-sportove-oblecenie-softshell-aj-detska-bielizen", "Kedy nepoužívať aviváž"),
        ],
        "faq": [
            ("Môžem dať kompresné pančuchy do sušičky?", "Iba ak to výslovne povoľuje výrobca. Inak je bezpečnejšie voľné sušenie bez horúceho zdroja."),
            ("Prečo nepoužívať aviváž?", "Môže zanechať film na elastických vláknach a zhoršiť funkciu alebo priľnavosť materiálu."),
            ("Ako často ich prať?", "Podľa nosenia a odporúčania výrobcu. Pri dennom kontakte s pokožkou skôr pravidelne a šetrne."),
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
        <p>{config["problem"].capitalize()}. Preto je najdôležitejšie rozlíšiť, či riešite pigment, mastnotu, bielkovinu, soľ, pach alebo citlivé elastické vlákno. Jeden univerzálny postup nestačí.</p>
        <p>Pri textile ako {config["scope"]} rozhoduje materiál, farba, hrúbka, švy a spôsob sušenia. Najväčšie riziko je {config["avoid"]}.</p>
        {note_card("Rýchla praktická diagnostika", config["diagnosis"])}
        <h2>Postup podľa stavu škvrny alebo textilu</h2>
        {table(["Stav", "Čo urobiť", "Poznámka"], config["state_rows"])}
        <h2>Postup podľa typu textilu</h2>
        {table(["Textil", "Postup", "Prečo"], config["textile_rows"])}
        {sections}
        <h2>{config["expert_title"]}</h2>
        <p>{config["expert_p1"]}</p>
        <p>Pri domácich škvrnách a citlivých materiáloch sa oplatí držať jednoduchého princípu: najprv odstrániť povrchový problém, potom prať podľa štítku a až nakoniec riešiť vôňu alebo sušenie. Užitočný odborný zdroj k typom škvŕn a predčisteniu je <a rel="noopener" href="https://extension.illinois.edu/global/stain-solutions" target="_blank">Illinois Extension Stain Solutions</a>.</p>
        {depth}
        <h2>Kontrolný checklist pred praním</h2>
        <p>Pred praním skontrolujte, či na látke nezostal objem škvrny, mastný film, farebný okraj, zvyšok jedla alebo poškodený elastický lem. Pri obrusoch a košeliach sledujte aj rub, švy a okraje. Pri zdravotných a elastických textíliách si najprv prečítajte štítok výrobcu.</p>
        <p>Do jednej dávky nedávajte kusy, ktoré potrebujú úplne iný režim. Mastný obrus, pigmentové tričko a kompresné pančuchy nepatria do rovnakej logiky prania. Triedenie a lokálne predčistenie často urobia väčší rozdiel než dlhší program.</p>
        <h2>Kedy postup zopakovať</h2>
        <p>Ak po praní zostal tieň, pach, mastnota, lepkavosť alebo zmena pružnosti, nesušte textil horúco. Najprv rozlíšte, či ide o zvyšok škvrny alebo už o zmenu materiálu. Opakovaný mierny postup je bezpečnejší než jeden agresívny zásah.</p>
        <p>Ak látka púšťa farbu, mení povrch, tvrdne alebo sa elastický kus vyťahuje, znížte mechaniku a teplotu. Pri drahších alebo zdravotných výrobkoch je dôležitejšie zachovať funkciu než za každú cenu odstrániť stopu jedným pokusom.</p>
        <h2>Ako predísť zafixovaniu pri sušení</h2>
        <p>Mokrá látka môže skryť hnedý tieň, mastnú mapu aj žltý pigment. Preto miesto skontrolujte pri dennom svetle ešte pred sušičkou, žehlením alebo radiátorom. Teplo má prísť až vtedy, keď je výsledok čistý a štítok ho povoľuje.</p>
        <p>Pri elastických zdravotných textíliách je sušenie ešte citlivejšie. Horúci zdroj môže skrátiť životnosť pružných vlákien a zhoršiť tvar výrobku.</p>
        <h2>Domáca rutina pri opakovaných škvrnách</h2>
        <p>Ak sa podobné škvrny opakujú, nastavte si rutinu: rýchla kontrola po jedle alebo nosení, odobratie prebytku, lokálne predčistenie, pranie v primerane plnom bubne a kontrola pred sušením. Pri obrusoch pomáha označiť škvrny hneď po oslave.</p>
        <p>Pri elastických a zdravotných kusoch je rutina ešte jednoduchšia: prať jemne, pravidelne, bez aviváže a bez horúceho sušenia. Tak sa znižuje riziko pachu aj straty funkcie.</p>
        <h2>Čo sledovať po druhom praní</h2>
        <p>Ak ani druhé šetrné pranie nepomohlo, sledujte, či ide o pigment, mastnotu, pach alebo poškodenie materiálu. Farebný tieň potrebuje iný prístup než olejový film a vyťahaný elastan už nie je škvrna.</p>
        <p>Pri opakovanom probléme si zapíšte, čo škvrnu spôsobilo a čo pomohlo. Pri ďalšom praní potom nebudete skúšať náhodné postupy, ktoré môžu textil zbytočne poškodiť.</p>
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
    parser = argparse.ArgumentParser(description="Conservatively expand VEVO retrofit wave 19 food and elastic articles.")
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
                "wave": "retrofit-wave-19-food-elastic-five",
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
                "wave": "retrofit-wave-19-food-elastic-five",
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
