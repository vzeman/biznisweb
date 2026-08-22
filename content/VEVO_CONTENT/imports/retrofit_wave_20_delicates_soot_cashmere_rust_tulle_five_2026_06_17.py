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
OUT_JSON = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-20-delicates-soot-cashmere-rust-tulle-five-2026-06-17.json"
MCP_RESULTS = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-20-delicates-soot-cashmere-rust-tulle-five-2026-06-17-mcp-results.json"


ARTICLES = [
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-16-2026-06-10-articles.json",
        "slug": "ako-prat-podprsenku-a-jemnu-spodnu-bielizen-bez-deformacie",
        "post_id": "2150",
        "url": "https://www.vevo.sk/n/ako-prat-podprsenku-a-jemnu-spodnu-bielizen-bez-deformacie",
        "topic": "bra_lingerie",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-18-2026-06-10-articles.json",
        "slug": "ako-odstranit-sadze-z-oblecenia-po-sviecke-grile-alebo-krbe",
        "post_id": "2194",
        "url": "https://www.vevo.sk/n/ako-odstranit-sadze-z-oblecenia-po-sviecke-grile-alebo-krbe",
        "topic": "soot",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-16-2026-06-10-articles.json",
        "slug": "ako-prat-kasmirovy-sveter-doma-bez-zrazenia-a-zmolkov",
        "post_id": "2151",
        "url": "https://www.vevo.sk/n/ako-prat-kasmirovy-sveter-doma-bez-zrazenia-a-zmolkov",
        "topic": "cashmere",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-18-2026-06-10-articles.json",
        "slug": "ako-odstranit-hrdzu-z-oblecenia-obrusu-a-pracovnych-nohavic",
        "post_id": "2192",
        "url": "https://www.vevo.sk/n/ako-odstranit-hrdzu-z-oblecenia-obrusu-a-pracovnych-nohavic",
        "topic": "rust",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-18-2026-06-10-articles.json",
        "slug": "ako-prat-tylovu-suknu-zavoj-a-jemny-tyl-bez-potrhania",
        "post_id": "2190",
        "url": "https://www.vevo.sk/n/ako-prat-tylovu-suknu-zavoj-a-jemny-tyl-bez-potrhania",
        "topic": "tulle",
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
        <p>Pri jemných materiáloch, funkčných elastických dieloch a škvrnách, ktoré sa nesmú zafixovať teplom, je dôležité dávkovať mierne a dobre oplachovať.</p>
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
    "bra_lingerie": {
        "marker": "Detailnejší postup na podprsenku, kostice, čipku a jemnú spodnú bielizeň",
        "problem": "podprsenka a jemná spodná bielizeň sa neničia iba špinou, ale najmä trením, teplom, prekrútením košíkov a zvyškom pracieho prostriedku v elastických vláknach",
        "scope": "podprsenkách s kosticami, vystužených košíkoch, čipke, elastane, nohavičkách a jemných ramienkach",
        "avoid": "horúce pranie, sušičku, krútenie košíkov, pranie so zipsami a aviváž pri elastických dieloch",
        "diagnosis": [
            "<strong>Košík nesmie zmeniť tvar:</strong> vystuženie sa poškodí skôr mechanikou než samotnou vodou.",
            "<strong>Kostice potrebujú ochranu:</strong> vyhnite sa preťaženiu bubna a drsným kusom.",
            "<strong>Čipka sa zatrhne ľahko:</strong> pomáha ochranné vrecko a zapnuté háčiky.",
            "<strong>Elastan nemá rád teplo:</strong> pružnosť chráni nižšia teplota a voľné sušenie.",
        ],
        "state_rows": [
            ("bežné nosenie", "jemné pranie a dobrý oplach", "pot a krém sa držia v lemoch"),
            ("vystužený košík", "nekrútiť a netlačiť", "tvar je kľúčový"),
            ("čipka", "ochranné vrecko", "nižšie riziko zatrhnutia"),
            ("zápach po nosení", "prať skôr pravidelne než agresívne", "neprevoňať bez vyprania"),
        ],
        "textile_rows": [
            ("podprsenka s kosticou", "ručné alebo jemné pranie", "ochrana tvaru a kovových častí"),
            ("športová podprsenka", "bez aviváže a dobre opláchnuť", "pot a elastan"),
            ("čipkovaná bielizeň", "vrecko a nízka mechanika", "jemné okraje"),
            ("nohavičky s elastanom", "nižšia teplota", "ochrana pružnosti"),
        ],
        "sections": [
            ("Ako prať podprsenku v ruke", "Najbezpečnejší postup je vlažná voda, malé množstvo pracieho gélu a krátke jemné prepranie bez krútenia. Košíky nestláčajte do jednej hrče a kostice neohýbajte. Po praní bielizeň len jemne vytlačte do uteráka.", "Tento postup je vhodný najmä pri vystužených košíkoch, drahšej čipke a podprsenkách, ktoré chcete udržať v pôvodnom tvare čo najdlhšie."),
            ("Ako prať podprsenku v práčke", "Ak štítok povoľuje práčku, zapnite háčiky, vložte podprsenku do ochranného vrecka a perte s jemnými kusmi, nie s džínsami, uterákmi alebo zipsami. Vyberte jemný program, nižšiu teplotu a mierne odstreďovanie.", "Bubon nesmie byť preplnený. Podprsenka potrebuje priestor, aby sa košík neprelomil a ramienka sa nezamotali do iných vecí."),
            ("Prečo nepoužívať sušičku", "Sušička kombinuje teplo a mechaniku. Pri podprsenkách to znamená riziko deformácie košíkov, oslabenia elastanu a skrútenia čipky. Aj keď sa bielizeň zdá odolná, opakované horúce sušenie skracuje životnosť.", "Najlepšie je sušiť voľne na vzduchu, mimo radiátora a priameho horúceho zdroja."),
            ("Športová podprsenka, pot a elastan", "Športová podprsenka potrebuje odstrániť pot a kožný maz, ale zároveň si musí zachovať pružnosť. Aviváž nie je dobrý nápad, pretože môže zanechať film na elastických vláknach. Dôležitý je dobrý oplach a rýchle vysušenie.", "K elastanu nadväzuje článok <a href=\"/n/co-je-elastan-preco-je-v-leginach-spodnej-bielizni-a-sportovom-obleceni\">čo je elastan a prečo je v spodnej bielizni</a>."),
            ("Ako skladovať jemnú bielizeň po praní", "Podprsenky neskladajte tak, že jeden košík prevrátite do druhého, ak to konštrukcia neumožňuje. Skladujte ich voľne, aby sa košíky netlačili a kostice neohýbali. Jemná čipka by nemala byť zachytená o háčiky iných kusov.", "Správne skladovanie dopĺňa pranie. Aj dokonale vypraná podprsenka sa môže zdeformovať v preplnenej zásuvke."),
        ],
        "depth": [
            ("Prečo sa podprsenka deformuje", "Deformácia často vzniká kombináciou vody, tlaku a nesprávneho sušenia. Košík sa môže zlomiť, kostica posunúť a elastické lemy vytiahnuť. Preto je pri tejto bielizni dôležitejší jemný režim než silnejší prací program.", "Ak sa košík raz prelomí, pranie ho nemusí vrátiť späť. Prevencia je tu podstatne účinnejšia než oprava."),
            ("Zvyšky pracieho gélu a citlivá pokožka", "Spodná bielizeň je v priamom kontakte s pokožkou, preto musí byť dobre opláchnutá. Priveľa pracieho prostriedku môže zostať v lemoch a čipke. Pri citlivej pokožke je mierne dávkovanie a oplach dôležitejší než silná parfumácia.", "Ak bielizeň po praní pôsobí klzko alebo tuhšie, skontrolujte dávkovanie a veľkosť náplne."),
        ],
        "expert_title": "Odbornejší pohľad: elastan, konštrukcia a mechanická záťaž",
        "expert_p1": "Podprsenka je konštrukčný textil. Nejde iba o látku, ale o košík, lem, kostice, ramienka, háčiky a elastické vlákna. Každá časť reaguje na pranie inak. Preto domáca starostlivosť musí chrániť tvar aj pružnosť, nie iba odstrániť pach.",
        "rule": "Pri podprsenke chráňte tvar: zapnúť háčiky, použiť vrecko, nízka mechanika, bez sušičky a bez krútenia košíkov.",
        "recommendation_intro": "Pri jemnej spodnej bielizni je dôležité používať malé množstvo šetrného pracieho gélu a dobre oplachovať. Silná vôňa ani aviváž nenahradia správne pranie.",
        "product_text": "Vhodný na šetrné pranie jemnej bielizne, ak ho použijete v primeranej dávke a rešpektujete štítok výrobcu.",
        "links": [
            ("/n/co-je-elastan-preco-je-v-leginach-spodnej-bielizni-a-sportovom-obleceni", "Čo je elastan"),
            ("/n/kedy-nepouzivat-avivaz-uteraky-sportove-oblecenie-softshell-aj-detska-bielizen", "Kedy nepoužívať aviváž"),
            ("/n/ako-prat-menstruacne-nohavicky-bezpecne-a-hygienicky", "Ako prať menštruačné nohavičky"),
        ],
        "faq": [
            ("Môžem prať podprsenku v práčke?", "Áno, ak to povoľuje štítok. Použite ochranné vrecko, jemný program a neperte ju s drsnými kusmi."),
            ("Prečo sa košíky deformujú?", "Najčastejšie pre tlak v bubne, krútenie, sušičku alebo nesprávne skladovanie."),
            ("Je aviváž vhodná na spodnú bielizeň?", "Pri elastane a funkčných lemoch radšej nie. Dôležitejší je dobrý oplach."),
        ],
    },
    "soot": {
        "marker": "Detailnejší postup na sadze, čierny prach a textil po sviečke, grile alebo krbe",
        "problem": "sadze sú jemný uhlíkový prach, ktorý sa pri mokrom trení ľahko rozmaže do väčšej sivej mapy",
        "scope": "bavlnenom tričku, mikine, obruse, pracovných nohaviciach, závese a textile pri krbe alebo grile",
        "avoid": "okamžité mokré šúchanie, kefovanie do strán a pranie bez odstránenia voľného prachu",
        "diagnosis": [
            "<strong>Najprv nasucho:</strong> voľné sadze treba odobrať pred vodou.",
            "<strong>Nerozmazávať:</strong> mokrá handrička môže vytvoriť sivú mapu.",
            "<strong>Gril pridáva mastnotu:</strong> sadze po grile môžu byť aj olejové.",
            "<strong>Kontrola pred sušením:</strong> sivý tieň sa po teple horšie rieši.",
        ],
        "state_rows": [
            ("voľný čierny prach", "vytriasť a jemne odsať", "bez vody"),
            ("sivá mapa", "lokálne predčistiť po odobratí prachu", "nešúchať do strán"),
            ("gril a mastnota", "riešiť prach aj tuk", "kombinovaná škvrna"),
            ("záves alebo poťah", "skontrolovať prateľnosť", "väčší kus môže vyžadovať čistiareň"),
        ],
        "textile_rows": [
            ("tričko", "vytriasť a prať naruby", "úplet drží prach"),
            ("obrus", "odsať a predčistiť lokálne", "nezažehliť tieň"),
            ("pracovné nohavice", "oddeliť od jemnej bielizne", "prenáša prach"),
            ("záves", "overiť štítok", "veľká plocha sa ľahko mapuje"),
        ],
        "sections": [
            ("Ako odstrániť sadze z oblečenia nasucho", "Najprv textil opatrne vytraste vonku alebo nad košom. Voľný prach sa snažte dostať preč skôr, než príde voda. Ak máte vhodný nadstavec, pomôže jemné odsatie bez pritláčania priamo do látky.", "Až keď je voľný prach preč, má zmysel lokálne predčistenie a pranie. Tento prvý krok rozhoduje o tom, či vznikne malý fľak alebo veľká sivá mapa."),
            ("Sadze po sviečke na obruse", "Pri sviečke býva problém kombinovaný: sadze, vosk a niekedy parfumovaná zložka. Ak je na obruse aj vosk, najprv riešte pevný voskový zvyšok a až potom čierny tieň. Obrus nežehlite, kým si nie ste istí výsledkom.", "K vosku nadväzuje článok <a href=\"/n/ako-odstranit-vosk-zo-sviecky-z-obrusu-a-textilu\">ako odstrániť vosk zo sviečky z obrusu</a>."),
            ("Sadze po grile alebo krbe", "Po grile a krbe sa sadze často miešajú s mastnotou, dymom a pachom. Pracovné nohavice alebo mikinu neperte s jemnou bielizňou. Najprv odstráňte prach, potom skontrolujte mastné miesta a až následne perte podľa štítku.", "Ak ostáva aj dymový pach, pomôže dobré vysušenie a čistá práčka, nie iba silnejšia vôňa."),
            ("Prečo sadze nerozotierať vlhkou handričkou", "Vlhká handrička môže sadze rozpustiť do sivej vrstvy a zatlačiť ich hlbšie do vlákien. Na hladkej ploche to vyzerá logicky, ale na textile sa prach správa inak. Voda patrí až po odstránení voľných častíc.", "To platí najmä pri bielych a svetlých látkach, kde je sivý tieň viditeľný aj po slabom zvyškovom znečistení."),
            ("Ako kontrolovať výsledok po praní", "Po praní skontrolujte textil pri dennom svetle. Sadze môžu zanechať slabý sivý tieň, ktorý na mokrej látke nevidno. Ak tam je, nesušte horúco. Zopakujte mierne lokálne predčistenie.", "Pri pracovných veciach sledujte aj to, či sa sadze nepreniesli na ďalšiu bielizeň v dávke."),
        ],
        "depth": [
            ("Sadze ako časticová škvrna", "Sadze sú najmä jemné častice. Preto sa správajú inak než čaj, olej alebo hrdza. Prvý cieľ je dostať častice preč z povrchu, nie ich rozmiešať do vody. Až potom riešite zvyškovú farbu a pach.", "Ak sa k sadziam pridá mastnota z grilu, postup sa mení na kombinovanú škvrnu."),
            ("Kedy zvoliť čistiareň", "Pri závesoch, čalúnení, saku alebo veľkom textile pri krbe môže byť domáce pranie rizikové. Ak štítok nepovoľuje pranie alebo ide o veľkú plochu, profesionálne čistenie je bezpečnejšie než rozšírenie sadzí.", "Najmä pri čalúnení sa voda môže zmeniť na mapu, ktorú už doma nevyrovnáte."),
        ],
        "expert_title": "Odbornejší pohľad: časticová škvrna, mastnota a riziko rozmazania",
        "expert_p1": "Pri sadziach nejde primárne o rozpustené farbivo, ale o jemné čierne častice. Mechanika prvého kroku je preto kľúčová. Ak sa častice zatlačia do vlákna, pranie musí riešiť väčšiu plochu a výsledok býva horší.",
        "rule": "Pri sadziach najprv nasucho odstráňte voľný prach, až potom lokálne predčistite a perte podľa štítku.",
        "recommendation_intro": "Pri sadziach má prací gél zmysel až po tom, čo z textilu odstránite voľný čierny prach. Inak sa môže škvrna v bubne rozšíriť.",
        "product_text": "Vhodný na následné pranie bežných textílií po tom, čo sadze najprv odstránite nasucho a lokálne predčistíte.",
        "links": [
            ("/n/ako-odstranit-vosk-zo-sviecky-z-obrusu-a-textilu", "Ako odstrániť vosk zo sviečky z obrusu"),
            ("/n/ako-vyprat-stare-skvrny-kompletny-sprievodca-pre-ciste-oblecenie", "Ako vyprať staré škvrny"),
            ("/n/ako-prat-obrus-po-oslave-aby-nezostali-mastne-skvrny-a-pachy", "Ako prať obrus po oslave"),
        ],
        "faq": [
            ("Mám sadze hneď namočiť?", "Nie. Najprv odstráňte voľný prach nasucho, aby ste ho nerozmazali."),
            ("Prečo ostal sivý tieň?", "Častice sa vtlačili do vlákna alebo sa škvrna zafixovala teplom."),
            ("Čo ak sú sadze aj mastné?", "Po odstránení prachu riešte aj mastnú časť, najmä po grile."),
        ],
    },
    "cashmere": {
        "marker": "Detailnejší postup na kašmírový sveter, zrazenie a žmolky",
        "problem": "kašmír je jemné živočíšne vlákno, ktoré môže pri teple, trení a zlom sušení stratiť mäkkosť, zmeniť tvar alebo začať viac žmolkovať",
        "scope": "kašmírovom svetri, kardigane, šále, jemnom úplete, vlnených zmesiach a luxusných zimných vrstvách",
        "avoid": "horúcu vodu, prudké trenie, vešanie mokrého svetra, sušičku a agresívne žmýkanie",
        "diagnosis": [
            "<strong>Kašmír neperte po každom nosení:</strong> často stačí vetrať.",
            "<strong>Teplota musí byť stabilná:</strong> prudké zmeny škodia vláknu.",
            "<strong>Mokrý sveter nevešajte:</strong> vytiahne sa vlastnou váhou.",
            "<strong>Žmolky riešte jemne:</strong> neťahajte ich rukou.",
        ],
        "state_rows": [
            ("iba pach po nosení", "vetrať a nechať odpočinúť", "nie vždy prať"),
            ("pot v podpazuší", "jemné lokálne predčistenie", "bez trenia"),
            ("sveter je mokrý", "tvarovať naplocho", "nevešať"),
            ("žmolky", "jemný hrebeň alebo odžmolkovač s citom", "nevytrhávať vlákna"),
        ],
        "textile_rows": [
            ("kašmírový sveter", "ručné alebo špeciálne jemné pranie", "ochrana vlákna"),
            ("kašmírová zmes", "riadiť sa najcitlivejšou zložkou", "zmes sa môže správať inak"),
            ("šál", "minimum trenia", "jemný kontakt s pokožkou"),
            ("kardigan", "sušiť naplocho", "ťažší mokrý úplet"),
        ],
        "sections": [
            ("Ako prať kašmírový sveter v ruke", "Použite vlažnú vodu, malé množstvo jemného pracieho gélu a sveter len opatrne pretláčajte vo vode. Netrite podpazušie silou, nekrúťte rukávy a nemeňte teplotu vody prudko. Dôležité je zachovať rovnaký pokojný režim od namočenia po oplach.", "Ak je podpazušie cítiť potom, nepomôže silnejšie šúchanie. Miesto radšej krátko predmočte, jemne pretláčajte medzi prstami a oplachujte dovtedy, kým vo vlákne nezostane klzký pocit z pracieho gélu. Po praní sveter nevytáčajte. Jemne ho vytlačte do uteráka a pripravte na sušenie naplocho."),
            ("Ako sušiť kašmír bez vytiahnutia", "Mokrý kašmír nevešajte na vešiak. Vlastná váha vody vytiahne ramená, rukávy a spodný lem. Položte ho na suchý uterák, upravte tvar a nechajte schnúť voľne mimo radiátora a priameho slnka.", "Tvarovanie za vlhka je pri kašmíri rovnako dôležité ako samotné pranie."),
            ("Ako často prať kašmírový sveter", "Kašmír nemusíte prať po každom nosení. Ak nie je znečistený, často stačí vyvetrať ho a nechať odpočinúť. Časté pranie zvyšuje mechanickú záťaž a môže prispieť k žmolkovaniu.", "Pri pachu po nosení pomáha aj článok <a href=\"/n/ako-prat-vlneny-sveter-ked-zapacha-po-noseni\">ako prať vlnený sveter, keď zapácha</a>."),
            ("Kašmír a žmolky", "Žmolky na kašmíri neznamenajú automaticky zlý materiál. Jemné vlákna sa pri nosení trú v podpazuší, na bokoch alebo pod kabátom. Žmolky odstraňujte jemne, bez vytrhávania. Príliš agresívne holenie oslabuje povrch.", "K téme nadväzuje článok <a href=\"/n/preco-sa-oblecenie-zmolkuje-vlakna-trenie-pranie-a-susenie\">prečo sa oblečenie žmolkuje</a>."),
            ("Kedy kašmír radšej dať do čistiarne", "Ak má sveter škvrnu, ktorú neviete identifikovať, zložité farbenie, výšivku alebo veľmi jemný úplet, domáce pranie nemusí byť najbezpečnejšie. Čistiareň zvoľte aj vtedy, keď štítok domáce pranie nepovoľuje.", "Domáce pranie má byť konzervatívne. Pri luxusnom svetri sa neoplatí riskovať tvar ani povrch."),
        ],
        "depth": [
            ("Prečo sa kašmír zrazí alebo splstnatí", "Kašmír je živočíšne vlákno podobne citlivé na teplo a trenie ako vlna. Ak sa vlákna vo vlhku silno trú alebo dostanú teplotný šok, môžu sa zachytiť do seba a sveter stratí pôvodný tvar.", "Prevencia je jednoduchšia než oprava. Raz splstnatený sveter sa nemusí podariť vrátiť späť."),
            ("Kašmír v zmesi s vlnou alebo syntetikou", "Zmesové svetre sa môžu správať nepredvídateľne. Syntetika môže držať tvar, vlna citlivo reagovať na teplo a kašmír meniť povrch. Preto sa riaďte najcitlivejšou zložkou a štítkom, nie iba názvom materiálu.", "Ak si nie ste istí, zvoľte ručné pranie alebo čistiareň."),
        ],
        "expert_title": "Odbornejší pohľad: jemné živočíšne vlákno a mechanika prania",
        "expert_p1": "Kašmír je cenený pre jemnosť, ale tá istá jemnosť znamená vyššiu citlivosť na mechanické namáhanie. Pri praní preto rozhoduje nízka intenzita pohybu, stabilná teplota a sušenie v tvare. Silnejší program nepridá čistotu, skôr zvýši opotrebovanie.",
        "rule": "Pri kašmíri menej znamená viac: menej prania, menej trenia, menej tepla a viac kontroly tvaru pri sušení.",
        "recommendation_intro": "Pri kašmíri používajte šetrný prací gél veľmi striedmo a len vtedy, keď štítok povoľuje domáce pranie. Dôležitejšie než vôňa je zachovanie tvaru a mäkkosti.",
        "product_text": "Vhodný na veľmi šetrné pranie jemných textílií, ak to povoľuje štítok. Pri kašmíri používajte malé množstvo a dôkladný oplach.",
        "links": [
            ("/n/ako-prat-vlneny-sveter-ked-zapacha-po-noseni", "Ako prať vlnený sveter, keď zapácha"),
            ("/n/co-je-merino-vlna-vyhody-nevyhody-a-pranie-bez-zrazenia", "Čo je merino vlna"),
            ("/n/preco-sa-oblecenie-zrazi-po-prani-teplota-vlakna-susicka-a-prevencia", "Prečo sa oblečenie zrazí po praní"),
        ],
        "faq": [
            ("Môžem kašmír prať v práčke?", "Iba ak to povoľuje štítok a práčka má veľmi jemný program. Ručné pranie býva bezpečnejšie."),
            ("Prečo kašmír žmolkuje?", "Jemné vlákna sa trením uvoľňujú a zachytávajú do uzlíkov, najmä pod pazuchami a pod kabátom."),
            ("Ako sušiť kašmírový sveter?", "Naplocho v tvare, mimo radiátora a bez vešania mokrého svetra."),
        ],
    },
    "rust": {
        "marker": "Detailnejší postup na hrdzu z oblečenia, obrusu a pracovných nohavíc",
        "problem": "hrdza je kovové zafarbenie, ktoré sa nespráva ako bežná špina a pri nesprávnom zásahu môže zostať ako oranžový alebo hnedý tieň",
        "scope": "pracovných nohaviciach, bielej košeli, obruse, bavlnenom tričku, uteráku a svetlej bielizni",
        "avoid": "chlórové bielidlo ako prvý pokus, žehlenie pred kontrolou a opakované pranie bez nájdenia zdroja hrdze",
        "diagnosis": [
            "<strong>Najprv zdroj:</strong> klinec, náradie, štipec, sušiak alebo kovová šnúra môže fľak opakovať.",
            "<strong>Hrdza nie je blato:</strong> bežný prací cyklus často nestačí.",
            "<strong>Biela látka je citlivá:</strong> agresívne pokusy môžu vytvoriť svetlý kruh.",
            "<strong>Teplo odložte:</strong> žehlenie fixuje zvyškový tieň.",
        ],
        "state_rows": [
            ("bod od kovu", "lokálne riešiť a nájsť zdroj", "neprať naslepo"),
            ("oranžová mapa", "postupovať mierne opakovane", "kontrola pred sušením"),
            ("pracovné nohavice", "oddeliť od jemnej bielizne", "kovový prach"),
            ("biely obrus", "bez žehlenia pred výsledkom", "tieň sa zvýrazní"),
        ],
        "textile_rows": [
            ("pracovné nohavice", "odstrániť prach a kovové zvyšky", "kombinovaná špina"),
            ("obrus", "lokálne ošetriť a kontrolovať", "teplo je riziko"),
            ("bavlnené tričko", "testovať na skrytom mieste", "farba môže púšťať"),
            ("uterák", "pozor na slučky", "hrdza sa drží v štruktúre"),
        ],
        "sections": [
            ("Ako odstrániť hrdzu z oblečenia", "Najprv zistite, či je fľak naozaj hrdza. Oranžový bod po kontakte s kovom, náradím, štipcom alebo sušiakom sa správa inak než blato. Bežné pranie môže odstrániť povrchovú špinu, ale kovový tieň nechá v látke.", "Postupujte lokálne, opatrne a bez žehlenia pred kontrolou výsledku."),
            ("Hrdza na pracovných nohaviciach", "Pracovné nohavice môžu mať okrem hrdze aj olej, prach, zeminu alebo kovové piliny. Pred praním ich vytraste a neperte s jemnou bielizňou. Hrdzavé body riešte lokálne, nie agresívnym programom pre celú dávku.", "Ak je na nohaviciach aj mastnota, kombinujte postup s odmasťovaním po predchádzajúcej kontrole materiálu."),
            ("Hrdza na obruse a bielej látke", "Na bielom obruse je lákavé použiť silný zásah, ale pri hrdzi to nemusí byť najlepší prvý krok. Dôležité je vyhnúť sa teplu pred kontrolou. Žehlička môže slabý oranžový tieň zafixovať.", "Ak neviete, či ide o hrdzu, pozrite sa na tvar a miesto škvrny. Bod po kovovom predmete býva ostrejší, mapa po sušiaku alebo šnúre môže kopírovať kontakt s kovom a škvrna od jedla má často aj mastný okraj. Ak fľak vznikol pri sušení, súvisiaci článok je <a href=\"/n/ako-odstranit-hrdzave-flaky-od-stipcov-a-snury-na-bielizen\">hrdzavé fľaky od štipcov a šnúry</a>."),
            ("Prečo sa hrdzavé fľaky vracajú", "Ak nevyriešite zdroj, hrdza sa objaví znova. Skontrolujte štipce, sušiak, kovový kôš, náradie, zábradlie alebo miesto, kde sa textil dotýkal kovu vo vlhku. Pranie textilu bez odstránenia zdroja je len dočasná oprava.", "Pri sušiaku pomáha návod <a href=\"/n/ako-vycistit-susiak-na-bielizen-aby-neprenasal-spinu-na-pradlo\">ako vyčistiť sušiak na bielizeň</a>."),
            ("Kedy hrdzu neriešiť doma", "Pri drahom saku, jemnej blúzke, hodvábe, vlne alebo nestálej farbe je riziko domáceho experimentu vyššie. Ak neviete, ako materiál zareaguje, testujte na skrytom mieste alebo zvoľte čistiareň.", "Cieľom nie je vytvoriť svetlý kruh okolo pôvodného oranžového bodu."),
        ],
        "depth": [
            ("Hrdza verzus pigmentová škvrna", "Hrdza pochádza z kovu a má inú chémiu než čaj, kari alebo make-up. Preto sa na ňu nedá spoľahnúť rovnakým postupom ako na bežné farebné škvrny. Najprv identifikujte zdroj a až potom nastavte čistenie.", "Ak si hrdzu pomýlite s blatom, pranie môže byť príliš slabé a teplo príliš skoré."),
            ("Kontrola pred žehlením", "Mokrá látka vie oranžový tieň skryť. Po praní preto nechajte miesto preschnúť alebo ho skontrolujte pri dennom svetle. Žehlenie alebo sušička patria až po tom, čo je miesto čisté.", "Pri obrusoch je táto kontrola mimoriadne dôležitá, pretože žehlenie býva bežný ďalší krok."),
        ],
        "expert_title": "Odbornejší pohľad: kovové zafarbenie a vlhký kontakt",
        "expert_p1": "Hrdzavý fľak často vznikne tak, že vlhký textil príde do kontaktu s kovovým zdrojom. Voda pomôže preniesť zafarbenie na vlákno a po vyschnutí zostane oranžová stopa. Preto sa musí riešiť textil aj zdroj hrdze.",
        "rule": "Pri hrdzi najprv nájdite zdroj, potom riešte fľak lokálne a teplo pridajte až po kontrole výsledku.",
        "recommendation_intro": "Pri hrdzi je prací gél skôr následný krok po lokálnom ošetrení. Samotný prací cyklus bez vyriešenia zdroja a fľaku nemusí stačiť.",
        "product_text": "Vhodný na následné pranie po lokálnom riešení hrdzavého fľaku, ak to povoľuje štítok textilu.",
        "links": [
            ("/n/ako-odstranit-hrdzave-flaky-od-stipcov-a-snury-na-bielizen", "Hrdzavé fľaky od štipcov a šnúry"),
            ("/n/ako-vycistit-susiak-na-bielizen-aby-neprenasal-spinu-na-pradlo", "Ako vyčistiť sušiak na bielizeň"),
            ("/n/ako-vyprat-stare-skvrny-kompletny-sprievodca-pre-ciste-oblecenie", "Ako vyprať staré škvrny"),
        ],
        "faq": [
            ("Prečo hrdza nejde dole bežným praním?", "Ide o kovové zafarbenie, nie len povrchovú špinu."),
            ("Môžem hrdzavý fľak vyžehliť?", "Nie pred kontrolou. Teplo môže zvyškový oranžový tieň zafixovať."),
            ("Ako zabrániť návratu hrdze?", "Nájdite a odstráňte zdroj: štipec, sušiak, náradie, zábradlie alebo kovový kontakt."),
        ],
    },
    "tulle": {
        "marker": "Detailnejší postup na tylovú sukňu, závoj a jemný tyl bez potrhania",
        "problem": "tyl je ľahká sieťovaná alebo jemne štruktúrovaná látka, ktorá sa môže zachytiť, potrhať, zdeformovať alebo stratiť objem pri príliš hrubom praní",
        "scope": "tylovej sukni, závoji, spodničke, detskom kostýme, spoločenských šatách a jemných dekoratívnych vrstvách",
        "avoid": "pranie so zipsami, suchým zipsom, háčikmi, vysoké otáčky, krútenie a vešanie ťažkého mokrého tylu za jeden bod",
        "diagnosis": [
            "<strong>Najprv skontrolovať zachytenia:</strong> dierky a zatrhnutia sa pri praní zväčšia.",
            "<strong>Objem je súčasť vzhľadu:</strong> prílišný tlak tyl sploští.",
            "<strong>Závoj je citlivejší:</strong> často má lem, čipku alebo aplikáciu.",
            "<strong>Vrecko pomáha:</strong> chráni pred zachytením o bubon a iné kusy.",
        ],
        "state_rows": [
            ("jemný prach", "vytriasť a vetrať", "nie vždy prať"),
            ("lokálna škvrna", "čistiť bodovo", "netrieť sieťku"),
            ("závoj", "ručné pranie podľa štítku", "lem a aplikácie"),
            ("kostým", "oddeliť suchý zips", "riziko zatrhnutia"),
        ],
        "textile_rows": [
            ("tylová sukňa", "voda, jemný gél, minimum trenia", "objem a sieťka"),
            ("závoj", "ručné pranie alebo čistiareň", "citlivá hodnota kusu"),
            ("spodnička", "jemný program vo vrecku", "viac vrstiev"),
            ("kostým", "skontrolovať ozdoby", "lepidlá a aplikácie"),
        ],
        "sections": [
            ("Ako prať tylovú sukňu v ruke", "Tylovú sukňu najprv vytraste a skontrolujte zatrhnutia. Vo vlažnej vode použite malé množstvo pracieho gélu a látku len jemne pretláčajte. Netrite sieťku o seba a nekrúťte ju. Ak je špinavá len malá časť, čistite skôr lokálne.", "Po praní nechajte vodu odtiecť a sukňu jemne vytvarujte. Tyl potrebuje priestor, nie tlak."),
            ("Ako prať závoj", "Závoj je často citlivejší než bežná tylová sukňa, pretože môže mať lem, čipku, korálky alebo symbolickú hodnotu. Ak štítok nie je jasný alebo ide o svadobný kus, čistiareň môže byť bezpečnejšia než domáce pranie.", "Ak ho periete doma, používajte minimum mechaniky a nikdy ho nedávajte k zipsom alebo háčikom."),
            ("Tyl v práčke: kedy áno a kedy nie", "Práčka je možná len pri menej citlivých kusoch, ak to povoľuje štítok. Použite ochranné vrecko, jemný program, nízke otáčky a samostatnú alebo veľmi šetrnú dávku. Suchý zips, zipsy a kovové časti držte mimo.", "Pri viacvrstvovej sukni sa oplatí prať radšej ručne, aby sa vrstvy nezamotali."),
            ("Ako sušiť tyl bez deformácie", "Tyl nesušte horúco. Nechajte ho odkvapkať a potom ho rozložte alebo zaveste tak, aby sa hmotnosť nerozložila do jedného bodu. Pri závoji dávajte pozor na lem a aplikácie.", "Ak tyl po praní splasol, jemné vytvarovanie pri sušení pomôže viac než agresívne žehlenie."),
            ("Tyl, flitre a aplikácie", "Mnohé spoločenské a detské kúsky kombinujú tyl s flitrami, korálkami alebo lepenými aplikáciami. Tie môžu reagovať na vodu, teplo aj trenie. Pred praním skontrolujte, či sú pevne uchytené a či štítok povoľuje domáce pranie.", "Súvisiaca téma je <a href=\"/n/ako-prat-oblecenie-s-flitrami-koralkami-a-aplikaciami\">ako prať oblečenie s flitrami a korálkami</a>."),
        ],
        "depth": [
            ("Prečo sa tyl trhá", "Tyl má štruktúru, ktorá sa ľahko zachytí o háčik, zips, suchý zips alebo ostrý okraj bubna. Malé zatrhnutie sa pri praní môže zväčšiť. Preto je kontrola pred praním dôležitejšia než pri hladkej bavlne.", "Ochranné vrecko znižuje riziko, ale nenahrádza čítanie štítku a oddelenie od drsných kusov."),
            ("Tyl a žehlenie", "Tyl sa môže teplom deformovať. Ak potrebuje vyrovnať, postupujte podľa štítku a veľmi opatrne. Pri mnohých kúskoch je bezpečnejšie parenie z odstupu alebo prirodzené vyrovnanie pri sušení než priamy kontakt žehličky.", "Pri závoji alebo spoločenských šatách sa oplatí neriskovať a zveriť úpravu odborníkovi."),
        ],
        "expert_title": "Odbornejší pohľad: sieťovaná štruktúra a mechanické zachytenie",
        "expert_p1": "Tyl sa pri praní neničí najmä chemicky, ale mechanicky. Jemná sieťka sa zachytí, pretrhne alebo zdeformuje. Preto pranie tylu nie je o silnejšom pracom účinku, ale o znížení kontaktu, trenia a ťahu.",
        "rule": "Pri tyle chráňte sieťku: minimum trenia, ochranné vrecko, žiadne zipsy a sušenie bez horúceho tepla.",
        "recommendation_intro": "Pri tyle používajte prací gél veľmi striedmo a iba vtedy, keď štítok povoľuje pranie. Rozhodujúce je znížiť trenie a zachovať tvar.",
        "product_text": "Vhodný na jemné pranie textílií, ak to povoľuje štítok. Pri závojoch, šatách a aplikáciách zvážte čistiareň.",
        "links": [
            ("/n/ako-prat-oblecenie-s-flitrami-koralkami-a-aplikaciami", "Ako prať oblečenie s flitrami a korálkami"),
            ("/n/ako-prat-spolocenske-saty-doma-a-kedy-zvolit-cistiaren", "Ako prať spoločenské šaty doma"),
            ("/n/ako-prat-sako-doma-a-kedy-ho-radsej-dat-do-cistiarne", "Ako prať sako doma"),
        ],
        "faq": [
            ("Môžem tyl prať v práčke?", "Len ak to povoľuje štítok. Použite ochranné vrecko, jemný program a nízke otáčky."),
            ("Ako prať svadobný závoj?", "Pri hodnotnom alebo zdobenom závoji je bezpečnejšia čistiareň. Doma len veľmi jemne podľa štítku."),
            ("Prečo sa tyl po praní sploští?", "Mohol byť stlačený, zle sušený alebo preťažený mechanikou. Pri sušení mu treba vrátiť tvar."),
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
        <p>{config["problem"].capitalize()}. Preto je dôležité rozlíšiť, či riešite tvar, pružnosť, časticovú škvrnu, kovové zafarbenie alebo jemnú sieťovanú štruktúru. Jeden silný program môže narobiť viac škody než úžitku.</p>
        <p>Pri textile ako {config["scope"]} rozhoduje materiál, konštrukcia, švy, aplikácie a spôsob sušenia. Najväčšie riziko je {config["avoid"]}.</p>
        {note_card("Rýchla praktická diagnostika", config["diagnosis"])}
        <h2>Postup podľa stavu textilu alebo škvrny</h2>
        {table(["Stav", "Čo urobiť", "Poznámka"], config["state_rows"])}
        <h2>Postup podľa typu textilu</h2>
        {table(["Textil", "Postup", "Prečo"], config["textile_rows"])}
        {sections}
        <h2>{config["expert_title"]}</h2>
        <p>{config["expert_p1"]}</p>
        <p>Pri citlivých materiáloch je najdôležitejšie neignorovať štítok. Symboly prania, zákaz sušičky, odporúčaná teplota a informácia o čistení nie sú formalita. Pomáhajú rozhodnúť, či má zmysel domáce pranie alebo je bezpečnejšia čistiareň.</p>
        {depth}
        <h2>Kontrolný checklist pred praním</h2>
        <p>Pred praním skontrolujte štítok, konštrukciu, švy, kovové časti, háčiky, aplikácie, mieru znečistenia a to, či je problém lokálny alebo na celom kuse. Pri jemnej bielizni sledujte košíky a elastan, pri sadziach voľný prach, pri kašmíri tvar, pri hrdzi zdroj a pri tyle zatrhnutia.</p>
        <p>Do jednej dávky nedávajte textílie s protichodnými potrebami. Čipkovaná podprsenka, pracovné nohavice so sadzami, kašmírový sveter a tylový závoj nepatria do rovnakého režimu prania. Triedenie je súčasť kvality výsledku.</p>
        <h2>Kedy postup zopakovať</h2>
        <p>Ak po praní zostal tieň, prach, pach, mastný pocit, deformácia alebo oslabená pružnosť, nesušte textil horúco. Najprv rozlíšte, či ide o nečistotu alebo zmenu materiálu. Opakovaný mierny postup býva bezpečnejší než jeden tvrdý zásah.</p>
        <p>Ak sa textil trhá, púšťa farbu, mení tvar alebo stráca funkciu, zastavte domáce experimentovanie skôr. Pri drahších a jemných kusoch je zachovanie materiálu dôležitejšie než agresívna snaha o okamžitý výsledok.</p>
        <h2>Ako predísť poškodeniu pri sušení</h2>
        <p>Sušenie často rozhodne o výsledku. Košíky sa môžu zlomiť, kašmír vytiahnuť, hrdza zafixovať, sadze zvýrazniť a tyl zdeformovať. Sušičku, radiátor a žehličku používajte iba vtedy, keď to štítok povoľuje a keď je textil po praní skontrolovaný.</p>
        <p>Pri jemných kusoch nechajte vodu odtiecť, tvarujte ich za vlhka a sušte voľne. Pri škvrnách najprv overte, že miesto je čisté, až potom pridajte teplo.</p>
        <h2>Domáca rutina pri citlivých kusoch</h2>
        <p>Ak sa podobné problémy opakujú, nastavte si jednoduchú rutinu: kontrola pred košom na bielizeň, oddelenie citlivých kusov, lokálne predčistenie, ochranné vrecko, primeraná dávka pracieho gélu, nepreplnený bubon a kontrola pred sušením.</p>
        <p>Pri jemnej bielizni a kašmíri pomáha prať menej agresívne. Pri sadziach a hrdzi pomáha najprv vyriešiť povrchový problém alebo zdroj. Pri tyle pomáha zabrániť zachyteniu skôr, než sa stane.</p>
        <h2>Čo sledovať po druhom praní</h2>
        <p>Ak ani druhé šetrné pranie nepomohlo, sledujte, či ide ešte o škvrnu alebo už o poškodenie. Sivý prach, oranžová hrdza, zlomený košík, splstnatený kašmír a natrhnutý tyl sú rozdielne problémy.</p>
        <p>Pri každom citlivom kuse si zapamätajte, ktorý postup bol bezpečný. Pri ďalšom praní tak nezačnete náhodným experimentom, ale overeným miernym režimom.</p>
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
    parser = argparse.ArgumentParser(description="Conservatively expand VEVO retrofit wave 20 delicate/soot/cashmere/rust/tulle articles.")
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
                "wave": "retrofit-wave-20-delicates-soot-cashmere-rust-tulle-five",
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
                "wave": "retrofit-wave-20-delicates-soot-cashmere-rust-tulle-five",
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
