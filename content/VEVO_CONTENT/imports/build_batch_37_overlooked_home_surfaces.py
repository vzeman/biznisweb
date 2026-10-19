import html
import json
import re
from pathlib import Path
from urllib.parse import urljoin

import requests


BASE = "https://www.vevo.sk"
PUBLISH_DATE = "2026-07-14"
OUT_JSON = Path("content/VEVO_CONTENT/imports/batch-37-2026-07-14-articles.json")
OUT_PREFLIGHT = Path("content/VEVO_CONTENT/exports/batch-37-2026-07-14-link-preflight.json")

PRODUCT_URL = "/p-1630/univerzalny-cistic-vevo-pure-harmony-500ml"
CATEGORY_URL = "/c/vevo-home-care/upratovanie/cistiace-prostriedky/univerzalny-cistic-do-domacnosti"
PARENT_CATEGORY_URL = "/c/vevo-home-care/upratovanie/cistiace-prostriedky"

EPA_DUST = "https://www.epa.gov/indoor-air-quality-iaq/sources-indoor-particulate-matter-pm"
EPA_HOME_AIR = "https://www.epa.gov/indoor-air-quality-iaq/what-can-i-do-improve-indoor-air-quality-my-home"
EPA_BIO = "https://www.epa.gov/indoor-air-quality-iaq/biological-contaminants-and-indoor-air-quality"
ELECTRICAL_SAFETY = "https://www.electricalsafetyfirst.org.uk/safety-advice/products-and-appliances/cleaning-clothing/steam-cleaners/"

FORBIDDEN_PUBLIC_RE = re.compile(
    r"longtail|long-tail|long tail|\bkeyword(?:s)?\b|kľúčov\w*\s+slov\w*|"
    r"\bSEO\b|search\s+intent|sub[- ]?quer(?:y|ies)|sub[- ]?query|fan[- ]?out|fanout|\bCTA\b",
    re.IGNORECASE,
)


def esc(value):
    return html.escape(str(value), quote=True)


def table(headers, rows):
    head = "".join(
        f'<th style="border: 1px solid #e5e5e5; padding: 10px; text-align: left;">{esc(header)}</th>'
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


def callout(title, bullets, background="#fffaf5", border="#e6ded2"):
    items = "".join(f"<li>{item}</li>" for item in bullets)
    return f"""
<div style="border: 1px solid {border}; border-radius: 8px; padding: 18px; margin: 22px 0; background: {background};">
<h2 style="margin-top: 0;">{esc(title)}</h2>
<ul>{items}</ul>
</div>
""".strip()


def product_blocks(article):
    return f"""
<div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 24px 0; background: #f7fbf8;">
<h2 style="margin-top: 0;">Praktické riešenie na kompatibilné umývateľné povrchy</h2>
<p>{article['product_intro']}</p>
<div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff; margin: 14px 0;">
<h3 style="margin-top: 0;">Univerzálny voňavý čistič Vevo Pure Harmony 500 ml</h3>
<p><strong>Kedy dáva zmysel:</strong> {article['product_use']}</p>
<p><strong>Kedy ho nepoužiť naslepo:</strong> {article['product_limit']}</p>
<p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="{PRODUCT_URL}">Pozrieť produkt</a></p>
</div>
</div>
<div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 24px 0; background: #fffaf5;">
<h2 style="margin-top: 0;">Vyberte čistič podľa povrchu, nie podľa sily vône</h2>
<p>Pri domácom upratovaní je najdôležitejšia kompatibilita s materiálom. Začnite malým množstvom, použite čistú handričku a prvý pokus urobte na skrytom mieste. Vôňa je až výsledný dojem po odstránení prachu a nečistôt.</p>
<p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="{CATEGORY_URL}">Pozrieť univerzálne čističe</a></p>
<p><a href="{PARENT_CATEGORY_URL}">Prejsť na všetky čistiace prostriedky</a></p>
</div>
""".strip()


def source_box(article):
    links = "".join(
        f'<li><a rel="noopener" href="{href}" target="_blank">{esc(label)}</a></li>'
        for label, href in article["sources"]
    )
    return f"""
<div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; margin: 22px 0; background: #fbfbfb;">
<h2 style="margin-top: 0;">Odbornejší pohľad a zdroje</h2>
<p>{article['source_intro']}</p>
<ul>{links}</ul>
</div>
""".strip()


def related_links(items):
    links = "".join(f'<li><a href="{href}">{esc(label)}</a></li>' for label, href in items)
    return f"<h2>Súvisiace návody na VEVO</h2>\n<ul>{links}</ul>"


def faq(title, items):
    parts = [f"<h2>FAQ: {esc(title)}</h2>"]
    for question, answer in items:
        parts.append(f"<h3>{esc(question)}</h3><p>{answer}</p>")
    return "\n".join(parts)


def render_article(article):
    parts = [
        f"<p><strong>Rýchla odpoveď:</strong> {article['answer']}</p>",
        f"<p>{article['short']}</p>",
        callout("Najprv si overte tieto body", article["quick"]),
        f"<h2>{esc(article['overview_heading'])}</h2>",
    ]
    parts.extend(f"<p>{paragraph}</p>" for paragraph in article["overview"])
    parts.append(table(["Povrch alebo situácia", "Hlavné riziko", "Bezpečný začiatok"], article["surface_rows"]))
    parts.append(f"<h2>{esc(article['prep_heading'])}</h2>")
    parts.extend(f"<p>{paragraph}</p>" for paragraph in article["prep"])
    parts.append("<h2>Postup krok za krokom</h2>")
    parts.append("<ol>" + "".join(f"<li>{step}</li>" for step in article["steps"]) + "</ol>")
    parts.append(f"<h2>{esc(article['diagnosis_heading'])}</h2>")
    parts.append(table(["Problém", "Pravdepodobná príčina", "Čo urobiť"], article["diagnosis_rows"]))
    for heading, paragraphs in article["sections"]:
        parts.append(f"<h2>{esc(heading)}</h2>")
        parts.extend(f"<p>{paragraph}</p>" for paragraph in paragraphs)
    parts.append(callout("Čo si zapamätať", article["remember"], background="#f7fbf8", border="#dbe5de"))
    parts.append("<h2>Najčastejšie chyby pri čistení</h2>")
    parts.append("<ul>" + "".join(f"<li>{item}</li>" for item in article["mistakes"]) + "</ul>")
    parts.append("<h2>Kedy prestať a zvoliť odbornú pomoc</h2>")
    parts.extend(f"<p>{paragraph}</p>" for paragraph in article["caution"])
    parts.append("<h2>Prečo funguje jemné vlhké čistenie lepšie než vírenie prachu</h2>")
    parts.extend(f"<p>{paragraph}</p>" for paragraph in article["expert"])
    parts.append(source_box(article))
    parts.append(product_blocks(article))
    parts.append(related_links(article["related"]))
    parts.append(faq(article["faq_title"], article["faq"]))
    return "\n".join(parts)


ARTICLES = [
    {
        "title": "Ako vyčistiť radiátor od prachu: rebrá, zadná strana, mastnota a bezpečná údržba",
        "link": "ako-vycistit-radiator-od-prachu-rebra-zadna-strana-mastnota-a-bezpecna-udrzba",
        "meta": "Návod, ako bezpečne vyčistiť radiátor od prachu a mastnoty. Postup pre rebrá, zadnú stranu, lakované plochy aj elektrické konvektory.",
        "short": "Radiátor čistite až po vypnutí a úplnom vychladnutí. Najprv odstráňte voľný prach vysávačom alebo úzkou kefou, potom utrite dostupné lakované plochy iba mierne vlhkou handričkou a všetko vysušte. Čistiaci prostriedok nikdy nestriekajte do elektrických častí, ventilov ani priamo medzi rebrá.",
        "answer": "Vypnite kúrenie, nechajte radiátor vychladnúť a chráňte podlahu aj stenu. Prach z rebier a zadnej strany najprv vysajte úzkym nadstavcom alebo uvoľnite kefou smerom nadol. Až potom utrite pevné umývateľné plochy mierne vlhkou handričkou. Pri elektrickom radiátore sa riaďte návodom výrobcu, odpojte ho od napájania a neotvárajte kryt.",
        "quick": [
            "<strong>Teplota:</strong> radiátor musí byť vypnutý a studený na dotyk.",
            "<strong>Prach:</strong> najprv ho zachyťte vysávačom, nerozfúkavajte ho po izbe.",
            "<strong>Voda:</strong> handrička má byť mierne vlhká, nie mokrá a kvapkajúca.",
            "<strong>Elektrina:</strong> elektrický radiátor alebo konvektor nečistite pod napätím a neotvárajte ho.",
            "<strong>Povrch:</strong> nový prostriedok skúste na skrytom kúsku laku.",
        ],
        "overview_heading": "Prečo sa radiátor zanáša prachom aj v upratovanej domácnosti",
        "overview": [
            "Radiátor vytvára prúdenie vzduchu: chladnejší vzduch pri podlahe sa ohrieva a stúpa. Spolu s ním sa pohybujú drobné častice prachu, textilné vlákna, peľ a chlpy. Časť sa zachytí na hornej hrane, medzi lamelami, na zadnej strane aj pri konzolách. Preto môže byť predná plocha relatívne čistá, kým vnútro panelového radiátora drží súvislú vrstvu nečistôt.",
            "Prach na radiátore nie je dôvod na agresívnu dezinfekciu. Vo väčšine domácností ide najmä o mechanické odstránenie usadenín. Dôležité je zvoliť postup, ktorý prach zachytí namiesto toho, aby ho rozvíril. Prudké fúkanie zhora bez vysávača pod radiátorom síce uvoľní nečistoty, ale veľká časť skončí vo vzduchu, na závesoch a na posteľných textíliách.",
            "Iný problém vzniká v kuchyni alebo blízko jedálenského kúta. Prach sa môže spojiť s jemným mastným filmom a na lakovanom povrchu vytvorí sivú lepkavú vrstvu. Vtedy suchá prachovka nestačí. Stále však platí, že najprv treba odstrániť voľný prach a až potom pracovať s mierne vlhkou handričkou a malým množstvom prípravku vhodného na konkrétny lak.",
            "Rozhodujúci je typ vykurovacieho telesa. Panelový radiátor má úzke konvekčné plechy, článkový radiátor hlboké medzery, kúpeľňový rebrík zvary a spoje a elektrický konvektor navyše elektrické prvky. Jeden univerzálny pohyb ani jedna pomôcka preto nefungujú rovnako všade. Bezpečný postup začína identifikáciou typu radiátora a návodom výrobcu.",
        ],
        "surface_rows": [
            ("Panelový radiátor", "prach medzi konvekčnými plechmi", "úzky nadstavec, mäkká kefa a vysávač pod radiátorom"),
            ("Liatinový alebo článkový radiátor", "hlboké medzery a starší citlivý náter", "kefa obalená mäkkou handričkou, bez drhnutia odlupujúcej farby"),
            ("Kúpeľňový rebrík", "prach pri zvaroch a vlhkosť", "mikrovlákno okolo každej rúrky a dôkladné vysušenie"),
            ("Radiátor pri kuchyni", "mastný film viažuci prach", "najprv suché odstránenie, potom jemné odmastenie po skúške"),
            ("Elektrický konvektor", "napätie, vetracie otvory a citlivá elektronika", "odpojiť podľa návodu a čistiť iba povolené vonkajšie časti"),
        ],
        "prep_heading": "Ako pripraviť izbu a radiátor pred čistením",
        "prep": [
            "Kúrenie vypnite s dostatočným predstihom. Na horúcom laku môže voda zasyčať, čistiaci roztok zasychať nerovnomerne a pri dotyku hrozí popálenie. Pod radiátor položte starý uterák alebo umývateľnú podložku a medzi radiátor a stenu môžete zasunúť tenký kartón. Zachytíte prach aj prípadné kvapky bez toho, aby ste si vytvorili ďalšiu škvrnu na podlahe.",
            "Pripravte si vysávač s úzkym nadstavcom, mäkkú dlhú kefu na radiátory, dve čisté handričky a nádobu s čistou vodou. Jedna handrička slúži na umývanie, druhá na okamžité osušenie. Nepoužívajte kovové predmety, ktoré môžu poškriabať lak alebo zachytiť kábel. Ak je náter popraskaný, hrdzavý alebo sa odlupuje, čistenie obmedzte na jemné odsatie prachu a ďalší postup riešte podľa stavu povrchu.",
        ],
        "steps": [
            "Vypnite vykurovanie a počkajte, kým je celé teleso studené.",
            "Odložte predmety z parapetu a okolia, pod radiátor rozprestrite ochrannú textíliu.",
            "Povysávajte hornú hranu, priestor pod radiátorom a ľahko dostupné otvory.",
            "Dlhú mäkkú kefu veďte medzi rebrami zhora nadol; uvoľnený prach priebežne vysávajte.",
            "Zadnú stranu čistite úzkou kefou alebo handričkou na plochom bezpečnom držiaku bez tlačenia na ventily.",
            "Prednú a bočné umývateľné plochy utrite dobre vyžmýkanou handričkou.",
            "Mastný film ošetrite malým množstvom kompatibilného čističa naneseného na handričku, nie priamo na radiátor.",
            "Povrch utrite čistou vodou, vysušte a kúrenie zapnite až po úplnom preschnutí.",
        ],
        "diagnosis_heading": "Čo robiť pri mastnote, zápachu, hrdzi alebo odlupujúcom sa nátere",
        "diagnosis_rows": [
            ("Sivý lepkavý povlak", "prach spojený s kuchynskou mastnotou", "pracovať po malých plochách a čistič dávať na handričku"),
            ("Prach padá aj po čistení", "nevyčistené vnútorné rebrá alebo zadná strana", "zopakovať suchú fázu s vysávačom pod telesom"),
            ("Zatuchnutý pach", "prach, vlhká handrička alebo zdroj za radiátorom", "skontrolovať stenu, podlahu a úplné vysušenie"),
            ("Bodky hrdze", "poškodený náter alebo dlhodobá vlhkosť", "nedrhnúť naslepo, riešiť náter alebo servis"),
            ("Odlupujúca sa farba", "starý alebo tepelne poškodený náter", "nepoužívať abrazívum a zvážiť odbornú obnovu"),
        ],
        "sections": [
            ("Ako vyčistiť radiátor medzi rebrami bez rozfúkania prachu", [
                "Najlepšia kombinácia je kefa, ktorá prach uvoľní, a vysávač, ktorý ho okamžite zachytí. Hubicu držte pri spodnom okraji alebo na uteráku pod radiátorom a kefou postupujte po menších úsekoch. Ak použijete iba fén alebo stlačený vzduch, častice sa rozptýlia do miestnosti a časť sa neskôr vráti späť na vykurovacie teleso.",
                "Pri panelovom radiátore netlačte kefu silou do tenkých plechov. Ohnuté lamely sa čistia horšie a môžu znižovať priechod vzduchu. Ak sa horná mriežka podľa návodu výrobcu jednoducho odníma bez náradia, môžete postupovať podľa návodu. Ak si tým nie ste istí, kryt nerozoberajte a využite prístupné otvory.",
            ]),
            ("Ako vyčistiť zadnú stranu radiátora a stenu za ním", [
                "Za radiátorom sa prach usádza na stene, konzolách aj potrubí. Použite úzku mäkkú kefu alebo plochú pomôcku obalenú čistou handričkou. Pohyb má byť zhora nadol a bez prudkého narážania do ventilov. Na stene najprv skúste suché odstránenie; mokrá handrička môže zo sypkého prachu vytvoriť tmavú mapu.",
                "Ak je za radiátorom viditeľná vlhkosť, pleseň alebo opakované tmavnutie, nejde už len o upratovanie. Skontrolujte kondenzáciu, netesnosť a stav steny. Samotné prevoňanie alebo rýchle zotretie príčinu nevyrieši a pri úniku vody treba vykurovanie riešiť so servisom.",
            ]),
            ("Ako odstrániť mastnotu z radiátora v kuchyni", [
                "Mastný radiátor čistite v dvoch fázach. Najprv vysajte prach, aby ste ho pri mokrom čistení nerozotreli do sivého filmu. Potom na skrytom mieste otestujte malé množstvo jemného univerzálneho čističa. Nanášajte ho na handričku a pracujte po jednej ploche; roztok nesmie stekať do medzier ani na termostatickú hlavicu.",
                "Po odmastení prejdite povrch handričkou navlhčenou čistou vodou a osušte ho. Silné rozpúšťadlá, drôtenka a hrubá čistiaca pasta môžu zmatniť alebo poškriabať lak. Ak škvrna nepovolí po jemnom opakovaní, bezpečnejšie je zmieriť sa s miernym tieňom než poškodiť celý náter.",
            ]),
            ("Ako často čistiť radiátor pri alergii, zvieratách a počas vykurovacej sezóny", [
                "Frekvencia závisí od množstva prachu v domácnosti. Pri zvieratách, kobercoch, otvorených oknách počas peľovej sezóny alebo alergii má zmysel hornú hranu a priestor pod radiátorom vysávať častejšie. Hĺbkové čistenie plánujte pred vykurovacou sezónou a zopakujte ho, ak pri prvom zapnutí cítiť prach alebo sa nečistoty viditeľne vracajú.",
                "Súčasne perte alebo vysávajte textílie v okolí. Záves, posteľný prehoz či koberec môžu byť významnejším zdrojom častíc než samotný radiátor. Pomôžu súvisiace návody na <a href=\"/n/ako-odstranit-prach-z-textilii-po-malovani-alebo-rekonstrukcii\">odstránenie prachu z textílií</a> a na <a href=\"/n/ako-cistit-textilne-tienidla-lamp-a-dekoracie-od-prachu\">čistenie textilných tienidiel a dekorácií</a>.",
            ]),
            ("Ako vyčistiť elektrický radiátor alebo konvektor", [
                "Pri elektrickom vykurovacom telese má prednosť návod výrobcu. Spotrebič vypnite, odpojte od siete, nechajte vychladnúť a čistite iba povrchy, ktoré výrobca povoľuje. Vetracie štrbiny nevymývajte, nestriekajte do nich čistič a nepoužívajte paru. Vlhkosť pri elektrických častiach je riziko, nie spôsob dôkladnejšieho upratovania.",
                "Ak spotrebič zapácha spáleninou, iskri, má poškodený kábel alebo sa neobvykle prehrieva, čistením problém neriešte. Prestaňte ho používať a obráťte sa na servis. Prach na vonkajšom kryte je údržba; porucha vo vnútri zariadenia patrí odborníkovi.",
            ]),
        ],
        "remember": [
            "Najprv prach zachyťte nasucho, až potom používajte mierne vlhkú handričku.",
            "Čistič patrí na handričku, nie do rebier, ventilov alebo vetracích otvorov.",
            "Hrdza, odlupujúci sa náter, únik vody alebo elektrická porucha nie sú bežné upratovanie.",
        ],
        "mistakes": [
            "Čistenie ešte horúceho radiátora.",
            "Rozfúkanie prachu fénom bez súčasného zachytávania vysávačom.",
            "Priame nastriekanie prípravku medzi rebrá alebo na ventil.",
            "Použitie drôtenky, hrubej pasty alebo rozpúšťadla na lakovaný povrch.",
            "Zasunutie kovového predmetu do elektrického konvektora.",
            "Zapnutie kúrenia skôr, než sú plochy a okolie úplne suché.",
        ],
        "caution": [
            "Ak vidíte únik vody, mokrú stenu, silnú koróziu alebo uvoľnený ventil, zastavte čistenie a riešte technický stav vykurovania. Kozmetické zotretie nečistôt nesmie prekryť poruchu, ktorá môže poškodiť podlahu alebo stenu.",
            "Elektrický radiátor s poškodeným káblom, prasknutým krytom, iskrením alebo zápachom po spálenine nepoužívajte. Neotvárajte ho a neskúšajte vnútro umývať. Servis je v tejto situácii bezpečnejší než domáca oprava.",
        ],
        "expert": [
            "EPA uvádza, že vnútorný prach môže obsahovať peľ, kožné častice, zvieracie alergény a ďalšie biologické zložky. Chôdza, zametanie aj suché utieranie ho môžu znovu zdvihnúť do vzduchu. Pri radiátore preto dáva zmysel kombinovať pomalé uvoľnenie usadenín s ich okamžitým odsávaním.",
            "Mierne navlhčená handrička pomáha zachytiť zvyšný prach na dostupnom tvrdom povrchu. Neznamená to však, že treba radiátor premáčať. Cieľom je viazať častice v handričke a súčasne chrániť náter, stenu, podlahu a technické časti.",
            "Kvalitu vzduchu nevyrieši jedna pomôcka. Najlepší výsledok vzniká kombináciou kontroly zdrojov prachu, vetrania, primeranej filtrácie a pravidelného upratovania textílií aj tvrdých plôch. Radiátor je jedna časť tejto rutiny, nie jediný vinník.",
        ],
        "source_intro": "Odborné zdroje podporujú časté zachytávanie prachu, používanie vlhkej handričky na tvrdé plochy a riešenie zdrojov častíc v celej miestnosti. Bezpečnostné pokyny výrobcu vykurovacieho telesa majú vždy prednosť.",
        "sources": [
            ("US EPA: Sources of Indoor Particulate Matter", EPA_DUST),
            ("US EPA: What can I do to improve indoor air quality in my home?", EPA_HOME_AIR),
        ],
        "product_intro": "Na studenú, nepoškodenú a umývateľnú lakovanú plochu môže po odstránení prachu dávať zmysel jemný univerzálny čistič. Vždy ho najprv skúste na skrytom mieste a používajte iba malé množstvo na handričke.",
        "product_use": "na odstránenie ľahkého mastného filmu z kompatibilnej vonkajšej plochy radiátora, parapetu alebo umývateľnej steny v jeho okolí po predchádzajúcej skúške.",
        "product_limit": "na horúci povrch, odlupujúci sa náter, hrdzu, ventily, elektrické časti, vetracie otvory ani vnútro konvektora.",
        "related": [
            ("Ako odstrániť prach z textílií po maľovaní alebo rekonštrukcii", "/n/ako-odstranit-prach-z-textilii-po-malovani-alebo-rekonstrukcii"),
            ("Ako čistiť textilné tienidlá lámp a dekorácie od prachu", "/n/ako-cistit-textilne-tienidla-lamp-a-dekoracie-od-prachu"),
            ("Ako umyť podlahu bez šmúh v praxi", "/n/ako-umyt-podlahu-bez-smuh-laminat-vinyl-dlazba-a-mopovanie-v-praxi"),
        ],
        "faq_title": "čistenie radiátora",
        "faq": [
            ("Môžem radiátor umyť vodou?", "Dostupné vonkajšie umývateľné plochy môžete utrieť dobre vyžmýkanou handričkou. Radiátor však nepremáčajte a voda nesmie stekať do ventilov, spojov ani elektrických častí."),
            ("Ako dostať prach spoza radiátora?", "Použite dlhú mäkkú kefu alebo plochú pomôcku obalenú handričkou. Postupujte zhora nadol a uvoľnený prach zachytávajte vysávačom."),
            ("Čím odmastiť radiátor v kuchyni?", "Po odstránení voľného prachu skúste na skrytom mieste malé množstvo jemného čističa na handričke. Nepoužívajte agresívne rozpúšťadlá ani abrazívne pomôcky."),
            ("Môžem radiátor po čistení hneď zapnúť?", "Nie. Najprv osušte povrch aj okolie a skontrolujte, že nikde nezostala vlhkosť. Až potom vykurovanie znovu zapnite."),
        ],
    },
    {
        "title": "Ako vyčistiť parapety a okenné rámy: prach, peľ, čierne mapy a škvrny",
        "link": "ako-vycistit-parapety-a-okenne-ramy-prach-pel-cierne-mapy-a-skvrny",
        "meta": "Praktický návod na čistenie parapetov a okenných rámov od prachu, peľu, čiernych máp a škvŕn podľa materiálu, bez zbytočného poškodenia.",
        "short": "Parapet a rám okna najprv povysávajte, aby sa prach a peľ pri navlhčení nezmenili na sivé blato. Potom ich umývajte po malých častiach dobre vyžmýkanou handričkou a prípravkom vhodným pre PVC, lakované drevo, hliník alebo kameň. Čierne mapy pri tesnení môžu signalizovať kondenzáciu, preto nestačí iba prevoňať alebo pretrieť povrch.",
        "answer": "Odložte dekorácie, vysajte rohy, vodiace drážky a voľné nečistoty. Okenný rám aj parapet utrite mierne vlhkou handričkou od čistejších miest k špinavším. Na PVC nepoužívajte drsné abrazíva, lakované drevo nepremáčajte a hliník chráňte pred agresívnymi kyselinami či zásadami. Tesnenia a odtokové otvory čistite jemne bez poškodenia.",
        "quick": [
            "<strong>Materiál:</strong> PVC, drevo, hliník a kameň potrebujú odlišné prípravky.",
            "<strong>Peľ:</strong> najprv ho vysajte alebo zotrite nasucho, inak vytvorí lepkavú vrstvu.",
            "<strong>Čierne mapy:</strong> skontrolujte kondenzáciu a vlhkosť, nielen povrchovú špinu.",
            "<strong>Tesnenie:</strong> nevyťahujte ho a nepoužívajte ostré predmety v drážkach.",
            "<strong>Sušenie:</strong> rámy a rohy po umytí osušte, najmä pri dreve a MDF parapete.",
        ],
        "overview_heading": "Prečo sa parapety a rámy špinia rýchlejšie než sklo",
        "overview": [
            "Parapet je vodorovná plocha, na ktorej sa zachytáva prach, peľ, textilné vlákna, zemina z kvetináčov aj kvapky po polievaní. Okenný rám má zase rohy, tesnenia, kovanie a odtokové drážky. Nečistoty sa v nich držia dlhšie než na hladkom skle a pri prvom daždi alebo kondenzácii sa môžu zmeniť na tmavé mapy.",
            "Najčastejšia chyba je začať mokrou handričkou. Suchý peľ a jemný prach sa spoja s vodou a vytvoria film, ktorý treba opakovane oplachovať. Rýchlejšie je najprv použiť mäkkú kefku a vysávač, až potom vlhké čistenie. Platí to najmä na jar, pri rušnej ceste a v domácnosti, kde sa okná často vetrajú dokorán.",
            "Čierna či sivá škvrna pri spodnom rohu rámu nemusí byť iba bežná špina. Môže ísť o usadeniny pri kondenzácii, začínajúci biologický rast alebo nečistotu prenesenú z tesnenia. Ak sa mapa vracia krátko po umytí, treba riešiť vetranie, vlhkosť, funkciu odtokov a prípadný tepelný most. Silnejší čistič bez odstránenia príčiny prinesie iba krátky efekt.",
            "Materiál rozhoduje o limite čistenia. Biele PVC môže poškriabať drsná hubka, lakované drevo môže napučať pri dlhom kontakte s vodou, eloxovaný hliník môže reagovať na nevhodnú chémiu a prírodný kameň nemusí znášať kyslé prostriedky. Univerzálna rada preto znie: najprv identifikovať povrch, prečítať etiketu a urobiť skúšku na menej viditeľnom mieste.",
        ],
        "surface_rows": [
            ("Biele PVC", "škrabance, zažltnutie a sivý film", "mäkká handrička, jemný čistič a žiadny drsný prášok"),
            ("Lakované drevo", "napučanie hrán a poškodenie laku", "minimum vody, rýchle osušenie a prostriedok vhodný na náter"),
            ("Hliníkový rám", "mapy a reakcia na agresívnu chémiu", "neutrálny postup podľa výrobcu a čistá mäkká handrička"),
            ("Kamenný parapet", "škvrny a citlivosť niektorých kameňov na kyseliny", "overiť typ kameňa, nepoužiť ocot naslepo"),
            ("Laminovaný alebo MDF parapet", "voda pri spojoch a hranách", "dobre vyžmýkaná handrička a okamžité vysušenie"),
        ],
        "prep_heading": "Príprava okna bez neporiadku na podlahe a stenách",
        "prep": [
            "Z parapetu zložte kvetináče, sviečky a dekorácie. Dno kvetináčov skontrolujte oddelene, pretože mokrá zemina alebo hrdzavý kovový obal môže po návrate okamžite vytvoriť nový kruh. Pod okno položte uterák a pripravte si úzky vysávač, mäkkú kefku, dve handričky a čistú vodu. Ak čistíte aj sklo, rám riešte pred finálnym leštením skla.",
            "Otvorte okno iba v bezpečnej polohe a nesiahajte von z výšky bez stabilného prístupu. Voľný prach vysajte z rohov, kovania a drážok. Do odtokových otvorov nestrkajte kovové ihly ani skrutkovače; jemne odstráňte viditeľnú nečistotu mäkkou tyčinkou alebo kefkou podľa konštrukcie okna. Kovanie nemažte náhodným olejom, ak výrobca neurčuje vhodný typ údržby.",
        ],
        "steps": [
            "Odložte predmety a povysávajte parapet, rohy rámu, tesnenie aj dostupné drážky.",
            "Na skrytom mieste overte, že čistič nemení farbu ani lesk povrchu.",
            "Začnite hornou a bočnou časťou rámu, aby špinavá voda nestekala na hotový parapet.",
            "Handričku často preplachujte; špinavá handrička iba rozotiera sivý film.",
            "Tesnenie utrite jemne bez naťahovania a ostrých pomôcok.",
            "Parapet čistite od vnútorného okraja smerom k oknu a škvrny riešte lokálne.",
            "Zvyšky prípravku odstráňte čistou mierne vlhkou handričkou.",
            "Rám, spoje, tesnenie a parapet dôkladne osušte, až potom vráťte dekorácie.",
        ],
        "diagnosis_heading": "Ako rozlíšiť bežnú špinu, peľ, kondenzáciu a poškodenie povrchu",
        "diagnosis_rows": [
            ("Žltý jemný povlak", "peľ a prach z otvoreného okna", "najprv vysať, potom umyť bez rozmazania"),
            ("Čierne bodky pri tesnení", "nečistota pri kondenzácii alebo biologický rast", "vyčistiť, vysušiť a sledovať vlhkosť"),
            ("Sivá mapa na PVC", "rozotretý prach alebo prenesená guma", "čistá handrička a jemný prostriedok po skúške"),
            ("Kruh pod kvetináčom", "voda, zemina alebo korózia obalu", "ošetriť podľa materiálu parapetu a zabrániť opakovaniu"),
            ("Napučaná hrana", "dlhodobá vlhkosť v MDF alebo dreve", "nepremáčať a riešiť poškodenie materiálu"),
        ],
        "sections": [
            ("Ako vyčistiť biele plastové okenné rámy bez poškriabania", [
                "Na PVC používajte mäkké mikrovlákno alebo bavlnenú handričku. Drsná strana hubky, abrazívny prášok a tvrdá pasta môžu vytvoriť mikroškrabance, v ktorých sa neskôr špina drží ešte viac. Pri staršom ráme najprv skúste malú plochu, pretože zmena farby nemusí byť nečistota, ale starnutie materiálu.",
                "Ak ostane sivý tieň, zopakujte jemné čistenie s čistou handričkou namiesto zvyšovania sily. Rozpúšťadlá môžu narušiť povrch, tesnenie alebo potlač. Pri samolepkách a lepidle postupujte podľa odporúčania výrobcu okna a neodstraňujte ich kovovou škrabkou.",
            ]),
            ("Ako čistiť drevené rámy a parapety bez napučania", [
                "Drevený rám nevnímajte ako kus surového dreva, ale ako lakovaný alebo olejovaný systém. Prípravok musí byť kompatibilný s povrchovou úpravou. Handričku dobre vyžmýkajte, nenechávajte na ráme kaluže a po každom úseku ho osušte. Najcitlivejšie sú spoje, spodné hrany a miesta s poškodeným náterom.",
                "Ak sa lak odlupuje alebo drevo tmavne pod povrchom, ďalšie drhnutie situáciu nezlepší. Povrch môže potrebovať obnovu náteru a kontrolu zatekania. Pravidelné jemné utieranie je prevencia; dlhodobé pôsobenie kondenzátu je technický problém.",
            ]),
            ("Ako odstrániť peľ a prach z parapetu počas jari", [
                "Počas peľovej sezóny čistite parapet častejšie a nezačínajte mokrou handričkou. Vysávač s mäkkým nadstavcom zachytí väčšinu častíc bez ich rozmazania. Potom stačí krátke vlhké zotretie. Ak je v domácnosti alergik, handričku po práci vyperte a prach nevyklepávajte v interiéri.",
                "EPA odporúča pri znižovaní vnútorného prachu pravidelné vysávanie a utieranie vlhkou handričkou. Zmysel má aj kontrola prísunu peľu: počas dní s vysokou koncentráciou nevetrávať zbytočne dlho a po vetraní utrieť vodorovné plochy. Okná a parapety sú pri tejto rutine prvou líniou, nie jediným miestom upratovania.",
            ]),
            ("Čierne mapy okolo okna: kedy nestačí čistič", [
                "Ak sa čierna mapa objavuje najmä v chladných rohoch, sledujte kondenzáciu. Po sprchovaní, varení alebo sušení bielizne v byte stúpa vlhkosť a na chladnom ráme sa môže zrážať voda. Povrch očistite a vysušte, ale zároveň vetrajte, používajte odsávanie a sledujte, či voda nestojí na parapete.",
                "Rozsiahly, opakujúci sa alebo zapáchajúci rast môže vyžadovať odborné posúdenie vlhkosti a stavebnej príčiny. Nemiešajte čistiace chemikálie a nepoužívajte chlór na materiál, pre ktorý nie je určený. Silná vôňa iba prekryje problém na krátky čas.",
            ]),
            ("Ako spojiť čistenie rámov s umývaním okien bez šmúh", [
                "Praktické poradie je: odložiť predmety, povysávať rám a parapet, umyť rám, vyčistiť sklo a nakoniec skontrolovať spodnú hranu. Ak najprv vyleštíte sklo a potom umývate špinavý rám, kvapky vám výsledok pokazia. Na detailný postup použite aj návod <a href=\"/n/ako-umyt-okna-bez-smuh-kompletny-sprievodca\">ako umyť okná bez šmúh</a>.",
                "Na sklo a rám nemusí byť vhodný rovnaký produkt. Čistič skla má pomáhať s odparovaním bez šmúh, kým rám potrebuje kompatibilitu s PVC, lakom alebo kovom. Nestriekajte prípravok tak, aby ste ho vdychovali alebo aby ste zasiahli textílie; dávkujte ho cielene na handričku alebo podľa etikety.",
            ]),
        ],
        "remember": [
            "Peľ a prach odstráňte pred mokrým čistením, inak ich rozotriete.",
            "Čierna mapa, ktorá sa vracia, je signál na kontrolu vlhkosti a kondenzácie.",
            "PVC, drevo, hliník, kameň a MDF nie sú jeden povrch a neznášajú rovnakú chémiu.",
        ],
        "mistakes": [
            "Začať mokrou handričkou na silnej vrstve peľu.",
            "Použiť drsnú hubku na biele PVC.",
            "Premáčať drevený alebo MDF parapet pri spojoch.",
            "Čistiť prírodný kameň octom bez overenia jeho odolnosti.",
            "Vyťahovať tesnenie alebo čistiť drážky ostrým kovovým predmetom.",
            "Prekryť opakovanú vlhkosť vôňou namiesto riešenia kondenzácie.",
        ],
        "caution": [
            "Ak je rám deformovaný, drevo mäkké, náter sa odlupuje alebo voda preniká do steny, zastavte kozmetické čistenie a riešte príčinu. Problém môže byť v tesnení, montáži, odtoku alebo stavebnom detaile.",
            "Pri práci vo výške nepoužívajte nestabilnú stoličku a nesiahajte ďaleko von z otvoreného okna. Vonkajšiu stranu vysokých okien, ku ktorej nemáte bezpečný prístup, nechajte profesionálom.",
        ],
        "expert": [
            "Vodorovné plochy pri okne zachytávajú častice prichádzajúce zvonka aj prach z interiéru. Suché prudké utieranie môže časť z nich znovu zdvihnúť. Preto má zmysel kombinácia vysatia a následného jemného vlhkého zotretia.",
            "EPA pri kvalite vnútorného vzduchu zdôrazňuje kontrolu zdrojov znečistenia, vetranie a filtráciu. V praxi to znamená, že samotné čistenie parapetu nestačí, ak sa v byte dlhodobo hromadí vlhkosť, suší veľa bielizne bez vetrania alebo sa peľ dostáva dnu počas celého dňa.",
            "Dôkladnosť neznamená viac vody. Na citlivých rámoch je kvalitné čistenie presné: čistá handrička, správny produkt, malé množstvo vlhkosti, časté oplachovanie a vysušenie spojov. Takýto postup chráni materiál a znižuje riziko nových máp.",
        ],
        "source_intro": "Zdroje k vnútornému prachu a biologickým časticiam podporujú pravidelné vysávanie, vlhké utieranie tvrdých plôch a kontrolu vlhkosti. Pri konkrétnom okne má vždy prednosť návod výrobcu rámu, tesnenia a parapetu.",
        "sources": [
            ("US EPA: Sources of Indoor Particulate Matter", EPA_DUST),
            ("US EPA: Biological Contaminants and Indoor Air Quality", EPA_BIO),
        ],
        "product_intro": "Univerzálny čistič môže pomôcť na kompatibilnom PVC, lamináte alebo lakovanom povrchu, ale nie je automaticky vhodný na každý kameň, drevený náter či eloxovaný kov. Skúška na skrytom mieste je povinná súčasť postupu.",
        "product_use": "na bežný sivý film, odtlačky a ľahké nečistoty na umývateľnom parapete alebo ráme, ak etiketa povoľuje daný materiál.",
        "product_limit": "na prírodný kameň citlivý na zloženie, poškodený lak, napučané drevo, tesnenie bez overenia ani na odstraňovanie plesne z neznámej príčiny.",
        "related": [
            ("Ako umyť okná bez šmúh", "/n/ako-umyt-okna-bez-smuh-kompletny-sprievodca"),
            ("Ako prať záclony v kuchyni od mastnoty a pachov", "/n/ako-prat-zaclony-v-kuchyni-od-mastnoty-a-pachov"),
            ("Ako odstrániť prach z textílií po rekonštrukcii", "/n/ako-odstranit-prach-z-textilii-po-malovani-alebo-rekonstrukcii"),
        ],
        "faq_title": "čistenie parapetov a okenných rámov",
        "faq": [
            ("Čím vyčistiť biele plastové okenné rámy?", "Použite mäkkú handričku a jemný prípravok vhodný na PVC. Vyhnite sa drsným hubkám, abrazívnemu prášku a rozpúšťadlám bez súhlasu výrobcu."),
            ("Ako odstrániť čierne bodky pri tesnení?", "Najprv povrch vyčistite a vysušte. Ak sa bodky vracajú, sledujte kondenzáciu, vlhkosť a funkciu odtokov; opakovaný rast môže potrebovať odborné posúdenie."),
            ("Môžem kamenný parapet čistiť octom?", "Nie naslepo. Niektoré prírodné kamene sú citlivé na kyseliny. Najprv overte typ kameňa a odporúčanie výrobcu."),
            ("Ako často čistiť parapet počas peľovej sezóny?", "Podľa množstva peľu aj niekoľkokrát týždenne. Najprv častice vysajte a až potom použite mierne vlhkú handričku."),
        ],
    },
    {
        "title": "Ako vyčistiť interiérové dvere a zárubne: odtlačky, mastnota a povrch bez šmúh",
        "link": "ako-vycistit-interierove-dvere-a-zarubne-odtlacky-mastnota-a-povrch-bez-smuh",
        "meta": "Ako vyčistiť interiérové dvere a zárubne od odtlačkov, mastnoty a šmúh. Bezpečný postup pre laminát, fóliu, dyhu, lak aj sklo.",
        "short": "Interiérové dvere čistite podľa povrchovej úpravy, nie podľa farby. Laminát a fólia znesú dobre vyžmýkanú handričku, dyha a lakované drevo potrebujú minimum vody a matné povrchy môžu po nevhodnom leštení zostať fľakaté. Najprv zotrite prach, potom odtlačky a mastnotu, nakoniec povrch osušte v smere jeho štruktúry.",
        "answer": "Z dverí najprv odstráňte prach mäkkou suchou handričkou, najmä z hornej hrany a zárubní. Na skrytom mieste otestujte mierne vlhkú handričku s jemným kompatibilným čističom. Postupujte zhora nadol, okolie kľučky čistite častejšie a pri spodnej hrane nepoužívajte veľa vody. Dvere nakoniec utrite čistou handričkou a vysušte.",
        "quick": [
            "<strong>Povrch:</strong> laminát, fólia, dyha, masív, lak a matná farba reagujú odlišne.",
            "<strong>Poradie:</strong> horná hrana, plocha dverí, okolie kľučky, spodná hrana a zárubňa.",
            "<strong>Voda:</strong> pri dreve, MDF a hranách používajte iba dobre vyžmýkanú handričku.",
            "<strong>Mastnota:</strong> čistič dávkujte na handričku, nie priamo na dvere.",
            "<strong>Šmuhy:</strong> čistite rovnomerne v smere kresby alebo štruktúry povrchu.",
        ],
        "overview_heading": "Prečo na dverách vidno odtlačky, šmuhy a sivé pásy",
        "overview": [
            "Interiérové dvere patria medzi najčastejšie dotýkané veľké plochy v domácnosti. Ruky smerujú ku kľučke, deti sa opierajú o hranu, domáce zvieratá sa trú o spodnú časť a pri vysávaní či mopovaní sa na dvere prenášajú kvapky. Na svetlom dekore ostáva sivý film, na tmavom sú viditeľné mastné odtlačky a na matnom povrchu môže nevhodný čistič vytvoriť lesklé mapy.",
            "Prach sa drží najmä na hornej hrane krídla, profiloch zárubne a pri pántoch. Ak začnete mokrým čistením bez odstránenia prachu, vytvoríte šmuhy. Preto je pri dverách dôležité rovnaké pravidlo ako pri nábytku: suchá fáza predchádza vlhkej fáze.",
            "Názov materiálu nemusí byť na prvý pohľad jasný. Dvere s dekorom dreva môžu byť laminované, fóliované alebo dyhované. Masívne drevo môže byť lakované, olejované či voskované. Každá úprava má iný limit vody a chémie. Ak nemáte dokumentáciu, test na skrytej hrane je bezpečnejší než domnienka podľa vzhľadu.",
            "Cieľom nie je sterilizovať celé dvere pri každom upratovaní. Bežné znečistenie odstráni mechanické čistenie vhodným prípravkom. Zvýšenú pozornosť venujte kľučke a okoliu dotyku, najmä po návšteve alebo počas choroby. Ak používate dezinfekčný produkt, musí byť určený na daný povrch a treba dodržať etiketu vrátane času pôsobenia.",
        ],
        "surface_rows": [
            ("Laminát alebo CPL", "šmuhy a zatečenie do hrán", "jemný čistič, mäkká handrička a malé množstvo vody"),
            ("Fóliované MDF", "odlepovanie fólie pri spojoch", "nepremáčať hrany a ihneď osušiť"),
            ("Dyhované dvere", "poškodenie tenkej drevenej vrstvy", "produkt vhodný na povrchovú úpravu a minimálny tlak"),
            ("Lakovaný masív", "zmatnenie alebo narušenie laku", "test na skrytom mieste a čistenie v smere kresby"),
            ("Matný lak", "vyleštené fľaky po silnom drhnutí", "rovnomerný jemný pohyb bez abrazíva"),
        ],
        "prep_heading": "Ako zistiť povrch dverí a pripraviť si správne pomôcky",
        "prep": [
            "Ak poznáte výrobcu, pozrite si návod na údržbu. Inak skontrolujte hornú alebo spodnú hranu, kde býva viditeľná konštrukcia. Pripravte dve mäkké handričky, jemný vysávač alebo prachovku na hornú hranu a malú nádobu s vodou. Nepoužívajte parný čistič, drôtenku ani melamínovú hubku bez overenia, pretože môžu zmeniť lesk povrchu.",
            "Pod dvere položte handričku, ak hrozí kvapkanie na citlivú podlahu. Kľučku a kovanie čistite oddelene od plochy krídla; kov môže potrebovať iný produkt než laminát alebo drevo. Pri pántoch neodstraňujte mazivo agresívnym odmasťovačom a nepoužívajte čistiaci roztok ako náhradu za servis mechaniky.",
        ],
        "steps": [
            "Dvere otvorte a suchou handričkou odstráňte prach z hornej hrany, profilov a zárubne.",
            "Na menej viditeľnom mieste otestujte vodu aj vybraný čistiaci prostriedok.",
            "Plochu utierajte zhora nadol v smere dekoru alebo kresby.",
            "Okolie kľučky čistite samostatnou čistou časťou handričky, aby ste mastnotu nerozniesli.",
            "Na odolnejšiu škvrnu nechajte navlhčenú handričku krátko pôsobiť bez premáčania hrany.",
            "Spodnú časť a zárubňu čistite opatrne, pretože bývajú poškodené od topánok a mopu.",
            "Zvyšok prípravku zotrite čistou mierne vlhkou handričkou.",
            "Celý povrch osušte a skontrolujte ho z boku pri dennom svetle, či nezostali šmuhy.",
        ],
        "diagnosis_heading": "Ako riešiť typické škvrny na dverách bez poškodenia dekoru",
        "diagnosis_rows": [
            ("Mastné odtlačky", "kožný maz okolo kľučky", "malé množstvo jemného čističa na handričke"),
            ("Čierne šmuhy pri podlahe", "obuv, guma alebo mop", "lokálny test a jemné opakovanie bez abrazíva"),
            ("Fľak po vode", "kvapky zaschli na matnom povrchu", "rovnomerne pretrieť celú menšiu plochu a osušiť"),
            ("Lepkavý povrch", "priveľa produktu alebo nevhodný prípravok", "zotrieť čistou vodou a dôkladne vysušiť"),
            ("Napučaná hrana", "voda prenikla do MDF alebo spoja", "nepokračovať mokrou cestou a riešiť poškodenie"),
        ],
        "sections": [
            ("Ako vyčistiť biele dvere od odtlačkov a sivých šmúh", [
                "Na bielych dverách býva viditeľná špina, nie vždy však ide o škvrnu, ktorú treba silno drhnúť. Najprv použite čistú mäkkú handričku a malé množstvo jemného prípravku. Pracujte po menších obdĺžnikoch a prechody medzi nimi zjednoťte čistou vlhkou handričkou. Silný tlak môže zmeniť lesk a vytvoriť svetlejšie miesto.",
                "Šmuhy pri spodnom okraji často pochádzajú z topánok alebo znečisteného mopu. Najlepšie je riešiť ich čerstvé. Ak sú staré, skúšku opakujte jemne namiesto použitia drsnej pomôcky. Pri fólii sledujte, či sa hrana neoddeľuje; do poškodeného spoja nesmie zatiecť voda.",
            ]),
            ("Ako čistiť tmavé a matné dvere bez fľakov", [
                "Tmavý matný povrch ukáže každú kvapku aj rozdiel v tlaku. Čistič nerozstrekujte na plochu, pretože kvapky môžu zaschnúť skôr, než ich rozotriete. Naneste malé množstvo na handričku, pretrite súvislý úsek a druhou handričkou ho osušte v rovnakom smere.",
                "Leštiace prípravky môžu na matnom laku vytvoriť nežiaduci lesk. Ak neviete, či je povrch odolný, použite najprv iba vodu a mechanické jemné zotretie. Výsledok kontrolujte z rôznych uhlov, nie iba spredu.",
            ]),
            ("Ako odstrániť mastnotu okolo kľučky", [
                "Okolie kľučky čistite častejšie než celé krídlo. Mastnota sa tam vrství a viaže prach. Kľučku najprv utrite samostatne, potom použite novú čistú časť handričky na dvere. Ak by ste použili jednu znečistenú plochu mikrovlákna na celé krídlo, vytvoríte väčšiu mapu.",
                "Pri silnejšom znečistení nechajte mierne navlhčenú handričku na škvrne pôsobiť niekoľko sekúnd, ale nenechávajte produkt stekať do zámku alebo pod kovanie. Zvyšky prípravku zotrite a povrch vysušte.",
            ]),
            ("Ako vyčistiť zárubne, pánty a spodnú hranu", [
                "Zárubne čistite zhora nadol. V profiloch sa drží prach a pri podlahe bývajú šmuhy od vysávača či mopu. Pri drevenej alebo MDF zárubni používajte minimum vody. Na pánty neprenášajte čistiaci roztok; odstránenie maziva môže spôsobiť vŕzganie alebo zrýchliť opotrebovanie.",
                "Spodná hrana dverí sa často prehliada, no zachytáva prach a chlpy. Dvere otvorte, povysávajte okolie a hranu utrite tak, aby ste nenamočili rez materiálu. Ak je hrana poškodená od vody z mopovania, ďalšie premáčanie ju zhorší.",
            ]),
            ("Čistenie dverí po maľovaní, sťahovaní alebo návšteve", [
                "Po maľovaní najprv odstráňte stavebný prach nasucho. Vlhká handrička by ho mohla zatlačiť do štruktúry. Kvapky farby nestrhávajte kovovou škrabkou; postup závisí od typu farby aj povrchu dverí. Pri hodnotných dverách je lepšia rada výrobcu než agresívny domáci experiment.",
                "Po návšteve stačí bežné vyčistenie dotykových miest. Celoplošná dezinfekcia nemusí byť v zdravej domácnosti potrebná. Ak je niekto chorý a používate dezinfekciu, prečítajte si, či je vhodná na daný materiál a či sa má po čase zotrieť.",
            ]),
        ],
        "remember": [
            "Dekor dreva neznamená, že ide o masív; vždy posudzujte skutočnú povrchovú úpravu.",
            "Najviac vody ohrozuje hrany, spoje a spodnú časť dverí.",
            "Na matnom povrchu môže silné drhnutie vytvoriť trvalý lesklý fľak.",
        ],
        "mistakes": [
            "Nastriekať čistič priamo na celé krídlo a nechať kvapky zasychať.",
            "Použiť jednu špinavú handričku na kľučku aj celú plochu dverí.",
            "Premáčať spodnú hranu MDF dverí.",
            "Leštiť matný povrch silným tlakom.",
            "Použiť parný čistič bez výslovného povolenia výrobcu.",
            "Zamieňať bežné čistenie s dezinfekciou a miešať rôzne chemikálie.",
        ],
        "caution": [
            "Ak sa fólia odlepuje, dyha dvíha, MDF napučala alebo lak mäkne, ďalšie mokré čistenie zastavte. Poškodený povrch potrebuje opravu alebo posúdenie výrobcu, nie silnejší čistič.",
            "Pri historických, ručne lakovaných alebo neznámych povrchoch urobte iba jemné suché čistenie, kým nezískate odporúčanie. Domáci pokus s rozpúšťadlom môže nenávratne odstrániť vrchnú vrstvu.",
        ],
        "expert": [
            "Na dverách sa stretávajú dva typy znečistenia: voľné častice a mastný film z dotyku. Prvý typ treba odstrániť nasucho, druhý jemnou vlhkou cestou. Ak poradie otočíte, prach sa spojí s mastnotou a vytvorí šmuhy.",
            "Častejšie čistenie dotykových zón je účinnejšie než zriedkavé agresívne drhnutie celého povrchu. Znižuje vrstvu nečistôt a chráni dekor. Pri domácnosti bez chorého človeka je bežné čistenie spravidla základ; dezinfekcia je samostatný krok podľa situácie a etikety produktu.",
            "Kvalita výsledku závisí aj od handričky. Zanesené mikrovlákno môže obsahovať mastnotu alebo drobné zrnká, ktoré povrch poškriabu. Použite čistú handričku, neperte ju s avivážou znižujúcou savosť a pri zmene z kľučky na plochu vezmite novú čistú stranu.",
            "Rozdiel medzi šmuhou a poškodenou povrchovou úpravou spoznáte podľa toho, ako sa miesto správa po vysušení. Bežný zvyšok čistiaceho roztoku sa často dá odstrániť čistou mierne vlhkou handričkou a následným osušením. Zmena lesku, zmäknutie laku, zdvihnuté vlákna alebo napučaná hrana však signalizujú zásah do materiálu. Vtedy nepokračujte ďalším drhnutím. Zapíšte si použitý prípravok, nechajte plochu vyschnúť a overte odporúčaný postup u výrobcu dverí.",
        ],
        "source_intro": "Odborné odporúčania k domácemu prachu podporujú pravidelné zachytávanie častíc a čistenie tvrdých povrchov primeranou vlhkou metódou. Pri dverách je navyše rozhodujúca kompatibilita s povrchovou úpravou a návod výrobcu.",
        "sources": [
            ("US EPA: Sources of Indoor Particulate Matter", EPA_DUST),
            ("US EPA: What can I do to improve indoor air quality in my home?", EPA_HOME_AIR),
        ],
        "product_intro": "Na laminát, kompatibilný lak alebo fóliu môže jemný univerzálny čistič pomôcť s odtlačkami a bežným mastným filmom. Najprv však overte etiketu a reakciu povrchu na skrytej hrane.",
        "product_use": "na pravidelné utretie kompatibilných umývateľných dverí, zárubní a okolia kľučky s malým množstvom prípravku na handričke.",
        "product_limit": "na surové drevo, poškodenú dyhu, odlepenú fóliu, napučané MDF, neznámy matný lak ani priamo do zámku a pántov.",
        "related": [
            ("Ako vyčistiť kuchynskú linku bez poškodenia povrchu", "/n/ako-vycistit-kuchynsku-linku-od-mastnoty-prachu-a-smuh-bez-poskodenia-povrchu"),
            ("Ako umyť podlahu bez šmúh", "/n/ako-umyt-podlahu-bez-smuh-laminat-vinyl-dlazba-a-mopovanie-v-praxi"),
            ("Ako odstrániť prach z textílií po rekonštrukcii", "/n/ako-odstranit-prach-z-textilii-po-malovani-alebo-rekonstrukcii"),
        ],
        "faq_title": "čistenie interiérových dverí",
        "faq": [
            ("Čím umyť biele interiérové dvere?", "Mäkkou handričkou a jemným prípravkom vhodným na konkrétny laminát, fóliu alebo lak. Produkt najprv otestujte na skrytom mieste."),
            ("Ako odstrániť mastné odtlačky z matných dverí?", "Prípravok naneste na handričku, čistite rovnomerne bez silného tlaku a povrch hneď osušte. Leštidlo môže vytvoriť lesklú mapu."),
            ("Môžem dvere čistiť parou?", "Iba ak to výslovne povoľuje výrobca. Para a voda môžu poškodiť fóliu, lepidlo, MDF hrany aj lak."),
            ("Ako často čistiť okolie kľučky?", "Podľa používania aj raz týždenne alebo pri viditeľných odtlačkoch. Pravidelné jemné čistenie je šetrnejšie než neskoré agresívne drhnutie."),
        ],
    },
    {
        "title": "Ako vyčistiť vypínače a kľučky: dotykové miesta, mastnota a bezpečný postup",
        "link": "ako-vycistit-vypinace-a-klucky-dotykove-miesta-mastnota-a-bezpecny-postup",
        "meta": "Ako bezpečne vyčistiť vypínače, zásuvkové rámiky a kľučky bez zatekania. Postup pre mastnotu, dotykové nečistoty aj rôzne materiály.",
        "short": "Vypínače a zásuvkové rámiky čistite bez priameho striekania a bez zatekania. Ruky musia byť suché, pri vlhkom čistení vypnite príslušný okruh, kryt nerozoberajte a použite iba takmer suchú handričku. Kľučky čistite oddelene podľa materiálu; dezinfekčný prostriedok používajte len vtedy, keď je vhodný na povrch a dodržíte etiketu.",
        "answer": "Najprv suchou handričkou odstráňte prach. Pred akýmkoľvek vlhkým čistením vypínača vypnite príslušný elektrický okruh, overte suché ruky a nikdy nestriekajte produkt priamo na vypínač. Vonkajší kryt utrite iba dobre vyžmýkanou handričkou bez zatekania do škár. Prasknutý, uvoľnený, teplý alebo iskriaci vypínač nečistite, ale nechajte skontrolovať elektrikárom.",
        "quick": [
            "<strong>Elektrina:</strong> pred vlhkým čistením vypnite príslušný okruh a pracujte suchými rukami.",
            "<strong>Aplikácia:</strong> čistič patrí na handričku, nikdy priamo do vypínača alebo zásuvky.",
            "<strong>Vlhkosť:</strong> handrička má byť takmer suchá a nesmie z nej kvapkať.",
            "<strong>Kľučka:</strong> kov, lak, drevo a povrchová úprava vyžadujú kompatibilný produkt.",
            "<strong>Porucha:</strong> praskliny, teplo, bzučanie alebo iskrenie patria elektrikárovi.",
        ],
        "overview_heading": "Prečo sa dotykové miesta špinia rýchlo a prečo ich nemožno čistiť rovnako",
        "overview": [
            "Vypínače, kľučky a madlá sú malé plochy, ktorých sa dotýkame mnohokrát denne. Kožný maz, krém na ruky, kuchynská mastnota a prach vytvárajú sivý alebo lepkavý film. Pri kľučke je hlavnou otázkou kompatibilita s kovom či lakom. Pri vypínači sa k tomu pridáva elektrická bezpečnosť a riziko, že kvapalina prenikne do škáry.",
            "Bežné čistenie a dezinfekcia nie sú to isté. Čistenie mechanicky odstraňuje nečistoty a časť mikroorganizmov. Dezinfekčný produkt má vlastný návod, určené povrchy a čas pôsobenia. V zdravej domácnosti nemusíte pri každom upratovaní dezinfikovať všetky vypínače, no pravidelné odstránenie viditeľnej mastnoty je praktické a chráni povrch.",
            "Najrizikovejší zvyk je priame striekanie spreja na vypínač. Kvapky môžu stiecť pod kolísku alebo do rámika. Rovnako nevhodná je para, mokrá špongia a čistenie s mokrými rukami. Elektrické zariadenie neotvárajte kvôli upratovaniu; odnímanie krytu a zásah do inštalácie patrí kvalifikovanej osobe.",
            "Kľučky môžu byť z nehrdzavejúcej ocele, mosadze, hliníka, lakovaného kovu alebo plastu. Agresívna kyselina, chlór či abrazívna pasta môžu zmeniť povrch alebo poškodiť ochrannú vrstvu. Aj tu platí skúška na nenápadnom mieste a čítanie etikety.",
        ],
        "surface_rows": [
            ("Plastový vypínač", "zatečenie do škár a zmatnenie plastu", "vypnúť okruh, použiť takmer suchú handričku"),
            ("Zásuvkový rámik", "kontakt kvapaliny s otvorom zásuvky", "čistiť iba vonkajšiu plochu bez striekania"),
            ("Nerezová kľučka", "odtlačky a mapy", "jemný kompatibilný čistič a osušenie"),
            ("Mosadzná alebo povrchovo upravená kľučka", "poškodenie patiny alebo povlaku", "riadiť sa výrobcom a nepoužiť abrazívum"),
            ("Lakované madlo", "odlupovanie a zmena lesku", "malá skúška, minimum produktu a mäkká handrička"),
        ],
        "prep_heading": "Bezpečná príprava pred čistením vypínačov a kľučiek",
        "prep": [
            "Na vypínače si pripravte suchú a druhú iba mierne navlhčenú handričku. Vypnite príslušný okruh v rozvádzači a zabezpečte, aby ho počas práce nikto nezapol. Ak neviete bezpečne určiť okruh alebo je inštalácia stará a poškodená, zostaňte pri suchom zotretí povrchu a ďalší zásah nechajte elektrikárovi.",
            "Na kľučky použite inú handričku než na vypínače. Čistiaci roztok si nedávajte do otvoreného vedra tesne pri zásuvke. Produkt naneste na handričku mimo elektrického prvku a handričku dôkladne vyžmýkajte. Parný čistič v okolí vypínačov, otvorených elektrických bodov a kabeláže nepoužívajte.",
        ],
        "steps": [
            "Skontrolujte vypínač: ak je prasknutý, uvoľnený, teplý alebo bzučí, prestaňte a volajte elektrikára.",
            "Pred vlhkým čistením vypnite príslušný elektrický okruh a pracujte suchými rukami.",
            "Suchou handričkou odstráňte prach z rámika, steny v okolí a hornej hrany vypínača.",
            "Malé množstvo vhodného prípravku naneste na handričku ďaleko od elektrického prvku.",
            "Vonkajšiu plochu utrite bez tlačenia kvapaliny do škár a otvorov.",
            "Druhou suchou handričkou povrch okamžite osušte.",
            "Kľučku vyčistite oddelene podľa materiálu vrátane spodnej strany a rozety.",
            "Okruh zapnite až po úplnom vyschnutí a iba ak vypínač nevykazuje poruchu.",
        ],
        "diagnosis_heading": "Kedy ide o špinu a kedy už o technický problém",
        "diagnosis_rows": [
            ("Sivý film", "prach a kožný maz", "jemné čistenie vonkajšieho povrchu"),
            ("Lepkavý rámik", "krém, kuchynská mastnota alebo zvyšok produktu", "malé množstvo kompatibilného čističa a osušenie"),
            ("Žltý alebo hnedý fľak", "starnutie plastu alebo tepelné poškodenie", "nepredpokladať špinu, skontrolovať stav elektrikárom"),
            ("Bzučanie alebo iskrenie", "elektrická porucha", "nepoužívať a volať elektrikára"),
            ("Kľučka mení farbu", "reakcia povrchovej úpravy s chémiou", "prestať s produktom a riadiť sa výrobcom"),
        ],
        "sections": [
            ("Ako vyčistiť vypínač bez zatečenia", [
                "Po vypnutí okruhu najprv zotrite prach suchou handričkou. Na mastný film použite iba takmer suchú handričku s minimom vhodného prípravku. Pohybujte sa po vonkajšej ploche a rámiku, nie cez otvor zásuvky alebo hlboko v škáre. Kvapalina nesmie vytvoriť kvapku, ktorú by gravitácia vtiahla dovnútra.",
                "Ak ostáva špina v úzkej medzere, nepoužívajte kovový predmet ani mokrú vatovú tyčinku. Kryt nerozoberajte. Hranicu medzi bežným čistením a zásahom do elektroinštalácie rešpektujte aj vtedy, keď sa špina zdá esteticky nepríjemná.",
            ]),
            ("Ako vyčistiť kľučky od mastnoty a odtlačkov", [
                "Kľučku utrite zo všetkých strán, vrátane spodnej časti a okolia rozety. Na nerezovej oceli postupujte v smere brúsenia a povrch osušte, aby neostali mapy. Pri lakovanej alebo farebnej kľučke najprv otestujte produkt na malej ploche.",
                "Mosadz a dekoratívne povlaky môžu mať zámernú patinu alebo tenkú ochrannú vrstvu. Univerzálnu pastu ani kyslý roztok nepoužívajte bez odporúčania výrobcu. Cieľom je odstrániť mastnotu, nie obrúsiť povrch do iného odtieňa.",
            ]),
            ("Čistenie verzus dezinfekcia dotykových miest", [
                "Najprv musí byť povrch čistý. Vrstva mastnoty a prachu znižuje rovnomernosť pôsobenia ďalšieho produktu. Ak je dezinfekcia potrebná, vyberte prípravok určený na konkrétny materiál a dodržte čas pôsobenia. Nie každý voňavý čistič je dezinfekcia a nie každý dezinfekčný prípravok je bezpečný pre lak či kovový povlak.",
                "Počas choroby alebo pri osobe so zníženou imunitou sa režim môže sprísniť, no riaďte sa aktuálnym odporúčaním zdravotníkov a etiketou produktu. Chemikálie nemiešajte. Pri vypínači zostáva elektrická bezpečnosť nadradená snahe o maximálne mokrý kontakt.",
            ]),
            ("Ako často čistiť vypínače, kľučky a madlá", [
                "V bežnej domácnosti ich čistite pri viditeľnom znečistení a v rámci pravidelného upratovania. V kuchyni, kúpeľni, detskej izbe a pri vchodových dverách to môže byť častejšie. Kľučky po návšteve alebo počas choroby môžete riešiť samostatne bez umývania celých dverí.",
                "Vytvorte jednoduché poradie: najprv čisté plochy, potom dotykové miesta a nakoniec kúpeľňa či kuchyňa. Handričku z mastnej kľučky nepoužívajte na ďalší vypínač. Tak znižujete prenos nečistôt a šmúh medzi miestnosťami.",
            ]),
            ("Kedy vypínač už nečistiť, ale vymeniť alebo opraviť", [
                "Teplý rámik, zápach po spálenine, praskanie, bzučanie, iskra, uvoľnenie zo steny alebo tmavnutie plastu sú varovné signály. Vypínač nepoužívajte a problém neprekryte novým krytom či čistiacim prípravkom. Elektrikár musí posúdiť príčinu.",
                "Rovnako nebezpečné je čistenie zásuvky, do ktorej už zatiekla voda. Nedotýkajte sa mokrej oblasti a bezpečne vypnite napájanie, ak to viete urobiť bez kontaktu s vodou. Pri pochybnosti volajte odbornú pomoc.",
            ]),
        ],
        "remember": [
            "Vypínač sa nikdy nestrieka priamo a na jeho čistenie sa nepoužíva para.",
            "Pred vlhkým čistením vypnite príslušný okruh; kryt nerozoberajte.",
            "Teplo, bzučanie, iskrenie, prasklina alebo zápach po spálenine sú dôvod volať elektrikára.",
        ],
        "mistakes": [
            "Nastriekať čistič priamo na kolísku vypínača.",
            "Čistiť vypínač mokrými rukami alebo kvapkajúcou handričkou.",
            "Použiť parný čistič v blízkosti otvorov zásuvky.",
            "Rozoberať rámik bez kvalifikácie.",
            "Predpokladať, že hnednutie plastu je iba špina.",
            "Použiť abrazívnu pastu na mosadznú alebo lakovanú kľučku bez skúšky.",
        ],
        "caution": [
            "Pri akomkoľvek náznaku elektrickej poruchy prestaňte vypínač používať. Čistenie neodstráni uvoľnený kontakt, prehrievanie ani poškodenú izoláciu. Kvalifikovaný elektrikár je potrebný aj vtedy, keď chyba nie je viditeľná zvonka.",
            "Ak do zásuvky alebo vypínača zatiekl čistiaci roztok, nedotýkajte sa ho a nepokúšajte sa ho vysušiť zapnutým spotrebičom či fénom. Bezpečne odpojte napájanie a nechajte stav skontrolovať odborníkom.",
        ],
        "expert": [
            "Dotykové miesta sa znečisťujú kombináciou pevných častíc a mastnoty. Jemné čistenie funguje tak, že handrička mechanicky zachytí vrstvu a malé množstvo vhodného detergentu pomôže uvoľniť mastný film. Viac produktu neznamená automaticky lepší výsledok.",
            "Bezpečnostná organizácia Electrical Safety First upozorňuje, že parné čističe sa nemajú používať pri otvorených elektrických bodoch alebo kabeláži. Rovnaký princíp platí pri vypínačoch: voda a para nesmú preniknúť k elektrickým častiam.",
            "Pri bežnom upratovaní oddeľte handričky podľa zón. Znečistenie z kuchynskej kľučky alebo kúpeľne neprenášajte na vypínač v spálni. Čistá pomôcka znižuje šmuhy a zároveň zabraňuje roznášaniu nečistôt.",
            "Vypnutie okruhu pred vlhkým čistením je iba jedna časť bezpečného postupu. Dôležité je aj spoľahlivo vedieť, ktorý istič daný bod napája, pracovať suchými rukami a nenechať handričku kvapkať. Ak si označením rozvádzača nie ste istí, zostaňte pri jemnom suchom zotretí alebo si nechajte okruhy identifikovať elektrikárom. Kryt vypínača neodoberajte len preto, aby ste vyčistili jeho hrany. Bežná údržba má ostať na dostupnom vonkajšom povrchu a nesmie zasahovať do elektroinštalácie.",
            "Kľučka a vypínač síce ležia blízko seba, no často sú z odlišných materiálov. Lakovaný kov, nerez, mosadz, plast a povrch s antibakteriálnou úpravou nemusia znášať rovnaký prípravok ani čas pôsobenia. Najprv si overte návod výrobcu a skúšku urobte na nenápadnom mieste. Po očistení odstráňte zvyšky roztoku podľa etikety a povrch vysušte, aby na ňom nezostala lepkavá vrstva zachytávajúca ďalší prach.",
        ],
        "source_intro": "Verejné bezpečnostné odporúčania zdôrazňujú odstup vody a pary od elektrických bodov, kontrolu poškodenia a používanie zariadení podľa návodu. Tento článok rieši iba vonkajšie čistenie bez rozoberania elektroinštalácie.",
        "sources": [
            ("Electrical Safety First: Steam cleaner safety", ELECTRICAL_SAFETY),
            ("US EPA: Sources of Indoor Particulate Matter", EPA_DUST),
        ],
        "product_intro": "Na kompatibilnú kľučku, umývateľnú rozetu alebo plastový vonkajší kryt môže po bezpečnom vypnutí okruhu stačiť veľmi malé množstvo prípravku na handričke. Nikdy nejde o čistič elektrických kontaktov.",
        "product_use": "na bežnú mastnotu na kompatibilnej kľučke alebo vonkajšej umývateľnej ploche, ak prípravok nanesiete mimo elektrického prvku a handričku dobre vyžmýkate.",
        "product_limit": "na vnútro vypínača, zásuvkové otvory, elektrické kontakty, poškodený rámik, neznámy kovový povlak ani ako náhradu za dezinfekciu alebo opravu.",
        "related": [
            ("Ako vyčistiť kuchynskú linku od mastnoty", "/n/ako-vycistit-kuchynsku-linku-od-mastnoty-prachu-a-smuh-bez-poskodenia-povrchu"),
            ("Ako umyť podlahu bez šmúh", "/n/ako-umyt-podlahu-bez-smuh-laminat-vinyl-dlazba-a-mopovanie-v-praxi"),
            ("Ako vyčistiť drez a batériu", "/n/ako-vycistit-drez-a-bateriu-vodny-kamen-mastnota-zapach-a-bezpecne-cistenie"),
        ],
        "faq_title": "čistenie vypínačov a kľučiek",
        "faq": [
            ("Môžem na vypínač nastriekať dezinfekciu?", "Nie priamo. Kvapalina môže zatiecť do škár. Ak je produkt vhodný na vonkajší plast, naneste ho mimo vypínača na handričku a dodržte bezpečný postup aj etiketu."),
            ("Treba pred čistením vypínača vypnúť elektrinu?", "Pred akýmkoľvek vlhkým čistením áno. Vypnite príslušný okruh, pracujte suchými rukami a okruh zapnite až po úplnom vyschnutí."),
            ("Ako vyčistiť mosadznú kľučku?", "Najprv zistite, či ide o masívnu mosadz, povlak alebo lakovanú patinu. Použite iba odporúčanie vhodné pre danú úpravu a vyhnite sa abrazívu bez skúšky."),
            ("Čo robiť, keď vypínač bzučí alebo je teplý?", "Nepoužívajte ho, nečistite ho mokrou cestou a zavolajte kvalifikovaného elektrikára. Ide o technický problém, nie bežnú špinu."),
        ],
    },
    {
        "title": "Ako vyčistiť soklové lišty: prach, šmuhy, chlpy a rohy bez poškodenia",
        "link": "ako-vycistit-soklove-listy-prach-smuhy-chlpy-a-rohy-bez-poskodenia",
        "meta": "Ako vyčistiť soklové lišty od prachu, chlpov a šmúh. Postup pre PVC, MDF, drevo aj káblové lišty bez premáčania a poškodenia.",
        "short": "Soklové lišty čistite po vysávaní a pred finálnym umytím podlahy. Hornú hranu, rohy a spoje najprv povysávajte mäkkým nadstavcom, potom utrite dobre vyžmýkanou handričkou podľa materiálu. MDF a drevo nepremáčajte, PVC nedrhnite abrazívom a pri káblových lištách nedovoľte, aby voda zatekala do spojov.",
        "answer": "Povysávajte podlahu aj hornú hranu líšt a odstráňte chlpy z rohov. Na skrytom mieste otestujte mierne vlhkú handričku. Lišty utierajte po krátkych úsekoch, škvrny riešte lokálne a druhou handričkou ich hneď osušte. Pri MDF, dreve a poškodených spojoch používajte minimum vody. Po čistení líšt dokončite podlahu čistým mopom.",
        "quick": [
            "<strong>Poradie:</strong> vysať miestnosť, vyčistiť lišty a až potom umyť podlahu.",
            "<strong>Horná hrana:</strong> drží najviac prachu a chlpov, hoci spredu nie sú viditeľné.",
            "<strong>MDF a drevo:</strong> neznášajú dlhé premáčanie pri spojoch.",
            "<strong>PVC:</strong> je odolnejšie voči vode, ale môže sa poškriabať.",
            "<strong>Káblová lišta:</strong> čistiaci roztok nesmie zatiecť dovnútra.",
        ],
        "overview_heading": "Prečo sú soklové lišty sivé aj krátko po umytí podlahy",
        "overview": [
            "Soklová lišta leží v zóne, kde sa stretáva prach zo vzduchu, chlpy, omrvinky, nečistoty z topánok a kvapky z mopu. Horná vodorovná hrana funguje ako malý parapet a zachytáva jemné častice. Predná plocha zase dostáva zásahy vysávačom, robotickým vysávačom, mopom a nábytkom.",
            "Ak lišty utierate až po umytí podlahy, špinavá voda stečie na hotovú plochu a výsledkom sú nové šmuhy. Praktickejšie je najprv celú miestnosť vysať, potom vyčistiť lišty a nakoniec podlahu umyť čistou vodou alebo roztokom. Tak zachytíte aj nečistotu, ktorá pri práci spadne z hornej hrany.",
            "Materiál líšt je dôležitý. PVC a keramika sú zvyčajne odolnejšie voči krátkemu vlhkému čisteniu. MDF a drevené lišty môžu napučať pri spojoch, najmä ak je hrana poškodená. Lakovaný profil môže zmatnieť po abrazívnej hubke a káblová lišta nesmie byť zaplavená.",
            "Čierne šmuhy pri dverách alebo v chodbe môžu pochádzať z gumovej podrážky, kolies vysávača alebo nábytku. Na rozdiel od voľného prachu potrebujú lokálne čistenie, ale nie automaticky silné rozpúšťadlo. Začnite najjemnejším postupom a tlak zvyšujte iba v rámci odolnosti povrchu.",
        ],
        "surface_rows": [
            ("PVC lišta", "škrabance a zatečenie do spojov", "mäkká handrička a jemný produkt bez abrazíva"),
            ("MDF lišta", "napučanie rezu a rohov", "minimum vody a okamžité vysušenie"),
            ("Masívne alebo dyhované drevo", "poškodenie povrchovej úpravy", "čistič vhodný na lak, olej alebo vosk"),
            ("Keramický sokel", "špára, mastnota a vodný film", "čistiť podľa dlažby a škáry, bez prenášania špiny"),
            ("Káblová lišta", "zatečenie k vedeniu", "iba vonkajšie utretie takmer suchou handričkou"),
        ],
        "prep_heading": "Ako pripraviť podlahu, rohy a pomôcky",
        "prep": [
            "Odsuňte ľahký nábytok a povysávajte podlahu po obvode miestnosti. Použite mäkký kefový nadstavec na hornú hranu líšt a úzku hubicu do rohov, ale dávajte pozor, aby tvrdý plast hubice nepoškriabal lak. Chlpy, pavučiny a prach odstráňte pred vodou.",
            "Pripravte si dve malé handričky a nepoužívajte vedro plné špinavej vody z mopovania. Jedna handrička čistí, druhá suší. Pri dlhom obvode ich pravidelne oplachujte alebo vymeňte. Najprv otestujte povrch za nábytkom a skontrolujte spoje; ak sú otvorené alebo napučané, vodu obmedzte na minimum.",
        ],
        "steps": [
            "Povysávajte podlahu, hornú hranu líšt, rohy a priestor za dostupným nábytkom.",
            "Určte materiál lišty a na skrytom mieste otestujte vlhkosť aj prípravok.",
            "Začnite v najčistejšej časti miestnosti a postupujte po úsekoch dlhých približne jeden meter.",
            "Hornú hranu utrite prvú, potom prednú plochu a nakoniec spoj pri podlahe.",
            "Čierne šmuhy riešte lokálne jemným opakovaním, nie silným drhnutím celého profilu.",
            "Rohy a spoje čistite mäkkou handričkou bez tlačenia vody do medzier.",
            "Každý úsek pri MDF, dreve a káblovej lište hneď osušte.",
            "Po dokončení líšt povysávajte spadnuté nečistoty a umyte podlahu čistým mopom.",
        ],
        "diagnosis_heading": "Čo znamenajú typické škvrny a poškodenia na lištách",
        "diagnosis_rows": [
            ("Sivá horná hrana", "vrstva prachu a textilných vlákien", "najprv vysať, potom vlhko zotrieť"),
            ("Čierne pásy", "guma z topánok, vysávača alebo nábytku", "lokálny test jemného čističa"),
            ("Lepkavý povrch", "zvyšok produktu alebo kuchynská mastnota", "pretrieť čistou vodou a osušiť"),
            ("Napučaný roh", "voda v MDF alebo poškodenom spoji", "prestať premáčať a riešiť opravu"),
            ("Tmavnutie pri stene", "vlhkosť, únik alebo kondenzácia", "skontrolovať zdroj, nielen povrch"),
        ],
        "sections": [
            ("Ako odstrániť prach a chlpy z hornej hrany líšt", [
                "Mäkký kefový nadstavec vysávača je rýchlejší než prachovka, ktorá častice často iba zhodí na podlahu. Postupujte po obvode miestnosti a venujte pozornosť rohom, miestam za posteľou a priestoru pri radiátore. Pri domácich zvieratách sa chlpy môžu zachytiť v škáre medzi lištou a podlahou.",
                "Po vysatí prejdite hornú hranu mierne vlhkou handričkou. Handričku skladajte tak, aby ste stále používali čistú plochu. Ak je po prvom metri sivá, opláchnite ju; inak budete prach iba presúvať po miestnosti.",
            ]),
            ("Ako vyčistiť biele PVC lišty od čiernych šmúh", [
                "Na PVC začnite vodou a jemným kompatibilným čističom. Čierny pás čistite lokálne malými krúživými pohybmi bez drsnej strany hubky. Melamínová hubka môže pôsobiť ako jemné abrazívum a zmeniť lesk, preto ju nepoužívajte bez testu.",
                "Ak šmuha nepovolí, zistite jej pôvod. Guma z nábytku a stavebná farba potrebujú odlišný postup. Rozpúšťadlo môže naleptať plast a vytvoriť väčší trvalý fľak než pôvodná nečistota.",
            ]),
            ("Ako čistiť MDF a drevené lišty bez napučania", [
                "MDF lišta je najcitlivejšia na miestach rezu, rohoch a spojoch. Handričku vyžmýkajte tak, aby nezanechávala kvapky, a každý úsek hneď osušte. Ak je fólia alebo lak poškodený, čistite iba povrchovo a nedovoľte, aby voda prenikla pod vrstvu.",
                "Drevená lišta môže byť lakovaná, olejovaná alebo voskovaná. Používajte prípravok vhodný na danú úpravu. Univerzálny odmasťovač môže odstrániť ochranný film, preto je informácia od výrobcu cennejšia než domáci experiment.",
            ]),
            ("Soklové lišty v kuchyni, chodbe a domácnosti so zvieratami", [
                "V kuchyni sa na lištách spája prach s mastnotou, v chodbe s nečistotou z topánok a pri zvieratách s chlpmi. Preto nemusí byť rovnaká frekvencia ani postup v celej domácnosti. Kuchynský úsek čistite častejšie a handričku z neho neprenášajte do spálne.",
                "V domácnosti so zvieratami pomáha pravidelné vysávanie rohov ešte predtým, než sa chlpy spoja s vlhkosťou z mopu. Robotický vysávač nemusí dosiahnuť na hornú hranu lišty a môže na nej zanechať šmuhy od nárazníka. Po jeho cykle preto skontrolujte exponované rohy.",
            ]),
            ("Ako zladiť čistenie líšt s umývaním podlahy", [
                "Správne poradie znižuje počet opakovaných krokov: najprv prach na nábytku, potom vysávanie, lišty a nakoniec mokré umytie podlahy. Ak po čistení líšt na podlahu spadnú omrvinky alebo prach, krátko ich znovu povysávajte. Podrobný postup nájdete aj v článku <a href=\"/n/ako-umyt-podlahu-bez-smuh-laminat-vinyl-dlazba-a-mopovanie-v-praxi\">ako umyť podlahu bez šmúh</a>.",
                "Mop nepritláčajte silou k MDF lište a nenechávajte vodu stáť pri stene. Pri lamináte a drevenej podlahe je nadmerná vlhkosť rizikom pre podlahu aj lištu. Čistý, dobre vyžmýkaný mop vytvorí lepší výsledok než veľké množstvo voňavého roztoku.",
            ]),
        ],
        "remember": [
            "Lišty čistite po vysávaní a pred finálnym umytím podlahy.",
            "MDF, drevo, PVC, keramika a káblové lišty majú odlišný limit vody.",
            "Napučanie, opakované tmavnutie alebo vlhkosť pri stene nie sú iba estetická špina.",
        ],
        "mistakes": [
            "Utierať lišty až po umytí podlahy a znovu ju zašpiniť.",
            "Použiť špinavú vodu z mopu na celý obvod miestnosti.",
            "Premáčať MDF spoje a rohy.",
            "Drhnúť lesklé PVC abrazívnou hubkou.",
            "Nechať čistič zatiecť do káblovej lišty.",
            "Prekryť tmavnutie pri stene vôňou bez kontroly vlhkosti.",
        ],
        "caution": [
            "Ak je lišta napučaná, odlepená, prasknutá alebo sa pod ňou opakovane objavuje vlhkosť, ďalšie umývanie zastavte. Skontrolujte únik, kondenzáciu, stav podlahy a steny. Poškodený MDF profil sa čističom nevráti do pôvodného tvaru.",
            "Káblovú lištu neotvárajte mokrými rukami a nepoužívajte tekutinu v jej vnútri. Ak je kryt poškodený alebo vedenie viditeľné, obráťte sa na elektrikára alebo správcu inštalácie.",
        ],
        "expert": [
            "Prach sa usádza najmä na vodorovných hranách a v rohoch s malým pohybom vzduchu. Suché zametanie ho môže znovu rozvíriť, zatiaľ čo vysávač a následná mierne vlhká handrička ho lepšie zachytia. Tento princíp je užitočný pri lištách, parapetoch aj radiátoroch.",
            "Pri domácich alergénoch nie je rozhodujúca jedna veľká generálka, ale pravidelnosť. Častejšie odstránenie menšej vrstvy prachu znižuje potrebu agresívneho drhnutia a obmedzuje množstvo častíc, ktoré sa pri pohybe v miestnosti vracajú do vzduchu.",
            "Vlhké čistenie má byť kontrolované. Na odolnej keramike si môžete dovoliť viac vody než na MDF, no ani tam nemá zmysel nechávať špinavý roztok zaschnúť. Materiál, čistota handričky a vysušenie sú dôležitejšie než intenzita vône.",
            "Sivý pás tesne nad lištou nemusí pochádzať zo samotnej lišty. Môže ísť o prach zachytený na stene, o stopu od mopu alebo o vlhkosť prenikajúcu zo spoja podlahy. Najprv preto vyčistite malý úsek každého materiálu samostatne a po vysušení ho porovnajte s okolím. Tak zistíte, či treba riešiť lištu, náter steny, techniku mopovania alebo zdroj vlhkosti. Jedným silným prípravkom aplikovaným cez oba povrchy by ste mohli vytvoriť novú škvrnu a zároveň prekryť pôvodnú príčinu.",
            "Pri vysávaní používajte mäkký nadstavec bez ostrých hrán a sledujte drobné kamienky zachytené v kefke. Na lesklom PVC alebo lakovanom dreve môžu pri ťahaní vytvoriť ryhy. V rohoch je bezpečnejšie nečistotu najprv uvoľniť mäkkým štetcom a priebežne ju odsávať, nie tlačiť hubkou hlbšie do spoja. Po vlhkom kroku utrite zvlášť hornú hranu, čelnú plochu a spoj s podlahou čistou suchou stranou handričky.",
        ],
        "source_intro": "EPA pri vnútornom prachu odporúča pravidelné vysávanie a utieranie tvrdých plôch vlhkou handričkou. Pri lištách tento princíp treba prispôsobiť materiálu, najmä hranám MDF, drevu a káblovým profilom.",
        "sources": [
            ("US EPA: Sources of Indoor Particulate Matter", EPA_DUST),
            ("US EPA: Biological Contaminants and Indoor Air Quality", EPA_BIO),
        ],
        "product_intro": "Na kompatibilné PVC, laminované alebo lakované lišty môže malé množstvo univerzálneho čističa pomôcť odstrániť sivý film a bežné šmuhy. Produkt však dávkujte na handričku a vždy ho vyskúšajte za nábytkom.",
        "product_use": "na pravidelnú údržbu umývateľných líšt a lokálne čistenie ľahkej mastnoty alebo šmúh po predchádzajúcom vysatí.",
        "product_limit": "na poškodené MDF hrany, surové drevo, otvorenú káblovú lištu, prírodný povrch bez overenia ani na prekrytie vlhkosti pri stene.",
        "related": [
            ("Ako umyť podlahu bez šmúh v praxi", "/n/ako-umyt-podlahu-bez-smuh-laminat-vinyl-dlazba-a-mopovanie-v-praxi"),
            ("Ako odstrániť prach z textílií po rekonštrukcii", "/n/ako-odstranit-prach-z-textilii-po-malovani-alebo-rekonstrukcii"),
            ("Ako vyčistiť kuchynskú linku od mastnoty a prachu", "/n/ako-vycistit-kuchynsku-linku-od-mastnoty-prachu-a-smuh-bez-poskodenia-povrchu"),
        ],
        "faq_title": "čistenie soklových líšt",
        "faq": [
            ("Čím vyčistiť biele plastové lišty?", "Mäkkou handričkou a jemným čističom vhodným na PVC. Najprv vysajte prach a vyhnite sa drsnej hubke, ktorá môže zmeniť lesk."),
            ("Ako čistiť MDF lišty?", "Použite iba dobre vyžmýkanú handričku, minimum produktu a každý úsek hneď osušte. Voda nesmie zatekať do spojov a rezov."),
            ("Kedy čistiť lišty pri umývaní podlahy?", "Po vysávaní, ale pred finálnym mokrým umytím podlahy. Nečistota z líšt tak neskončí na už hotovej podlahe."),
            ("Prečo sa pri lište objavuje tmavá mapa?", "Môže ísť o prach a šmuhy, ale aj o dlhodobú vlhkosť. Ak sa mapa vracia alebo je povrch mokrý, skontrolujte zdroj vlhkosti."),
        ],
    },
]


def hrefs(markup):
    return re.findall(r'href="([^"]+)"', markup)


def preflight_links(articles):
    rows = []
    seen = set()
    headers = {"User-Agent": "Mozilla/5.0 (compatible; VEVOContentPreflight/1.0)"}
    for article in articles:
        target_url = f"{BASE}/n/{article['link']}"
        try:
            response = requests.get(target_url, timeout=30, allow_redirects=True, headers=headers)
            status = response.status_code
            ok = status == 404
            error = None
        except Exception as exc:
            status = None
            ok = False
            error = str(exc)
        rows.append({"url": target_url, "kind": "target_slug_precheck", "ok": ok, "status": status, "error": error})

        for href in hrefs(article["long"]):
            url = urljoin(BASE, href)
            if url in seen:
                continue
            seen.add(url)
            try:
                response = requests.get(url, timeout=30, allow_redirects=True, headers=headers)
                status = response.status_code
                ok = 200 <= status < 400
                error = None
            except Exception as exc:
                status = None
                ok = False
                error = str(exc)
            rows.append({"url": url, "kind": "article_link", "ok": ok, "status": status, "error": error})

    return {
        "checked_count": len(rows),
        "failure_count": sum(1 for row in rows if not row["ok"]),
        "links": rows,
    }


def main():
    rendered = []
    for index, article in enumerate(ARTICLES):
        long = render_article(article)
        if not 120 <= len(article["meta"]) <= 165:
            raise SystemExit(
                f"Meta description length must be 120-165 for {article['title']}: {len(article['meta'])}"
            )
        for value in (article["title"], article["short"], article["meta"], long):
            hits = FORBIDDEN_PUBLIC_RE.findall(value)
            if hits:
                raise SystemExit(f"Forbidden public wording in {article['title']}: {hits}")
        rendered.append(
            {
                "title": article["title"],
                "title_tag": article["title"],
                "description": article["meta"],
                "short": article["short"],
                "long": long,
                "date_posted": PUBLISH_DATE,
                "time_posted": f"10:{index * 10:02d}",
                "active": True,
                "link": article["link"],
                "commenting": False,
            }
        )

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(rendered, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    report = preflight_links(rendered)
    OUT_PREFLIGHT.parent.mkdir(parents=True, exist_ok=True)
    OUT_PREFLIGHT.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"article_count": len(rendered), "output": str(OUT_JSON), **report}, ensure_ascii=False, indent=2))
    if report["failure_count"]:
        raise SystemExit("Link preflight failed")


if __name__ == "__main__":
    main()
