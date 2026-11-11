import html
import json
import re
from pathlib import Path
from urllib.parse import urljoin

import requests


BASE = "https://www.vevo.sk"
PUBLISH_DATE = "2026-07-14"
CANDIDATES = Path("content/VEVO_CONTENT/batches/batch-38-candidates-2026-07-14.txt")
OUT_JSON = Path("content/VEVO_CONTENT/imports/batch-38-2026-07-14-articles.json")
OUT_PREFLIGHT = Path("content/VEVO_CONTENT/exports/batch-38-2026-07-14-link-preflight.json")

ALL_CLEANERS = "/c/vevo-home-care/upratovanie/cistiace-prostriedky"
UNIVERSAL_CATEGORY = "/c/vevo-home-care/upratovanie/cistiace-prostriedky/univerzalny-cistic-do-domacnosti"
FLOOR_CATEGORY = "/c/vevo-home-care/upratovanie/cistiace-prostriedky/vona-na-umyvanie-podlah"
UNIVERSAL_PRODUCT = "/p-1630/univerzalny-cistic-vevo-pure-harmony-500ml"
FLOOR_PRODUCT = "/p-1553/parfum-na-podlahy-vevo-no-01-cotton-paradise"

EPA_CLEANING = "https://www.epa.gov/coronavirus-and-disinfectants/whats-difference-between-products-disinfect-sanitize-and-clean"
EPA_CLEANING_VENTILATION = "https://www.epa.gov/indoor-air-quality-iaq/ventilation-important-indoor-air-quality-when-cleaning-andor-sanitizing"
EPA_INDOOR = "https://www.epa.gov/indoor-air-quality-iaq/improving-your-indoor-environment"
EPA_PM = "https://www.epa.gov/indoor-air-quality-iaq/sources-indoor-particulate-matter-pm"
EPA_DUCTS = "https://www.epa.gov/indoor-air-quality-iaq/should-you-have-air-ducts-your-home-cleaned"
EPA_MOLD = "https://www.epa.gov/mold/mold-cleanup-your-home"
OSHA_LADDER = "https://www.osha.gov/sites/default/files/publications/PORTABLE_LADDER_QC.pdf"
SPONGE_STUDY = "https://doi.org/10.1038/s41598-017-06055-9"

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


def source_box(article):
    links = "".join(
        f'<li><a rel="noopener" href="{href}" target="_blank">{esc(label)}</a></li>'
        for label, href in article["sources"]
    )
    return f"""
<div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; margin: 22px 0; background: #fbfbfb;">
<h2 style="margin-top: 0;">Odbornejší pohľad a použité zdroje</h2>
<p>{article['source_intro']}</p>
<ul>{links}</ul>
</div>
""".strip()


def commercial_blocks(article):
    return f"""
<div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 24px 0; background: #f7fbf8;">
<h2 style="margin-top: 0;">{esc(article['product_heading'])}</h2>
<p>{article['product_intro']}</p>
<div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff; margin: 14px 0;">
<h3 style="margin-top: 0;">{esc(article['product_name'])}</h3>
<p><strong>Kedy dáva zmysel:</strong> {article['product_use']}</p>
<p><strong>Dôležitá hranica:</strong> {article['product_limit']}</p>
<p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="{article['product_url']}">Pozrieť produkt</a></p>
</div>
</div>
<div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 24px 0; background: #fffaf5;">
<h2 style="margin-top: 0;">Vyberte riešenie podľa povrchu a spôsobu použitia</h2>
<p>{article['category_intro']}</p>
<div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff; margin: 14px 0;">
<h3 style="margin-top: 0;">{esc(article['category_name'])}</h3>
<p>{article['category_text']}</p>
<p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="{article['category_url']}">Pozrieť kategóriu</a></p>
</div>
<p><a href="{ALL_CLEANERS}">Prejsť na všetky čistiace prostriedky</a></p>
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
    parts.append(table(article["table1_headers"], article["table1_rows"]))
    parts.append(f"<h2>{esc(article['prep_heading'])}</h2>")
    parts.extend(f"<p>{paragraph}</p>" for paragraph in article["prep"])
    parts.append("<h2>Postup krok za krokom</h2>")
    parts.append("<ol>" + "".join(f"<li>{step}</li>" for step in article["steps"]) + "</ol>")
    parts.append(f"<h2>{esc(article['diagnosis_heading'])}</h2>")
    parts.append(table(article["table2_headers"], article["table2_rows"]))
    for heading, paragraphs in article["sections"]:
        parts.append(f"<h2>{esc(heading)}</h2>")
        parts.extend(f"<p>{paragraph}</p>" for paragraph in paragraphs)
    parts.append(callout("Čo si zapamätať", article["remember"], background="#f7fbf8", border="#dbe5de"))
    parts.append("<h2>Najčastejšie chyby</h2>")
    parts.append("<ul>" + "".join(f"<li>{item}</li>" for item in article["mistakes"]) + "</ul>")
    parts.append("<h2>Kedy prestať a zvoliť opravu alebo odbornú pomoc</h2>")
    parts.extend(f"<p>{paragraph}</p>" for paragraph in article["caution"])
    parts.append(f"<h2>{esc(article['expert_heading'])}</h2>")
    parts.extend(f"<p>{paragraph}</p>" for paragraph in article["expert"])
    parts.append(source_box(article))
    parts.append(commercial_blocks(article))
    parts.append(related_links(article["related"]))
    parts.append(faq(article["faq_title"], article["faq"]))
    return "\n".join(parts)


ARTICLES = [
    {
        "title": "Ako vyčistiť mop a vedro: špinavá voda, usadeniny a správne sušenie",
        "link": "ako-vycistit-mop-a-vedro-spinava-voda-usadeniny-a-spravne-susenie",
        "meta": "Ako vyčistiť mop a vedro po umývaní podlahy: hlavica, špinavá voda, usadeniny, zápach, sušenie a signály, kedy pomôcku vymeniť.",
        "short": "Mop ani vedro neodkladajte hneď po vyliatí špinavej vody. Hlavicu najprv opláchnite alebo vyperte podľa materiálu, vedro umyte od dna po okraj a obe časti nechajte samostatne úplne vyschnúť. Tak zabránite tomu, aby sa pri ďalšom umývaní na podlahu vracali nečistoty, zatuchnutá voda a zvyšky prípravku.",
        "answer": "Špinavú vodu vylejte do vhodného odpadu, z mopu odstráňte vlasy a pevné nečistoty a hlavicu vyčistite podľa štítku výrobcu. Vedro umyte teplou vodou a primeraným čistiacim roztokom, dôkladne opláchnite a osušte. Mop skladujte s rozloženými vláknami a prístupom vzduchu, nie mokrý vo vedre.",
        "quick": [
            "<strong>Materiál hlavice:</strong> mikrovlákno, bavlna, špongia a jednorazová podložka neznášajú rovnaký postup.",
            "<strong>Špinavá voda:</strong> nevylievajte ju tam, kde môže upchať odtok vlasmi alebo hrubými nečistotami.",
            "<strong>Čistiaci produkt:</strong> zvyšok prípravku nie je výhoda; vedro aj hlavica potrebujú oplach.",
            "<strong>Sušenie:</strong> mokrá hlavica uložená v uzavretom vedre je častou príčinou zatuchnutého pachu.",
            "<strong>Výmena:</strong> rozpadnuté vlákna, prasknutý držiak alebo trvalý zápach sa nedajú vyriešiť silnejšou vôňou.",
        ],
        "overview_heading": "Prečo môže špinavý mop zhoršiť výsledok umývania podlahy",
        "overview": [
            "Mop počas upratovania zachytáva prach, jemný piesok, vlasy, mastnotu, zvyšky jedla aj tekutinu z podlahy. Keď sa hlavica opakovane ponára do jedného vedra, časť nečistôt sa uvoľní do vody a časť zostane medzi vláknami. Na konci preto nestačí vodu len vyliať. Ak hlavicu neprepláchnete, pri ďalšom použití sa staré zvyšky znovu navlhčia a prenesú na povrch.",
            "Vedro sa zanáša inak než mop. Na dne ostáva piesok a sediment, pri hladine sa môže vytvoriť prstenec z mastnoty a čistiaceho roztoku a v ryhách pri výlevke sa drží voda. Priehľadná nádoba odhalí povlak rýchlo, na tmavom plaste ho však často prezradí až klzký pocit alebo pach. Dôležité je preto čistiť celé vedro, nielen ho raz prepláchnuť.",
            "Zápach po umývaní podlahy nevzniká iba z nedostatku vône. Často ide o vlhkosť, organické zvyšky a pomalé schnutie. Pridanie väčšieho množstva parfumovaného prípravku môže pach na chvíľu prekryť, ale neodstráni usadeninu z vedra ani špinu z vnútra hlavice. Najprv treba obnoviť čistotu pomôcok a až potom riešiť vôňu čerstvo umytej podlahy.",
            "Správny postup závisí aj od konštrukcie. Plochý mop má odnímateľný návlek, rotačný mop strapcovú hlavicu a špongiový mop nasiakavý valec s mechanizmom žmýkania. Návod výrobcu rozhoduje, či sa textilná časť smie prať v práčke, pri akej teplote a či sa dá bezpečne oddeliť od plastového držiaka.",
        ],
        "table1_headers": ["Typ pomôcky", "Kde sa drží nečistota", "Bezpečný začiatok"],
        "table1_rows": [
            ("Plochý mop s návlekom", "vo vláknach, švoch a suchom zipse", "návlek odobrať, vytriasť a čistiť podľa štítku"),
            ("Rotačný alebo strapcový mop", "v strede hlavice a pri závite", "odstrániť vlasy, opakovane prepláchnuť a vyžmýkať"),
            ("Špongiový mop", "v póroch špongie a pri žmýkacom mechanizme", "jemne prepláchnuť bez lámania a nechať otvorený vyschnúť"),
            ("Vedro so žmýkačom", "na dne, pod košom a v spojoch", "rozobrať iba odnímateľné časti podľa návodu"),
            ("Vedro s dvoma komorami", "v rohoch a medzi čistou a špinavou zónou", "každú komoru umyť osobitne a úplne vyprázdniť"),
        ],
        "prep_heading": "Čo pripraviť pred čistením mopu a vedra",
        "prep": [
            "Nasaďte si rukavice, ak pracujete so silne znečistenou vodou alebo citlivou pokožkou. Pripravte si starú kefku určenú iba na upratovacie pomôcky, čistú handričku a miesto, kde môže mop voľne odkvapkať. Vlasy, chumáče a väčšie kúsky najprv vyberte mechanicky; nemajú končiť v úzkom umývadlovom odpade.",
            "Pred použitím ďalšieho prípravku zistite, čo už vo vedre bolo. Rôzne čističe sa nemajú miešať a silnejšia koncentrácia neznamená automaticky lepší výsledok. Ak ste použili produkt podľa etikety na podlahu, na bežné umytie vedra často stačí voda, primerané množstvo kompatibilného detergentu a dôkladné opláchnutie. Dezinfekciu riešte iba pri reálnom dôvode a presne podľa etikety určeného produktu.",
        ],
        "steps": [
            "Špinavú vodu nechajte krátko ustáť a zachyťte vlasy či pevné nečistoty, aby zbytočne nešli do úzkeho odpadu.",
            "Z mopu odstráňte návlek alebo hlavicu len spôsobom, ktorý povoľuje výrobca; skontrolujte závit, suchý zips a pohyblivé časti.",
            "Voľný piesok, vlasy a chumáče vyberte nasucho alebo pod miernym prúdom vody bez rozstreku po okolí.",
            "Textilný návlek vyperte samostatne podľa štítku, bez aviváže, ktorá môže obaliť savé vlákna; ak pranie v práčke povolené nie je, dôkladne ho ručne prepláchnite.",
            "Vedro opláchnite, potom kefkou prejdite dno, rohy, výlevku, držadlo a dostupné časti žmýkacieho koša.",
            "Pri mastnom prstenci použite malé množstvo kompatibilného čističa naneseného na handričku alebo kefku, nie náhodnú zmes viacerých produktov.",
            "Všetky umývané časti opláchnite čistou vodou tak, aby na nich neostal klzký film ani pena.",
            "Vedro obráťte alebo utrite dosucha a hlavicu rozložte tak, aby medzi vláknami prúdil vzduch.",
            "Pred uložením skontrolujte, že v dutinách, pod žmýkačom a v strede hlavice neostala voda; mop neskladujte ponorený vo vedre.",
        ],
        "diagnosis_heading": "Diagnostika zápachu, šmúh a slabej savosti",
        "table2_headers": ["Prejav", "Pravdepodobná príčina", "Ďalší krok"],
        "table2_rows": [
            ("Mop zapácha hneď po namočení", "vlhká hlavica bola skladovaná bez vzduchu", "dôkladne vyčistiť, vysušiť a pri trvalom pachu vymeniť"),
            ("Podlaha ostáva lepkavá", "veľa produktu, špinavá voda alebo slabý oplach", "znížiť dávku, pracovať s čistejšou vodou a opláchnuť pomôcky"),
            ("Mop zanecháva sivé šmuhy", "vo vláknach zostal prach a mastnota", "najprv vyprať hlavicu a pri práci meniť vodu skôr"),
            ("Návlek prestal sať", "obalené alebo poškodené vlákna", "prať bez aviváže a zhodnotiť opotrebovanie"),
            ("Vedro je klzké", "film z prípravku a nečistôt", "umyť rohy aj steny, oplachovať do straty klzkého pocitu"),
        ],
        "sections": [
            ("Ako vyčistiť textilnú hlavicu mopu bez straty savosti", [
                "Odnímateľný návlek najprv vytraste a zbavte vlasov. Ak štítok povoľuje pranie, perte ho oddelene od uterákov a oblečenia, ktoré nechcete zaťažiť nečistotami z podlahy. Nepoužívajte aviváž: zmäkčujúci film môže znížiť kontakt vlákien s vodou a zhoršiť zachytávanie jemného prachu. Užitočný je aj samostatný návod <a href=\"/n/ako-prat-mikrovlaknove-utierky-aby-nezapachali-a-dobre-cistili\">ako prať mikrovláknové utierky</a>.",
                "Ak hlavica nie je určená do práčky, ručne ju preplachujte dovtedy, kým odtekajúca voda nie je bez viditeľných nečistôt a peny. Netreba ju dlho namáčať v koncentrovanom čističi. Dôležitejšie je mechanicky uvoľniť zvyšky, dobre opláchnuť a sušiť tak, aby neostal zrolovaný vlhký stred.",
            ]),
            ("Ako vyčistiť vedro od usadenín a mastného prstenca", [
                "Vedro umývajte od najčistejších dostupných častí k dnu. Najprv opláchnite voľnú špinu, potom prejdite držadlo, okraj, steny, výlevku a nakoniec dno. Starou kefkou sa dostanete do rohov a pod plastové výstupky. Ak je žmýkač odnímateľný bez náradia, vyčistite aj priestor pod ním; ak nie, mechanizmus nenásilne nerozoberajte.",
                "Biely minerálny povlak a mastný sivý film nie sú ten istý problém. Pri každom produkte skontrolujte etiketu a kompatibilitu s plastom. Kyslé a zásadité prípravky sa nesmú miešať. Ak neviete, čo zostalo vo vedre, najprv ho opakovane opláchnite čistou vodou a až potom použite jeden zvolený postup.",
            ]),
            ("Ako zabrániť tomu, aby sa špinavá voda vracala na podlahu", [
                "Pri veľkej alebo veľmi špinavej ploche vymeňte vodu skôr, než je nepriehľadná a cítiť z nej nečistoty. Praktické je najprv povysávať alebo pozametať hrubý prach a až potom mopovať. Menej piesku vo vode znamená menšie riziko škrabancov aj menšie zaťaženie mopu. Podrobný postup podľa povrchu nájdete v článku <a href=\"/n/ako-umyt-podlahu-bez-smuh-laminat-vinyl-dlazba-a-mopovanie-v-praxi\">ako umyť podlahu bez šmúh</a>.",
                "Jednokomorové vedro má prirodzený limit: hlavica sa vracia do tej istej vody. Pomáha používať primerane malú plochu, vodu včas vymeniť a posledný úsek nečistiť už sivým roztokom. Dvojkomorový systém oddelí časť nečistôt, ale aj jeho obe komory treba po práci umyť a vysušiť.",
            ]),
            ("Ako sušiť mop, aby nezatuchol", [
                "Najhorší spôsob je nechať mokrú hlavicu stlačenú na dne vedra. Voda sa drží v strede, vzduch sa k vláknam nedostane a pach sa objaví pri ďalšom navlhčení. Návlek zaveste alebo rozložte v jednej vrstve, strapcovú hlavicu nechajte otvorenú a špongiu nezaťažujte predmetom, ktorý ju deformuje.",
                "Mop nesušte na špinavom radiátore ani tesne pri otvorenom ohni. Primerané prúdenie vzduchu je dôležitejšie než prudké teplo. Pred uložením skontrolujte nielen povrch, ale aj švy, plastový úchyt a dutinu pri závite. Ak je pomôcka zvonka suchá a v strede stále vlhká, zápach sa môže vrátiť.",
            ]),
            ("Kedy mop vyčistiť a kedy už vymeniť hlavicu", [
                "Čistenie pomôže, keď sú vlákna zanesené bežnou špinou, ale stále držia tvar a hlavica sa dá úplne opláchnuť. Výmena je rozumnejšia pri rozpadajúcej sa pene, stvrdnutých vláknach, uvoľnených švoch, prasknutom plastovom uchytení alebo pachu, ktorý sa vracia aj po dôkladnom vyčistení a vysušení.",
                "Nečakajte na univerzálny počet použití. Mop v domácnosti so zvieraťom, pri vstupe z ulice alebo po rekonštrukcii pracuje v iných podmienkach než mop v malej spálni. Rozhoduje stav materiálu, čistota odtekajúcej vody, savosť a možnosť hygienicky pomôcku vysušiť.",
            ]),
            ("Ako oddeliť čistenie od dezinfekcie", [
                "Bežné umytie mopu odstraňuje viditeľnú špinu, zvyšky produktu a časť mikroorganizmov mechanicky. Dezinfekcia je iný proces s určeným prípravkom, koncentráciou a časom pôsobenia. EPA vysvetľuje, že čistenie odstraňuje špinu a organické zvyšky, kým dezinfekčný účinok musí byť podložený určeným produktom a dodržanou etiketou.",
                "Ak ste mop použili na telesné tekutiny, po infekčnom ochorení alebo v priestore s osobitnými hygienickými požiadavkami, riaďte sa etiketou vhodného dezinfekčného produktu a materiálovým limitom hlavice. Nemieste chemikálie a nepredpokladajte, že vôňa alebo pena dokazujú dezinfekčný účinok.",
            ]),
        ],
        "remember": [
            "Hlavica, vedro a žmýkací mechanizmus sa čistia ako tri odlišné zóny.",
            "Oplach odstráni zvyšky produktu; úplné vysušenie obmedzí návrat zatuchnutého pachu.",
            "Vôňa patrí až k čistej podlahe a čistej pomôcke, nie na prekrytie zaneseného mopu.",
        ],
        "mistakes": [
            "Odloženie mokrého mopu priamo do zatvoreného vedra.",
            "Pranie mikrovláknového návleku s avivážou alebo textíliami plnými chlpov.",
            "Dolievane čistej vody do už silne znečisteného roztoku namiesto úplnej výmeny.",
            "Zmiešanie zvyškov viacerých čističov bez opláchnutia vedra.",
            "Násilné rozoberanie žmýkacieho mechanizmu alebo ignorovanie prasknutého uchytenia.",
            "Hodnotenie mopu iba podľa vône, nie podľa savosti, nečistôt a stavu vlákien.",
        ],
        "caution": [
            "Prasknuté vedro, uvoľnené držadlo alebo poškodený žmýkací mechanizmus môže pri prenášaní plnej nádoby zlyhať. Takú pomôcku neopravujte provizórne počas práce. Vymeňte ju alebo použite iba po spoľahlivej oprave podľa výrobcu.",
            "Ak sa pri čistení objaví ostrý chemický pach, podráždenie očí alebo nezvyčajná reakcia zmesi, odíďte na čerstvý vzduch, vyvetrajte a ďalšie produkty nepridávajte. Pri vážnych ťažkostiach postupujte podľa etikety a vyhľadajte odbornú pomoc.",
        ],
        "expert_heading": "Prečo rozhoduje mechanické odstránenie, oplach a sušenie",
        "expert": [
            "Čistiaci roztok uvoľňuje časť mastnoty a špiny, no nečistota musí z pomôcky aj fyzicky odísť. Preto je dôležité vybrať vlasy, vypláchnuť sediment a meniť vodu. Ak sa iba pridáva ďalší produkt, koncentrácia rozpustených a rozptýlených zvyškov rastie, ale mop sa automaticky neobnoví.",
            "EPA odlišuje čistenie od dezinfekcie: čistenie odstraňuje špinu a organický materiál pomocou detergentu, kým dezinfekcia používa registrovaný chemický produkt na mikroorganizmy. Pri mopu má zmysel najprv odstrániť špinu a produktový film. Dezinfekčný krok bez predchádzajúceho čistenia nemá byť náhradou za zanedbanú údržbu.",
            "EPA pri používaní čistiacich produktov odporúča riadiť sa etiketou, nemiešať chemikálie a počas čistenia vetrať. Tieto zásady platia aj pri vedre: viac rôznych prípravkov neznamená lepší výsledok a uzavretá kúpeľňa môže zvyšovať expozíciu výparom.",
        ],
        "source_intro": "Zdroje podporujú rozlíšenie bežného čistenia a dezinfekcie, čítanie etikety, nemiešanie produktov a vetranie. Konkrétny štítok mopu a podlahy má pred všeobecným návodom prednosť.",
        "sources": [
            ("US EPA: Difference Between Cleaning, Sanitizing and Disinfecting", EPA_CLEANING),
            ("US EPA: Ventilation When Cleaning or Sanitizing Indoors", EPA_CLEANING_VENTILATION),
            ("US EPA: Improving Your Indoor Environment", EPA_INDOOR),
        ],
        "product_heading": "Jemná vôňa až po vyčistení mopu a podlahy",
        "product_name": "Parfum na podlahy Vevo Premium No.01 Cotton Paradise",
        "product_url": FLOOR_PRODUCT,
        "product_intro": "Keď je mop čistý, vedro opláchnuté a podlaha zbavená nečistôt, môžete pri ďalšom umývaní zvoliť produkt určený na prevoňanie podlahy. Dávkovanie a kompatibilitu vždy skontrolujte na etikete aj pri výrobcovi podlahy.",
        "product_use": "ako finálny voňavý krok pri ručnom umývaní kompatibilnej podlahy podľa návodu produktu.",
        "product_limit": "nepoužívajte ho na čistenie zatuchnutej hlavice ani v parnom zariadení či stroji, ktorý takýto produkt výslovne nepovoľuje.",
        "category_name": "Vône na umývanie podláh",
        "category_url": FLOOR_CATEGORY,
        "category_intro": "Pri podlahe oddeľte tri otázky: čo odstráni špinu, čo je bezpečné pre materiál a čo vytvorí výsledný voňavý dojem. Jeden produkt nemusí riešiť všetky tri kroky.",
        "category_text": "Pozrite si vône určené na umývanie podláh a pred použitím porovnajte ich návod s typom povrchu aj spôsobom mopovania.",
        "related": [
            ("Ako umyť podlahu bez šmúh", "/n/ako-umyt-podlahu-bez-smuh-laminat-vinyl-dlazba-a-mopovanie-v-praxi"),
            ("Ako prať mikrovláknové utierky", "/n/ako-prat-mikrovlaknove-utierky-aby-nezapachali-a-dobre-cistili"),
            ("Ako vybrať robotický vysávač s mopom", "/n/ako-vybrat-roboticky-vysavac-s-mopom"),
        ],
        "faq_title": "čistenie mopu a vedra",
        "faq": [
            ("Môžem dať hlavicu mopu do práčky?", "Iba ak to povoľuje štítok výrobcu. Oddeľte ju od plastového držiaka, odstráňte vlasy a pri mikrovlákne nepoužívajte aviváž. Ak pranie povolené nie je, hlavicu dôkladne ručne prepláchnite."),
            ("Prečo mop zapácha aj po opláchnutí?", "Vo vnútri mohla zostať organická špina alebo vlhkosť. Pomôcku treba vyčistiť do hĺbky, úplne vysušiť a pri trvalom pachu či poškodení vymeniť, nie iba silnejšie prevoňať."),
            ("Ako často treba vedro umyť?", "Po každom použití ho aspoň vyprázdnite, opláchnite a vysušte. Dôkladnejšie prejdite dno, rohy a žmýkač vždy, keď vidíte sediment, klzký film alebo cítite pach."),
            ("Môžem nechať mop namočený vo vedre do ďalšieho dňa?", "Nie je to vhodné. Vlákna ostanú dlho mokré, zvyšky nečistôt sa neuvoľnia čistým oplachom a pomôcka môže zatuchnúť. Mop vyberte, opláchnite a sušte s prístupom vzduchu."),
            ("Pomôže viac čističa proti šmuhám?", "Často práve naopak. Prebytok produktu môže na podlahe aj v mopu zanechať film. Dodržte dávkovanie, pracujte s primerane čistou vodou a pomôcky po práci opláchnite."),
        ],
    },
    {
        "title": "Ako vyčistiť strop a rohy od prachu a pavučín bez rozmazania",
        "link": "ako-vycistit-strop-a-rohy-od-prachu-a-pavucin-bez-rozmazania",
        "meta": "Ako odstrániť prach a pavučiny zo stropu a rohov bez sivých máp. Postup pre maľovku, tapetu, škvrny, bezpečnú prácu vo výške aj pleseň.",
        "short": "Strop a rohy čistite najprv nasucho: povysávajte mäkkým nadstavcom alebo pavučiny zachyťte čistou prachovkou bez tlačenia. Vlhké čistenie skúšajte iba lokálne na umývateľnom povrchu. Ak vidíte vlhkostnú mapu, odlupujúcu sa farbu alebo opakujúce sa čierne bodky, neriešte ich rozmazaním ani prevoňaním, ale príčinou.",
        "answer": "Odstráňte predmety spod pracovnej zóny, vypnite stropné zariadenia a použite stabilné schodíky. Pavučiny a voľný prach sťahujte pomaly od rohu do stredu pomocou čistého mäkkého nadstavca. Až potom na skrytom mieste overte, či maľovka znesie mierne vlhkú handričku. Veľké mapy, pleseň alebo poškodená omietka patria osobitnému riešeniu.",
        "quick": [
            "<strong>Najprv diagnostika:</strong> suchý prach, pavučina, mastný povlak, vodná mapa a pleseň potrebujú rozdielny postup.",
            "<strong>Práca vo výške:</strong> použite stabilné schodíky na rovnej suchej podlahe, nie stoličku alebo posteľ.",
            "<strong>Maľovka:</strong> aj farba označená ako umývateľná sa testuje na nenápadnom mieste a bez silného trenia.",
            "<strong>Prach:</strong> mokrá handrička na hrubej vrstve vytvorí sivý film a zväčší škvrnu.",
            "<strong>Vlhkosť:</strong> opakovaná mapa alebo čierne bodky sa najprv diagnostikujú, nie prekrývajú farbou či vôňou.",
        ],
        "overview_heading": "Prečo sa prach drží práve v rohoch a pri strope",
        "overview": [
            "Jemné častice sa pohybujú so vzduchom a usádzajú sa na vodorovných hranách, pri rímsach, svietidlách a v miestach so slabším prúdením. Pavučina vytvára tenké vlákna, ktoré zachytia ďalší prach a peľ. Keď ju prudko zotriete o stenu, tmavý povlak sa môže rozmazať do maľovky a z malej pavučiny vznikne viditeľná sivá mapa.",
            "Iný charakter má strop v kuchyni. Aerosól z varenia môže spolu s prachom vytvoriť lepkavý film, najmä pri otvorenej kuchyni, digestore a horných hranách. V kúpeľni zase rozhoduje para a kondenzácia. Na prvý pohľad podobný tmavý roh preto môže byť iba pavučina, mastný povlak alebo prejav dlhodobej vlhkosti.",
            "Povrch stropu býva citlivejší než obklad či pracovná doska. Matná maľovka sa môže po trení vyleštiť, stará farba púšťa pigment a papierová tapeta reaguje na vodu. Hrubá špongia, melamínová pena alebo koncentrovaný čistič môžu odstrániť nielen škvrnu, ale aj vrchnú vrstvu povrchu.",
            "Bezpečnosť je súčasťou výsledku. Pri pohľade nahor sa mení rovnováha a človek má tendenciu naťahovať sa do strany. Lepšie je schodíky opakovane premiestniť, než pracovať z posledného stupňa alebo stáť na nestabilnom nábytku. Pred čistením svietidla či dymového hlásiča sa riaďte samostatným návodom zariadenia.",
        ],
        "table1_headers": ["Povrch alebo prejav", "Hlavné riziko", "Bezpečný prvý krok"],
        "table1_rows": [
            ("Matná maľovka", "vyleštenie, strata pigmentu a mapy", "mäkké suché odsatie bez tlaku"),
            ("Umývateľná farba", "lokálny farebný rozdiel po trení", "test mierne vlhkou bielou handričkou"),
            ("Tapeta na strope", "odlepovanie a nasiaknutie vody", "suchá metóda a kontrola výrobcu tapety"),
            ("Pavučina v rohu", "rozmazanie zachyteného prachu", "pomaly ju namotať alebo odsať od okrajov"),
            ("Vodná mapa alebo čierne bodky", "netesnosť, kondenzácia alebo pleseň", "zdokumentovať a nájsť zdroj vlhkosti pred čistením"),
        ],
        "prep_heading": "Ako pripraviť miestnosť a bezpečné pracovné miesto",
        "prep": [
            "Odsuňte posteľ, sedačku a drobné predmety, textílie zakryte čistou plachtou a povysávajte podlahu až po skončení práce. Vypnite stropný ventilátor a zariadenia, ktoré by mohli rozvíriť prach. Ak čistíte pri svietidle, nepribližujte vlhkú pomôcku k elektrickým častiam a postupujte podľa návodu zariadenia.",
            "Schodíky skontrolujte, úplne rozložte a položte na rovnú suchú podlahu. OSHA pri prenosných rebríkoch zdôrazňuje kontrolu poškodenia, čisté stupne a tri body kontaktu pri výstupe. V domácnosti je praktický záver jednoduchý: nenakláňajte sa ďaleko do strany, nevystupujte na plochu, ktorá na státie nie je určená, a majte druhú osobu nablízku pri náročnejšom priestore.",
        ],
        "steps": [
            "Pri dennom alebo bočnom svetle si strop prezrite a odlíšte voľný prach, pavučiny, mastnú mapu, vodné zafarbenie a možné známky plesne.",
            "Odstráňte predmety spod pracovného miesta, zakryte textílie a vypnite ventilátory či zariadenia, ktoré by prach rozfúkali.",
            "Stabilné schodíky umiestnite pod čistený úsek; nepracujte z postele, otočnej stoličky ani mokrej podlahy.",
            "Pavučiny zachyťte čistou mäkkou prachovkou alebo vysávačom s kefovým nadstavcom a pohybujte sa pomaly od rohov.",
            "Voľný prach odsávajte bez tlaku na maľovku. Pomôcku priebežne kontrolujte, aby špinavý okraj nezanechal čiaru.",
            "Na skrytom mieste urobte test bielou mierne vlhkou handričkou. Ak sa prenáša farba alebo vzniká lesk, vlhké čistenie zastavte.",
            "Malú škvrnu na preukázateľne umývateľnom povrchu čistite od okraja do stredu jemným prikladaním, nie širokým drhnutím.",
            "Miesto vysušte a výsledok posudzujte až po úplnom preschnutí, pretože mokrá maľovka vyzerá tmavšia.",
            "Po skončení povysávajte spadnutý prach, vyperte ochrannú textíliu a skontrolujte, či sa v rohoch neobjavila vlhkosť alebo poškodenie.",
        ],
        "diagnosis_heading": "Ako rozlíšiť prach, mastnotu, zatečenie a pleseň",
        "table2_headers": ["Prejav", "Možná príčina", "Ďalší krok"],
        "table2_rows": [
            ("Sivá čiara po utretí", "prach bol navlhčený alebo bola špinavá pomôcka", "nechať vyschnúť, zvyšok najprv odsať a testovať jemnejšie"),
            ("Žltkastý lepkavý film", "mastný aerosól z varenia", "overiť umývateľnosť a čistiť malý úsek kompatibilným roztokom"),
            ("Hnedý kruh alebo mapa", "staršie alebo aktívne zatečenie", "zdokumentovať, skontrolovať zdroj vody a nepremaľovať naslepo"),
            ("Čierne bodky v studenom rohu", "kondenzácia alebo možná pleseň", "riešiť vlhkosť, vetranie a rozsah problému"),
            ("Farba sa odlupuje", "vlhkosť, zlý podklad alebo poškodený náter", "čistenie zastaviť a zvoliť opravu povrchu"),
        ],
        "sections": [
            ("Ako odstrániť pavučiny bez tmavých čiar", [
                "Použite dokonale čistú prachovku alebo kefový nadstavec vysávača. Pavučinu zachyťte na jednom mieste a pomaly ju namotajte alebo odsajte, namiesto toho, aby ste ju dlhým pohybom rozotreli po stene. Začnite v rohu a postupujte po krátkych úsekoch. Keď je pomôcka sivá, vyčistite ju alebo vymeňte návlek.",
                "Pri reliéfnej omietke netlačte štetiny do povrchu. Cieľom je uvoľniť ľahký nános, nie obrúsiť štruktúru. Pavučina pri záclone alebo lampe často zachytáva aj textilné vlákna; súvisiaci návod na <a href=\"/n/ako-cistit-textilne-tienidla-lamp-a-dekoracie-od-prachu\">čistenie textilných tienidiel a dekorácií</a> pomôže odstrániť ďalší zdroj prachu.",
            ]),
            ("Ako vyčistiť rohy stropu od prachu", [
                "Roh čistite z oboch priľahlých plôch smerom k hrane. Mäkkú hubicu držte tesne pri povrchu, ale neťahajte tvrdý plast po maľovke. Ak je roh za skriňou, najprv skontrolujte, či tam nie je chladná vlhká zóna. Nábytok neposúvajte nasilu a nechajte priestor na prúdenie vzduchu.",
                "Po suchom čistení sledujte, či ostal iba farebný tieň. Ak sa zmenšuje po odsatí, išlo o prach. Ak je povrch vlhký, mäkký alebo sa bodky rýchlo vracajú, ďalšie utieranie problém nevyrieši. Zaznamenajte dátum a podmienky, aby ste odlíšili jednorazový nános od opakujúceho sa stavebného problému.",
            ]),
            ("Môže sa strop umývať vodou?", [
                "Iba niektoré nátery sú určené na vlhké čistenie a aj pri nich rozhoduje vek, podklad a spôsob aplikácie. Urobte test bielou handričkou na menej viditeľnom mieste. Ak sa na handričku prenáša pigment, povrch sa leskne alebo sa okraj škvrny zväčšuje, prestaňte. Mokré miesto nechajte vyschnúť bez ďalšieho trenia.",
                "Na lokálnu škvrnu použite minimum vody a jemný tlak. Rozprašovanie priamo nad hlavu zvyšuje riziko kvapiek do očí, na nábytok a k elektrickým zariadeniam. Produkt naneste na handričku mimo stropu. Pri neznámej maľovke je bezpečnejšie zostať pri suchom odstránení prachu a prípadnú obnovu náteru naplánovať samostatne.",
            ]),
            ("Mastný strop v kuchyni", [
                "Mastnota viaže prach a čistá suchá prachovka po nej môže kĺzať. Najprv jemne odstráňte voľnú vrstvu a potom otestujte umývateľnosť farby. Pracujte po malých štvorcoch a často meňte čistú stranu handričky. Veľký mokrý kruh môže po vyschnutí zostať viditeľnejší než pôvodná škvrna.",
                "Ak je povrch silne zanesený po rokoch varenia a náter nie je umývateľný, nekonečné lokálne drhnutie vytvorí nerovnomerné miesta. Najprv riešte digestor a zdroj aerosólu, potom zvážte odbornú prípravu a nový vhodný náter. Pri čistení ďalších kuchynských plôch použite postup pre <a href=\"/n/ako-vycistit-kuchynsku-linku-od-mastnoty-prachu-a-smuh-bez-poskodenia-povrchu\">mastnotu na kuchynskej linke</a>.",
            ]),
            ("Vodná mapa na strope nie je bežná škvrna", [
                "Hnedý kruh, mäkká omietka, bubliny alebo odlupovanie môžu signalizovať zatečenie zo strechy, potrubia alebo bytu nad vami. Pred čistením odfoťte stav a zistite, či je miesto suché. Premaľovanie aktívneho úniku iba ukryje symptóm a ďalšia voda môže poškodiť náter aj konštrukciu.",
                "Po odstránení zdroja musí materiál bezpečne vyschnúť a rozsah poškodenia treba posúdiť. Ak je strop pri elektrickom zariadení mokrý, nedotýkajte sa ho a riešte elektrickú bezpečnosť aj únik s odborníkom. Domáci čistič nie je určený na opravu podkladu alebo neutralizáciu neznámej kontaminovanej vody.",
            ]),
            ("Čierne bodky a podozrenie na pleseň", [
                "Pleseň súvisí s vlhkosťou, preto je potrebné odstrániť zdroj vody alebo kondenzácie. EPA uvádza, že rozsah a príčina rozhodujú o tom, či je vhodné domáce čistenie alebo odborná sanácia. Pórovité stropné dosky môžu byť zasiahnuté do hĺbky a nie vždy sa dajú obnoviť jednoduchým utretím.",
                "Ak je plocha väčšia, problém sa vracia, došlo k väčšiemu poškodeniu vodou alebo má niekto zdravotné ťažkosti, obmedzte expozíciu a konzultujte odborný postup. Pleseň neprekrývajte farbou ani vôňou. Povrch sa môže kozmeticky zmeniť, no vlhkostná príčina zostane.",
            ]),
        ],
        "remember": [
            "Prach a pavučiny sa najprv zachytávajú nasucho; voda prichádza až po skúške povrchu.",
            "Bezpečné schodíky a časté premiestňovanie sú lepšie než naťahovanie sa do strany.",
            "Vodná mapa, odlupovanie a opakujúce sa čierne bodky vyžadujú riešenie príčiny.",
        ],
        "mistakes": [
            "Státie na otočnej stoličke, posteli alebo hornom nepochôdznom stupni.",
            "Rozotretie suchej pavučiny mokrou špongiou cez veľkú plochu.",
            "Priame striekanie čističa nad hlavu pri svietidle alebo hlásiči.",
            "Silné drhnutie matnej farby, ktoré vytvorí lesklý fľak.",
            "Premaľovanie aktívnej vodnej mapy bez odstránenia úniku.",
            "Prekrytie zatuchnutia vôňou namiesto kontroly vlhkosti a vetrania.",
        ],
        "caution": [
            "Ak je strop mokrý pri svietidle, praská, prehýba sa alebo z neho odpadáva omietka, nevstupujte pod poškodené miesto. Vypnutie elektrického okruhu a posúdenie úniku či konštrukcie patrí kvalifikovanému odborníkovi alebo správcovi budovy.",
            "Pri väčšom rozsahu plesne, rozsiahlej škode po vode alebo nejasnej kontaminácii obmedzte domáce drhnutie. Odborné posúdenie má prednosť najmä pri pórovitom materiáli a u ľudí s dýchacími alebo imunitnými problémami.",
        ],
        "expert_heading": "Prečo vlhké utieranie viaže prach a kedy je nevhodné",
        "expert": [
            "EPA odporúča na bežné tvrdé povrchy vlhké utieranie, pretože navlhčená handrička pomáha zachytiť usadený prach a obmedziť jeho návrat do vzduchu. Strop však často nie je umývateľný tvrdý povrch. Preto sa táto zásada používa až po suchom odstránení nánosu a po teste náteru.",
            "Vnútorný prach môže obsahovať peľ, zvieracie alergény, kožné častice a častice z varenia či sviečok. Prudké zametanie alebo špinavá prachovka ho rozvíri. Pomalé odsávanie s mäkkým nadstavcom zachytí väčšiu časť tam, kde sa uvoľňuje, a znižuje množstvo spadnuté na posteľ alebo sedačku.",
            "Pri plesni je zásadné riadenie vlhkosti. EPA odporúča opraviť úniky, materiály úplne vysušiť a rozsah problému posúdiť podľa plochy a typu materiálu. Na pórovitom strope nemusí byť jednoduché čistenie dostatočné. Preto sa vizuálna škvrna a technická príčina posudzujú oddelene.",
        ],
        "source_intro": "EPA podporuje časté zachytávanie prachu vlhkou handričkou na vhodných povrchoch a riešenie zdroja vlhkosti pri plesni. OSHA dopĺňa základné pravidlá bezpečnej práce na prenosných schodíkoch.",
        "sources": [
            ("US EPA: Sources of Indoor Particulate Matter", EPA_PM),
            ("US EPA: Mold Cleanup in Your Home", EPA_MOLD),
            ("OSHA: Portable Ladder Safety QuickCard", OSHA_LADDER),
        ],
        "product_heading": "Lokálne čistenie iba na preukázateľne umývateľnom povrchu",
        "product_name": "Univerzálny voňavý čistič Vevo Pure Harmony 500 ml",
        "product_url": UNIVERSAL_PRODUCT,
        "product_intro": "Na malú škvrnu na umývateľnej maľovke alebo kompatibilnej lište môže byť vhodný univerzálny čistič, ale až po odstránení prachu a skúške na skrytom mieste.",
        "product_use": "v minimálnom množstve nanesenom na bielu handričku pri lokálnom čistení povrchu, ktorý výrobca označil ako umývateľný.",
        "product_limit": "nepoužívajte ho na pleseň, vodnú mapu, odlupujúcu sa farbu, papierovú tapetu ani priamo pri elektrických zariadeniach.",
        "category_name": "Univerzálne čističe do domácnosti",
        "category_url": UNIVERSAL_CATEGORY,
        "category_intro": "Pri stropoch je kompatibilita dôležitejšia než sila prípravku. Najprv identifikujte povrch a príčinu škvrny, potom voľte najmenší účinný zásah.",
        "category_text": "Kategória je vhodná na porovnanie produktov pre bežné umývateľné povrchy. Matnú maľovku, tapetu, poškodený podklad alebo pleseň však neriešte univerzálnym produktom naslepo.",
        "related": [
            ("Ako vyčistiť radiátor od prachu", "/n/ako-vycistit-radiator-od-prachu-rebra-zadna-strana-mastnota-a-bezpecna-udrzba"),
            ("Ako čistiť textilné tienidlá a dekorácie", "/n/ako-cistit-textilne-tienidla-lamp-a-dekoracie-od-prachu"),
            ("Ako vyčistiť interiérové dvere a zárubne", "/n/ako-vycistit-interierove-dvere-a-zarubne-odtlacky-mastnota-a-povrch-bez-smuh"),
        ],
        "faq_title": "strop, rohy a pavučiny",
        "faq": [
            ("Ako odstrániť pavučinu bez šmuhy?", "Použite čistú suchú prachovku alebo mäkký nadstavec vysávača a pavučinu pomaly zachyťte od rohu. Netlačte ju mokrou handričkou po maľovke."),
            ("Môžem umyť maľovaný strop?", "Iba ak je náter umývateľný a test na skrytom mieste neukáže púšťanie farby, lesk alebo mapu. Použite minimum vody a jemné prikladanie bez širokého drhnutia."),
            ("Čo znamená hnedá mapa na strope?", "Môže ísť o staré alebo aktívne zatečenie. Najprv zistite, či je miesto suché, nájdite zdroj vody a skontrolujte podklad. Premaľovanie bez opravy príčinu nevyrieši."),
            ("Ako často odstraňovať pavučiny v rohoch?", "Keď sú viditeľné alebo sa v nich drží prach. Krátke pravidelné suché čistenie je šetrnejšie než odstraňovanie hrubého nánosu mokrou pomôckou."),
            ("Môžem čierne bodky jednoducho zotrieť?", "Najprv posúďte vlhkosť, rozsah a opakovanie. Malý povrchový problém na vhodnom materiáli môže mať domáci postup, ale väčšia alebo vracajúca sa pleseň vyžaduje riešenie zdroja a často odborné posúdenie."),
        ],
    },
    {
        "title": "Ako vyčistiť garnižu a koľajnice záclon: prach, mastnota a zasekávanie",
        "link": "ako-vycistit-garnizu-a-kolajnice-zaclon-prach-mastnota-a-zasekavanie",
        "meta": "Ako vyčistiť garnižu a koľajnice záclon od prachu a mastnoty, uvoľniť drážky, skontrolovať jazdce, bezpečne sušiť a riešiť zasekávanie.",
        "short": "Z garniže a koľajnice najprv odstráňte záclonu podľa spôsobu zavesenia a povysávajte voľný prach. Kovové, plastové aj lakované časti čistite až po skúške materiálu mierne vlhkou handričkou. Drážku úplne vysušte, skontrolujte jazdce a neuvoľňujte pohyb náhodným olejom, ktorý by mohol zachytiť prach alebo zašpiniť textil.",
        "answer": "Záclony odopnite bez ťahania za koľajnicu, odfoťte poradie háčikov a pracujte zo stabilných schodíkov. Garnižu najprv povysávajte mäkkým nadstavcom, potom utrite kompatibilné povrchy dobre vyžmýkanou handričkou. V drážke odstráňte nite a prach, všetko vysušte a pri zasekávaní najprv skontrolujte poškodený jazdec, spoj alebo uvoľnené kotvenie.",
        "quick": [
            "<strong>Kotvenie:</strong> pred čistením skontrolujte uvoľnené konzoly, praskliny a prehnutú tyč.",
            "<strong>Poradie:</strong> odfoťte háčiky, koncovky a jazdce, aby sa systém správne zložil.",
            "<strong>Prach:</strong> najprv ho vysajte; mokré rozotieranie vytvorí sivú špinu v drážke.",
            "<strong>Materiál:</strong> surový kov, lakovaný kov, plast, drevo a eloxovaný hliník vyžadujú odlišnú opatrnosť.",
            "<strong>Zasekávanie:</strong> mazivo nepridávajte naslepo; často prekáža niť, poškodený jazdec alebo posunutý spoj.",
        ],
        "overview_heading": "Prečo sa garniža zanáša, hoci sa jej takmer nedotýkame",
        "overview": [
            "Garniža a koľajnica sú vysoko nad bežnou úrovňou očí, preto sa kontrolujú zriedka. Na hornej hrane sa usádza prach, peľ a textilné vlákna zo záclon. Pri otvorenom okne pribúdajú vonkajšie častice a v kuchyni sa môžu spojiť s mastným aerosólom. Keď záclonu posúvate, časť nánosu padá na textil alebo sa vtlačí do drážky.",
            "Zasekávanie nemusí znamenať, že koľajnica potrebuje olej. Častou príčinou je chumáč prachu, zachytená niť, ohnutý háčik, poškodený jazdec, nerovný spoj dvoch profilov alebo príliš veľká hmotnosť závesu. Olejový film môže naopak zachytiť ďalší prach a zanechať škvrnu na svetlej záclone.",
            "Samotná garniža môže byť drevená, kovová, lakovaná, plastová alebo kombinovaná. Koncovky bývajú lepené či skrutkované a koľajnicový profil môže mať servisný otvor. Jeden univerzálny spôsob rozoberania preto neexistuje. Návod výrobcu a pôvodná montáž určujú, čo sa môže odobrať bez narušenia kotvenia.",
            "Práca prebieha vo výške a často pri okne. Pred čistením zatvorte krídlo, odstráňte parapetné predmety a schodíky postavte na rovnú suchú podlahu. Neopierajte sa o tyč ani koľajnicu; nie sú určené ako držadlo a uvoľnená konzola môže vytrhnúť časť omietky.",
        ],
        "table1_headers": ["Typ systému", "Kde sa drží nečistota", "Na čo si dať pozor"],
        "table1_rows": [
            ("Okrúhla kovová tyč", "horná hrana, konzoly a krúžky", "poškriabanie laku a uvoľnené skrutky"),
            ("Drevená garniža", "prach pri koncovkách a spojoch", "premáčanie, vosk alebo povrchovú úpravu"),
            ("Plastová koľajnica", "drážka, jazdce a koncové zátky", "krehký plast, deformáciu a násilné páčenie"),
            ("Hliníkový profil", "spoj profilov a vnútorná drážka", "nevhodné abrazívum a zmenu povrchu"),
            ("Dvojitá koľajnica", "zadný kanál a priestor medzi textíliami", "poradie jazdcov, záclony a závesu"),
        ],
        "prep_heading": "Ako zložiť záclonu a pritom nestratiť poradie dielov",
        "prep": [
            "Pred odopnutím odfoťte koniec koľajnice, zátku, počet jazdcov a spôsob pripevnenia textilu. Záclonu podopierajte, aby celá hmotnosť nevisela na poslednom háčiku. Háčiky a krúžky vložte do označenej nádoby. Ak plánujete textil prať, najprv si prečítajte štítok a samostatne vyriešte kovové alebo plastové doplnky.",
            "Pripravte vysávač s mäkkým nadstavcom, jemnú kefku, dve biele handričky a nádobu na drobné diely. Pod okno dajte ochrannú textíliu. Schodíky úplne rozložte a vždy ich posuňte bližšie k úseku, namiesto naťahovania tela do strany. Uvoľnené kotvenie najprv opravte; čistenie na kývajúcej sa konzole nie je bezpečné.",
        ],
        "steps": [
            "Zatvorte okno, odstráňte predmety z parapetu a stabilné schodíky postavte na rovnú podlahu.",
            "Odfoťte koncovky, háčiky, jazdce a spoje, potom textil zložte postupne bez ťahania za koľajnicu.",
            "Mäkkým nadstavcom povysávajte hornú hranu garniže, konzoly, tyč aj dostupnú drážku koľajnice.",
            "Niť, vlasy a chumáče z drážky vyberte pinzetou alebo jemnou kefkou bez poškriabania profilu.",
            "Na skrytom mieste overte povrch a kompatibilnú časť utrite dobre vyžmýkanou handričkou od jedného konca k druhému.",
            "Mastný film riešte malým množstvom jedného vhodného produktu naneseného na handričku, nie priamym rozstrekom na stenu a textil.",
            "Drážku, spoje a koncovky utrite čistou vlhkou handričkou, ak to materiál povoľuje, a následne ich úplne vysušte.",
            "Jazdce vložte v pôvodnom poradí, skontrolujte plynulý pohyb bez záclony a poškodené kusy vymeňte za kompatibilný typ.",
            "Záclonu alebo záves zaveste až na suchý a stabilný systém, potom pohyb vyskúšajte pomaly bez prudkého trhnutia.",
        ],
        "diagnosis_heading": "Prečo sa záclona zasekáva a čo skontrolovať skôr než použijete mazivo",
        "table2_headers": ["Problém", "Pravdepodobná príčina", "Riešenie"],
        "table2_rows": [
            ("Jeden jazdec stojí", "poškodené koliesko, ohnutie alebo niť", "jazdec vybrať podľa návodu a nahradiť kompatibilným kusom"),
            ("Zadrháva sa vždy v rovnakom mieste", "nerovný spoj profilov alebo nečistota v drážke", "vyčistiť spoj a skontrolovať zarovnanie bez násilia"),
            ("Celá tyč sa kýve", "uvoľnená konzola alebo kotva", "nezavesovať textil a najprv opraviť kotvenie"),
            ("Na záclone sú sivé pásy", "prach alebo mastnota z tyče a krúžkov", "vyčistiť systém pred praním a zavesením textilu"),
            ("Po namazaní sa prach vracia", "nevhodný olejový film v otvorenom profile", "odstrániť prebytok podľa materiálu a riadiť sa návodom výrobcu"),
        ],
        "sections": [
            ("Ako vyčistiť kovovú garnižu", [
                "Po vysatí prachu utrite lakovanú alebo pochrómovanú tyč mierne vlhkou mäkkou handričkou. Prípravok testujte na strane pri stene. Nepoužívajte drôtenku ani hrubú pastu, ktorá môže vytvoriť jemné škrabance viditeľné pri bočnom svetle. Konzoly čistite bez opierania váhy tela o tyč.",
                "Pri bodkách korózie nerozotierajte hrdzu po celej ploche a neprekrývajte ju mokrým textilom. Skontrolujte, či zdrojom nie je kondenzácia pri okne alebo poškodená povrchová úprava. Rozsiahle odlupovanie či oslabený kov patrí oprave alebo výmene, nie agresívnemu lešteniu vo výške.",
            ]),
            ("Ako vyčistiť plastovú alebo hliníkovú koľajnicu", [
                "Najprv povysávajte otvorenú drážku a jemnou kefkou uvoľnite prach pri spojoch. Drobné vlákna vyberte pinzetou, ale neškrabte profil nožom. Plast môže byť vekom krehký a hliník môže zmatnieť po abrazívnom prostriedku. Vlhkú handričku veďte po hrane, nie tak, aby roztok stekal do steny.",
                "Odnímateľnú koncovku uvoľnite iba určeným smerom. Ak kladie veľký odpor, najprv hľadajte skrutku alebo poistku. Násilné páčenie môže zlomiť diel, ktorý drží jazdce v drážke. Pred opätovným zložením profil vysušte a skontrolujte, že vnútri neostal klzký film.",
            ]),
            ("Ako vyčistiť drážku koľajnice", [
                "Drážka potrebuje úzku pomôcku, nie veľa vody. Vysávač priložte k otvoru a mäkkou kefkou posúvajte prach k hubici. Na lepkavé miesto použite roh bielej handričky okolo plastovej špachtličky bez ostrej hrany. Tak viete kontrolovať tlak aj množstvo vlhkosti.",
                "Po čistení prejdite drážku suchou handričkou a nechajte otvorenú. Jazdce skúšajte bez záclony po jednom úseku. Ak sa zadrhnú v spoji, skontrolujte zarovnanie profilov. Nepokúšajte sa problém prekonať prudkým trhnutím, ktoré môže vytrhnúť koncovku alebo konzolu.",
            ]),
            ("Čo robiť, keď sa jazdce záclon zasekávajú", [
                "Najprv nájdite presné miesto odporu. Ak sa zasekáva iba jeden jazdec, problém je pravdepodobne v ňom. Ak všetky zastavia v rovnakom bode, skontrolujte spoj, skrutku zasahujúcu do drážky alebo deformáciu profilu. Fotografia a pomalý test bez textilu pomôžu rozlíšiť poruchu od preťaženia.",
                "Mazivo používajte iba vtedy, keď ho výrobca odporúča, v určenej forme a malom množstve. Kuchynský olej alebo univerzálny mastný sprej môže zachytiť prach a znečistiť svetlú záclonu. Niekedy je správnym riešením výmena jazdca alebo oprava spoja, nie hladší povrch za každú cenu.",
            ]),
            ("Garniža v kuchyni: prach spojený s mastnotou", [
                "V otvorenej kuchyni sa na hornej hrane vytvorí lepkavý povlak. Suché vysatie odstráni iba voľnú časť. Na skrytom mieste preto otestujte jemný univerzálny čistič, naneste ho na handričku a tyč čistite po krátkych úsekoch. Roztok nesmie kvapkať na čerstvo vypranú záclonu, stenu ani drevený parapet.",
                "Súčasne skontrolujte hornú hranu okna a radiátor, ktoré môžu byť ďalším zdrojom prachu. Pomôžu návody <a href=\"/n/ako-vycistit-parapety-a-okenne-ramy-prach-pel-cierne-mapy-a-skvrny\">ako vyčistiť parapety a okenné rámy</a> a <a href=\"/n/ako-vycistit-radiator-od-prachu-rebra-zadna-strana-mastnota-a-bezpecna-udrzba\">ako vyčistiť radiátor</a>.",
            ]),
            ("Kedy prať záclony a v akom poradí upratovať", [
                "Najpraktickejšie poradie je zložiť textil, vyčistiť garnižu, koľajnicu, okno a okolie, až potom zavesiť čistú záclonu. Ak záclonu vyperiete ako prvú a vrátite ju na zaprášenú tyč, sivý pás sa môže objaviť okamžite. Háčiky skontrolujte a ostré či zhrdzavené kusy vymeňte.",
                "Pri praní sa riaďte štítkom a hmotnosťou mokrej textílie. Súvisiace symboly vysvetľuje <a href=\"/n/symboly-prania-kompletny-sprievodca-praciim-stitkom\">sprievodca symbolmi prania</a>. Ak záclonu žehlíte, pomôže aj návod <a href=\"/n/ako-spravne-vyzehlit-zaclonu-kompletny-sprievodca\">ako správne vyžehliť záclonu</a>. Na garnižu ju zaveste až po úplnom vysušení čistených dielov.",
            ]),
        ],
        "remember": [
            "Pred rozoberaním odfoťte poradie jazdcov, háčikov a koncoviek.",
            "Zasekávanie sa najprv diagnostikuje; olej nie je univerzálna oprava koľajnice.",
            "Čistá záclona patrí na suchú, stabilnú a odmastenú garnižu.",
        ],
        "mistakes": [
            "Ťahanie celej záclony cez posledný háčik bez podopretia hmotnosti.",
            "Opieranie sa o tyč alebo koľajnicu pri práci zo schodíkov.",
            "Mokré rozotretie prachu v drážke bez predchádzajúceho vysatia.",
            "Násilné páčenie koncovej zátky bez kontroly skrutky alebo poistky.",
            "Použitie kuchynského oleja, ktorý zachytí prach a môže zašpiniť textil.",
            "Zavesenie vypranej záclony na ešte vlhkú alebo uvoľnenú konštrukciu.",
        ],
        "caution": [
            "Ak sa konzola hýbe, kotva vychádza zo steny, profil je prehnutý alebo okolo skrutiek praská omietka, textil nezavesujte. Najprv zistite typ podkladu a nechajte kotvenie spoľahlivo opraviť. Ťažký záves môže poškodenie náhle zväčšiť.",
            "Pri motorickej koľajnici, elektrickom pohone alebo inteligentnom závesnom systéme zariadenie vypnite a riaďte sa servisným návodom. Motor, napájací zdroj a elektroniku nečistite mokrou handričkou ani nerozoberajte ako bežný plastový profil.",
        ],
        "expert_heading": "Prach na vysokej hrane, trenie jazdcov a bezpečná práca",
        "expert": [
            "EPA uvádza, že vnútorný prach sa ľahko znovu dostane do vzduchu pri upratovaní a odporúča časté zachytávanie vlhkou handričkou na vhodných povrchoch. Pri garniži má poradie význam: najprv mäkké odsatie, potom kontrolované vlhké utretie. Tak sa suchý nános nezmení na sivú pastu v úzkej drážke.",
            "Pohyb jazdca závisí od čistoty, tvaru a zarovnania. Mastný film síce krátkodobo zníži trenie, ale môže viazať častice a poškodiť textil. Technicky čisté riešenie je odstrániť cudzie predmety, vymeniť poškodený jazdec, zarovnať spoj a použiť iba výrobcom odporúčaný prostriedok.",
            "OSHA pri práci na prenosnom rebríku zdôrazňuje kontrolu zariadenia, čisté stupne a stabilnú polohu tela. Garniža nie je oporný bod. Schodíky premiestňujte po malých úsekoch a pri výstupe udržujte bezpečný kontakt; čistenie jednou rukou nemá byť vykúpené stratou rovnováhy.",
        ],
        "source_intro": "EPA vysvetľuje, prečo je vhodné prach najprv zachytiť a až potom vlhko utierať kompatibilný povrch. OSHA dopĺňa základné pravidlá práce na prenosných schodíkoch.",
        "sources": [
            ("US EPA: Sources of Indoor Particulate Matter", EPA_PM),
            ("OSHA: Portable Ladder Safety QuickCard", OSHA_LADDER),
            ("US EPA: Ventilation When Cleaning or Sanitizing Indoors", EPA_CLEANING_VENTILATION),
        ],
        "product_heading": "Jemné odmastenie kompatibilnej garniže po odstránení prachu",
        "product_name": "Univerzálny voňavý čistič Vevo Pure Harmony 500 ml",
        "product_url": UNIVERSAL_PRODUCT,
        "product_intro": "Na lakovanú kovovú alebo plastovú časť môže byť vhodný univerzálny čistič, ak povrch prejde skúškou a produkt nanesiete kontrolovane na handričku mimo záclony a steny.",
        "product_use": "na bežný mastný film a odtlačky na kompatibilnej umývateľnej tyči, koncovke alebo vonkajšej ploche profilu.",
        "product_limit": "nepoužívajte ho ako mazivo v koľajnici, na nelakované drevo, elektrický pohon ani na materiál, ktorý výrobca vylučuje.",
        "category_name": "Univerzálne čističe do domácnosti",
        "category_url": UNIVERSAL_CATEGORY,
        "category_intro": "Pri garniži vyberajte produkt podľa povrchovej úpravy a rizika kontaktu so záclonou. Najprv odstráňte prach a mechanickú príčinu zasekávania.",
        "category_text": "Pozrite si univerzálne čističe pre kompatibilné umývateľné povrchy. Každý produkt najprv otestujte na skrytom mieste a nepoužívajte ho namiesto maziva alebo opravy kotvenia.",
        "related": [
            ("Ako správne vyžehliť záclonu", "/n/ako-spravne-vyzehlit-zaclonu-kompletny-sprievodca"),
            ("Symboly prania na štítkoch", "/n/symboly-prania-kompletny-sprievodca-praciim-stitkom"),
            ("Ako vyčistiť parapety a okenné rámy", "/n/ako-vycistit-parapety-a-okenne-ramy-prach-pel-cierne-mapy-a-skvrny"),
        ],
        "faq_title": "garniže a koľajnice záclon",
        "faq": [
            ("Čím vyčistiť koľajnicu záclony?", "Najprv vysávačom a mäkkou kefkou odstráňte prach a nite. Potom použite mierne vlhkú handričku podľa materiálu a drážku vysušte. Pri mastnote produkt testujte na skrytom mieste."),
            ("Môžem koľajnicu namazať olejom?", "Nie naslepo. Bežný olej môže zachytiť prach a zafarbiť záclonu. Najprv skontrolujte jazdce, spoj a nečistoty a použite iba prostriedok odporúčaný výrobcom systému."),
            ("Prečo sa záclona zasekáva na jednom mieste?", "Pravdepodobná je nečistota, nerovný spoj profilov, zasahujúca skrutka alebo poškodený jazdec. Systém skúšajte bez textilu pomaly a problém neprekonávajte prudkým ťahaním."),
            ("Treba pred praním záclon vyčistiť garnižu?", "Áno, je to praktické poradie. Čistá záclona sa na zaprášenej alebo mastnej tyči môže okamžite zašpiniť. Najprv vyčistite a vysušte systém, potom zaveste vypraný textil."),
            ("Ako vyčistiť drevenú garnižu?", "Prach odstráňte mäkkou suchou handričkou alebo vysávačom. Vlhký postup a ošetrujúci produkt voľte podľa konkrétnej povrchovej úpravy; nelakované alebo voskované drevo nečistite univerzálnym roztokom bez overenia."),
        ],
    },
]


def article_hrefs(markup):
    return re.findall(r'href="([^"]+)"', markup)


def preflight_links(articles):
    rows = []
    seen = set()
    headers = {"User-Agent": "Codex VEVO batch 38 link preflight"}
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

        for href in article_hrefs(article["long"]):
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
    candidate_titles = [line.strip() for line in CANDIDATES.read_text(encoding="utf-8").splitlines() if line.strip()]
    article_by_title = {article["title"]: article for article in ARTICLES}
    if len(article_by_title) != len(ARTICLES) or set(candidate_titles) != set(article_by_title):
        raise SystemExit("Batch 38 article titles do not exactly match the duplicate-guard candidate file")
    ordered_articles = [article_by_title[title] for title in candidate_titles]

    rendered = []
    for index, article in enumerate(ordered_articles):
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
                "time_posted": f"12:{index * 10:02d}",
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
        raise SystemExit("Batch 38 link preflight failed")


EXTRA_ARTICLES = [
    {
        "title": "Ako vyčistiť hubky a kefy na upratovanie: mastnota, opotrebovanie a včasná výmena",
        "link": "ako-vycistit-hubky-a-kefy-na-upratovanie-mastnota-opotrebovanie-a-vcasna-vymena",
        "meta": "Ako čistiť hubky a kefy na upratovanie, správne ich oddeliť podľa zón, vysušiť a spoznať mastnotu, poškodenie či čas na bezpečnú výmenu.",
        "short": "Hubku alebo kefu po použití zbavte pevných zvyškov, umyte detergentom primeraným materiálu, dôkladne opláchnite a nechajte voľne vyschnúť. Pomôcky z kuchyne, kúpeľne a WC držte oddelene. Ak sú popraskané, zapáchajú, nepustia mastný film alebo sa štetiny deformovali, vymeňte ich.",
        "answer": "Najprv odstráňte omrvinky, vlasy a hrubú špinu, potom pomôcku premyte teplou vodou a vhodným detergentom. Hubku stlačte bez trhania, kefu prečistite medzi štetinami a obe opláchnite do straty peny. Sušte ich tak, aby neležali vo vlastnej vode, a nikdy nepremiestňujte rovnakú hubku medzi kuchyňou, kúpeľňou a toaletou.",
        "quick": [
            "<strong>Zóny:</strong> kuchynská hubka, kúpeľňová kefa a WC pomôcka nemajú byť zameniteľné.",
            "<strong>Pevné zvyšky:</strong> jedlo, vlasy a chlpy vyberte pred samotným umývaním.",
            "<strong>Oplach:</strong> pena ani parfum nie sú dôkaz, že vo vnútri nezostal mastný film.",
            "<strong>Sušenie:</strong> stojan musí umožniť odtok vody a prístup vzduchu z viacerých strán.",
            "<strong>Výmena:</strong> trvalý pach, drobenie, ostré poškodenie a deformované štetiny sú praktické hranice životnosti.",
        ],
        "overview_heading": "Prečo sa hubka alebo kefa môže stať zdrojom rozmazanej špiny",
        "overview": [
            "Hubka má veľký vnútorný povrch a zadržiava vodu aj drobné organické zvyšky. Pri umývaní riadu alebo linky sa do pórov dostáva mastnota, omrvinky a zvyšok detergentu. Krátke opláchnutie vrchnej strany preto nemusí odstrániť to, čo zostalo v strede. Keď hubku opäť stlačíte na čistom povrchu, časť starého obsahu sa môže uvoľniť späť.",
            "Kefa funguje inak. Voda z nej odteká rýchlejšie, ale nečistota sa zachytáva pri koreni štetín, v spoji s hlavicou a okolo rukoväti. Dlhé vlasy sa môžu omotať do chumáča, mastnota vytvorí klzký film a príliš silné drhnutie štetiny ohne. Kefa potom čistí nerovnomerne a na citlivom povrchu môže zanechať stopy.",
            "Najdôležitejšie je zónovanie. Pomôcka použitá na surové potraviny, mastný sporák, umývadlo, podlahu alebo WC prichádza do kontaktu s odlišným typom znečistenia. Farebné rozlíšenie, samostatné stojany alebo jasné miesto skladovania znižujú riziko, že niekto použije WC kefu na sprchový odtok alebo kuchynskú hubku na podlahu.",
            "Čistenie pomôcky neznamená automaticky sterilitu. V bežnej domácnosti je cieľom odstrániť zvyšky, znížiť množstvo nečistôt, pomôcku vysušiť a včas ju vyradiť. Ak potrebujete dezinfekčný proces, musí byť vhodný pre materiál a vykonaný podľa etikety konkrétneho produktu; náhodné miešanie chemikálií alebo improvizované zohrievanie môže byť nebezpečné.",
        ],
        "table1_headers": ["Pomôcka", "Typické znečistenie", "Čo kontrolovať"],
        "table1_rows": [
            ("Kuchynská hubka", "mastnota, jedlo, zvyšky detergentu", "pach po navlhčení, drobenie a mastný pocit"),
            ("Drôtenka alebo abrazívna hubka", "pripečené zvyšky a kovové čiastočky", "hrdzu, ostré hrany a vhodnosť pre povrch"),
            ("Kefa na riad", "jedlo pri koreni štetín", "uvoľnené štetiny a čistotu spoja s rukoväťou"),
            ("Kúpeľňová kefka", "mydlový film, vlasy a minerálny povlak", "zónové označenie a úplné vysušenie"),
            ("Kefa na škáry alebo podlahu", "piesok, chlpy a hrubá špina", "deformáciu štetín a praskliny hlavice"),
        ],
        "prep_heading": "Ako pomôcky roztriediť ešte pred čistením",
        "prep": [
            "Rozložte pomôcky podľa použitia: riad a pracovná doska, kúpeľňa, podlaha, toaleta a špeciálne úlohy. Silne kontaminovanú alebo poškodenú pomôcku neumývajte spolu s kuchynským riadom. Ak neviete, odkiaľ hubka pochádza, nepokračujte s ňou na povrchu, ktorý prichádza do styku s jedlom.",
            "Pripravte si rukavice, vhodný detergent, čistú vodu a stojan, z ktorého voda skutočne odteká. Skontrolujte výrobné pokyny, najmä pri prírodných vláknach, drevenej rukoväti, elektrickej kefke alebo pomôcke s lepenými časťami. Voda nemá vtekať do batériového priestoru a drevo sa nemá bezdôvodne dlho namáčať.",
        ],
        "steps": [
            "Pomôcku označte podľa zóny a oddeľte kuchynské, kúpeľňové, podlahové a WC vybavenie.",
            "Mechanicky odstráňte omrvinky, vlasy, chlpy, nite a hrubé usadeniny; použite na to papier alebo samostatnú pomocnú kefku.",
            "Hubku opakovane premyte v teplej vode s primeraným množstvom detergentu a stláčajte ju tak, aby sa voda vymenila aj v strede.",
            "Kefu čistite v smere štetín, prejdite priestor pri ich koreni aj spoj hlavice s rukoväťou a neohýbajte ich násilne do strán.",
            "Pomôcku oplachujte čistou vodou, kým z nej neodchádza viditeľná pena, mastnota ani pevné zvyšky.",
            "Hubku vyžmýkajte bez krútenia, ktoré trhá penu; kefu pretraste smerom do drezu a utrite rukoväť.",
            "Položte alebo zaveste pomôcku tak, aby sa nedotýkala špinavého dna nádoby a mohla vyschnúť zo všetkých strán.",
            "Po vysušení skontrolujte pach, tvar, povrch a pevnosť. Pomôcku, ktorá ostala klzká, zapácha alebo sa rozpadáva, nepoužívajte ďalej.",
        ],
        "diagnosis_heading": "Čo znamená pach, mastný film alebo deformácia",
        "table2_headers": ["Prejav", "Možné vysvetlenie", "Rozumné rozhodnutie"],
        "table2_rows": [
            ("Hubka zapácha po navlhčení", "zvyšky a dlhé vlhké skladovanie", "vyčistiť a úplne vysušiť; pri návrate pachu vymeniť"),
            ("Hubka je stále mastná", "tuk ostal v póroch alebo je materiál opotrebovaný", "zopakovať umytie oddelene; pri neúspechu vyradiť"),
            ("Kefa škrabe nerovnomerne", "ohnuté, stvrdnuté alebo uvoľnené štetiny", "nepoužívať na citlivý povrch a vymeniť hlavicu"),
            ("Pri koreni štetín je čierny povlak", "dlhodobá vlhkosť alebo usadenina v spoji", "neprekryť vôňou; vyčistiť, vysušiť alebo vyradiť"),
            ("Rukoväť je prasknutá", "mechanické opotrebovanie a dutina pre nečistoty", "výmena je bezpečnejšia než provizórne lepenie"),
        ],
        "sections": [
            ("Ako vyčistiť kuchynskú hubku od mastnoty", [
                "Po riade alebo sporáku najprv odstráňte kúsky jedla. Hubku premyte detergentom, ktorý je určený na dané použitie, a niekoľkokrát ju stlačte pod čistou vodou. Nestačí prejsť iba zelenú abrazívnu vrstvu; voda sa musí vymeniť aj v mäkkom jadre. Ak po oplachu ostáva mastný pocit alebo pach, nepoužívajte ju na čisté poháre a pracovnú dosku.",
                "Na jednu hubku neklaďte všetky úlohy. Pomôcka na surové mäso alebo veľmi mastný plech nemá bez dôkladného vyčistenia pokračovať na stole. Pri čistení pracovnej plochy najprv odstráňte zdroj mastnoty; praktický postup ponúka článok <a href=\"/n/ako-vycistit-kuchynsku-linku-od-mastnoty-prachu-a-smuh-bez-poskodenia-povrchu\">ako vyčistiť kuchynskú linku</a>.",
            ]),
            ("Ako vyčistiť kefu na riad a priestor medzi štetinami", [
                "Kefu držte štetinami nadol a odstráňte viditeľné zvyšky. Jemným pohybom druhej vyhradenej kefky alebo prstami v rukavici prejdite priestor pri koreni štetín. Dôležitý je aj krčok a rukoväť, ktorých sa dotýkate mokrými rukami. Po oplachu kefu pretraste a uložte hlavicou tak, aby voda mohla odtiecť.",
                "Drevenú rukoväť nenechávajte ponorenú, ak to výrobca nepovoľuje. Pri vymeniteľnej hlavici skontrolujte závit a miesto spojenia, ale mechanizmus nenásilne nerozoberajte. Uvoľnené štetiny môžu ostať na riade; deformovaná kefa zase zvyšuje tlak na niekoľko bodov a môže poškriabať citlivejší povrch.",
            ]),
            ("Hubka do kúpeľne nie je hubka na kuchynskú linku", [
                "V kúpeľni sa pomôcka stretáva s mydlovým filmom, vlasmi, kozmetikou a minerálnymi usadeninami. Farebne ju odlíšte od kuchynskej a neskladujte obe v jednej mokrej nádobe. Po čistení umývadla alebo sprchy ju prepláchnite, vyžmýkajte a vysušte mimo striekajúcej vody.",
                "Ak hubka prešla po podlahe, odtoku alebo toalete, nevracajte ju na umývadlo bez jasne stanoveného hygienického postupu. Jednoduchšie a bezpečnejšie je mať lacné pomôcky pevne priradené jednotlivým zónam. Zónovanie znižuje počet rozhodnutí počas upratovania a pomáha celej domácnosti používať správnu pomôcku.",
            ]),
            ("Prečo samotné opláchnutie nemusí odstrániť všetko", [
                "Voda odstráni časť voľných zvyškov, no mastnota sa drží na materiáli a pevné častice môžu ostať v póroch alebo pri štetinách. Potrebný je primeraný detergent, mechanický pohyb a následný oplach. Priveľa detergentu však vytvára ďalší problém: ak ho nevypláchnete, pomôcka ostane klzká a prenáša film na ďalší povrch.",
                "Výskum použitých kuchynských hubiek ukázal husté a rozmanité bakteriálne osídlenie. Autori zároveň upozornili, že budúce kontrolované štúdie majú lepšie vyhodnotiť skutočnú patogenitu a účinok domácich sanitačných postupov. Praktický záver preto nie je panika ani sľub sterility, ale odstraňovanie zvyškov, sušenie, oddelenie zón a včasná výmena.",
            ]),
            ("Ako sušiť hubky a kefy bez stojatej vody", [
                "Stojan s plným dnom môže vyzerať upratane, ale zachytáva vodu presne pod pomôckou. Lepší je perforovaný držiak alebo zavesenie s voľným odtokom. Hubka nemá byť stlačená medzi fľašami a kefa nemá ležať štetinami v mláke. Aj samotný stojan pravidelne umyte, inak sa z neho stane zdroj klzkého povlaku.",
                "Prirodzené sušenie urýchlite vyžmýkaním a prúdením vzduchu, nie ukladaním mokrej hubky na horúci spotrebič. Neimprovizujte s mikrovlnnou rúrou pri hubkách s kovovou alebo abrazívnou zložkou; materiál a riziko požiaru nie sú vždy zrejmé. Riaďte sa iba jasným pokynom výrobcu, inak je bezpečnejšie pomôcku vyčistiť, vysušiť alebo vymeniť.",
            ]),
            ("Kedy hubku alebo kefu vymeniť", [
                "Pevný kalendár nefunguje pre každú domácnosť. Sledujte materiál: pena sa drobí, abrazívna vrstva sa oddeľuje, kefa púšťa štetiny, rukoväť praská alebo pomôcka po vyčistení stále zapácha. Takéto prejavy znižujú výsledok a vytvárajú miesta, ktoré sa čistia čoraz ťažšie.",
                "Pomôcku vymeňte skôr po kontakte s nebezpečnou látkou, ktorú z nej neviete bezpečne odstrániť. Silne znečistená lacná hubka nemusí stáť za chemický experiment. Včasná výmena však nenahrádza dobrú rutinu: aj nová hubka rýchlo zatuchne, ak ostane plná jedla a vody v uzavretom dreze.",
            ]),
        ],
        "remember": [
            "Zónovanie pomôcok je rovnako dôležité ako ich samotné umytie.",
            "Čistá pomôcka je bez pevných zvyškov, filmu a peny a môže úplne vyschnúť.",
            "Trvalý pach alebo poškodenie je signál na výmenu, nie na pridanie silnejšej vône.",
        ],
        "mistakes": [
            "Používanie jednej hubky na riad, podlahu aj kúpeľňu.",
            "Uloženie mokrej pomôcky do stojana bez odtoku vody.",
            "Ponechanie vlasov a jedla pri koreni štetín.",
            "Miešanie čističov alebo improvizované zohrievanie neznámeho materiálu.",
            "Pokračovanie s drobiacou sa hubkou alebo kefou s uvoľnenými štetinami.",
            "Zamieňanie parfumovaného pachu za dôkaz čistoty alebo dezinfekcie.",
        ],
        "caution": [
            "Pomôcku, ktorá prišla do kontaktu s agresívnou chemikáliou, telesnou tekutinou alebo neznámou látkou, nepoužívajte na potravinové povrchy. Ak nepoznáte bezpečný dekontaminačný postup pre daný materiál, vyradenie je rozumnejšie než presun rizika do ďalšej zóny.",
            "Pri elektrickej kefke, dávkovači s motorom alebo batériovej pomôcke neumývajte nabíjací port ani telo zariadenia ponorením, ak to výrobca výslovne nepovoľuje. Poškodený kábel, batéria alebo tesnenie patrí servisu alebo výmene.",
        ],
        "expert_heading": "Čo vieme o vlhkej hubke a prečo netreba preháňať závery",
        "expert": [
            "Štúdia v časopise Scientific Reports analyzovala mikrobiálne spoločenstvá v použitých domácich kuchynských hubkách. Potvrdila, že pórovitý, vlhký materiál môže hostiť vysoké množstvo mikroorganizmov. Zároveň nejde o dôkaz, že každá hubka spôsobí ochorenie alebo že jeden domáci trik zaručí sterilitu.",
            "Pre domácu prax je dôležitá kombinácia viacerých bariér: odstrániť potravinové zvyšky, použiť detergent, dôkladne opláchnuť, vytlačiť vodu, sušiť s prístupom vzduchu a oddeliť pomôcky podľa zón. Ak materiál zostáva poškodený alebo zapáchajúci, výmena je jednoduchá kontrolná hranica.",
            "EPA rozlišuje čistenie, sanitáciu a dezinfekciu. Bežný detergent a drhnutie odstraňujú nečistoty; dezinfekčný účinok možno tvrdiť len pri určenom produkte a dodržanom čase pôsobenia. Preto v článku nesľubujeme, že krátke opláchnutie alebo vôňa urobí z používanej hubky sterilnú pomôcku.",
        ],
        "source_intro": "Odborná štúdia vysvetľuje mikrobiálne osídlenie použitých kuchynských hubiek; materiály EPA pomáhajú presne odlíšiť odstránenie špiny od dezinfekcie.",
        "sources": [
            ("Scientific Reports: Microbiome analysis of used kitchen sponges", SPONGE_STUDY),
            ("US EPA: Difference Between Cleaning, Sanitizing and Disinfecting", EPA_CLEANING),
            ("US EPA: Ventilation When Cleaning or Sanitizing Indoors", EPA_CLEANING_VENTILATION),
        ],
        "product_heading": "Čistá pomôcka patrí na správne zvolený umývateľný povrch",
        "product_name": "Univerzálny voňavý čistič Vevo Pure Harmony 500 ml",
        "product_url": UNIVERSAL_PRODUCT,
        "product_intro": "Univerzálny čistič má zmysel na kompatibilnom umývateľnom povrchu, keď použijete čistú, správne označenú pomôcku. Najprv odstráňte hrubú špinu a vždy urobte skúšku na skrytom mieste.",
        "product_use": "na bežné čistenie vhodných tvrdých povrchov podľa etikety, s čistou handričkou, hubkou alebo kefou určenou pre danú zónu.",
        "product_limit": "nie je to automatická dezinfekcia hubky ani riešenie pre citlivý, nelakovaný, pórovitý či výrobcom vylúčený materiál.",
        "category_name": "Univerzálne čističe do domácnosti",
        "category_url": UNIVERSAL_CATEGORY,
        "category_intro": "Produkt vyberajte podľa povrchu, typu nečistoty a pokynov výrobcu. Výsledok závisí aj od čistej pomôcky, primeranej dávky a oplachu tam, kde je potrebný.",
        "category_text": "V kategórii nájdete riešenia na bežné umývateľné povrchy. Pred nákupom porovnajte spôsob použitia s materiálom a tým, či povrch prichádza do styku s jedlom.",
        "related": [
            ("Ako vyčistiť kuchynskú linku", "/n/ako-vycistit-kuchynsku-linku-od-mastnoty-prachu-a-smuh-bez-poskodenia-povrchu"),
            ("Ako vyčistiť drez a batériu", "/n/ako-vycistit-drez-a-bateriu-vodny-kamen-mastnota-zapach-a-bezpecne-cistenie"),
            ("Ako prať mikrovláknové utierky", "/n/ako-prat-mikrovlaknove-utierky-aby-nezapachali-a-dobre-cistili"),
        ],
        "faq_title": "hubky a kefy na upratovanie",
        "faq": [
            ("Ako často meniť kuchynskú hubku?", "Neriadte sa iba dátumom. Vymeňte ju pri trvalom pachu, drobení, oddelenej abrazívnej vrstve, mastnom filme po umytí alebo po kontakte s látkou, ktorú z nej neviete bezpečne odstrániť."),
            ("Môžem rovnakú hubku používať v kuchyni aj kúpeľni?", "Nie je to dobrá prax. Pomôcky rozdeľte podľa zón a farebne alebo miestom ich označte, aby sa nečistota z podlahy, odtoku či WC neprenášala k riadu a potravinám."),
            ("Je opláchnutá hubka dezinfikovaná?", "Nie automaticky. Oplach a detergent sú čistenie. Dezinfekcia vyžaduje vhodný výrobok, správnu koncentráciu a čas pôsobenia podľa etikety."),
            ("Prečo kefa zapácha, hoci rýchlo schne?", "Zvyšky môžu ostať pri koreni štetín, v spoji hlavice alebo v praskline rukoväti. Tieto miesta vyčistite a skontrolujte. Ak pach pretrváva alebo je spoj poškodený, kefu vymeňte."),
            ("Ako skladovať hubku pri dreze?", "Po oplachu ju vyžmýkajte a položte na perforovaný držiak s odtokom a prístupom vzduchu. Nenechávajte ju pod fľašami, v uzavretej miske ani vo vode na dne drezu."),
        ],
    },
    {
        "title": "Ako vyčistiť vetracie mriežky v domácnosti: prach, mastnota a prúdenie vzduchu",
        "link": "ako-vycistit-vetracie-mriezky-v-domacnosti-prach-mastnota-a-prudenie-vzduchu",
        "meta": "Bezpečný návod na čistenie vetracích mriežok v kúpeľni, kuchyni a na dverách: prach, mastnota, odnímanie, sušenie aj hranice domáceho zásahu.",
        "short": "Vetraciu mriežku najprv povysávajte mäkkým nadstavcom. Odnímajte ju iba vtedy, ak to povoľuje návod a nejde o zásah do ventilátora, elektriny alebo vzduchotechniky. Umývateľnú mriežku čistite malým množstvom vhodného roztoku, opláchnite, úplne vysušte a až potom vráťte na miesto.",
        "answer": "Vypnite ventilátor alebo zariadenie podľa pokynov výrobcu a odstráňte voľný prach vysávačom s mäkkou kefou. Pevnú mriežku utrite dobre vyžmýkanou handričkou; odnímateľnú časť môžete umyť samostatne, ak je to výslovne povolené. Nestriekajte čistič do motora, kábla ani potrubia a nezakrývajte vetrací otvor.",
        "quick": [
            "<strong>Typ otvoru:</strong> pasívna mriežka, kryt ventilátora, digestor a systémová výustka nie sú to isté.",
            "<strong>Napájanie:</strong> pri ventilátore má prednosť bezpečné vypnutie podľa manuálu, nie iba vypínač svetla.",
            "<strong>Prach:</strong> najprv ho zachyťte nasucho, aby sa po navlhčení nezmenil na sivú pastu.",
            "<strong>Mastnota:</strong> kuchynská mriežka potrebuje jemné odmastenie po skúške materiálu.",
            "<strong>Hranica:</strong> domáci návod sa týka dostupného krytu, nie rozoberania motora alebo čistenia potrubia.",
        ],
        "overview_heading": "Čo sa na vetracej mriežke usádza a čo to ešte neznamená",
        "overview": [
            "Vzduch nesie jemný prach, textilné vlákna, peľ, aerosóly z varenia a vlhkosť. Keď prúdi cez úzke lamely, časť častíc sa zachytí na hranách. V kúpeľni sa môžu spojiť s vlhkosťou, v kuchyni s mastným filmom a na dverovej mriežke s prachom pri podlahe. Viditeľná sivá vrstva preto nie je prekvapenie ani automatický dôkaz poruchy potrubia.",
            "Mriežka však môže ovplyvniť prúdenie, ak sú otvory mechanicky zanesené. Cieľom údržby je uvoľniť dostupnú plochu bez zatlačenia špiny hlbšie a bez poškodenia zariadenia. Prudké fúkanie stlačeným vzduchom rozptýli prach do miestnosti a mokré striekanie môže zaviesť tekutinu tam, kam nepatrí.",
            "Rozhodujúci je typ systému. Jednoduchá pasívna mriežka vo dverách nemá motor. Kúpeľňový ventilátor môže mať elektrické časti a spätnú klapku. Výustka centrálneho vetrania je súčasťou vyregulovaného systému a kuchynský digestor má samostatné filtre podľa manuálu. Všeobecný návod preto končí na bezpečne dostupnom kryte.",
            "EPA upozorňuje, že ľahká vrstva prachu v potrubí sama osebe neznamená potrebu plošného čistenia vzduchovodov. Domáce čistenie mriežky nemožno zamieňať za servis vzduchotechniky. Ak je prítomná voda, podozrenie na pleseň pri nasávaní, zápach spáleniny alebo nezvyčajný hluk, treba najprv riešiť technickú príčinu.",
        ],
        "table1_headers": ["Miesto", "Typické znečistenie", "Bezpečný rozsah"],
        "table1_rows": [
            ("Kúpeľňová mriežka", "prach spojený s vlhkosťou", "kryt a dostupné lamely po vypnutí zariadenia"),
            ("Kuchynská mriežka", "mastný aerosól a prach", "jemné odmastenie odnímateľného alebo pevného krytu"),
            ("Dverová prestupová mriežka", "prach, chlpy a textilné vlákna", "vysatie z oboch strán a vlhké utretie"),
            ("Výustka rekuperácie", "jemný prach na hrane", "iba údržba povolená výrobcom bez zmeny nastavenia"),
            ("Kryt ventilátora", "prach na lamelách", "bez zásahu do motora, kabeláže a lopatiek bez manuálu"),
        ],
        "prep_heading": "Ako zistiť, či mriežku môžete odňať",
        "prep": [
            "Vyhľadajte značku zariadenia a návod. Skontrolujte, či je kryt nacvakávací, skrutkovaný alebo pevnou súčasťou systému. Pred odnímaním si odfoťte polohu a orientáciu. Ak mriežka zároveň nastavuje smer alebo prietok, neotáčajte regulačný prvok a nepočítajte závity bez pokynu výrobcu.",
            "Pri elektrickom ventilátore zariadenie bezpečne vypnite spôsobom uvedeným v manuáli. Pripravte vysávač s mäkkým nadstavcom, dve handričky, mäkkú kefku a malú nádobu. Čistiaci roztok nedržte nad otvoreným motorom. Pod mriežku položte uterák, aby prach a kvapky nekončili na stene alebo podlahe.",
        ],
        "steps": [
            "Určte, či ide o pasívnu mriežku, elektrický ventilátor, digestor alebo časť centrálneho systému.",
            "Vypnite súvisiace zariadenie podľa návodu a overte, že sa ventilátor nemôže neočakávane spustiť.",
            "Mäkkým nadstavcom povysávajte prednú plochu, lamely a okolie bez zatláčania hubice do otvoru.",
            "Pevnú mriežku utrite handričkou, ktorá je iba mierne vlhká, a pracujte od hornej hrany nadol.",
            "Odnímateľný kryt uvoľnite len povoleným spôsobom; nepáčte ho nožom a neťahajte za kábel alebo klapku.",
            "Mastnotu čistite po malých úsekoch jedným kompatibilným produktom naneseným na handričku, nie priamo do otvoru.",
            "Kryt opláchnite iba vtedy, ak to materiál a výrobca povoľujú, potom ho vysušte vrátane drážok a spojov.",
            "Skontrolujte otvor bez vkladania ruky alebo nástroja do motorovej časti a mriežku namontujte v pôvodnej orientácii.",
            "Zariadenie spustite a sledujte nezvyčajný hluk, vibrácie alebo nedoliehanie; pri probléme ho vypnite a skontrolujte montáž alebo servis.",
        ],
        "diagnosis_heading": "Ako rozlíšiť bežný prach od technického problému",
        "table2_headers": ["Pozorovanie", "Čo môže znamenať", "Čo urobiť"],
        "table2_rows": [
            ("Sivý suchý prach na hrane", "bežné usadzovanie častíc", "pomaly povysávať a utrieť mierne vlhkou handričkou"),
            ("Lepkavý žltkastý film", "aerosól z varenia spojený s prachom", "jemne odmastiť po skúške materiálu"),
            ("Kvapky vody alebo mokrá stena", "kondenzácia, netesnosť alebo problém systému", "neprekryť čistením; nájsť zdroj a zvážiť servis"),
            ("Čierne bodky, ktoré sa vracajú", "možná vlhkosť alebo mikrobiálny rast", "obmedziť expozíciu a riešiť príčinu vlhkosti"),
            ("Hluk, zápach spáleniny alebo slabý chod", "mechanická alebo elektrická porucha", "vypnúť a obrátiť sa na servis"),
        ],
        "sections": [
            ("Ako vyčistiť pevnú vetraciu mriežku bez rozmazania", [
                "Suchý prach najprv zachyťte vysávačom. Na úzke lamely použite mäkkú kefku súčasne s hubicou pri spodnej hrane. Až keď voľná vrstva zmizne, prejdite povrch dobre vyžmýkanou handričkou. Ak začnete mokrou špongiou, prach sa spojí s vodou a vytvorí sivú mapu na mriežke aj okolitej stene.",
                "Handričku často prekladajte na čistú stranu. Pri perforovanom plechu netlačte vodu do otvorov a pri lakovanom povrchu nepoužívajte hrubú pastu. Ak je mriežka na tapete alebo citlivej maľovke, chráňte okolie a čistič nenanášajte rozstrekom.",
            ]),
            ("Ako vyčistiť odnímateľný kryt ventilátora", [
                "Odnímanie má zmysel iba podľa manuálu. Po bezpečnom vypnutí kryt podoprite oboma rukami a uvoľnite predpísanú západku alebo skrutku. Odfoťte polohu, aby ste ho neotočili. Elektrické časti, motor a kábel neumývajte; domáci postup sa týka samotného odnímateľného krytu.",
                "Plastový kryt umyte jemne, opláchnite a nechajte úplne vyschnúť. Teplá voda nesmie byť taká horúca, aby plast zmäkol alebo sa deformoval. Pred nasadením skontrolujte západky a tesnenie. Ak je plast krehký alebo prasknutý, netlačte ho nasilu späť.",
            ]),
            ("Ako odstrániť mastnotu z kuchynskej mriežky", [
                "Kuchynský aerosól vytvorí lepkavú vrstvu, ktorá viaže prach. Najprv odsajte voľné častice, potom testujte malé množstvo univerzálneho čističa na skrytom mieste. Produkt dávajte na handričku a pracujte po jednej lamele. Silné rozpúšťadlo môže poškodiť lak, potlač alebo plast.",
                "Ak ide o digestor, mriežka a tukový filter nemusia byť rovnaká súčasť. Filter čistite alebo meňte podľa samostatného návodu výrobcu. Nestriekajte čistič do ventilátora. Pri mastnote na ďalších plochách nadväzuje návod <a href=\"/n/ako-vycistit-kuchynsku-linku-od-mastnoty-prachu-a-smuh-bez-poskodenia-povrchu\">ako vyčistiť kuchynskú linku</a>.",
            ]),
            ("Vetracia mriežka v kúpeľni: prach, para a čierne bodky", [
                "V kúpeľni môže prach navlhnúť a priľnúť pevnejšie. Po sprchovaní miestnosť vetrajte a povrch mriežky nechajte vyschnúť. Čierne bodky neoznačujte automaticky za bežnú špinu ani ich neprekryte vôňou. Skontrolujte, či sa na stene tvorí kondenzácia a či ventilátor skutočne funguje.",
                "Ak sa povlak vracia, je rozsiahly alebo je vlhkosť aj v potrubí, riešte príčinu. EPA odporúča pri podozrení na kontamináciu systému plesňou konzultovať postup a systém nespúšťať, ak by mohol problém rozšíriť. Domáce zotretie krytu nie je sanácia vnútra vzduchotechniky.",
            ]),
            ("Mriežka rekuperácie a prečo nemeníme nastavenie prietoku", [
                "Niektoré stropné a stenové výustky sú nastavené na konkrétny prietok. Pootočenie taniera alebo zmena hĺbky môže narušiť vyváženie systému. Čistite iba povrch a odnímajte výustku len podľa návodu, ideálne s označením pôvodnej polohy. Filter systému sa mení osobitne v intervale výrobcu.",
                "Ak je prúdenie slabé aj po vyčistení dostupnej mriežky, nehľadajte riešenie zasúvaním kefy do potrubia. Skontrolujte filter a servisné pokyny. Centrálne vetranie je systém; lokálna mriežka je iba jeho viditeľný koniec.",
            ]),
            ("Ako často mriežky čistiť", [
                "Interval prispôsobte priestoru. V kuchyni sa mastnota hromadí rýchlejšie, pri domácich zvieratách sa na nízkych mriežkach drží viac chlpov a počas peľovej sezóny pribúdajú vonkajšie častice. Krátka pravidelná kontrola je bezpečnejšia než čakanie na hrubú vrstvu, ktorá už vyžaduje silné drhnutie.",
                "Pri bežnom upratovaní povysávajte viditeľnú hranu a sledujte, či sa povlak vracia nezvyčajne rýchlo. Súčasne čistite ďalšie lapače prachu, napríklad <a href=\"/n/ako-vycistit-radiator-od-prachu-rebra-zadna-strana-mastnota-a-bezpecna-udrzba\">radiátory</a> a <a href=\"/n/ako-vycistit-parapety-a-okenne-ramy-prach-pel-cierne-mapy-a-skvrny\">parapety s okennými rámami</a>.",
            ]),
        ],
        "remember": [
            "Najprv určte typ systému; pasívna mriežka a elektrický ventilátor vyžadujú inú hranicu zásahu.",
            "Čistič patrí na handričku alebo odobratý kryt, nie do motora a potrubia.",
            "Voda, pleseň, spálený pach alebo neobvyklý hluk sú dôvod riešiť príčinu, nie iba povrch.",
        ],
        "mistakes": [
            "Mokré utieranie hrubej vrstvy prachu bez predchádzajúceho vysatia.",
            "Odnímanie krytu bez vypnutia zariadenia a bez návodu.",
            "Priame striekanie čističa cez lamely do motora alebo potrubia.",
            "Pootočenie nastavenej výustky rekuperácie bez označenia polohy.",
            "Použitie abrazívnej pasty na lakovaný alebo mäkký plast.",
            "Zakrytie mriežky nábytkom, textíliou alebo dekoráciou po čistení.",
        ],
        "caution": [
            "Zariadenie vypnite a zavolajte servis, ak počujete brúsenie, vidíte poškodený kábel, cítite spáleninu alebo sa ventilátor neotáča plynulo. Čistenie krytu neopraví ložisko, motor ani elektrické pripojenie.",
            "Pri vode v potrubí, opakovanej vlhkosti na stene alebo podozrení na pleseň v systéme sa vyhnite rozptyľovaniu nečistôt. Zdroj vlhkosti a rozsah problému má posúdiť správca, technik alebo odborná firma podľa typu budovy a systému.",
        ],
        "expert_heading": "Prach, prúdenie a hranice čistenia vzduchovodov",
        "expert": [
            "EPA opisuje vnútorný prach ako zmes častíc z vonkajšieho prostredia aj domácich aktivít. Čistenie môže časť usadeného prachu znovu rozvíriť, preto odporúča pravidelné upratovanie a vlhké utieranie po zachytení voľných častíc. Pri mriežke je praktická kombinácia mäkkej kefy, vysávača a až následne mierne vlhkej handričky.",
            "Viditeľný prach na mriežke nie je dôkaz, že treba automaticky vyčistiť celé potrubie. EPA uvádza, že ľahké množstvo prachu v rozvodoch samo osebe nepreukazuje zdravotné riziko a plošné čistenie odporúča riešiť podľa konkrétnej potreby. Dôležitá je diagnostika vody, plesne, škodcov alebo nadmerného nánosu, nie marketingový sľub.",
            "Pri čistiacich produktoch platí etiketa, vetranie a zákaz miešania. V blízkosti elektriny je potrebná ešte väčšia opatrnosť: roztok sa nanáša kontrolovane na handričku a odnímateľný kryt sa vracia až úplne suchý. Manuál výrobcu má vždy prednosť pred všeobecným postupom.",
        ],
        "source_intro": "EPA vysvetľuje pôvod vnútorných častíc, situácie, keď čistenie vzduchovodov nie je automaticky potrebné, aj bezpečný rámec práce s čistiacimi produktmi.",
        "sources": [
            ("US EPA: Sources of Indoor Particulate Matter", EPA_PM),
            ("US EPA: Should You Have the Air Ducts in Your Home Cleaned?", EPA_DUCTS),
            ("US EPA: Ventilation When Cleaning or Sanitizing Indoors", EPA_CLEANING_VENTILATION),
        ],
        "product_heading": "Praktické riešenie na kompatibilný odnímateľný kryt",
        "product_name": "Univerzálny voňavý čistič Vevo Pure Harmony 500 ml",
        "product_url": UNIVERSAL_PRODUCT,
        "product_intro": "Na umývateľný odnímateľný kryt alebo pevnú vonkajšiu plochu môže byť vhodný univerzálny čistič, ak to povoľuje výrobca a materiál prejde skúškou na skrytom mieste.",
        "product_use": "na mastný film a bežnú špinu na kompatibilnom plaste alebo lakovanom povrchu, nanesený najprv na handričku.",
        "product_limit": "nestriekajte ho do ventilátora, elektrických častí, filtra ani vzduchotechnického potrubia a nepoužívajte ho namiesto servisu.",
        "category_name": "Univerzálne čističe do domácnosti",
        "category_url": UNIVERSAL_CATEGORY,
        "category_intro": "Vetraciu mriežku nemožno vyberať podľa vône produktu. Najprv rozhoduje návod zariadenia, materiál krytu a to, či sa dá roztok udržať mimo motora a steny.",
        "category_text": "Porovnajte univerzálne čističe pre kompatibilné umývateľné povrchy. Pri elektrických zariadeniach používajte iba malé kontrolované množstvo na odobratý alebo bezpečne dostupný kryt.",
        "related": [
            ("Ako vyčistiť radiátor od prachu", "/n/ako-vycistit-radiator-od-prachu-rebra-zadna-strana-mastnota-a-bezpecna-udrzba"),
            ("Ako vyčistiť kuchynskú linku", "/n/ako-vycistit-kuchynsku-linku-od-mastnoty-prachu-a-smuh-bez-poskodenia-povrchu"),
            ("Ako vyčistiť parapety a okenné rámy", "/n/ako-vycistit-parapety-a-okenne-ramy-prach-pel-cierne-mapy-a-skvrny"),
        ],
        "faq_title": "vetracie mriežky",
        "faq": [
            ("Môžem vetraciu mriežku umyť pod vodou?", "Iba ak je odnímateľná, výrobca to povoľuje a neobsahuje elektrické či citlivé časti. Pred montážou ju úplne vysušte vrátane drážok a spojov."),
            ("Ako vyčistiť mriežku bez demontáže?", "Najprv ju povysávajte mäkkým nadstavcom a potom utrite dobre vyžmýkanou handričkou. Čistič aplikujte na handričku, nie cez lamely do otvoru."),
            ("Znamená prach na mriežke, že sú špinavé celé rozvody?", "Nie automaticky. Mriežka zachytáva častice priamo v prúde vzduchu. Potrebu zásahu do rozvodov posudzujte podľa vody, plesne, škodcov, nadmerného nánosu a odporúčania technika."),
            ("Čo robiť s mastnou mriežkou v kuchyni?", "Najprv odstráňte voľný prach, potom na skrytom mieste otestujte jemný odmasťovací postup. Pri digestore rozlišujte kryt a filter a riaďte sa samostatným návodom."),
            ("Prečo sa mriežka po čistení rosí?", "Príčinou môže byť vysoká vlhkosť, chladný povrch, nedostatočné prúdenie alebo technický problém. Opakovanú kondenzáciu neriešte len utieraním; skontrolujte vetranie a systém."),
        ],
    },
]

ARTICLES.extend(EXTRA_ARTICLES)


if __name__ == "__main__":
    main()
