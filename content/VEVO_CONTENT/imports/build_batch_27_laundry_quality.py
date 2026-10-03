import json
import re
import unicodedata
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from urllib.parse import urljoin

import requests
import xlwt


BASE = "https://www.vevo.sk"
BATCH = "batch-27"
BATCH_DATE = "2025-09-25"
OUT_JSON = Path("content/VEVO_CONTENT/imports/batch-27-2026-06-16-articles.json")
OUT_XLS = Path.home() / "AppData/Local/Temp/vevo-batch-27-laundry-quality-clean-urls.xls"


def slugify(value):
    value = unicodedata.normalize("NFKD", value)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = re.sub(r"[^a-zA-Z0-9]+", "-", value.lower())
    return re.sub(r"-+", "-", value).strip("-")


def table(headers, rows):
    head = "".join(
        f'<th style="border: 1px solid #e5e5e5; padding: 10px; text-align: left;">{escape(str(header))}</th>'
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
<h2 style="margin-top: 0;">{title}</h2>
<ul>{items}</ul>
</div>
""".strip()


def fanout_box(queries):
    items = "".join(f"<li>{escape(query)}</li>" for query in queries)
    return f"""
<div style="border: 1px solid #d7e2ec; border-radius: 8px; padding: 18px; margin: 22px 0; background: #f7fbff;">
<h2 style="margin-top: 0;">Čo v článku nájdete</h2>
<p>Nižšie nájdete praktické odpovede aj na otázky, ktoré ľudia pri praní často riešia samostatne.</p>
<ul>{items}</ul>
</div>
""".strip()


def sources(items):
    rows = "".join(f'<li><a rel="noopener" href="{href}" target="_blank">{escape(label)}</a></li>' for label, href in items)
    return f"""
<div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; margin: 22px 0; background: #fbfbfb;">
<h2 style="margin-top: 0;">Zdroje a odborný kontext</h2>
<ul>{rows}</ul>
</div>
""".strip()


def sales_block(sales):
    bullets = "".join(
        f"<li><strong>{escape(label)}:</strong> {escape(text)}</li>"
        for label, text in sales["category_bullets"]
    )
    return f"""
<div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 24px 0; background: #f7fbf8;">
<h2 style="margin-top: 0;">{escape(sales["heading"])}</h2>
<p>{escape(sales["intro"])}</p>
<div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff; margin: 14px 0;">
<h3 style="margin-top: 0;">{escape(sales["product_name"])}</h3>
<p><strong>Kedy dáva zmysel:</strong> {escape(sales["fit"])}</p>
<p><strong>Kedy najprv riešiť príčinu:</strong> {escape(sales["boundary"])}</p>
<p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="{sales["product_href"]}">{escape(sales["product_button"])}</a></p>
</div>
</div>
<div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 24px 0; background: #fffaf5;">
<h2 style="margin-top: 0;">{escape(sales["category_title"])}</h2>
<p>{escape(sales["category_intro"])}</p>
<ul>{bullets}</ul>
<p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="{sales["category_href"]}">{escape(sales["category_button"])}</a></p>
</div>
""".strip()


def related(items):
    links = "".join(f'<li><a href="{href}">{escape(label)}</a></li>' for label, href in items)
    return f"<h2>Súvisiace návody na VEVO</h2>\n<ul>{links}</ul>"


ARTICLES = [
    {
        "title": "Ako dávkovať prací gél podľa tvrdosti vody, náplne a znečistenia",
        "short": "Prací gél dávkujte podľa troch vecí: tvrdosť vody, veľkosť náplne a miera znečistenia. Pri mäkkej vode a malej náplni stačí menej, pri tvrdej vode, uterákoch alebo spotenom oblečení treba dávku prispôsobiť, ale nie naslepo zdvojnásobiť.",
        "queries": [
            "koľko pracieho gélu na 4 kg bielizne",
            "koľko pracieho gélu na 7 kg bielizne",
            "dávkovanie pracieho gélu pri tvrdej vode",
            "dávkovanie pracieho gélu pri mäkkej vode",
            "gél do zásobníka alebo priamo do bubna",
            "koľko gélu na uteráky",
            "koľko gélu na športové oblečenie",
            "prečo bielizeň lepí po praní",
            "priveľa pracieho gélu v práčke",
            "dávkovanie gélu pri krátkom programe",
        ],
        "quick": [
            "<strong>Začnite návodom na obale.</strong> Berte ho ako štart, nie ako absolútne pravidlo pre každú dávku.",
            "<strong>Tvrdá voda zvyčajne potrebuje vyššiu dávku než mäkká.</strong> Netreba však pridávať automaticky dvojnásobok.",
            "<strong>Preplnený bubon nevyrieši viac gélu.</strong> Zhorší pohyb textilu aj oplach.",
            "<strong>Pri citlivej pokožke sledujte hlavne oplach.</strong> Menej zvyškov v textile je dôležitejšie než silná vôňa.",
        ],
        "intro": [
            "Dávkovanie pracieho gélu vyzerá banálne, ale v praxi patrí medzi najčastejšie príčiny tvrdých uterákov, lepkavých tričiek, slabého oplachu a oblečenia, ktoré po vysušení stále nevonia čisto. Problém nie je len v samotnom produkte. Rozhoduje voda, množstvo textilu, program, miera špiny a to, či má prací roztok priestor dostať sa medzi vlákna.",
            "Ak hľadáte presnú odpoveď typu koľko pracieho gélu na 4 kg alebo 7 kg bielizne, najpresnejšia odpoveď je kombinácia odporúčania výrobcu a domácej reality. Inak sa správa mäkká voda v malej dávke tričiek a inak tvrdá voda pri uterákoch, športovej syntetike alebo pracovnom oblečení.",
            "Dôležité je nepliesť si čistotu s intenzitou vône. Ak bielizeň nevonia, nemusí to znamenať, že gélu bolo málo. Často je ho naopak veľa, zle sa vypláchol a v textile drží pot, maz alebo minerály z vody.",
        ],
        "why": [
            "Prací gél obsahuje povrchovo aktívne látky, ktoré pomáhajú vode uvoľniť mastnotu a špinu. Aby fungovali, potrebujú správnu koncentráciu. Pri príliš nízkej dávke môže byť prací kúpeľ slabý, pri príliš vysokej dávke sa zvyšky horšie vyplachujú.",
            "Tvrdosť vody mení účinnosť čistiacich látok. Tvrdšia voda obsahuje viac minerálov, najmä vápnika a horčíka, ktoré môžu ovplyvniť pranie aj pocit z textilu. Preto sa pri tvrdej vode dávkovanie upravuje, ale stále musí zostať v rozumnom pomere k veľkosti náplne.",
        ],
        "rows": [
            ("Malá náplň tričiek", "nižšia dávka podľa obalu", "priveľa gélu sa môže horšie vypláchnuť"),
            ("Uteráky", "nepreplniť bubon a pridať dôkladný oplach", "froté drží vodu aj zvyšky prostriedku"),
            ("Športové oblečenie", "primeraná dávka a dlhší oplach", "pot a maz sa nemajú maskovať vôňou"),
            ("Tvrdá voda", "dávku upraviť podľa tvrdosti", "sledovať aj tvrdosť bielizne po vysušení"),
            ("Krátky program", "skôr menej gélu a malá náplň", "má menej času na rozpustenie a oplach"),
        ],
        "steps": [
            "Pozrite odporúčanie na obale pre mäkkú, stredne tvrdú a tvrdú vodu.",
            "Zhodnoťte náplň: poloprázdny bubon, bežná náplň a veľká dávka uterákov nie sú rovnaká situácia.",
            "Pri bežnom nosení nezačínajte vyššou dávkou. Zvýšenie má zmysel pri reálne špinavšej dávke alebo tvrdej vode.",
            "Ak bielizeň lepí, je príliš voňavá alebo škrabe, najbližšie znížte dávku a pridajte oplach.",
            "Pri opakovanom zápachu skontrolujte aj zásobník, filter, tesnenie a sušenie. Dávka gélu nemusí byť hlavný problém.",
        ],
        "decision_rows": [
            ("Bielizeň je po praní lepkavá", "znížiť dávku a dať extra oplach", "zvyšky gélu ostali vo vláknach"),
            ("Bielizeň po vysušení zapácha", "nepridávať hneď viac gélu", "môže ísť o pot, vlhkosť alebo špinavú práčku"),
            ("Uteráky sú tvrdé", "menej gélu, viac priestoru, riešiť tvrdú vodu", "froté zle znáša prebytky"),
            ("Čierne oblečenie má mapy", "menšia dávka a lepší oplach", "zvyšky sú na tmavom textile viditeľnejšie"),
        ],
        "mistakes": [
            "Dávkovať podľa vône, nie podľa vody, náplne a znečistenia.",
            "Zdvojnásobiť gél pri preplnenom bubne.",
            "Použiť veľa gélu v krátkom programe a očakávať lepší výsledok.",
            "Prať uteráky, obliečky a športové veci v jednej veľkej dávke.",
            "Ignorovať zásobník a filter, keď sa zápach opakuje.",
        ],
        "expert": [
            "Tvrdosť vody sa odborne spája najmä s obsahom rozpusteného vápnika a horčíka. V domácnosti sa prejavuje vodným kameňom, usadeninami a aj tým, ako čistiace prostriedky pracujú s vodou. Pri praní preto nejde iba o silu produktu, ale o celú kombináciu vody, dávky, mechaniky a oplachu.",
            "Pri citlivej pokožke alebo detskom oblečení je rozumné myslieť na zvyšky v textile. Jemnejší produkt je len jedna časť riešenia. Druhá časť je správna dávka, nepreplnený bubon a dostatočný oplach.",
        ],
        "sources": [
            ("USGS: Hardness of Water", "https://www.usgs.gov/water-science-school/science/hardness-water"),
            ("EPA: Safer Choice Criteria for Surfactants", "https://www.epa.gov/saferchoice/safer-choice-criteria-surfactants"),
            ("Energy.gov: Laundry", "https://www.energy.gov/energysaver/laundry"),
        ],
        "sales": {
            "heading": "Riešenie pre bežné pranie bez zbytočných zvyškov",
            "intro": "Ak upravujete dávkovanie, vyberajte produkt, pri ktorom viete pracovať s množstvom a ktorý nepoužívate na prekrývanie chýb v praní.",
            "product_name": "Prací gél hypoalergénny z Marseillského mydla 1L",
            "product_href": "/p-1551/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l",
            "fit": "pri bežnom praní, keď chcete jemný prací základ a dávku prispôsobiť vode, náplni a znečisteniu.",
            "boundary": "ak bielizeň lepí alebo zapácha, najprv znížte náplň, upravte dávku a pridajte oplach. Produkt nemá nahradiť čistú práčku.",
            "product_button": "Pozrieť prací gél",
            "category_title": "Porovnajte pracie gély podľa rutiny",
            "category_intro": "Výber pracieho gélu dávajte do súvisu s tým, čo periete najčastejšie: tričká, uteráky, obliečky, šport alebo detskú bielizeň.",
            "category_bullets": [
                ("Bežné pranie", "dôležitá je stabilná dávka a dobrý oplach."),
                ("Tvrdá voda", "sledujte dávkovanie a pocit z textilu po vysušení."),
                ("Citlivá pokožka", "nepreháňajte dávku a pridajte dôkladný oplach."),
            ],
            "category_href": "/c/vevo-home-care/pranie/praci-gel",
            "category_button": "Pozrieť pracie gély",
        },
        "related": [
            ("Ako funguje prací gél", "/n/ako-funguje-praci-gel-tenzidy-enzymy-ph-a-davkovanie-pri-beznom-prani"),
            ("Prečo je bielizeň po praní tvrdá alebo lepkavá", "/n/preco-je-bielizen-po-prani-tvrda-alebo-lepkava-zvysky-gelu-davkovanie-a-oplach"),
            ("Ako vybrať prací gél podľa typu bielizne", "/n/ako-vybrat-praci-gel-podla-typu-bielizne"),
            ("Tvrdá voda a pranie", "/n/tvrda-voda-a-pranie-preco-je-bielizen-tvrda-siva-a-bez-vone"),
        ],
        "faq": [
            ("Môžem dať prací gél priamo do bubna?", "Závisí od produktu a odporúčania výrobcu. Pri niektorých géloch to možné je, ale zásobník má výhodu kontrolovaného dávkovania počas programu."),
            ("Je viac pracieho gélu lepšie pri špinavej bielizni?", "Nie automaticky. Pri silnej špine je často dôležitejší vhodný program, predčistenie, priestor v bubne a oplach."),
            ("Prečo bielizeň po praní príliš silno vonia?", "Môže v nej zostať priveľa gélu alebo parfumácie. Skúste menšiu dávku a extra oplach."),
            ("Ako zistím tvrdosť vody?", "Pozrite údaje od vodárenskej spoločnosti alebo použite orientačný test tvrdosti vody. Dávkovanie potom prispôsobte návodu produktu."),
        ],
    },
    {
        "title": "Prací gél alebo prací prášok: kedy čo funguje lepšie a prečo",
        "short": "Prací gél sa dobre hodí na bežné pranie, nižšie teploty a tmavšie oblečenie, prací prášok býva praktický pri bielej bielizni a niektorých odolnejších škvrnách. Rozhoduje materiál, teplota, škvrna, voda a to, či sa prostriedok dobre vypláchne.",
        "queries": [
            "prací gél alebo prací prášok",
            "prací gél vs prášok na biele prádlo",
            "prací gél vs prášok na farebné oblečenie",
            "čo je lepšie na škvrny",
            "čo je lepšie pri nízkej teplote",
            "prací prášok a biele šmuhy",
            "prací gél na čierne oblečenie",
            "prací gél alebo kapsuly",
            "prací prostriedok pri citlivej pokožke",
            "zvyšky pracieho prostriedku v oblečení",
        ],
        "quick": [
            "<strong>Na bežné farebné pranie je gél praktická voľba.</strong> Ľahko sa dávkuje a dobre sa používa pri nižších teplotách.",
            "<strong>Na bielu bielizeň môže mať prášok výhodu.</strong> Závisí však od zloženia a od toho, čo povoľuje štítok.",
            "<strong>Na tmavom oblečení sledujte zvyšky.</strong> Nerozpustený prášok alebo priveľa prostriedku môže robiť mapy.",
            "<strong>Pri citlivej pokožke rozhoduje oplach.</strong> Forma produktu je menej dôležitá než zvyšky v textile.",
        ],
        "intro": [
            "Otázka prací gél alebo prací prášok nemá jednu univerzálnu odpoveď. Oba typy môžu prať dobre, ak sa použijú v správnej situácii. Rozdiel je v tom, ako sa dávkujú, rozpúšťajú, oplachujú a ako pracujú s rôznymi typmi škvŕn a textílií.",
            "Prací gél ľudia často volia pri farebnom a tmavom oblečení, pri nižších teplotách a pri bežnom nosení. Prášok môže byť silnejší pri niektorých odolnejších škvrnách a bielej bielizni, ale pri zlom dávkovaní alebo krátkom programe môže zanechať šmuhy.",
            "Dôležité je neriešiť formu produktu oddelene od programu. Inak sa správa dlhší program s dostatkom vody a inak rýchly program, preplnený bubon a tvrdá voda.",
        ],
        "why": [
            "Gély sú tekuté, preto sa ľahko dávkujú a v bežnej domácej rutine sa dobre kombinujú s nižšími teplotami. Pri prebytku však môžu v textile zanechať film podobne ako iný prací prostriedok.",
            "Prášky môžu obsahovať zložky, ktoré sú praktické pri bielej bielizni alebo odolnejších škvrnách. Ich slabina v domácnosti býva nerozpustený zvyšok pri krátkom programe, nízkej teplote, tvrdej vode alebo preplnenom bubne.",
        ],
        "rows": [
            ("Bežné farebné tričká", "prací gél", "praktický pri nižšej teplote a každodennom praní"),
            ("Biele uteráky", "podľa štítku a typu škvŕn", "riešte aj tvrdosť vody, dávku a oplach"),
            ("Čierne oblečenie", "často skôr gél", "nižšie riziko viditeľných práškových šmúh"),
            ("Blato a pracovné veci", "podľa špiny, často aj predčistenie", "forma produktu nenahradí mechanické odstránenie špiny"),
            ("Citlivá pokožka", "jemnejší produkt + oplach", "sledujte najmä zvyšky v textile"),
        ],
        "steps": [
            "Najprv rozdeľte bielizeň podľa farby a materiálu, nie podľa toho, čo je práve poruke.",
            "Pri tmavom oblečení a rýchlych programoch začnite skôr tekutým prostriedkom a primeranou dávkou.",
            "Pri bielej bielizni posúďte, či riešite zašednutie, škvrny alebo zápach. Každý problém má inú príčinu.",
            "Pri škvrnách nepoužívajte iba viac prostriedku. Škvrnu často treba predčistiť lokálne.",
            "Ak sa objavujú šmuhy, znížte dávku, nepreplňte bubon a skontrolujte, či program stačí na rozpustenie a oplach.",
        ],
        "decision_rows": [
            ("Nízka teplota", "gél býva praktickejší", "ľahšie sa používa v tekutej forme"),
            ("Biele textílie", "zvážiť prášok alebo špecializovaný postup", "záleží na zložení a štítku"),
            ("Tmavé veci", "gél a dôkladný oplach", "menej rizika bielych šmúh"),
            ("Silná škvrna", "predčistiť a až potom prať", "samotná forma prostriedku nemusí stačiť"),
        ],
        "mistakes": [
            "Striedať gél a prášok bez zmeny dávkovania.",
            "Použiť prášok v krátkom programe a preplnenom bubne.",
            "Riešiť každú škvrnu väčšou dávkou pracieho prostriedku.",
            "Prať tmavé oblečenie s priveľkou dávkou a potom riešiť šmuhy.",
            "Ignorovať štítok pri vlne, membráne, funkčných materiáloch a jemných zmesiach.",
        ],
        "expert": [
            "Z odborného pohľadu je forma produktu len jedna premenná. Čistiaci výsledok vzniká kombináciou chemického zloženia, teploty, času, mechaniky, vody a oplachu. Preto môže rovnaký produkt fungovať výborne v jednej práčke a slabšie v druhej rutine.",
            "Pri porovnaní gélu a prášku si všímajte hlavne to, či sa prostriedok rozpustí, či sa dostane k vláknu a či sa z textilu vypláchne. Viditeľné šmuhy alebo silný film na dotyk sú signál, že proces nie je vyvážený.",
        ],
        "sources": [
            ("EPA: Safer Choice Criteria for Surfactants", "https://www.epa.gov/saferchoice/safer-choice-criteria-surfactants"),
            ("Energy.gov: Laundry", "https://www.energy.gov/energysaver/laundry"),
            ("FTC: Care Labeling Rule guidance", "https://www.ftc.gov/business-guidance/resources/clothes-captioning-complying-care-labeling-rule"),
        ],
        "sales": {
            "heading": "Riešenie pre bežné farebné a každodenné pranie",
            "intro": "Ak hľadáte univerzálny základ pre každodenné pranie, tekutý prací gél je praktická voľba. Stále však platí, že dávka a oplach rozhodujú rovnako ako produkt.",
            "product_name": "Prací gél hypoalergénny z Marseillského mydla 1L",
            "product_href": "/p-1551/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l",
            "fit": "pri farebnom a bežnom praní, kde chcete pracovať s jasnou dávkou a vyhnúť sa zbytočným zvyškom.",
            "boundary": "ak riešite špecifické bielenie alebo veľmi odolné škvrny, najprv zvoľte správny postup podľa materiálu a štítku.",
            "product_button": "Pozrieť prací gél",
            "category_title": "Vyberte formu prania podľa bielizne",
            "category_intro": "Pri porovnávaní pracích prostriedkov začnite tým, čo doma reálne periete najčastejšie.",
            "category_bullets": [
                ("Farebné oblečenie", "dôležitá je stabilná dávka a šetrný program."),
                ("Biela bielizeň", "riešte škvrny, zašednutie a tvrdosť vody oddelene."),
                ("Citlivá pokožka", "vyberajte jemnejšie riešenie a nepridávajte zbytočne veľa produktu."),
            ],
            "category_href": "/c/vevo-home-care/pranie/praci-gel",
            "category_button": "Pozrieť pracie gély",
        },
        "related": [
            ("Ako funguje prací gél", "/n/ako-funguje-praci-gel-tenzidy-enzymy-ph-a-davkovanie-pri-beznom-prani"),
            ("Ako vybrať prací gél podľa typu bielizne", "/n/ako-vybrat-praci-gel-podla-typu-bielizne"),
            ("Ako odstrániť biele šmuhy od pracieho prášku z čierneho oblečenia", "/n/ako-odstranit-biele-smuhy-od-pracieho-prasku-z-cierneho-oblecenia"),
            ("Ako prať bielu bielizeň", "/n/ako-prat-bielu-bielizen-aby-nezosedla-a-nezapachala"),
        ],
        "faq": [
            ("Je prací gél slabší ako prášok?", "Nie automaticky. Záleží na zložení, škvrne, vode, programe a dávke."),
            ("Čo je lepšie na čierne oblečenie?", "Často je praktickejší gél, pretože pri správnej dávke znižuje riziko viditeľných šmúh."),
            ("Čo je lepšie pri bielej bielizni?", "Pri bielej bielizni môže mať výhodu prášok alebo špecializovaný postup, ale vždy rešpektujte štítok a materiál."),
            ("Sú kapsuly lepšie než gél?", "Sú pohodlné, ale horšie sa prispôsobujú malej náplni alebo špecifickému dávkovaniu."),
        ],
    },
    {
        "title": "Extra oplach v práčke: kedy pomôže pri zápachu, tvrdej bielizni a citlivej pokožke",
        "short": "Extra oplach má zmysel vtedy, keď v textile ostávajú zvyšky pracieho prostriedku, bielizeň lepí, príliš silno vonia, škrabe alebo ju nosí človek s citlivejšou pokožkou. Nepomôže však, ak je problém v špinavej práčke, pomalom sušení alebo nevhodnom programe.",
        "queries": [
            "kedy použiť extra oplach v práčke",
            "extra oplach pri citlivej pokožke",
            "extra oplach pri detskom oblečení",
            "extra oplach pri uterákoch",
            "extra oplach pri tvrdej bielizni",
            "zvyšky pracieho gélu v oblečení",
            "bielizeň po praní lepí",
            "silná vôňa po praní",
            "extra oplach alebo menej pracieho gélu",
            "extra oplach pri športovom oblečení",
        ],
        "quick": [
            "<strong>Extra oplach pomáha pri zvyškoch.</strong> Ak textil lepí, škrabe alebo vonia príliš silno, oplach môže byť správny prvý krok.",
            "<strong>Nie je to náhrada čistenia práčky.</strong> Ak zapácha bubon, tesnenie alebo filter, treba riešiť spotrebič.",
            "<strong>Pri citlivej pokožke začnite opatrne.</strong> Menej produktu a lepší oplach sú praktickejšie než silnejšia parfumácia.",
            "<strong>Pri uterákoch sledujte savosť.</strong> Zvyšky gélu a aviváže môžu zhoršiť pocit aj funkciu froté.",
        ],
        "intro": [
            "Extra oplach je jedna z najpraktickejších funkcií práčky, no veľa ľudí ju používa náhodne. Niekedy pomôže okamžite: bielizeň je príjemnejšia, menej škrabe a nepôsobí lepkavo. Inokedy neprinesie veľký rozdiel, pretože príčina nie je v oplachu, ale v špinavej práčke, preplnení alebo zlom sušení.",
            "Najviac dáva zmysel vtedy, keď máte podozrenie na zvyšky pracieho prostriedku, aviváže alebo parfumácie. Typické signály sú silná vôňa po praní, film na textile, svrbenie po oblečení, tvrdé uteráky alebo tričká, ktoré po zahriatí tela znova zapáchajú.",
            "Extra oplach však nie je povinný pri každej dávke. Predlžuje program a spotrebuje viac vody. Preto je dobré vedieť, kedy je užitočný a kedy iba maskuje chybu v dávkovaní.",
        ],
        "why": [
            "Po hlavnom praní má oplach odstrániť z textilu uvoľnenú špinu a zvyšky pracieho roztoku. Ak je prostriedku priveľa, bubon je preplnený alebo program je krátky, časť zvyškov môže ostať vo vláknach.",
            "Pri citlivejšej pokožke nie je vhodné sľubovať medicínsky účinok, ale prakticky platí, že menej zvyškov v textile býva príjemnejšie. Extra oplach je preto rozumný test pri bielizni, ktorá ide priamo na telo.",
        ],
        "rows": [
            ("Lepkavé tričká", "áno, skúsiť extra oplach", "pravdepodobné zvyšky gélu"),
            ("Tvrdé uteráky", "áno, spolu s menšou dávkou", "froté drží zvyšky aj minerály"),
            ("Zápach z práčky", "nie ako jediné riešenie", "treba čistiť bubon, tesnenie alebo filter"),
            ("Detské oblečenie", "môže dávať zmysel", "najmä pri bielizni priamo na pokožke"),
            ("Rýchly program", "často áno alebo zvoliť dlhší program", "krátky čas môže zhoršiť oplach"),
        ],
        "steps": [
            "Najbližšiu dávku vyperte s menším množstvom gélu než zvyčajne.",
            "Neplňte bubon nadoraz, najmä pri uterákoch, obliečkach a športových veciach.",
            "Zapnite extra oplach a po vysušení porovnajte pocit z textilu.",
            "Ak sa zlepší lepkavosť alebo silná vôňa, problém bol pravdepodobne v dávke alebo oplachu.",
            "Ak zápach ostáva, skontrolujte práčku, filter, zásobník a spôsob sušenia.",
        ],
        "decision_rows": [
            ("Textil škrabe", "menej gélu + extra oplach", "test zvyškov v textile"),
            ("Bielizeň je zatuchnutá", "riešiť sušenie a práčku", "oplach neodstráni plesnivý zdroj"),
            ("Silná vôňa", "znížiť parfumáciu a opláchnuť", "vôňa môže zostať ako zvyšok"),
            ("Športová syntetika", "menšia dávka, dobrý oplach", "pot a maz netreba prekryť"),
        ],
        "mistakes": [
            "Pridať extra oplach, ale ponechať dvojnásobnú dávku gélu.",
            "Riešiť zápach z bubna iba ďalším oplachom bielizne.",
            "Použiť extra oplach pri každej dávke bez hľadania príčiny.",
            "Miešať uteráky, šport a jemné oblečenie v jednej preplnenej dávke.",
            "Pridať viac parfumu do prania, keď je problém v zvyškoch produktu.",
        ],
        "expert": [
            "Extra oplach je praktická reakcia na riziko zvyškov v textile. Pri moderných úsporných práčkach sa pracuje s optimalizovaným množstvom vody, čo je výhoda pre spotrebu, ale pri nesprávnej dávke alebo preplnení môže byť oplach slabšie vnímaný.",
            "Pri zdravotne citlivých témach treba zostať presný: extra oplach nie je liečba kožných problémov. Je to domáce opatrenie, ktorým znížite pravdepodobnosť zvyškov pracieho prostriedku v textile. Ak má človek pretrvávajúce podráždenie, rieši sa to s odborníkom.",
        ],
        "sources": [
            ("Energy Star: Clothes washers", "https://www.energystar.gov/products/clothes_washers"),
            ("Energy.gov: Laundry", "https://www.energy.gov/energysaver/laundry"),
            ("EPA: Safer Choice Label", "https://www.epa.gov/saferchoice/learn-about-safer-choice-label"),
        ],
        "sales": {
            "heading": "Riešenie pre bielizeň priamo na pokožku",
            "intro": "Ak používate extra oplach pre lepší pocit z bielizne, pracujte aj s jemnejším pracím základom a správnou dávkou.",
            "product_name": "Prací gél hypoalergénny z Marseillského mydla 1L",
            "product_href": "/p-1551/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l",
            "fit": "pri bežnej bielizni priamo na telo, keď chcete spojiť rozumnú dávku s dôkladným oplachom.",
            "boundary": "ak práčka zapácha alebo je zásobník zanesený, najprv riešte čistenie spotrebiča. Extra oplach nie je náhrada hygieny práčky.",
            "product_button": "Pozrieť prací gél",
            "category_title": "Vyberte pranie pre citlivejší režim",
            "category_intro": "Pri citlivejšom praní nejde iba o produkt. Dôležitý je krátky zoznam: primeraná dávka, nepreplnený bubon a dobrý oplach.",
            "category_bullets": [
                ("Bielizeň na telo", "uprednostnite menej zvyškov a dobrý oplach."),
                ("Detské oblečenie", "nepreháňajte dávku ani parfumáciu."),
                ("Uteráky", "sledujte savosť, tvrdosť a pocit po vysušení."),
            ],
            "category_href": "/c/vevo-home-care/pranie/hypoalergenne-pracie-prostriedky",
            "category_button": "Pozrieť hypoalergénne pranie",
        },
        "related": [
            ("Prečo je bielizeň po praní tvrdá alebo lepkavá", "/n/preco-je-bielizen-po-prani-tvrda-alebo-lepkava-zvysky-gelu-davkovanie-a-oplach"),
            ("Prečo moje oblečenie zapácha po praní", "/n/preco-moje-oblecenie-zapacha-po-prani-priciny-a-riesenia"),
            ("Ako prať detské oblečenie bez podráždenia pokožky", "/n/ako-prat-detske-oblecenie-a-oblecenie-pre-babaetko-bez-podrazdenia-pokozky"),
            ("Ako vyčistiť zásobník práčky", "/n/ako-vycistit-zasobnik-pracky-od-usadenin-pracieho-gelu-a-avivaze"),
        ],
        "faq": [
            ("Je extra oplach potrebný vždy?", "Nie. Používajte ho cielene pri zvyškoch, citlivejšej bielizni, uterákoch alebo silnej vôni po praní."),
            ("Pomôže extra oplach na zápach z práčky?", "Nie ako hlavné riešenie. Ak zapácha práčka, treba vyčistiť zásobník, filter, tesnenie alebo bubon."),
            ("Je extra oplach vhodný pre detské oblečenie?", "Môže byť užitočný, najmä ak periete bielizeň priamo na pokožku. Zároveň znížte dávku a parfumáciu."),
            ("Čo ak je bielizeň stále tvrdá?", "Skontrolujte tvrdosť vody, dávkovanie, preplnenie bubna a použitie aviváže."),
        ],
    },
    {
        "title": "Krátky program v práčke: kedy stačí a kedy zhoršuje zvyšky pracieho prostriedku",
        "short": "Krátky program stačí na málo nosené, ľahko znečistené oblečenie a malú náplň. Nie je vhodný na uteráky, obliečky, spotené športové veci, pracovné oblečenie ani situácie, kde potrebujete dôkladné pranie a oplach.",
        "queries": [
            "kedy použiť krátky program v práčke",
            "rýchle pranie 15 minút",
            "rýchly program a zvyšky gélu",
            "krátky program na uteráky",
            "krátky program na obliečky",
            "krátky program na športové oblečenie",
            "prečo bielizeň po krátkom programe zapácha",
            "koľko gélu pri krátkom programe",
            "krátky program alebo normálny program",
            "rýchle pranie pri citlivej pokožke",
        ],
        "quick": [
            "<strong>Krátky program je na ľahkú dávku.</strong> Hodí sa na málo nosené veci bez výrazného potu a škvŕn.",
            "<strong>Pri krátkom programe dávkujte menej.</strong> Menej času znamená menší priestor na rozpustenie a oplach.",
            "<strong>Uteráky a obliečky ním neperte rutinne.</strong> Držia vlhkosť, pot, maz a zvyšky produktu.",
            "<strong>Ak bielizeň zapácha, krátky program často problém zhorší.</strong> Vyberte radšej dlhší program a dobrý oplach.",
        ],
        "intro": [
            "Krátky program v práčke je lákavý: šetrí čas a pôsobí prakticky pri malej dávke. Problém vzniká vtedy, keď sa z rýchleho riešenia stane univerzálny režim na všetko. Bielizeň sa síce namočí a otočí v bubne, ale nemusí mať dostatok času na uvoľnenie potu, mazu, škvŕn a zvyškov pracieho prostriedku.",
            "Rýchle pranie môže stačiť pri tričku nosenom krátko, pri ľahkej blúzke alebo pri osviežení malej dávky. Na uteráky, obliečky, športové veci po tréningu, pracovné oblečenie a detské nehody je to slabý kompromis.",
            "Najväčšia chyba je použiť krátky program a zároveň veľa pracieho gélu. Krátky čas a prebytok produktu spolu zvyšujú riziko lepkavého pocitu, šmúh, silnej vône a návratu zápachu po vysušení.",
        ],
        "why": [
            "Pranie potrebuje čas na navlhčenie textilu, pohyb, pôsobenie pracieho roztoku a oplach. Keď program skrátite, niektoré kroky sú obmedzené. Pri malej a ľahkej dávke to môže stačiť, pri objemných textíliách nie.",
            "Krátky program tiež nie je rovnaký ako úsporný program. Niektoré ekologické programy sú dlhšie práve preto, že pracujú s nižšou teplotou a optimalizovanou spotrebou vody. Rýchlosť preto netreba automaticky chápať ako najšetrnejšiu voľbu.",
        ],
        "rows": [
            ("Tričko po krátkom nosení", "krátky program môže stačiť", "malá dávka a primeraná dávka gélu"),
            ("Uteráky", "radšej bežný program", "potrebujú priestor, vodu a oplach"),
            ("Obliečky", "radšej dlhší program", "držia pot, maz a objem vody"),
            ("Šport po tréningu", "krátky program často nestačí", "pot a maz sa môžu vrátiť zápachom"),
            ("Pracovné oblečenie", "nie", "špina potrebuje čas alebo predčistenie"),
        ],
        "steps": [
            "Krátky program používajte len pri malej a ľahko znečistenej dávke.",
            "Znížte dávku gélu oproti bežnému programu, najmä pri mäkkej vode.",
            "Neperte na rýchlom programe veci, ktoré boli spotené, mastné alebo dlho vlhké.",
            "Ak sa po krátkom programe objaví zápach alebo lepkavosť, ďalšiu dávku perte dlhšie a s extra oplachom.",
            "Pri pravidelnom zápachu vyčistite zásobník a skontrolujte, či práčku nepreťažujete krátkymi vlhkými dávkami.",
        ],
        "decision_rows": [
            ("Malá dávka bez škvŕn", "krátky program áno", "stačí osvieženie"),
            ("Potené športové veci", "skôr nie", "pot a maz potrebujú čas a oplach"),
            ("Uteráky po sprche", "nie ako rutina", "froté zadrží vlhkosť a zvyšky"),
            ("Detské znečistenie", "radšej predčistenie a normálny program", "rýchlosť nenahradí odstránenie špiny"),
        ],
        "mistakes": [
            "Prať uteráky na 15-minútovom programe a čakať hotelový pocit.",
            "Použiť veľkú dávku gélu, aby krátky program pral silnejšie.",
            "Dávať do krátkeho programu preplnený bubon.",
            "Používať rýchle pranie na spotené športové oblečenie zatvorené v taške.",
            "Ignorovať šmuhy a lepkavosť ako signál zlého oplachu.",
        ],
        "expert": [
            "Z technického pohľadu je krátky program kompromis medzi časom, mechanikou, vodou a oplachom. Ak znížite čas, musíte znížiť aj nároky na program: menšia náplň, menej špiny a rozumná dávka prostriedku.",
            "Pri moderných práčkach sa programy líšia nielen trvaním, ale aj množstvom vody, profilom otáčania, ohrevom a oplachom. Preto je dôležité čítať manuál práčky a štítky na textíliách, nie používať najkratší režim ako univerzálny.",
        ],
        "sources": [
            ("Energy.gov: Laundry", "https://www.energy.gov/energysaver/laundry"),
            ("Energy Star: Clothes washers", "https://www.energystar.gov/products/clothes_washers"),
            ("FTC: Care Labeling Rule guidance", "https://www.ftc.gov/business-guidance/resources/clothes-captioning-complying-care-labeling-rule"),
        ],
        "sales": {
            "heading": "Riešenie pre krátky program bez zbytočných zvyškov",
            "intro": "Ak používate krátke programy, najdôležitejšie je neprehnať dávku a neprať veľké alebo špinavé dávky narýchlo.",
            "product_name": "Prací gél hypoalergénny z Marseillského mydla 1L",
            "product_href": "/p-1551/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l",
            "fit": "pri menších bežných dávkach, kde chcete tekutý prací základ a dávku viete znížiť podľa krátkeho programu.",
            "boundary": "ak periete uteráky, obliečky, šport alebo pracovné oblečenie, zvoľte radšej plnohodnotný program a dobrý oplach.",
            "product_button": "Pozrieť prací gél",
            "category_title": "Vyberte pranie podľa času a znečistenia",
            "category_intro": "Krátky program je nástroj, nie hlavná stratégia prania. Pri výbere produktu myslite na dávku, program a oplach spolu.",
            "category_bullets": [
                ("Krátky program", "malá náplň, nízka dávka a ľahké znečistenie."),
                ("Bežný program", "lepší pri pote, uterákoch a obliečkach."),
                ("Opakovaný zápach", "riešte aj práčku, nie iba produkt."),
            ],
            "category_href": "/c/vevo-home-care/pranie/praci-gel",
            "category_button": "Pozrieť pracie gély",
        },
        "related": [
            ("Predpieranie v práčke", "/n/predpieranie-v-pracke-kedy-ma-zmysel-a-kedy-len-mina-vodu-cas-a-praci-prostriedok"),
            ("Ako funguje prací gél", "/n/ako-funguje-praci-gel-tenzidy-enzymy-ph-a-davkovanie-pri-beznom-prani"),
            ("Prečo oblečenie zapácha po praní", "/n/preco-moje-oblecenie-zapacha-po-prani-priciny-a-riesenia"),
            ("Preplnená práčka", "/n/preplnena-pracka-preco-sa-bielizen-nevyperie-neoplachne-a-zapacha"),
        ],
        "faq": [
            ("Je 15-minútový program hygienický?", "Na hygienu a silnejšie znečistenie sa nespoliehajte na veľmi krátky program. Použite program vhodný pre materiál a situáciu."),
            ("Koľko gélu dať pri krátkom programe?", "Zvyčajne menej než pri plnej bežnej dávke. Riaďte sa obalom, náplňou a tvrdosťou vody."),
            ("Môžem prať uteráky na krátkom programe?", "Ako núdzové osvieženie možno, ale ako rutina to nie je vhodné. Uteráky potrebujú dôkladnejší program a oplach."),
            ("Prečo bielizeň po rýchlom praní zapácha?", "Najčastejšie pre pot, maz, preplnený bubon, priveľa gélu alebo slabé sušenie."),
        ],
    },
    {
        "title": "Koľko bielizne dať do práčky: praktická kapacita podľa uterákov, obliečok a športu",
        "short": "Do práčky nedávajte toľko bielizne, koľko sa fyzicky zmestí. Praktická kapacita závisí od programu a typu textilu: uteráky, obliečky a športové veci potrebujú viac priestoru než tenké tričká.",
        "queries": [
            "koľko bielizne dať do práčky",
            "koľko uterákov do práčky",
            "koľko obliečok do práčky",
            "koľko športového oblečenia do práčky",
            "ako zistiť preplnenú práčku",
            "plný bubon a zápach bielizne",
            "kg bielizne v praxi",
            "ruka nad prádlom v bubne",
            "prečo sa bielizeň nevyperie",
            "preplnená práčka a zvyšky gélu",
        ],
        "quick": [
            "<strong>Fyzicky plný bubon je zvyčajne priveľa.</strong> Textil potrebuje priestor na pohyb, vodu a oplach.",
            "<strong>Uteráky perte v menšej dávke.</strong> Sú objemné, savé a ľahko držia zvyšky prostriedku.",
            "<strong>Obliečky zapnite a nepreplňte.</strong> Inak sa môžu zamotať do veľkej mokrej gule.",
            "<strong>Športové veci perte radšej menšiu dávku.</strong> Pot a maz sa musia dostať z vlákien preč.",
        ],
        "intro": [
            "Kapacita práčky v kilogramoch vyzerá presne, ale v bežnej domácnosti často mätie. Sedem kilogramov tenkých tričiek nie je to isté ako sedem kilogramov mokrých uterákov, obliečok alebo mikín. Každý textil má iný objem, savosť a potrebu pohybu v bubne.",
            "Ak bielizeň po praní nevonia, ostáva tvrdá, má šmuhy alebo je po programe zvláštne zlepená, príčina môže byť jednoduchá: v bubne bolo priveľa vecí. Viac pracieho gélu tento problém nezachráni. Naopak, často pridá ďalšie zvyšky.",
            "Praktická otázka preto neznie len koľko kilogramov bielizne má práčka, ale koľko priestoru potrebuje konkrétny typ textilu, aby sa vypral, opláchol a následne normálne vysušil.",
        ],
        "why": [
            "Pranie funguje vďaka kombinácii vody, pracieho roztoku, času a mechanického pohybu. Keď je bubon nadoraz, textil sa nehýbe voľne, prací roztok sa nedostane všade a oplach má menšiu šancu vytiahnuť zvyšky prostriedku.",
            "Objemné textílie majú vlastnú dynamiku. Obliečky sa môžu zamotať, uteráky nasiaknu veľa vody a športové oblečenie môže držať pot a maz vo vláknach. Preto rovnaká práčka potrebuje rozdielne plnenie pri rôznych dávkach.",
        ],
        "rows": [
            ("Tenké tričká", "bežná náplň s voľným priestorom", "neutláčať bubon nadoraz"),
            ("Uteráky", "menšia dávka", "froté potrebuje vodu a oplach"),
            ("Obliečky", "zapnúť zipsy, nepreplniť", "veľké kusy sa zvyknú zamotať"),
            ("Športové veci", "menšia a oddelená dávka", "pot a maz sa ľahšie vypláchnu"),
            ("Rifle a mikiny", "nemiešať s jemnými kusmi", "ťažké kusy zvyšujú trenie"),
        ],
        "steps": [
            "Najprv rozdeľte dávku podľa typu textilu: uteráky, obliečky, šport a bežné oblečenie zvlášť.",
            "Bubon naplňte tak, aby textil nebol natlačený a mal priestor sa prevracať.",
            "Veľké kusy zapnite a rozložte, aby sa nezamotali do jedného mokrého balíka.",
            "Pri uterákoch a športe dávku radšej zmenšite a nepridávajte viac gélu.",
            "Ak je bielizeň po praní zle vypratá, zopakujte pranie v menšej dávke a skontrolujte oplach.",
        ],
        "decision_rows": [
            ("Do bubna sa ešte zmestí jeden uterák", "radšej ho nechajte do ďalšej dávky", "uterák výrazne zvýši objem a vlhkosť"),
            ("Obliečky sa zamotali", "prať menej veľkých kusov naraz", "zlepší sa pohyb aj odstreďovanie"),
            ("Šport smrdí po vysušení", "prať menšiu dávku s dobrým oplachom", "pot sa musí vypláchnuť, nie prekryť"),
            ("Bielizeň má šmuhy", "znížiť náplň aj dávku produktu", "zvyšky sa nedostali von"),
        ],
        "mistakes": [
            "Riadiť sa iba tým, čo sa do bubna fyzicky zmestí.",
            "Doplniť ťažký uterák do takmer plnej dávky tričiek.",
            "Prať obliečky, uteráky a šport spolu, aby sa ušetril čas.",
            "Zvyšovať dávku gélu pri preplnení.",
            "Nechať veľkú mokrú dávku po praní dlho v zatvorenej práčke.",
        ],
        "expert": [
            "Výrobcovia práčok uvádzajú kapacitu pre konkrétne podmienky, no domáce dávky sú rôznorodé. Program pre bavlnu, jemné pranie, rýchly program a športové oblečenie nemusia mať rovnakú odporúčanú náplň. Preto má zmysel čítať návod práčky a sledovať reálny výsledok.",
            "Pri preplnení klesá účinnosť mechanického pohybu aj oplachu. Následky sa často ukážu až po vysušení: pach, tvrdosť, šmuhy, pokrčenie alebo pocit, že textil nie je skutočne čistý.",
        ],
        "sources": [
            ("Energy Star: Clothes washers", "https://www.energystar.gov/products/clothes_washers"),
            ("Energy.gov: Laundry", "https://www.energy.gov/energysaver/laundry"),
            ("FTC: Care Labeling Rule guidance", "https://www.ftc.gov/business-guidance/resources/clothes-captioning-complying-care-labeling-rule"),
        ],
        "sales": {
            "heading": "Riešenie pre menšiu dávku a lepší oplach",
            "intro": "Pri správnej veľkosti náplne prací prostriedok konečne funguje tak, ako má. Menšia dávka často vyperie lepšie než preplnený bubon s väčším množstvom gélu.",
            "product_name": "Prací gél hypoalergénny z Marseillského mydla 1L",
            "product_href": "/p-1551/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l",
            "fit": "pri bežných dávkach oblečenia, obliečok a domácich textílií, keď chcete jasne pracovať s dávkou podľa náplne.",
            "boundary": "ak je bubon preplnený, nepomôže viac produktu. Najprv rozdeľte dávku a zlepšite pohyb textilu v bubne.",
            "product_button": "Pozrieť prací gél",
            "category_title": "Vyberte riešenie podľa typu dávky",
            "category_intro": "Pri plnení práčky myslite na textil, nie iba na kilogramy. Iný režim potrebuje froté uterák, iný športové tričko a iný obliečka.",
            "category_bullets": [
                ("Uteráky", "menej kusov a dôkladný oplach."),
                ("Obliečky", "zapnúť, netlačiť a dobre vysušiť."),
                ("Šport", "menšia dávka, aby sa vypláchol pot a maz."),
            ],
            "category_href": "/c/vevo-home-care/pranie/praci-gel",
            "category_button": "Pozrieť pracie gély",
        },
        "related": [
            ("Preplnená práčka", "/n/preplnena-pracka-preco-sa-bielizen-nevyperie-neoplachne-a-zapacha"),
            ("Ako správne prať obliečky", "/n/ako-spravne-prat-obliecky-kompletny-sprievodca-starostlivostou"),
            ("Ako často prať posteľné prádlo", "/n/ako-casto-prat-postelne-pradlo"),
            ("Prečo uteráky zapáchajú aj po praní", "/n/preco-uteraky-zapachaju-aj-po-prani-zatuchnuty-pach-tvrdost-a-strata-savosti"),
        ],
        "faq": [
            ("Ako zistím, že je práčka preplnená?", "Ak je bubon natlačený a textil sa nemá kde prevracať, je plný priveľmi. Pri uterákoch a obliečkach nechajte ešte viac priestoru."),
            ("Koľko uterákov mám dať do práčky?", "Radšej menej, aby mali priestor na vodu a oplach. Presný počet závisí od veľkosti práčky a uterákov."),
            ("Môžem prať obliečky s uterákmi?", "Niekedy áno, ale často sa tým zhorší pohyb, odstreďovanie a sušenie. Veľké dávky radšej rozdeľte."),
            ("Pomôže viac gélu pri veľkej dávke?", "Nie spoľahlivo. Pri preplnení sa prostriedok horšie roznesie aj vypláchne."),
        ],
    },
]


def build_long(article):
    html = [
        f"<p><strong>Rýchla odpoveď:</strong> {escape(article['short'])}</p>",
        fanout_box(article["queries"]),
        callout("Rýchly praktický výber", article["quick"]),
    ]
    html.extend(f"<p>{escape(paragraph)}</p>" for paragraph in article["intro"])
    html.append("<h2>Prečo sa to deje</h2>")
    html.extend(f"<p>{escape(paragraph)}</p>" for paragraph in article["why"])
    html.append("<h2>Praktické rozhodovanie podľa situácie</h2>")
    html.append(table(["Situácia", "Čo urobiť", "Prečo"], article["rows"]))
    html.append("<h2>Postup krok za krokom</h2>")
    html.append("<ol>" + "".join(f"<li>{escape(step)}</li>" for step in article["steps"]) + "</ol>")
    html.append("<h2>Diagnostická tabuľka</h2>")
    html.append(table(["Príznak", "Prvý krok", "Dôvod"], article["decision_rows"]))
    html.append("<h2>Čomu sa vyhnúť</h2>")
    html.append("<ul>" + "".join(f"<li>{escape(item)}</li>" for item in article["mistakes"]) + "</ul>")
    html.append("<h2>Odbornejší pohľad</h2>")
    html.extend(f"<p>{escape(paragraph)}</p>" for paragraph in article["expert"])
    html.append(sources(article["sources"]))
    html.append(sales_block(article["sales"]))
    html.append(related(article["related"]))
    html.append("<h2>FAQ</h2>")
    for question, answer in article["faq"]:
        html.append(f"<h3>{escape(question)}</h3><p>{escape(answer)}</p>")
    return "\n".join(html)


def check_links(articles):
    hrefs = sorted({href for row in articles for href in re.findall(r'href="([^"]+)"', row["long"])})
    checks = []
    for href in hrefs:
        if href.startswith("/"):
            url = urljoin(BASE, href)
        elif href.startswith("http"):
            url = href
        else:
            continue
        response = requests.get(
            url,
            headers={"User-Agent": f"Codex VEVO {BATCH} link preflight"},
            timeout=45,
            allow_redirects=True,
        )
        checks.append({"url": url, "status": response.status_code, "final_url": response.url, "ok": response.status_code == 200})
        if response.status_code != 200:
            raise SystemExit(f"Link preflight failed: {url} -> {response.status_code} {response.url}")
    return checks


def main():
    times = ["08:00:00", "08:12:00", "08:24:00", "08:36:00", "08:48:00"]
    articles = []
    for index, article in enumerate(ARTICLES):
        long = build_long(article)
        if re.search(r"\bCTA\b", long, re.IGNORECASE):
            raise SystemExit(f"Forbidden internal acronym in {article['title']}")
        if "Cena:" in long or "€" in long:
            raise SystemExit(f"Fixed price marker in {article['title']}")
        if len(long) > 32700:
            raise SystemExit(f"XLS cell too long for {article['title']}: {len(long)}")
        articles.append(
            {
                "title": article["title"],
                "short": article["short"],
                "long": long,
                "date_posted": BATCH_DATE,
                "time_posted": times[index],
                "active": 1,
                "link": slugify(article["title"]),
                "commenting": "none",
            }
        )

    checks = check_links(articles)
    OUT_JSON.write_text(json.dumps(articles, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    book = xlwt.Workbook(encoding="utf-8")
    sheet = book.add_sheet("news")
    headers = ["title", "short", "long", "date_posted", "time_posted", "active", "link", "commenting"]
    for col, header in enumerate(headers):
        sheet.write(0, col, header)
    for row_index, article in enumerate(articles, start=1):
        for col, header in enumerate(headers):
            sheet.write(row_index, col, article[header])
    OUT_XLS.parent.mkdir(parents=True, exist_ok=True)
    book.save(str(OUT_XLS))

    print(
        json.dumps(
            {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "batch": BATCH,
                "article_count": len(articles),
                "json": str(OUT_JSON),
                "xls": str(OUT_XLS),
                "links_checked": len(checks),
                "lengths": {article["title"]: len(article["long"]) for article in articles},
                "slugs": [article["link"] for article in articles],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
