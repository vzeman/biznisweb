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
SOURCE = ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-27-2026-06-16-articles.json"
OUT_JSON = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-03-laundry-programs-2026-06-16.json"
MCP_RESULTS = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-03-laundry-programs-2026-06-16-mcp-results.json"


ARTICLES = [
    {
        "slug": "kratky-program-v-pracke-kedy-staci-a-kedy-zhorsuje-zvysky-pracieho-prostriedku",
        "post_id": "2263",
        "url": "https://www.vevo.sk/n/kratky-program-v-pracke-kedy-staci-a-kedy-zhorsuje-zvysky-pracieho-prostriedku",
        "title": "Krátky program v práčke: kedy stačí a kedy zhoršuje zvyšky pracieho prostriedku",
        "expansion": "short_program",
    },
    {
        "slug": "kolko-bielizne-dat-do-pracky-prakticka-kapacita-podla-uterakov-obliecok-a-sportu",
        "post_id": "2264",
        "url": "https://www.vevo.sk/n/kolko-bielizne-dat-do-pracky-prakticka-kapacita-podla-uterakov-obliecok-a-sportu",
        "title": "Koľko bielizne dať do práčky: praktická kapacita podľa uterákov, obliečok a športu",
        "expansion": "washer_capacity",
    },
    {
        "slug": "extra-oplach-v-pracke-kedy-pomoze-pri-zapachu-tvrdej-bielizni-a-citlivej-pokozke",
        "post_id": "2262",
        "url": "https://www.vevo.sk/n/extra-oplach-v-pracke-kedy-pomoze-pri-zapachu-tvrdej-bielizni-a-citlivej-pokozke",
        "title": "Extra oplach v práčke: kedy pomôže pri zápachu, tvrdej bielizni a citlivej pokožke",
        "expansion": "extra_rinse",
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
    "short_program": clean(
        f"""
        <h2>Kontrolný postup pred zapnutím krátkeho programu</h2>
        <p>Krátky program má zmysel len vtedy, keď sú splnené tri podmienky naraz: bielizeň je málo nosená, náplň je malá a nejde o textil, ktorý potrebuje dlhší kontakt s vodou. Ak je oblečenie spotené, mastné, zatuchnuté alebo veľmi nasiaknuté, krátky program často problém iba posunie do ďalšieho prania.</p>
        <p>Pred zapnutím krátkeho programu sa pozrite na náplň prakticky. Dve tričká po krátkom nosení sú iná situácia než uteráky po sprche, športové legíny alebo obliečky. Krátky čas znamená menej priestoru na uvoľnenie nečistôt, rozptýlenie pracieho prostriedku aj oplach. Preto je pri rýchlom praní dôležitejšie dávkovať menej a nepreplniť bubon.</p>
        {note_card("Kedy krátky program radšej nepoužiť", [
            "<strong>Uteráky a osušky:</strong> držia vlhkosť, maz a zvyšky kozmetiky, preto potrebujú dôkladnejší oplach.",
            "<strong>Športové oblečenie:</strong> pot a syntetické vlákna často potrebujú viac času než 15 alebo 30 minút.",
            "<strong>Obliečky:</strong> veľký objem textilu potrebuje priestor na pohyb a plákanie.",
            "<strong>Pracovné veci:</strong> prach, mastnota a blato nepatria do veľmi krátkeho režimu."
        ])}
        <h2>Rozhodovanie podľa času programu</h2>
        {table(["Typ programu", "Kedy môže stačiť", "Kedy je rizikový"], [
            ("15 minút", "jedno až dve málo nosené tričká", "uteráky, pot, škvrny, plný bubon"),
            ("30 minút", "ľahko nosené oblečenie bez škvŕn", "šport, obliečky, pracovné veci"),
            ("45 až 60 minút", "bežná malá dávka pri správnom dávkovaní", "veľmi špinavé alebo objemné dávky"),
            ("Bežný program", "dávky, kde záleží na oplachu a čistote", "nie je nutný pri každom jemnom osviežení"),
        ])}
        <h2>Dávkovanie pri krátkom programe</h2>
        <p>Pri krátkom programe je častá chyba naliať rovnaké množstvo gélu ako pri plnej dávke. Prostriedok sa nemusí stihnúť rovnomerne rozptýliť ani vypláchnuť. Výsledkom môže byť bielizeň, ktorá síce vonia, ale je na dotyk ťažšia, lepkavá alebo po vysušení zatuchne.</p>
        <p>Ak periete malú náplň na krátkom programe, začnite nižšou dávkou podľa obalu, tvrdosti vody a množstva bielizne. Ak sa problém so zvyškami opakuje, nie je riešením pridať viac produktu. Skôr treba predĺžiť program, zmenšiť náplň, pridať oplach alebo skontrolovať práčku.</p>
        <h2>Signály, že krátky program nestačil</h2>
        <p>Krátky program nestačil, ak bielizeň po vysušení cítiť zatuchnuto, športové oblečenie sa po zahriatí tela rýchlo rozvonia potom, uteráky sú tvrdé alebo v tmavom oblečení vidíte šmuhy po pracom prostriedku. Vtedy má zmysel zmeniť rutinu, nie iba pridať parfum do prania.</p>
        <p>Pri opakovanom probléme si jeden týždeň všímajte, čo periete na rýchlom programe. Ak sa tam dostávajú uteráky, pyžamá po chorobe, šport alebo väčšie dávky, krátky program používate na úlohu, na ktorú nie je vhodný. Lepší výsledok často prinesie bežný program s rozumnou dávkou pracieho gélu.</p>
        """
    ),
    "washer_capacity": clean(
        f"""
        <h2>Praktický test kapacity pred zatvorením dvierok</h2>
        <p>Kapacita práčky v kilogramoch neznamená, že každý typ textilu môžete natlačiť až po okraj bubna. Kilogram suchých uterákov sa v praní správa inak než kilogram tenkých tričiek. Uteráky nasajú veľa vody, obliečky sa vedia zamotať a športové veci potrebujú priestor, aby sa pot a prací prostriedok dobre vypláchli.</p>
        <p>Najjednoduchší domáci test je priestor nad bielizňou. Pri bežnom oblečení by ste mali vedieť vložiť ruku nad náplň bez tlačenia. Pri uterákoch, obliečkach a objemných textíliách nechajte ešte viac priestoru. Ak musíte bielizeň zatláčať, bubon nie je prakticky naplnený správne, aj keď sa dvierka ešte zavrú.</p>
        {note_card("Kedy dávku rozdeliť na dve", [
            "<strong>Bubon je natlačený:</strong> textil sa nemá kde prevracať a oplach bude slabší.",
            "<strong>Periete veľké obliečky:</strong> môžu vytvoriť vak, ktorý zadrží vodu aj menšie kusy.",
            "<strong>Uteráky sú ťažké už nasucho:</strong> po nasiaknutí zaťažia bubon výrazne viac.",
            "<strong>Športové veci zapáchajú aj po praní:</strong> často potrebujú menšiu dávku a lepší oplach."
        ])}
        <h2>Kapacita podľa typu textilu</h2>
        {table(["Textil", "Koľko priestoru nechať", "Prečo"], [
            ("Tenké tričká", "približne dlaň až ruka nad bielizňou", "ľahšie sa hýbu a oplachujú"),
            ("Uteráky", "viac voľného priestoru než pri tričkách", "nasávajú vodu a púšťajú vlákna"),
            ("Obliečky", "neprať natlačené, zapnúť zipsy", "môžu sa zamotať a zhoršiť odstreďovanie"),
            ("Športové oblečenie", "menšia dávka, dobrý oplach", "pot a syntetika sa pri preplnení horšie vyperú"),
        ])}
        <h2>Čo sa deje v preplnenom bubne</h2>
        <p>V preplnenom bubne sa bielizeň viac tlačí než prevracia. Voda a prací prostriedok sa nedostanú rovnomerne ku každému kusu a pri oplachu sa zvyšky horšie dostávajú von. Preto môže byť bielizeň po praní tvrdá, lepkavá, zatuchnutá alebo fľakatá, hoci ste použili kvalitný prací prostriedok.</p>
        <p>Preplnenie zároveň zhoršuje odstreďovanie. Veľké kusy sa môžu zhlukovať, práčka sa snaží vyvážiť bubon a program sa predlžuje alebo skončí s mokrejšou bielizňou. Mokrá bielizeň potom schne dlhšie a ľahšie zatuchne, najmä v malom byte alebo pri slabom vetraní.</p>
        <h2>Ako si nastaviť rutinu pre týždenné pranie</h2>
        <p>Najpraktickejšie je nerobiť dávky podľa toho, čo sa ešte zmestí, ale podľa typu textilu. Uteráky perte ako samostatnú dávku, obliečky nespájajte automaticky s ťažkými osuškami a športové oblečenie nedávajte do preplneného bubna s bežným prádlom. Tak sa zlepší pranie, oplach aj sušenie.</p>
        <p>Ak potrebujete prať veľa naraz, radšej rozdeľte dávku na dve menšie. Vyzerá to pomalšie, ale často ušetrí opakované pranie, zápach, tvrdé uteráky a dlhé sušenie. Pri pracom géli platí to isté: väčšia dávka bielizne neznamená automaticky liať viac a viac gélu. Rozhoduje priestor, voda, pohyb a oplach.</p>
        """
    ),
    "extra_rinse": clean(
        f"""
        <h2>Test, či extra oplach rieši správny problém</h2>
        <p>Extra oplach je užitočný vtedy, keď problémom sú zvyšky pracieho prostriedku v textile. Spoznáte to podľa toho, že bielizeň pôsobí lepkavo, je príliš výrazne parfumovaná, škrabe alebo sa na tmavých kusoch objavujú stopy po prostriedku. Ak však bielizeň zapácha po zatuchnutí, príčina môže byť v práčke, pomalom sušení alebo v preplnenom bubne.</p>
        <p>Pred zapnutím extra oplachu sa opýtajte, čo sa snažíte opraviť. Ak ste dali priveľa pracieho gélu, extra oplach môže pomôcť. Ak ste prali spotené športové veci na krátkom programe, problémom je skôr program a dávka. Ak zapácha samotná práčka, extra voda v závere nevyrieši filter, zásobník ani tesnenie.</p>
        {note_card("Kedy extra oplach dáva najväčší zmysel", [
            "<strong>Citlivejšia pokožka:</strong> najmä pri bielizni, ktorá je priamo na tele.",
            "<strong>Detské oblečenie:</strong> keď chcete znížiť zvyšky prostriedku vo vláknach.",
            "<strong>Uteráky:</strong> ak sú tvrdé, ťažké alebo príliš voňajú po praní.",
            "<strong>Prebytok gélu:</strong> keď ste omylom dávkovali viac, než bolo potrebné."
        ])}
        <h2>Extra oplach podľa typu bielizne</h2>
        {table(["Bielizeň", "Kedy pridať oplach", "Kedy hľadať inú príčinu"], [
            ("Detské body a spodná bielizeň", "pri citlivejšej pokožke alebo silnej vôni", "ak je vyrážka výrazná alebo pretrváva, neriešiť to len praním"),
            ("Uteráky", "pri tvrdosti, zvyškoch gélu alebo slabej savosti", "ak zapáchajú po sušení, riešiť aj sušenie a práčku"),
            ("Športové oblečenie", "pri šmuhách a zvyškoch prostriedku", "ak sa pach vracia po zahriatí tela, riešiť pot a program"),
            ("Posteľná bielizeň", "pri silnej vôni alebo pocite zvyškov", "ak je zatuchnutá, skontrolovať sušenie a skladovanie"),
        ])}
        <h2>Extra oplach a dávkovanie pracieho gélu</h2>
        <p>Extra oplach by nemal byť ospravedlnenie pre príliš vysoké dávkovanie. Ak pravidelne potrebujete ďalší oplach, najprv znížte množstvo gélu, skontrolujte veľkosť náplne a tvrdosť vody. Pri menšej dávke bielizne často stačí menej prostriedku, než človek naleje od oka.</p>
        <p>Pri citlivejšej pokožke má zmysel kombinovať šetrnejší produkt, primerané dávkovanie a dostatočný oplach. Samotný extra oplach nepomôže, ak bielizeň periete v preplnenom bubne alebo ak sa prostriedok nemá kde rovnomerne rozptýliť.</p>
        <h2>Kedy extra oplach nepoužívať ako prvú voľbu</h2>
        <p>Ak oblečenie zapácha po každom praní, začnite kontrolou práčky a programu. Pozrite zásobník, filter, tesnenie, množstvo bielizne v bubne a spôsob sušenia. Extra oplach môže odstrániť časť zvyškov, ale nevyrieši vlhký nános v spotrebiči ani bielizeň, ktorá schne dva dni v nevetranej miestnosti.</p>
        <p>Rovnako opatrne pri veľmi krátkych programoch. Ak bol hlavný program príliš krátky na typ bielizne, dodatočný oplach nie je vždy plná náhrada za správne pranie. Lepšie je zvoliť vhodnejší program, menšiu dávku a až potom podľa potreby pridať oplach.</p>
        """
    ),
}


TOP_UP = {
    "short_program": clean(
        """
        <h2>Príklady z domácnosti: kedy rýchle pranie zlyhá</h2>
        <p>Typický problém vzniká pri oblečení, ktoré navonok nevyzerá špinavo, ale nesie pot alebo vlhkosť. Športové tričko po tréningu, pyžamo po chorobe alebo tielko nosené celý deň môžu vyzerať relatívne čisto, no krátky program nemusí stačiť na odstránenie pachu. Pri syntetike sa to často prejaví až po ďalšom nosení, keď sa textil zahreje na tele.</p>
        <p>Iná situácia sú dva kusy oblečenia po krátkom nosení doma alebo v kancelárii. Tam môže rýchle pranie fungovať, ak je bubon takmer prázdny, dávka gélu je primeraná a textil sa hneď po programe dobre vysuší. Rozdiel teda nerobí iba počet minút na displeji, ale kombinácia znečistenia, objemu, materiálu a sušenia.</p>
        <h2>Kontrola po vysušení</h2>
        <p>Krátky program hodnotí veľa ľudí hneď po otvorení práčky, keď bielizeň ešte vonia pracím prostriedkom. Reálny výsledok však uvidíte až po vysušení. Ak textil po pár hodinách v skrini zatuchne alebo sa pri nosení rýchlo vráti pach potu, krátky program bol pre danú dávku slabý.</p>
        <p>Pri opakovanom probléme si nastavte pravidlo: krátky program používajte len na ľahké osvieženie, nie na riešenie potu, škvŕn, uterákov alebo posteľnej bielizne. Ak chcete ušetriť čas, často viac pomôže menšia dávka a správny bežný program než extrémne krátky cyklus s preplneným bubnom.</p>
        """
    ),
    "washer_capacity": clean(
        """
        <h2>Príklady rozdelenia prania počas týždňa</h2>
        <p>Praktická rutina môže vyzerať jednoducho: samostatne uteráky, samostatne obliečky, zvlášť šport a zvlášť bežné oblečenie. Nie preto, aby bolo prania viac, ale preto, aby každá dávka dostala správny pohyb, program a oplach. Keď sa všetko zmieša do jednej veľkej dávky, práčka síce pracuje, ale výsledok býva slabší.</p>
        <p>Ak máte malú práčku, rozdiel je ešte výraznejší. Jedna sada obliečok môže zabrať viac praktického priestoru než viacero tenkých tričiek. Dva veľké uteráky sa po nasiaknutí správajú ako ťažká dávka. Športové veci zas nepotrebujú veľký objem, ale potrebujú dobré rozloženie vo vode a dôkladné vypláchnutie potu aj gélu.</p>
        <h2>Kontrola výsledku po praní</h2>
        <p>Preplnenie spoznáte aj po programe. Bielizeň je nerovnomerne mokrá, niektoré kusy sú pokrčené do tvrdých záhybov, uteráky pôsobia ťažko alebo v obliečkach ostali menšie kúsky uzavreté ako vo vreci. To sú signály, že náplň sa počas prania nehýbala dostatočne.</p>
        <p>Ak sa to opakuje, neriešte problém iba vyššími otáčkami alebo väčšou dávkou pracieho prostriedku. Najprv zmenšite náplň. Práčka potrebuje priestor na mechanický pohyb a voda potrebuje priestor na oplach. Bez toho sa ani dobrý prací gél nedostane tam, kam má.</p>
        """
    ),
    "extra_rinse": clean(
        """
        <h2>Ako rozlíšiť zvyšky gélu od iného problému</h2>
        <p>Zvyšky pracieho prostriedku sa často prejavia pocitom na dotyk: bielizeň je ťažšia, hladká až lepkavá, príliš výrazne vonia alebo pri nosení dráždi. Zápach z vlhkosti je iný. Ten býva zatuchnutý, kyslastý alebo sa objaví až po pomalom sušení. Extra oplach pomáha najmä v prvej situácii, nie v každom type zápachu.</p>
        <p>Ak neviete, kde je problém, skúste jednu dávku vyprať s menším množstvom gélu, nepreplneným bubnom a zapnutým extra oplachom. Ak sa pocit zlepší, príčinou boli pravdepodobne zvyšky prostriedku alebo slabý oplach. Ak sa nezlepší, hľadajte problém v práčke, programe, sušení alebo v samotnom type znečistenia.</p>
        <h2>Extra oplach ako súčasť citlivejšej rutiny</h2>
        <p>Pri detskom oblečení, spodnej bielizni, uterákoch na tvár alebo posteľnej bielizni pre citlivejšiu pokožku môže byť extra oplach užitočný preventívne. Stále však platí, že najprv má byť správna dávka produktu. Ak sa pravidelne používa priveľa gélu alebo silná parfumácia, extra oplach len kompenzuje chybu v predchádzajúcom kroku.</p>
        <p>Pri citlivej pokožke sledujte aj ďalšie faktory: zloženie produktu, množstvo parfumácie, úplné vysušenie a skladovanie. Ak sa podráždenie opakuje alebo je výrazné, nejde už len o otázku pracieho programu a je rozumné riešiť to opatrnejšie.</p>
        """
    ),
}


FINAL_TOP_UP = {
    "short_program": clean(
        """
        <h2>Jednoduché pravidlo pre ďalšie pranie</h2>
        <p>Ak krátky program raz nevyšiel, nepoužívajte ho na rovnaký typ dávky znova bez zmeny. Buď zmenšite náplň, znížte dávku gélu, pridajte oplach alebo vyberte dlhší program. Inak sa bude opakovať rovnaký výsledok: bielizeň bude na prvý dojem vypraná, ale po vysušení alebo pri nosení sa problém vráti.</p>
        """
    ),
    "washer_capacity": clean(
        """
        <h2>Jednoduché pravidlo pre plnenie bubna</h2>
        <p>Ak váhate, či je dávka ešte v poriadku, uberte. Menšia dávka sa často vyperie lepšie, rýchlejšie vyschne a nebude potrebovať opakované pranie. Pri uterákoch, obliečkach a športovom oblečení je voľný priestor v bubne súčasťou prania, nie premrhaná kapacita. Výsledok býva stabilnejší aj pri bežnom domácom sušení.</p>
        """
    ),
    "extra_rinse": clean(
        """
        <h2>Jednoduché pravidlo pre ďalší oplach</h2>
        <p>Ak extra oplach pomôže, berte ho ako signál, že dávkovanie alebo veľkosť náplne treba upraviť. Ak nepomôže, nehľadajte riešenie v ďalšom a ďalšom oplachu. Vtedy sa oplatí skontrolovať práčku, program, sušenie a typ znečistenia.</p>
        """
    ),
}


FINAL_EXTRA = {
    "washer_capacity": clean(
        """
        <h2>Kontrola pri ďalšom praní rovnakej dávky</h2>
        <p>Ak ste dávku rozdelili a bielizeň bola po praní citeľne čistejšia, mäkšia alebo rýchlejšie suchá, problém bol pravdepodobne v praktickej kapacite bubna. Toto pozorovanie si zapamätajte pri ďalšom praní rovnakého typu textilu.</p>
        """
    ),
}


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
    marker_by_key = {
        "short_program": "Kontrolný postup pred zapnutím krátkeho programu",
        "washer_capacity": "Praktický test kapacity pred zatvorením dvierok",
        "extra_rinse": "Test, či extra oplach rieši správny problém",
    }
    top_marker_by_key = {
        "short_program": "Príklady z domácnosti: kedy rýchle pranie zlyhá",
        "washer_capacity": "Príklady rozdelenia prania počas týždňa",
        "extra_rinse": "Ako rozlíšiť zvyšky gélu od iného problému",
    }
    if marker_by_key[key] not in long:
        index = insertion_index(long)
        long = long[:index].rstrip() + "\n" + EXPANSIONS[key] + "\n" + long[index:].lstrip()
    if top_marker_by_key[key] not in long:
        index = insertion_index(long)
        long = long[:index].rstrip() + "\n" + TOP_UP[key] + "\n" + long[index:].lstrip()
    final_marker = {
        "short_program": "Jednoduché pravidlo pre ďalšie pranie",
        "washer_capacity": "Jednoduché pravidlo pre plnenie bubna",
        "extra_rinse": "Jednoduché pravidlo pre ďalší oplach",
    }[key]
    if final_marker not in long:
        index = insertion_index(long)
        long = long[:index].rstrip() + "\n" + FINAL_TOP_UP[key] + "\n" + long[index:].lstrip()
    if key in FINAL_EXTRA and "Kontrola pri ďalšom praní rovnakej dávky" not in long:
        index = insertion_index(long)
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
    parser = argparse.ArgumentParser(description="Conservatively expand VEVO laundry program retrofit wave 03.")
    parser.add_argument("--update-live", action="store_true")
    parser.add_argument("--sleep", type=float, default=1.5)
    args = parser.parse_args()

    rows = json.loads(SOURCE.read_text(encoding="utf-8"))
    updates = []
    for config in ARTICLES:
        for article in rows:
            if article.get("link") != config["slug"]:
                continue
            if article.get("title") != config["title"]:
                raise SystemExit(f"Title changed unexpectedly for {config['slug']}: {article.get('title')}")
            original_long = article["long"]
            original_short = article.get("short", "")
            article["long"] = insert_expansion(article["long"], config["expansion"])
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
                    "source_file": str(SOURCE.relative_to(ROOT)),
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

    SOURCE.write_text(json.dumps(rows, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    OUT_JSON.write_text(
        json.dumps(
            {
                "project": "VEVO_CONTENT",
                "wave": "retrofit-wave-03-laundry-programs",
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
                "wave": "retrofit-wave-03-laundry-programs",
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
