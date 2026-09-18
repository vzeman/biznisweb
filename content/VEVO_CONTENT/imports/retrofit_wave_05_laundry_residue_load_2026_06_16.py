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
OUT_JSON = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-05-laundry-residue-load-2026-06-16.json"
MCP_RESULTS = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-05-laundry-residue-load-2026-06-16-mcp-results.json"


ARTICLES = [
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-26-2026-06-16-quality-update.json",
        "slug": "preco-je-bielizen-po-prani-tvrda-alebo-lepkava-zvysky-gelu-davkovanie-a-oplach",
        "post_id": "2259",
        "url": "https://www.vevo.sk/n/preco-je-bielizen-po-prani-tvrda-alebo-lepkava-zvysky-gelu-davkovanie-a-oplach",
        "title": "Prečo je bielizeň po praní tvrdá alebo lepkavá: zvyšky gélu, dávkovanie a oplach",
        "expansion": "hard_sticky_laundry",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-27-2026-06-16-articles.json",
        "slug": "ako-davkovat-praci-gel-podla-tvrdosti-vody-naplne-a-znecistenia",
        "post_id": "2260",
        "url": "https://www.vevo.sk/n/ako-davkovat-praci-gel-podla-tvrdosti-vody-naplne-a-znecistenia",
        "title": "Ako dávkovať prací gél podľa tvrdosti vody, náplne a znečistenia",
        "expansion": "gel_dosing",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-26-2026-06-16-quality-update.json",
        "slug": "preplnena-pracka-preco-sa-bielizen-nevyperie-neoplachne-a-zapacha",
        "post_id": "2258",
        "url": "https://www.vevo.sk/n/preplnena-pracka-preco-sa-bielizen-nevyperie-neoplachne-a-zapacha",
        "title": "Preplnená práčka: prečo sa bielizeň nevyperie, neopláchne a zapácha",
        "expansion": "overloaded_washer",
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
    "hard_sticky_laundry": clean(
        f"""
        <h2>Rozlíšte tvrdosť, lepkavosť a zatuchnutie</h2>
        <p>Tvrdá bielizeň a lepkavá bielizeň nie sú ten istý problém. Tvrdosť často súvisí s tvrdou vodou, froté vláknom, rýchlym sušením na radiátore alebo zvyškami minerálov. Lepkavosť skôr naznačuje priveľa pracieho gélu, slabý oplach, preplnený bubon alebo príliš krátky program. Zatuchnutie zas zvyčajne smeruje k vlhkosti, pomalému sušeniu alebo nečistotám v práčke.</p>
        <p>Najhoršie výsledky vznikajú vtedy, keď sa všetky príčiny miešajú. Bielizeň je natlačená v bubne, použije sa veľa gélu, program je krátky a sušenie trvá dlho. Vtedy nepomôže pridať ďalšiu vôňu. Treba najprv zistiť, či vo vláknach ostal prací prostriedok, či je problém v tvrdej vode alebo či bielizeň zatuchla až po praní.</p>
        {note_card("Domáci test na jednu dávku", [
            "<strong>Ak je textil lepkavý:</strong> vyperte menšiu dávku bez aviváže, s nižšou dávkou gélu a extra oplachom.",
            "<strong>Ak je textil tvrdý:</strong> sledujte tvrdosť vody, spôsob sušenia a to, či nejde najmä o uteráky.",
            "<strong>Ak sa vracia zápach:</strong> skontrolujte práčku, kôš na bielizeň a dĺžku sušenia.",
            "<strong>Ak sú viditeľné mapy:</strong> znížte dávku, nepreplňte bubon a nepoužívajte príliš krátky program."
        ])}
        <h2>Diagnostika podľa pocitu z bielizne</h2>
        {table(["Príznak", "Pravdepodobná príčina", "Prvý krok"], [
            ("Tvrdé uteráky", "tvrdá voda, veľa zvyškov alebo zlé sušenie", "menšia dávka, dôkladný oplach, nepreplniť bubon"),
            ("Lepkavé tričko", "prebytok gélu alebo slabý oplach", "preprať s menšou dávkou a extra oplachom"),
            ("Silná vôňa po praní", "príliš veľa produktu vo vláknach", "znížiť dávkovanie a sledovať výsledok po vysušení"),
            ("Zatuchnutá bielizeň", "pomalé sušenie alebo špinavá práčka", "vybrať hneď po praní, vetrať a skontrolovať spotrebič"),
            ("Šmuhy na tmavom oblečení", "nerozptýlený alebo nevypláchnutý prostriedok", "menšia náplň, vhodný program a presnejšia dávka"),
        ])}
        <h2>Prečo problém často vzniká až po vysušení</h2>
        <p>Po otvorení práčky môže bielizeň pôsobiť v poriadku, pretože je vlhká a vôňa pracieho prostriedku je výrazná. Skutočný výsledok sa ukáže až po vysušení. Ak sú vlákna po vysušení tvrdé, ťažké alebo lepkavé, prací proces nebol vyvážený. Vlhkosť zamaskovala zvyšky, ktoré sa neskôr prejavili na dotyk.</p>
        <p>Pri uterákoch je dôležitá aj savosť. Ťažký povlak z pracieho prostriedku alebo nevhodne používanej aviváže môže spôsobiť, že uterák síce vonia, ale horšie saje a pôsobí tvrdšie. Preto pri uterákoch riešte najprv dávkovanie, oplach a priestor v bubne. Vôňa a mäkší pocit majú prísť až po odstránení príčiny.</p>
        <h2>Ako nastaviť opravnú dávku</h2>
        <p>Ak je bielizeň lepkavá alebo prevoňaná, neperte ju znovu s plnou dávkou prostriedku. Zvoľte menšiu náplň, primeraný program a podľa potreby extra oplach. Pri tvrdosti skúste oddeliť uteráky od bežného oblečenia a sledujte, či problém ostáva iba pri froté alebo pri všetkých materiáloch.</p>
        <p>Ak sa stav po jednej opravnej dávke výrazne zlepší, problém bol pravdepodobne v dávkovaní, oplachu alebo preplnení. Ak sa nezlepší, hľadajte ďalej: tvrdosť vody, zanesený zásobník, filter, tesnenie, pomalé sušenie alebo nevhodný program pre daný textil.</p>
        <div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 24px 0; background: #f7fbf8;">
        <h2 style="margin-top: 0;">Keď chcete mäkší pocit bez prekrývania problému</h2>
        <p>Najprv odstráňte zvyšky gélu a zlepšite oplach. Až potom má zmysel riešiť jemnejší pocit pri uterákoch alebo domácich textíliách.</p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="/p-1626/prava-octova-avivaz-lesna-zmes-1l">Pozrieť octovú aviváž</a></p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="/c/vevo-home-care/pranie/avivaz/octova-avivaz">Pozrieť octové aviváže</a></p>
        </div>
        <h2>Jednoduché pravidlo pre ďalšie pranie</h2>
        <p>Ak je bielizeň lepkavá, uberte produkt a pridajte oplach. Ak je tvrdá, riešte vodu, typ textilu, sušenie a zvyšky vo vláknach. Ak zapácha, skontrolujte aj práčku a sušenie. Jedna univerzálna oprava neexistuje, ale dobré dávkovanie, nepreplnený bubon a rýchle sušenie vyriešia väčšinu domácich prípadov.</p>
        """
    ),
    "gel_dosing": clean(
        f"""
        <h2>Dávkovanie nie je len ryska na vrchnáku</h2>
        <p>Ryska na uzávere alebo odporúčanie na obale je dobrý začiatok, nie automatická odpoveď pre každú dávku. Rovnaké množstvo pracieho gélu sa správa inak pri malej dávke tričiek, plnom bubne uterákov, tvrdej vode alebo športovom oblečení po tréningu. Preto dávku nastavujte podľa troch vecí naraz: tvrdosť vody, veľkosť náplne a úroveň znečistenia.</p>
        <p>Najčastejšia chyba je liať gél od oka. Pri mäkkej vode a malej dávke môže byť aj bežné množstvo priveľa. Pri tvrdej vode a silnejšej špine zas nestačí iba pridať produkt, ak je bubon preplnený alebo program krátky. Dávkovanie preto vždy posudzujte spolu s programom a oplachom.</p>
        {note_card("Tri otázky pred naliatím gélu", [
            "<strong>Aká veľká je náplň?</strong> Polovičný bubon nepotrebuje dávku ako plný bubon.",
            "<strong>Aká je voda?</strong> Tvrdšia voda môže meniť výsledok aj pocit z textilu.",
            "<strong>Aká je špina?</strong> Pot a bežné nosenie sú iné než blato, mastnota alebo zatuchnutie.",
            "<strong>Aký je program?</strong> Krátky program má menej času na rozptýlenie a oplach."
        ])}
        <h2>Praktická tabuľka dávkovania podľa situácie</h2>
        {table(["Situácia", "Ako uvažovať o dávke", "Čo sledovať po praní"], [
            ("Malá dávka bežného oblečenia", "skôr spodná hranica odporúčania", "či textil nie je príliš voňavý alebo lepkavý"),
            ("Plnší bubon uterákov", "dávku nepridávať naslepo, nechať priestor", "tvrdosť, savosť a dĺžku sušenia"),
            ("Tvrdšia voda", "riadiť sa obalom a miestnou tvrdosťou vody", "šednutie, tvrdý pocit a zvyšky vo vláknach"),
            ("Športové oblečenie", "menšia náplň a vhodný program sú dôležitejšie než veľa gélu", "návrat pachu po zahriatí tela"),
            ("Krátky program", "dávku znížiť a prať len ľahké veci", "šmuhy, lepkavosť a slabý oplach"),
        ])}
        <h2>Test zníženia dávky na tri prania</h2>
        <p>Ak máte pocit, že bielizeň po praní lepí, silno vonia alebo je ťažká na dotyk, skúste tri bežné prania s nižšou dávkou. Nemeňte naraz všetko. Nechajte podobný program, podobnú náplň a sledujte rozdiel po vysušení. Ak sa pocit zlepší, predtým ste pravdepodobne dávkovali viac, než bolo potrebné.</p>
        <p>Pri teste je dôležité nepreplniť bubon. Ak znížite dávku, ale necháte bubon natlačený, výsledok nemusí ukázať skutočný problém. Textil sa musí hýbať, voda musí prechádzať vláknami a oplach musí mať priestor odviesť zvyšky von.</p>
        <h2>Dávkovanie pri uterákoch, obliečkach a športovom oblečení</h2>
        <p>Uteráky a obliečky sú objemné, preto pri nich rozhoduje najmä priestor. Veľa gélu v plnom bubne môže zhoršiť oplach a predĺžiť sušenie. Pri športovom oblečení zas problém často nie je nedostatok gélu, ale pot usadený v syntetických vláknach, krátky program alebo odloženie vlhkých vecí do tašky.</p>
        <p>Pri týchto typoch textilu je lepšie rozdeliť dávku a zvoliť vhodný program, než zvyšovať množstvo produktu. Ak sa pach alebo lepkavosť opakuje, spojte dávkovanie s ďalšími kontrolami: extra oplach, menšia náplň, čistenie zásobníka a rýchle sušenie po praní.</p>
        <div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 24px 0; background: #f7fbf8;">
        <h2 style="margin-top: 0;">Prací gél dávkujte podľa reálnej dávky, nie podľa zvyku</h2>
        <p>Pri bežnom praní je praktický tekutý prací základ, ktorý viete upraviť podľa náplne a programu. Najväčší rozdiel však robí presnosť dávkovania.</p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="/p-1551/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l">Pozrieť prací gél</a></p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="/c/vevo-home-care/pranie/praci-gel">Pozrieť pracie gély</a></p>
        </div>
        <h2>Jednoduché pravidlo pre dávkovanie</h2>
        <p>Ak je bielizeň čistá, ale príliš silno vonia, lepí alebo je tvrdá, skúste ubrať. Ak je bielizeň stále špinavá, najprv skontrolujte program, náplň a predčistenie. Viac gélu má zmysel len vtedy, keď naozaj zodpovedá vode, množstvu bielizne a znečisteniu.</p>
        """
    ),
    "overloaded_washer": clean(
        f"""
        <h2>Preplnenie je mechanický problém, nie problém vône</h2>
        <p>V preplnenej práčke sa bielizeň nehýbe tak, ako má. Textil sa namiesto prevracania tlačí do seba, voda cezň neprechádza rovnomerne a prací roztok sa nedostane ku každému kusu. Preto môže byť výsledkom bielizeň, ktorá vonia po prostriedku, ale nie je dobre vypraná ani vypláchnutá.</p>
        <p>Pri preplnení nepomôže pridať viac gélu ani silnejšiu vôňu. Práve naopak, viac produktu sa v natlačenom bubne horšie oplachuje. Zvyšky potom ostávajú vo vláknach, bielizeň je ťažšia, lepkavá, tvrdá alebo po vysušení zatuchne. Riešením je menšia dávka, správny program a lepšie rozdelenie textilu.</p>
        {note_card("Rýchly test pred spustením programu", [
            "<strong>Ruka nad bielizňou:</strong> pri bežnom oblečení by mala vojsť bez tlačenia.",
            "<strong>Uteráky a obliečky:</strong> nechajte viac priestoru než pri tričkách.",
            "<strong>Veľké kusy:</strong> zapnite zipsy a nedovoľte, aby z nich vznikol vak.",
            "<strong>Športové veci:</strong> perte radšej menšiu dávku s dobrým oplachom."
        ])}
        <h2>Koľko priestoru nechať podľa textilu</h2>
        {table(["Textil", "Praktické pravidlo", "Prečo"], [
            ("Bežné tričká", "nechať priestor na pohyb a prevracanie", "mechanika pomáha uvoľniť pot a špinu"),
            ("Uteráky", "neplniť nadoraz", "po nasiaknutí sú ťažké a potrebujú oplach"),
            ("Obliečky", "prať s priestorom a zapnutými zipsami", "veľké kusy môžu uzavrieť menšie prádlo"),
            ("Športové oblečenie", "menšia dávka, nie plný bubon", "syntetika potrebuje dobrý oplach potu aj gélu"),
            ("Pracovné veci", "oddeliť od bežného prania", "hrubá špina a prach zaťažia práčku"),
        ])}
        <h2>Čo sa deje s oplachom v plnom bubne</h2>
        <p>Oplach potrebuje vodu a priestor. Keď je bubon natlačený, voda sa nedostane rovnomerne medzi vrstvy textilu a zvyšky pracieho prostriedku ostávajú v niektorých kusoch. Preto sa problém často prejaví nerovnomerne: jedno tričko je v poriadku, druhé lepí, uterák je tvrdý a obliečka je v rohoch stále vlhká.</p>
        <p>Ak sa to deje opakovane, neriešte iba poslednú dávku. Pozrite sa na celý systém: koľko bielizne dávate do bubna, či dávkujete gél podľa náplne, aký program používate a ako rýchlo bielizeň sušíte. Preplnenie je často prvý krok reťazca, ktorý končí zápachom.</p>
        <h2>Ako rozdeliť veľkú dávku prakticky</h2>
        <p>Najjednoduchšie pravidlo je oddeliť objemné textílie. Uteráky perte ako samostatnú dávku, obliečky nespájajte automaticky s ťažkými osuškami a športové veci nenechávajte v plnom bubne s bežným oblečením. Tak sa zlepší pranie, oplach aj sušenie.</p>
        <p>Ak máte malú práčku, rozdiel je ešte výraznejší. Jedna sada obliečok alebo niekoľko veľkých uterákov môže vyplniť praktickú kapacitu skôr, než sa bubon opticky zdá plný. Pri pochybnosti uberte. Menšia dávka často ušetrí opakované pranie a dlhé sušenie.</p>
        <div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 24px 0; background: #f7fbf8;">
        <h2 style="margin-top: 0;">Ak sa zápach drží aj v práčke</h2>
        <p>Keď sa po opakovane preplnených dávkach drží zápach v bubne, tesnení alebo zásobníku, riešte aj hygienu práčky, nielen ďalšiu dávku bielizne.</p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="/p-1549/vevo-shot-koncentrat-na-cistenie-pracky">Pozrieť čistič práčky</a></p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="/c/vevo-home-care/pranie/detox-pracky">Pozrieť detox práčky</a></p>
        </div>
        <h2>Jednoduché pravidlo pre ďalšie pranie</h2>
        <p>Ak musíte bielizeň do bubna zatláčať, dávka je príliš veľká. Ak po programe ostáva bielizeň nerovnomerne mokrá, pokrčená do tvrdých záhybov alebo zapácha, najprv znížte náplň. Až potom riešte dávku gélu, extra oplach alebo čistenie práčky.</p>
        """
    ),
}


MARKERS = {
    "hard_sticky_laundry": "Rozlíšte tvrdosť, lepkavosť a zatuchnutie",
    "gel_dosing": "Dávkovanie nie je len ryska na vrchnáku",
    "overloaded_washer": "Preplnenie je mechanický problém, nie problém vône",
}


EXTRA_SECTIONS = {
    "hard_sticky_laundry": clean(
        """
        <h2>Kontrola pri ďalšom praní rovnakej bielizne</h2>
        <p>Pri ďalšej podobnej dávke si zapamätajte tri veci: koľko produktu ste použili, ako plný bol bubon a ako rýchlo bielizeň vyschla. Ak sa tvrdosť alebo lepkavosť zlepší už po menšej dávke a lepšom oplachu, máte jasný signál, že problém nebol v chýbajúcej vôni ani v slabom produkte, ale v nerovnováhe celého pracieho procesu.</p>
        """
    ),
    "gel_dosing": clean(
        """
        <h2>Keď dávku meníte, meňte iba jednu vec naraz</h2>
        <p>Ak chcete zistiť, či dávkujete správne, nemeňte naraz gél, program, teplotu aj veľkosť náplne. Najprv upravte iba množstvo gélu a nechajte podobnú dávku bielizne. Pri ďalšom praní môžete upraviť veľkosť náplne alebo pridať oplach. Tak zistíte, ktorá zmena reálne pomohla.</p>
        """
    ),
    "overloaded_washer": clean(
        """
        <h2>Kontrola po skončení programu</h2>
        <p>Po praní sa pozrite, či bielizeň nevychádza z bubna v jednej ťažkej hrude. Ak sú obliečky skrútené, uteráky nerovnomerne mokré alebo tričká pokrčené do tvrdých záhybov, dávka sa počas prania pravdepodobne nehýbala dostatočne. Pri ďalšom praní rovnakého typu textilu uberte množstvo skôr, než začnete meniť produkt alebo program.</p>
        """
    ),
}


EXTRA_MARKERS = {
    "hard_sticky_laundry": "Kontrola pri ďalšom praní rovnakej bielizne",
    "gel_dosing": "Keď dávku meníte, meňte iba jednu vec naraz",
    "overloaded_washer": "Kontrola po skončení programu",
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
    if MARKERS[key] not in long:
        index = insertion_index(long)
        long = long[:index].rstrip() + "\n" + EXPANSIONS[key] + "\n" + long[index:].lstrip()
    if EXTRA_MARKERS[key] not in long:
        target = "<h2>Jednoduché pravidlo"
        index = long.find(target)
        if index == -1:
            index = insertion_index(long)
        long = long[:index].rstrip() + "\n" + EXTRA_SECTIONS[key] + "\n" + long[index:].lstrip()
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
    parser = argparse.ArgumentParser(description="Conservatively expand VEVO laundry residue/load wave 05.")
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
                "wave": "retrofit-wave-05-laundry-residue-load",
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
                "wave": "retrofit-wave-05-laundry-residue-load",
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
