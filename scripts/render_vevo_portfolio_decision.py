"""Render a private decision memo from reproducible aggregate analysis outputs."""
from __future__ import annotations

import argparse
from decimal import Decimal
import html
import json
from pathlib import Path


def number(value, places=1, suffix=""):
    if value is None:
        return "n. a."
    rendered = f"{Decimal(str(value)):,.{places}f}".replace(",", " ").replace(".", ",")
    return rendered + suffix


def euros(value):
    return number(value, 2, " €")


def rate(value):
    return number(None if value is None else Decimal(str(value)) * 100, 1, " %")


def table(headers, rows):
    return ('<div class="table-wrap"><table><thead><tr>'
            + "".join(f"<th>{html.escape(str(x))}</th>" for x in headers)
            + "</tr></thead><tbody>"
            + "".join("<tr>" + "".join(f"<td>{html.escape(str(x))}</td>" for x in row)
                      + "</tr>" for row in rows) + "</tbody></table></div>")


def render(economics, retention):
    if economics["source_sha256"] != retention["source_sha256"]:
        raise ValueError("Decision inputs must use the same immutable source")
    periods = economics["periods"]
    if any(period["to"] != retention["end"] for period in periods.values()):
        raise ValueError("Decision inputs must use the same observation end")
    recent = periods["last_180_days"]
    remove_p = recent["remove"]["Premium"]
    remove_n = recent["remove"]["Natural"]
    money_scope = ("Financie zahŕňajú samostatné fľaše 200/500 ml a čisté parfumové sety. "
                   "Spoločné ceny a náklady zmiešaných setov sa nepriraďujú parfumu. "
                   "Návraty zahŕňajú aj overené parfumové zložky týchto setov. "
                   "Ďalšie produkty v košíku nikdy nevyradia nákup parfumu. Vzorky sa do porovnania rád nepočítajú.")
    chunks = ["""<!doctype html><html lang="sk"><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>VEVO — rozhodnutie o Natural a Premium</title>
<style>body{font:16px/1.65 system-ui,sans-serif;color:#18252b;background:#f5f7f7;margin:0}
main{max-width:1000px;margin:auto;padding:32px 24px 64px;background:#fff}h1{line-height:1.2}
h2{margin-top:2em;line-height:1.3}p{max-width:88ch}.table-wrap{overflow-x:auto}
table{border-collapse:collapse;width:100%;margin:18px 0}th,td{text-align:left;padding:10px 12px;
border-bottom:1px solid #d4dfe0;font-variant-numeric:tabular-nums}th{background:#edf3f3}
.muted{color:#52646c}.lead{font-size:1.12em}a{color:#155d72}li{margin:8px 0}
@media(max-width:600px){main{padding:20px 14px}th,td{padding:8px}}
@media print{body,main{background:white}main{max-width:none;padding:0}h2{break-after:avoid}}
</style><main><h1>Natural ponechať. Vyradenie Premium najprv overiť.</h1>""",
        f'<p class="muted">Dáta: {periods["all_time"]["from"]} až {retention["end"]}. '
        'Verejná ponuka overená 19. 10. 2026. Ide o rozhodovaciu analýzu, nie zmenu e-shopu.</p>',
        '<p class="lead">Dostupné nákupy nepotvrdzujú, že Premium prináša vyššiu spokojnosť. '
        'Natural má vyšší podiel návratov k tej istej rade aj vyšší príspevok z predaja. '
        'Premium však prináša kladný príspevok a má zákazníkov, pri ktorých nepoznáme ochotu prejsť na Natural.</p>',
        '<p>' + money_scope + '</p>',
        '<h2>Čo vieme o návratoch</h2>']
    rows = []
    for days in sorted(retention["windows"], key=int):
        window = retention["windows"][days]
        n, p = (window["all"][series] for series in ("Natural", "Premium"))
        rows.append([f"Tá istá rada do {days} dní", f'{n["same"]} / {n["customers"]} = {number(n["same_pct"], 1, " %")}',
                     f'{p["same"]} / {p["customers"]} = {number(p["same_pct"], 1, " %")}'])
        rows.append([f"Ktorákoľvek rada do {days} dní", number(n["any_pct"], 1, " %"), number(p["any_pct"], 1, " %")])
    chunks += [table(["Od prvého nákupu príslušnej rady", "Natural", "Premium"], rows),
        '<p>Každý má celé sledované okno. Prvý nákup príslušnej rady musí byť v spoločnom období pozorovaného predaja '
        'od prvého pozorovaného predaja Premium. Jeden človek môže byť v oboch skupinách. '
        'Návrat ku značke a opakovaný nákup Premium sú odlišné výsledky; prechod na Natural nie je vernosť Premium.</p>']
    rows = []
    labels = {"one_200_of_entered_series": "Jedna fľaša 200 ml danej rady",
              "one_500_of_entered_series": "Jedna fľaša 500 ml danej rady",
              "one_standalone_bottle_total_shared_scents": "Jedna samostatná fľaša, len tri spoločné vône"}
    for days in ("90", "180"):
        for key, label in labels.items():
            view = retention["windows"][days]["sensitivities"][key]
            rows.append([f"{label}, {days} dní", *[f'{view[s]["same"]}/{view[s]["customers"]} ({number(view[s]["same_pct"], 1, " %")})'
                                                      for s in ("Natural", "Premium")]])
    chunks += [table(["Kontrola veľkosti a prvého nákupu", "Natural", "Premium"], rows),
        '<p>V prvých dvoch riadkoch pre každé okno môže byť v objednávke aj druhá rada. '
        'Kontrola troch spoločných vôní vyžaduje presne jednu fľašu celkovo; ostatný tovar v košíku je povolený. '
        'Následný nákup môže mať ktorúkoľvek vôňu v tej istej rade. Ani tieto kontroly nenahrádzajú experiment.</p>']
    intervals = []
    for days in ("90", "180"):
        diff = retention["windows"][days]["difference"]
        bounds = diff["ci95_percentile_pp"]
        intervals.append(f'{days} dní: {number(diff["difference_pp"], 2)} percentuálneho bodu '
                         f'(orientačný 95 % interval {number(bounds[0], 2)} až {number(bounds[1], 2)})')
    chunks += ['<p>Rozdiel Natural mínus Premium pri všetkých kupujúcich: ' + '; '.join(intervals)
               + '. Intervaly zo vzorkovania celých zákazníkov zachovávajú prekrývanie skupín. '
               'Opisujú nákupné podiely, neodstraňujú rozdiely v cene, dostupnosti alebo zákazníkoch. '
               'Pri 180 dňoch interval prakticky zahŕňa nulu.</p>']
    both = retention["post_both"]["all_observed"]
    chunks += [f'<p>Z {both["customers"]} zákazníkov, ktorí kúpili obe rady, urobilo po tomto bode ďalší '
               f'nákup parfumu {both["returned"]}. Ich najbližšia objednávka obsahovala len Natural '
               f'{both["next"].get("Natural", 0)}-krát, len Premium {both["next"].get("Premium", 0)}-krát '
               f'a obe rady {both["next"].get("Both", 0)}-krát. Ide o malú vybranú skupinu s rôznym časom sledovania, '
               'nie náhodný test preferencie.</p>',
        '<h2>Ekonomika oboch rád</h2>']
    rows = []
    for key, label in (("all_time", "Celé obdobie"), ("2026", "2026 do konca exportu"), ("last_180_days", "Posledných 180 dní"), ("last_90_days", "Posledných 90 dní")):
        for series in ("Natural", "Premium"):
            f = periods[key]["financial"][series]
            rows.append([label, series, euros(f["net"]), euros(f["cogs"]), euros(f["contribution"]), rate(f["contribution_rate"])])
    chunks += [table(["Obdobie", "Rada", "Tržby bez DPH", "Nákupné náklady", "Príspevok", "% z tržieb"], rows),
        '<p>Príspevok = tržby bez DPH po zľavách mínus nákupné náklady podľa reportingu. '
        'Nie je to čistý zisk: reklama, doprava, balenie, práca a fixné náklady tu nie sú odpočítané. '
        'Náklady v exporte sú reportingové priradenia, nie samostatný audit historických dodávateľských faktúr.</p>',
        '<h2>Rozdiel medzi 200 ml a 500 ml</h2>']
    rows = []
    for size in ("200", "500"):
        view = recent["paid_single_bottle_size_economics"]
        rows.append([size + " ml", *[euros(view[s][size]["contribution_per_paid_bottle"])
                                     for s in ("Natural", "Premium")]])
    ylang_cost = recent["premium_price_cost_to_match_natural_absolute_contribution"]["200:07"]["premium_cogs_per_bottle"]
    chunks += [table(["Samostatná fľaša, posledných 180 dní", "Príspevok Natural / kus", "Príspevok Premium / kus"], rows),
        '<p>Slabšia ekonomika Premium sa sústreďuje do 500 ml. Pri 200 ml sú priemery veľmi blízke. '
        'Nižšie uvedená kontrola rovnakej vône zachováva pôvodný mix Premium, takže neporovnáva len rôzne skladby vôní.</p>',
        f'<p>Osobitne treba potvrdiť nákupný náklad Premium Ylang 200 ml: export používa {euros(ylang_cost)} '
        'na fľašu, rovnako ako nastavenie Natural Ylang 200 ml. Je to overené priradenie reportingu, '
        'nie potvrdená dodávateľská faktúra. Ovplyvňuje najmä priaznivejší výsledok 200 ml Premium.</p>',
        '<h2>Čo by znamenalo vyradenie Premium</h2>',
        f'<p>Nasledujúce scenáre používajú posledných 180 dní ({recent["from"]} až {recent["to"]}). '
        'Nemenia existujúci predaj Natural. Prenášajú iba časť pozorovaného predaja Premium. '
        'Nejde o predpoveď počtu zákazníkov. Zatiaľ neuvažujú úsporu prevádzky ani stratu doplnkového košíka.</p>']
    rows = []
    matched = remove_p["same_size_same_scent_singles_only"]
    matched_potential = (Decimal(matched["matched_replacement_contribution"])
                         if matched["break_even_retained_fraction_for_full_removed_contribution"] is not None else None)
    for fraction in ("0", "0.25", "0.5", "0.75", "1"):
        matched_change = (Decimal(fraction) * matched_potential - Decimal(remove_p["lost_observed_contribution"])
                          if matched_potential is not None else None)
        rows.append([rate(fraction), euros(remove_p["same_bottle_quantity_by_size"]["contribution_change_by_retention_fraction"][fraction]),
                     euros(matched_change),
                     euros(remove_p["same_spending"]["contribution_change_by_retention_fraction"][fraction])])
    chunks += [table(["Podiel preneseného predaja", "Rovnaký počet 200/500 ml fliaš", "Aj rovnaká vôňa", "Rovnaká suma nákupov bez DPH"], rows),
        '<p>Tabuľka ukazuje <strong>zmenu</strong> príspevku voči ponechaniu Premium. Pri počte fliaš '
        'sa použijú priemerné ceny a príspevok Natural rovnakej veľkosti v tom istom období. '
        'Pri sume nákupov sa použije celková marža Natural. Tieto predpoklady opisujú dve odlišné situácie.</p>',
        f'<p>Hranica bez straty je <strong>{rate(remove_p["same_bottle_quantity_by_size"]["break_even_retained_fraction"])}</strong> '
        f'počtu fliaš alebo <strong>{rate(remove_p["same_spending"]["break_even_retained_fraction"])}</strong> sumy nákupov. '
        f'Pri presnejšom nahradení rovnakej veľkosti aj vône vychádza '
        f'<strong>{rate(remove_p["same_size_same_scent_singles_only"]["break_even_retained_fraction_for_full_removed_contribution"])}</strong> '
        'počtu fliaš. Posledný údaj je dostupný len pri úplnom pokrytí rovnakých vôní.</p>',
        '<p>Všeobecne: zmena príspevku = prenesený predaj × príspevok Natural pri úplnom prenose '
        '− dnešný príspevok Premium + skutočne odstrániteľné náklady − stratený príspevok ďalších produktov. '
        'Spoločný nájom ani fixné náklady firmy sa automaticky neušetria.</p>']
    rows = []
    for size, v in sorted(recent["premium_size_transfer_sensitivity"].items()):
        rows.append([size + " ml", euros(v["observed_premium_contribution"]),
                     euros(v["delta_at_100pct_transfer"]), rate(v["break_even_retained_bottle_fraction"])])
    chunks += [table(["Samostatné rozhodnutie o balení Premium", "Dnešný príspevok", "Zmena pri 100 % prenose", "Hranica bez straty"], rows),
        '<p>Pri tomto čiastkovom scenári sa nahrádza rovnaký počet fliaš rovnakej veľkosti aj vône. '
        'Ostatné balenie Premium sa nemení. Nezahŕňa prípadný prechod z Premium 500 ml do Premium 200 ml. '
        'Zvyšovanie ceny alebo znižovanie nákladov 500 ml preto stojí za samostatné overenie pred zrušením celej rady.</p>']
    counts = economics["lifetime_buyers"]
    chunks += [f'<p>V celom exporte kúpilo Premium bez pozorovaného nákupu Natural {counts["Premium_only"]} ľudí; '
               f'obe rady kúpilo {counts["Both"]}. „Iba Premium“ neznamená, že odmietajú Natural, '
               'a „obe rady“ nezaručuje prechod. Ochotu prejsť po vyradení v týchto dátach nepozorujeme.</p>',
        '<p>Nižšia marža sama osebe nie je dôvod vyradiť produkt s kladným príspevkom. '
        'Premium môže prinášať ľudí, ktorí by inak nenakúpili, a predaj ďalších produktov. '
        'Zároveň môže presúvať existujúce nákupy z výnosnejšieho Natural. Tieto dve zložky export neoddeľuje.</p>',
        '<h2>Čo by znamenalo vyradenie Natural</h2>',
        f'<p>Pri prenose celej sumy jeho nákupov do Premium za pozorovanú maržu by sa za rovnakých 180 dní '
        f'príspevok zmenil o {euros(remove_n["same_spending"]["contribution_change_by_retention_fraction"]["1"])}. '
        'To je mechanický scenár, nie odhad dopytu. Časť vôní Natural nemá Premium ekvivalent. '
        'Dáta neposkytujú oporu pre vyradenie Natural zo strachu, že je príliš slabý.</p>',
        '<h2>Čo odporúčam overiť pred zmenou ponuky</h2>',
        '<ol><li>Ponechať Natural ako hlavnú radu. Premium zatiaľ zachovať pre zákazníkov, ktorí ho vyhľadávajú.</li>'
        '<li>Porovnať nákupné ceny, zľavy a predajnú cenu Premium. Zlepšenie príspevku nemusí vyžadovať zrušenie rady. '
        'Pri vyššej cene však nemožno predpokladať nezmenený dopyt.</li>'
        '<li>Pri stabilnej dostupnosti otestovať slabšie odporúčanie Premium novým návštevníkom a silnejšie odporúčanie Natural. '
        'Náhodne rozdelené skupiny majú mať rovnakú reklamu a cenové podmienky. Test odporúčania nezmeria úplné odstránenie Premium.</li>'
        '<li>Vyhodnocovať príspevok celého košíka na návštevníka po variabilných nákladoch, konverziu a priemernú hodnotu objednávky. '
        'Následne sledovať návraty v rovnakom 90/180-dňovom okne. Potrebnú dĺžku testu určí návštevnosť a zvolená minimálna zmena.</li>'
        '<li>Pred prípadným úplným vyradením zohľadniť zásoby, viazané peniaze, minimálny odber, dopredaj a odpis zásob. '
        'Tieto vstupy v exporte chýbajú.</li></ol>',
        '<h2>Čo údaje nedokazujú</h2>',
        '<p>Vyššia spokojnosť Premium nie je potvrdená; ani nižšiu spokojnosť nedokazujeme kauzálne. '
        'Chýba skutočná dávka a frekvencia používania, história skladovej dostupnosti, náhodné priradenie a meranie spokojnosti. '
        'Realizovaný export vylučuje stornované a dobropisované objednávky podľa reportingovej politiky; '
        'toto preto nie je analýza reklamácií a vratiek. Menšie podskupiny majú širokú neistotu.</p>',
        '<p>Aktuálne produktové stránky uvádzajú pri oboch radách dávkovanie 5–20 ml. '
        'Samotné označenie „ultra koncentrovaný“ nedáva merateľný pomer spotreby. '
        'Pri kontrole 19. 10. 2026 bol Premium Ylang 500 ml vypredaný, jeho 200 ml variant dostupný. '
        'Dnešný stav nie je dôkazom historického výpadku.</p>',
        '<p>Verejné zdroje: <a href="https://www.vevo.sk/p-1531/parfum-do-prania-vevo-no-07-ylang-absolute">Natural Ylang</a>, '
        '<a href="https://www.vevo.sk/p-1576/parfum-do-prania-vevo-no07-ylang-absolute">Premium Ylang</a>, '
        '<a href="https://www.vevo.sk/c/vevo-fragrance/parfum-do-prania/vevo-natural">ponuka Natural</a>, '
        '<a href="https://www.vevo.sk/c/vevo-fragrance/parfum-do-prania/vevo-premium">ponuka Premium</a>. '
        'Obchodné výsledky pochádzajú z položkového exportu VEVO a z reprodukovateľných súkromných agregácií.</p>',
        '</main></html>']
    return "\n".join(chunks)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--economics", required=True, type=Path)
    parser.add_argument("--retention", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    result = render(json.loads(args.economics.read_text(encoding="utf-8")),
                    json.loads(args.retention.read_text(encoding="utf-8")))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(result, encoding="utf-8")
    print(args.output)


if __name__ == "__main__":
    main()
